package mori

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// blockingTransport is a Transport whose packet writes block until released,
// simulating a hung network peer so schedule callbacks (probe/gossip) are
// reliably mid-flight when Shutdown runs.
type blockingTransport struct {
	packetCh chan *Packet
	streamCh chan net.Conn
	inFlight atomic.Int32
	release  chan struct{}
}

func newBlockingTransport() *blockingTransport {
	return &blockingTransport{
		packetCh: make(chan *Packet),
		streamCh: make(chan net.Conn),
		release:  make(chan struct{}),
	}
}

func (t *blockingTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	return net.IPv4(127, 0, 0, 1), 12345, nil
}

func (t *blockingTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	t.inFlight.Add(1)
	defer t.inFlight.Add(-1)
	select {
	case <-t.release:
	case <-time.After(10 * time.Second):
	}
	return time.Now(), nil
}

func (t *blockingTransport) PacketCh() <-chan *Packet { return t.packetCh }

func (t *blockingTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return nil, errors.New("blockingTransport: no streams")
}

func (t *blockingTransport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *blockingTransport) Shutdown() error { return nil }

// TestShutdown_JoinsInFlightScheduleCallbacks: a gossip/probe callback stuck
// in a hung network write must be joined — Shutdown must not return while a
// goroutine of this instance is still inside the transport.
func TestShutdown_JoinsInFlightScheduleCallbacks(t *testing.T) {
	bt := newBlockingTransport()

	c := testConfig(t)
	c.Transport = bt
	c.GossipInterval = time.Millisecond
	c.GossipNodes = 1
	c.ProbeInterval = time.Millisecond
	c.PushPullInterval = 0

	m, err := Create(c)
	require.NoError(t, err)

	// Give gossip a target so it enters the hung write.
	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)

	deadline := time.Now().Add(5 * time.Second)
	for bt.inFlight.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Positive(t, bt.inFlight.Load(), "no schedule callback entered the transport")

	// Release the hung writes shortly after Shutdown starts waiting.
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(bt.release)
	}()

	require.NoError(t, m.Shutdown())
	require.Zero(t, bt.inFlight.Load(),
		"Shutdown returned while a schedule callback was still inside the transport")
}

// TestCreateShutdown_NoGoroutineLeak: acceptance from issue #3 — a tight
// Create → Shutdown loop leaves zero goroutines behind.
func TestCreateShutdown_NoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	for range 10 {
		c := testConfig(t)
		c.GossipInterval = time.Millisecond
		c.ProbeInterval = time.Millisecond
		c.PushPullInterval = time.Millisecond

		m, err := Create(c)
		require.NoError(t, err)
		require.NoError(t, m.Shutdown())
	}
}

// TestCreateJoinShutdown_NoGoroutineLeak: the full acceptance loop with a
// real join between two nodes over loopback.
func TestCreateJoinShutdown_NoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c1 := testConfig(t)
	c1.GossipInterval = time.Millisecond
	m1, err := Create(c1)
	require.NoError(t, err)

	for range 5 {
		c2 := testConfig(t)
		c2.GossipInterval = time.Millisecond
		c2.BindPort = m1.config.BindPort

		m2, err := Create(c2)
		require.NoError(t, err)

		n, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.Equal(t, 1, n)

		require.NoError(t, m2.Shutdown())
	}

	require.NoError(t, m1.Shutdown())
}

// TestShutdown_PushPullOnlySchedule: with probe and gossip disabled but
// push/pull enabled, schedule() starts no tickers — the stop channel must
// still be recorded so deschedule can stop pushPullTrigger and Shutdown's
// join terminates.
func TestShutdown_PushPullOnlySchedule(t *testing.T) {
	c := testConfig(t)
	c.ProbeInterval = 0
	c.GossipInterval = 0
	c.PushPullInterval = time.Second

	m, err := Create(c)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- m.Shutdown() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown wedged: pushPullTrigger was registered but never stopped")
	}
}

// shutdownOnJoinDelegate calls Shutdown synchronously from NotifyJoin when
// a peer (not self) joins — the callback runs on a tracked goroutine
// (handleConn/packetHandler), which must not deadlock the join.
type shutdownOnJoinDelegate struct {
	self string
	m    *Memberlist
	once sync.Once
	done chan error
}

func (d *shutdownOnJoinDelegate) NotifyJoin(n *Node) {
	if n.Name == d.self {
		return
	}
	d.once.Do(func() { d.done <- d.m.Shutdown() })
}
func (d *shutdownOnJoinDelegate) NotifyLeave(*Node)  {}
func (d *shutdownOnJoinDelegate) NotifyUpdate(*Node) {}

// TestShutdown_FromDelegateCallback_NoSelfJoinDeadlock: Shutdown invoked
// synchronously from a delegate callback that runs on a tracked goroutine
// must not wait for itself.
func TestShutdown_FromDelegateCallback_NoSelfJoinDeadlock(t *testing.T) {
	d := &shutdownOnJoinDelegate{done: make(chan error, 1)}

	c1 := testConfig(t)
	c1.GossipInterval = time.Millisecond
	d.self = c1.Name
	c1.Events = d

	m1, err := Create(c1)
	require.NoError(t, err)
	d.m = m1
	defer func() { _ = m1.Shutdown() }()

	// m2 joins m1: m1 learns m2 inside handleConn/packetHandler (tracked),
	// firing NotifyJoin there, which calls m1.Shutdown() synchronously.
	c2 := testConfig(t)
	c2.GossipInterval = time.Millisecond
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { _ = m2.Shutdown() }()

	_, err = m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)

	select {
	case err := <-d.done:
		require.NoError(t, err, "re-entrant Shutdown from callback must succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown called from a delegate callback deadlocked (self-join)")
	}

	// The instance must still quiesce: a second, outside Shutdown call is
	// a no-op, and the background goroutines drain.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m1.hasShutdown() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.True(t, m1.hasShutdown())
}

// shutdownOnLeaveDelegate calls Shutdown from NotifyLeave — which runs on
// the *user* goroutine (via Leave) while holding nodeLock.
type shutdownOnLeaveDelegate struct {
	m    *Memberlist
	once sync.Once
	done chan error
}

func (d *shutdownOnLeaveDelegate) NotifyJoin(*Node)   {}
func (d *shutdownOnLeaveDelegate) NotifyUpdate(*Node) {}
func (d *shutdownOnLeaveDelegate) NotifyLeave(*Node) {
	d.once.Do(func() { d.done <- d.m.Shutdown() })
}

// TestShutdown_FromUntrackedCallbackHoldingNodeLock: Shutdown called from a
// delegate callback on the application goroutine (Leave → NotifyLeave, with
// nodeLock held) must not deadlock — the join cannot run synchronously in a
// context that holds nodeLock.
func TestShutdown_FromUntrackedCallbackHoldingNodeLock(t *testing.T) {
	d := &shutdownOnLeaveDelegate{done: make(chan error, 1)}

	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive())
	d.m = m
	defer func() { _ = m.Shutdown() }()

	leaveDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		leaveDone <- m.LeaveContext(ctx)
	}()

	select {
	case err := <-d.done:
		require.NoError(t, err, "Shutdown from NotifyLeave must not fail")
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown from an untracked callback holding nodeLock deadlocked")
	}

	select {
	case <-leaveDone:
		// Leave may fail with ErrShutdown-wrapped errors; only the return matters.
	case <-time.After(5 * time.Second):
		t.Fatal("Leave never returned after in-callback Shutdown")
	}
}

// TestShutdown_ReentrantFirst_QuiescesAsynchronously: when the FIRST (and
// only) Shutdown call happens inside a delegate callback, the reaper alone
// must drive the instance to quiescence — no second Shutdown call allowed.
func TestShutdown_ReentrantFirst_QuiescesAsynchronously(t *testing.T) {
	d := &shutdownOnLeaveDelegate{done: make(chan error, 1)}

	m := GetMemberlist(t, func(c *Config) {
		c.Events = d
		// Suspicion window far beyond the test: only finishShutdown can
		// clear the timer armed below.
		c.ProbeInterval = time.Second
	})
	require.NoError(t, m.setAlive())
	d.m = m

	// Arm a suspicion timer for a peer: tracked goroutines self-remove on
	// exit, so this timer is the observable that ONLY the reaper's
	// finishShutdown clears.
	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)
	m.suspectNode(&suspect{Incarnation: 1, Node: "peer", From: m.config.Name})

	m.nodeLock.RLock()
	armed := len(m.nodeTimers)
	m.nodeLock.RUnlock()
	require.Equal(t, 1, armed, "suspicion timer must be armed before the leave")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = m.LeaveContext(ctx) // NotifyLeave calls Shutdown re-entrantly

	select {
	case err := <-d.done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("in-callback Shutdown did not return")
	}

	// No further Shutdown calls: the reaper alone must drain the tracked
	// goroutines and clear the suspicion timer.
	deadline := time.Now().Add(5 * time.Second)
	timers := -1
	for time.Now().Before(deadline) {
		m.nodeLock.RLock()
		timers = len(m.nodeTimers)
		m.nodeLock.RUnlock()
		if timers == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Zero(t, timers, "reaper did not clear suspicion timers after re-entrant-first Shutdown")

	m.trackedMu.Lock()
	tracked := len(m.trackedGoids)
	m.trackedMu.Unlock()
	require.Zero(t, tracked, "tracked goroutines survived quiescence")
}

// panicRecoverDelegate panics on NotifyUpdate; the caller recovers. Used to
// prove a panicking callback does not leak the callback-context marker.
type panicRecoverDelegate struct{}

func (panicRecoverDelegate) NotifyJoin(*Node)   {}
func (panicRecoverDelegate) NotifyLeave(*Node)  {}
func (panicRecoverDelegate) NotifyUpdate(*Node) { panic("boom") }

// TestShutdown_AfterRecoveredCallbackPanic_JoinsSynchronously: a delegate
// panic that the application recovers must not leave the goroutine marked
// as callback context — a later Shutdown on it must join synchronously.
func TestShutdown_AfterRecoveredCallbackPanic_JoinsSynchronously(t *testing.T) {
	d := &MockDelegate{}
	d.setMeta([]byte("v1"))

	m := GetMemberlist(t, func(c *Config) {
		c.Delegate = d
		c.Events = panicRecoverDelegate{}
	})
	require.NoError(t, m.setAlive())

	// Trigger NotifyUpdate (meta change) which panics; recover here.
	d.setMeta([]byte("v2"))
	require.Panics(t, func() { _ = m.UpdateNodeContext(context.Background()) })

	// The marker must have been released by the deferred unmark.
	require.False(t, m.inReentrantContext(),
		"recovered callback panic leaked the callback-context marker")

	// And Shutdown on this goroutine must take the synchronous path: after
	// it returns, every tracked goroutine is already gone.
	require.NoError(t, m.Shutdown())
	m.trackedMu.Lock()
	tracked := len(m.trackedGoids)
	m.trackedMu.Unlock()
	require.Zero(t, tracked, "synchronous Shutdown returned before quiescence")
}

// TestShutdown_ConcurrentFromTrackedGoroutine: a tracked goroutine calling
// Shutdown while a normal Shutdown is mid-join must not deadlock on
// shutdownLock (the outer join waits for the tracked goroutine, which used
// to wait for the outer caller's shutdownLock).
func TestShutdown_ConcurrentFromTrackedGoroutine(t *testing.T) {
	bt := newBlockingTransport()

	c := testConfig(t)
	c.Transport = bt
	c.GossipInterval = time.Millisecond
	c.GossipNodes = 1
	c.ProbeInterval = 0
	c.PushPullInterval = 0

	m, err := Create(c)
	require.NoError(t, err)

	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)

	deadline := time.Now().Add(5 * time.Second)
	for bt.inFlight.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Positive(t, bt.inFlight.Load())

	// Outer Shutdown will block joining the gossip goroutine stuck in the
	// hung WriteTo until release fires.
	outer := make(chan error, 1)
	go func() { outer <- m.Shutdown() }()
	time.Sleep(100 * time.Millisecond)

	// A tracked goroutine now calls Shutdown: it must return promptly
	// instead of blocking on shutdownLock (which the outer join would then
	// wait on, forever).
	inner := make(chan error, 1)
	m.goBackground(func() { inner <- m.Shutdown() })

	go func() {
		time.Sleep(400 * time.Millisecond)
		close(bt.release)
	}()

	select {
	case err := <-inner:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("tracked-goroutine Shutdown deadlocked against the outer join")
	}
	select {
	case err := <-outer:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("outer Shutdown never completed")
	}
}

// TestSuspectNode_AfterShutdown_DoesNotArmTimer: suspect messages delivered
// by in-flight handlers while Shutdown joins them must not arm suspicion
// timers that would outlive the instance.
func TestSuspectNode_AfterShutdown_DoesNotArmTimer(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())

	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)

	require.NoError(t, m.Shutdown())

	s := suspect{Incarnation: 1, Node: "peer", From: m.config.Name}
	m.suspectNode(&s)

	m.nodeLock.RLock()
	armed := len(m.nodeTimers)
	m.nodeLock.RUnlock()
	require.Zero(t, armed, "suspicion timer armed on a shut-down instance")
}

// countingEventDelegate counts NotifyLeave calls.
type countingEventDelegate struct {
	leaves atomic.Int32
}

func (d *countingEventDelegate) NotifyJoin(*Node)   {}
func (d *countingEventDelegate) NotifyUpdate(*Node) {}
func (d *countingEventDelegate) NotifyLeave(*Node)  { d.leaves.Add(1) }

// TestShutdown_StopsSuspicionTimers: a pending suspicion timer must not fire
// after Shutdown — no goroutine of a dead instance may mutate node state or
// deliver events.
func TestShutdown_StopsSuspicionTimers(t *testing.T) {
	d := &countingEventDelegate{}

	c := testConfig(t)
	c.Events = d
	// Long probe interval => suspicion timeout far beyond the test window,
	// so the timer can only fire early through the shutdown race we assert
	// against — and never legitimately within the test.
	c.ProbeInterval = time.Second
	c.SuspicionMult = 1

	m, err := Create(c)
	require.NoError(t, err)
	defer func() { _ = m.Shutdown() }()

	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)

	// Suspect the peer: arms a suspicion timer whose expiry marks it dead
	// (which would fire NotifyLeave).
	s := suspect{Incarnation: 1, Node: "peer", From: m.config.Name}
	m.suspectNode(&s)

	m.nodeLock.RLock()
	armed := len(m.nodeTimers)
	m.nodeLock.RUnlock()
	require.Equal(t, 1, armed, "suspicion timer must be armed before shutdown")

	baseline := d.leaves.Load()
	require.NoError(t, m.Shutdown())

	// Force the underlying timer window to elapse; a survivor would fire.
	m.nodeLock.RLock()
	remaining := len(m.nodeTimers)
	m.nodeLock.RUnlock()
	require.Zero(t, remaining, "suspicion timers must be cleared by Shutdown")
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, baseline, d.leaves.Load(),
		"suspicion timer fired after Shutdown")
}
