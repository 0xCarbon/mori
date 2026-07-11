package mori

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingEventDelegate appends (kind, name, meta) tuples in delivery order.
type recordingEventDelegate struct {
	mu     sync.Mutex
	events []string
	metas  [][]byte
	nodes  []*Node
}

func (d *recordingEventDelegate) record(kind string, n *Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, kind+":"+n.Name)
	d.metas = append(d.metas, n.Meta)
	d.nodes = append(d.nodes, n)
}
func (d *recordingEventDelegate) NotifyJoin(n *Node)   { d.record("join", n) }
func (d *recordingEventDelegate) NotifyUpdate(n *Node) { d.record("update", n) }
func (d *recordingEventDelegate) NotifyLeave(n *Node)  { d.record("leave", n) }
func (d *recordingEventDelegate) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.events...)
}

// gateEventDelegate blocks every callback until release is closed.
type gateEventDelegate struct {
	recordingEventDelegate
	entered chan struct{} // closed on first callback entry
	release chan struct{}
	once    sync.Once
}

func (d *gateEventDelegate) gate() {
	d.once.Do(func() { close(d.entered) })
	<-d.release
}
func (d *gateEventDelegate) NotifyJoin(n *Node)   { d.gate(); d.record("join", n) }
func (d *gateEventDelegate) NotifyUpdate(n *Node) { d.gate(); d.record("update", n) }
func (d *gateEventDelegate) NotifyLeave(n *Node)  { d.gate(); d.record("leave", n) }

func newGateEventDelegate() *gateEventDelegate {
	return &gateEventDelegate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func alivePeer(m *Memberlist, name string, oct byte, inc uint32, meta []byte) {
	a := alive{
		Incarnation: inc,
		Node:        name,
		Addr:        []byte{127, 0, 0, oct},
		Port:        7946,
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&a, false)
}

func waitEvents(t *testing.T, d *recordingEventDelegate, want int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := d.snapshot()
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d events, have %v", want, got)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestEvents_DeliveryOrderIsCommitOrder: sequentially committed mutations
// are delivered in exactly that order, single-threaded.
func TestEvents_DeliveryOrderIsCommitOrder(t *testing.T) {
	d := &recordingEventDelegate{}
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	alivePeer(m, "a", 2, 1, nil)
	alivePeer(m, "b", 3, 1, nil)
	alivePeer(m, "a", 2, 2, []byte("m2")) // meta change -> update
	m.deadNode(&dead{Incarnation: 2, Node: "a", From: "a"})
	alivePeer(m, "c", 4, 1, nil)

	got := waitEvents(t, d, 6)
	require.Equal(t,
		[]string{"join:" + m.config.Name, "join:a", "join:b", "update:a", "leave:a", "join:c"},
		got[:6])
}

// TestEvents_PerNodeOrderUnderConcurrentMutators: concurrent producers for
// distinct nodes; each node's lifecycle must arrive in legal per-node order
// (join before update before leave), -race clean.
func TestEvents_PerNodeOrderUnderConcurrentMutators(t *testing.T) {
	d := &recordingEventDelegate{}
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	const nodes = 8
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Go(func() {
			name := fmt.Sprintf("n%d", i)
			oct := byte(10 + i)
			alivePeer(m, name, oct, 1, nil)
			alivePeer(m, name, oct, 2, []byte("v2"))
			m.deadNode(&dead{Incarnation: 2, Node: name, From: name})
		})
	}
	wg.Wait()

	got := waitEvents(t, d, 1+nodes*3)
	perNode := map[string][]string{}
	for _, ev := range got {
		kind, name, _ := strings.Cut(ev, ":")
		perNode[name] = append(perNode[name], kind)
	}
	for i := range nodes {
		name := fmt.Sprintf("n%d", i)
		require.Equal(t, []string{"join", "update", "leave"}, perNode[name],
			"per-node order violated for %s", name)
	}
}

// TestEvents_BlockingConsumerDoesNotStallProtocol: with the consumer blocked
// in a callback, reads and state mutations proceed unimpeded.
func TestEvents_BlockingConsumerDoesNotStallProtocol(t *testing.T) {
	d := newGateEventDelegate()
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()
	defer close(d.release)

	<-d.entered // consumer now blocked in NotifyJoin(self)

	// Mutations and reads must not block. (Lifecycle calls' ctx-bounded
	// behavior under a blocked consumer is covered by the barrier tests —
	// their broadcast wait needs a scheduler this fixture doesn't run.)
	done := make(chan struct{})
	go func() {
		alivePeer(m, "p1", 2, 1, nil)
		alivePeer(m, "p2", 3, 1, nil)
		_ = m.Members()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked event consumer stalled membership processing")
	}
}

// TestEvents_LeaveBarrierBoundedAndRetryable: with a blocked consumer, the
// leave commits but delivery confirmation times out with the caller's ctx;
// retries wait on the SAME persisted receipt; after release, a retry
// confirms.
func TestEvents_LeaveBarrierBoundedAndRetryable(t *testing.T) {
	d := newGateEventDelegate()
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	<-d.entered // block the dispatcher on the self-join delivery

	ctx1, cancel1 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel1()
	err := m.LeaveContext(ctx1)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, m.hasLeft(), "leave must be committed despite unconfirmed delivery")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	err = m.LeaveContext(ctx2)
	require.Error(t, err, "retry against a still-blocked consumer must also time out")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(d.release)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	require.NoError(t, m.LeaveContext(ctx3), "retry after release confirms via the persisted receipt")
}

// leaveFromJoinDelegate calls LeaveContext from inside NotifyJoin.
type leaveFromJoinDelegate struct {
	m    *Memberlist
	self string
	once sync.Once
	done chan error
}

func (d *leaveFromJoinDelegate) NotifyJoin(n *Node) {
	if n.Name != d.self {
		return
	}
	d.once.Do(func() { d.done <- d.m.LeaveContext(context.Background()) })
}
func (d *leaveFromJoinDelegate) NotifyUpdate(*Node) {}
func (d *leaveFromJoinDelegate) NotifyLeave(*Node)  {}

// TestEvents_ReentrantLeaveFromCallback: LeaveContext called from inside a
// callback must not deadlock on leaveSem nor self-wait on its own delivery
// (barrier bypassed in callback context).
func TestEvents_ReentrantLeaveFromCallback(t *testing.T) {
	d := &leaveFromJoinDelegate{done: make(chan error, 1)}
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	d.m = m
	d.self = m.config.Name
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	select {
	case err := <-d.done:
		require.NoError(t, err, "re-entrant LeaveContext must succeed without deadlock")
	case <-time.After(5 * time.Second):
		t.Fatal("LeaveContext from a callback deadlocked")
	}
	require.True(t, m.hasLeft())
}

// TestEvents_SnapshotsAreImmutable: delivered events carry private deep
// copies — later state mutations must not alter an already-delivered (or
// queued) snapshot.
func TestEvents_SnapshotsAreImmutable(t *testing.T) {
	d := &recordingEventDelegate{}
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	meta := []byte("aaaa")
	alivePeer(m, "p", 2, 1, meta)
	// Mutate the caller's meta buffer in place after commit.
	copy(meta, "XXXX")
	// And advance the node's state, which rewrites nodeMap's meta.
	alivePeer(m, "p", 2, 2, []byte("bbbb"))

	got := waitEvents(t, d, 3) // self-join, join:p, update:p
	require.Equal(t, "join:p", got[1])
	d.mu.Lock()
	joinMeta := string(d.metas[1])
	updateMeta := string(d.metas[2])
	d.mu.Unlock()
	require.Equal(t, "aaaa", joinMeta, "queued snapshot must not alias caller/nodeMap memory")
	require.Equal(t, "bbbb", updateMeta)
}

// TestEvents_WakeStress: an enqueue storm with a fast consumer loses no
// events and no wakeups.
func TestEvents_WakeStress(t *testing.T) {
	d := &recordingEventDelegate{}
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	const updates = 5000
	for i := range updates {
		alivePeer(m, "p", 2, uint32(i+1), fmt.Appendf(nil, "m%d", i))
	}
	// self-join + join:p + (updates-1) meta updates
	got := waitEvents(t, d, 1+updates)
	require.Len(t, got, 1+updates)
}

// TestEvents_PendingIncludesInFlight: with the consumer blocked on the
// first event, the pending count covers the in-flight event plus the queue.
func TestEvents_PendingIncludesInFlight(t *testing.T) {
	d := newGateEventDelegate()
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	<-d.entered // dispatcher blocked delivering self-join (in flight)

	alivePeer(m, "p1", 2, 1, nil)
	alivePeer(m, "p2", 3, 1, nil)

	m.eventMu.Lock()
	pending := m.eventPending
	queued := len(m.events)
	m.eventMu.Unlock()
	require.Equal(t, 3, pending, "pending must include the in-flight event")
	require.Equal(t, 2, queued)

	close(d.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.eventMu.Lock()
		pending = m.eventPending
		m.eventMu.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending never drained: %d", pending)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestEvents_CreateDeliversSelfJoinBeforeReturn: Create preserves the
// pre-#6 synchrony for the local join.
func TestEvents_CreateDeliversSelfJoinBeforeReturn(t *testing.T) {
	d := &recordingEventDelegate{}
	c := testConfig(t)
	c.Events = d

	m, err := Create(c)
	require.NoError(t, err)
	defer func() { _ = m.Shutdown() }()

	got := d.snapshot()
	require.NotEmpty(t, got, "NotifyJoin(self) must be delivered before Create returns")
	require.Equal(t, "join:"+c.Name, got[0])
}

// TestEvents_ShutdownDrainsQueue: Shutdown seals admission, drains every
// committed event, and joins the dispatcher — even when the consumer is
// blocked at teardown start (released concurrently).
func TestEvents_ShutdownDrainsQueue(t *testing.T) {
	d := newGateEventDelegate()
	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive(nil))

	<-d.entered
	alivePeer(m, "p1", 2, 1, nil)
	alivePeer(m, "p2", 3, 1, nil)

	go func() {
		time.Sleep(200 * time.Millisecond)
		close(d.release)
	}()
	require.NoError(t, m.Shutdown())

	got := d.snapshot()
	require.Len(t, got, 3, "all committed events must be delivered before Shutdown returns: %v", got)

	m.eventMu.Lock()
	sealed, pending := m.eventsSealed, m.eventPending
	m.eventMu.Unlock()
	require.True(t, sealed)
	require.Zero(t, pending)
}

// TestEvents_UnattachedReceipts: with no EventDelegate configured, the
// lifecycle barriers are trivially satisfied — no waiting, no hang.
func TestEvents_UnattachedReceipts(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive(nil))
	defer func() { _ = m.Shutdown() }()

	start := time.Now()
	require.NoError(t, m.UpdateNodeContext(context.Background()))
	require.NoError(t, m.LeaveContext(context.Background()))
	// And the idempotent retry with no receipt-bearing event:
	require.NoError(t, m.LeaveContext(context.Background()))
	require.Less(t, time.Since(start), 2*time.Second)
}
