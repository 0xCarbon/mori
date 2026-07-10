package mori

import (
	"errors"
	"net"
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
