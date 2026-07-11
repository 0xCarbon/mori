package mori

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dialRecordingTransport records DialTimeout targets and blocks each dial
// until released, so tests can observe whether push/pull exchanges overlap.
type dialRecordingTransport struct {
	packetCh chan *Packet
	streamCh chan net.Conn

	mu        sync.Mutex
	dialTimes map[string]time.Time
	dialSeen  chan string // receives the addr of every dial as it starts

	release chan struct{}
}

func newDialRecordingTransport() *dialRecordingTransport {
	return &dialRecordingTransport{
		packetCh:  make(chan *Packet),
		streamCh:  make(chan net.Conn),
		dialTimes: make(map[string]time.Time),
		dialSeen:  make(chan string, 16),
		release:   make(chan struct{}),
	}
}

func (t *dialRecordingTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	return net.IPv4(127, 0, 0, 1), 12345, nil
}

func (t *dialRecordingTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return time.Now(), nil
}

func (t *dialRecordingTransport) PacketCh() <-chan *Packet { return t.packetCh }

func (t *dialRecordingTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	t.mu.Lock()
	if _, ok := t.dialTimes[addr]; !ok {
		t.dialTimes[addr] = time.Now()
	}
	t.mu.Unlock()
	t.dialSeen <- addr

	// Block like a slow peer until the test releases us.
	select {
	case <-t.release:
	case <-time.After(5 * time.Second):
	}
	return nil, errors.New("dialRecordingTransport: no real streams")
}

func (t *dialRecordingTransport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *dialRecordingTransport) Shutdown() error { return nil }

// TestPushPull_ConcurrencyOverlapsExchanges: with PushPullConcurrency=2 and
// one slow peer, the second exchange must start while the first is still in
// flight — no head-of-line blocking. With the old single-peer scheduler
// only one dial per cycle ever happens.
func TestPushPull_ConcurrencyOverlapsExchanges(t *testing.T) {
	tr := newDialRecordingTransport()

	c := testConfig(t)
	c.Transport = tr
	c.ProbeInterval = 0
	c.GossipInterval = 0
	c.PushPullInterval = 0 // no scheduler: we drive pushPull() directly
	c.PushPullConcurrency = 2

	m, err := Create(c)
	require.NoError(t, err)
	defer func() { _ = m.Shutdown() }()

	// Two alive peers to exchange with.
	for i, name := range []string{"peer1", "peer2"} {
		a := alive{
			Incarnation: 1,
			Node:        name,
			Addr:        []byte{127, 0, 0, byte(2 + i)},
			Port:        7946,
			Vsn:         m.config.BuildVsnArray(),
		}
		m.aliveNode(&a, false)
	}

	done := make(chan struct{})
	go func() { m.pushPull(); close(done) }()

	// Both dials must START while both are still blocked in the transport:
	// that is the overlap the knob promises.
	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case addr := <-tr.dialSeen:
			seen[addr] = true
		case <-deadline:
			t.Fatalf("only %d concurrent exchange(s) started; PushPullConcurrency=2 must overlap them: %v", len(seen), seen)
		}
	}

	// Distinct peers were selected.
	require.Len(t, seen, 2, "exchanges must target distinct peers")

	// Release the transport and the cycle must join both exchanges.
	close(tr.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pushPull did not join its exchanges")
	}
}

// TestPushPull_DefaultRemainsSinglePeer: zero/one keeps the old scheduling —
// exactly one exchange per cycle.
func TestPushPull_DefaultRemainsSinglePeer(t *testing.T) {
	tr := newDialRecordingTransport()

	c := testConfig(t)
	c.Transport = tr
	c.ProbeInterval = 0
	c.GossipInterval = 0
	c.PushPullInterval = 0

	m, err := Create(c)
	require.NoError(t, err)
	defer func() { _ = m.Shutdown() }()

	for i, name := range []string{"peer1", "peer2", "peer3"} {
		a := alive{
			Incarnation: 1,
			Node:        name,
			Addr:        []byte{127, 0, 0, byte(2 + i)},
			Port:        7946,
			Vsn:         m.config.BuildVsnArray(),
		}
		m.aliveNode(&a, false)
	}

	done := make(chan struct{})
	go func() { m.pushPull(); close(done) }()

	select {
	case <-tr.dialSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("no exchange started")
	}
	// No second dial may start while the first is blocked.
	select {
	case addr := <-tr.dialSeen:
		t.Fatalf("default config dispatched a second concurrent exchange to %s", addr)
	case <-time.After(300 * time.Millisecond):
	}

	close(tr.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pushPull did not return")
	}
}

// TestPushPull_FailureIsolation: one exchange failing must not abort its
// siblings — all selected peers are attempted.
func TestPushPull_FailureIsolation(t *testing.T) {
	tr := newDialRecordingTransport()
	close(tr.release) // dials fail immediately

	c := testConfig(t)
	c.Transport = tr
	c.ProbeInterval = 0
	c.GossipInterval = 0
	c.PushPullInterval = 0
	c.PushPullConcurrency = 3

	m, err := Create(c)
	require.NoError(t, err)
	defer func() { _ = m.Shutdown() }()

	for i, name := range []string{"peer1", "peer2", "peer3"} {
		a := alive{
			Incarnation: 1,
			Node:        name,
			Addr:        []byte{127, 0, 0, byte(2 + i)},
			Port:        7946,
			Vsn:         m.config.BuildVsnArray(),
		}
		m.aliveNode(&a, false)
	}

	done := make(chan struct{})
	go func() { m.pushPull(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pushPull did not return with failing exchanges")
	}

	tr.mu.Lock()
	attempted := len(tr.dialTimes)
	tr.mu.Unlock()
	require.Equal(t, 3, attempted, "every selected peer must be attempted despite sibling failures")
}

// TestPushPull_ConcurrentE2EConvergence: sanity — a real 3-node cluster with
// the knob enabled still converges (state exchange remains correct under
// concurrent scheduling, -race clean).
func TestPushPull_ConcurrentE2EConvergence(t *testing.T) {
	newConfig := func() *Config {
		c := testConfig(t)
		c.GossipInterval = 2 * time.Millisecond
		c.PushPullInterval = 15 * time.Millisecond
		c.PushPullConcurrency = 2
		return c
	}

	c1 := newConfig()
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { _ = m1.Shutdown() }()

	others := make([]*Memberlist, 0, 2)
	for range 2 {
		c := newConfig()
		c.BindPort = m1.config.BindPort
		m, err := Create(c)
		require.NoError(t, err)
		defer func() { _ = m.Shutdown() }()

		n, err := m.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, 1)
		others = append(others, m)
	}

	deadline := time.Now().Add(10 * time.Second)
	for _, m := range append([]*Memberlist{m1}, others...) {
		for len(m.Members()) != 3 {
			if time.Now().After(deadline) {
				t.Fatalf("cluster did not converge with PushPullConcurrency=2: %s sees %d members",
					m.config.Name, len(m.Members()))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
