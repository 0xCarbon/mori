package mori

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// limitRecordingDelegate records the limit passed to NodeMeta and returns a
// fixed meta payload.
type limitRecordingDelegate struct {
	meta      []byte
	lastLimit atomic.Int64
}

func (d *limitRecordingDelegate) NodeMeta(limit int) []byte {
	d.lastLimit.Store(int64(limit))
	return d.meta
}
func (d *limitRecordingDelegate) NotifyMsg([]byte)                {}
func (d *limitRecordingDelegate) GetBroadcasts(o, l int) [][]byte { return nil }
func (d *limitRecordingDelegate) LocalState(join bool) []byte     { return nil }
func (d *limitRecordingDelegate) MergeRemoteState(b []byte, join bool) {
}

func TestMetaMaxSize_ZeroMeansDefault(t *testing.T) {
	d := &limitRecordingDelegate{meta: []byte("ok")}

	m := GetMemberlist(t, func(c *Config) { c.Delegate = d })
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	require.Equal(t, int64(MetaMaxSize), d.lastLimit.Load(),
		"delegate must be offered the default limit when MetaMaxSize is 0")

	// Over the default cap still errors.
	d.meta = make([]byte, MetaMaxSize+1)
	require.ErrorIs(t, m.UpdateNode(time.Second), ErrMetaTooLarge)
}

func TestMetaMaxSize_KnobRaisesTheCap(t *testing.T) {
	d := &limitRecordingDelegate{meta: make([]byte, 700)}

	m := GetMemberlist(t, func(c *Config) {
		c.Delegate = d
		c.MetaMaxSize = 1024
	})
	require.NoError(t, m.setAlive(), "700 B meta must be accepted with a 1024 B cap")
	defer func() { _ = m.Shutdown() }()

	require.Equal(t, int64(1024), d.lastLimit.Load(),
		"delegate must be offered the configured limit")

	// Above the configured cap still errors.
	d.meta = make([]byte, 1025)
	require.ErrorIs(t, m.UpdateNode(time.Second), ErrMetaTooLarge)

	// Back under the cap works again.
	d.meta = make([]byte, 1000)
	require.NoError(t, m.UpdateNode(time.Second))
}

func TestCreate_MetaMaxSizeExceedsTransportBudget(t *testing.T) {
	d := &limitRecordingDelegate{meta: []byte("small")}

	c := testConfig(t)
	c.Delegate = d
	// A full-size alive message with 2000 B of meta cannot fit the default
	// 1400 B UDP packet budget: Create must fail with a clear error, not
	// let gossip silently drop oversized packets later.
	c.MetaMaxSize = 2000

	m, err := Create(c)
	if m != nil {
		defer func() { _ = m.Shutdown() }()
	}
	require.Error(t, err)
	require.Nil(t, m)
	require.Contains(t, err.Error(), "MetaMaxSize")
}

func TestCreate_AbsurdMetaMaxSize_FailsWithoutAllocating(t *testing.T) {
	c := testConfig(t)
	// An absurd cap must be rejected by comparing against the budget
	// before materializing any slice of that size — a clean error, not an
	// OOM or makeslice panic.
	c.MetaMaxSize = 1 << 40

	m, err := Create(c)
	if m != nil {
		defer func() { _ = m.Shutdown() }()
	}
	require.Error(t, err)
	require.Nil(t, m)
	require.Contains(t, err.Error(), "MetaMaxSize")
}

// smallPacketTransport advertises a tiny MaxPacketSize.
type smallPacketTransport struct {
	*blockingTransport
}

func (t *smallPacketTransport) MaxPacketSize() int { return 300 }

// TestCreate_TransportMaxPacketSizeConstrains: a transport advertising a
// small MaxPacketSize must constrain the meta budget even when
// UDPBufferSize would allow it — including through the internal shim
// wrapper that would otherwise hide the optional interface.
func TestCreate_TransportMaxPacketSizeConstrains(t *testing.T) {
	c := testConfig(t)
	c.Transport = &smallPacketTransport{newBlockingTransport()}
	// Default MetaMaxSize (512): a full alive message exceeds 300 bytes.

	m, err := Create(c)
	if m != nil {
		defer func() { _ = m.Shutdown() }()
	}
	require.Error(t, err)
	require.Nil(t, m)
	require.Contains(t, err.Error(), "MetaMaxSize")
}

// TestMetaMaxSize_CrossConfigConvergence: a producer with a raised cap must
// gossip 1 KB meta to receivers running the default config — the cap is
// producer-side only, so clusters can be upgraded node by node. Uses a
// raised UDPBufferSize so the 1 KB alive message fits the packet budget.
func TestMetaMaxSize_CrossConfigConvergence(t *testing.T) {
	bigMeta := make([]byte, 1024)
	for i := range bigMeta {
		bigMeta[i] = byte('a' + i%26)
	}
	d1 := &limitRecordingDelegate{meta: bigMeta}

	newConfig := func(f func(*Config)) *Config {
		c := testConfig(t)
		c.GossipInterval = 2 * time.Millisecond
		c.PushPullInterval = 10 * time.Millisecond
		c.UDPBufferSize = 4096
		if f != nil {
			f(c)
		}
		return c
	}

	c1 := newConfig(func(c *Config) {
		c.Delegate = d1
		c.MetaMaxSize = 2048
	})
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { _ = m1.Shutdown() }()

	// Receivers stay on the default MetaMaxSize.
	receivers := make([]*Memberlist, 0, 2)
	for range 2 {
		c := newConfig(func(c *Config) { c.BindPort = m1.config.BindPort })
		m, err := Create(c)
		require.NoError(t, err)
		defer func() { _ = m.Shutdown() }()

		n, err := m.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, 1)
		receivers = append(receivers, m)
	}

	metaOn := func(m *Memberlist, name string) []byte {
		for _, node := range m.Members() {
			if node.Name == name {
				return node.Meta
			}
		}
		return nil
	}

	deadline := time.Now().Add(10 * time.Second)
	for _, m := range receivers {
		for {
			if got := metaOn(m, c1.Name); len(got) == len(bigMeta) && string(got) == string(bigMeta) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("receiver %s never observed the 1 KB meta from the raised-cap producer",
					m.config.Name)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
