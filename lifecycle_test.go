package mori

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingEventDelegate blocks NotifyLeave until release is closed, signaling
// entered as soon as the callback starts. It reproduces the shape of a
// consumer (taba's healthManager teardown) that cannot finish its NotifyLeave
// handling until an in-flight UpdateNode call returns.
type blockingEventDelegate struct {
	entered chan struct{} // closed when NotifyLeave starts
	release chan struct{} // NotifyLeave returns once this is closed
	once    sync.Once
}

func (d *blockingEventDelegate) NotifyJoin(*Node)   {}
func (d *blockingEventDelegate) NotifyUpdate(*Node) {}
func (d *blockingEventDelegate) NotifyLeave(*Node) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
}

func TestLeave_AfterShutdown_ReturnsErrShutdown(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	require.NoError(t, m.Shutdown())

	err := m.Leave(time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrShutdown)
}

func TestUpdateNode_AfterShutdown_ReturnsErrShutdown(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	require.NoError(t, m.Shutdown())

	err := m.UpdateNode(time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrShutdown)
}

func TestCreate_OversizedMeta_ReturnsError(t *testing.T) {
	d := &MockDelegate{}
	d.setMeta(make([]byte, MetaMaxSize+1))

	c := testConfig(t)
	c.Delegate = d

	m, err := Create(c)
	if m != nil {
		defer func() { _ = m.Shutdown() }()
	}
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMetaTooLarge)
	require.Nil(t, m)
}

func TestUpdateNode_OversizedMeta_ReturnsErrMetaTooLarge(t *testing.T) {
	d := &MockDelegate{}
	d.setMeta([]byte("ok"))

	m := GetMemberlist(t, func(c *Config) { c.Delegate = d })
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	d.setMeta(make([]byte, MetaMaxSize+1))

	err := m.UpdateNode(time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMetaTooLarge)
}

func TestUpdateNode_MissingSelf_ReturnsErrNoLocalNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()
	delete(m.nodeMap, m.config.Name)
	m.nodeLock.Unlock()

	err := m.UpdateNode(time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoLocalNode)
}

func TestLeave_MissingSelf_ReturnsErrNoLocalNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()
	delete(m.nodeMap, m.config.Name)
	m.nodeLock.Unlock()

	err := m.Leave(time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoLocalNode)
}

func TestUpdateNodeContext_HonorsDeadlineUnderLockContention(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	// Simulate a wedged lock holder: the deadline must bound the entire
	// call, including nodeLock acquisition.
	m.nodeLock.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.UpdateNodeContext(ctx)
	elapsed := time.Since(start)

	m.nodeLock.Unlock()

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, time.Second, "call must return promptly on ctx expiry")

	// The abandoned lock waiter must self-clean: the instance stays usable.
	require.NoError(t, m.UpdateNodeContext(context.Background()))
	require.Len(t, m.Members(), 1)
}

func TestLeaveContext_HonorsDeadlineUnderLockContention(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.LeaveContext(ctx)
	elapsed := time.Since(start)

	m.nodeLock.Unlock()

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, time.Second, "call must return promptly on ctx expiry")

	// The aborted Leave must not have committed: the node did not leave and
	// a later unbounded Leave still works.
	require.False(t, m.hasLeft(), "aborted Leave must not mark the node as left")
	require.NoError(t, m.LeaveContext(context.Background()))
	require.True(t, m.hasLeft())
}

// TestLeaveContext_BlockedNotifyLeave_Issue373 reproduces the taba#373 wedge:
// Leave runs the EventDelegate's NotifyLeave while holding nodeLock, and the
// callback cannot return until a concurrent UpdateNode call finishes. With an
// unbounded UpdateNode this deadlocks forever; with a ctx-bounded call the
// update returns DeadlineExceeded, the callback completes, and Leave finishes.
func TestLeaveContext_BlockedNotifyLeave_Issue373(t *testing.T) {
	d := &blockingEventDelegate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	m := GetMemberlist(t, func(c *Config) { c.Events = d })
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	updateDone := make(chan error, 1)
	go func() {
		// Wait until Leave holds nodeLock inside NotifyLeave, then try to
		// update: the old code wedges here forever.
		<-d.entered
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		err := m.UpdateNodeContext(ctx)
		updateDone <- err
		close(d.release)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m.LeaveContext(ctx), "Leave must complete once the blocked update is bounded")

	select {
	case err := <-updateDone:
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("UpdateNodeContext never returned: lifecycle wedge")
	}
}

func TestLifecycle_ConcurrentUpdateLeaveShutdown(t *testing.T) {
	c1 := testConfig(t)
	c1.GossipInterval = time.Millisecond
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { _ = m1.Shutdown() }()

	c2 := testConfig(t)
	c2.GossipInterval = time.Millisecond
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { _ = m2.Shutdown() }()

	n, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Hammer the lifecycle surface concurrently. Every call must return
	// within its bound: no wedge, no panic, race-clean.
	const updaters = 8
	var wg sync.WaitGroup
	errs := make(chan error, updaters*20+2)

	for range updaters {
		wg.Go(func() {
			for range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				errs <- m2.UpdateNodeContext(ctx)
				cancel()
			}
		})
	}

	wg.Go(func() {
		time.Sleep(10 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		errs <- m2.LeaveContext(ctx)
	})

	wg.Go(func() {
		time.Sleep(30 * time.Millisecond)
		errs <- m2.Shutdown()
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("lifecycle calls wedged: concurrent Update/Leave/Shutdown did not finish")
	}
	close(errs)

	// Calls may fail (deadline, shutdown, leave in progress) but must fail
	// with a known error, never wedge or panic.
	for err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, ErrShutdown) ||
			errors.Is(err, ErrLeft) ||
			errors.Is(err, ErrNoLocalNode) {
			continue
		}
		t.Fatalf("unexpected lifecycle error: %v", err)
	}
}

// TestLifecycle_E2E_UpdateAndLeavePropagate exercises the changed surface on
// a real 3-node cluster over loopback: a meta update via UpdateNodeContext
// must propagate to peers, and LeaveContext must converge peers to StateLeft.
func TestLifecycle_E2E_UpdateAndLeavePropagate(t *testing.T) {
	d1 := &MockDelegate{}
	d1.setMeta([]byte("v1"))

	newConfig := func(f func(*Config)) *Config {
		c := testConfig(t)
		c.GossipInterval = 2 * time.Millisecond
		c.PushPullInterval = 10 * time.Millisecond
		if f != nil {
			f(c)
		}
		return c
	}

	c1 := newConfig(func(c *Config) { c.Delegate = d1 })
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { _ = m1.Shutdown() }()

	members := []*Memberlist{m1}
	for range 2 {
		c := newConfig(func(c *Config) { c.BindPort = m1.config.BindPort })
		m, err := Create(c)
		require.NoError(t, err)
		defer func() { _ = m.Shutdown() }()

		n, err := m.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, 1)
		members = append(members, m)
	}

	waitFor := func(desc string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", desc)
	}

	for _, m := range members {
		waitFor("full membership", func() bool { return len(m.Members()) == 3 })
	}

	// Update the local meta and re-advertise; peers must observe it.
	d1.setMeta([]byte("v2"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m1.UpdateNodeContext(ctx))

	metaOn := func(m *Memberlist, name string) []byte {
		for _, node := range m.Members() {
			if node.Name == name {
				return node.Meta
			}
		}
		return nil
	}
	for _, m := range members[1:] {
		waitFor("meta v2 on peer", func() bool {
			return string(metaOn(m, c1.Name)) == "v2"
		})
	}

	// Leave and verify peers converge to 2 members with m1 marked left.
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaveCancel()
	require.NoError(t, m1.LeaveContext(leaveCtx))

	for _, m := range members[1:] {
		waitFor("peer sees m1 leave", func() bool {
			if len(m.Members()) != 2 {
				return false
			}
			m.nodeLock.RLock()
			state, ok := m.nodeMap[c1.Name]
			left := ok && state.State == StateLeft
			m.nodeLock.RUnlock()
			return left
		})
	}
}

// TestLockNodes_AbandonedWaitersDoNotAccumulate: repeated ctx-expired
// lifecycle calls while the lock is wedged must not leave one parked
// goroutine each — the acquisition queue keeps at most one helper parked
// per instance regardless of call count.
func TestLockNodes_AbandonedWaitersDoNotAccumulate(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()

	before := runtime.NumGoroutine()
	for range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		err := m.UpdateNodeContext(ctx)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	after := runtime.NumGoroutine()

	// Every aborted call must have deregistered its waiter; the queue
	// design allows at most the single parked helper to remain.
	m.lockq.mu.Lock()
	waiting := len(m.lockq.waiters)
	m.lockq.mu.Unlock()
	require.Zero(t, waiting, "aborted calls left waiters registered")

	m.nodeLock.Unlock()

	// Loose global sanity bound only (NumGoroutine is process-wide and
	// noisy): 50 leaked waiters would trip it, background churn will not.
	require.LessOrEqual(t, after-before, 10,
		"abandoned lock waiters accumulated: %d goroutines grew over 50 aborted calls", after-before)

	// Once the wedge clears, the instance must be fully usable.
	require.NoError(t, m.UpdateNodeContext(context.Background()))
}

// panickingEventDelegate panics on NotifyUpdate to verify lifecycle calls
// do not retain nodeLock when user callbacks panic.
type panickingEventDelegate struct{}

func (panickingEventDelegate) NotifyJoin(*Node)   {}
func (panickingEventDelegate) NotifyLeave(*Node)  {}
func (panickingEventDelegate) NotifyUpdate(*Node) { panic("delegate exploded") }

// TestUpdateNodeContext_DelegatePanicReleasesLock: a panicking EventDelegate
// must not leave nodeLock held — the panic propagates but the lock is
// released, keeping the instance readable.
func TestUpdateNodeContext_DelegatePanicReleasesLock(t *testing.T) {
	d := &MockDelegate{}
	d.setMeta([]byte("v1"))

	m := GetMemberlist(t, func(c *Config) {
		c.Delegate = d
		c.Events = panickingEventDelegate{}
	})
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	// Change meta so aliveNodeLocked fires NotifyUpdate, which panics.
	d.setMeta([]byte("v2"))
	require.Panics(t, func() { _ = m.UpdateNodeContext(context.Background()) })

	// The lock must have been released by the panic unwind.
	done := make(chan struct{})
	go func() { m.Members(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nodeLock retained after delegate panic")
	}
}

// TestUpdateNode_AfterLeave_ReturnsErrLeft: once the node has left, an
// update cannot be broadcast (aliveNode drops self-alive messages after
// leave), so the call must fail fast instead of waiting for a broadcast
// confirmation that can never come.
func TestUpdateNode_AfterLeave_ReturnsErrLeft(t *testing.T) {
	c1 := testConfig(t)
	c1.GossipInterval = time.Millisecond
	m1, err := Create(c1)
	require.NoError(t, err)
	defer func() { _ = m1.Shutdown() }()

	c2 := testConfig(t)
	c2.GossipInterval = time.Millisecond
	c2.BindPort = m1.config.BindPort
	m2, err := Create(c2)
	require.NoError(t, err)
	defer func() { _ = m2.Shutdown() }()

	n, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m2.LeaveContext(ctx))

	// With a live peer present, the old behavior waited forever for a
	// broadcast that aliveNodeLocked never enqueues after leave.
	start := time.Now()
	err = m2.UpdateNode(3 * time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLeft)
	require.Less(t, time.Since(start), time.Second, "must fail fast, not wait for the ctx bound")
}

// TestUpdateNodeContext_UnboundedReleasedByShutdown: an unbounded call
// waiting for a broadcast that will never be transmitted must be released
// by Shutdown instead of wedging forever.
func TestUpdateNodeContext_UnboundedReleasedByShutdown(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	// Inject an alive peer so anyAlive() is true. No scheduler is running
	// (GetMemberlist does not schedule), so broadcasts are never sent and
	// the update wait can only be released by Shutdown.
	peer := alive{
		Incarnation: 1,
		Node:        "peer",
		Addr:        []byte{127, 0, 0, 2},
		Port:        7946,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&peer, false)
	require.True(t, m.anyAlive())

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- m.UpdateNodeContext(context.Background())
	}()

	// Give the update time to reach the broadcast wait, then shut down.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, m.Shutdown())

	select {
	case err := <-updateDone:
		require.Error(t, err)
		require.ErrorIs(t, err, ErrShutdown)
	case <-time.After(2 * time.Second):
		t.Fatal("unbounded UpdateNodeContext wedged across Shutdown")
	}
}

// TestLeave_TimeoutWrapper_BoundsWholeCall verifies the time.Duration
// wrappers now bound the entire call, not just the broadcast wait.
func TestLeave_TimeoutWrapper_BoundsWholeCall(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()

	start := time.Now()
	err := m.Leave(100 * time.Millisecond)
	elapsed := time.Since(start)

	m.nodeLock.Unlock()

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, time.Second)
}

func TestUpdateNode_TimeoutWrapper_BoundsWholeCall(t *testing.T) {
	m := GetMemberlist(t, nil)
	require.NoError(t, m.setAlive())
	defer func() { _ = m.Shutdown() }()

	m.nodeLock.Lock()

	start := time.Now()
	err := m.UpdateNode(100 * time.Millisecond)
	elapsed := time.Since(start)

	m.nodeLock.Unlock()

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, time.Second)
}

// Compile-time interface checks for the test delegates.
var (
	_ EventDelegate = (*blockingEventDelegate)(nil)
	_ Delegate      = (*MockDelegate)(nil)
)
