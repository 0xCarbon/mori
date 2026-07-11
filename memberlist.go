// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

/*
Package mori is a library that manages cluster
membership and member failure detection using a gossip based protocol.
It is 0xCarbon's maintained hard fork of github.com/hashicorp/memberlist.

The use cases for such a library are far-reaching: all distributed systems
require membership, and mori is a re-usable solution to managing
cluster membership and node failure detection.

mori is eventually consistent but converges quickly on average.
The speed at which it converges can be heavily tuned via various knobs
on the protocol. Node failures are detected and network partitions are partially
tolerated by attempting to communicate to potentially dead nodes through
multiple routes.
*/
package mori

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/hashicorp/go-metrics/compat"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-sockaddr"
	"github.com/miekg/dns"
)

var errNodeNamesAreRequired = errors.New("memberlist: node names are required by configuration but one was not provided")

// Sentinel errors returned by the lifecycle methods (Create, Leave,
// UpdateNode and their Context variants). Match with errors.Is.
var (
	// ErrShutdown is returned by lifecycle methods invoked after the
	// instance has been shut down.
	ErrShutdown = errors.New("memberlist: already shut down")

	// ErrMetaTooLarge is returned when the delegate provides node meta data
	// longer than MetaMaxSize.
	ErrMetaTooLarge = errors.New("memberlist: node meta data exceeds size limit")

	// ErrNoLocalNode is returned when the local node is missing from the
	// node map, e.g. because the instance never went alive.
	ErrNoLocalNode = errors.New("memberlist: local node not found in node map")

	// ErrLeft is returned by UpdateNode/UpdateNodeContext after the node
	// has left the cluster: the update can never be broadcast, because
	// self-alive messages are dropped once the node has left.
	ErrLeft = errors.New("memberlist: node has left the cluster")
)

type Memberlist struct {
	sequenceNum uint32        // Local sequence number
	incarnation atomic.Uint32 // Local incarnation number
	numNodes    atomic.Uint32 // Number of known nodes (estimate)
	pushPullReq atomic.Uint32 // Number of push/pull requests

	advertiseLock sync.RWMutex
	advertiseAddr net.IP
	advertisePort uint16

	config         *Config
	shutdown       atomic.Uint32 // Used as an atomic boolean value
	shutdownCh     chan struct{}
	shutdownWG     sync.WaitGroup // Joins background goroutines on Shutdown
	trackedMu      sync.Mutex
	trackedGoids   map[uint64]struct{} // goids of shutdownWG-joined goroutines
	callbackGoids  map[uint64]int      // goid -> delegate-callback nesting depth
	leave          atomic.Int32        // Used as an atomic boolean value
	leaveBroadcast chan struct{}

	shutdownLock sync.Mutex // Serializes calls to Shutdown

	// leaveSem is a 1-slot semaphore serializing calls to Leave. A channel
	// rather than a mutex so acquisition can be bounded by a context.
	//
	// Lock ordering: leaveSem is acquired before nodeLock; shutdownLock is
	// independent of both. nodeLock (write) is held while delegate
	// callbacks run (EventDelegate, ConflictDelegate, AliveDelegate), so
	// delegate implementations must not call back into Memberlist methods
	// that acquire nodeLock (Members, UpdateNode, Leave, ...) or they will
	// deadlock; bound such calls with a context if they cannot be avoided.
	leaveSem chan struct{}

	transport NodeAwareTransport

	handoffCh            chan struct{}
	highPriorityMsgQueue *list.List
	lowPriorityMsgQueue  *list.List
	msgQueueLock         sync.Mutex

	nodeLock   sync.RWMutex
	lockq      lockNodesQueue        // ctx-bounded nodeLock acquisition queue
	nodes      []*nodeState          // Known nodes
	nodeMap    map[string]*nodeState // Maps Node.Name -> NodeState
	nodeTimers map[string]*suspicion // Maps Node.Name -> suspicion timer
	awareness  *awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTick   chan struct{}
	probeIndex int

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	broadcasts *TransmitLimitedQueue

	logger *log.Logger

	// metricLabels is the slice of labels to put on all emitted metrics
	metricLabels []metrics.Label
}

// BuildVsnArray creates the array of Vsn
func (conf *Config) BuildVsnArray() []uint8 {
	return []uint8{
		ProtocolVersionMin, ProtocolVersionMax, conf.ProtocolVersion,
		conf.DelegateProtocolMin, conf.DelegateProtocolMax,
		conf.DelegateProtocolVersion,
	}
}

// newMemberlist creates the network listeners.
// Does not schedule execution of background maintenance.
func newMemberlist(conf *Config) (*Memberlist, error) {
	if conf.ProtocolVersion < ProtocolVersionMin {
		return nil, fmt.Errorf("protocol version '%d' too low. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	} else if conf.ProtocolVersion > ProtocolVersionMax {
		return nil, fmt.Errorf("protocol version '%d' too high. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}

	if len(conf.SecretKey) > 0 {
		if conf.Keyring == nil {
			keyring, err := NewKeyring(nil, conf.SecretKey)
			if err != nil {
				return nil, err
			}
			conf.Keyring = keyring
		} else {
			if err := conf.Keyring.AddKey(conf.SecretKey); err != nil {
				return nil, err
			}
			if err := conf.Keyring.UseKey(conf.SecretKey); err != nil {
				return nil, err
			}
		}
	}

	if conf.LogOutput != nil && conf.Logger != nil {
		return nil, fmt.Errorf("cannot specify both LogOutput and Logger; please choose a single log configuration setting")
	}

	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	// Set up a network transport by default if a custom one wasn't given
	// by the config.
	transport := conf.Transport
	if transport == nil {
		nc := &NetTransportConfig{
			BindAddrs:    []string{conf.BindAddr},
			BindPort:     conf.BindPort,
			Logger:       logger,
			MetricLabels: conf.MetricLabels,
		}

		// See comment below for details about the retry in here.
		makeNetRetry := func(limit int) (*NetTransport, error) {
			var err error
			for range limit {
				var nt *NetTransport
				if nt, err = NewNetTransport(nc); err == nil {
					return nt, nil
				}
				if strings.Contains(err.Error(), "address already in use") {
					logger.Printf("[DEBUG] memberlist: Got bind error: %v", err)
					continue
				}
			}

			return nil, fmt.Errorf("failed to obtain an address: %v", err)
		}

		// The dynamic bind port operation is inherently racy because
		// even though we are using the kernel to find a port for us, we
		// are attempting to bind multiple protocols (and potentially
		// multiple addresses) with the same port number. We build in a
		// few retries here since this often gets transient errors in
		// busy unit tests.
		limit := 1
		if conf.BindPort == 0 {
			limit = 10
		}

		nt, err := makeNetRetry(limit)
		if err != nil {
			return nil, fmt.Errorf("could not set up network transport: %v", err)
		}
		if conf.BindPort == 0 {
			port := nt.GetAutoBindPort()
			conf.BindPort = port
			conf.AdvertisePort = port
			logger.Printf("[DEBUG] memberlist: Using dynamic bind port %d", port)
		}
		transport = nt
	}

	nodeAwareTransport, ok := transport.(NodeAwareTransport)
	if !ok {
		logger.Printf("[DEBUG] memberlist: configured Transport is not a NodeAwareTransport and some features may not work as desired")
		nodeAwareTransport = &shimNodeAwareTransport{transport}
	}

	if len(conf.Label) > LabelMaxSize {
		return nil, fmt.Errorf("could not use %q as a label: too long", conf.Label)
	}

	if conf.Label != "" {
		nodeAwareTransport = &labelWrappedTransport{
			label:              conf.Label,
			NodeAwareTransport: nodeAwareTransport,
		}
	}

	m := &Memberlist{
		config:               conf,
		shutdownCh:           make(chan struct{}),
		trackedGoids:         make(map[uint64]struct{}),
		callbackGoids:        make(map[uint64]int),
		leaveBroadcast:       make(chan struct{}, 1),
		leaveSem:             make(chan struct{}, 1),
		transport:            nodeAwareTransport,
		handoffCh:            make(chan struct{}, 1),
		highPriorityMsgQueue: list.New(),
		lowPriorityMsgQueue:  list.New(),
		nodeMap:              make(map[string]*nodeState),
		nodeTimers:           make(map[string]*suspicion),
		awareness:            newAwareness(conf.AwarenessMaxMultiplier, conf.MetricLabels),
		ackHandlers:          make(map[uint32]*ackHandler),
		broadcasts:           &TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		logger:               logger,
		metricLabels:         conf.MetricLabels,
	}
	m.broadcasts.NumNodes = func() int {
		return m.estNumNodes()
	}

	// Fail fast if a full-size alive message could never fit the transport
	// packet budget — otherwise gossip drops oversized packets at runtime
	// with no clear signal.
	if err := m.validateMetaMaxSize(); err != nil {
		if serr := m.transport.Shutdown(); serr != nil {
			logger.Printf("[ERR] Failed to shutdown transport: %v", serr)
		}
		return nil, err
	}

	// Get the final advertise address from the transport, which may need
	// to see which address we bound to. We'll refresh this each time we
	// send out an alive message.
	if _, _, err := m.refreshAdvertise(); err != nil {
		return nil, err
	}

	m.goBackground(m.streamListen)
	m.goBackground(m.packetListen)
	m.goBackground(m.packetHandler)
	m.goBackground(m.checkBroadcastQueueDepth)
	return m, nil
}

// Create will create a new Memberlist using the given configuration.
// This will not connect to any other node (see Join) yet, but will start
// all the listeners to allow other nodes to join this memberlist.
// After creating a Memberlist, the configuration given should not be
// modified by the user anymore.
func Create(conf *Config) (*Memberlist, error) {
	m, err := newMemberlist(conf)
	if err != nil {
		return nil, err
	}
	if err := m.setAlive(); err != nil {
		_ = m.Shutdown()
		return nil, err
	}
	m.schedule()
	return m, nil
}

// Join is used to take an existing Memberlist and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause
// remote nodes to become aware of the existence of this node, effectively
// joining the cluster.
//
// This returns the number of hosts successfully contacted and an error if
// none could be reached. If an error is returned, the node did not successfully
// join the cluster.
func (m *Memberlist) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		addrs, err := m.resolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("failed to resolve %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			m.logger.Printf("[WARN] memberlist: %v", err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			a := Address{Addr: hp, Name: addr.nodeName}
			if err := m.pushPullNode(a, true); err != nil {
				err = fmt.Errorf("failed to join %s: %v", a.Addr, err)
				errs = multierror.Append(errs, err)
				m.logger.Printf("[DEBUG] memberlist: %v", err)
				continue
			}
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

// ipPort holds information about a node we want to try to join.
type ipPort struct {
	ip       net.IP
	port     uint16
	nodeName string // optional
}

// tcpLookupIP is a helper to initiate a TCP-based DNS lookup for the given host.
// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
// the response has the truncate bit set, which isn't common on DNS servers like
// Consul's. By doing the TCP lookup directly, we get the best chance for the
// largest list of hosts to join. Since joins are relatively rare events, it's ok
// to do this rather expensive operation.
func (m *Memberlist) tcpLookupIP(host string, defaultPort uint16, nodeName string) ([]ipPort, error) {
	// Don't attempt any TCP lookups against non-fully qualified domain
	// names, since those will likely come from the resolv.conf file.
	if !strings.Contains(host, ".") {
		return nil, nil
	}

	// Make sure the domain name is terminated with a dot (we know there's
	// at least one character at this point).
	dn := host
	if dn[len(dn)-1] != '.' {
		dn = dn + "."
	}

	// See if we can find a server to try.
	cc, err := dns.ClientConfigFromFile(m.config.DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {
		// We support host:port in the DNS config, but need to add the
		// default port if one is not supplied.
		server := cc.Servers[0]
		if !hasPort(server) {
			server = net.JoinHostPort(server, cc.Port)
		}

		// Do the lookup.
		c := new(dns.Client)
		c.Net = "tcp"
		msg := new(dns.Msg)
		msg.SetQuestion(dn, dns.TypeANY)
		in, _, err := c.Exchange(msg, server)
		if err != nil {
			return nil, err
		}

		// Handle any IPs we get back that we can attempt to join.
		var ips []ipPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case (*dns.A):
				ips = append(ips, ipPort{ip: rr.A, port: defaultPort, nodeName: nodeName})
			case (*dns.AAAA):
				ips = append(ips, ipPort{ip: rr.AAAA, port: defaultPort, nodeName: nodeName})
			case (*dns.CNAME):
				m.logger.Printf("[DEBUG] memberlist: Ignoring CNAME RR in TCP-first answer for '%s'", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

// resolveAddr is used to resolve the address into an address,
// port, and error. If no port is given, use the default
func (m *Memberlist) resolveAddr(hostStr string) ([]ipPort, error) {
	// First peel off any leading node name. This is optional.
	nodeName := ""
	if slashIdx := strings.Index(hostStr, "/"); slashIdx >= 0 {
		if slashIdx == 0 {
			return nil, fmt.Errorf("empty node name provided")
		}
		nodeName = hostStr[0:slashIdx]
		hostStr = hostStr[slashIdx+1:]
	}

	// This captures the supplied port, or the default one.
	hostStr = ensurePort(hostStr, m.config.BindPort)
	host, sport, err := net.SplitHostPort(hostStr)
	if err != nil {
		return nil, err
	}
	lport, err := strconv.ParseUint(sport, 10, 16)
	if err != nil {
		return nil, err
	}
	port := uint16(lport)

	// If it looks like an IP address we are done. The SplitHostPort() above
	// will make sure the host part is in good shape for parsing, even for
	// IPv6 addresses.
	if ip := net.ParseIP(host); ip != nil {
		return []ipPort{
			ipPort{ip: ip, port: port, nodeName: nodeName},
		}, nil
	}

	// First try TCP so we have the best chance for the largest list of
	// hosts to join. If this fails it's not fatal since this isn't a standard
	// way to query DNS, and we have a fallback below.
	ips, err := m.tcpLookupIP(host, port, nodeName)
	if err != nil {
		m.logger.Printf("[DEBUG] memberlist: TCP-first lookup failed for '%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	// If TCP didn't yield anything then use the normal Go resolver which
	// will try UDP, then might possibly try TCP again if the UDP response
	// indicates it was truncated.
	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips = make([]ipPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, ipPort{ip: ip, port: port, nodeName: nodeName})
	}
	return ips, nil
}

// setAlive is used to mark this node as being alive. This is the same
// as if we received an alive notification our own network channel for
// ourself.
func (m *Memberlist) setAlive() error {
	// Get the final advertise address from the transport, which may need
	// to see which address we bound to.
	addr, port, err := m.refreshAdvertise()
	if err != nil {
		return err
	}

	// Check if this is a public address without encryption
	ipAddr, err := sockaddr.NewIPAddr(addr.String())
	if err != nil {
		return fmt.Errorf("failed to parse interface addresses: %v", err)
	}
	ifAddrs := []sockaddr.IfAddr{
		sockaddr.IfAddr{
			SockAddr: ipAddr,
		},
	}
	_, publicIfs, _ := sockaddr.IfByRFC("6890", ifAddrs)

	if len(publicIfs) > 0 && !m.config.EncryptionEnabled() {
		m.logger.Printf("[WARN] memberlist: Binding to public address without encryption!")
	}

	// Set any metadata from the delegate.
	meta, err := m.delegateMeta()
	if err != nil {
		return err
	}

	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&a, true)

	return nil
}

func (m *Memberlist) getAdvertise() (net.IP, uint16) {
	m.advertiseLock.RLock()
	defer m.advertiseLock.RUnlock()
	return m.advertiseAddr, m.advertisePort
}

func (m *Memberlist) setAdvertise(addr net.IP, port int) {
	m.advertiseLock.Lock()
	defer m.advertiseLock.Unlock()
	m.advertiseAddr = addr
	m.advertisePort = uint16(port)
}

func (m *Memberlist) refreshAdvertise() (net.IP, int, error) {
	addr, port, err := m.transport.FinalAdvertiseAddr(
		m.config.AdvertiseAddr, m.config.AdvertisePort)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get final advertise address: %v", err)
	}
	m.setAdvertise(addr, port)
	return addr, port, nil
}

// LocalNode is used to return the local Node
func (m *Memberlist) LocalNode() *Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	state := m.nodeMap[m.config.Name]
	return &state.Node
}

// metaMaxSize returns the effective producer-side cap on node meta data:
// Config.MetaMaxSize, or the MetaMaxSize default when unset.
func (m *Memberlist) metaMaxSize() int {
	if m.config.MetaMaxSize > 0 {
		return m.config.MetaMaxSize
	}
	return MetaMaxSize
}

// delegateMeta fetches the local node meta data from the delegate, if any,
// enforcing the size limit.
func (m *Memberlist) delegateMeta() ([]byte, error) {
	if m.config.Delegate == nil {
		return nil, nil
	}
	limit := m.metaMaxSize()
	var meta []byte
	m.runCallback(func() { meta = m.config.Delegate.NodeMeta(limit) })
	if len(meta) > limit {
		return nil, fmt.Errorf("%w: %d bytes > %d-byte limit", ErrMetaTooLarge, len(meta), limit)
	}
	return meta, nil
}

// validateMetaMaxSize checks at construction time that a worst-case alive
// message — IPv6 address, max port/incarnation, meta at the configured
// cap — fits the transport packet budget.
func (m *Memberlist) validateMetaMaxSize() error {
	limit := m.metaMaxSize()

	a := alive{
		Incarnation: math.MaxUint32,
		Node:        m.config.Name,
		Addr:        make([]byte, 16),
		Port:        math.MaxUint16,
		Meta:        make([]byte, limit),
		Vsn:         m.config.BuildVsnArray(),
	}
	buf, err := encode(aliveMsg, &a, m.config.MsgpackUseNewTimeFormat)
	if err != nil {
		return fmt.Errorf("memberlist: could not size a full alive message: %w", err)
	}
	msgSize := buf.Len()

	budget := m.config.UDPBufferSize
	if size, ok := maxPacketSizeOf(m.transport); ok {
		if budget <= 0 {
			budget = size
		} else {
			budget = min(budget, size)
		}
	}
	if budget <= 0 {
		// No packet budget configured and none advertised: nothing to
		// validate against (degenerate, stream-only style setups).
		return nil
	}
	budget -= labelOverhead(m.config.Label)
	if m.config.EncryptionEnabled() {
		budget -= encryptOverhead(m.encryptionVersion())
	}

	if msgSize > budget {
		return fmt.Errorf("memberlist: MetaMaxSize %d does not fit the transport packet budget: a full-size alive message is %d bytes but only %d are available (UDPBufferSize or the transport's MaxPacketSize, minus label and encryption overhead)",
			limit, msgSize, budget)
	}
	return nil
}

// lockNodesQueue coordinates ctx-bounded acquisitions of nodeLock so that
// abandoned attempts do not each park a goroutine on the mutex: at most one
// helper goroutine waits on nodeLock per instance, handing ownership to the
// oldest still-interested caller.
type lockNodesQueue struct {
	mu      sync.Mutex
	waiters []chan struct{} // each buffered(1); receiving means owning nodeLock
	parked  bool            // a helper goroutine is live (parked or handing off)
}

// lockNodes acquires the nodeLock write lock, giving up if ctx is done
// first. Bounded: no matter how many calls time out while the lock is
// wedged, at most one helper goroutine stays parked on the lock, and it
// self-cleans once the lock is released. Note that a pending writer blocks
// new readers, so a parked helper can delay readers until the current lock
// holder releases.
func (m *Memberlist) lockNodes(ctx context.Context) error {
	if ctx.Done() == nil {
		m.nodeLock.Lock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Intentionally non-FIFO: this fast path can barge ahead of queued
	// waiters when the lock frees up at the right instant. Fairness is
	// bounded by the mutex's own starvation mode; all waiters are served.
	if m.nodeLock.TryLock() {
		return nil
	}

	q := &m.lockq
	ready := make(chan struct{}, 1)
	q.mu.Lock()
	q.waiters = append(q.waiters, ready)
	spawn := !q.parked
	q.parked = true
	q.mu.Unlock()
	if spawn {
		go m.lockNodesHelper()
	}

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
	}

	// Deregister. Removal and handoff are mutually exclusive under q.mu:
	// either we are still queued (deregister and give up) or ownership was
	// already handed off (take the token and release the lock).
	q.mu.Lock()
	for i, w := range q.waiters {
		if w == ready {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			q.mu.Unlock()
			return ctx.Err()
		}
	}
	q.mu.Unlock()
	<-ready
	m.nodeLock.Unlock()
	return ctx.Err()
}

// lockNodesHelper acquires nodeLock on behalf of queued lockNodes callers,
// handing ownership over FIFO. It exits — releasing the lock if nobody
// wants it anymore — as soon as the queue drains.
func (m *Memberlist) lockNodesHelper() {
	q := &m.lockq
	for {
		m.nodeLock.Lock()
		q.mu.Lock()
		if len(q.waiters) == 0 {
			q.parked = false
			q.mu.Unlock()
			m.nodeLock.Unlock()
			return
		}
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w <- struct{}{} // buffered: never blocks; the receiver owns the lock now
		if len(q.waiters) == 0 {
			q.parked = false
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
	}
}

// waitForBroadcast blocks until the given broadcast-notify channel fires,
// ctx is done, or the instance shuts down (after which the broadcast would
// never be transmitted). hasPeers reports whether there was any other alive
// member to broadcast to when the broadcast was enqueued; without one there
// is nothing to wait for. It is a plain value so this wait acquires no
// locks: the caller evaluates it inside the critical section that enqueued
// the broadcast, keeping the ctx-bounded path lock-free after commit.
func (m *Memberlist) waitForBroadcast(ctx context.Context, notifyCh <-chan struct{}, hasPeers bool, what string) error {
	if !hasPeers {
		return nil
	}
	select {
	case <-notifyCh:
		return nil
	case <-m.shutdownCh:
		return fmt.Errorf("memberlist: %s broadcast interrupted: %w", what, ErrShutdown)
	case <-ctx.Done():
		return fmt.Errorf("memberlist: timeout waiting for %s broadcast: %w", what, ctx.Err())
	}
}

// timeoutContext converts a legacy timeout into a context: a positive
// timeout bounds the call, zero or negative means no bound.
func timeoutContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.Background(), func() {}
}

// UpdateNodeContext is used to trigger re-advertising the local node. This
// is primarily used with a Delegate to support dynamic updates to the local
// meta data. The entire call — including internal lock acquisition and the
// wait for the update broadcast to reach a member — is bounded by ctx.
//
// The bound cannot cover user code: delegate callbacks (Delegate.NodeMeta,
// EventDelegate notifications) run synchronously within the call and are
// required not to block — see the EventDelegate contract.
func (m *Memberlist) UpdateNodeContext(ctx context.Context) error {
	if m.hasShutdown() {
		return ErrShutdown
	}

	// Get the node meta data
	meta, err := m.delegateMeta()
	if err != nil {
		return err
	}

	if err := m.lockNodes(ctx); err != nil {
		return fmt.Errorf("memberlist: update node: %w", err)
	}
	notifyCh := make(chan struct{}, 1)
	var hasPeers bool
	// The critical section runs in a closure so the deferred unlock also
	// covers panics from delegate callbacks inside aliveNodeLocked.
	err = func() error {
		defer m.nodeLock.Unlock()

		// After leave, self-alive messages are dropped (aliveNodeLocked
		// guard), so the update could never be broadcast: fail fast.
		if m.hasLeft() {
			return ErrLeft
		}
		state, ok := m.nodeMap[m.config.Name]
		if !ok {
			return ErrNoLocalNode
		}

		// Format a new alive message
		a := alive{
			Incarnation: m.nextIncarnation(),
			Node:        m.config.Name,
			Addr:        state.Addr,
			Port:        state.Port,
			Meta:        meta,
			Vsn:         m.config.BuildVsnArray(),
		}
		m.aliveNodeLocked(&a, notifyCh, true)
		hasPeers = m.anyAliveLocked()
		return nil
	}()
	if err != nil {
		return err
	}

	// Wait for the broadcast or cancellation
	return m.waitForBroadcast(ctx, notifyCh, hasPeers, "update")
}

// UpdateNode is a convenience wrapper around UpdateNodeContext: a positive
// timeout bounds the entire call, zero or negative means no bound.
func (m *Memberlist) UpdateNode(timeout time.Duration) error {
	ctx, cancel := timeoutContext(timeout)
	defer cancel()
	return m.UpdateNodeContext(ctx)
}

// Deprecated: SendTo is deprecated in favor of SendBestEffort, which requires a node to
// target. If you don't have a node then use SendToAddress.
func (m *Memberlist) SendTo(to net.Addr, msg []byte) error {
	a := Address{Addr: to.String(), Name: ""}
	return m.SendToAddress(a, msg)
}

func (m *Memberlist) SendToAddress(a Address, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	return m.rawSendMsgPacket(a, nil, buf)
}

// Deprecated: SendToUDP is deprecated in favor of SendBestEffort.
func (m *Memberlist) SendToUDP(to *Node, msg []byte) error {
	return m.SendBestEffort(to, msg)
}

// Deprecated: SendToTCP is deprecated in favor of SendReliable.
func (m *Memberlist) SendToTCP(to *Node, msg []byte) error {
	return m.SendReliable(to, msg)
}

// SendBestEffort uses the unreliable packet-oriented interface of the transport
// to target a user message at the given node (this does not use the gossip
// mechanism). The maximum size of the message depends on the configured
// UDPBufferSize for this memberlist instance.
func (m *Memberlist) SendBestEffort(to *Node, msg []byte) error {
	// Encode as a user message
	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	// Send the message
	a := Address{Addr: to.Address(), Name: to.Name}
	return m.rawSendMsgPacket(a, to, buf)
}

// SendReliable uses the reliable stream-oriented interface of the transport to
// target a user message at the given node (this does not use the gossip
// mechanism). Delivery is guaranteed if no error is returned, and there is no
// limit on the size of the message.
func (m *Memberlist) SendReliable(to *Node, msg []byte) error {
	return m.sendUserMsg(to.FullAddress(), msg)
}

// Members returns a list of all known live nodes. The node structures
// returned must not be modified. If you wish to modify a Node, make a
// copy first.
func (m *Memberlist) Members() []*Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

// NumMembers returns the number of alive nodes currently known. Between
// the time of calling this and calling Members, the number of alive nodes
// may have changed, so this shouldn't be used to determine how many
// members will be returned by Members.
func (m *Memberlist) NumMembers() (alive int) {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	for _, n := range m.nodes {
		if !n.DeadOrLeft() {
			alive++
		}
	}

	return
}

// LeaveContext will broadcast a leave message but will not shutdown the
// background listeners, meaning the node will continue participating in
// gossip and state updates.
//
// The entire call — including internal lock acquisition and the wait for
// the leave broadcast to reach a member — is bounded by ctx. If ctx is done
// before the leave is committed, no state is changed and the call can be
// retried. This method is safe to call multiple times; after shutdown it
// returns ErrShutdown.
//
// The bound cannot cover user code: the EventDelegate.NotifyLeave callback
// runs synchronously within the call and is required not to block — see
// the EventDelegate contract.
func (m *Memberlist) LeaveContext(ctx context.Context) error {
	// Serialize concurrent leavers.
	select {
	case m.leaveSem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("memberlist: leave: %w", ctx.Err())
	}
	defer func() { <-m.leaveSem }()

	if m.hasShutdown() {
		return ErrShutdown
	}
	if m.hasLeft() {
		return nil
	}

	if err := m.lockNodes(ctx); err != nil {
		return fmt.Errorf("memberlist: leave: %w", err)
	}
	var hasPeers bool
	// The critical section runs in a closure so the deferred unlock also
	// covers panics from the NotifyLeave callback inside deadNodeLocked.
	err := func() error {
		defer m.nodeLock.Unlock()

		state, ok := m.nodeMap[m.config.Name]
		if !ok {
			return ErrNoLocalNode
		}

		// Flag the leave while holding nodeLock: any queued aliveMsg about
		// the local node is blocked on the lock and will observe the flag,
		// so it cannot re-join us — and a ctx abort before this point
		// leaves no state behind.
		m.leave.Store(1)

		// This dead message is special, because Node and From are the
		// same. This helps other nodes figure out that a node left
		// intentionally. When Node equals From, other nodes know for
		// sure this node is gone.
		d := dead{
			Incarnation: state.Incarnation,
			Node:        state.Name,
			From:        state.Name,
		}
		m.deadNodeLocked(&d)
		hasPeers = m.anyAliveLocked()
		return nil
	}()
	if err != nil {
		return err
	}

	// Block until the broadcast goes out or ctx is done.
	return m.waitForBroadcast(ctx, m.leaveBroadcast, hasPeers, "leave")
}

// Leave is a convenience wrapper around LeaveContext: a positive timeout
// bounds the entire call, zero or negative means no bound.
func (m *Memberlist) Leave(timeout time.Duration) error {
	ctx, cancel := timeoutContext(timeout)
	defer cancel()
	return m.LeaveContext(ctx)
}

// Check for any other alive node.
func (m *Memberlist) anyAlive() bool {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	return m.anyAliveLocked()
}

// anyAliveLocked is anyAlive for callers that already hold nodeLock.
func (m *Memberlist) anyAliveLocked() bool {
	for _, n := range m.nodes {
		if !n.DeadOrLeft() && n.Name != m.config.Name {
			return true
		}
	}
	return false
}

// GetHealthScore gives this instance's idea of how well it is meeting the soft
// real-time requirements of the protocol. Lower numbers are better, and zero
// means "totally healthy".
func (m *Memberlist) GetHealthScore() int {
	return m.awareness.GetHealthScore()
}

// ProtocolVersion returns the protocol version currently in use by
// this memberlist.
func (m *Memberlist) ProtocolVersion() uint8 {
	// NOTE: This method exists so that in the future we can control
	// any locking if necessary, if we change the protocol version at
	// runtime, etc.
	return m.config.ProtocolVersion
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// goBackground runs fn on a goroutine that Shutdown joins via shutdownWG,
// recording its goid so a re-entrant Shutdown from a delegate callback can
// detect it would be waiting for itself. Goroutines that are themselves
// tracked may safely spawn more (the counter is non-zero while they run).
func (m *Memberlist) goBackground(fn func()) {
	m.shutdownWG.Go(func() {
		id := goid()
		m.trackedMu.Lock()
		m.trackedGoids[id] = struct{}{}
		m.trackedMu.Unlock()
		defer func() {
			m.trackedMu.Lock()
			delete(m.trackedGoids, id)
			m.trackedMu.Unlock()
		}()
		fn()
	})
}

// runCallback runs a delegate callback with the current goroutine marked
// as callback context, so a re-entrant Shutdown from user code — which may
// hold nodeLock or run on a joined goroutine — is detected. The unmark is
// deferred: a panicking callback must not leak the marker (the panic
// propagates).
func (m *Memberlist) runCallback(fn func()) {
	id := goid()
	m.trackedMu.Lock()
	m.callbackGoids[id]++
	m.trackedMu.Unlock()
	defer func() {
		m.trackedMu.Lock()
		if m.callbackGoids[id]--; m.callbackGoids[id] <= 0 {
			delete(m.callbackGoids, id)
		}
		m.trackedMu.Unlock()
	}()
	fn()
}

// inReentrantContext reports whether the caller runs on a goroutine that
// Shutdown joins, or inside a delegate callback (which may hold nodeLock).
// In either case Shutdown must not join synchronously.
func (m *Memberlist) inReentrantContext() bool {
	id := goid()
	m.trackedMu.Lock()
	_, tracked := m.trackedGoids[id]
	depth := m.callbackGoids[id]
	m.trackedMu.Unlock()
	return tracked || depth > 0
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// Shutdown returns only after every joined background goroutine of this
// instance (listeners, packet handler, schedule callbacks such as probe/
// gossip/push-pull, in-flight stream handlers, and TCP ping fallbacks) has
// finished, and all pending suspicion timers have been stopped and cleared:
// once it returns, no goroutine of this instance mutates node state,
// delivers delegate events, or touches the transport. Two bounded
// exceptions stay untracked: a suspicion timer callback that already fired
// past the best-effort stop runs to completion but is a guarded no-op
// (hasShutdown), and an abandoned ctx lock waiter may stay parked until the
// current nodeLock holder releases.
//
// The join is unconditional; it terminates promptly because all callback
// waits are bounded (ack timeouts, connection deadlines). Worst case is
// about 2×TCPTimeout when a stream peer stalls mid push/pull (accept
// deadline plus the sendLocalState deadline reset). A delegate callback
// that violates the non-blocking contract can delay it indefinitely.
//
// Exception: when Shutdown is called re-entrantly — from a delegate
// callback (which may hold internal locks and may run on a joined
// goroutine) — it cannot wait for the quiescence it is part of. In that
// case the teardown still completes, but the join and timer cleanup finish
// asynchronously and Shutdown returns immediately.
//
// This method is safe to call multiple times and from delegate callbacks.
func (m *Memberlist) Shutdown() error {
	// Phase 1 — teardown, exactly once. shutdownLock is never held across
	// the join below: a delegate callback on a joined goroutine calling
	// Shutdown would block on it, and the join would wait for that
	// goroutine — a cycle.
	m.shutdownLock.Lock()
	didTeardown := false
	if !m.hasShutdown() {
		didTeardown = true

		// Shut down the transport first, which should block until it's
		// completely torn down. If we kill the memberlist-side handlers
		// those I/O handlers might get stuck.
		if err := m.transport.Shutdown(); err != nil {
			m.logger.Printf("[ERR] Failed to shutdown transport: %v", err)
		}

		// Now tear down everything else.
		m.shutdown.Store(1)
		close(m.shutdownCh)
		m.deschedule()
	}
	m.shutdownLock.Unlock()

	// Phase 2 — quiescence.
	if m.inReentrantContext() {
		// Waiting here would deadlock: the caller either runs on a
		// goroutine the join waits for, or holds nodeLock inside a
		// delegate callback. Hand the join to an untracked reaper (only
		// the call that performed the teardown spawns one).
		if didTeardown {
			go m.finishShutdown()
		}
		return nil
	}
	m.finishShutdown()
	return nil
}

// finishShutdown joins every background goroutine and then clears pending
// suspicion timers. Never called while holding nodeLock: the goroutines
// being joined acquire it.
func (m *Memberlist) finishShutdown() {
	m.shutdownWG.Wait()

	// Stop pending suspicion timers — after the join, so timers armed by
	// in-flight handlers (suspectNode also guards against arming past this
	// point) cannot resurrect entries. Expiry callbacks mutate node state
	// and deliver events, which must not happen after Shutdown; a callback
	// that already slipped past Stop is a no-op via its hasShutdown guard.
	m.nodeLock.Lock()
	for node, timer := range m.nodeTimers {
		timer.timer.Stop()
		delete(m.nodeTimers, node)
	}
	m.nodeLock.Unlock()
}

func (m *Memberlist) hasShutdown() bool {
	return m.shutdown.Load() == 1
}

func (m *Memberlist) hasLeft() bool {
	return m.leave.Load() == 1
}

func (m *Memberlist) getNodeState(addr string) NodeStateType {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.State
}

func (m *Memberlist) getNodeStateChange(addr string) time.Time {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	n := m.nodeMap[addr]
	return n.StateChange
}

func (m *Memberlist) changeNode(addr string, f func(*nodeState)) {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()

	n := m.nodeMap[addr]
	f(n)
}

// checkBroadcastQueueDepth periodically checks the size of the broadcast queue
// to see if it is too large
func (m *Memberlist) checkBroadcastQueueDepth() {
	for {
		select {
		case <-time.After(m.config.QueueCheckInterval):
			numq := m.broadcasts.NumQueued()
			metrics.AddSampleWithLabels([]string{"memberlist", "queue", "broadcasts"}, float32(numq), m.metricLabels)
		case <-m.shutdownCh:
			return
		}
	}
}
