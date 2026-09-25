// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// simNet is an in-memory network for protocol tests. Everything it uses is
// channels and net.Pipe, so a cluster built on it runs inside a
// testing/synctest bubble with exact, instantaneous time.
//
// Semantics follow UDP and TCP where the protocol depends on them:
// packets to an unknown or down address, or to a full receive queue, are
// dropped silently; dialing an unknown or down address fails with a
// *net.OpError{Op: "dial"}, which failedRemote treats as a remote failure.
type simNet struct {
	mu     sync.Mutex
	nodes  map[string]*simTransport // by "ip:port"
	byName map[string]*simTransport

	// latency delays every packet delivery (one way). Zero delivers at
	// once, which inside a synctest bubble means zero elapsed time.
	latency time.Duration
}

func newSimNet() *simNet {
	return &simNet{nodes: map[string]*simTransport{}, byName: map[string]*simTransport{}}
}

// simAddr is a net.Addr in the simulated network.
type simAddr struct{ hostPort string }

func (a simAddr) Network() string { return "sim" }
func (a simAddr) String() string  { return a.hostPort }

// simTransport is one node's NodeAwareTransport on a simNet.
type simTransport struct {
	net      *simNet
	name     string
	ip       net.IP
	port     int
	addr     simAddr
	packetCh chan *Packet
	streamCh chan net.Conn

	mu   sync.Mutex
	down bool
}

var _ NodeAwareTransport = (*simTransport)(nil)

// add registers a node at ip:7946 and returns its transport.
func (n *simNet) add(name string, ip net.IP) *simTransport {
	const port = 7946
	t := &simTransport{
		net:      n,
		name:     name,
		ip:       ip,
		port:     port,
		addr:     simAddr{net.JoinHostPort(ip.String(), strconv.Itoa(port))},
		packetCh: make(chan *Packet, 1024),
		streamCh: make(chan net.Conn, 64),
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[t.addr.hostPort] = t
	n.byName[name] = t
	return t
}

// setDown makes a node unreachable (true) or reachable again (false).
func (t *simTransport) setDown(down bool) {
	t.mu.Lock()
	t.down = down
	t.mu.Unlock()
}

func (t *simTransport) isDown() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.down
}

// peer resolves a destination; nil when unknown or down.
func (t *simTransport) peer(a Address) *simTransport {
	t.net.mu.Lock()
	dest := t.net.nodes[a.Addr]
	t.net.mu.Unlock()
	if dest == nil || dest.isDown() || t.isDown() {
		return nil
	}
	return dest
}

func (t *simTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	return t.ip, t.port, nil
}

func (t *simTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, Address{Addr: addr})
}

func (t *simTransport) WriteToAddress(b []byte, a Address) (time.Time, error) {
	now := time.Now()
	dest := t.peer(a)
	if dest == nil {
		return now, nil
	}
	p := &Packet{Buf: append([]byte(nil), b...), From: t.addr}
	deliver := func() {
		p.Timestamp = time.Now()
		select {
		case dest.packetCh <- p:
		default: // receive queue full: dropped, like UDP
		}
	}
	if t.net.latency > 0 {
		time.AfterFunc(t.net.latency, deliver)
	} else {
		deliver()
	}
	return now, nil
}

func (t *simTransport) PacketCh() <-chan *Packet { return t.packetCh }

func (t *simTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(Address{Addr: addr}, timeout)
}

func (t *simTransport) DialAddressTimeout(a Address, _ time.Duration) (net.Conn, error) {
	dest := t.peer(a)
	if dest == nil {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Addr: simAddr{a.Addr}, Err: syscall.ECONNREFUSED}
	}
	local, remote := net.Pipe()
	select {
	case dest.streamCh <- simConn{Conn: remote, local: dest.addr, remote: t.addr}:
	default:
		_ = local.Close()
		_ = remote.Close()
		return nil, &net.OpError{Op: "dial", Net: "tcp", Addr: dest.addr, Err: errors.New("accept backlog full")}
	}
	return simConn{Conn: local, local: t.addr, remote: dest.addr}, nil
}

func (t *simTransport) StreamCh() <-chan net.Conn { return t.streamCh }

func (t *simTransport) Shutdown() error {
	t.setDown(true)
	return nil
}

// simConn reports simulated addresses instead of net.Pipe's "pipe".
type simConn struct {
	net.Conn
	local, remote simAddr
}

func (c simConn) LocalAddr() net.Addr  { return c.local }
func (c simConn) RemoteAddr() net.Addr { return c.remote }

// simIP returns the i-th test address, 10.0.0.i (i in 1..254).
func simIP(i int) net.IP { return net.IPv4(10, 0, 0, byte(i)).To4() }

// simConfig returns a DefaultLocalConfig for node name "nodeI" at
// 10.0.0.I on net, registering (or replacing) its transport. f may adjust
// the config.
func simConfig(t *testing.T, n *simNet, i int, f func(*Config)) *Config {
	t.Helper()
	name := fmt.Sprintf("node%d", i)
	c := DefaultLocalConfig()
	c.Name = name
	c.BindAddr = simIP(i).String()
	c.BindPort = 7946
	c.AdvertisePort = 7946
	c.Transport = n.add(name, simIP(i))
	c.Logger = testLogger(t, name)
	if f != nil {
		f(c)
	}
	return c
}

// simMember creates (but does not schedule or announce) a Memberlist for
// node I on net and shuts it down when the test ends.
func simMember(t *testing.T, n *simNet, i int, f func(*Config)) *Memberlist {
	t.Helper()
	m, err := newMemberlist(simConfig(t, n, i, f))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return m
}

// simCreate runs Create for node I on net (alive and scheduled) and shuts
// it down when the test ends.
func simCreate(t *testing.T, n *simNet, i int, f func(*Config)) *Memberlist {
	t.Helper()
	m, err := Create(simConfig(t, n, i, f))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return m
}

// TestSimNetSemantics pins the transport behavior the protocol tests rely
// on.
func TestSimNetSemantics(t *testing.T) {
	n := newSimNet()
	a, b := n.add("a", simIP(1)), n.add("b", simIP(2))

	if _, err := a.WriteToAddress([]byte("x"), Address{Addr: "10.0.0.9:7946"}); err != nil {
		t.Fatalf("packet to an unknown address: %v, want a silent drop", err)
	}
	if _, err := a.WriteToAddress([]byte("hi"), Address{Addr: b.addr.hostPort}); err != nil {
		t.Fatal(err)
	}
	p := <-b.PacketCh()
	if string(p.Buf) != "hi" || p.From.String() != a.addr.hostPort {
		t.Fatalf("delivered %q from %s", p.Buf, p.From)
	}

	_, err := a.DialAddressTimeout(Address{Addr: "10.0.0.9:7946"}, time.Second)
	if !failedRemote(err) {
		t.Fatalf("dial to an unknown address = %v, want a remote failure", err)
	}
	b.setDown(true)
	if _, err := a.DialAddressTimeout(Address{Addr: b.addr.hostPort}, time.Second); !failedRemote(err) {
		t.Fatalf("dial to a down node = %v, want a remote failure", err)
	}
	_, _ = a.WriteToAddress([]byte("lost"), Address{Addr: b.addr.hostPort})
	select {
	case p := <-b.PacketCh():
		t.Fatalf("down node received %q", p.Buf)
	default:
	}
	b.setDown(false)

	conn, err := a.DialAddressTimeout(Address{Addr: b.addr.hostPort}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	accepted := <-b.StreamCh()
	if accepted.RemoteAddr().String() != a.addr.hostPort || conn.RemoteAddr().String() != b.addr.hostPort {
		t.Fatalf("stream addresses %s -> %s", accepted.RemoteAddr(), conn.RemoteAddr())
	}
	_ = conn.Close()
	_ = accepted.Close()
}
