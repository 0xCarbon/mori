// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func HostMemberlist(host string, t *testing.T, f func(*Config)) *Memberlist {
	t.Helper()

	c := DefaultLANConfig()
	c.Name = host
	c.BindAddr = host
	c.BindPort = 0 // choose a free port
	c.Logger = testLogger(t, host)
	if f != nil {
		f(c)
	}

	m, err := newMemberlist(c)
	if err != nil {
		t.Fatalf("failed to get memberlist: %s", err)
	}
	return m
}

func TestMemberList_Probe(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{
		Node:        addr1.String(),
		Addr:        []byte(addr1),
		Port:        uint16(m1.config.BindPort),
		Incarnation: 1,
		Vsn:         m1.config.BuildVsnArray(),
	}
	m1.aliveNode(&a1, true)
	a2 := alive{
		Node:        addr2.String(),
		Addr:        []byte(addr2),
		Port:        uint16(m2.config.BindPort),
		Incarnation: 1,
		Vsn:         m2.config.BuildVsnArray(),
	}
	m1.aliveNode(&a2, false)

	// should ping addr2
	m1.probe()

	// Should not be marked suspect
	n := m1.nodeMap[addr2.String()]
	if n.State != StateAlive {
		t.Fatalf("Expect node to be alive")
	}

	// Should increment seqno
	if m1.sequenceNum != 1 {
		t.Fatalf("bad seqno %v", m2.sequenceNum)
	}
}

// probeCluster builds, in a synctest bubble, node1 plus the given live
// peers (node2..) and absent nodes on a simulated network, all known to
// node1 as alive with vsn (nil vsn means a legacy peer with no versions).
func probeCluster(t *testing.T, peers, absent int, vsn []uint8, f func(*Config)) (*Memberlist, []*Memberlist) {
	t.Helper()
	n := newSimNet()
	m1 := simMember(t, n, 1, func(c *Config) {
		c.IndirectChecks = 3
		if f != nil {
			f(c)
		}
	})
	if err := m1.setAlive(nil); err != nil {
		t.Fatal(err)
	}
	var live []*Memberlist
	for i := range peers {
		p := simMember(t, n, 2+i, func(c *Config) {
			c.ProbeTimeout = 10 * time.Millisecond
			c.ProbeInterval = 200 * time.Millisecond
		})
		live = append(live, p)
		m1.aliveNode(&alive{Node: p.config.Name, Addr: simIP(2 + i), Port: 7946, Incarnation: 1, Vsn: vsn}, false)
	}
	for i := range absent {
		name := fmt.Sprintf("absent%d", i)
		m1.aliveNode(&alive{Node: name, Addr: simIP(100 + i), Port: 7946, Incarnation: 1, Vsn: vsn}, false)
	}
	return m1, live
}

// probeAbsent has m1 probe absent0 and returns how long the probe took.
func probeAbsent(t *testing.T, m1 *Memberlist) time.Duration {
	t.Helper()
	m1.nodeLock.RLock()
	n := m1.nodeMap["absent0"]
	m1.nodeLock.RUnlock()
	start := time.Now()
	m1.probeNode(n)
	synctest.Wait()
	if state := m1.getNodeState("absent0"); state != StateSuspect {
		t.Fatalf("expect node to be suspect, got %v", state)
	}
	return time.Since(start)
}

func TestMemberList_ProbeNode_Suspect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m1, peers := probeCluster(t, 2, 1, []uint8{1, 5, 2, 0, 0, 0}, func(c *Config) {
			c.ProbeTimeout = time.Millisecond
			c.ProbeInterval = 10 * time.Millisecond
		})
		equal(t, m1.config.ProbeInterval, probeAbsent(t, m1), "probe duration")

		// Both peers were asked for an indirect probe (IndirectChecks=3).
		for _, p := range peers {
			equal(t, uint32(1), atomic.LoadUint32(&p.sequenceNum), "%s indirect pings", p.config.Name)
		}
	})
}

// TestMemberList_ProbeNode_Suspect_Dogpile runs in a synctest bubble on a
// simulated network, so every suspicion timeout is checked exactly: the
// node is still suspect one nanosecond before the expected timeout and dead
// at it. Timeouts follow from SuspicionMult 5, ProbeInterval 100ms and
// SuspicionMaxTimeoutMult 2: min = 5 * max(1, log10(n)) * 100ms = 500ms,
// max = 1000ms, and with k = 3 expected confirmations
// timeout = max - log(c+1)/log(k+1) * (max - min), floored to the ms.
func TestMemberList_ProbeNode_Suspect_Dogpile(t *testing.T) {
	cases := []struct {
		name          string
		numPeers      int
		confirmations int
		expected      time.Duration
	}{
		{"n=2, k=3 (max timeout disabled)", 1, 0, 500 * time.Millisecond},
		{"n=3, k=3", 2, 0, 500 * time.Millisecond},
		{"n=4, k=3", 3, 0, 500 * time.Millisecond},
		{"n=5, k=3 (max timeout starts to take effect)", 4, 0, 1000 * time.Millisecond},
		{"n=6, k=3", 5, 0, 1000 * time.Millisecond},
		{"n=6, k=3 (confirmations start to lower timeout)", 5, 1, 750 * time.Millisecond},
		{"n=6, k=3 (1000 - log(3)/log(4) * 500 = 603.76ms)", 5, 2, 603 * time.Millisecond},
		{"n=6, k=3 (timeout driven to nominal value)", 5, 3, 500 * time.Millisecond},
		{"n=6, k=3", 5, 4, 500 * time.Millisecond},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n := newSimNet()
				m := simMember(t, n, 1, func(c *Config) {
					c.ProbeTimeout = time.Millisecond
					c.ProbeInterval = 100 * time.Millisecond
					c.SuspicionMult = 5
					c.SuspicionMaxTimeoutMult = 2
				})
				if err := m.setAlive(nil); err != nil {
					t.Fatal(err)
				}

				// All but one peer are real, alive instances.
				vsn := m.config.BuildVsnArray()
				for j := range c.numPeers - 1 {
					peer := simMember(t, n, 2+j, nil)
					a := alive{Node: peer.config.Name, Addr: simIP(2 + j), Port: 7946, Incarnation: 1, Vsn: vsn}
					m.aliveNode(&a, false)
				}

				// The last peer is not on the network, so it never answers,
				// but the memberlist is told it is alive.
				bad := alive{Node: "bad", Addr: simIP(200), Port: 7946, Incarnation: 1, Vsn: vsn}
				m.aliveNode(&bad, false)

				// Force a probe, which should start us into the suspect state.
				m.probeNodeByAddr("bad")
				if m.getNodeState("bad") != StateSuspect {
					t.Fatalf("expected node to be suspect")
				}

				// Add the requested number of confirmations.
				for j := range c.confirmations {
					s := suspect{Node: "bad", Incarnation: 1, From: fmt.Sprintf("peer%d", j)}
					m.suspectNode(&s)
				}

				time.Sleep(c.expected - time.Nanosecond)
				synctest.Wait()
				if m.getNodeState("bad") != StateSuspect {
					t.Fatalf("declared dead before %v", c.expected)
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if m.getNodeState("bad") != StateDead {
					t.Fatalf("still suspect at %v", c.expected)
				}
			})
		})
	}
}

/*
func TestMemberList_ProbeNode_FallbackTCP(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	addr3 := getBindAddr()
	addr4 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)
	ip3 := []byte(addr3)
	ip4 := []byte(addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a2, false)
	a3 := alive{Node: addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a3, false)

	// Make sure m4 is configured with the same protocol version as m1 so
	// the TCP fallback behavior is enabled.
	a4 := alive{
		Node:        addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersionMax,
			m1.config.ProtocolVersion,
			m1.config.DelegateProtocolMin,
			m1.config.DelegateProtocolMax,
			m1.config.DelegateProtocolVersion,
		},
	}
	m1.aliveNode(&a4, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.udpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.udpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Have node m1 probe m4.
	n := m1.nodeMap[addr4.String()]
	startProbe := time.Now()
	m1.probeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Should be marked alive because of the TCP fallback ping.
	if n.State != stateAlive {
		t.Fatalf("expect node to be alive")
	}

	// Make sure TCP activity completed in a timely manner.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.sequenceNum != 1 && m3.sequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.sequenceNum, m3.sequenceNum)
	}

	// Now shutdown all inbound TCP traffic to make sure the TCP fallback
	// path properly fails when the node is really unreachable.
	if err = m4.tcpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	tcpAddr := &net.TCPAddr{IP: ip4, Port: 9999}
	if m4.tcpListener, err = net.ListenTCP("tcp", tcpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Probe again, this time there should be no contact.
	startProbe = time.Now()
	m1.probeNode(n)
	probeTime = time.Now().Sub(startProbe)

	// Node should be reported suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.sequenceNum != 2 && m3.sequenceNum != 2 {
		t.Fatalf("bad seqnos %v, %v", m2.sequenceNum, m3.sequenceNum)
	}
}

func TestMemberList_ProbeNode_FallbackTCP_Disabled(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	addr3 := getBindAddr()
	addr4 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)
	ip3 := []byte(addr3)
	ip4 := []byte(addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a2, false)
	a3 := alive{Node: addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a3, false)

	// Make sure m4 is configured with the same protocol version as m1 so
	// the TCP fallback behavior is enabled.
	a4 := alive{
		Node:        addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersionMax,
			m1.config.ProtocolVersion,
			m1.config.DelegateProtocolMin,
			m1.config.DelegateProtocolMax,
			m1.config.DelegateProtocolVersion,
		},
	}
	m1.aliveNode(&a4, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.udpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.udpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Disable the TCP pings using the config mechanism.
	m1.config.DisableTcpPings = true

	// Have node m1 probe m4.
	n := m1.nodeMap[addr4.String()]
	startProbe := time.Now()
	m1.probeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.sequenceNum != 1 && m3.sequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.sequenceNum, m3.sequenceNum)
	}
}

func TestMemberList_ProbeNode_FallbackTCP_OldProtocol(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	addr3 := getBindAddr()
	addr4 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)
	ip3 := []byte(addr3)
	ip4 := []byte(addr4)

	var probeTimeMax time.Duration
	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
		probeTimeMax = c.ProbeInterval + 20*time.Millisecond
	})
	defer m1.Shutdown()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m2.Shutdown()

	m3 := HostMemberlist(addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m3.Shutdown()

	m4 := HostMemberlist(addr4.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer m4.Shutdown()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a2, false)
	a3 := alive{Node: addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a3, false)

	// Set up m4 so that it doesn't understand a version of the protocol
	// that supports TCP pings.
	a4 := alive{
		Node:        addr4.String(),
		Addr:        ip4,
		Port:        uint16(bindPort),
		Incarnation: 1,
		Vsn: []uint8{
			ProtocolVersionMin,
			ProtocolVersion2Compatible,
			ProtocolVersion2Compatible,
			m1.config.DelegateProtocolMin,
			m1.config.DelegateProtocolMax,
			m1.config.DelegateProtocolVersion,
		},
	}
	m1.aliveNode(&a4, false)

	// Isolate m4 from UDP traffic by re-opening its listener on the wrong
	// port. This should force the TCP fallback path to be used.
	var err error
	if err = m4.udpListener.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}
	udpAddr := &net.UDPAddr{IP: ip4, Port: 9999}
	if m4.udpListener, err = net.ListenUDP("udp", udpAddr); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Have node m1 probe m4.
	n := m1.nodeMap[addr4.String()]
	startProbe := time.Now()
	m1.probeNode(n)
	probeTime := time.Now().Sub(startProbe)

	// Node should be reported suspect.
	if n.State != stateSuspect {
		t.Fatalf("expect node to be suspect")
	}

	// Make sure TCP activity didn't cause us to wait too long before
	// timing out.
	if probeTime > probeTimeMax {
		t.Fatalf("took to long to probe, %9.6f", probeTime.Seconds())
	}

	// Confirm at least one of the peers attempted an indirect probe.
	time.Sleep(probeTimeMax)
	if m2.sequenceNum != 1 && m3.sequenceNum != 1 {
		t.Fatalf("bad seqnos %v, %v", m2.sequenceNum, m3.sequenceNum)
	}
}
*/

func TestMemberList_ProbeNode_Awareness_Degraded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		vsn := []uint8{ProtocolVersionMin, ProtocolVersionMax, ProtocolVersionMin, 1, 1, 1}
		m1, peers := probeCluster(t, 2, 1, vsn, func(c *Config) {
			c.ProbeTimeout = 10 * time.Millisecond
			c.ProbeInterval = 200 * time.Millisecond
		})

		// Start the health in a degraded state: the probe interval (and
		// so the probe) is scaled by score+1.
		m1.awareness.ApplyDelta(1)
		equal(t, 1, m1.GetHealthScore())

		equal(t, 2*m1.config.ProbeInterval, probeAbsent(t, m1), "degraded probe duration")
		for _, p := range peers {
			equal(t, uint32(1), atomic.LoadUint32(&p.sequenceNum), "%s indirect pings", p.config.Name)
		}

		// Every expected nack arrived, so the score is unchanged: the
		// failure was the target's, not ours.
		equal(t, 1, m1.GetHealthScore())
	})
}

func TestMemberList_ProbeNode_Wrong_VSN(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	addr3 := getBindAddr()
	addr4 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)
	ip3 := []byte(addr3)
	ip4 := []byte(addr4)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	m3 := HostMemberlist(addr3.String(), t, func(c *Config) {
		c.BindPort = bindPort
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer func() {
		if err := m3.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a2, false)
	a3 := alive{Node: addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.config.BuildVsnArray()}
	m1.aliveNode(&a3, false)

	vsn4 := []uint8{
		0, 0, 0,
		0, 0, 0,
	}
	// Node 4 never gets started.
	a4 := alive{Node: addr4.String(), Addr: ip4, Port: uint16(bindPort), Incarnation: 1, Vsn: vsn4}
	m1.aliveNode(&a4, false)

	// Start the health in a degraded state.
	m1.awareness.ApplyDelta(1)
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m4.
	n, ok := m1.nodeMap[addr4.String()]
	if ok || n != nil {
		t.Fatalf("expect node a4 to be not taken into account, because of its wrong version")
	}
}

func TestMemberList_ProbeNode_Awareness_Improved(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = 10 * time.Millisecond
		c.ProbeInterval = 200 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a2, false)

	// Start the health in a degraded state.
	m1.awareness.ApplyDelta(1)
	if score := m1.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}

	// Have node m1 probe m2.
	n := m1.nodeMap[addr2.String()]
	m1.probeNode(n)

	// Node should be reported alive.
	if n.State != StateAlive {
		t.Fatalf("expect node to be suspect")
	}

	// Our score should have improved since we did a good probe.
	if score := m1.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_ProbeNode_Awareness_MissedNack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// One live peer and two absent nodes: node1 asks the live peer and
		// absent1 for indirect probes and expects two nacks, but only the
		// live peer can answer.
		m1, _ := probeCluster(t, 1, 2, []uint8{1, 5, 2, 0, 0, 0}, func(c *Config) {
			c.ProbeTimeout = 10 * time.Millisecond
			c.ProbeInterval = 200 * time.Millisecond
		})
		equal(t, 0, m1.GetHealthScore())

		equal(t, m1.config.ProbeInterval, probeAbsent(t, m1), "probe duration")

		// Dinged once for the missed nack.
		equal(t, 1, m1.GetHealthScore())
	})
}

func TestMemberList_ProbeNode_Awareness_OldProtocol(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Legacy peers (no version vector) neither send nacks nor accept
		// TCP pings, so any failed probe counts against our health.
		m1, peers := probeCluster(t, 2, 1, nil, func(c *Config) {
			c.ProbeTimeout = 10 * time.Millisecond
			c.ProbeInterval = 200 * time.Millisecond
		})
		equal(t, 0, m1.GetHealthScore())

		equal(t, m1.config.ProbeInterval, probeAbsent(t, m1), "probe duration")
		for _, p := range peers {
			equal(t, uint32(1), atomic.LoadUint32(&p.sequenceNum), "%s indirect pings", p.config.Name)
		}
		equal(t, 1, m1.GetHealthScore())
	})
}

func TestMemberList_ProbeNode_Buddy(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}

	m1.aliveNode(&a1, true)
	m1.aliveNode(&a2, false)
	m2.aliveNode(&a2, true)

	// Force the state to suspect so we piggyback a suspect message with the ping.
	// We should see this get refuted later, and the ping will succeed.
	n := m1.nodeMap[addr2.String()]
	n.State = StateSuspect
	m1.probeNode(n)

	// Make sure a ping was sent.
	if m1.sequenceNum != 1 {
		t.Fatalf("bad seqno %v", m1.sequenceNum)
	}

	// Check a broadcast is queued.
	if num := m2.broadcasts.NumQueued(); num != 1 {
		t.Fatalf("expected only one queued message: %d", num)
	}

	// Should be alive msg.
	if messageType(m2.broadcasts.orderedView()[0].b.Message()[0]) != aliveMsg {
		t.Fatalf("expected queued alive msg")
	}
}

func TestMemberList_ProbeNode(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.ProbeTimeout = time.Millisecond
		c.ProbeInterval = 10 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1}
	m1.aliveNode(&a2, false)

	n := m1.nodeMap[addr2.String()]
	m1.probeNode(n)

	// Should be marked alive
	if n.State != StateAlive {
		t.Fatalf("Expect node to be alive")
	}

	// Should increment seqno
	if m1.sequenceNum != 1 {
		t.Fatalf("bad seqno %v", m1.sequenceNum)
	}
}

// TestMemberList_Ping runs on a simulated network with a fixed one-way
// latency, so the measured round trip is exact.
func TestMemberList_Ping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newSimNet()
		n.latency = 7 * time.Millisecond
		m1 := simMember(t, n, 1, func(c *Config) {
			c.ProbeTimeout = 1 * time.Second
			c.ProbeInterval = 10 * time.Second
		})
		simMember(t, n, 2, nil)

		// Do a legit ping.
		addr := simAddr{net.JoinHostPort(simIP(2).String(), "7946")}
		rtt, err := m1.Ping("node2", addr)
		noErr(t, err)
		equal(t, 2*n.latency, rtt)

		// This ping has a bad node name so should time out, after exactly
		// ProbeTimeout.
		start := time.Now()
		_, err = m1.Ping("bad", addr)
		if _, ok := err.(NoPingResponseError); !ok {
			t.Fatalf("bad: %v", err)
		}
		equal(t, m1.config.ProbeTimeout, time.Since(start))
	})
}

func TestMemberList_ResetNodes(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.GossipToTheDeadTime = 100 * time.Millisecond
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: "test1", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a1, false)
	a2 := alive{Node: "test2", Addr: []byte{127, 0, 0, 2}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a2, false)
	a3 := alive{Node: "test3", Addr: []byte{127, 0, 0, 3}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a3, false)
	d := dead{Node: "test2", Incarnation: 1}
	m.deadNode(&d)

	m.resetNodes()
	if len(m.nodes) != 3 {
		t.Fatalf("Bad length")
	}
	if _, ok := m.nodeMap["test2"]; !ok {
		t.Fatalf("test2 should not be unmapped")
	}

	time.Sleep(200 * time.Millisecond)
	m.resetNodes()
	if len(m.nodes) != 2 {
		t.Fatalf("Bad length")
	}
	if _, ok := m.nodeMap["test2"]; ok {
		t.Fatalf("test2 should be unmapped")
	}
}

func TestMemberList_NextSeq(t *testing.T) {
	m := &Memberlist{}
	if m.nextSeqNo() != 1 {
		t.Fatalf("bad sequence no")
	}
	if m.nextSeqNo() != 2 {
		t.Fatalf("bad sequence no")
	}
}

func ackHandlerExists(t *testing.T, m *Memberlist) bool {
	t.Helper()

	m.ackLock.Lock()
	_, ok := m.ackHandlers[0]
	m.ackLock.Unlock()

	return ok
}

func TestMemberList_setProbeChannels(t *testing.T) {
	m := &Memberlist{ackHandlers: make(map[uint32]*ackHandler)}

	ch := make(chan ackMessage, 1)
	m.setProbeChannels(0, ch, nil, 10*time.Millisecond)

	isTrue(t, ackHandlerExists(t, m), "missing handler")

	time.Sleep(20 * time.Millisecond)

	isFalse(t, ackHandlerExists(t, m), "non-reaped handler")
}

func TestMemberList_setAckHandler(t *testing.T) {
	m := &Memberlist{ackHandlers: make(map[uint32]*ackHandler)}

	f := func([]byte, time.Time) {}
	m.setAckHandler(0, f, 10*time.Millisecond)

	isTrue(t, ackHandlerExists(t, m), "missing handler")

	time.Sleep(20 * time.Millisecond)

	isFalse(t, ackHandlerExists(t, m), "non-reaped handler")
}

func TestMemberList_invokeAckHandler(t *testing.T) {
	m := &Memberlist{ackHandlers: make(map[uint32]*ackHandler)}

	// Does nothing
	m.invokeAckHandler(ackResp{}, time.Now())

	var b bool
	f := func(payload []byte, timestamp time.Time) { b = true }
	m.setAckHandler(0, f, 10*time.Millisecond)

	// Should set b
	m.invokeAckHandler(ackResp{SeqNo: 0}, time.Now())
	if !b {
		t.Fatalf("b not set")
	}

	isFalse(t, ackHandlerExists(t, m), "non-reaped handler")
}

func TestMemberList_invokeAckHandler_Channel_Ack(t *testing.T) {
	m := &Memberlist{ackHandlers: make(map[uint32]*ackHandler)}

	ack := ackResp{SeqNo: 0, Payload: []byte{0, 0, 0}}

	// Does nothing
	m.invokeAckHandler(ack, time.Now())

	ackCh := make(chan ackMessage, 1)
	nackCh := make(chan struct{}, 1)
	m.setProbeChannels(0, ackCh, nackCh, 10*time.Millisecond)

	// Should send message
	m.invokeAckHandler(ack, time.Now())

	select {
	case v := <-ackCh:
		if v.Complete != true {
			t.Fatalf("Bad value")
		}
		if !bytes.Equal(v.Payload, ack.Payload) {
			t.Fatalf("wrong payload. expected: %v; actual: %v", ack.Payload, v.Payload)
		}

	case <-nackCh:
		t.Fatalf("should not get a nack")

	default:
		t.Fatalf("message not sent")
	}

	isFalse(t, ackHandlerExists(t, m), "non-reaped handler")
}

func TestMemberList_invokeAckHandler_Channel_Nack(t *testing.T) {
	m := &Memberlist{ackHandlers: make(map[uint32]*ackHandler)}

	nack := nackResp{SeqNo: 0}

	// Does nothing.
	m.invokeNackHandler(nack)

	ackCh := make(chan ackMessage, 1)
	nackCh := make(chan struct{}, 1)
	m.setProbeChannels(0, ackCh, nackCh, 10*time.Millisecond)

	// Should send message.
	m.invokeNackHandler(nack)

	select {
	case <-ackCh:
		t.Fatalf("should not get an ack")

	case <-nackCh:
		// Good.

	default:
		t.Fatalf("message not sent")
	}

	// Getting a nack doesn't reap the handler so that we can still forward
	// an ack up to the reap time, if we get one.
	isTrue(t, ackHandlerExists(t, m), "handler should not be reaped")

	ack := ackResp{SeqNo: 0, Payload: []byte{0, 0, 0}}
	m.invokeAckHandler(ack, time.Now())

	select {
	case v := <-ackCh:
		if v.Complete != true {
			t.Fatalf("Bad value")
		}
		if !bytes.Equal(v.Payload, ack.Payload) {
			t.Fatalf("wrong payload. expected: %v; actual: %v", ack.Payload, v.Payload)
		}

	case <-nackCh:
		t.Fatalf("should not get a nack")

	default:
		t.Fatalf("message not sent")
	}

	isFalse(t, ackHandlerExists(t, m), "non-reaped handler")
}

func TestMemberList_AliveNode_NewNode(t *testing.T) {
	ch := make(chan NodeEvent, 1)
	m := GetMemberlist(t, func(c *Config) {
		c.Events = &ChannelEventDelegate{ch}
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	if len(m.nodes) != 1 {
		t.Fatalf("should add node")
	}

	state, ok := m.nodeMap["test"]
	if !ok {
		t.Fatalf("should map node")
	}

	if state.Incarnation != 1 {
		t.Fatalf("bad incarnation")
	}
	if state.State != StateAlive {
		t.Fatalf("bad state")
	}
	if time.Since(state.StateChange) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check for a join message
	if e := recvNodeEvent(t, ch); e.Node.Name != "test" {
		t.Fatalf("bad node name")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected queued message")
	}
}

func TestMemberList_AliveNode_SuspectNode(t *testing.T) {
	ch := make(chan NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &ChannelEventDelegate{ch},
	}
	m := GetMemberlist(t, func(c *Config) {
		c.Events = ted
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	// Listen only after the first join was fully delivered (async since #6)
	waitEventQuiesce(t, m)
	ted.Toggle(true)

	// Make suspect
	state := m.nodeMap["test"]
	state.State = StateSuspect
	state.StateChange = state.StateChange.Add(-time.Hour)

	// Old incarnation number, should not change
	m.aliveNode(&a, false)
	if state.State != StateSuspect {
		t.Fatalf("update with old incarnation!")
	}

	// Should reset to alive now
	a.Incarnation = 2
	m.aliveNode(&a, false)
	if state.State != StateAlive {
		t.Fatalf("no update with new incarnation!")
	}

	if time.Since(state.StateChange) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check for a no join message (settle deliveries first)
	waitEventQuiesce(t, m)
	select {
	case <-ch:
		t.Fatalf("got bad join message")
	default:
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected queued message")
	}
}

func TestMemberList_AliveNode_Idempotent(t *testing.T) {
	ch := make(chan NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &ChannelEventDelegate{ch},
	}
	m := GetMemberlist(t, func(c *Config) {
		c.Events = ted
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	// Listen only after the first join was fully delivered (async since #6)
	waitEventQuiesce(t, m)
	ted.Toggle(true)

	// Make suspect
	state := m.nodeMap["test"]
	stateTime := state.StateChange

	// Should reset to alive now
	a.Incarnation = 2
	m.aliveNode(&a, false)
	if state.State != StateAlive {
		t.Fatalf("non idempotent")
	}

	if stateTime != state.StateChange {
		t.Fatalf("should not change state")
	}

	// Check for a no join message (settle deliveries first)
	waitEventQuiesce(t, m)
	select {
	case <-ch:
		t.Fatalf("got bad join message")
	default:
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}
}

// recvNodeEvent receives one event with a bound — delivery is asynchronous
// since #6.
func recvNodeEvent(t *testing.T, ch <-chan NodeEvent) NodeEvent {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for node event")
		return NodeEvent{}
	}
}

// waitEventQuiesce blocks until the event dispatcher has delivered every
// committed event, so a subsequent negative assertion (or a delegate
// toggle) observes a settled state.
func waitEventQuiesce(t *testing.T, m *Memberlist) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.eventMu.Lock()
		pending := m.eventPending
		m.eventMu.Unlock()
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("event queue never quiesced: %d pending", pending)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type toggledEventDelegate struct {
	mu      sync.Mutex
	real    EventDelegate
	enabled bool
}

func (d *toggledEventDelegate) Toggle(enabled bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = enabled
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (d *toggledEventDelegate) NotifyJoin(n *Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyJoin(n)
	}
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (d *toggledEventDelegate) NotifyLeave(n *Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyLeave(n)
	}
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (d *toggledEventDelegate) NotifyUpdate(n *Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.enabled {
		d.real.NotifyUpdate(n)
	}
}

// Serf Bug: GH-58, Meta data does not update
func TestMemberList_AliveNode_ChangeMeta(t *testing.T) {
	ch := make(chan NodeEvent, 1)
	ted := &toggledEventDelegate{
		real: &ChannelEventDelegate{ch},
	}

	m := GetMemberlist(t, func(c *Config) {
		c.Events = ted
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{
		Node:        "test",
		Addr:        []byte{127, 0, 0, 1},
		Meta:        []byte("val1"),
		Incarnation: 1,
		Vsn:         m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	// Listen only after the first join was fully delivered (async since #6)
	waitEventQuiesce(t, m)
	ted.Toggle(true)

	// Make suspect
	state := m.nodeMap["test"]

	// Should reset to alive now
	a.Incarnation = 2
	a.Meta = []byte("val2")
	m.aliveNode(&a, false)

	// Check updates
	if !bytes.Equal(state.Meta, a.Meta) {
		t.Fatalf("meta did not update")
	}

	// Check for a NotifyUpdate
	e := recvNodeEvent(t, ch)
	if e.Event != NodeUpdate {
		t.Fatalf("bad event: %v", e)
	}
	if !reflect.DeepEqual(*e.Node, state.Node) {
		t.Fatalf("expected %v, got %v", *e.Node, state.Node)
	}
	if !bytes.Equal(e.Node.Meta, a.Meta) {
		t.Fatalf("meta did not update")
	}

}

func TestMemberList_AliveNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: m.config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, true)

	// Clear queue
	m.broadcasts.Reset()

	// Conflicting alive
	s := alive{
		Node:        m.config.Name,
		Addr:        []byte{127, 0, 0, 1},
		Incarnation: 2,
		Meta:        []byte("foo"),
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&s, false)

	state := m.nodeMap[m.config.Name]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}
	if state.Meta != nil {
		t.Fatalf("meta should still be nil")
	}

	// Check a broad cast is queued
	if num := m.broadcasts.NumQueued(); num != 1 {
		t.Fatalf("expected only one queued message: %d",
			num)
	}

	// Should be alive mesg
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != aliveMsg {
		t.Fatalf("expected queued alive msg")
	}
}

func TestMemberList_AliveNode_Conflict(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.DeadNodeReclaimTime = 10 * time.Millisecond
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	nodeName := "test"
	a := alive{Node: nodeName, Addr: []byte{127, 0, 0, 1}, Port: 8000, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, true)

	// Clear queue
	m.broadcasts.Reset()

	// Conflicting alive
	s := alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 2,
		Meta:        []byte("foo"),
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&s, false)

	state := m.nodeMap[nodeName]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}
	if state.Meta != nil {
		t.Fatalf("meta should still be nil")
	}
	if bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("address should not be updated")
	}
	if state.Port == 9000 {
		t.Fatalf("port should not be updated")
	}

	// Check a broad cast is queued
	if num := m.broadcasts.NumQueued(); num != 0 {
		t.Fatalf("expected 0 queued messages: %d", num)
	}

	// Change the node to dead
	d := dead{Node: nodeName, Incarnation: 2}
	m.deadNode(&d)
	m.broadcasts.Reset()

	state = m.nodeMap[nodeName]
	if state.State != StateDead {
		t.Fatalf("should be dead")
	}

	time.Sleep(m.config.DeadNodeReclaimTime)

	// New alive node
	s2 := alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 3,
		Meta:        []byte("foo"),
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&s2, false)

	state = m.nodeMap[nodeName]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}
	if !bytes.Equal(state.Meta, []byte("foo")) {
		t.Fatalf("meta should be updated")
	}
	if !bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("address should be updated")
	}
	if state.Port != 9000 {
		t.Fatalf("port should be updated")
	}
}

func TestMemberList_SuspectNode_NoNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	s := suspect{Node: "test", Incarnation: 1}
	m.suspectNode(&s)
	if len(m.nodes) != 0 {
		t.Fatalf("don't expect nodes")
	}
}

func TestMemberList_SuspectNode(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.ProbeInterval = time.Millisecond
		c.SuspicionMult = 1
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	m.changeNode("test", func(state *nodeState) {
		state.StateChange = state.StateChange.Add(-time.Hour)
	})

	s := suspect{Node: "test", Incarnation: 1}
	m.suspectNode(&s)

	if m.getNodeState("test") != StateSuspect {
		t.Fatalf("Bad state")
	}

	change := m.getNodeStateChange("test")
	if time.Since(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a suspect message
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != suspectMsg {
		t.Fatalf("expected queued suspect msg")
	}

	// Wait for the timeout
	time.Sleep(10 * time.Millisecond)

	if m.getNodeState("test") != StateDead {
		t.Fatalf("Bad state")
	}

	newChange := m.getNodeStateChange("test")
	if time.Since(newChange) > time.Second {
		t.Fatalf("bad change delta")
	}
	if !newChange.After(change) {
		t.Fatalf("should increment time")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a suspect message
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != deadMsg {
		t.Fatalf("expected queued dead msg")
	}
}

func TestMemberList_SuspectNode_DoubleSuspect(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	state := m.nodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	s := suspect{Node: "test", Incarnation: 1}
	m.suspectNode(&s)

	if state.State != StateSuspect {
		t.Fatalf("Bad state")
	}

	change := state.StateChange
	if time.Since(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	// clear the broadcast queue
	m.broadcasts.Reset()

	// Suspect again
	m.suspectNode(&s)

	if state.StateChange != change {
		t.Fatalf("unexpected state change")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}

}

func TestMemberList_SuspectNode_OldSuspect(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	state := m.nodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	// Clear queue
	m.broadcasts.Reset()

	s := suspect{Node: "test", Incarnation: 1}
	m.suspectNode(&s)

	if state.State != StateAlive {
		t.Fatalf("Bad state")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}
}

func TestMemberList_SuspectNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: m.config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, true)

	// Clear queue
	m.broadcasts.Reset()

	// Make sure health is in a good state
	if score := m.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	s := suspect{Node: m.config.Name, Incarnation: 1}
	m.suspectNode(&s)

	state := m.nodeMap[m.config.Name]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Should be alive mesg
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != aliveMsg {
		t.Fatalf("expected queued alive msg")
	}

	// Health should have been dinged
	if score := m.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_DeadNode_NoNode(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	d := dead{Node: "test", Incarnation: 1}
	m.deadNode(&d)
	if len(m.nodes) != 0 {
		t.Fatalf("don't expect nodes")
	}
}

func TestMemberList_DeadNodeLeft(t *testing.T) {
	ch := make(chan NodeEvent, 1)

	m := GetMemberlist(t, func(c *Config) {
		c.Events = &ChannelEventDelegate{ch}
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	nodeName := "node1"
	s1 := alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 1},
		Port:        8000,
		Incarnation: 1,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&s1, false)

	// Read the join event
	<-ch

	d := dead{Node: nodeName, From: nodeName, Incarnation: 1}
	m.deadNode(&d)

	// Read the dead event
	<-ch

	state := m.nodeMap[nodeName]
	if state.State != StateLeft {
		t.Fatalf("Bad state")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a dead message
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != deadMsg {
		t.Fatalf("expected queued dead msg")
	}

	// Clear queue
	// m.broadcasts.Reset()

	// New alive node
	s2 := alive{
		Node:        nodeName,
		Addr:        []byte{127, 0, 0, 2},
		Port:        9000,
		Incarnation: 3,
		Meta:        []byte("foo"),
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&s2, false)

	// Read the join event
	<-ch

	state = m.nodeMap[nodeName]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}
	if !bytes.Equal(state.Meta, []byte("foo")) {
		t.Fatalf("meta should be updated")
	}
	if !bytes.Equal(state.Addr, []byte{127, 0, 0, 2}) {
		t.Fatalf("address should be updated")
	}
	if state.Port != 9000 {
		t.Fatalf("port should be updated")
	}
}

func TestMemberList_DeadNode(t *testing.T) {
	ch := make(chan NodeEvent, 1)

	m := GetMemberlist(t, func(c *Config) {
		c.Events = &ChannelEventDelegate{ch}
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	// Read the join event
	<-ch

	state := m.nodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := dead{Node: "test", Incarnation: 1}
	m.deadNode(&d)

	if state.State != StateDead {
		t.Fatalf("Bad state")
	}

	change := state.StateChange
	if time.Since(change) > time.Second {
		t.Fatalf("bad change delta")
	}

	if leave := recvNodeEvent(t, ch); leave.Event != NodeLeave || leave.Node.Name != "test" {
		t.Fatalf("bad node name")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Check its a dead message
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != deadMsg {
		t.Fatalf("expected queued dead msg")
	}
}

func TestMemberList_DeadNode_Double(t *testing.T) {
	ch := make(chan NodeEvent, 1)
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	state := m.nodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := dead{Node: "test", Incarnation: 1}
	m.deadNode(&d)

	// Clear queue
	m.broadcasts.Reset()

	// Notify after the first dead
	m.config.Events = &ChannelEventDelegate{ch}

	// Should do nothing
	d.Incarnation = 2
	m.deadNode(&d)

	waitEventQuiesce(t, m)
	select {
	case <-ch:
		t.Fatalf("should not get leave")
	default:
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 0 {
		t.Fatalf("expected only one queued message")
	}
}

func TestMemberList_DeadNode_OldDead(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	state := m.nodeMap["test"]
	state.StateChange = state.StateChange.Add(-time.Hour)

	d := dead{Node: "test", Incarnation: 1}
	m.deadNode(&d)

	if state.State != StateAlive {
		t.Fatalf("Bad state")
	}
}

func TestMemberList_DeadNode_AliveReplay(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: "test", Addr: []byte{127, 0, 0, 1}, Incarnation: 10, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, false)

	d := dead{Node: "test", Incarnation: 10}
	m.deadNode(&d)

	// Replay alive at same incarnation
	m.aliveNode(&a, false)

	// Should remain dead
	state, ok := m.nodeMap["test"]
	if ok && state.State != StateDead {
		t.Fatalf("Bad state")
	}
}

func TestMemberList_DeadNode_Refute(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a := alive{Node: m.config.Name, Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a, true)

	// Clear queue
	m.broadcasts.Reset()

	// Make sure health is in a good state
	if score := m.GetHealthScore(); score != 0 {
		t.Fatalf("bad: %d", score)
	}

	d := dead{Node: m.config.Name, Incarnation: 1}
	m.deadNode(&d)

	state := m.nodeMap[m.config.Name]
	if state.State != StateAlive {
		t.Fatalf("should still be alive")
	}

	// Check a broad cast is queued
	if m.broadcasts.NumQueued() != 1 {
		t.Fatalf("expected only one queued message")
	}

	// Should be alive mesg
	if messageType(m.broadcasts.orderedView()[0].b.Message()[0]) != aliveMsg {
		t.Fatalf("expected queued alive msg")
	}

	// We should have been dinged
	if score := m.GetHealthScore(); score != 1 {
		t.Fatalf("bad: %d", score)
	}
}

func TestMemberList_MergeState(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: "test1", Addr: []byte{127, 0, 0, 1}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a1, false)
	a2 := alive{Node: "test2", Addr: []byte{127, 0, 0, 2}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a2, false)
	a3 := alive{Node: "test3", Addr: []byte{127, 0, 0, 3}, Incarnation: 1, Vsn: m.config.BuildVsnArray()}
	m.aliveNode(&a3, false)

	s := suspect{Node: "test1", Incarnation: 1}
	m.suspectNode(&s)

	remote := []pushNodeState{
		{
			Name:        "test1",
			Addr:        []byte{127, 0, 0, 1},
			Incarnation: 2,
			State:       int(StateAlive),
		},
		{
			Name:        "test2",
			Addr:        []byte{127, 0, 0, 2},
			Incarnation: 1,
			State:       int(StateSuspect),
		},
		{
			Name:        "test3",
			Addr:        []byte{127, 0, 0, 3},
			Incarnation: 1,
			State:       int(StateDead),
		},
		{
			Name:        "test4",
			Addr:        []byte{127, 0, 0, 4},
			Incarnation: 2,
			State:       int(StateAlive),
		},
	}

	// Listen for changes
	eventCh := make(chan NodeEvent, 1)
	m.config.Events = &ChannelEventDelegate{eventCh}

	// Merge remote state
	m.mergeState(remote)

	// Check the states
	state := m.nodeMap["test1"]
	if state.State != StateAlive || state.Incarnation != 2 {
		t.Fatalf("Bad state %v", state)
	}

	state = m.nodeMap["test2"]
	if state.State != StateSuspect || state.Incarnation != 1 {
		t.Fatalf("Bad state %v", state)
	}

	state = m.nodeMap["test3"]
	if state.State != StateSuspect {
		t.Fatalf("Bad state %v", state)
	}

	state = m.nodeMap["test4"]
	if state.State != StateAlive || state.Incarnation != 2 {
		t.Fatalf("Bad state %v", state)
	}

	// Check the channels
	if e := recvNodeEvent(t, eventCh); e.Event != NodeJoin || e.Node.Name != "test4" {
		t.Fatalf("bad node %v", e)
	}

	waitEventQuiesce(t, m)
	select {
	case e := <-eventCh:
		t.Fatalf("Unexpect event: %v", e)
	default:
	}
}

func TestMemberlist_Gossip(t *testing.T) {
	ch := make(chan NodeEvent, 3)

	addr1 := getBindAddr()
	addr2 := getBindAddr()
	addr3 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)
	ip3 := []byte(addr3)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		// Set the gossip interval fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
		c.GossipInterval = 10 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
		c.Events = &ChannelEventDelegate{ch}
		// Set the gossip interval fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
		c.GossipInterval = 10 * time.Millisecond
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	m3 := HostMemberlist(addr2.String(), t, func(c *Config) {
	})
	defer func() {
		if err := m3.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a2, false)
	a3 := alive{Node: addr3.String(), Addr: ip3, Port: uint16(bindPort), Incarnation: 1, Vsn: m3.config.BuildVsnArray()}
	m1.aliveNode(&a3, false)

	// Gossip should send all this to m2. Retry a few times because it's UDP and
	// timing and stuff makes this flaky without.
	retry(t, 15, 250*time.Millisecond, func(failf func(string, ...any)) {
		m1.gossip()

		time.Sleep(3 * time.Millisecond)

		if len(ch) < 3 {
			failf("expected 3 messages from gossip but only got %d", len(ch))
		}
	})
}

func retry(t *testing.T, n int, w time.Duration, fn func(func(string, ...any))) {
	t.Helper()
	for try := 1; try <= n; try++ {
		failed := false
		failFormat := ""
		failArgs := []any{}
		failf := func(format string, args ...any) {
			failed = true
			failFormat = format
			failArgs = args
		}

		fn(failf)

		if !failed {
			return
		}
		if try == n {
			t.Fatalf(failFormat, failArgs...)
		}
		time.Sleep(w)
	}
}

func TestMemberlist_GossipToDead(t *testing.T) {
	ch := make(chan NodeEvent, 2)

	addr1 := getBindAddr()
	addr2 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.GossipInterval = time.Millisecond
		c.GossipToTheDeadTime = 100 * time.Millisecond
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
		c.Events = &ChannelEventDelegate{ch}
	})

	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a2, false)

	// Shouldn't send anything to m2 here, node has been dead for 2x the GossipToTheDeadTime
	m1.nodeMap[addr2.String()].State = StateDead
	m1.nodeMap[addr2.String()].StateChange = time.Now().Add(-200 * time.Millisecond)
	m1.gossip()

	select {
	case <-ch:
		t.Fatalf("shouldn't get gossip")
	case <-time.After(50 * time.Millisecond):
	}

	// Should gossip to m2 because its state has changed within GossipToTheDeadTime
	m1.nodeMap[addr2.String()].StateChange = time.Now().Add(-20 * time.Millisecond)

	retry(t, 5, 10*time.Millisecond, func(failf func(string, ...any)) {
		m1.gossip()

		time.Sleep(3 * time.Millisecond)

		if len(ch) < 2 {
			failf("expected 2 messages from gossip")
		}
	})
}

func TestMemberlist_FailedRemote(t *testing.T) {
	type test struct {
		name     string
		err      error
		expected bool
	}
	tests := []test{
		{"nil error", nil, false},
		{"normal error", fmt.Errorf(""), false},
		{"net.OpError for file", &net.OpError{Net: "file"}, false},
		{"net.OpError for udp", &net.OpError{Net: "udp"}, false},
		{"net.OpError for udp4", &net.OpError{Net: "udp4"}, false},
		{"net.OpError for udp6", &net.OpError{Net: "udp6"}, false},
		{"net.OpError for tcp", &net.OpError{Net: "tcp"}, false},
		{"net.OpError for tcp4", &net.OpError{Net: "tcp4"}, false},
		{"net.OpError for tcp6", &net.OpError{Net: "tcp6"}, false},
		{"net.OpError for tcp with dial", &net.OpError{Net: "tcp", Op: "dial"}, true},
		{"net.OpError for tcp with write", &net.OpError{Net: "tcp", Op: "write"}, true},
		{"net.OpError for tcp with read", &net.OpError{Net: "tcp", Op: "read"}, true},
		{"net.OpError for udp with write", &net.OpError{Net: "udp", Op: "write"}, true},
		{"net.OpError for udp with read", &net.OpError{Net: "udp", Op: "read"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := failedRemote(test.err)
			if actual != test.expected {
				t.Fatalf("expected %t, got %t", test.expected, actual)
			}
		})
	}
}

func TestMemberlist_PushPull(t *testing.T) {
	addr1 := getBindAddr()
	addr2 := getBindAddr()
	ip1 := []byte(addr1)
	ip2 := []byte(addr2)

	sink := newRecordingSink()

	ch := make(chan NodeEvent, 3)

	m1 := HostMemberlist(addr1.String(), t, func(c *Config) {
		c.GossipInterval = 10 * time.Second
		c.PushPullInterval = time.Millisecond
		c.Metrics = sink
	})
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	m2 := HostMemberlist(addr2.String(), t, func(c *Config) {
		c.BindPort = bindPort
		c.GossipInterval = 10 * time.Second
		c.Events = &ChannelEventDelegate{ch}
	})
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	a1 := alive{Node: addr1.String(), Addr: ip1, Port: uint16(bindPort), Incarnation: 1, Vsn: m1.config.BuildVsnArray()}
	m1.aliveNode(&a1, true)
	a2 := alive{Node: addr2.String(), Addr: ip2, Port: uint16(bindPort), Incarnation: 1, Vsn: m2.config.BuildVsnArray()}
	m1.aliveNode(&a2, false)

	// Gossip should send all this to m2. It's UDP though so retry a few times
	retry(t, 10, 50*time.Millisecond, func(failf func(string, ...any)) {
		m1.pushPull()

		// Wait longer for metrics to be emitted after pushPull completes
		time.Sleep(20 * time.Millisecond)

		if len(ch) < 2 {
			failf("expected 2 messages from pushPull")
		}

		if _, ok := sink.gauge("memberlist.size.local"); !ok {
			failf("memberlist.size.local gauge not emitted")
		}
		for _, s := range nodeStates {
			if _, ok := sink.gauge("memberlist.node.instances;node_state=" + s.metricsString()); !ok {
				failf("memberlist.node.instances gauge not emitted for %s", s.metricsString())
			}
		}
	})
}

func TestVerifyProtocol(t *testing.T) {
	cases := []struct {
		Anodes   [][3]uint8
		Bnodes   [][3]uint8
		expected bool
	}{
		// Both running identical everything
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{0, 0, 0},
			},
			expected: true,
		},

		// One can understand newer, but speaking same protocol
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{0, 1, 0},
			},
			expected: true,
		},

		// One is speaking outside the range
		{
			Anodes: [][3]uint8{
				{0, 0, 0},
			},
			Bnodes: [][3]uint8{
				{1, 1, 1},
			},
			expected: false,
		},

		// Transitively outside the range
		{
			Anodes: [][3]uint8{
				{0, 1, 0},
				{0, 2, 1},
			},
			Bnodes: [][3]uint8{
				{1, 3, 1},
			},
			expected: false,
		},

		// Multi-node
		{
			Anodes: [][3]uint8{
				{0, 3, 2},
				{0, 2, 0},
			},
			Bnodes: [][3]uint8{
				{0, 2, 1},
				{0, 5, 0},
			},
			expected: true,
		},
	}

	for _, tc := range cases {
		aCore := make([][6]uint8, len(tc.Anodes))
		aApp := make([][6]uint8, len(tc.Anodes))
		for i, n := range tc.Anodes {
			aCore[i] = [6]uint8{n[0], n[1], n[2], 0, 0, 0}
			aApp[i] = [6]uint8{0, 0, 0, n[0], n[1], n[2]}
		}

		bCore := make([][6]uint8, len(tc.Bnodes))
		bApp := make([][6]uint8, len(tc.Bnodes))
		for i, n := range tc.Bnodes {
			bCore[i] = [6]uint8{n[0], n[1], n[2], 0, 0, 0}
			bApp[i] = [6]uint8{0, 0, 0, n[0], n[1], n[2]}
		}

		// Test core protocol verification
		testVerifyProtocolSingle(t, aCore, bCore, tc.expected)
		testVerifyProtocolSingle(t, bCore, aCore, tc.expected)

		//  Test app protocol verification
		testVerifyProtocolSingle(t, aApp, bApp, tc.expected)
		testVerifyProtocolSingle(t, bApp, aApp, tc.expected)
	}
}

func testVerifyProtocolSingle(t *testing.T, A [][6]uint8, B [][6]uint8, expect bool) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	m.nodes = make([]*nodeState, len(A))
	for i, n := range A {
		m.nodes[i] = &nodeState{
			PMin: n[0],
			PMax: n[1],
			PCur: n[2],
			DMin: n[3],
			DMax: n[4],
			DCur: n[5],
		}
	}

	remote := make([]pushNodeState, len(B))
	for i, n := range B {
		remote[i] = pushNodeState{
			Name: fmt.Sprintf("node %d", i),
			Vsn:  []uint8{n[0], n[1], n[2], n[3], n[4], n[5]},
		}
	}

	err := m.verifyProtocol(remote)
	if (err == nil) != expect {
		t.Fatalf("bad:\nA: %v\nB: %v\nErr: %s", A, B, err)
	}
}

// TestMalformedVsnFromWire covers upstream hashicorp/memberlist#368: the
// protocol version vector arrives from the network, so a peer can send
// fewer than the six entries every correct sender produces. Such a vector
// must be refused on every path — never indexed out of range — while an
// empty vector (legacy protocol-0 senders) keeps its old meaning.
func TestMalformedVsnFromWire(t *testing.T) {
	newM := func(t *testing.T) *Memberlist {
		m := GetMemberlist(t, func(c *Config) {
			c.Transport = (&MockNetwork{}).NewTransport("local")
			c.Merge = acceptMerge{}
		})
		t.Cleanup(func() { _ = m.Shutdown() })
		if err := m.setAlive(nil); err != nil {
			t.Fatal(err)
		}
		return m
	}

	for n := 1; n < 6; n++ {
		vsn := []uint8{1, 5, 2, 0, 0, 0}[:n]

		t.Run(fmt.Sprintf("alive/len%d", n), func(t *testing.T) {
			m := newM(t)
			noPanic(t, "aliveNode", func() {
				m.aliveNode(&alive{Incarnation: 1, Node: "peer", Addr: []byte{127, 0, 0, 2}, Port: 7946, Vsn: vsn}, false)
			})
			m.nodeLock.RLock()
			_, admitted := m.nodeMap["peer"]
			m.nodeLock.RUnlock()
			if admitted {
				t.Fatalf("alive message with a %d-entry Vsn was admitted", n)
			}
		})

		t.Run(fmt.Sprintf("alive-update/len%d", n), func(t *testing.T) {
			m := newM(t)
			m.aliveNode(&alive{Incarnation: 1, Node: "peer", Addr: []byte{127, 0, 0, 2}, Port: 7946, Vsn: []uint8{1, 5, 2, 0, 0, 0}}, false)
			noPanic(t, "aliveNode", func() {
				m.aliveNode(&alive{Incarnation: 2, Node: "peer", Addr: []byte{127, 0, 0, 2}, Port: 7946, Vsn: vsn}, false)
			})
			m.nodeLock.RLock()
			inc := m.nodeMap["peer"].Incarnation
			m.nodeLock.RUnlock()
			if inc != 1 {
				t.Fatalf("alive update with a %d-entry Vsn was applied (incarnation %d)", n, inc)
			}
		})

		for _, state := range []NodeStateType{StateAlive, StateSuspect} {
			t.Run(fmt.Sprintf("push-pull/%s/len%d", state.metricsString(), n), func(t *testing.T) {
				m := newM(t)
				remote := []pushNodeState{{Name: "peer", Addr: []byte{127, 0, 0, 2}, Port: 7946, State: int(state), Vsn: vsn}}
				var err error
				noPanic(t, "mergeRemoteState", func() { err = m.mergeRemoteState(true, remote, nil) })
				if err == nil {
					t.Fatalf("push/pull state with a %d-entry Vsn was merged", n)
				}
			})
		}
	}

	t.Run("empty-vsn-is-legacy-zero", func(t *testing.T) {
		m := newM(t)
		m.aliveNode(&alive{Incarnation: 1, Node: "legacy", Addr: []byte{127, 0, 0, 3}, Port: 7946}, false)
		m.nodeLock.RLock()
		n, ok := m.nodeMap["legacy"]
		m.nodeLock.RUnlock()
		if !ok || n.PMin != 0 || n.PMax != 0 {
			t.Fatalf("empty Vsn: admitted=%v node=%+v, want admitted with zero versions", ok, n)
		}
	})
}

// acceptMerge is a MergeDelegate that admits every merge, so tests reach the
// code paths that run after the merge decision.
type acceptMerge struct{}

func (acceptMerge) NotifyMerge([]*Node) error { return nil }

// TestNodeStateVisibleToUsers covers finding F1: the Node values handed to
// users must carry the node's current state. nodeState used to declare its
// own State field, shadowing Node.State, so every Node copy reported
// StateAlive — including the one passed to NotifyLeave.
func TestNodeStateVisibleToUsers(t *testing.T) {
	events := make(chan NodeEvent, 16)
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("local")
		c.Events = &ChannelEventDelegate{Ch: events}
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	vsn := []uint8{1, 5, 2, 0, 0, 0}
	m.aliveNode(&alive{Incarnation: 1, Node: "suspect", Addr: []byte{127, 0, 0, 2}, Port: 7946, Vsn: vsn}, false)
	m.aliveNode(&alive{Incarnation: 1, Node: "dead", Addr: []byte{127, 0, 0, 3}, Port: 7946, Vsn: vsn}, false)
	m.suspectNode(&suspect{Incarnation: 1, Node: "suspect", From: "someone"})
	m.deadNode(&dead{Incarnation: 1, Node: "dead", From: "someone"})

	for _, n := range m.Members() {
		if n.Name == "suspect" && n.State != StateSuspect {
			t.Fatalf("Members() reports %q as state %d, want StateSuspect", n.Name, n.State)
		}
	}

	var leave *Node
	for leave == nil {
		select {
		case ev := <-events:
			if ev.Event == NodeLeave {
				leave = ev.Node
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no NotifyLeave delivered")
		}
	}
	if leave.Name != "dead" || leave.State != StateDead {
		t.Fatalf("NotifyLeave got %q in state %d, want %q in StateDead", leave.Name, leave.State, "dead")
	}
}

// TestAliveAddressComparisonIgnoresIPForm: the same IPv4 address can arrive
// as 4 bytes or as its 16-byte IPv4-mapped form (a node bound to 0.0.0.0
// advertises the 16-byte form). Treating the two as different addresses
// reported a conflict and refused the node's own refutation.
func TestAliveAddressComparisonIgnoresIPForm(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("local")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	vsn := []uint8{1, 5, 2, 0, 0, 0}
	m.aliveNode(&alive{Incarnation: 1, Node: "peer", Addr: []byte{10, 0, 0, 7}, Port: 7946, Vsn: vsn}, false)
	m.aliveNode(&alive{Incarnation: 2, Node: "peer", Addr: net.ParseIP("10.0.0.7"), Port: 7946, Vsn: vsn}, false)

	m.nodeLock.RLock()
	inc := m.nodeMap["peer"].Incarnation
	m.nodeLock.RUnlock()
	if inc != 2 {
		t.Fatalf("incarnation %d after an alive with the 16-byte form of the same address, want 2", inc)
	}
}
