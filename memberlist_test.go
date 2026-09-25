// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	iretry "github.com/0xCarbon/mori/internal/retry"
)

var bindLock sync.Mutex
var bindNum byte = 10

func getBindAddrNet(network byte) net.IP {
	bindLock.Lock()
	defer bindLock.Unlock()

	result := net.IPv4(127, 0, network, bindNum)
	bindNum++

	return result
}

func getBindAddr() net.IP {
	return getBindAddrNet(0)
}

func testConfigNet(tb testing.TB, network byte) *Config {
	tb.Helper()

	config := DefaultLANConfig()
	config.BindAddr = getBindAddrNet(network).String()
	config.Name = config.BindAddr
	config.BindPort = 0 // choose free port
	config.RequireNodeNames = true
	config.Logger = testLogger(tb, config.Name)
	return config
}

func testConfig(tb testing.TB) *Config {
	return testConfigNet(tb, 0)
}

func yield() {
	time.Sleep(250 * time.Millisecond)
}

type MockDelegate struct {
	mu          sync.Mutex
	meta        []byte
	msgs        [][]byte
	broadcasts  [][]byte
	state       []byte
	remoteState []byte
}

func (m *MockDelegate) setMeta(meta []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meta = meta
}

func (m *MockDelegate) setState(state []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
}

func (m *MockDelegate) setBroadcasts(broadcasts [][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = broadcasts
}

func (m *MockDelegate) getRemoteState() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]byte, len(m.remoteState))
	copy(out, m.remoteState)
	return out
}

func (m *MockDelegate) getMessages() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([][]byte, len(m.msgs))
	for i, msg := range m.msgs {
		out[i] = make([]byte, len(msg))
		copy(out[i], msg)
	}
	return out
}

func (m *MockDelegate) NodeMeta(limit int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.meta
}

func (m *MockDelegate) NotifyMsg(msg []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]byte, len(msg))
	copy(cp, msg)
	m.msgs = append(m.msgs, cp)
}

func (m *MockDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := m.broadcasts
	m.broadcasts = nil
	return b
}

func (m *MockDelegate) LocalState(join bool) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state
}

func (m *MockDelegate) MergeRemoteState(s []byte, join bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.remoteState = s
}

func GetMemberlist(tb testing.TB, f func(c *Config)) *Memberlist {
	c := testConfig(tb)
	c.BindPort = 0 // assign a free port
	if f != nil {
		f(c)
	}

	m, err := newMemberlist(c)
	noErr(tb, err)
	return m
}

func TestDefaultLANConfig_protocolVersion(t *testing.T) {
	c := DefaultLANConfig()
	if c.ProtocolVersion != ProtocolVersion2Compatible {
		t.Fatalf("should be max: %d", c.ProtocolVersion)
	}
}

func TestCreate_protocolVersion(t *testing.T) {
	cases := []struct {
		name    string
		version uint8
		err     bool
	}{
		{"min", ProtocolVersionMin, false},
		{"max", ProtocolVersionMax, false},
		// TODO(mitchellh): uncommon when we're over 0
		//{"uncommon", ProtocolVersionMin - 1, true},
		{"max+1", ProtocolVersionMax + 1, true},
		{"min-1", ProtocolVersionMax - 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultLANConfig()
			c.BindAddr = getBindAddr().String()
			c.ProtocolVersion = tc.version

			m, err := Create(c)
			if err == nil {
				noErr(t, m.Shutdown())
			}

			if tc.err && err == nil {
				t.Fatalf("Should've failed with version: %d", tc.version)
			} else if !tc.err && err != nil {
				t.Fatalf("Version '%d' error: %s", tc.version, err)
			}
		})
	}
}

func TestCreate_secretKey(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
		err  bool
	}{
		{"size-0", make([]byte, 0), false},
		{"abc", []byte("abc"), true},
		{"size-16", make([]byte, 16), false},
		{"size-38", make([]byte, 38), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultLANConfig()
			c.BindAddr = getBindAddr().String()
			c.SecretKey = tc.key

			m, err := Create(c)
			if err == nil {
				noErr(t, m.Shutdown())
			}

			if tc.err && err == nil {
				t.Fatalf("Should've failed with key: %#v", tc.key)
			} else if !tc.err && err != nil {
				t.Fatalf("Key '%#v' error: %s", tc.key, err)
			}
		})
	}
}

func TestCreate_secretKeyEmpty(t *testing.T) {
	c := DefaultLANConfig()
	c.BindAddr = getBindAddr().String()
	c.SecretKey = make([]byte, 0)

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	if m.config.EncryptionEnabled() {
		t.Fatalf("Expected encryption to be disabled")
	}
}

func TestCreate_checkBroadcastQueueMetrics(t *testing.T) {
	sink := newRecordingSink()
	c := testConfig(t)
	c.QueueCheckInterval = 10 * time.Millisecond
	c.SecretKey = make([]byte, 0)
	c.Metrics = sink

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// The first sample lands one QueueCheckInterval after Create.
	iretry.Run(t, func(r *iretry.R) {
		if sink.sampleCount("memberlist.queue.broadcasts") == 0 {
			r.Fatalf("memberlist.queue.broadcasts sample not emitted")
		}
	})
}

func TestCreate_keyringOnly(t *testing.T) {
	c := DefaultLANConfig()
	c.BindAddr = getBindAddr().String()

	keyring, err := NewKeyring(nil, make([]byte, 16))
	noErr(t, err)
	c.Keyring = keyring

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	if !m.config.EncryptionEnabled() {
		t.Fatalf("Expected encryption to be enabled")
	}
}

func TestCreate_keyringAndSecretKey(t *testing.T) {
	c := DefaultLANConfig()
	c.BindAddr = getBindAddr().String()

	keyring, err := NewKeyring(nil, make([]byte, 16))
	noErr(t, err)
	c.Keyring = keyring
	c.SecretKey = []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	if !m.config.EncryptionEnabled() {
		t.Fatalf("Expected encryption to be enabled")
	}

	ringKeys := c.Keyring.GetKeys()
	if !bytes.Equal(c.SecretKey, ringKeys[0]) {
		t.Fatalf("Unexpected primary key %v", ringKeys[0])
	}
}

func TestCreate(t *testing.T) {
	c := testConfig(t)
	c.ProtocolVersion = ProtocolVersionMin
	c.DelegateProtocolVersion = 13
	c.DelegateProtocolMin = 12
	c.DelegateProtocolMax = 24

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	yield()

	members := m.Members()
	if len(members) != 1 {
		t.Fatalf("bad number of members")
	}

	if members[0].PMin != ProtocolVersionMin {
		t.Fatalf("bad: %#v", members[0])
	}

	if members[0].PMax != ProtocolVersionMax {
		t.Fatalf("bad: %#v", members[0])
	}

	if members[0].PCur != c.ProtocolVersion {
		t.Fatalf("bad: %#v", members[0])
	}

	if members[0].DMin != c.DelegateProtocolMin {
		t.Fatalf("bad: %#v", members[0])
	}

	if members[0].DMax != c.DelegateProtocolMax {
		t.Fatalf("bad: %#v", members[0])
	}

	if members[0].DCur != c.DelegateProtocolVersion {
		t.Fatalf("bad: %#v", members[0])
	}
}

func TestMemberList_CreateShutdown(t *testing.T) {
	m := GetMemberlist(t, nil)
	m.schedule()
	noErr(t, m.Shutdown())
}

func TestMemberList_ResolveAddr(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	defaultPort := uint16(m.config.BindPort)

	type testCase struct {
		name           string
		in             string
		expectErr      bool
		ignoreExpectIP bool
		expect         []ipPort
	}

	baseCases := []testCase{
		{
			name:           "localhost",
			in:             "localhost",
			ignoreExpectIP: true,
			expect: []ipPort{
				{port: defaultPort},
			},
		},
		{
			name: "ipv6 pair",
			in:   "[::1]:80",
			expect: []ipPort{
				{ip: net.IPv6loopback, port: 80},
			},
		},
		{
			name: "ipv6 non-pair",
			in:   "[::1]",
			expect: []ipPort{
				{ip: net.IPv6loopback, port: defaultPort},
			},
		},
		{
			name:      "hostless port",
			in:        ":80",
			expectErr: true,
		},
		{
			name:           "hostname port combo",
			in:             "localhost:80",
			ignoreExpectIP: true,
			expect: []ipPort{
				{port: 80},
			},
		},
		{
			name:      "too high port",
			in:        "localhost:80000",
			expectErr: true,
		},
		{
			name: "ipv4 port combo",
			in:   "127.0.0.1:80",
			expect: []ipPort{
				{ip: net.IPv4(127, 0, 0, 1), port: 80},
			},
		},
		{
			name: "ipv6 port combo",
			in:   "[2001:db8:a0b:12f0::1]:80",
			expect: []ipPort{
				{
					ip:   net.IP{0x20, 0x01, 0x0d, 0xb8, 0x0a, 0x0b, 0x12, 0xf0, 0, 0, 0, 0, 0, 0, 0, 0x1},
					port: 80,
				},
			},
		},
		{
			name:      "ipv4 port combo with empty tag",
			in:        "/127.0.0.1:80",
			expectErr: true,
		},
		{
			name: "ipv4 only",
			in:   "127.0.0.1",
			expect: []ipPort{
				{ip: net.IPv4(127, 0, 0, 1), port: defaultPort},
			},
		},
		{
			name: "ipv6 only",
			in:   "[2001:db8:a0b:12f0::1]",
			expect: []ipPort{
				{
					ip:   net.IP{0x20, 0x01, 0x0d, 0xb8, 0x0a, 0x0b, 0x12, 0xf0, 0, 0, 0, 0, 0, 0, 0, 0x1},
					port: defaultPort,
				},
			},
		},
	}

	// explode the cases to include tagged versions of everything
	var cases []testCase
	for _, tc := range baseCases {
		cases = append(cases, tc)
		if !strings.Contains(tc.in, "/") { // don't double tag already tagged cases
			tc2 := testCase{
				name:           tc.name + " (tagged)",
				in:             "foo.bar/" + tc.in,
				expectErr:      tc.expectErr,
				ignoreExpectIP: tc.ignoreExpectIP,
			}
			for _, ipp := range tc.expect {
				tc2.expect = append(tc2.expect, ipPort{
					ip:       ipp.ip,
					port:     ipp.port,
					nodeName: "foo.bar",
				})
			}
			cases = append(cases, tc2)
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := m.resolveAddr(tc.in)
			if tc.expectErr {
				isErr(t, err)
			} else {
				noErr(t, err)
				if tc.ignoreExpectIP {
					if len(got) > 1 {
						got = got[0:1]
					}
					for i := 0; i < len(got); i++ {
						got[i].ip = nil
					}
				}
				equal(t, tc.expect, got)
			}
		})
	}
}

func TestMemberList_Members(t *testing.T) {
	n1 := &Node{Name: "test", State: StateAlive}
	n2 := &Node{Name: "test2", State: StateDead}
	n3 := &Node{Name: "test3", State: StateSuspect}

	m := &Memberlist{}
	nodes := []*nodeState{
		{Node: *n1},
		{Node: *n2},
		{Node: *n3},
	}
	m.nodes = nodes

	members := m.Members()
	if !reflect.DeepEqual(members, []*Node{n1, n3}) {
		t.Fatalf("bad members")
	}
}

func TestMemberlist_Join(t *testing.T) {
	c1 := testConfig(t)
	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfig(t)
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 1 {
		t.Fatalf("unexpected 1: %d", num)
	}
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// Check the hosts
	if len(m2.Members()) != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}
}

func TestMemberlist_Join_with_Labels(t *testing.T) {
	testMemberlist_Join_with_Labels(t, nil)
}
func TestMemberlist_Join_with_Labels_and_Encryption(t *testing.T) {
	secretKey := TestKeys[0]
	testMemberlist_Join_with_Labels(t, secretKey)
}
func testMemberlist_Join_with_Labels(t *testing.T, secretKey []byte) {
	c1 := testConfig(t)
	c1.Label = "blah"
	c1.SecretKey = secretKey
	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfig(t)
	c2.Label = "blah"
	c2.BindPort = bindPort
	c2.SecretKey = secretKey
	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	runStep(t, "same label can join", func(t *testing.T) {
		num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		noErr(t, err)
		equal(t, 1, num)

		// Wait for cluster convergence (both members and estimate)
		waitUntilSizeAndEstimate(t, m2, 2)
		waitUntilSizeAndEstimate(t, m1, 2)
	})

	// Create a third node that uses no label
	c3 := testConfig(t)
	c3.Label = ""
	c3.BindPort = bindPort
	c3.SecretKey = secretKey
	m3, err := Create(c3)
	noErr(t, err)
	defer func() {
		if err := m3.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()
	runStep(t, "no label cannot join", func(t *testing.T) {
		_, err := m3.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		isErr(t, err)

		// Verify cluster state remains unchanged after failed join
		waitUntilSizeAndEstimate(t, m3, 1)
		waitUntilSizeAndEstimate(t, m2, 2)
		waitUntilSizeAndEstimate(t, m1, 2)
	})

	// Create a fourth node that uses a mismatched label
	c4 := testConfig(t)
	c4.Label = "not-blah"
	c4.BindPort = bindPort
	c4.SecretKey = secretKey
	m4, err := Create(c4)
	noErr(t, err)
	defer func() {
		if err := m4.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	runStep(t, "mismatched label cannot join", func(t *testing.T) {
		_, err := m4.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		isErr(t, err)

		// Verify cluster state remains unchanged after failed join
		waitUntilSizeAndEstimate(t, m4, 1)
		waitUntilSizeAndEstimate(t, m3, 1)
		waitUntilSizeAndEstimate(t, m2, 2)
		waitUntilSizeAndEstimate(t, m1, 2)
	})
}

func TestMemberlist_JoinDifferentNetworksUniqueMask(t *testing.T) {
	c1 := testConfigNet(t, 0)
	c1.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/8"})
	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfigNet(t, 1)
	c2.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/8"})
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 1 {
		t.Fatalf("unexpected 1: %d", num)
	}
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// Check the hosts
	if len(m2.Members()) != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}
}

func TestMemberlist_JoinDifferentNetworksMultiMasks(t *testing.T) {
	c1 := testConfigNet(t, 0)
	c1.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/24", "127.0.1.0/24"})
	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfigNet(t, 1)
	c2.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/24", "127.0.1.0/24"})
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	err = joinAndTestMemberShip(t, m2, []string{m1.config.Name + "/" + m1.config.BindAddr}, 2)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// Create a rogue node that allows all networks
	// It should see others, but will not be seen by others
	c3 := testConfigNet(t, 2)
	c3.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/8"})
	c3.BindPort = bindPort

	m3, err := Create(c3)
	noErr(t, err)
	defer func() {
		if err := m3.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()
	// The rogue can see others, but others cannot see it
	err = joinAndTestMemberShip(t, m3, []string{m1.config.Name + "/" + m1.config.BindAddr}, 3)
	// For the node itself, everything seems fine, it should see others
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// m1 and m2 should not see newcomer however
	if len(m1.Members()) != 2 {
		t.Fatalf("m1 should have 2 nodes! %v", m1.Members())
	}
	if m1.estNumNodes() != 2 {
		t.Fatalf("m1 should have 2 est. nodes! %v", m1.estNumNodes())
	}

	if len(m2.Members()) != 2 {
		t.Fatalf("m2 should have 2 nodes! %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("m2 should have 2 est. nodes! %v", m2.estNumNodes())
	}

	// Another rogue, this time with a config that denies itself
	// Create a rogue node that allows all networks
	// It should see others, but will not be seen by others
	c4 := testConfigNet(t, 2)
	c4.CIDRsAllowed, _ = ParseCIDRs([]string{"127.0.0.0/24", "127.0.1.0/24"})
	c4.BindPort = bindPort

	m4, err := Create(c4)
	noErr(t, err)
	defer func() {
		if err := m4.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// This time, the node should not even see itself, so 2 expected nodes
	_ = joinAndTestMemberShip(t, m4, []string{m1.config.BindAddr, m2.config.BindAddr}, 2)
	// m1 and m2 should not see newcomer however
	if len(m1.Members()) != 2 {
		t.Fatalf("m1 should have 2 nodes! %v", m1.Members())
	}
	if m1.estNumNodes() != 2 {
		t.Fatalf("m1 should have 2 est. nodes! %v", m1.estNumNodes())
	}

	if len(m2.Members()) != 2 {
		t.Fatalf("m2 should have 2 nodes! %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("m2 should have 2 est. nodes! %v", m2.estNumNodes())
	}
}

type CustomMergeDelegate struct {
	invoked atomic.Bool
	t       *testing.T
}

func (c *CustomMergeDelegate) NotifyMerge(nodes []*Node) error {
	c.t.Logf("Cancel merge")
	c.invoked.Store(true)
	return fmt.Errorf("Custom merge canceled")
}

func TestMemberlist_Join_Cancel(t *testing.T) {
	c1 := testConfig(t)
	merge1 := &CustomMergeDelegate{t: t}
	c1.Merge = merge1

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfig(t)
	c2.BindPort = bindPort
	merge2 := &CustomMergeDelegate{t: t}
	c2.Merge = merge2

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 0 {
		t.Fatalf("unexpected 0: %d", num)
	}
	if !strings.Contains(err.Error(), "Custom merge canceled") {
		t.Fatalf("unexpected err: %s", err)
	}

	// Check the hosts
	if len(m2.Members()) != 1 {
		t.Fatalf("should have 1 nodes! %v", m2.Members())
	}
	if len(m1.Members()) != 1 {
		t.Fatalf("should have 1 nodes! %v", m1.Members())
	}

	// Check delegate invocation. m2's merge ran inside Join; m1 merges
	// after sending its reply, so its delegate may still be running when
	// Join returns.
	if !merge2.invoked.Load() {
		t.Fatalf("should invoke delegate")
	}
	iretry.Run(t, func(r *iretry.R) {
		if !merge1.invoked.Load() {
			r.Fatalf("should invoke delegate")
		}
	})
}

type CustomAliveDelegate struct {
	Ignore string
	count  int

	t *testing.T
}

func (c *CustomAliveDelegate) NotifyAlive(peer *Node) error {
	c.count++
	if peer.Name == c.Ignore {
		return nil
	}
	c.t.Logf("Cancel alive")
	return fmt.Errorf("Custom alive canceled")
}

func TestMemberlist_Join_Cancel_Passive(t *testing.T) {
	c1 := testConfig(t)
	alive1 := &CustomAliveDelegate{
		Ignore: c1.Name,
		t:      t,
	}
	c1.Alive = alive1

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := testConfig(t)
	c2.BindPort = bindPort
	alive2 := &CustomAliveDelegate{
		Ignore: c2.Name,
		t:      t,
	}
	c2.Alive = alive2

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 1 {
		t.Fatalf("unexpected 1: %d", num)
	}
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Check the hosts
	if len(m2.Members()) != 1 {
		t.Fatalf("should have 1 nodes! %v", m2.Members())
	}
	if len(m1.Members()) != 1 {
		t.Fatalf("should have 1 nodes! %v", m1.Members())
	}

	// Check delegate invocation
	if alive1.count == 0 {
		t.Fatalf("should invoke delegate: %d", alive1.count)
	}
	if alive2.count == 0 {
		t.Fatalf("should invoke delegate: %d", alive2.count)
	}
}

func TestMemberlist_Join_protocolVersions(t *testing.T) {
	c1 := testConfig(t)

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	c2 := testConfig(t)
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	c3 := testConfig(t)
	c3.BindPort = bindPort
	c3.ProtocolVersion = ProtocolVersionMax

	m3, err := Create(c3)
	noErr(t, err)
	defer func() {
		if err := m3.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	_, err = m1.Join([]string{c2.Name + "/" + c2.BindAddr})
	noErr(t, err)

	yield()

	_, err = m1.Join([]string{c3.Name + "/" + c3.BindAddr})
	noErr(t, err)
}

func joinAndTestMemberShip(t *testing.T, self *Memberlist, membersToJoin []string, expectedMembers int) error {
	t.Helper()
	num, err := self.Join(membersToJoin)
	if err != nil {
		return err
	}
	if num != len(membersToJoin) {
		t.Fatalf("unexpected %d, was expecting %d to be joined", num, len(membersToJoin))
	}
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	// Check the hosts
	if len(self.Members()) != expectedMembers {
		t.Fatalf("should have 2 nodes! %v", self.Members())
	}
	if len(self.Members()) != expectedMembers {
		t.Fatalf("should have 2 nodes! %v", self.Members())
	}
	return nil
}

func TestMemberlist_Leave(t *testing.T) {
	newConfig := func() *Config {
		c := testConfig(t)
		c.GossipInterval = time.Millisecond
		return c
	}

	c1 := newConfig()

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := newConfig()
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	err = joinAndTestMemberShip(t, m2, []string{m1.config.Name + "/" + m1.config.BindAddr}, 2)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// Leave
	err = m1.Leave(time.Second)
	noErr(t, err)

	// Wait for leave
	time.Sleep(10 * time.Millisecond)

	// m1 should think dead
	if len(m1.Members()) != 1 {
		t.Fatalf("should have 1 node")
	}

	if len(m2.Members()) != 1 {
		t.Fatalf("should have 1 node")
	}

	if m2.nodeMap[c1.Name].State != StateLeft {
		t.Fatalf("bad state")
	}
}

func TestMemberlist_JoinShutdown(t *testing.T) {
	newConfig := func() *Config {
		c := testConfig(t)
		c.ProbeInterval = time.Millisecond
		c.ProbeTimeout = 100 * time.Microsecond
		c.SuspicionMaxTimeoutMult = 1
		return c
	}

	c1 := newConfig()

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := newConfig()
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 1 {
		t.Fatalf("unexpected 1: %d", num)
	}
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// Check the hosts
	if len(m2.Members()) != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}

	noErr(t, m1.Shutdown())

	waitForCondition(t, func() (bool, string) {
		n := len(m2.Members())
		return n == 1, fmt.Sprintf("expected 1 node, got %d", n)
	})
}

func TestMemberlist_delegateMeta(t *testing.T) {
	c1 := testConfig(t)
	c1.Delegate = &MockDelegate{meta: []byte("web")}

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	c2 := testConfig(t)
	c2.BindPort = bindPort
	c2.Delegate = &MockDelegate{meta: []byte("lb")}

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	_, err = m1.Join([]string{c2.Name + "/" + c2.BindAddr})
	noErr(t, err)

	yield()

	var roles map[string]string

	// Check the roles of members of m1
	m1m := m1.Members()
	if len(m1m) != 2 {
		t.Fatalf("bad: %#v", m1m)
	}

	roles = make(map[string]string)
	for _, m := range m1m {
		roles[m.Name] = string(m.Meta)
	}

	if r := roles[c1.Name]; r != "web" {
		t.Fatalf("bad role for %s: %s", c1.Name, r)
	}

	if r := roles[c2.Name]; r != "lb" {
		t.Fatalf("bad role for %s: %s", c2.Name, r)
	}

	// Check the roles of members of m2
	m2m := m2.Members()
	if len(m2m) != 2 {
		t.Fatalf("bad: %#v", m2m)
	}

	roles = make(map[string]string)
	for _, m := range m2m {
		roles[m.Name] = string(m.Meta)
	}

	if r := roles[c1.Name]; r != "web" {
		t.Fatalf("bad role for %s: %s", c1.Name, r)
	}

	if r := roles[c2.Name]; r != "lb" {
		t.Fatalf("bad role for %s: %s", c2.Name, r)
	}
}

func TestMemberlist_delegateMeta_Update(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newSimNet()
		mock1 := &MockDelegate{meta: []byte("web")}
		m1 := simCreate(t, n, 1, func(c *Config) { c.Delegate = mock1 })
		mock2 := &MockDelegate{meta: []byte("lb")}
		m2 := simCreate(t, n, 2, func(c *Config) { c.Delegate = mock2 })

		_, err := m1.Join([]string{m2.config.Name + "/" + m2.config.BindAddr})
		noErr(t, err)
		yield()

		// Update the meta data roles
		mock1.setMeta([]byte("api"))
		mock2.setMeta([]byte("db"))
		noErr(t, m1.UpdateNode(0))
		noErr(t, m2.UpdateNode(0))
		yield()

		// Both members see both updates.
		for _, m := range []*Memberlist{m1, m2} {
			roles := map[string]string{}
			for _, node := range m.Members() {
				roles[node.Name] = string(node.Meta)
			}
			equal(t, map[string]string{"node1": "api", "node2": "db"}, roles, "roles seen by %s", m.config.Name)
		}
	})
}

func TestMemberlist_UserData(t *testing.T) {
	newConfig := func() (*Config, *MockDelegate) {
		d := &MockDelegate{}
		c := testConfig(t)
		// Set the gossip/pushpull intervals fast enough to get a reasonable test,
		// but slow enough to avoid "sendto: operation not permitted"
		c.GossipInterval = 100 * time.Millisecond
		c.PushPullInterval = 100 * time.Millisecond
		c.Delegate = d
		return c, d
	}

	c1, d1 := newConfig()
	d1.setState([]byte("something"))

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	bcasts := make([][]byte, 256)
	for i := range bcasts {
		bcasts[i] = fmt.Appendf(nil, "%d", i)
	}

	// Create a second node
	c2, d2 := newConfig()
	c2.BindPort = bindPort

	// Second delegate has things to send
	d2.setBroadcasts(bcasts)
	d2.setState([]byte("my state"))

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	if num != 1 {
		t.Fatalf("unexpected 1: %d", num)
	}
	noErr(t, err)

	// Check the hosts
	if m2.NumMembers() != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}

	// Wait for a little while
	iretry.Run(t, func(r *iretry.R) {
		msgs1 := d1.getMessages()

		// Ensure we got the messages. Ordering of messages is not guaranteed so just
		// check we got them both in either order.
		elementsMatch(r, bcasts, msgs1)

		rs1 := d1.getRemoteState()
		rs2 := d2.getRemoteState()

		// Check the push/pull state
		if !reflect.DeepEqual(rs1, []byte("my state")) {
			r.Fatalf("bad state %s", rs1)
		}
		if !reflect.DeepEqual(rs2, []byte("something")) {
			r.Fatalf("bad state %s", rs2)
		}
	})
}

func TestMemberlist_SendTo(t *testing.T) {
	newConfig := func() (*Config, *MockDelegate, net.IP) {
		d := &MockDelegate{}
		c := testConfig(t)
		c.GossipInterval = time.Millisecond
		c.PushPullInterval = time.Millisecond
		c.Delegate = d
		return c, d, net.ParseIP(c.BindAddr)
	}

	c1, d1, _ := newConfig()

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	c2, d2, addr2 := newConfig()
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	noErr(t, err)
	equal(t, 1, num)

	// Check the hosts
	equal(t, 2, m2.NumMembers(), "should have 2 nodes! %v", m2.Members())

	// Try to do a direct send
	m2Addr := &net.UDPAddr{
		IP:   addr2,
		Port: bindPort,
	}
	m2Address := Address{
		Addr: m2Addr.String(),
		Name: m2.config.Name,
	}
	if err := m1.SendToAddress(m2Address, []byte("ping")); err != nil {
		t.Fatalf("err: %v", err)
	}

	m1Addr := &net.UDPAddr{
		IP:   net.ParseIP(m1.config.BindAddr),
		Port: bindPort,
	}
	m1Address := Address{
		Addr: m1Addr.String(),
		Name: m1.config.Name,
	}
	if err := m2.SendToAddress(m1Address, []byte("pong")); err != nil {
		t.Fatalf("err: %v", err)
	}

	waitForCondition(t, func() (bool, string) {
		msgs := d1.getMessages()
		return len(msgs) == 1, fmt.Sprintf("expected 1 message, got %d", len(msgs))
	})

	msgs1 := d1.getMessages()
	if !reflect.DeepEqual(msgs1[0], []byte("pong")) {
		t.Fatalf("bad msg %v", msgs1[0])
	}

	waitForCondition(t, func() (bool, string) {
		msgs := d2.getMessages()
		return len(msgs) == 1, fmt.Sprintf("expected 1 message, got %d", len(msgs))
	})
	msgs2 := d2.getMessages()
	if !reflect.DeepEqual(msgs2[0], []byte("ping")) {
		t.Fatalf("bad msg %v", msgs2[0])
	}
}

func waitForCondition(t *testing.T, fn func() (bool, string)) {
	start := time.Now()

	var msg string
	for time.Since(start) < 20*time.Second {
		var done bool
		done, msg = fn()
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for condition: %v", msg)
}

func TestMemberlistProtocolVersion(t *testing.T) {
	c := testConfig(t)
	c.ProtocolVersion = ProtocolVersionMax

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	result := m.ProtocolVersion()
	if result != ProtocolVersionMax {
		t.Fatalf("bad: %d", result)
	}
}

func TestMemberlist_Join_DeadNode(t *testing.T) {
	c1 := testConfig(t)
	c1.TCPTimeout = 50 * time.Millisecond

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second "node", which is just a TCP listener that
	// does not ever respond. This is to test our deadlines
	addr2 := getBindAddr()
	list, err := net.Listen("tcp", net.JoinHostPort(addr2.String(), strconv.Itoa(bindPort)))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() {
		if err := list.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Ensure we don't hang forever
	timer := time.AfterFunc(100*time.Millisecond, func() {
		panic("should have timed out by now")
	})
	defer timer.Stop()

	num, err := m1.Join([]string{"fake/" + addr2.String()})
	if num != 0 {
		t.Fatalf("unexpected 0: %d", num)
	}
	if err == nil {
		t.Fatal("expect err")
	}
}

// Tests that nodes running different versions of the protocol can successfully
// discover each other and add themselves to their respective member lists.
func TestMemberlist_Join_Protocol_Compatibility(t *testing.T) {
	testProtocolVersionPair := func(t *testing.T, pv1 uint8, pv2 uint8) {
		t.Helper()

		c1 := testConfig(t)
		c1.ProtocolVersion = pv1

		m1, err := Create(c1)
		noErr(t, err)
		defer func() {
			if err := m1.Shutdown(); err != nil {
				t.Fatal(err)
			}
		}()

		bindPort := m1.config.BindPort

		c2 := testConfig(t)
		c2.BindPort = bindPort
		c2.ProtocolVersion = pv2

		m2, err := Create(c2)
		noErr(t, err)
		defer func() {
			if err := m2.Shutdown(); err != nil {
				t.Fatal(err)
			}
		}()

		num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
		noErr(t, err)
		equal(t, 1, num)

		// Wait for cluster convergence
		waitUntilSize(t, m2, 2)
		waitUntilSize(t, m1, 2)
	}

	t.Run("2,1", func(t *testing.T) {
		testProtocolVersionPair(t, 2, 1)
	})
	t.Run("2,3", func(t *testing.T) {
		testProtocolVersionPair(t, 2, 3)
	})
	t.Run("3,2", func(t *testing.T) {
		testProtocolVersionPair(t, 3, 2)
	})
	t.Run("3,1", func(t *testing.T) {
		testProtocolVersionPair(t, 3, 1)
	})
}

var (
	ipv6LoopbackAvailableOnce sync.Once
	ipv6LoopbackAvailable     bool
)

func isIPv6LoopbackAvailable(t *testing.T) bool {
	const ipv6LoopbackAddress = "::1"
	ipv6LoopbackAvailableOnce.Do(func() {
		ifaces, err := net.Interfaces()
		noErr(t, err)

		for _, iface := range ifaces {
			if iface.Flags&net.FlagLoopback == 0 {
				continue
			}
			addrs, err := iface.Addrs()
			noErr(t, err)

			for _, addr := range addrs {
				ipaddr := addr.(*net.IPNet)
				if ipaddr.IP.String() == ipv6LoopbackAddress {
					ipv6LoopbackAvailable = true
					return
				}
			}
		}
		ipv6LoopbackAvailable = false
		t.Logf("IPv6 loopback address %q not found, disabling tests that require it", ipv6LoopbackAddress)
	})

	return ipv6LoopbackAvailable
}

func TestMemberlist_Join_IPv6(t *testing.T) {
	if !isIPv6LoopbackAvailable(t) {
		t.SkipNow()
		return
	}
	// Since this binds to all interfaces we need to exclude other tests
	// from grabbing an interface.
	bindLock.Lock()
	defer bindLock.Unlock()

	c1 := DefaultLANConfig()
	c1.Name = "A"
	c1.BindAddr = "[::1]"
	c1.BindPort = 0 // choose free
	c1.Logger = testLogger(t, c1.Name)

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// Create a second node
	c2 := DefaultLANConfig()
	c2.Name = "B"
	c2.BindAddr = "[::1]"
	c2.BindPort = 0 // choose free
	c2.Logger = testLogger(t, c2.Name)

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{fmt.Sprintf("%s/%s:%d", m1.config.Name, m1.config.BindAddr, m1.config.BindPort)})
	noErr(t, err)
	equal(t, 1, num)

	// Check the hosts. m1 merges after replying, so it may lag Join.
	if len(m2.Members()) != 2 {
		t.Fatalf("should have 2 nodes! %v", m2.Members())
	}
	iretry.Run(t, func(r *iretry.R) {
		if len(m1.Members()) != 2 {
			r.Fatalf("should have 2 nodes! %v", m1.Members())
		}
	})
}

func reservePort(t *testing.T, ip net.IP, purpose string) int {
	for range 10 {
		tcpAddr := &net.TCPAddr{IP: ip, Port: 0}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			if strings.Contains(err.Error(), "address already in use") {
				continue
			}
			t.Fatalf("unexpected error: %v", err)
		}

		port := tcpLn.Addr().(*net.TCPAddr).Port

		udpAddr := &net.UDPAddr{IP: ip, Port: port}
		udpLn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			_ = tcpLn.Close()
			if strings.Contains(err.Error(), "address already in use") {
				continue
			}
			t.Fatalf("unexpected error: %v", err)
		}

		t.Logf("Using dynamic bind port %d for %s", port, purpose)
		_ = tcpLn.Close()
		_ = udpLn.Close()
		return port
	}

	t.Fatalf("could not find a free TCP+UDP port to listen on for %s", purpose)
	panic("IMPOSSIBLE")
}

func TestAdvertiseAddr(t *testing.T) {
	bindAddr := getBindAddr()
	advertiseAddr := getBindAddr()

	bindPort := reservePort(t, bindAddr, "BIND")
	advertisePort := reservePort(t, advertiseAddr, "ADVERTISE")

	c := DefaultLANConfig()
	c.BindAddr = bindAddr.String()
	c.BindPort = bindPort
	c.Name = c.BindAddr

	c.AdvertiseAddr = advertiseAddr.String()
	c.AdvertisePort = advertisePort

	m, err := Create(c)
	noErr(t, err)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	yield()

	members := m.Members()
	equal(t, 1, len(members))

	equal(t, advertiseAddr.String(), members[0].Addr.String())
	equal(t, advertisePort, int(members[0].Port))
}

type MockConflict struct {
	mu       sync.Mutex
	existing *Node
	other    *Node
}

func (m *MockConflict) NotifyConflict(existing, other *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.existing = existing
	m.other = other
}

func (m *MockConflict) get() (existing, other *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.existing, m.other
}

func TestMemberlist_conflictDelegate(t *testing.T) {
	c1 := testConfig(t)
	mock := &MockConflict{}
	c1.Conflict = mock

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Ensure name conflict
	c2 := testConfig(t)
	c2.Name = c1.Name
	c2.BindPort = bindPort

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m1.Join([]string{c2.Name + "/" + c2.BindAddr})
	noErr(t, err)
	equal(t, 1, num)

	yield()

	// Ensure we were notified (delivery is async since #6)
	var existing, other *Node
	deadline := time.Now().Add(5 * time.Second)
	for {
		existing, other = mock.get()
		if existing != nil && other != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("should get notified existing=%v other=%v", existing, other)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if existing.Name != other.Name {
		t.Fatalf("bad: %v %v", existing, other)
	}
}

type MockPing struct {
	mu      sync.Mutex
	other   *Node
	rtt     time.Duration
	payload []byte
}

func (m *MockPing) NotifyPingComplete(other *Node, rtt time.Duration, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.other = other
	m.rtt = rtt
	m.payload = payload
}

func (m *MockPing) getContents() (*Node, time.Duration, []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.other, m.rtt, m.payload
}

const DEFAULT_PAYLOAD = "whatever"

func (m *MockPing) AckPayload() []byte {
	return []byte(DEFAULT_PAYLOAD)
}

func TestMemberlist_PingDelegate(t *testing.T) {
	newConfig := func() *Config {
		c := testConfig(t)
		c.ProbeInterval = 100 * time.Millisecond
		c.Ping = &MockPing{}
		return c
	}

	c1 := newConfig()

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	// Create a second node
	c2 := newConfig()
	c2.BindPort = bindPort
	mock := c2.Ping.(*MockPing)

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	num, err := m2.Join([]string{m1.config.Name + "/" + m1.config.BindAddr})
	noErr(t, err)
	equal(t, 1, num)

	waitUntilSize(t, m1, 2)
	waitUntilSize(t, m2, 2)

	time.Sleep(2 * c1.ProbeInterval)

	noErr(t, m1.Shutdown())
	noErr(t, m2.Shutdown())

	mOther, mRTT, mPayload := mock.getContents()

	// Ensure we were notified
	if mOther == nil {
		t.Fatalf("should get notified")
	}

	if !reflect.DeepEqual(mOther, m1.LocalNode()) {
		t.Fatalf("not notified about the correct node; expected: %+v; actual: %+v",
			m2.LocalNode(), mOther)
	}

	if mRTT <= 0 {
		t.Fatalf("rtt should be greater than 0")
	}

	if !bytes.Equal(mPayload, []byte(DEFAULT_PAYLOAD)) {
		t.Fatalf("incorrect payload. expected: %v; actual: %v",
			[]byte(DEFAULT_PAYLOAD), mPayload)
	}
}

func waitUntilSize(t *testing.T, m *Memberlist, expected int) {
	t.Helper()
	retry(t, 15, 500*time.Millisecond, func(failf func(string, ...any)) {
		t.Helper()

		if m.NumMembers() != expected {
			failf("%s expected to have %d members but had: %v", m.config.Name, expected, m.Members())
		}
	})
}

// waitUntilSizeAndEstimate waits for both NumMembers and estNumNodes to reach expected value
// Use this when you need to verify both metrics converge (e.g., after successful joins)
func waitUntilSizeAndEstimate(t *testing.T, m *Memberlist, expected int) {
	t.Helper()
	retry(t, 15, 500*time.Millisecond, func(failf func(string, ...any)) {
		t.Helper()

		if m.NumMembers() != expected {
			failf("%s expected to have %d members but had: %v", m.config.Name, expected, m.Members())
		}
		if m.estNumNodes() != expected {
			failf("%s expected to have %d estimated nodes but had: %d", m.config.Name, expected, m.estNumNodes())
		}
	})
}

// TestMemberlist_EncryptedGossipTransition walks a two-node cluster
// through the three-stage upshift to encrypted gossip (verify incoming and
// outgoing off, then outgoing on, then both on), restarting each node at
// its address at every stage. It runs in a synctest bubble on a simulated
// network.
func TestMemberlist_EncryptedGossipTransition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newSimNet()
		key := []byte("Hi16ZXu2lNCRVwtr20khAg==")
		stage := func(verifyIn, verifyOut bool, secret []byte) func(*Config) {
			return func(c *Config) {
				c.GossipInterval = 100 * time.Millisecond
				c.SecretKey = secret
				c.GossipVerifyIncoming = verifyIn
				c.GossipVerifyOutgoing = verifyOut
			}
		}

		joinOK := func(src, dst *Memberlist) {
			t.Helper()
			num, err := src.Join([]string{dst.config.Name + "/" + dst.config.BindAddr})
			noErr(t, err)
			equal(t, 1, num)
			for _, m := range []*Memberlist{src, dst} {
				waitUntilSize(t, m, 2)
				equal(t, 2, len(m.Members()), "nodes: %v", m.Members())
				equal(t, 2, m.estNumNodes(), "nodes: %v", m.Members())
			}
		}
		// restart has m leave and shut down, then starts node i again
		// with the stage's settings and joins it to the bystander.
		restart := func(m, bystander *Memberlist, i int, settings func(*Config)) *Memberlist {
			t.Helper()
			noErr(t, m.Leave(time.Second))
			waitUntilSize(t, bystander, 1)
			noErr(t, m.Shutdown())
			waitUntilSize(t, bystander, 1)
			next := simCreate(t, n, i, settings)
			joinOK(next, bystander)
			return next
		}

		// Stage 0: a two-node unencrypted cluster.
		plain := stage(true, true, nil)
		m0 := simCreate(t, n, 0+1, plain)
		m1 := simCreate(t, n, 1+1, plain)
		joinOK(m1, m0)

		// Stage 1: keys installed, nothing verified or encrypted yet.
		m0 = restart(m0, m1, 1, stage(false, false, key))
		m1 = restart(m1, m0, 2, stage(false, false, key))

		// Stage 2: outgoing gossip encrypted, incoming not yet enforced.
		m0 = restart(m0, m1, 1, stage(false, true, key))
		m1 = restart(m1, m0, 2, stage(false, true, key))

		// Stage 3: fully enforced.
		m0 = restart(m0, m1, 1, stage(true, true, key))
		m1 = restart(m1, m0, 2, stage(true, true, key))

		// Control: a plaintext node cannot join the locked-down cluster,
		// so the stages above really exercised encryption.
		outsider := simCreate(t, n, 3, plain)
		if _, err := outsider.Join([]string{m0.config.Name + "/" + m0.config.BindAddr}); err == nil {
			t.Fatal("a plaintext node joined an encryption-enforcing cluster")
		}
		equal(t, 2, m1.NumMembers())
	})
}

// Consul bug, rapid restart (before failure detection),
// with an updated meta data. Should be at incarnation 1 for
// both.
//
// This test is uncommented because it requires that either we
// can rebind the socket (SO_REUSEPORT) which Go does not allow,
// OR we must disable the address conflict checking in memberlist.
// I just comment out that code to test this case.
//
//func TestMemberlist_Restart_delegateMeta_Update(t *testing.T) {
//    c1 := testConfig()
//    c2 := testConfig()
//    mock1 := &MockDelegate{meta: []byte("web")}
//    mock2 := &MockDelegate{meta: []byte("lb")}
//    c1.Delegate = mock1
//    c2.Delegate = mock2

//    m1, err := Create(c1)
//    if err != nil {
//        t.Fatalf("err: %s", err)
//    }
//    defer m1.Shutdown()

//    m2, err := Create(c2)
//    if err != nil {
//        t.Fatalf("err: %s", err)
//    }
//    defer m2.Shutdown()

//    _, err = m1.Join([]string{c2.BindAddr})
//    if err != nil {
//        t.Fatalf("err: %s", err)
//    }

//    yield()

//    // Recreate m1 with updated meta
//    m1.Shutdown()
//    c3 := testConfig()
//    c3.Name = c1.Name
//    c3.Delegate = mock1
//    c3.GossipInterval = time.Millisecond
//    mock1.meta = []byte("api")

//    m1, err = Create(c3)
//    if err != nil {
//        t.Fatalf("err: %s", err)
//    }
//    defer m1.Shutdown()

//    _, err = m1.Join([]string{c2.BindAddr})
//    if err != nil {
//        t.Fatalf("err: %s", err)
//    }

//    yield()
//    yield()

//    // Check the updates have propagated
//    var roles map[string]string

//    // Check the roles of members of m1
//    m1m := m1.Members()
//    if len(m1m) != 2 {
//        t.Fatalf("bad: %#v", m1m)
//    }

//    roles = make(map[string]string)
//    for _, m := range m1m {
//        roles[m.Name] = string(m.Meta)
//    }

//    if r := roles[c1.Name]; r != "api" {
//        t.Fatalf("bad role for %s: %s", c1.Name, r)
//    }

//    if r := roles[c2.Name]; r != "lb" {
//        t.Fatalf("bad role for %s: %s", c2.Name, r)
//    }

//    // Check the roles of members of m2
//    m2m := m2.Members()
//    if len(m2m) != 2 {
//        t.Fatalf("bad: %#v", m2m)
//    }

//    roles = make(map[string]string)
//    for _, m := range m2m {
//        roles[m.Name] = string(m.Meta)
//    }

//    if r := roles[c1.Name]; r != "api" {
//        t.Fatalf("bad role for %s: %s", c1.Name, r)
//    }

//    if r := roles[c2.Name]; r != "lb" {
//        t.Fatalf("bad role for %s: %s", c2.Name, r)
//    }
//}

func runStep(t *testing.T, name string, fn func(t *testing.T)) {
	t.Helper()
	if !t.Run(name, fn) {
		t.FailNow()
	}
}

// Regression: Members() and LocalNode() must return snapshot copies, not
// pointers into Memberlist-internal state. Handing out live *Node aliases
// lets callers read Node.Meta (and Addr/Incarnation/...) while alive/update
// handling mutates the same memory under nodeLock — a data race caught by
// the race detector (originally surfaced by taba's Cluster.Meta reading
// Members()[i].Meta concurrently with UpdateNode).
func TestMemberlist_Members_SnapshotNoRace(t *testing.T) {
	d := &MockDelegate{}
	d.setMeta([]byte{0})

	c := testConfig(t)
	c.Delegate = d
	m, err := Create(c)
	noErr(t, err)
	defer func() { _ = m.Shutdown() }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, n := range m.Members() {
				_ = append([]byte(nil), n.Meta...) // read Meta contents
			}
			_ = append([]byte(nil), m.LocalNode().Meta...)
		}
	})

	for i := range 100 {
		d.setMeta([]byte{byte(i)})
		noErr(t, m.UpdateNode(time.Second))
	}
	close(stop)
	wg.Wait()
}

// DNS record types used by the test server.
const (
	dnsTypeA     uint16 = 1
	dnsTypeHINFO uint16 = 13
	dnsTypeAAAA  uint16 = 28
	dnsTypeANY   uint16 = 255
)

// serveTestDNS runs a minimal DNS-over-TCP server (RFC 1035 §4.2.2 framing)
// that answers queries for name with the records in answers, keyed by query
// type, and returns its address. Unknown names get NXDOMAIN. It is written
// from the RFC with the standard library only, so the test does not share
// code with the resolver under test.
func serveTestDNS(t *testing.T, name string, answers map[uint16][][]byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(getBindAddr().String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestDNSConn(conn, name, answers)
		}
	}()
	return ln.Addr().String()
}

func serveTestDNSConn(conn net.Conn, name string, answers map[uint16][][]byte) {
	defer func() { _ = conn.Close() }()
	for {
		var size [2]byte
		if _, err := io.ReadFull(conn, size[:]); err != nil {
			return
		}
		query := make([]byte, binary.BigEndian.Uint16(size[:]))
		if _, err := io.ReadFull(conn, query); err != nil || len(query) < 12 {
			return
		}
		// Question: labels, then qtype and qclass.
		var labels []string
		i := 12
		for i < len(query) && query[i] != 0 {
			n := int(query[i])
			if i+1+n > len(query) {
				return
			}
			labels = append(labels, string(query[i+1:i+1+n]))
			i += 1 + n
		}
		if i+5 > len(query) {
			return
		}
		qend := i + 5
		qtype := binary.BigEndian.Uint16(query[i+1:])
		qname := strings.ToLower(strings.Join(labels, ".") + ".")

		var rrs [][]byte
		rcode := uint16(0)
		if qname == name {
			rrs = answers[qtype]
		} else {
			rcode = 3 // NXDOMAIN
		}
		resp := binary.BigEndian.AppendUint16(nil, binary.BigEndian.Uint16(query))
		flags := uint16(0x8400) | binary.BigEndian.Uint16(query[2:])&0x0100 | rcode // QR, AA, echo RD
		resp = binary.BigEndian.AppendUint16(resp, flags)
		resp = binary.BigEndian.AppendUint16(resp, 1)
		resp = binary.BigEndian.AppendUint16(resp, uint16(len(rrs)))
		resp = binary.BigEndian.AppendUint16(resp, 0)
		resp = binary.BigEndian.AppendUint16(resp, 0)
		resp = append(resp, query[12:qend]...)
		for _, rdata := range rrs {
			rtype := qtype
			if qtype == dnsTypeANY {
				rtype = dnsTypeHINFO
			}
			resp = append(resp, 0xc0, 12) // name: pointer to the question
			resp = binary.BigEndian.AppendUint16(resp, rtype)
			resp = binary.BigEndian.AppendUint16(resp, 1) // class IN
			resp = binary.BigEndian.AppendUint32(resp, 60)
			resp = binary.BigEndian.AppendUint16(resp, uint16(len(rdata)))
			resp = append(resp, rdata...)
		}
		out := binary.BigEndian.AppendUint16(nil, uint16(len(resp)))
		if _, err := conn.Write(append(out, resp...)); err != nil {
			return
		}
	}
}

// resolvConf writes a resolv.conf naming server and returns its path.
func resolvConf(t *testing.T, server string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(path, []byte("# test\nsearch example.org\nnameserver "+server+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestResolveAddrTCPFirstRFC8482 covers decision D4: resolvers following
// RFC 8482 answer ANY queries with a single synthesized HINFO record, so a
// TCP-first lookup built on an ANY query finds no addresses and silently
// falls back to the system resolver. The lookup must ask for A and AAAA.
func TestResolveAddrTCPFirstRFC8482(t *testing.T) {
	const name = "join.service.consul."
	server := serveTestDNS(t, name, map[uint16][][]byte{
		dnsTypeA:    {{127, 0, 0, 1}},
		dnsTypeAAAA: {net.ParseIP("2001:db8:a0b:12f0::1").To16()},
		dnsTypeANY:  {append([]byte{7}, "RFC8482"...)},
	})
	m := GetMemberlist(t, func(c *Config) {
		c.DNSConfigPath = resolvConf(t, server)
		c.Transport = (&MockNetwork{}).NewTransport("local")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	for _, host := range []string{"join.service.consul", "join.service.consul.", "node1/join.service.consul:4000"} {
		ips, err := m.resolveAddr(host)
		if err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		var got []string
		for _, ip := range ips {
			got = append(got, joinHostPort(ip.ip.String(), ip.port)+"/"+ip.nodeName)
		}
		slices.Sort(got)
		port, nodeName := strconv.Itoa(m.config.BindPort), ""
		if strings.Contains(host, "/") {
			port, nodeName = "4000", "node1"
		}
		want := []string{
			net.JoinHostPort("127.0.0.1", port) + "/" + nodeName,
			net.JoinHostPort("2001:db8:a0b:12f0::1", port) + "/" + nodeName,
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s resolved to %v, want %v", host, got, want)
		}
	}
}

func TestReadNameservers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	conf := "# comment\n; also a comment\nsearch example.org\nnameserver 10.0.0.1\nnameserver 10.0.0.2:5353 # trailing\nnameserver ::1\nnameserver [fe80::1]:53\noptions ndots:2\nnameserver\n"
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readNameservers(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.1:53", "10.0.0.2:5353", "[::1]:53", "[fe80::1]:53"}
	if !slices.Equal(got, want) {
		t.Fatalf("readNameservers = %v, want %v", got, want)
	}
	if _, err := readNameservers(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing resolv.conf was not an error")
	}
}
