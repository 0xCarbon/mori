// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestUtil_PortFunctions(t *testing.T) {
	tests := []struct {
		addr       string
		hasPort    bool
		ensurePort string
	}{
		{"1.2.3.4", false, "1.2.3.4:8301"},
		{"1.2.3.4:1234", true, "1.2.3.4:1234"},
		{"2600:1f14:e22:1501:f9a:2e0c:a167:67e8", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]", false, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:8301"},
		{"[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234", true, "[2600:1f14:e22:1501:f9a:2e0c:a167:67e8]:1234"},
		{"localhost", false, "localhost:8301"},
		{"localhost:1234", true, "localhost:1234"},
		{"hashicorp.com", false, "hashicorp.com:8301"},
		{"hashicorp.com:1234", true, "hashicorp.com:1234"},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if got, want := hasPort(tt.addr), tt.hasPort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
			if got, want := ensurePort(tt.addr, 8301), tt.ensurePort; got != want {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf := encode(pingMsg, msg)
	var out ping
	if err := decode(buf[1:], &out); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if msg.SeqNo != out.SeqNo {
		t.Fatalf("bad sequence no")
	}
}

func TestRandomOffset(t *testing.T) {
	vals := make(map[int]struct{})
	for range 100 {
		offset := randomOffset(1 << 30) // fits int on 32-bit targets
		if _, ok := vals[offset]; ok {
			t.Fatalf("got collision")
		}
		vals[offset] = struct{}{}
	}
}

func TestRandomOffset_Zero(t *testing.T) {
	offset := randomOffset(0)
	if offset != 0 {
		t.Fatalf("bad offset")
	}
}

func TestSuspicionTimeout(t *testing.T) {
	timeouts := map[int]time.Duration{
		5:    1000 * time.Millisecond,
		10:   1000 * time.Millisecond,
		50:   1698 * time.Millisecond,
		100:  2000 * time.Millisecond,
		500:  2698 * time.Millisecond,
		1000: 3000 * time.Millisecond,
	}
	for n, expected := range timeouts {
		timeout := suspicionTimeout(3, n, time.Second) / 3
		if timeout != expected {
			t.Fatalf("bad: %v, %v", expected, timeout)
		}
	}
}

func TestRetransmitLimit(t *testing.T) {
	lim := retransmitLimit(3, 0)
	if lim != 0 {
		t.Fatalf("bad val %v", lim)
	}
	lim = retransmitLimit(3, 1)
	if lim != 3 {
		t.Fatalf("bad val %v", lim)
	}
	lim = retransmitLimit(3, 99)
	if lim != 6 {
		t.Fatalf("bad val %v", lim)
	}
}

func TestShuffleNodes(t *testing.T) {
	orig := []*nodeState{
		{
			State: StateDead,
		},
		{
			State: StateAlive,
		},
		{
			State: StateAlive,
		},
		{
			State: StateDead,
		},
		{
			State: StateAlive,
		},
		{
			State: StateAlive,
		},
		{
			State: StateDead,
		},
		{
			State: StateAlive,
		},
	}
	nodes := make([]*nodeState, len(orig))
	copy(nodes[:], orig[:])

	if !reflect.DeepEqual(nodes, orig) {
		t.Fatalf("should match")
	}

	shuffleNodes(nodes)

	// A shuffle must preserve the exact pointer multiset — nothing added,
	// dropped, or duplicated. It must NOT be asserted to change the
	// arrangement: with only two distinct states among 8 nodes, a valid
	// random permutation reproduces a DeepEqual-identical slice with
	// probability 3!*5!/8! = 1/56 — the source of a long-standing flake
	// (upstream memberlist#202, mori#11).
	if len(nodes) != len(orig) {
		t.Fatalf("shuffle changed length: %d != %d", len(nodes), len(orig))
	}
	seen := make(map[*nodeState]int, len(orig))
	for _, n := range orig {
		seen[n]++
	}
	for _, n := range nodes {
		seen[n]--
		if seen[n] < 0 {
			t.Fatalf("shuffle introduced or duplicated a node: %p", n)
		}
	}
	for _, count := range seen {
		if count != 0 {
			t.Fatalf("shuffle dropped nodes: %v", seen)
		}
	}
}

func TestPushPullScale(t *testing.T) {
	sec := time.Second
	for i := 0; i <= 32; i++ {
		if s := pushPullScale(sec, i); s != sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 33; i <= 64; i++ {
		if s := pushPullScale(sec, i); s != 2*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
	for i := 65; i <= 128; i++ {
		if s := pushPullScale(sec, i); s != 3*sec {
			t.Fatalf("Bad time scale: %v", s)
		}
	}
}

func TestMoveDeadNodes(t *testing.T) {
	nodes := []*nodeState{
		{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		// This dead node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		{
			State:       StateDead,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		// This left node should not be moved, as its state changed
		// less than the specified GossipToTheDead time ago
		{
			State:       StateLeft,
			StateChange: time.Now().Add(-10 * time.Second),
		},
		{
			State:       StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		{
			State:       StateDead,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		{
			State:       StateAlive,
			StateChange: time.Now().Add(-20 * time.Second),
		},
		{
			State:       StateLeft,
			StateChange: time.Now().Add(-20 * time.Second),
		},
	}

	idx := moveDeadNodes(nodes, (15 * time.Second))
	if idx != 5 {
		t.Fatalf("bad index")
	}
	for i := range idx {
		switch i {
		case 2:
			// Recently dead node remains at index 2,
			// since nodes are swapped out to move to end.
			if nodes[i].State != StateDead {
				t.Fatalf("Bad state %d", i)
			}
		case 3:
			//Recently left node should remain at 3
			if nodes[i].State != StateLeft {
				t.Fatalf("Bad State %d", i)
			}
		default:
			if nodes[i].State != StateAlive {
				t.Fatalf("Bad state %d", i)
			}
		}
	}
	for i := idx; i < len(nodes); i++ {
		if !nodes[i].DeadOrLeft() {
			t.Fatalf("Bad state %d", i)
		}
	}
}

func TestKRandomNodes(t *testing.T) {
	nodes := []*nodeState{}
	for i := range 90 {
		// Half the nodes are in a bad state
		state := StateAlive
		switch i % 3 {
		case 0:
			state = StateAlive
		case 1:
			state = StateSuspect
		case 2:
			state = StateDead
		}
		nodes = append(nodes, &nodeState{
			Name:  fmt.Sprintf("test%d", i),
			State: state,
		})
	}

	filterFunc := func(n *nodeState) bool {
		if n.Name == "test0" || n.State != StateAlive {
			return true
		}
		return false
	}

	s1 := kRandomNodes(3, nodes, filterFunc)
	s2 := kRandomNodes(3, nodes, filterFunc)
	s3 := kRandomNodes(3, nodes, filterFunc)

	if reflect.DeepEqual(s1, s2) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s1, s3) {
		t.Fatalf("unexpected equal")
	}
	if reflect.DeepEqual(s2, s3) {
		t.Fatalf("unexpected equal")
	}

	for _, s := range [][]Node{s1, s2, s3} {
		if len(s) != 3 {
			t.Fatalf("bad len")
		}
		for _, n := range s {
			if n.Name == "test0" {
				t.Fatalf("Bad name")
			}
			if n.State != StateAlive {
				t.Fatalf("Bad state")
			}
		}
	}

	// make sure we test the very-small path
	nodes = nodes[:8]
	s4 := kRandomNodes(3, nodes, filterFunc)
	if len(s4) != 2 {
		t.Fatalf("expected 2 nodes")
	}
	for _, n := range s4 {
		if n.Name != "test3" && n.Name != "test6" {
			t.Fatalf("unexpected node picked")
		}
	}
}

func TestMakeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf := encode(pingMsg, msg)

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	if len(compound) != 3*len(buf)+3*compoundOverhead+compoundHeaderOverhead {
		t.Fatalf("bad len")
	}
}

func TestDecodeCompoundMessage(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf := encode(pingMsg, msg)

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 0 {
		t.Fatalf("should not truncate")
	}
	if len(parts) != 3 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != len(buf) {
			t.Fatalf("bad part len")
		}
	}
}

func TestDecodeCompoundMessage_NumberOfPartsOverflow(t *testing.T) {
	buf := []byte{0x80}
	_, _, err := decodeCompoundMessage(buf)
	isErr(t, err)
	equal(t, err.Error(), "truncated len slice")
}

func TestDecodeCompoundMessage_Trunc(t *testing.T) {
	msg := &ping{SeqNo: 100}
	buf := encode(pingMsg, msg)

	msgs := [][]byte{buf, buf, buf}
	compound := makeCompoundMessage(msgs)

	trunc, parts, err := decodeCompoundMessage(compound[1:38])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if trunc != 1 {
		t.Fatalf("truncate: %d", trunc)
	}
	if len(parts) != 2 {
		t.Fatalf("bad parts")
	}
	for _, p := range parts {
		if len(p) != len(buf) {
			t.Fatalf("bad part len")
		}
	}
}

func TestCompressDecompressPayload(t *testing.T) {
	buf, err := compressPayload([]byte("testing"))
	if err != nil {
		t.Fatal(err)
	}

	decomp, err := decompressPayload(buf[1:])
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if !reflect.DeepEqual(decomp, []byte("testing")) {
		t.Fatalf("bad payload: %v", decomp)
	}
}

// TestDecompressBufferLimit covers upstream hashicorp/memberlist#363: a small
// compressed message must not expand without bound. Streams accept up to
// maxDecompressedBytes; packets, which senders build within the packet
// budget, accept far less.
func TestDecompressBufferLimit(t *testing.T) {
	compressed := func(n int) *compress {
		buf, err := compressPayload(make([]byte, n))
		if err != nil {
			t.Fatal(err)
		}
		var c compress
		if err := decode(buf[1:], &c); err != nil {
			t.Fatal(err)
		}
		return &c
	}

	if _, err := decompressBuffer(compressed(maxDecompressedBytes+1), maxDecompressedBytes); err == nil {
		t.Fatal("stream payload above maxDecompressedBytes was accepted")
	}
	if out, err := decompressBuffer(compressed(maxPushStateBytes+1), maxDecompressedBytes); err != nil || len(out) != maxPushStateBytes+1 {
		t.Fatalf("stream payload within the limit: len %d, err %v", len(out), err)
	}

	bomb, err := compressPayload(make([]byte, maxPacketDecompressedBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	if len(bomb) > 65507 {
		t.Fatalf("test bomb does not fit one UDP payload: %d bytes", len(bomb))
	}
	if _, err := decompressPayload(bomb[1:]); err == nil {
		t.Fatal("packet payload above maxPacketDecompressedBytes was accepted")
	}
}

// TestMaxIncompressiblePacket: no message of at most maxIncompressiblePacket
// bytes compresses to fewer bytes, so skipping compression for them cannot
// change what is sent. The most compressible inputs (short periods) are
// checked, and a 25-byte run of zeros does compress, so the bound is not
// vacuous.
func TestMaxIncompressiblePacket(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for n := 1; n <= maxIncompressiblePacket; n++ {
		inputs := [][]byte{}
		for period := 1; period <= 4; period++ {
			in := make([]byte, n)
			for i := range in {
				in[i] = byte(i % period)
			}
			inputs = append(inputs, in)
		}
		for range 64 {
			in := make([]byte, n)
			for i := range in {
				in[i] = byte(r.IntN(3))
			}
			inputs = append(inputs, in)
		}
		for _, in := range inputs {
			out, err := compressPayload(in)
			noErr(t, err)
			if len(out) < len(in) {
				t.Fatalf("%d-byte input %x compressed to %d bytes", n, in, len(out))
			}
		}
	}
	out, err := compressPayload(make([]byte, 25))
	noErr(t, err)
	less(t, len(out), 25, "a 25-byte zero run must compress")
}

// TestCompressionConcurrent exercises the pooled LZW coders from many
// goroutines (run under -race in make ci).
func TestCompressionConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 200 {
				in := bytes.Repeat([]byte{byte(g), byte(i)}, 100+i)
				out, err := compressPayload(in)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := decompressPayload(out[1:])
				if err != nil || !bytes.Equal(got, in) {
					t.Errorf("round trip %d/%d: %v", g, i, err)
					return
				}
			}
		})
	}
	wg.Wait()
}
