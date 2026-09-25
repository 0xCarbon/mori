// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"log/slog"
	"net"
	"testing"
	"time"
)

// fuzzMember returns a live instance whose listeners, handler and event
// dispatcher are not running: fuzz inputs are processed synchronously and
// reach the full state machine. Suspicion and ack timers still fire on
// their own goroutines, and instances persist across inputs, so a crash may
// depend on earlier inputs as well as the reported one.
func fuzzMember(tb testing.TB, f func(*Config)) *Memberlist {
	tb.Helper()
	m, err := newFuzzMember(f)
	if err != nil {
		tb.Fatal(err)
	}
	return m
}

// newFuzzMember builds a fuzz instance without a testing.TB, so fuzz
// targets can rebuild one from inside the fuzz function.
func newFuzzMember(f func(*Config)) (*Memberlist, error) {
	c := DefaultLANConfig()
	c.Name = "local"
	c.BindAddr = "127.0.0.1"
	c.BindPort = 7946
	c.Transport = (&MockNetwork{}).NewTransport("local")
	c.Logger = slog.New(slog.DiscardHandler)
	if f != nil {
		f(c)
	}
	m, err := buildMemberlist(c)
	if err != nil {
		return nil, err
	}
	return m, m.setAlive(nil)
}

// mustFuzzMember is newFuzzMember for use inside fuzz functions.
func mustFuzzMember(t *testing.T, f func(*Config)) *Memberlist {
	t.Helper()
	m, err := newFuzzMember(f)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// maxFuzzNodes bounds the node table a fuzz instance accumulates from
// random names before it is rebuilt.
const maxFuzzNodes = 512

// seedPackets are well-formed packets of every kind the packet path
// handles, as fuzzing seeds.
func seedPackets(m *Memberlist) [][]byte {
	vsn := []uint8{1, 5, 2, 0, 0, 0}
	a := alive{Incarnation: 2, Node: "peer", Addr: []byte{10, 0, 0, 2}, Port: 7946, Meta: []byte("m"), Vsn: vsn}
	msgs := [][]byte{
		encode(pingMsg, ping{SeqNo: 1, Node: m.config.Name, SourceAddr: []byte{10, 0, 0, 2}, SourcePort: 7946, SourceNode: "peer"}),
		encode(indirectPingMsg, indirectPingReq{SeqNo: 2, Target: []byte{10, 0, 0, 3}, Port: 7946, Node: "x", Nack: true}),
		encode(ackRespMsg, ackResp{SeqNo: 1, Payload: []byte("p")}),
		encode(nackRespMsg, nackResp{SeqNo: 2}),
		encode(aliveMsg, a),
		encode(suspectMsg, suspect{Incarnation: 2, Node: "peer", From: "x"}),
		encode(deadMsg, dead{Incarnation: 3, Node: "peer", From: "x"}),
		append([]byte{byte(userMsg)}, "hello"...),
	}
	msgs = append(msgs, makeCompoundMessage(msgs[:4]))
	if c, err := compressPayload(makeCompoundMessage(msgs[4:8])); err == nil {
		msgs = append(msgs, c)
	}
	return msgs
}

// ingestSync runs one datagram through the packet path and processes the
// messages it queued, synchronously.
func ingestSync(m *Memberlist, packet []byte) {
	m.ingestPacket(packet, &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 7946}, time.Now())
	for {
		msg, ok := m.getNextMessage()
		if !ok {
			return
		}
		m.handleQueued(msg)
	}
}

// TestFuzzInstanceReachesStateMachine: the fuzz instances process inputs
// through the full state machine (not a shut-down fence): the seed alive
// message adds its node, and the seed dead message then marks it dead.
func TestFuzzInstanceReachesStateMachine(t *testing.T) {
	m := fuzzMember(t, nil)
	for _, p := range seedPackets(m) {
		ingestSync(m, p)
	}
	m.nodeLock.RLock()
	peer, ok := m.nodeMap["peer"]
	m.nodeLock.RUnlock()
	if !ok || peer.State != StateDead {
		t.Fatalf("seed packets did not reach the state machine: peer %+v (known %v)", peer, ok)
	}
}

// FuzzIngestPacket feeds arbitrary datagrams through the packet path —
// label, decryption, CRC, envelopes, every message decoder and the state
// machine — which must never panic.
func FuzzIngestPacket(f *testing.F) {
	key := []byte("0123456789abcdef")
	secureConfig := func(c *Config) {
		c.SecretKey = key
		c.Label = "fuzz"
	}
	plain, secure := fuzzMember(f, nil), fuzzMember(f, secureConfig)
	for _, p := range seedPackets(plain) {
		f.Add(p, false)
		sealed, err := sealPayload(1, secure.config.Keyring.primaryAEAD(), p, []byte("fuzz"), nil)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(append(makeLabelHeader("fuzz", nil), sealed...), true)
	}
	f.Fuzz(func(t *testing.T, packet []byte, encrypted bool) {
		// Random node names accumulate; start over before the table
		// grows large enough to slow every input down.
		if plain.NumMembers() > maxFuzzNodes {
			plain = mustFuzzMember(t, nil)
		}
		if secure.NumMembers() > maxFuzzNodes {
			secure = mustFuzzMember(t, secureConfig)
		}
		if encrypted {
			ingestSync(secure, packet)
		} else {
			ingestSync(plain, packet)
		}
	})
}

// FuzzReadStream feeds arbitrary stream contents through the stream path:
// readStream (label-authenticated decryption for encrypted inputs,
// decompression, limits), then the push/pull state or user message decoder
// and the merge, which must never panic.
func FuzzReadStream(f *testing.F) {
	key := []byte("0123456789abcdef")
	plainConfig := func(c *Config) { c.Merge = acceptMerge{} }
	secureConfig := func(c *Config) {
		c.Merge = acceptMerge{}
		c.SecretKey = key
		c.Label = "fuzz"
	}
	plain, secure := fuzzMember(f, plainConfig), fuzzMember(f, secureConfig)

	// A joining push/pull with a live node (incarnation 1, so the merge
	// and the merge delegate run) and user state, and a user message.
	state := pushPullHeader{Nodes: 1, UserStateLen: 3, Join: true}.AppendMsgpack([]byte{byte(pushPullMsg)})
	state = pushNodeState{Name: "peer", Addr: []byte{10, 0, 0, 2}, Port: 7946, Incarnation: 1, Vsn: []uint8{1, 5, 2, 0, 0, 0}}.AppendMsgpack(state)
	state = append(state, "usr"...)
	user := append(userMsgHeader{UserMsgLen: 2}.AppendMsgpack([]byte{byte(userMsg)}), "hi"...)
	seeds := [][]byte{state, user}
	if c, err := compressPayload(state); err == nil {
		seeds = append(seeds, c)
	}
	for _, s := range seeds {
		f.Add(s, false)
		sealed, err := secure.encryptLocalState(s, "fuzz")
		if err != nil {
			f.Fatal(err)
		}
		f.Add(sealed, true)
	}

	f.Fuzz(func(t *testing.T, stream []byte, encrypted bool) {
		if plain.NumMembers() > maxFuzzNodes {
			plain = mustFuzzMember(t, plainConfig)
		}
		if secure.NumMembers() > maxFuzzNodes {
			secure = mustFuzzMember(t, secureConfig)
		}
		m, label := plain, ""
		if encrypted {
			m, label = secure, "fuzz"
		}
		r, w := net.Pipe()
		go func() {
			_, _ = w.Write(stream)
			_ = w.Close()
		}()
		defer func() { _ = r.Close() }()
		msgType, dec, err := m.readStream(r, label)
		if err != nil {
			return
		}
		switch msgType {
		case pushPullMsg:
			join, nodes, userState, err := m.readRemoteState(dec)
			if err == nil {
				_ = m.mergeRemoteState(join, nodes, userState)
			}
		case userMsg:
			_ = m.readUserMsg(dec)
		}
	})
}

// TestFuzzStreamSeedsReachMerge: the stream seeds, plain and encrypted,
// decode and merge their node (not only the rejection paths).
func TestFuzzStreamSeedsReachMerge(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		m := fuzzMember(t, func(c *Config) {
			if encrypted {
				c.SecretKey = []byte("0123456789abcdef")
				c.Label = "fuzz"
			}
		})
		state := pushPullHeader{Nodes: 1, Join: true}.AppendMsgpack([]byte{byte(pushPullMsg)})
		state = pushNodeState{Name: "peer", Addr: []byte{10, 0, 0, 2}, Port: 7946, Incarnation: 1, Vsn: []uint8{1, 5, 2, 0, 0, 0}}.AppendMsgpack(state)
		label := ""
		if encrypted {
			sealed, err := m.encryptLocalState(state, "fuzz")
			noErr(t, err)
			state, label = sealed, "fuzz"
		}
		msgType, dec, err := m.readStream(streamFrom(t, state), label)
		noErr(t, err)
		equal(t, pushPullMsg, msgType)
		join, nodes, user, err := m.readRemoteState(dec)
		noErr(t, err)
		noErr(t, m.mergeRemoteState(join, nodes, user))
		equal(t, StateAlive, m.getNodeState("peer"), "encrypted=%v", encrypted)
	}
}
