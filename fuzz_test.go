// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"log/slog"
	"net"
	"testing"
	"time"
)

// fuzzMember returns a live instance with no goroutines running: fuzz
// inputs are processed synchronously and reach the full state machine.
func fuzzMember(tb testing.TB, f func(*Config)) *Memberlist {
	tb.Helper()
	c := testConfig(tb)
	c.Transport = (&MockNetwork{}).NewTransport("local")
	c.Logger = slog.New(slog.DiscardHandler)
	if f != nil {
		f(c)
	}
	m, err := buildMemberlist(c)
	if err != nil {
		tb.Fatal(err)
	}
	if err := m.setAlive(nil); err != nil {
		tb.Fatal(err)
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
	newPlain := func() *Memberlist { return fuzzMember(f, nil) }
	newSecure := func() *Memberlist {
		return fuzzMember(f, func(c *Config) {
			c.SecretKey = key
			c.Label = "fuzz"
		})
	}
	plain, secure := newPlain(), newSecure()
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
			plain = newPlain()
		}
		if secure.NumMembers() > maxFuzzNodes {
			secure = newSecure()
		}
		if encrypted {
			ingestSync(secure, packet)
		} else {
			ingestSync(plain, packet)
		}
	})
}

// FuzzReadStream feeds arbitrary stream contents through the stream path:
// readStream (decryption, decompression, limits), then the push/pull state
// or user message decoder and the merge, which must never panic.
func FuzzReadStream(f *testing.F) {
	m := fuzzMember(f, func(c *Config) { c.Merge = acceptMerge{} })
	state := pushPullHeader{Nodes: 1, UserStateLen: 3}.AppendMsgpack([]byte{byte(pushPullMsg)})
	state = pushNodeState{Name: "peer", Addr: []byte{10, 0, 0, 2}, Port: 7946, Vsn: []uint8{1, 5, 2, 0, 0, 0}}.AppendMsgpack(state)
	state = append(state, "usr"...)
	f.Add(state)
	user := userMsgHeader{UserMsgLen: 2}.AppendMsgpack([]byte{byte(userMsg)})
	f.Add(append(user, "hi"...))
	if c, err := compressPayload(state); err == nil {
		f.Add(c)
	}
	f.Fuzz(func(t *testing.T, stream []byte) {
		if m.NumMembers() > maxFuzzNodes {
			m = fuzzMember(f, func(c *Config) { c.Merge = acceptMerge{} })
		}
		r, w := net.Pipe()
		go func() {
			_, _ = w.Write(stream)
			_ = w.Close()
		}()
		defer func() { _ = r.Close() }()
		msgType, dec, err := m.readStream(r, "")
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
