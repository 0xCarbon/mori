// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Package benchmarks measures Mori's hot paths through the public API only,
// so the same file compiles against earlier releases for A/B comparisons
// (see project/evidence/w7-perf).
package benchmarks

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xCarbon/mori"
)

// appendAlive hand-encodes an alive message (type byte 4, then a msgpack
// map with sorted keys, as every memberlist peer sends it), so the
// benchmark does not depend on either codec under comparison.
func appendAlive(b []byte, node string, addr net.IP, port uint16, inc uint32, meta []byte) []byte {
	str := func(b []byte, s string) []byte {
		if len(s) < 32 {
			b = append(b, 0xa0|byte(len(s)))
		} else {
			b = append(b, 0xda, byte(len(s)>>8), byte(len(s)))
		}
		return append(b, s...)
	}
	uint := func(b []byte, v uint32) []byte {
		switch {
		case v < 128:
			return append(b, byte(v))
		case v < 256:
			return append(b, 0xcc, byte(v))
		case v < 65536:
			return append(b, 0xcd, byte(v>>8), byte(v))
		}
		return binary.BigEndian.AppendUint32(append(b, 0xce), v)
	}
	b = append(b, 4, 0x86) // aliveMsg, fixmap with 6 entries
	b = str(b, "Addr")
	b = str(b, string(addr))
	b = str(b, "Incarnation")
	b = uint(b, inc)
	b = str(b, "Meta")
	b = str(b, string(meta))
	b = str(b, "Node")
	b = str(b, node)
	b = str(b, "Port")
	b = uint(b, uint32(port))
	b = str(b, "Vsn")
	return str(b, string([]byte{1, 5, 2, 0, 0, 0}))
}

// feedTransport is a Transport whose packets come from the benchmark.
type feedTransport struct {
	packets chan *mori.Packet
	streams chan net.Conn
}

func (t *feedTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	return net.IPv4(10, 0, 0, 1), 7946, nil
}
func (t *feedTransport) WriteTo(b []byte, addr string) (time.Time, error) { return time.Now(), nil }
func (t *feedTransport) PacketCh() <-chan *mori.Packet                    { return t.packets }
func (t *feedTransport) DialTimeout(string, time.Duration) (net.Conn, error) {
	return nil, fmt.Errorf("no streams")
}
func (t *feedTransport) StreamCh() <-chan net.Conn { return t.streams }
func (t *feedTransport) Shutdown() error           { return nil }

// countingEvents counts NotifyUpdate deliveries.
type countingEvents struct{ updates atomic.Int64 }

func (e *countingEvents) NotifyJoin(*mori.Node)   {}
func (e *countingEvents) NotifyLeave(*mori.Node)  {}
func (e *countingEvents) NotifyUpdate(*mori.Node) { e.updates.Add(1) }

// BenchmarkPacketIngestAlive measures the receive path of one gossip packet
// end to end: packet listener, envelope handling, decode, alive-state
// update and the resulting NotifyUpdate delivery. Each op is one alive
// message carrying a new incarnation and new meta for a known peer.
func BenchmarkPacketIngestAlive(b *testing.B) {
	tr := &feedTransport{packets: make(chan *mori.Packet, 1024), streams: make(chan net.Conn)}
	events := &countingEvents{}
	c := mori.DefaultLANConfig()
	c.Name = "local"
	c.Transport = tr
	c.Events = events
	c.ProbeInterval = time.Hour
	c.GossipInterval = time.Hour
	c.PushPullInterval = 0
	c.EnableCompression = false
	m, err := mori.Create(c)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = m.Shutdown() }()

	from := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 7946}
	peer := net.IPv4(10, 0, 0, 2).To4()
	metas := [2][]byte{[]byte("role=api,zone=a"), []byte("role=api,zone=b")}
	packets := make([][]byte, b.N+1)
	for i := range packets {
		packets[i] = appendAlive(nil, "peer", peer, 7946, uint32(i+1), metas[i%2])
	}
	tr.packets <- &mori.Packet{Buf: packets[0], From: from, Timestamp: time.Now()}
	for events.updates.Load() == 0 && len(m.Members()) < 2 {
		time.Sleep(time.Millisecond)
	}
	base := events.updates.Load()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		tr.packets <- &mori.Packet{Buf: packets[i], From: from, Timestamp: time.Now()}
		// One packet in flight at a time: the handler drops messages when
		// its queue overflows, and a pipelined benchmark would measure
		// drops, not work.
		for events.updates.Load()-base < int64(i) {
		}
	}
}

// namedBroadcast is a NamedBroadcast of a fixed size.
type namedBroadcast struct {
	name string
	msg  []byte
}

func (b *namedBroadcast) Invalidates(o mori.Broadcast) bool {
	nb, ok := o.(mori.NamedBroadcast)
	return ok && nb.Name() == b.name
}
func (b *namedBroadcast) Name() string    { return b.name }
func (b *namedBroadcast) Message() []byte { return b.msg }
func (b *namedBroadcast) Finished()       {}

// BenchmarkBroadcastQueue measures the gossip queue at cluster scale: each
// op queues a named broadcast (invalidating an older one about the same
// node when present) and fills one 1400-byte packet from the queue.
func BenchmarkBroadcastQueue(b *testing.B) {
	for _, nodes := range []int{100, 10_000} {
		b.Run(strconv.Itoa(nodes), func(b *testing.B) {
			q := &mori.TransmitLimitedQueue{NumNodes: func() int { return nodes }, RetransmitMult: 4}
			msgs := make([]*namedBroadcast, nodes)
			for i := range msgs {
				msgs[i] = &namedBroadcast{name: fmt.Sprint("node-", i), msg: make([]byte, 60+i%64)}
				q.QueueBroadcast(msgs[i])
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				q.QueueBroadcast(msgs[i%nodes])
				if len(q.GetBroadcasts(2, 1400)) == 0 {
					b.Fatal("empty packet")
				}
			}
		})
	}
}

// BenchmarkNetTransportReceive measures NetTransport's UDP receive path per
// packet: socket read, packet allocation and channel delivery.
func BenchmarkNetTransportReceive(b *testing.B) {
	for _, size := range []int{64, 1400} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			t, err := mori.NewNetTransport(&mori.NetTransportConfig{BindAddrs: []string{"127.0.0.1"}})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = t.Shutdown() }()
			dst := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: t.GetAutoBindPort()}
			conn, err := net.DialUDP("udp", nil, dst)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := conn.Write(payload); err != nil {
					b.Fatal(err)
				}
				<-t.PacketCh()
			}
		})
	}
}
