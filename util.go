// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"compress/lzw"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xCarbon/mori/internal/msgpack"
	"github.com/0xCarbon/mori/internal/wire"
)

// goid returns the current goroutine's id, parsed from the runtime stack
// header ("goroutine 123 [running]:"). Used only to detect re-entrant
// Shutdown from a joined goroutine — never for synchronization.
func goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := buf[len("goroutine "):n]
	id := uint64(0)
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	return id
}

// pushPullScale is the minimum number of nodes
// before we start scaling the push/pull timing. The scale
// effect is the log2(Nodes) - log2(pushPullScale). This means
// that the 33rd node will cause us to double the interval,
// while the 65th will triple it.
const pushPullScaleThreshold = 32

const (
	// Constant litWidth 2-8
	lzwLitWidth = 8
)

// messageDecoder is implemented by pointers to wire messages.
type messageDecoder interface {
	DecodeMsgpack(*msgpack.Decoder) error
}

// decode reverses encode's MessagePack part on a byte slice.
func decode(buf []byte, out messageDecoder) error {
	return out.DecodeMsgpack(msgpack.NewDecoder(buf))
}

// encode returns the message type byte followed by msg's encoding.
func encode(msgType messageType, msg wire.Message) []byte {
	return msg.AppendMsgpack(append(make([]byte, 0, 64), byte(msgType)))
}

// randomOffset returns a uniformly random offset in [0, n), or 0 when n is
// not positive.
func randomOffset(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

// suspicionTimeout computes the timeout that should be used when
// a node is suspected
func suspicionTimeout(suspicionMult, n int, interval time.Duration) time.Duration {
	nodeScale := math.Max(1.0, math.Log10(math.Max(1.0, float64(n))))
	// multiply by 1000 to keep some precision because time.Duration is an int64 type
	timeout := time.Duration(suspicionMult) * time.Duration(nodeScale*1000) * interval / 1000
	return timeout
}

// retransmitLimit computes the limit of retransmissions
func retransmitLimit(retransmitMult, n int) int {
	nodeScale := math.Ceil(math.Log10(float64(n + 1)))
	limit := retransmitMult * int(nodeScale)
	return limit
}

// shuffleNodes randomly shuffles the input nodes using the Fisher-Yates shuffle
func shuffleNodes(nodes []*nodeState) {
	n := len(nodes)
	rand.Shuffle(n, func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

// pushPushScale is used to scale the time interval at which push/pull
// syncs take place. It is used to prevent network saturation as the
// cluster size grows
func pushPullScale(interval time.Duration, n int) time.Duration {
	// Don't scale until we cross the threshold
	if n <= pushPullScaleThreshold {
		return interval
	}

	multiplier := math.Ceil(math.Log2(float64(n))-math.Log2(pushPullScaleThreshold)) + 1.0
	return time.Duration(multiplier) * interval
}

// moveDeadNodes moves dead and left nodes that that have not changed during the gossipToTheDeadTime interval
// to the end of the slice and returns the index of the first moved node.
func moveDeadNodes(nodes []*nodeState, gossipToTheDeadTime time.Duration) int {
	numDead := 0
	n := len(nodes)
	for i := 0; i < n-numDead; i++ {
		if !nodes[i].DeadOrLeft() {
			continue
		}

		// Respect the gossip to the dead interval
		if time.Since(nodes[i].StateChange) <= gossipToTheDeadTime {
			continue
		}

		// Move this node to the end
		nodes[i], nodes[n-numDead-1] = nodes[n-numDead-1], nodes[i]
		numDead++
		i--
	}
	return n - numDead
}

// kRandomNodes is used to select up to k random Nodes, excluding any nodes where
// the exclude function returns true. It is possible that less than k nodes are
// returned.
func kRandomNodes(k int, nodes []*nodeState, exclude func(*nodeState) bool) []Node {
	n := len(nodes)
	kNodes := make([]Node, 0, k)

	// When n is very small (ex. 3-5 node Raft clusters), we can easily miss
	// non-excluded nodes with random selection, so instead shuffle the slice
	// to ensure the search is exhaustive
	if n < k*3 {
		nodes := slices.Clone(nodes)
		shuffleNodes(nodes)
		for idx := 0; idx < n && len(kNodes) < k; idx++ {
			state := nodes[idx]
			if exclude != nil && exclude(state) {
				continue
			}
			kNodes = append(kNodes, state.Node)
		}
		return kNodes
	}

OUTER:
	// Probe up to 3*n times, with large n this is not necessary since k << n,
	// but when n >= k*3 but still not "large", we want to give the search a shot
	// at being exhaustive
	for i := 0; i < 3*n && len(kNodes) < k; i++ {
		idx := randomOffset(n)
		state := nodes[idx]

		if exclude != nil && exclude(state) {
			continue OUTER
		}

		// Check if we have this node already
		for j := 0; j < len(kNodes); j++ {
			if state.Name == kNodes[j].Name {
				continue OUTER
			}
		}

		kNodes = append(kNodes, state.Node)
	}
	return kNodes
}

// makeCompoundMessages takes a list of messages and packs
// them into one or multiple messages based on the limitations
// of compound messages (255 messages each).
func makeCompoundMessages(msgs [][]byte) [][]byte {
	const maxMsgs = 255
	out := make([][]byte, 0, (len(msgs)+(maxMsgs-1))/maxMsgs)

	for ; len(msgs) > maxMsgs; msgs = msgs[maxMsgs:] {
		out = append(out, makeCompoundMessage(msgs[:maxMsgs]))
	}
	if len(msgs) > 0 {
		out = append(out, makeCompoundMessage(msgs))
	}
	return out
}

// makeCompoundMessage takes a list of messages and generates
// a single compound message containing all of them: the type, the part
// count, a big-endian uint16 length per part, then the parts.
func makeCompoundMessage(msgs [][]byte) []byte {
	size := 2 + 2*len(msgs)
	for _, m := range msgs {
		size += len(m)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, uint8(compoundMsg), uint8(len(msgs)))
	for _, m := range msgs {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
	}
	for _, m := range msgs {
		buf = append(buf, m...)
	}
	return buf
}

// decodeCompoundMessage splits a compound message and returns
// the slices of individual messages. Also returns the number
// of truncated messages and any potential error
func decodeCompoundMessage(buf []byte) (trunc int, parts [][]byte, err error) {
	if len(buf) < 1 {
		err = fmt.Errorf("missing compound length byte")
		return
	}
	numParts := int(buf[0])
	buf = buf[1:]

	// Check we have enough bytes
	if len(buf) < numParts*2 {
		err = fmt.Errorf("truncated len slice")
		return
	}

	// Decode the lengths
	lengths := make([]uint16, numParts)
	for i := range numParts {
		lengths[i] = binary.BigEndian.Uint16(buf[i*2 : i*2+2])
	}
	buf = buf[numParts*2:]

	// Split each message
	for idx, msgLen := range lengths {
		if len(buf) < int(msgLen) {
			trunc = numParts - idx
			return
		}

		// Extract the slice, seek past on the buffer
		slice := buf[:msgLen]
		buf = buf[msgLen:]
		parts = append(parts, slice)
	}
	return
}

// maxIncompressiblePacket is the largest message LZW compression can never
// shrink, so rawSendMsgPacket skips compressing it: its result would be
// discarded anyway. Bound: the i-th LZW code covers at most i bytes, so n
// bytes need k codes with k(k+1)/2 >= n, each 9 bits wide plus a 9-bit end
// code, inside a 13-byte compress envelope (type, map header, "Algo",
// algorithm, "Buf", string header): 13 + ceil(9(k+1)/8) >= n for n <= 22.
const maxIncompressiblePacket = 22

// LZW coders are pooled: each allocates tables of tens of kilobytes, which
// used to dominate the cost of sending a compressed packet.
var (
	lzwWriters = sync.Pool{New: func() any { return lzw.NewWriter(nil, lzw.LSB, lzwLitWidth) }}
	lzwReaders = sync.Pool{New: func() any { return lzw.NewReader(nil, lzw.LSB, lzwLitWidth) }}
)

// compressPayload takes an opaque input buffer, compresses it
// and wraps it in a compress{} message that is encoded.
func compressPayload(inp []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(inp)/2 + 16)
	w := lzwWriters.Get().(*lzw.Writer)
	w.Reset(&buf, lzw.LSB, lzwLitWidth)
	defer lzwWriters.Put(w)

	if _, err := w.Write(inp); err != nil {
		return nil, err
	}
	// Ensure we flush everything out
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Create a compressed message
	c := compress{
		Algo: uint8(lzwAlgo),
		Buf:  buf.Bytes(),
	}
	return encode(compressMsg, c), nil
}

// decompressPayload is used to unpack an encoded compress{}
// message and return its payload uncompressed
func decompressPayload(msg []byte) ([]byte, error) {
	// Decode the message
	var c compress
	if err := decode(msg, &c); err != nil {
		return nil, err
	}
	return decompressBuffer(&c, maxPacketDecompressedBytes)
}

// decompressBuffer is used to decompress the buffer of
// a single compress message, handling multiple algorithms. The output may
// not exceed limit bytes.
func decompressBuffer(c *compress, limit int) ([]byte, error) {
	// Verify the algorithm
	if compressionType(c.Algo) != lzwAlgo {
		return nil, fmt.Errorf("cannot decompress unknown algorithm %d", c.Algo)
	}

	r := lzwReaders.Get().(*lzw.Reader)
	r.Reset(bytes.NewReader(c.Buf), lzw.LSB, lzwLitWidth)
	defer func() {
		_ = r.Close()
		lzwReaders.Put(r)
	}()

	// Read at most one byte past the limit, so an oversized payload is
	// detected without being materialized.
	var b bytes.Buffer
	b.Grow(min(4*len(c.Buf), limit+1))
	n, err := io.CopyN(&b, r, int64(limit)+1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n > int64(limit) {
		return nil, fmt.Errorf("decompressed message is larger than limit (%d)", limit)
	}

	// Return the uncompressed bytes
	return b.Bytes(), nil
}

// joinHostPort returns the host:port form of an address, for use with a
// transport.
func joinHostPort(host string, port uint16) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// hasPort is given a string of the form "host", "host:port", "ipv6::address",
// or "[ipv6::address]:port", and returns true if the string includes a port.
func hasPort(s string) bool {
	// IPv6 address in brackets.
	if strings.LastIndex(s, "[") == 0 {
		return strings.LastIndex(s, ":") > strings.LastIndex(s, "]")
	}

	// Otherwise the presence of a single colon determines if there's a port
	// since IPv6 addresses outside of brackets (count > 1) can't have a
	// port.
	return strings.Count(s, ":") == 1
}

// ensurePort makes sure the given string has a port number on it, otherwise it
// appends the given port as a default.
func ensurePort(s string, port int) string {
	if hasPort(s) {
		return s
	}

	// If this is an IPv6 address, the join call will add another set of
	// brackets, so we have to trim before we add the default port.
	s = strings.Trim(s, "[]")
	s = net.JoinHostPort(s, strconv.Itoa(port))
	return s
}
