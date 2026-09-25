// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Command oracle checks internal/wire against the codec memberlist peers
// use — github.com/hashicorp/go-msgpack/v2 with a default MsgpackHandle —
// and regenerates ../golden.json, the vectors internal/wire's tests use.
// It lives under testdata so the go command ignores it: it is a separate
// module, and the only place Mori depends on go-msgpack.
//
// For boundary-biased random values of every message type it checks:
//
//  1. Encoding: internal/wire's bytes equal go-msgpack's bytes.
//  2. Decoding: both codecs decode go-msgpack's bytes to equal values.
//  3. Tolerance: for alternative encodings of the same message (other
//     integer widths, str8/bin families, shuffled keys, unknown keys with
//     nested values, duplicate keys, nil values), both codecs agree: equal
//     values, or both refuse.
//
// Usage, from this directory:
//
//	go run . [-n 20000] [-seed 1] [-golden ../golden.json]
//
// Run it after any change to internal/wire or internal/msgpack; a mismatch
// exits non-zero. Pass -golden /dev/null to check without rewriting.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"strings"

	"github.com/0xCarbon/mori/internal/msgpack"
	"github.com/0xCarbon/mori/internal/wire"
	"github.com/hashicorp/go-msgpack/v2/codec"
)

// Mirror structs: field names, types and tags exactly as mori v0.7.0
// net.go declared them for go-msgpack.
type ping struct {
	SeqNo      uint32
	Node       string
	SourceAddr []byte `codec:",omitempty"`
	SourcePort uint16 `codec:",omitempty"`
	SourceNode string `codec:",omitempty"`
}

type indirectPingReq struct {
	SeqNo      uint32
	Target     []byte
	Port       uint16
	Node       string
	Nack       bool
	SourceAddr []byte `codec:",omitempty"`
	SourcePort uint16 `codec:",omitempty"`
	SourceNode string `codec:",omitempty"`
}

type ackResp struct {
	SeqNo   uint32
	Payload []byte
}

type nackResp struct{ SeqNo uint32 }

type errResp struct{ Error string }

type suspect struct {
	Incarnation uint32
	Node        string
	From        string
}

type alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Vsn         []uint8
}

type dead struct {
	Incarnation uint32
	Node        string
	From        string
}

type pushPullHeader struct {
	Nodes        int
	UserStateLen int
	Join         bool
}

type userMsgHeader struct{ UserMsgLen int }

type nodeStateType int

type pushNodeState struct {
	Name        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Incarnation uint32
	State       nodeStateType
	Vsn         []uint8
}

type compressionType uint8

type compress struct {
	Algo compressionType
	Buf  []byte
}

// kind pairs a mirror type with its wire type.
type kind struct {
	name string
	gen  func(*gen) any                  // random mirror value
	wire func() interface{ wireMessage } // fresh wire value
	conv func(any) wire.Message          // mirror -> wire
	back func(wireMessage) any           // wire -> mirror
	zero func() any                      // fresh mirror pointer
}

type wireMessage interface {
	wire.Message
	DecodeMsgpack(*msgpack.Decoder) error
}

func goEncode(v any) []byte {
	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf, &codec.MsgpackHandle{}).Encode(v); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func goDecode(b []byte, out any) error {
	return codec.NewDecoder(bytes.NewReader(b), &codec.MsgpackHandle{}).Decode(out)
}

// gen draws boundary-biased random values.
type gen struct{ r *rand.Rand }

func (g gen) u64(max uint64) uint64 {
	edges := []uint64{0, 1, 31, 32, 127, 128, 255, 256, 32767, 32768, 65535, 65536, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32, math.MaxInt64, math.MaxUint64}
	if g.r.IntN(3) == 0 {
		e := edges[g.r.IntN(len(edges))]
		if e <= max {
			return e
		}
	}
	if max == math.MaxUint64 {
		return g.r.Uint64() >> g.r.IntN(64)
	}
	return g.r.Uint64N(max+1) >> g.r.IntN(64)
}

func (g gen) int() int {
	edges := []int64{0, 1, -1, 31, 32, -32, -33, 127, 128, -128, -129, 255, 256, 32767, 32768, -32768, -32769, 65535, 65536, math.MaxInt32, math.MinInt32, math.MaxInt32 + 1, math.MinInt32 - 1, math.MaxInt64, math.MinInt64}
	if g.r.IntN(3) == 0 {
		return int(edges[g.r.IntN(len(edges))])
	}
	v := int(g.r.Int64() >> g.r.IntN(64))
	if g.r.IntN(2) == 0 {
		v = -v
	}
	return v
}

func (g gen) length() int {
	edges := []int{0, 1, 15, 16, 31, 32, 33, 255, 256, 65535, 65536}
	if g.r.IntN(4) == 0 {
		n := edges[g.r.IntN(len(edges))]
		if n > 256 && g.r.IntN(20) != 0 {
			n = g.r.IntN(64) // large values are expensive; keep them rare
		}
		return n
	}
	return g.r.IntN(64)
}

func (g gen) bytes() []byte {
	switch g.r.IntN(6) {
	case 0:
		return nil
	case 1:
		return []byte{}
	}
	b := make([]byte, g.length())
	for i := range b {
		b[i] = byte(g.r.UintN(256))
	}
	return b
}

func (g gen) str() string {
	b := make([]byte, g.length())
	ascii := g.r.IntN(2) == 0
	for i := range b {
		if ascii {
			b[i] = byte(' ' + g.r.UintN(95))
		} else {
			b[i] = byte(g.r.UintN(256)) // names are not required to be UTF-8
		}
	}
	return string(b)
}

func (g gen) boolean() bool { return g.r.IntN(2) == 0 }

var kinds = []kind{
	{
		name: "Ping",
		gen: func(g *gen) any {
			return &ping{uint32(g.u64(math.MaxUint32)), g.str(), g.bytes(), uint16(g.u64(math.MaxUint16)), g.str()}
		},
		wire: func() interface{ wireMessage } { return &wire.Ping{} },
		conv: func(v any) wire.Message { p := v.(*ping); return wire.Ping(*p) },
		back: func(w wireMessage) any { p := ping(*w.(*wire.Ping)); return &p },
		zero: func() any { return &ping{} },
	},
	{
		name: "IndirectPingReq",
		gen: func(g *gen) any {
			return &indirectPingReq{uint32(g.u64(math.MaxUint32)), g.bytes(), uint16(g.u64(math.MaxUint16)), g.str(), g.boolean(), g.bytes(), uint16(g.u64(math.MaxUint16)), g.str()}
		},
		wire: func() interface{ wireMessage } { return &wire.IndirectPingReq{} },
		conv: func(v any) wire.Message { p := v.(*indirectPingReq); return wire.IndirectPingReq(*p) },
		back: func(w wireMessage) any { p := indirectPingReq(*w.(*wire.IndirectPingReq)); return &p },
		zero: func() any { return &indirectPingReq{} },
	},
	{
		name: "AckResp",
		gen:  func(g *gen) any { return &ackResp{uint32(g.u64(math.MaxUint32)), g.bytes()} },
		wire: func() interface{ wireMessage } { return &wire.AckResp{} },
		conv: func(v any) wire.Message { p := v.(*ackResp); return wire.AckResp(*p) },
		back: func(w wireMessage) any { p := ackResp(*w.(*wire.AckResp)); return &p },
		zero: func() any { return &ackResp{} },
	},
	{
		name: "NackResp",
		gen:  func(g *gen) any { return &nackResp{uint32(g.u64(math.MaxUint32))} },
		wire: func() interface{ wireMessage } { return &wire.NackResp{} },
		conv: func(v any) wire.Message { p := v.(*nackResp); return wire.NackResp(*p) },
		back: func(w wireMessage) any { p := nackResp(*w.(*wire.NackResp)); return &p },
		zero: func() any { return &nackResp{} },
	},
	{
		name: "ErrResp",
		gen:  func(g *gen) any { return &errResp{g.str()} },
		wire: func() interface{ wireMessage } { return &wire.ErrResp{} },
		conv: func(v any) wire.Message { p := v.(*errResp); return wire.ErrResp(*p) },
		back: func(w wireMessage) any { p := errResp(*w.(*wire.ErrResp)); return &p },
		zero: func() any { return &errResp{} },
	},
	{
		name: "Suspect",
		gen:  func(g *gen) any { return &suspect{uint32(g.u64(math.MaxUint32)), g.str(), g.str()} },
		wire: func() interface{ wireMessage } { return &wire.Suspect{} },
		conv: func(v any) wire.Message { p := v.(*suspect); return wire.Suspect(*p) },
		back: func(w wireMessage) any { p := suspect(*w.(*wire.Suspect)); return &p },
		zero: func() any { return &suspect{} },
	},
	{
		name: "Alive",
		gen: func(g *gen) any {
			return &alive{uint32(g.u64(math.MaxUint32)), g.str(), g.bytes(), uint16(g.u64(math.MaxUint16)), g.bytes(), g.bytes()}
		},
		wire: func() interface{ wireMessage } { return &wire.Alive{} },
		conv: func(v any) wire.Message { p := v.(*alive); return wire.Alive(*p) },
		back: func(w wireMessage) any { p := alive(*w.(*wire.Alive)); return &p },
		zero: func() any { return &alive{} },
	},
	{
		name: "Dead",
		gen:  func(g *gen) any { return &dead{uint32(g.u64(math.MaxUint32)), g.str(), g.str()} },
		wire: func() interface{ wireMessage } { return &wire.Dead{} },
		conv: func(v any) wire.Message { p := v.(*dead); return wire.Dead(*p) },
		back: func(w wireMessage) any { p := dead(*w.(*wire.Dead)); return &p },
		zero: func() any { return &dead{} },
	},
	{
		name: "PushPullHeader",
		gen:  func(g *gen) any { return &pushPullHeader{g.int(), g.int(), g.boolean()} },
		wire: func() interface{ wireMessage } { return &wire.PushPullHeader{} },
		conv: func(v any) wire.Message { p := v.(*pushPullHeader); return wire.PushPullHeader(*p) },
		back: func(w wireMessage) any { p := pushPullHeader(*w.(*wire.PushPullHeader)); return &p },
		zero: func() any { return &pushPullHeader{} },
	},
	{
		name: "UserMsgHeader",
		gen:  func(g *gen) any { return &userMsgHeader{g.int()} },
		wire: func() interface{ wireMessage } { return &wire.UserMsgHeader{} },
		conv: func(v any) wire.Message { p := v.(*userMsgHeader); return wire.UserMsgHeader(*p) },
		back: func(w wireMessage) any { p := userMsgHeader(*w.(*wire.UserMsgHeader)); return &p },
		zero: func() any { return &userMsgHeader{} },
	},
	{
		name: "PushNodeState",
		gen: func(g *gen) any {
			return &pushNodeState{g.str(), g.bytes(), uint16(g.u64(math.MaxUint16)), g.bytes(), uint32(g.u64(math.MaxUint32)), nodeStateType(g.int()), g.bytes()}
		},
		wire: func() interface{ wireMessage } { return &wire.PushNodeState{} },
		conv: func(v any) wire.Message {
			p := v.(*pushNodeState)
			return wire.PushNodeState{Name: p.Name, Addr: p.Addr, Port: p.Port, Meta: p.Meta, Incarnation: p.Incarnation, State: int(p.State), Vsn: p.Vsn}
		},
		back: func(w wireMessage) any {
			p := w.(*wire.PushNodeState)
			return &pushNodeState{p.Name, p.Addr, p.Port, p.Meta, p.Incarnation, nodeStateType(p.State), p.Vsn}
		},
		zero: func() any { return &pushNodeState{} },
	},
	{
		name: "Compress",
		gen:  func(g *gen) any { return &compress{compressionType(g.u64(math.MaxUint8)), g.bytes()} },
		wire: func() interface{ wireMessage } { return &wire.Compress{} },
		conv: func(v any) wire.Message { p := v.(*compress); return wire.Compress{Algo: uint8(p.Algo), Buf: p.Buf} },
		back: func(w wireMessage) any { p := w.(*wire.Compress); return &compress{compressionType(p.Algo), p.Buf} },
		zero: func() any { return &compress{} },
	},
}

// golden is one captured vector for internal/wire's tests.
type golden struct {
	Type  string          `json:"type"`
	Hex   string          `json:"hex"`
	Value json.RawMessage `json:"value"`
}

func main() {
	n := flag.Int("n", 20000, "random messages per type")
	seed := flag.Uint64("seed", 1, "random seed")
	goldenPath := flag.String("golden", "../golden.json", "golden vector output")
	flag.Parse()

	g := &gen{rand.New(rand.NewPCG(*seed, *seed^0x9e3779b97f4a7c15))}
	var goldens []golden
	failures := 0
	fail := func(format string, args ...any) {
		failures++
		if failures <= 20 {
			fmt.Printf("FAIL "+format+"\n", args...)
		}
	}
	stats := map[string][3]int{}
	captured := map[string]int{}

	for _, k := range kinds {
		var st [3]int
		for range *n {
			v := k.gen(g)

			// 1. Encoding equality.
			want := goEncode(v)
			got := k.conv(v).AppendMsgpack(nil)
			if !bytes.Equal(want, got) {
				fail("%s encode: go-msgpack %x, wire %x (value %+v)", k.name, want, got, v)
				continue
			}
			st[0]++

			// 2. Both decoders agree on go-msgpack's bytes.
			goVal := k.zero()
			if err := goDecode(want, goVal); err != nil {
				fail("%s: go-msgpack cannot decode its own bytes: %v", k.name, err)
				continue
			}
			w := k.wire()
			if err := w.DecodeMsgpack(msgpack.NewDecoder(want)); err != nil {
				fail("%s decode: %v (bytes %x)", k.name, err, want)
				continue
			}
			if !reflect.DeepEqual(k.back(w), goVal) {
				fail("%s decode: wire %+v, go-msgpack %+v", k.name, k.back(w), goVal)
				continue
			}
			st[1]++

			// 3. Alternative encodings: both agree.
			alt := reencode(g, want)
			goAlt := k.zero()
			goErr := goDecode(alt, goAlt)
			wAlt := k.wire()
			wErr := wAlt.DecodeMsgpack(msgpack.NewDecoder(alt))
			switch {
			case goErr == nil && wErr == nil:
				if !reflect.DeepEqual(k.back(wAlt), goAlt) {
					fail("%s alternate: wire %+v, go-msgpack %+v (bytes %x)", k.name, k.back(wAlt), goAlt, alt)
					continue
				}
			case goErr != nil && wErr != nil:
			default:
				fail("%s alternate: go-msgpack err %v, wire err %v (bytes %x)", k.name, goErr, wErr, alt)
				continue
			}
			st[2]++

			// Golden vectors need a lossless JSON value: JSON replaces
			// invalid UTF-8, so keep only values that round-trip.
			if captured[k.name] < 12 {
				val, err := json.Marshal(v)
				if err != nil {
					panic(err)
				}
				back := k.zero()
				if json.Unmarshal(val, back) == nil && reflect.DeepEqual(back, v) {
					captured[k.name]++
					goldens = append(goldens, golden{Type: k.name, Hex: hex.EncodeToString(want), Value: val})
				}
			}
		}
		stats[k.name] = st
	}

	for _, k := range kinds {
		st := stats[k.name]
		fmt.Printf("%-16s encode=%d decode=%d alternate=%d\n", k.name, st[0], st[1], st[2])
	}
	if failures > 0 {
		fmt.Printf("FAILED: %d mismatches\n", failures)
		os.Exit(1)
	}
	// One vector per line: compact and reviewable in diffs.
	var out []byte
	for i, g := range goldens {
		line, err := json.Marshal(g)
		if err != nil {
			panic(err)
		}
		if i == 0 {
			out = append(out, "[\n"...)
		} else {
			out = append(out, ",\n"...)
		}
		out = append(out, line...)
	}
	out = append(out, "\n]\n"...)
	if err := os.WriteFile(*goldenPath, out, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("PASS: %d vectors written to %s\n", len(goldens), *goldenPath)
}

// reencode rewrites a go-msgpack map encoding with equivalent alternatives
// chosen at random: integer widths, str8/bin families for raw values,
// nil for zero values, shuffled entries, unknown keys with nested values
// and duplicate keys (the last one wins in both codecs).
func reencode(g *gen, b []byte) []byte {
	d := msgpack.NewDecoder(b)
	n, err := d.ReadMapHeader()
	if err != nil {
		panic(err)
	}
	type entry struct{ key, val []byte }
	var entries []entry
	for range n {
		k, err := d.ReadKey()
		if err != nil {
			panic(err)
		}
		start := len(b) - d.Remaining()
		if err := d.Skip(); err != nil {
			panic(err)
		}
		val := b[start : len(b)-d.Remaining()]
		entries = append(entries, entry{append([]byte(nil), k...), altValue(g, val)})
	}
	g.r.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
	if g.r.IntN(3) == 0 {
		entries = append(entries, entry{[]byte("Unknown" + strings.Repeat("x", g.r.IntN(80))), randomValue(g, 0)})
	}
	if len(entries) > 0 && g.r.IntN(4) == 0 {
		// Duplicate a key with a first value of a compatible kind; the
		// original, later entry must win.
		e := entries[g.r.IntN(len(entries))]
		entries = append([]entry{{e.key, altValue(g, e.val)}}, entries...)
	}
	var out []byte
	switch {
	case len(entries) < 16 && g.r.IntN(2) == 0:
		out = append(out, 0x80|byte(len(entries)))
	case g.r.IntN(2) == 0:
		out = append(out, 0xde, byte(len(entries)>>8), byte(len(entries)))
	default:
		out = append(out, 0xdf, 0, 0, byte(len(entries)>>8), byte(len(entries)))
	}
	for _, e := range entries {
		out = appendRaw(g, out, e.key, g.r.IntN(2) == 0)
		out = append(out, e.val...)
	}
	return out
}

// altValue returns an equivalent encoding of one scalar value.
func altValue(g *gen, v []byte) []byte {
	c := v[0]
	d := msgpack.NewDecoder(v)
	switch {
	case c == 0xc2 || c == 0xc3:
		if g.r.IntN(3) == 0 {
			return []byte{c - 0xc2} // fixint 0/1
		}
		return v
	case c <= 0x7f || c >= 0xe0 || (c >= 0xcc && c <= 0xd3):
		i, err := d.ReadInt(math.MinInt64, math.MaxInt64)
		if err != nil { // above MaxInt64: keep
			return v
		}
		return appendIntAlt(g, i)
	case (c >= 0xa0 && c <= 0xbf) || c == 0xd9 || c == 0xda || c == 0xdb || (c >= 0xc4 && c <= 0xc6):
		b, err := d.ReadBytes(math.MaxInt32)
		if err != nil {
			panic(err)
		}
		if len(b) == 0 && g.r.IntN(4) == 0 {
			return []byte{0xc0} // nil decodes to the zero value
		}
		return appendRaw(g, nil, b, g.r.IntN(2) == 0)
	}
	return v
}

// appendIntAlt encodes i in a randomly chosen format wide enough to hold it.
func appendIntAlt(g *gen, i int64) []byte {
	type f struct {
		c    byte
		n    int
		fits bool
	}
	u := uint64(i)
	opts := []f{
		{0x00, 0, i >= 0 && i <= 127},
		{0xe0, 0, i >= -32 && i < 0},
		{0xcc, 1, i >= 0 && i <= math.MaxUint8},
		{0xcd, 2, i >= 0 && i <= math.MaxUint16},
		{0xce, 4, i >= 0 && i <= math.MaxUint32},
		{0xcf, 8, i >= 0},
		{0xd0, 1, i >= math.MinInt8 && i <= math.MaxInt8},
		{0xd1, 2, i >= math.MinInt16 && i <= math.MaxInt16},
		{0xd2, 4, i >= math.MinInt32 && i <= math.MaxInt32},
		{0xd3, 8, true},
	}
	var fit []f
	for _, o := range opts {
		if o.fits {
			fit = append(fit, o)
		}
	}
	o := fit[g.r.IntN(len(fit))]
	if o.n == 0 {
		return []byte{byte(i)}
	}
	out := []byte{o.c}
	for k := o.n - 1; k >= 0; k-- {
		out = append(out, byte(u>>(8*k)))
	}
	return out
}

// appendRaw encodes b with a random valid str or bin header.
func appendRaw(g *gen, out, b []byte, bin bool) []byte {
	n := len(b)
	switch {
	case bin && n < 256 && g.r.IntN(2) == 0:
		out = append(out, 0xc4, byte(n))
	case bin && n < 65536 && g.r.IntN(2) == 0:
		out = append(out, 0xc5, byte(n>>8), byte(n))
	case bin:
		out = append(out, 0xc6, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	case n < 32 && g.r.IntN(2) == 0:
		out = append(out, 0xa0|byte(n))
	case n < 256 && g.r.IntN(2) == 0:
		out = append(out, 0xd9, byte(n))
	case n < 65536 && g.r.IntN(2) == 0:
		out = append(out, 0xda, byte(n>>8), byte(n))
	default:
		out = append(out, 0xdb, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(out, b...)
}

// randomValue returns a random MessagePack value for an unknown field.
func randomValue(g *gen, depth int) []byte {
	switch k := g.r.IntN(8); {
	case k == 0 && depth < 3:
		n := g.r.IntN(4)
		out := []byte{0x90 | byte(n)}
		for range n {
			out = append(out, randomValue(g, depth+1)...)
		}
		return out
	case k == 1 && depth < 3:
		n := g.r.IntN(4)
		out := []byte{0x80 | byte(n)}
		for range n {
			out = appendRaw(g, out, []byte(fmt.Sprint("k", g.r.IntN(9))), false)
			out = append(out, randomValue(g, depth+1)...)
		}
		return out
	case k == 2:
		return []byte{0xcb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18} // float64 pi
	case k == 3:
		return []byte{0xc0}
	case k == 4:
		return appendRaw(g, nil, []byte("some text"), g.r.IntN(2) == 0)
	default:
		return appendIntAlt(g, int64(g.int()))
	}
}
