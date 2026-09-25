// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/0xCarbon/mori/internal/msgpack"
)

type codecMessage interface {
	Message
	DecodeMsgpack(*msgpack.Decoder) error
}

// newMessage returns a zero message of the named type.
func newMessage(t testing.TB, name string) codecMessage {
	switch name {
	case "Ping":
		return &Ping{}
	case "IndirectPingReq":
		return &IndirectPingReq{}
	case "AckResp":
		return &AckResp{}
	case "NackResp":
		return &NackResp{}
	case "ErrResp":
		return &ErrResp{}
	case "Suspect":
		return &Suspect{}
	case "Alive":
		return &Alive{}
	case "Dead":
		return &Dead{}
	case "PushPullHeader":
		return &PushPullHeader{}
	case "UserMsgHeader":
		return &UserMsgHeader{}
	case "PushNodeState":
		return &PushNodeState{}
	case "Compress":
		return &Compress{}
	}
	t.Fatalf("unknown message type %q", name)
	return nil
}

type golden struct {
	Type  string          `json:"type"`
	Hex   string          `json:"hex"`
	Value json.RawMessage `json:"value"`
}

// loadGolden reads the vectors captured from go-msgpack by
// project/evidence/w5-wire (the independent oracle).
func loadGolden(t testing.TB) []golden {
	data, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []golden
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors) < 12*12 {
		t.Fatalf("only %d golden vectors", len(vectors))
	}
	return vectors
}

// omitEmptySource mirrors go-msgpack's omitempty: an empty source address
// is not encoded, so it decodes as nil.
func omitEmptySource(m any) {
	switch p := m.(type) {
	case *Ping:
		if len(p.SourceAddr) == 0 {
			p.SourceAddr = nil
		}
	case *IndirectPingReq:
		if len(p.SourceAddr) == 0 {
			p.SourceAddr = nil
		}
	}
}

// TestGoldenVectors pins encoding and decoding to bytes produced by
// go-msgpack v2's default MsgpackHandle.
func TestGoldenVectors(t *testing.T) {
	for i, v := range loadGolden(t) {
		want, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatal(err)
		}
		m := newMessage(t, v.Type)
		if err := json.Unmarshal(v.Value, m); err != nil {
			t.Fatalf("vector %d: %v", i, err)
		}
		if got := m.AppendMsgpack(nil); !bytes.Equal(got, want) {
			t.Fatalf("vector %d (%s): encoded %x, go-msgpack %x", i, v.Type, got, want)
		}
		omitEmptySource(m)
		dec := newMessage(t, v.Type)
		if err := dec.DecodeMsgpack(msgpack.NewDecoder(want)); err != nil {
			t.Fatalf("vector %d (%s): %v", i, v.Type, err)
		}
		if !reflect.DeepEqual(dec, m) {
			t.Fatalf("vector %d (%s): decoded %+v, want %+v", i, v.Type, dec, m)
		}
	}
}

// TestEncodedKeysSorted: go-msgpack writes struct fields sorted by name.
func TestEncodedKeysSorted(t *testing.T) {
	for _, v := range loadGolden(t) {
		m := newMessage(t, v.Type)
		if err := json.Unmarshal(v.Value, m); err != nil {
			t.Fatal(err)
		}
		d := msgpack.NewDecoder(m.AppendMsgpack(nil))
		n, err := d.ReadMapHeader()
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for range n {
			k, err := d.ReadKey()
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, string(k))
			if err := d.Skip(); err != nil {
				t.Fatal(err)
			}
		}
		if !slices.IsSorted(keys) {
			t.Fatalf("%s keys %v are not sorted", v.Type, keys)
		}
	}
}

// TestTruncatedMessagesFail: every strict prefix of a message is an error.
func TestTruncatedMessagesFail(t *testing.T) {
	for _, v := range loadGolden(t) {
		full, _ := hex.DecodeString(v.Hex)
		for n := range len(full) {
			if err := newMessage(t, v.Type).DecodeMsgpack(msgpack.NewDecoder(full[:n])); err == nil {
				t.Fatalf("%s: %d-byte prefix of %x decoded", v.Type, n, full)
			}
		}
	}
}

func TestDecodeResetsMessage(t *testing.T) {
	m := &Alive{Node: "stale", Meta: []byte("stale"), Port: 9}
	in := Alive{Node: "fresh"}.AppendMsgpack(nil)
	if err := m.DecodeMsgpack(msgpack.NewDecoder(in)); err != nil {
		t.Fatal(err)
	}
	if m.Node != "fresh" || m.Meta != nil || m.Port != 0 {
		t.Fatalf("decode kept stale fields: %+v", m)
	}
}

func TestFieldLimits(t *testing.T) {
	big := make([]byte, MaxFieldBytes+1)
	if err := new(Alive).DecodeMsgpack(msgpack.NewDecoder(Alive{Meta: big}.AppendMsgpack(nil))); err == nil {
		t.Fatal("meta above MaxFieldBytes decoded")
	}
	if err := new(Compress).DecodeMsgpack(msgpack.NewDecoder(Compress{Buf: big}.AppendMsgpack(nil))); err != nil {
		t.Fatalf("compressed buffer within MaxCompressedBytes refused: %v", err)
	}
	var tooWide []byte
	tooWide = msgpack.AppendMapHeader(tooWide, 1)
	tooWide = msgpack.AppendString(tooWide, "Port")
	tooWide = msgpack.AppendUint(tooWide, 65536)
	if err := new(Alive).DecodeMsgpack(msgpack.NewDecoder(tooWide)); err == nil {
		t.Fatal("port 65536 decoded")
	}
}

// TestDecodeAllocations: key dispatch allocates nothing; only non-empty
// string and byte fields allocate.
func TestDecodeAllocations(t *testing.T) {
	in := Alive{Incarnation: 7, Node: "node-1", Addr: []byte{10, 0, 0, 1}, Port: 7946, Meta: []byte("m"), Vsn: []byte{1, 5, 2, 0, 0, 0}}.AppendMsgpack(nil)
	var m Alive
	var d msgpack.Decoder
	allocs := testing.AllocsPerRun(200, func() {
		d.Reset(in)
		if err := m.DecodeMsgpack(&d); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 4 { // Node, Addr, Meta, Vsn
		t.Fatalf("decoding an alive message allocated %v times, want <= 4", allocs)
	}
	var out []byte
	allocs = testing.AllocsPerRun(200, func() { out = m.AppendMsgpack(out[:0]) })
	if allocs != 0 {
		t.Fatalf("encoding into a reused buffer allocated %v times", allocs)
	}
}

func FuzzDecodeMessages(f *testing.F) {
	for _, v := range loadGolden(f) {
		b, _ := hex.DecodeString(v.Hex)
		f.Add(v.Type, b)
	}
	f.Fuzz(func(t *testing.T, name string, in []byte) {
		switch name {
		case "Ping", "IndirectPingReq", "AckResp", "NackResp", "ErrResp", "Suspect", "Alive", "Dead", "PushPullHeader", "UserMsgHeader", "PushNodeState", "Compress":
		default:
			return
		}
		m := newMessage(t, name)
		if err := m.DecodeMsgpack(msgpack.NewDecoder(in)); err != nil {
			return
		}
		// A decoded message re-encodes canonically and decodes back to
		// itself.
		enc := m.AppendMsgpack(nil)
		again := newMessage(t, name)
		if err := again.DecodeMsgpack(msgpack.NewDecoder(enc)); err != nil {
			t.Fatalf("re-encoded %s does not decode: %v", name, err)
		}
		omitEmptySource(m)
		if !reflect.DeepEqual(again, m) {
			t.Fatalf("%s round trip: %+v != %+v", name, again, m)
		}
		if !bytes.Equal(again.AppendMsgpack(nil), enc) {
			t.Fatalf("%s encoding is not canonical", name)
		}
	})
}
