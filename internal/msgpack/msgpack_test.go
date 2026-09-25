// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package msgpack

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
)

// Expected bytes below are derived by hand from the MessagePack
// specification and go-msgpack's documented format choices, not from this
// package.

func TestAppendUint(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want string
	}{
		{0, "00"}, {127, "7f"}, {128, "cc80"}, {255, "ccff"}, {256, "cd0100"},
		{65535, "cdffff"}, {65536, "ce00010000"}, {math.MaxUint32, "ceffffffff"},
		{math.MaxUint32 + 1, "cf0000000100000000"}, {math.MaxUint64, "cfffffffffffffffff"},
	} {
		if got := hex.EncodeToString(AppendUint(nil, tc.v)); got != tc.want {
			t.Errorf("AppendUint(%d) = %s, want %s", tc.v, got, tc.want)
		}
	}
}

func TestAppendInt(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want string
	}{
		{0, "00"}, {127, "7f"}, {128, "d10080"}, {32767, "d17fff"}, {32768, "d200008000"},
		{math.MaxInt32, "d27fffffff"}, {math.MaxInt32 + 1, "d30000000080000000"},
		{-1, "ff"}, {-32, "e0"}, {-33, "d0df"}, {-128, "d080"}, {-129, "d1ff7f"},
		{math.MinInt16, "d18000"}, {math.MinInt16 - 1, "d2ffff7fff"},
		{math.MinInt32, "d280000000"}, {math.MinInt32 - 1, "d3ffffffff7fffffff"},
		{math.MinInt64, "d38000000000000000"}, {math.MaxInt64, "d37fffffffffffffff"},
	} {
		if got := hex.EncodeToString(AppendInt(nil, tc.v)); got != tc.want {
			t.Errorf("AppendInt(%d) = %s, want %s", tc.v, got, tc.want)
		}
	}
}

func TestAppendRawAndContainers(t *testing.T) {
	check := func(name string, got []byte, want string) {
		t.Helper()
		if h := hex.EncodeToString(got); h != want {
			t.Errorf("%s = %s, want %s", name, h, want)
		}
	}
	check("nil bytes", AppendBytes(nil, nil), "c0")
	check("empty bytes", AppendBytes(nil, []byte{}), "a0")
	check("empty string", AppendString(nil, ""), "a0")
	check("fixstr", AppendString(nil, "ab"), "a26162")
	// Legacy raw: 32..65535 bytes use str16, never str8.
	check("str16 header", AppendString(nil, strings.Repeat("x", 32))[:3], "da0020")
	check("str16 max header", AppendString(nil, strings.Repeat("x", 65535))[:3], "daffff")
	check("str32 header", AppendString(nil, strings.Repeat("x", 65536))[:5], "db00010000")
	check("fixmap", AppendMapHeader(nil, 15), "8f")
	check("map16", AppendMapHeader(nil, 16), "de0010")
	check("map32", AppendMapHeader(nil, 65536), "df00010000")
	check("bools", AppendBool(AppendBool(nil, false), true), "c2c3")
}

func decodeHex(t *testing.T, h string) *Decoder {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatal(err)
	}
	return NewDecoder(b)
}

func TestReadIntegers(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"00", 0}, {"7f", 127}, {"ff", -1}, {"e0", -32}, {"cc80", 128}, {"cdffff", 65535},
		{"ce00010000", 65536}, {"cf0000000100000000", math.MaxUint32 + 1}, {"d080", -128},
		{"d0ff", -1}, {"d17fff", 32767}, {"d18000", math.MinInt16}, {"d280000000", math.MinInt32},
		{"d38000000000000000", math.MinInt64}, {"c0", 0},
	} {
		v, err := decodeHex(t, tc.in).ReadInt(math.MinInt64, math.MaxInt64)
		if err != nil || v != tc.want {
			t.Errorf("ReadInt(%s) = %d, %v; want %d", tc.in, v, err, tc.want)
		}
	}
	for _, tc := range []struct {
		in    string
		limit uint64
		want  uint64
		ok    bool
	}{
		{"cfffffffffffffffff", math.MaxUint64, math.MaxUint64, true},
		{"d07f", 255, 127, true},
		{"ff", math.MaxUint64, 0, false},         // negative fixint
		{"d0ff", math.MaxUint64, 0, false},       // negative int8
		{"cd0100", 255, 0, false},                // overflow
		{"ce00010000", math.MaxUint16, 0, false}, // overflow
		{"c3", 1, 0, false},                      // bool
		{"a0", 1, 0, false},                      // string
	} {
		v, err := decodeHex(t, tc.in).ReadUint(tc.limit)
		if (err == nil) != tc.ok || v != tc.want {
			t.Errorf("ReadUint(%s, %d) = %d, %v; want %d ok=%v", tc.in, tc.limit, v, err, tc.want, tc.ok)
		}
	}
	if _, err := decodeHex(t, "cfffffffffffffffff").ReadInt(math.MinInt64, math.MaxInt64); err == nil {
		t.Error("uint64 above MaxInt64 read as a signed integer")
	}
	if _, err := decodeHex(t, "d1ff7f").ReadInt(-128, 127); err == nil {
		t.Error("-129 accepted for [-128, 127]")
	}
}

func TestReadBool(t *testing.T) {
	for in, want := range map[string]bool{"c3": true, "c2": false, "01": true, "00": false, "c0": false} {
		v, err := decodeHex(t, in).ReadBool()
		if err != nil || v != want {
			t.Errorf("ReadBool(%s) = %v, %v; want %v", in, v, err, want)
		}
	}
	for _, in := range []string{"02", "a0", "ff"} {
		if _, err := decodeHex(t, in).ReadBool(); err == nil {
			t.Errorf("ReadBool(%s) accepted", in)
		}
	}
}

func TestReadBytesAndStrings(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []byte
	}{
		{"c0", nil}, {"a0", []byte{}}, {"a26162", []byte("ab")}, {"d9026162", []byte("ab")},
		{"da00026162", []byte("ab")}, {"db000000026162", []byte("ab")}, {"c4026162", []byte("ab")},
		{"c500026162", []byte("ab")}, {"c600000002616263", []byte("ab")},
	} {
		v, err := decodeHex(t, tc.in).ReadBytes(1 << 20)
		if err != nil || !bytes.Equal(v, tc.want) || (v == nil) != (tc.want == nil) {
			t.Errorf("ReadBytes(%s) = %#v, %v; want %#v", tc.in, v, err, tc.want)
		}
		s, err := decodeHex(t, tc.in).ReadString(1 << 20)
		if err != nil || s != string(tc.want) {
			t.Errorf("ReadString(%s) = %q, %v", tc.in, s, err)
		}
	}
	if _, err := decodeHex(t, "a3616263").ReadBytes(2); err == nil {
		t.Error("value above the limit accepted")
	}
	if _, err := decodeHex(t, "a3616263").ReadString(2); err == nil {
		t.Error("string above the limit accepted")
	}
	if _, err := decodeHex(t, "7f").ReadBytes(8); err == nil {
		t.Error("integer read as bytes")
	}
}

func TestReadBytesCopiesInput(t *testing.T) {
	in := []byte{0xa2, 'a', 'b'}
	v, err := NewDecoder(in).ReadBytes(8)
	if err != nil {
		t.Fatal(err)
	}
	in[1] = 'z'
	if string(v) != "ab" {
		t.Fatalf("decoded bytes alias the input: %q", v)
	}
}

func TestTruncatedInputs(t *testing.T) {
	for _, in := range []string{"", "cd00", "ce0000", "cf00000000000000", "a3616263"[:6], "d905", "da0001", "db00000001", "de00", "c5"} {
		d := decodeHex(t, in)
		errs := []error{}
		_, err := d.ReadInt(math.MinInt64, math.MaxInt64)
		errs = append(errs, err)
		_, err = decodeHex(t, in).ReadBytes(1 << 20)
		errs = append(errs, err)
		_, err = decodeHex(t, in).ReadMapHeader()
		errs = append(errs, err)
		errs = append(errs, decodeHex(t, in).Skip())
		for _, err := range errs {
			if err == nil {
				t.Errorf("%q: truncated input decoded without error", in)
			}
		}
	}
	if _, err := decodeHex(t, "cd00").ReadUint(math.MaxUint64); !errors.Is(err, ErrTruncated) {
		t.Errorf("truncated uint16 error = %v, want ErrTruncated", err)
	}
}

func TestMapHeaderBoundedByInput(t *testing.T) {
	// A map32 header declaring 2^31-1 entries with none following is
	// truncated; one declaring 2^32-1 is over the count limit.
	if _, err := decodeHex(t, "df7fffffff").ReadMapHeader(); !errors.Is(err, ErrTruncated) {
		t.Fatalf("oversized map header error = %v, want ErrTruncated", err)
	}
	if _, err := decodeHex(t, "dfffffffff").ReadMapHeader(); err == nil {
		t.Fatal("map count 2^32-1 accepted")
	}
	n, err := decodeHex(t, "c0").ReadMapHeader()
	if err != nil || n != 0 {
		t.Fatalf("nil map = %d, %v; want 0, nil", n, err)
	}
}

func TestReadKey(t *testing.T) {
	d := decodeHex(t, "a3616263c403646566")
	for _, want := range []string{"abc", "def"} {
		k, err := d.ReadKey()
		if err != nil || string(k) != want {
			t.Fatalf("ReadKey = %q, %v; want %q", k, err, want)
		}
	}
	long := AppendString(nil, strings.Repeat("k", 65))
	long = AppendUint(long, 7)
	d = NewDecoder(long)
	k, err := d.ReadKey()
	if err != nil || k != nil {
		t.Fatalf("overlong key = %q, %v; want nil, nil", k, err)
	}
	if v, err := d.ReadUint(10); err != nil || v != 7 {
		t.Fatalf("value after an overlong key = %d, %v", v, err)
	}
}

func TestSkip(t *testing.T) {
	values := []string{
		"00", "ff", "c0", "c2", "c3", "cc01", "cd0001", "ce00000001", "cf0000000000000001",
		"d001", "d10001", "d200000001", "d30000000000000001", "ca3f800000", "cb3ff0000000000000",
		"a26162", "d9026162", "da00026162", "db000000026162", "c4026162", "c500026162", "c600000002616263"[:14],
		"d40101", "d5010102", "d601010203040", "c70101ff", "c8000101ff", "c900000001 01ff",
		"93010203", "dc0002c0c0", "dd00000001c0", "82a1610a01c0", "de0001c0c0", "df00000001c0c0",
	}
	for _, v := range values {
		v = strings.ReplaceAll(v, " ", "")
		if len(v)%2 == 1 {
			v = v[:len(v)-1]
		}
		in, err := hex.DecodeString(v + "2a") // a sentinel follows
		if err != nil {
			t.Fatal(err)
		}
		d := NewDecoder(in)
		if err := d.Skip(); err != nil {
			t.Errorf("Skip(%s): %v", v, err)
			continue
		}
		if s, err := d.ReadUint(255); err != nil || s != 42 {
			t.Errorf("Skip(%s) consumed the wrong length: next = %d, %v", v, s, err)
		}
	}
	if err := decodeHex(t, "c1").Skip(); err == nil {
		t.Error("reserved byte 0xc1 skipped")
	}
	deep := bytes.Repeat([]byte{0x91}, maxSkipDepth+2)
	deep = append(deep, 0xc0)
	if err := NewDecoder(deep).Skip(); err == nil {
		t.Error("value nested past the depth limit skipped")
	}
	if err := decodeHex(t, "ddffffffff").Skip(); !errors.Is(err, ErrTruncated) {
		t.Errorf("oversized array skip error = %v, want ErrTruncated", err)
	}
}

// TestStreamDeclaredLengthDoesNotAllocate: in stream mode a header that
// declares a huge value must not allocate that size before the bytes
// arrive.
func TestStreamDeclaredLengthDoesNotAllocate(t *testing.T) {
	in := append([]byte{0xdb, 0x7f, 0xff, 0xff, 0xff}, bytes.Repeat([]byte{'x'}, 100)...)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := NewStreamDecoder(bufio.NewReader(bytes.NewReader(in))).ReadBytes(math.MaxInt32)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 1<<20 {
		t.Fatalf("a 2 GiB declared length allocated %d bytes for 100 received", alloc)
	}
}

func TestStreamMatchesSlice(t *testing.T) {
	var in []byte
	in = AppendMapHeader(in, 2)
	in = AppendString(in, "k")
	in = AppendBytes(in, bytes.Repeat([]byte{7}, 200_000))
	in = AppendString(in, "n")
	in = AppendInt(in, -12345)
	in = append(in, "trailing"...)

	for _, d := range []*Decoder{NewDecoder(in), NewStreamDecoder(bufio.NewReader(bytes.NewReader(in)))} {
		n, err := d.ReadMapHeader()
		if err != nil || n != 2 {
			t.Fatalf("map header %d, %v", n, err)
		}
		if k, err := d.ReadKey(); err != nil || string(k) != "k" {
			t.Fatalf("key %q, %v", k, err)
		}
		b, err := d.ReadBytes(1 << 20)
		if err != nil || len(b) != 200_000 || b[199_999] != 7 {
			t.Fatalf("bytes len %d, %v", len(b), err)
		}
		if k, err := d.ReadKey(); err != nil || string(k) != "n" {
			t.Fatalf("key %q, %v", k, err)
		}
		if v, err := d.ReadInt(math.MinInt64, math.MaxInt64); err != nil || v != -12345 {
			t.Fatalf("int %d, %v", v, err)
		}
		raw, err := d.ReadRaw(8)
		if err != nil || string(raw) != "trailing" {
			t.Fatalf("raw %q, %v", raw, err)
		}
		if _, err := d.ReadRaw(1); !errors.Is(err, ErrTruncated) {
			t.Fatalf("read past the end: %v", err)
		}
	}
}

func FuzzDecoder(f *testing.F) {
	f.Add([]byte{0x82, 0xa1, 'a', 0x01, 0xa1, 'b', 0xc0})
	f.Add([]byte{0xdf, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0xdb, 0xff, 0xff, 0xff, 0xff, 1, 2})
	f.Fuzz(func(t *testing.T, in []byte) {
		// Every operation must return (value or error) without panicking,
		// in both modes, and consume the same bytes in both.
		for _, op := range []func(*Decoder) error{
			func(d *Decoder) error { return d.Skip() },
			func(d *Decoder) error { _, err := d.ReadMapHeader(); return err },
			func(d *Decoder) error { _, err := d.ReadBytes(1 << 16); return err },
			func(d *Decoder) error { _, err := d.ReadString(1 << 16); return err },
			func(d *Decoder) error { _, err := d.ReadKey(); return err },
			func(d *Decoder) error { _, err := d.ReadInt(math.MinInt64, math.MaxInt64); return err },
			func(d *Decoder) error { _, err := d.ReadUint(math.MaxUint64); return err },
			func(d *Decoder) error { _, err := d.ReadBool(); return err },
		} {
			slice := NewDecoder(in)
			r := bytes.NewReader(in)
			stream := NewStreamDecoder(r)
			// Slice mode knows the input length and may refuse an
			// oversized count earlier; otherwise the modes agree.
			errS, errR := op(slice), op(stream)
			if errR != nil && errS == nil {
				t.Fatalf("stream refused (%v) what slice mode accepted", errR)
			}
			if errS == nil && slice.Remaining() != r.Len() {
				t.Fatalf("slice left %d bytes, stream %d", slice.Remaining(), r.Len())
			}
		}
	})
}

// TestMapHeaderCountFitsInt: a map count that does not fit an int32 is
// refused on every platform; on 32-bit platforms it used to wrap negative
// in stream mode and decode as an empty map.
func TestMapHeaderCountFitsInt(t *testing.T) {
	in := []byte{0xdf, 0x80, 0x00, 0x00, 0x00, 0xa5, 'N', 'o', 'd', 'e', 's', 0x05}
	for _, d := range []*Decoder{NewDecoder(in), NewStreamDecoder(bufio.NewReader(bytes.NewReader(in)))} {
		n, err := d.ReadMapHeader()
		if err == nil {
			t.Fatalf("map count 2^31 accepted as %d", n)
		}
	}
}
