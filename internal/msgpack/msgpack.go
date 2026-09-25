// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Package msgpack implements the subset of MessagePack that the memberlist
// wire protocol uses, byte-for-byte compatible with the encoder the
// protocol was defined by: github.com/hashicorp/go-msgpack/v2 with a
// default MsgpackHandle (WriteExt false).
//
// Encoding follows that handle exactly:
//
//   - strings and byte slices use the legacy raw family: fixstr (< 32 bytes),
//     str16 (< 65536), str32; never str8 or bin. A nil byte slice is nil.
//   - unsigned integers use the smallest of positive fixint, uint8, uint16,
//     uint32, uint64.
//   - signed integers use positive fixint for 0..127, int16/int32/int64 for
//     larger positive values (never uint*), negative fixint for -32..-1,
//     and the smallest of int8/int16/int32/int64 below that.
//   - maps use fixmap (< 16 entries), map16, map32.
//
// Decoding accepts every encoding a MessagePack encoder can produce for the
// target type: any integer format whose value is in range, str and bin
// families for strings and bytes, 0/1 fixints for booleans, and nil for
// the zero value. It is deliberately stricter than go-msgpack's decoder,
// which also accepts arrays of integers as bytes or strings and wraps
// out-of-range integers; no memberlist encoder produces either. Map counts
// above 2^31-1 and skipped values nested deeper than 32 levels are refused.
// It never panics and never allocates more than the input holds: in slice
// mode lengths are checked against the remaining input, and in stream mode
// buffers grow with the bytes actually received.
package msgpack

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
)

// Format bytes (MessagePack specification).
const (
	posFixintMax = 0x7f
	fixmapMin    = 0x80
	fixmapMax    = 0x8f
	fixarrayMin  = 0x90
	fixarrayMax  = 0x9f
	fixstrMin    = 0xa0
	fixstrMax    = 0xbf
	nilByte      = 0xc0
	falseByte    = 0xc2
	trueByte     = 0xc3
	bin8         = 0xc4
	bin16        = 0xc5
	bin32        = 0xc6
	ext8         = 0xc7
	ext16        = 0xc8
	ext32        = 0xc9
	float32Byte  = 0xca
	float64Byte  = 0xcb
	uint8Byte    = 0xcc
	uint16Byte   = 0xcd
	uint32Byte   = 0xce
	uint64Byte   = 0xcf
	int8Byte     = 0xd0
	int16Byte    = 0xd1
	int32Byte    = 0xd2
	int64Byte    = 0xd3
	fixext1      = 0xd4
	fixext16     = 0xd8
	str8         = 0xd9
	str16        = 0xda
	str32        = 0xdb
	array16      = 0xdc
	array32      = 0xdd
	map16        = 0xde
	map32        = 0xdf
	negFixintMin = 0xe0
)

// AppendNil appends a nil value.
func AppendNil(b []byte) []byte { return append(b, nilByte) }

// AppendBool appends a boolean.
func AppendBool(b []byte, v bool) []byte {
	if v {
		return append(b, trueByte)
	}
	return append(b, falseByte)
}

// AppendUint appends an unsigned integer in its smallest format.
func AppendUint(b []byte, v uint64) []byte {
	switch {
	case v <= posFixintMax:
		return append(b, byte(v))
	case v <= math.MaxUint8:
		return append(b, uint8Byte, byte(v))
	case v <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(b, uint16Byte), uint16(v))
	case v <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(b, uint32Byte), uint32(v))
	default:
		return binary.BigEndian.AppendUint64(append(b, uint64Byte), v)
	}
}

// AppendInt appends a signed integer the way go-msgpack's EncodeInt does
// (PositiveIntUnsigned false): positive values above 127 use the signed
// formats.
func AppendInt(b []byte, v int64) []byte {
	switch {
	case v > math.MaxInt8:
		switch {
		case v <= math.MaxInt16:
			return binary.BigEndian.AppendUint16(append(b, int16Byte), uint16(v))
		case v <= math.MaxInt32:
			return binary.BigEndian.AppendUint32(append(b, int32Byte), uint32(v))
		default:
			return binary.BigEndian.AppendUint64(append(b, int64Byte), uint64(v))
		}
	case v >= -32:
		return append(b, byte(v))
	case v >= math.MinInt8:
		return append(b, int8Byte, byte(v))
	case v >= math.MinInt16:
		return binary.BigEndian.AppendUint16(append(b, int16Byte), uint16(v))
	case v >= math.MinInt32:
		return binary.BigEndian.AppendUint32(append(b, int32Byte), uint32(v))
	default:
		return binary.BigEndian.AppendUint64(append(b, int64Byte), uint64(v))
	}
}

// appendRawHeader appends a legacy raw (string) header of length n.
func appendRawHeader(b []byte, n int) []byte {
	switch {
	case n < 32:
		return append(b, fixstrMin|byte(n))
	case n < 1<<16:
		return binary.BigEndian.AppendUint16(append(b, str16), uint16(n))
	default:
		return binary.BigEndian.AppendUint32(append(b, str32), uint32(n))
	}
}

// AppendString appends a string in the legacy raw family.
func AppendString(b []byte, s string) []byte {
	return append(appendRawHeader(b, len(s)), s...)
}

// AppendBytes appends a byte slice in the legacy raw family; nil encodes as
// nil, an empty non-nil slice as an empty string.
func AppendBytes(b []byte, p []byte) []byte {
	if p == nil {
		return AppendNil(b)
	}
	return append(appendRawHeader(b, len(p)), p...)
}

// AppendMapHeader appends the header of a map with n entries.
func AppendMapHeader(b []byte, n int) []byte {
	switch {
	case n < 16:
		return append(b, fixmapMin|byte(n))
	case n < 1<<16:
		return binary.BigEndian.AppendUint16(append(b, map16), uint16(n))
	default:
		return binary.BigEndian.AppendUint32(append(b, map32), uint32(n))
	}
}

// ErrTruncated is returned when the input ends inside a value.
var ErrTruncated = errors.New("msgpack: truncated input")

// Source is a stream the decoder reads from without read-ahead: it takes
// exactly the bytes of each value, so the caller can read what follows.
type Source interface {
	io.Reader
	io.ByteReader
}

// maxSkipDepth bounds container nesting in values being skipped.
const maxSkipDepth = 32

// streamChunk is the largest buffer allocated for a stream value before
// the corresponding bytes have arrived.
const streamChunk = 64 << 10

// maxKeyLen is the longest map key returned by ReadKey; longer keys match
// no field and are skipped.
const maxKeyLen = 64

// Decoder reads values from a byte slice or a Source.
type Decoder struct {
	buf []byte // slice mode input
	off int
	src Source // stream mode input; nil in slice mode
	key [maxKeyLen]byte
	tmp [8]byte
}

// NewDecoder returns a decoder over b. Slices it returns alias b only where
// documented (ReadKey); byte and string values are copies.
func NewDecoder(b []byte) *Decoder { return &Decoder{buf: b} }

// NewStreamDecoder returns a decoder over src.
func NewStreamDecoder(src Source) *Decoder { return &Decoder{src: src} }

// Reset points d at a new slice input.
func (d *Decoder) Reset(b []byte) { *d = Decoder{buf: b} }

// Remaining reports the unread input in slice mode; -1 in stream mode.
func (d *Decoder) Remaining() int {
	if d.src != nil {
		return -1
	}
	return len(d.buf) - d.off
}

func (d *Decoder) readByte() (byte, error) {
	if d.src != nil {
		c, err := d.src.ReadByte()
		if err != nil {
			return 0, eofAsTruncated(err)
		}
		return c, nil
	}
	if d.off >= len(d.buf) {
		return 0, ErrTruncated
	}
	c := d.buf[d.off]
	d.off++
	return c, nil
}

// readFixed reads n <= 8 bytes into a big-endian integer.
func (d *Decoder) readFixed(n int) (uint64, error) {
	var p []byte
	if d.src != nil {
		// d.tmp lives with the decoder: a local array would escape
		// through the Reader interface and allocate on every call.
		p = d.tmp[:n]
		if _, err := io.ReadFull(d.src, p); err != nil {
			return 0, eofAsTruncated(err)
		}
	} else {
		if len(d.buf)-d.off < n {
			return 0, ErrTruncated
		}
		p = d.buf[d.off : d.off+n]
		d.off += n
	}
	var v uint64
	for _, c := range p {
		v = v<<8 | uint64(c)
	}
	return v, nil
}

func eofAsTruncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrTruncated
	}
	return err
}

// readInto fills a newly allocated n-byte slice from the input. In stream
// mode the allocation grows with the bytes received (at most about twice
// what has arrived, plus one chunk), so a declared length alone never buys
// memory.
func (d *Decoder) readInto(n int) ([]byte, error) {
	if d.src == nil {
		if len(d.buf)-d.off < n {
			return nil, ErrTruncated
		}
		out := make([]byte, n)
		copy(out, d.buf[d.off:])
		d.off += n
		return out, nil
	}
	out := make([]byte, 0, min(n, streamChunk))
	for len(out) < n {
		if len(out) == cap(out) {
			out = slices.Grow(out, min(n-len(out), max(len(out), streamChunk)))
		}
		chunk := out[len(out):min(cap(out), n)]
		if _, err := io.ReadFull(d.src, chunk); err != nil {
			return nil, eofAsTruncated(err)
		}
		out = out[:len(out)+len(chunk)]
	}
	return out, nil
}

// discard skips n bytes of input.
func (d *Decoder) discard(n uint64) error {
	if d.src == nil {
		if uint64(len(d.buf)-d.off) < n {
			return ErrTruncated
		}
		d.off += int(n)
		return nil
	}
	if _, err := io.CopyN(io.Discard, d.src, int64(min(n, math.MaxInt64))); err != nil {
		return eofAsTruncated(err)
	}
	return nil
}

// ReadRaw reads the next n raw bytes (not a MessagePack value), such as a
// payload that follows the encoded headers.
func (d *Decoder) ReadRaw(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("msgpack: negative raw length %d", n)
	}
	return d.readInto(n)
}

// typeError reports an unexpected format byte.
func typeError(want string, c byte) error {
	return fmt.Errorf("msgpack: cannot decode %s from format 0x%02x", want, c)
}

// ReadMapHeader reads a map header and returns its entry count. A nil
// value reads as an empty map (go-msgpack decodes nil into a zero struct).
func (d *Decoder) ReadMapHeader() (int, error) {
	c, err := d.readByte()
	if err != nil {
		return 0, err
	}
	var n uint64
	switch {
	case c >= fixmapMin && c <= fixmapMax:
		return int(c & 0x0f), nil
	case c == nilByte:
		return 0, nil
	case c == map16:
		n, err = d.readFixed(2)
	case c == map32:
		n, err = d.readFixed(4)
	default:
		return 0, typeError("map", c)
	}
	if err != nil {
		return 0, err
	}
	// No real map has 2^31 entries (each holds at least two bytes), and the
	// count must fit an int on 32-bit platforms.
	if n > math.MaxInt32 {
		return 0, fmt.Errorf("msgpack: map count %d exceeds the limit", n)
	}
	// Every entry holds at least two bytes.
	if r := d.Remaining(); r >= 0 && n > uint64(r)/2 {
		return 0, ErrTruncated
	}
	return int(n), nil
}

// rawLen reads a str/bin header. isNil reports a nil value.
func (d *Decoder) rawLen(want string) (n uint64, isNil bool, err error) {
	c, err := d.readByte()
	if err != nil {
		return 0, false, err
	}
	switch {
	case c >= fixstrMin && c <= fixstrMax:
		return uint64(c & 0x1f), false, nil
	case c == nilByte:
		return 0, true, nil
	case c == str8 || c == bin8:
		n, err = d.readFixed(1)
	case c == str16 || c == bin16:
		n, err = d.readFixed(2)
	case c == str32 || c == bin32:
		n, err = d.readFixed(4)
	default:
		return 0, false, typeError(want, c)
	}
	return n, false, err
}

// ReadBytes reads a str or bin value as a new byte slice. nil reads as nil;
// an empty value as an empty non-nil slice. Values longer than limit are
// refused before any allocation.
func (d *Decoder) ReadBytes(limit int) ([]byte, error) {
	n, isNil, err := d.rawLen("bytes")
	if err != nil || isNil {
		return nil, err
	}
	if n > uint64(limit) {
		return nil, fmt.Errorf("msgpack: %d-byte value exceeds the %d-byte limit", n, limit)
	}
	if n == 0 {
		return []byte{}, nil
	}
	return d.readInto(int(n))
}

// ReadString reads a str or bin value as a string; nil reads as "".
func (d *Decoder) ReadString(limit int) (string, error) {
	n, isNil, err := d.rawLen("string")
	if err != nil || isNil || n == 0 {
		return "", err
	}
	if n > uint64(limit) {
		return "", fmt.Errorf("msgpack: %d-byte value exceeds the %d-byte limit", n, limit)
	}
	if d.src == nil {
		if uint64(len(d.buf)-d.off) < n {
			return "", ErrTruncated
		}
		s := string(d.buf[d.off : d.off+int(n)])
		d.off += int(n)
		return s, nil
	}
	b, err := d.readInto(int(n))
	return string(b), err
}

// ReadKey reads a map key (str or bin). The returned slice is valid until
// the next call; keys longer than 64 bytes are consumed and returned as
// nil, which matches no field.
func (d *Decoder) ReadKey() ([]byte, error) {
	n, isNil, err := d.rawLen("map key")
	if err != nil || isNil {
		return nil, err
	}
	if n > maxKeyLen {
		return nil, d.discard(n)
	}
	if d.src == nil {
		if uint64(len(d.buf)-d.off) < n {
			return nil, ErrTruncated
		}
		k := d.buf[d.off : d.off+int(n) : d.off+int(n)]
		d.off += int(n)
		return k, nil
	}
	k := d.key[:n]
	if _, err := io.ReadFull(d.src, k); err != nil {
		return nil, eofAsTruncated(err)
	}
	return k, nil
}

// readInteger reads any integer format; nil reads as 0. neg reports a
// negative value, in which case v holds it as int64 bits.
func (d *Decoder) readInteger(want string) (v uint64, neg bool, err error) {
	c, err := d.readByte()
	if err != nil {
		return 0, false, err
	}
	switch {
	case c <= posFixintMax:
		return uint64(c), false, nil
	case c >= negFixintMin:
		return uint64(int64(int8(c))), true, nil
	case c == nilByte:
		return 0, false, nil
	case c == uint8Byte:
		v, err = d.readFixed(1)
	case c == uint16Byte:
		v, err = d.readFixed(2)
	case c == uint32Byte:
		v, err = d.readFixed(4)
	case c == uint64Byte:
		v, err = d.readFixed(8)
	case c == int8Byte:
		v, err = d.readFixed(1)
		i := int64(int8(v))
		return uint64(i), i < 0, err
	case c == int16Byte:
		v, err = d.readFixed(2)
		i := int64(int16(v))
		return uint64(i), i < 0, err
	case c == int32Byte:
		v, err = d.readFixed(4)
		i := int64(int32(v))
		return uint64(i), i < 0, err
	case c == int64Byte:
		v, err = d.readFixed(8)
		return v, int64(v) < 0, err
	default:
		return 0, false, typeError(want, c)
	}
	return v, false, err
}

// ReadUint reads an unsigned integer no larger than limit; nil reads as 0.
func (d *Decoder) ReadUint(limit uint64) (uint64, error) {
	v, neg, err := d.readInteger("unsigned integer")
	if err != nil {
		return 0, err
	}
	if neg {
		return 0, fmt.Errorf("msgpack: negative value %d for an unsigned integer", int64(v))
	}
	if v > limit {
		return 0, fmt.Errorf("msgpack: value %d overflows the %d limit", v, limit)
	}
	return v, nil
}

// ReadInt reads a signed integer within [lo, hi]; nil reads as 0.
func (d *Decoder) ReadInt(lo, hi int64) (int64, error) {
	v, neg, err := d.readInteger("signed integer")
	if err != nil {
		return 0, err
	}
	if !neg && v > math.MaxInt64 {
		return 0, fmt.Errorf("msgpack: value %d overflows a signed integer", v)
	}
	i := int64(v)
	if i < lo || i > hi {
		return 0, fmt.Errorf("msgpack: value %d outside [%d, %d]", i, lo, hi)
	}
	return i, nil
}

// ReadBool reads a boolean; like go-msgpack it accepts fixint 0 and 1, and
// nil reads as false.
func (d *Decoder) ReadBool() (bool, error) {
	c, err := d.readByte()
	if err != nil {
		return false, err
	}
	switch c {
	case trueByte, 1:
		return true, nil
	case falseByte, 0, nilByte:
		return false, nil
	}
	return false, typeError("bool", c)
}

// Skip consumes one value of any type.
func (d *Decoder) Skip() error { return d.skip(0) }

func (d *Decoder) skip(depth int) error {
	if depth > maxSkipDepth {
		return fmt.Errorf("msgpack: value nested deeper than %d levels", maxSkipDepth)
	}
	c, err := d.readByte()
	if err != nil {
		return err
	}
	var n uint64 // payload bytes (scalars) or elements (containers)
	elems := uint64(0)
	switch {
	case c <= posFixintMax, c >= negFixintMin, c == nilByte, c == falseByte, c == trueByte:
		return nil
	case c >= fixmapMin && c <= fixmapMax:
		elems = 2 * uint64(c&0x0f)
	case c >= fixarrayMin && c <= fixarrayMax:
		elems = uint64(c & 0x0f)
	case c >= fixstrMin && c <= fixstrMax:
		n = uint64(c & 0x1f)
	case c == uint8Byte, c == int8Byte:
		n = 1
	case c == uint16Byte, c == int16Byte:
		n = 2
	case c == uint32Byte, c == int32Byte, c == float32Byte:
		n = 4
	case c == uint64Byte, c == int64Byte, c == float64Byte:
		n = 8
	case c >= fixext1 && c <= fixext16:
		n = 1 + 1<<(c-fixext1)
	case c == str8, c == bin8:
		n, err = d.readFixed(1)
	case c == str16, c == bin16:
		n, err = d.readFixed(2)
	case c == str32, c == bin32:
		n, err = d.readFixed(4)
	case c == ext8:
		n, err = d.readFixed(1)
		n++
	case c == ext16:
		n, err = d.readFixed(2)
		n++
	case c == ext32:
		n, err = d.readFixed(4)
		n++
	case c == array16:
		elems, err = d.readFixed(2)
	case c == array32:
		elems, err = d.readFixed(4)
	case c == map16:
		elems, err = d.readFixed(2)
		elems *= 2
	case c == map32:
		elems, err = d.readFixed(4)
		elems *= 2
	default:
		return typeError("any value", c) // 0xc1 is never used
	}
	if err != nil {
		return err
	}
	if elems == 0 {
		return d.discard(n)
	}
	// Every element holds at least one byte.
	if r := d.Remaining(); r >= 0 && elems > uint64(r) {
		return ErrTruncated
	}
	for range elems {
		if err := d.skip(depth + 1); err != nil {
			return err
		}
	}
	return nil
}
