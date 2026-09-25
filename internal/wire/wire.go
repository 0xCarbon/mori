// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

// Package wire defines the memberlist protocol messages and their
// MessagePack codec.
//
// Every message encodes as a map keyed by field name, with the entries
// sorted by name (bytewise), exactly as github.com/hashicorp/go-msgpack/v2
// encodes the structs with a default MsgpackHandle; fields tagged omitempty
// upstream are omitted when zero. Decoding accepts any encoding go-msgpack accepts for the field
// types, ignores unknown fields, lets the last duplicate key win, and resets
// the message first. Byte-slice and string fields are copies, never aliases
// of the input.
//
// Field names and types are part of the protocol: never rename or retype a
// field. Encoders write entries in sorted key order; keep new fields sorted.
package wire

import (
	"fmt"
	"math"

	"github.com/0xCarbon/mori/internal/msgpack"
)

// Decoding limits. They bound values from the network; legitimate messages
// are far smaller (a node's meta is capped by the packet budget).
const (
	// MaxFieldBytes bounds any string or byte-slice field except
	// Compress.Buf.
	MaxFieldBytes = 1 << 20
	// MaxCompressedBytes bounds Compress.Buf.
	MaxCompressedBytes = 64 << 20
)

// Message is implemented by every protocol message.
type Message interface {
	// AppendMsgpack appends the message's encoding to b.
	AppendMsgpack(b []byte) []byte
}

// Ping is sent directly to a node.
type Ping struct {
	SeqNo uint32

	// Node is sent so the target can verify they are the intended
	// recipient. This protects against an agent restart with a new name.
	Node string

	SourceAddr []byte // omitempty; source address, used for a direct reply
	SourcePort uint16 // omitempty; source port, used for a direct reply
	SourceNode string // omitempty; source name, used for a direct reply
}

// IndirectPingReq asks a node to ping a target on the sender's behalf.
type IndirectPingReq struct {
	SeqNo  uint32
	Target []byte
	Port   uint16

	// Node is sent so the target can verify they are the intended
	// recipient. This protects against an agent restart with a new name.
	Node string

	Nack bool // true if we'd like a nack back

	SourceAddr []byte // omitempty; source address, used for a direct reply
	SourcePort uint16 // omitempty; source port, used for a direct reply
	SourceNode string // omitempty; source name, used for a direct reply
}

// AckResp is sent in response to a ping.
type AckResp struct {
	SeqNo   uint32
	Payload []byte
}

// NackResp is sent for an indirect ping when the pinger doesn't hear from
// the ping-ee within the configured timeout.
type NackResp struct {
	SeqNo uint32
}

// ErrResp relays an error from the remote end of a stream.
type ErrResp struct {
	Error string
}

// Suspect is broadcast when a node is suspected dead.
type Suspect struct {
	Incarnation uint32
	Node        string
	From        string // who is suspecting
}

// Alive is broadcast when a node is known alive; overloaded for joins.
type Alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
	Port        uint16
	Meta        []byte

	// The versions of the protocol/delegate that are being spoken, order:
	// pmin, pmax, pcur, dmin, dmax, dcur
	Vsn []uint8
}

// Dead is broadcast when a node is confirmed dead; overloaded for leaves.
type Dead struct {
	Incarnation uint32
	Node        string
	From        string // who is suspecting
}

// PushPullHeader tells the other side how many states follow.
type PushPullHeader struct {
	Nodes        int
	UserStateLen int  // byte length of the user state that follows
	Join         bool // join request, or an anti-entropy run
}

// UserMsgHeader precedes a stream user message.
type UserMsgHeader struct {
	UserMsgLen int // byte length of the message that follows
}

// PushNodeState is one node's state in a push/pull exchange.
type PushNodeState struct {
	Name        string
	Addr        []byte
	Port        uint16
	Meta        []byte
	Incarnation uint32
	State       int     // the node state (mori.NodeStateType)
	Vsn         []uint8 // protocol versions
}

// Compress wraps a payload compressed with Algo.
type Compress struct {
	Algo uint8
	Buf  []byte
}

// Field decoders. Each consumes one value into its destination.

func decUint(d *msgpack.Decoder, limit uint64) (uint64, error) { return d.ReadUint(limit) }

func decU32(d *msgpack.Decoder, p *uint32) error {
	v, err := decUint(d, math.MaxUint32)
	*p = uint32(v)
	return err
}

func decU16(d *msgpack.Decoder, p *uint16) error {
	v, err := decUint(d, math.MaxUint16)
	*p = uint16(v)
	return err
}

func decU8(d *msgpack.Decoder, p *uint8) error {
	v, err := decUint(d, math.MaxUint8)
	*p = uint8(v)
	return err
}

func decInt(d *msgpack.Decoder, p *int) error {
	v, err := d.ReadInt(math.MinInt, math.MaxInt)
	*p = int(v)
	return err
}

func decBool(d *msgpack.Decoder, p *bool) error {
	v, err := d.ReadBool()
	*p = v
	return err
}

func decString(d *msgpack.Decoder, p *string) error {
	v, err := d.ReadString(MaxFieldBytes)
	*p = v
	return err
}

func decBytes(d *msgpack.Decoder, p *[]byte, limit int) error {
	v, err := d.ReadBytes(limit)
	*p = v
	return err
}

// fieldDecoder decodes the value of one known field. It reports false,
// consuming nothing, for an unknown key. Implemented by message pointers,
// so dispatch through it does not allocate.
type fieldDecoder interface {
	decodeField(d *msgpack.Decoder, key []byte) (bool, error)
}

// decodeMap reads a map header and decodes every entry into m; unknown
// keys are skipped.
func decodeMap(d *msgpack.Decoder, msg string, m fieldDecoder) error {
	n, err := d.ReadMapHeader()
	if err != nil {
		return fmt.Errorf("%s: %w", msg, err)
	}
	for range n {
		k, err := d.ReadKey()
		if err != nil {
			return fmt.Errorf("%s: %w", msg, err)
		}
		known, err := m.decodeField(d, k)
		if err == nil && !known {
			err = d.Skip()
		}
		if err != nil {
			if k == nil {
				return fmt.Errorf("%s: %w", msg, err)
			}
			return fmt.Errorf("%s.%s: %w", msg, k, err)
		}
	}
	return nil
}

// Encoders. Keys are written as legacy raw strings like any string.

func appendU(b []byte, key string, v uint64) []byte {
	return msgpack.AppendUint(msgpack.AppendString(b, key), v)
}

func appendI(b []byte, key string, v int) []byte {
	return msgpack.AppendInt(msgpack.AppendString(b, key), int64(v))
}

func appendS(b []byte, key, v string) []byte {
	return msgpack.AppendString(msgpack.AppendString(b, key), v)
}

func appendB(b []byte, key string, v []byte) []byte {
	return msgpack.AppendBytes(msgpack.AppendString(b, key), v)
}

func appendBool(b []byte, key string, v bool) []byte {
	return msgpack.AppendBool(msgpack.AppendString(b, key), v)
}

// omitted counts the omitempty source fields that are zero.
func omitted(addr []byte, port uint16, node string) int {
	n := 0
	if len(addr) == 0 {
		n++
	}
	if port == 0 {
		n++
	}
	if node == "" {
		n++
	}
	return n
}

func appendSource(b []byte, addr []byte, port uint16, node string) []byte {
	if len(addr) != 0 {
		b = appendB(b, "SourceAddr", addr)
	}
	if node != "" {
		b = appendS(b, "SourceNode", node)
	}
	if port != 0 {
		b = appendU(b, "SourcePort", uint64(port))
	}
	return b
}

// AppendMsgpack implements Message.
func (m Ping) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 5-omitted(m.SourceAddr, m.SourcePort, m.SourceNode))
	b = appendS(b, "Node", m.Node)
	b = appendU(b, "SeqNo", uint64(m.SeqNo))
	return appendSource(b, m.SourceAddr, m.SourcePort, m.SourceNode)
}

// DecodeMsgpack resets m and decodes one Ping from d.
func (m *Ping) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = Ping{}
	return decodeMap(d, "ping", m)
}

func (m *Ping) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "SeqNo":
		return true, decU32(d, &m.SeqNo)
	case "Node":
		return true, decString(d, &m.Node)
	case "SourceAddr":
		return true, decBytes(d, &m.SourceAddr, MaxFieldBytes)
	case "SourcePort":
		return true, decU16(d, &m.SourcePort)
	case "SourceNode":
		return true, decString(d, &m.SourceNode)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m IndirectPingReq) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 8-omitted(m.SourceAddr, m.SourcePort, m.SourceNode))
	b = appendBool(b, "Nack", m.Nack)
	b = appendS(b, "Node", m.Node)
	b = appendU(b, "Port", uint64(m.Port))
	b = appendU(b, "SeqNo", uint64(m.SeqNo))
	b = appendSource(b, m.SourceAddr, m.SourcePort, m.SourceNode)
	return appendB(b, "Target", m.Target)
}

// DecodeMsgpack resets m and decodes one IndirectPingReq from d.
func (m *IndirectPingReq) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = IndirectPingReq{}
	return decodeMap(d, "indirect ping", m)
}

func (m *IndirectPingReq) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "SeqNo":
		return true, decU32(d, &m.SeqNo)
	case "Target":
		return true, decBytes(d, &m.Target, MaxFieldBytes)
	case "Port":
		return true, decU16(d, &m.Port)
	case "Node":
		return true, decString(d, &m.Node)
	case "Nack":
		return true, decBool(d, &m.Nack)
	case "SourceAddr":
		return true, decBytes(d, &m.SourceAddr, MaxFieldBytes)
	case "SourcePort":
		return true, decU16(d, &m.SourcePort)
	case "SourceNode":
		return true, decString(d, &m.SourceNode)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m AckResp) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 2)
	b = appendB(b, "Payload", m.Payload)
	return appendU(b, "SeqNo", uint64(m.SeqNo))
}

// DecodeMsgpack resets m and decodes one AckResp from d.
func (m *AckResp) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = AckResp{}
	return decodeMap(d, "ack", m)
}

func (m *AckResp) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "SeqNo":
		return true, decU32(d, &m.SeqNo)
	case "Payload":
		return true, decBytes(d, &m.Payload, MaxFieldBytes)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m NackResp) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 1)
	return appendU(b, "SeqNo", uint64(m.SeqNo))
}

// DecodeMsgpack resets m and decodes one NackResp from d.
func (m *NackResp) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = NackResp{}
	return decodeMap(d, "nack", m)
}

func (m *NackResp) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	if string(k) == "SeqNo" {
		return true, decU32(d, &m.SeqNo)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m ErrResp) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 1)
	return appendS(b, "Error", m.Error)
}

// DecodeMsgpack resets m and decodes one ErrResp from d.
func (m *ErrResp) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = ErrResp{}
	return decodeMap(d, "error response", m)
}

func (m *ErrResp) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	if string(k) == "Error" {
		return true, decString(d, &m.Error)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m Suspect) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 3)
	b = appendS(b, "From", m.From)
	b = appendU(b, "Incarnation", uint64(m.Incarnation))
	return appendS(b, "Node", m.Node)
}

// DecodeMsgpack resets m and decodes one Suspect from d.
func (m *Suspect) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = Suspect{}
	return decodeMap(d, "suspect", m)
}

func (m *Suspect) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Incarnation":
		return true, decU32(d, &m.Incarnation)
	case "Node":
		return true, decString(d, &m.Node)
	case "From":
		return true, decString(d, &m.From)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m Alive) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 6)
	b = appendB(b, "Addr", m.Addr)
	b = appendU(b, "Incarnation", uint64(m.Incarnation))
	b = appendB(b, "Meta", m.Meta)
	b = appendS(b, "Node", m.Node)
	b = appendU(b, "Port", uint64(m.Port))
	return appendB(b, "Vsn", m.Vsn)
}

// DecodeMsgpack resets m and decodes one Alive from d.
func (m *Alive) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = Alive{}
	return decodeMap(d, "alive", m)
}

func (m *Alive) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Incarnation":
		return true, decU32(d, &m.Incarnation)
	case "Node":
		return true, decString(d, &m.Node)
	case "Addr":
		return true, decBytes(d, &m.Addr, MaxFieldBytes)
	case "Port":
		return true, decU16(d, &m.Port)
	case "Meta":
		return true, decBytes(d, &m.Meta, MaxFieldBytes)
	case "Vsn":
		return true, decBytes(d, &m.Vsn, MaxFieldBytes)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m Dead) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 3)
	b = appendS(b, "From", m.From)
	b = appendU(b, "Incarnation", uint64(m.Incarnation))
	return appendS(b, "Node", m.Node)
}

// DecodeMsgpack resets m and decodes one Dead from d.
func (m *Dead) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = Dead{}
	return decodeMap(d, "dead", m)
}

func (m *Dead) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Incarnation":
		return true, decU32(d, &m.Incarnation)
	case "Node":
		return true, decString(d, &m.Node)
	case "From":
		return true, decString(d, &m.From)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m PushPullHeader) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 3)
	b = appendBool(b, "Join", m.Join)
	b = appendI(b, "Nodes", m.Nodes)
	return appendI(b, "UserStateLen", m.UserStateLen)
}

// DecodeMsgpack resets m and decodes one PushPullHeader from d.
func (m *PushPullHeader) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = PushPullHeader{}
	return decodeMap(d, "push/pull header", m)
}

func (m *PushPullHeader) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Nodes":
		return true, decInt(d, &m.Nodes)
	case "UserStateLen":
		return true, decInt(d, &m.UserStateLen)
	case "Join":
		return true, decBool(d, &m.Join)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m UserMsgHeader) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 1)
	return appendI(b, "UserMsgLen", m.UserMsgLen)
}

// DecodeMsgpack resets m and decodes one UserMsgHeader from d.
func (m *UserMsgHeader) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = UserMsgHeader{}
	return decodeMap(d, "user message header", m)
}

func (m *UserMsgHeader) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	if string(k) == "UserMsgLen" {
		return true, decInt(d, &m.UserMsgLen)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m PushNodeState) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 7)
	b = appendB(b, "Addr", m.Addr)
	b = appendU(b, "Incarnation", uint64(m.Incarnation))
	b = appendB(b, "Meta", m.Meta)
	b = appendS(b, "Name", m.Name)
	b = appendU(b, "Port", uint64(m.Port))
	b = appendI(b, "State", m.State)
	return appendB(b, "Vsn", m.Vsn)
}

// DecodeMsgpack resets m and decodes one PushNodeState from d.
func (m *PushNodeState) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = PushNodeState{}
	return decodeMap(d, "push node state", m)
}

func (m *PushNodeState) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Name":
		return true, decString(d, &m.Name)
	case "Addr":
		return true, decBytes(d, &m.Addr, MaxFieldBytes)
	case "Port":
		return true, decU16(d, &m.Port)
	case "Meta":
		return true, decBytes(d, &m.Meta, MaxFieldBytes)
	case "Incarnation":
		return true, decU32(d, &m.Incarnation)
	case "State":
		return true, decInt(d, &m.State)
	case "Vsn":
		return true, decBytes(d, &m.Vsn, MaxFieldBytes)
	}
	return false, nil
}

// AppendMsgpack implements Message.
func (m Compress) AppendMsgpack(b []byte) []byte {
	b = msgpack.AppendMapHeader(b, 2)
	b = appendU(b, "Algo", uint64(m.Algo))
	return appendB(b, "Buf", m.Buf)
}

// DecodeMsgpack resets m and decodes one Compress from d.
func (m *Compress) DecodeMsgpack(d *msgpack.Decoder) error {
	*m = Compress{}
	return decodeMap(d, "compress", m)
}

func (m *Compress) decodeField(d *msgpack.Decoder, k []byte) (bool, error) {
	switch string(k) {
	case "Algo":
		return true, decU8(d, &m.Algo)
	case "Buf":
		return true, decBytes(d, &m.Buf, MaxCompressedBytes)
	}
	return false, nil
}
