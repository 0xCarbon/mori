// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"time"

	"github.com/0xCarbon/mori/internal/msgpack"
	"github.com/0xCarbon/mori/internal/wire"
)

// This is the minimum and maximum protocol version that we can
// _understand_. We're allowed to speak at any version within this
// range. This range is inclusive.
const (
	ProtocolVersionMin uint8 = 1

	// Version 3 added support for TCP pings but we kept the default
	// protocol version at 2 to ease transition to this new feature.
	// A memberlist speaking version 2 of the protocol will attempt
	// to TCP ping another memberlist who understands version 3 or
	// greater.
	//
	// Version 4 added support for nacks as part of indirect probes.
	// A memberlist speaking version 2 of the protocol will expect
	// nacks from another memberlist who understands version 4 or
	// greater, and likewise nacks will be sent to memberlists who
	// understand version 4 or greater.
	ProtocolVersion2Compatible = 2

	ProtocolVersionMax = 5
)

// messageType is an integer ID of a type of message that can be received
// on network channels from other members.
type messageType uint8

// The list of available message types.
//
// WARNING: ONLY APPEND TO THIS LIST! The numeric values are part of the
// protocol itself.
const (
	pingMsg messageType = iota
	indirectPingMsg
	ackRespMsg
	suspectMsg
	aliveMsg
	deadMsg
	pushPullMsg
	compoundMsg
	userMsg // User mesg, not handled by us
	compressMsg
	encryptMsg
	nackRespMsg
	hasCrcMsg
	errMsg
)

const (
	// hasLabelMsg has a deliberately high value so that you can disambiguate
	// it from the encryptionVersion header which is either 0/1 right now and
	// also any of the existing messageTypes
	hasLabelMsg messageType = 244
)

// compressionType is used to specify the compression algorithm
type compressionType uint8

const (
	lzwAlgo compressionType = iota
)

const (
	MetaMaxSize            = 512 // Maximum size for node meta data
	compoundHeaderOverhead = 2   // Assumed header overhead
	compoundOverhead       = 2   // Assumed overhead per entry in compoundHeader
	userMsgOverhead        = 1
	blockingWarning        = 10 * time.Millisecond // Warn if a UDP packet takes this long to process
	maxPushStateBytes      = 20 * 1024 * 1024
	maxPushStateNodes      = 1024 * 1024      // Each requires conservatively  ~20 bytes when encoded
	maxUserMsgBytes        = 20 * 1024 * 1024 // Largest stream user message buffered off the wire
	maxPushPullRequests    = 128              // Maximum number of concurrent push/pull requests
	pushStatePrealloc      = 1024             // Node states preallocated before any is decoded

	// maxDecompressedBytes bounds a decompressed stream message: user state
	// plus an equal budget for the encoded node states.
	maxDecompressedBytes = 2 * maxPushStateBytes

	// maxPacketDecompressedBytes bounds a decompressed packet. Senders
	// compress packets built within the packet budget (at most one UDP
	// payload), so this leaves a wide margin while capping the expansion a
	// single unauthenticated datagram can force.
	maxPacketDecompressedBytes = 1 << 20

	// maxStreamMessageBytes bounds the plaintext bytes read for one stream
	// message (type byte, headers, node states and user payload).
	maxStreamMessageBytes = maxDecompressedBytes
)

// The protocol messages and their codec live in internal/wire; these
// aliases keep the protocol code readable.
type (
	ping            = wire.Ping
	indirectPingReq = wire.IndirectPingReq
	ackResp         = wire.AckResp
	nackResp        = wire.NackResp
	errResp         = wire.ErrResp
	suspect         = wire.Suspect
	alive           = wire.Alive
	dead            = wire.Dead
	pushPullHeader  = wire.PushPullHeader
	userMsgHeader   = wire.UserMsgHeader
	pushNodeState   = wire.PushNodeState
	compress        = wire.Compress
)

// msgHandoff is used to transfer a message between goroutines
type msgHandoff struct {
	msgType messageType
	buf     []byte
	from    net.Addr
}

// encryptionVersion returns the encryption version to use
func (m *Memberlist) encryptionVersion() encryptionVersion {
	switch m.ProtocolVersion() {
	case 1:
		return 0
	default:
		return 1
	}
}

// streamListen is a long running goroutine that pulls incoming streams from the
// transport and hands them off for processing.
func (m *Memberlist) streamListen() {
	for {
		select {
		case conn := <-m.transport.StreamCh():
			m.goBackground(func() { m.handleConn(conn) })

		case <-m.shutdownCh:
			return
		}
	}
}

// handleConn handles a single incoming stream connection from the transport.
func (m *Memberlist) handleConn(conn net.Conn) {
	m.logger.Debug("stream connection", connAttr(conn))

	m.metrics.counter(keyTCPAccept, 1)

	if err := conn.SetDeadline(time.Now().Add(m.config.TCPTimeout)); err != nil {
		m.logger.Error("could not set the stream deadline", "error", err)
	}

	var (
		streamLabel string
		err         error
		// Store the original conn, because the code below shadows it.
		// If reading the label header from the stream fail, we should still close the connection.
		origConn = conn
	)
	conn, streamLabel, err = RemoveLabelHeaderFromStream(conn)
	if err != nil {
		m.logger.Error("failed to receive and remove the stream label header", "error", err, connAttr(origConn))
		_ = origConn.Close()
		return
	}

	defer func() {
		// Always close the wrapped connection, that we got after removing the label header.
		_ = conn.Close()
	}()

	if m.config.SkipInboundLabelCheck {
		if streamLabel != "" {
			m.logger.Error("unexpected double stream label header", connAttr(conn))
			return
		}
		// Set this from config so that the auth data assertions work below.
		streamLabel = m.config.Label
	}

	if m.config.Label != streamLabel {
		m.logger.Error("discarding stream with unacceptable label", "label", streamLabel, connAttr(conn))
		return
	}

	msgType, dec, err := m.readStream(conn, streamLabel)
	if err != nil {
		if err != io.EOF {
			m.logger.Error("failed to receive", "error", err, connAttr(conn))

			out := encode(errMsg, errResp{Error: err.Error()})
			err = m.rawSendMsgStream(conn, out, streamLabel)
			if err != nil {
				m.logger.Error("failed to send error", "error", err, connAttr(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case userMsg:
		if err := m.readUserMsg(dec); err != nil {
			m.logger.Error("failed to receive user message", "error", err, connAttr(conn))
		}
	case pushPullMsg:
		// Increment counter of pending push/pulls
		numConcurrent := m.pushPullReq.Add(1)
		defer m.pushPullReq.Add(^uint32(0))

		// Check if we have too many open push/pull requests
		if numConcurrent >= maxPushPullRequests {
			m.logger.Error("too many pending push/pull requests", connAttr(conn))
			return
		}

		join, remoteNodes, userState, err := m.readRemoteState(dec)
		if err != nil {
			m.logger.Error("failed to read remote state", "error", err, connAttr(conn))
			return
		}

		if err := m.sendLocalState(conn, join, streamLabel); err != nil {
			m.logger.Error("failed to push local state", "error", err, connAttr(conn))
			return
		}

		if err := m.mergeRemoteState(join, remoteNodes, userState); err != nil {
			m.logger.Error("failed push/pull merge", "error", err, connAttr(conn))
			return
		}
	case pingMsg:
		var p ping
		if err := p.DecodeMsgpack(dec); err != nil {
			m.logger.Error("failed to decode ping", "error", err, connAttr(conn))
			return
		}

		if p.Node != "" && p.Node != m.config.Name {
			m.logger.Warn("got ping for unexpected node", "node", p.Node, connAttr(conn))
			return
		}

		out := encode(ackRespMsg, ackResp{SeqNo: p.SeqNo})
		if err := m.rawSendMsgStream(conn, out, streamLabel); err != nil {
			m.logger.Error("failed to send ack", "error", err, connAttr(conn))
			return
		}
	default:
		m.logger.Error("received invalid message type", "type", msgType, connAttr(conn))
	}
}

// packetListen is a long running goroutine that pulls packets out of the
// transport and hands them off for processing.
func (m *Memberlist) packetListen() {
	for {
		select {
		case packet := <-m.transport.PacketCh():
			m.ingestPacket(packet.Buf, packet.From, packet.Timestamp)

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) ingestPacket(buf []byte, from net.Addr, timestamp time.Time) {
	var (
		packetLabel string
		err         error
	)
	buf, packetLabel, err = RemoveLabelHeaderFromPacket(buf)
	if err != nil {
		m.logger.Error("failed to remove packet label header", "error", err, addrAttr(from))
		return
	}

	if m.config.SkipInboundLabelCheck {
		if packetLabel != "" {
			m.logger.Error("unexpected double packet label header", addrAttr(from))
			return
		}
		// Set this from config so that the auth data assertions work below.
		packetLabel = m.config.Label
	}

	if m.config.Label != packetLabel {
		m.logger.Error("discarding packet with unacceptable label", "label", packetLabel, addrAttr(from))
		return
	}

	// Check if encryption is enabled
	if m.config.EncryptionEnabled() {
		// Decrypt the payload
		authData := []byte(packetLabel)
		plain, err := openPayload(m.config.Keyring.getAEADs(), buf, authData)
		if err != nil {
			if !m.config.GossipVerifyIncoming {
				// Treat the message as plaintext
				plain = buf
			} else {
				m.logger.Error("decrypt packet failed", "error", err, addrAttr(from))
				return
			}
		}

		// Continue processing the plaintext buffer
		buf = plain
	}

	// See if there's a checksum included to verify the contents of the message
	if len(buf) >= 5 && messageType(buf[0]) == hasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			m.logger.Warn("invalid checksum for UDP packet", "crc", crc, "expected", expected, addrAttr(from))
			return
		}
		m.handleCommand(buf[5:], from, timestamp)
	} else {
		m.handleCommand(buf, from, timestamp)
	}
}

// envelope records which wrapping message types enclose a packet part.
// Senders compress at most once and never nest compound messages, so a
// repeated envelope is malformed; refusing it bounds the decompression work
// and recursion depth a single packet can cause.
type envelope uint8

const (
	inCompound envelope = 1 << iota
	inCompress
)

func (m *Memberlist) handleCommand(buf []byte, from net.Addr, timestamp time.Time) {
	m.handleCommandIn(buf, from, timestamp, 0)
}

func (m *Memberlist) handleCommandIn(buf []byte, from net.Addr, timestamp time.Time, env envelope) {
	if len(buf) < 1 {
		m.logger.Error("missing message type byte", addrAttr(from))
		return
	}
	// Decode the message type
	msgType := messageType(buf[0])
	buf = buf[1:]

	// Switch on the msgType
	switch msgType {
	case compoundMsg:
		if env&inCompound != 0 {
			m.logger.Error("nested compound message", addrAttr(from))
			return
		}
		m.handleCompound(buf, from, timestamp, env|inCompound)
	case compressMsg:
		if env&inCompress != 0 {
			m.logger.Error("nested compressed message", addrAttr(from))
			return
		}
		m.handleCompressed(buf, from, timestamp, env|inCompress)

	case pingMsg:
		m.handlePing(buf, from)
	case indirectPingMsg:
		m.handleIndirectPing(buf, from)
	case ackRespMsg:
		m.handleAck(buf, from, timestamp)
	case nackRespMsg:
		m.handleNack(buf, from)

	case suspectMsg:
		fallthrough
	case aliveMsg:
		fallthrough
	case deadMsg:
		fallthrough
	case userMsg:
		// Determine the message queue, prioritize alive
		queue := m.lowPriorityMsgQueue
		if msgType == aliveMsg {
			queue = m.highPriorityMsgQueue
		}

		// Check for overflow and append if not full
		m.msgQueueLock.Lock()
		if queue.Len() >= m.config.HandoffQueueDepth {
			m.logger.Warn("handler queue full, dropping message", "type", msgType, addrAttr(from))
		} else {
			queue.PushBack(msgHandoff{msgType, buf, from})
		}
		m.msgQueueLock.Unlock()

		// Notify of pending message
		select {
		case m.handoffCh <- struct{}{}:
		default:
		}

	default:
		m.logger.Error("message type not supported", "type", msgType, addrAttr(from))
	}
}

// getNextMessage returns the next message to process in priority order, using LIFO
func (m *Memberlist) getNextMessage() (msgHandoff, bool) {
	m.msgQueueLock.Lock()
	defer m.msgQueueLock.Unlock()

	if el := m.highPriorityMsgQueue.Back(); el != nil {
		m.highPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	} else if el := m.lowPriorityMsgQueue.Back(); el != nil {
		m.lowPriorityMsgQueue.Remove(el)
		msg := el.Value.(msgHandoff)
		return msg, true
	}
	return msgHandoff{}, false
}

// packetHandler is a long running goroutine that processes messages received
// over the packet interface, but is decoupled from the listener to avoid
// blocking the listener which may cause ping/ack messages to be delayed.
func (m *Memberlist) packetHandler() {
	for {
		select {
		case <-m.handoffCh:
			for {
				msg, ok := m.getNextMessage()
				if !ok {
					break
				}
				msgType := msg.msgType
				buf := msg.buf
				from := msg.from

				switch msgType {
				case suspectMsg:
					m.handleSuspect(buf, from)
				case aliveMsg:
					m.handleAlive(buf, from)
				case deadMsg:
					m.handleDead(buf, from)
				case userMsg:
					m.handleUser(buf, from)
				default:
					m.logger.Error("message type not supported by the packet handler", "type", msgType, addrAttr(from))
				}
			}

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) handleCompound(buf []byte, from net.Addr, timestamp time.Time, env envelope) {
	// Decode the parts
	trunc, parts, err := decodeCompoundMessage(buf)
	if err != nil {
		m.logger.Error("failed to decode compound request", "error", err, addrAttr(from))
		return
	}

	// Log any truncation
	if trunc > 0 {
		m.logger.Warn("compound request had truncated messages", "truncated", trunc, addrAttr(from))
	}

	// Handle each message
	for _, part := range parts {
		m.handleCommandIn(part, from, timestamp, env)
	}
}

func (m *Memberlist) handlePing(buf []byte, from net.Addr) {
	var p ping
	if err := decode(buf, &p); err != nil {
		m.logger.Error("failed to decode ping request", "error", err, addrAttr(from))
		return
	}
	// If node is provided, verify that it is for us
	if p.Node != "" && p.Node != m.config.Name {
		m.logger.Warn("got ping for unexpected node", "node", p.Node, addrAttr(from))
		return
	}
	var ack ackResp
	ack.SeqNo = p.SeqNo
	if m.config.Ping != nil {
		m.runCallback(func() { ack.Payload = m.config.Ping.AckPayload() })
	}

	addr := ""
	if len(p.SourceAddr) > 0 && p.SourcePort > 0 {
		addr = joinHostPort(net.IP(p.SourceAddr).String(), p.SourcePort)
	} else {
		addr = from.String()
	}

	a := Address{
		Addr: addr,
		Name: p.SourceNode,
	}
	if err := m.encodeAndSendMsg(a, ackRespMsg, &ack); err != nil {
		m.logger.Error("failed to send ack", "error", err, addrAttr(from))
	}
}

func (m *Memberlist) handleIndirectPing(buf []byte, from net.Addr) {
	var ind indirectPingReq
	if err := decode(buf, &ind); err != nil {
		m.logger.Error("failed to decode indirect ping request", "error", err, addrAttr(from))
		return
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port.
	if m.ProtocolVersion() < 2 || ind.Port == 0 {
		ind.Port = uint16(m.config.BindPort)
	}

	// Send a ping to the correct host.
	localSeqNo := m.nextSeqNo()
	selfAddr, selfPort := m.getAdvertise()
	ping := ping{
		SeqNo: localSeqNo,
		Node:  ind.Node,
		// The outbound message is addressed FROM us.
		SourceAddr: selfAddr,
		SourcePort: selfPort,
		SourceNode: m.config.Name,
	}

	// Forward the ack back to the requestor. If the request encodes an origin
	// use that otherwise assume that the other end of the UDP socket is
	// usable.
	indAddr := ""
	if len(ind.SourceAddr) > 0 && ind.SourcePort > 0 {
		indAddr = joinHostPort(net.IP(ind.SourceAddr).String(), ind.SourcePort)
	} else {
		indAddr = from.String()
	}

	// Setup a response handler to relay the ack
	cancelCh := make(chan struct{})
	respHandler := func(payload []byte, timestamp time.Time) {
		// Try to prevent the nack if we've caught it in time.
		close(cancelCh)

		ack := ackResp{SeqNo: ind.SeqNo}
		a := Address{
			Addr: indAddr,
			Name: ind.SourceNode,
		}
		if err := m.encodeAndSendMsg(a, ackRespMsg, &ack); err != nil {
			m.logger.Error("failed to forward ack", "error", err, "from", indAddr)
		}
	}
	m.setAckHandler(localSeqNo, respHandler, m.config.ProbeTimeout)

	// Send the ping.
	addr := joinHostPort(net.IP(ind.Target).String(), ind.Port)
	a := Address{
		Addr: addr,
		Name: ind.Node,
	}
	if err := m.encodeAndSendMsg(a, pingMsg, &ping); err != nil {
		m.logger.Error("failed to send indirect ping", "error", err, "from", indAddr)
	}

	// Setup a timer to fire off a nack if no ack is seen in time.
	if ind.Nack {
		m.goBackground(func() {
			select {
			case <-cancelCh:
				return
			case <-time.After(m.config.ProbeTimeout):
				nack := nackResp{SeqNo: ind.SeqNo}
				a := Address{
					Addr: indAddr,
					Name: ind.SourceNode,
				}
				if err := m.encodeAndSendMsg(a, nackRespMsg, &nack); err != nil {
					m.logger.Error("failed to send nack", "error", err, "from", indAddr)
				}
			}
		})
	}
}

func (m *Memberlist) handleAck(buf []byte, from net.Addr, timestamp time.Time) {
	var ack ackResp
	if err := decode(buf, &ack); err != nil {
		m.logger.Error("failed to decode ack response", "error", err, addrAttr(from))
		return
	}
	m.invokeAckHandler(ack, timestamp)
}

func (m *Memberlist) handleNack(buf []byte, from net.Addr) {
	var nack nackResp
	if err := decode(buf, &nack); err != nil {
		m.logger.Error("failed to decode nack response", "error", err, addrAttr(from))
		return
	}
	m.invokeNackHandler(nack)
}

func (m *Memberlist) handleSuspect(buf []byte, from net.Addr) {
	var sus suspect
	if err := decode(buf, &sus); err != nil {
		m.logger.Error("failed to decode suspect message", "error", err, addrAttr(from))
		return
	}
	m.suspectNode(&sus)
}

// ensureCanConnect return the IP from a RemoteAddress
// return error if this client must not connect
func (m *Memberlist) ensureCanConnect(from net.Addr) error {
	if !m.config.IPMustBeChecked() {
		return nil
	}
	source := from.String()
	if source == "pipe" {
		return nil
	}
	host, _, err := net.SplitHostPort(source)
	if err != nil {
		return err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("cannot parse IP from %s", host)
	}
	return m.config.IPAllowed(ip)
}

func (m *Memberlist) handleAlive(buf []byte, from net.Addr) {
	if err := m.ensureCanConnect(from); err != nil {
		m.logger.Debug("blocked alive message", "error", err, addrAttr(from))
		return
	}
	var live alive
	if err := decode(buf, &live); err != nil {
		m.logger.Error("failed to decode alive message", "error", err, addrAttr(from))
		return
	}
	if m.config.IPMustBeChecked() {
		innerIP := net.IP(live.Addr)
		if innerIP != nil {
			if err := m.config.IPAllowed(innerIP); err != nil {
				m.logger.Debug("blocked alive message address", "addr", innerIP, "error", err, addrAttr(from))
				return
			}
		}
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port
	if m.ProtocolVersion() < 2 || live.Port == 0 {
		live.Port = uint16(m.config.BindPort)
	}

	m.aliveNode(&live, false)
}

func (m *Memberlist) handleDead(buf []byte, from net.Addr) {
	var d dead
	if err := decode(buf, &d); err != nil {
		m.logger.Error("failed to decode dead message", "error", err, addrAttr(from))
		return
	}
	m.deadNode(&d)
}

// handleUser is used to notify channels of incoming user data
func (m *Memberlist) handleUser(buf []byte, _ net.Addr) {
	d := m.config.Delegate
	if d != nil {
		m.runCallback(func() { d.NotifyMsg(buf) })
	}
}

// handleCompressed is used to unpack a compressed message
func (m *Memberlist) handleCompressed(buf []byte, from net.Addr, timestamp time.Time, env envelope) {
	// Try to decode the payload
	payload, err := decompressPayload(buf)
	if err != nil {
		m.logger.Error("failed to decompress payload", "error", err, addrAttr(from))
		return
	}

	// Recursively handle the payload
	m.handleCommandIn(payload, from, timestamp, env)
}

// encodeAndSendMsg is used to combine the encoding and sending steps
func (m *Memberlist) encodeAndSendMsg(a Address, msgType messageType, msg wire.Message) error {
	return m.sendMsg(a, encode(msgType, msg))
}

// sendMsg is used to send a message via packet to another host. It will
// opportunistically create a compoundMsg and piggy back other broadcasts.
func (m *Memberlist) sendMsg(a Address, msg []byte) error {
	// Check if we can piggy back any messages
	// Reserve the primary message's own compound part length and the CRC
	// header rawSendMsgPacket may append after budgeting.
	bytesAvail := m.packetBufferSize - len(msg) - compoundHeaderOverhead - compoundOverhead - crcOverhead - labelOverhead(m.config.Label)
	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {
		bytesAvail -= encryptOverhead(m.encryptionVersion())
	}
	extra := m.getBroadcasts(compoundOverhead, bytesAvail)

	// Fast path if nothing to piggypack
	if len(extra) == 0 {
		return m.rawSendMsgPacket(a, nil, msg)
	}

	// Join all the messages
	msgs := make([][]byte, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	// Create and send a compound message
	return m.rawSendMsgPacket(a, nil, makeCompoundMessage(msgs))
}

// rawSendMsgPacket is used to send message via packet to another host without
// modification, other than compression or encryption if enabled.
func (m *Memberlist) rawSendMsgPacket(a Address, node *Node, msg []byte) error {
	if a.Name == "" && m.config.RequireNodeNames {
		return errNodeNamesAreRequired
	}

	// Check if we have compression enabled (and the message could shrink)
	if m.config.EnableCompression && len(msg) > maxIncompressiblePacket {
		buf, err := compressPayload(msg)
		if err != nil {
			m.logger.Warn("failed to compress payload", "error", err)
		} else if len(buf) < len(msg) {
			// Only use compression if it reduced the size
			msg = buf
		}
	}

	// Try to look up the destination node. Note this will only work if the
	// bare ip address is used as the node name, which is not guaranteed.
	if node == nil {
		toAddr, _, err := net.SplitHostPort(a.Addr)
		if err != nil {
			m.logger.Error("failed to parse address", "addr", a.Addr, "error", err)
			return err
		}
		m.nodeLock.RLock()
		nodeState, ok := m.nodeMap[toAddr]
		if ok {
			node = &nodeState.Node
		}
		m.nodeLock.RUnlock()
	}

	// Add a CRC to the end of the payload if the recipient understands
	// ProtocolVersion >= 5
	if node != nil && node.PMax >= 5 {
		crc := crc32.ChecksumIEEE(msg)
		header := make([]byte, 5, 5+len(msg))
		header[0] = byte(hasCrcMsg)
		binary.BigEndian.PutUint32(header[1:], crc)
		msg = append(header, msg...)
	}

	// Check if we have encryption enabled
	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {
		// Encrypt the payload
		sealed, err := sealPayload(m.encryptionVersion(), m.config.Keyring.primaryAEAD(), msg, []byte(m.config.Label), nil)
		if err != nil {
			m.logger.Error("encryption of message failed", "error", err)
			return err
		}
		msg = sealed
	}

	m.metrics.counter(keyUDPSent, float32(len(msg)))
	_, err := m.transport.WriteToAddress(msg, a)
	return err
}

// rawSendMsgStream is used to stream a message to another host without
// modification, other than applying compression and encryption if enabled.
func (m *Memberlist) rawSendMsgStream(conn net.Conn, sendBuf []byte, streamLabel string) error {
	// Check if compression is enabled
	if m.config.EnableCompression {
		compBuf, err := compressPayload(sendBuf)
		if err != nil {
			m.logger.Error("failed to compress payload", "error", err)
		} else {
			sendBuf = compBuf
		}
	}

	// Check if encryption is enabled
	if m.config.EncryptionEnabled() && m.config.GossipVerifyOutgoing {
		crypt, err := m.encryptLocalState(sendBuf, streamLabel)
		if err != nil {
			m.logger.Error("failed to encrypt local state", "error", err)
			return err
		}
		sendBuf = crypt
	}

	// Write out the entire send buffer
	m.metrics.counter(keyTCPSent, float32(len(sendBuf)))

	if n, err := conn.Write(sendBuf); err != nil {
		return err
	} else if n != len(sendBuf) {
		return fmt.Errorf("only %d of %d bytes written", n, len(sendBuf))
	}

	return nil
}

// sendUserMsg is used to stream a user message to another host.
func (m *Memberlist) sendUserMsg(a Address, sendBuf []byte) error {
	if a.Name == "" && m.config.RequireNodeNames {
		return errNodeNamesAreRequired
	}

	conn, err := m.transport.DialAddressTimeout(a, m.config.TCPTimeout)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	out := make([]byte, 0, 16+len(sendBuf))
	out = append(out, byte(userMsg))
	out = userMsgHeader{UserMsgLen: len(sendBuf)}.AppendMsgpack(out)
	out = append(out, sendBuf...)
	return m.rawSendMsgStream(conn, out, m.config.Label)
}

// sendAndReceiveState is used to initiate a push/pull over a stream with a
// remote host.
func (m *Memberlist) sendAndReceiveState(a Address, join bool) ([]pushNodeState, []byte, error) {
	if a.Name == "" && m.config.RequireNodeNames {
		return nil, nil, errNodeNamesAreRequired
	}

	// Attempt to connect
	conn, err := m.transport.DialAddressTimeout(a, m.config.TCPTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
	m.logger.Debug("initiating push/pull sync", "node", a.Name, "addr", conn.RemoteAddr())
	m.metrics.counter(keyTCPConnect, 1)

	// Send our state
	if err := m.sendLocalState(conn, join, m.config.Label); err != nil {
		return nil, nil, err
	}

	if err := conn.SetDeadline(time.Now().Add(m.config.TCPTimeout)); err != nil {
		m.logger.Error("could not set the stream deadline", "error", err)
	}
	msgType, dec, err := m.readStream(conn, m.config.Label)
	if err != nil {
		return nil, nil, err
	}

	if msgType == errMsg {
		var resp errResp
		if err := resp.DecodeMsgpack(dec); err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("remote error: %v", resp.Error)
	}

	// Quit if not push/pull
	if msgType != pushPullMsg {
		err := fmt.Errorf("received invalid msgType (%d), expected pushPullMsg (%d) from %s", msgType, pushPullMsg, conn.RemoteAddr())
		return nil, nil, err
	}

	// Read remote state
	_, remoteNodes, userState, err := m.readRemoteState(dec)
	return remoteNodes, userState, err
}

// sendLocalState is invoked to send our local state over a stream connection.
func (m *Memberlist) sendLocalState(conn net.Conn, join bool, streamLabel string) error {
	// Setup a deadline
	if err := conn.SetDeadline(time.Now().Add(m.config.TCPTimeout)); err != nil {
		m.logger.Error("could not set the stream deadline", "error", err)
	}

	// Prepare the local node state
	m.nodeLock.RLock()
	localNodes := make([]pushNodeState, len(m.nodes))
	for idx, n := range m.nodes {
		localNodes[idx].Name = n.Name
		localNodes[idx].Addr = n.Addr
		localNodes[idx].Port = n.Port
		localNodes[idx].Incarnation = n.Incarnation
		localNodes[idx].State = int(n.State)
		localNodes[idx].Meta = n.Meta
		localNodes[idx].Vsn = []uint8{
			n.PMin, n.PMax, n.PCur,
			n.DMin, n.DMax, n.DCur,
		}
	}
	m.nodeLock.RUnlock()

	var nodeStateCounts [len(nodeStates)]int
	for _, n := range localNodes {
		if int(n.State) < len(nodeStateCounts) {
			nodeStateCounts[n.State]++
		}
	}
	m.metrics.nodeInstances(nodeStateCounts)

	// Get the delegate state
	var userData []byte
	if m.config.Delegate != nil {
		m.runCallback(func() { userData = m.config.Delegate.LocalState(join) })
	}

	// Encode the message type, header, node states and user state.
	out := make([]byte, 0, 64+64*len(localNodes)+len(userData))
	out = append(out, byte(pushPullMsg))
	out = pushPullHeader{Nodes: len(localNodes), UserStateLen: len(userData), Join: join}.AppendMsgpack(out)
	for i := range localNodes {
		out = localNodes[i].AppendMsgpack(out)
	}
	out = append(out, userData...)

	m.metrics.gauge(keySizeLocal, float32(len(out)))
	return m.rawSendMsgStream(conn, out, streamLabel)
}

// encryptLocalState is used to help encrypt local state before sending
func (m *Memberlist) encryptLocalState(sendBuf []byte, streamLabel string) ([]byte, error) {
	encVsn := m.encryptionVersion()
	encLen := encryptedLength(encVsn, len(sendBuf))
	out := make([]byte, 5, 5+encLen)

	// The encryptMsg byte and the size of the message
	out[0] = byte(encryptMsg)
	binary.BigEndian.PutUint32(out[1:], uint32(encLen))

	// Authenticated Data is:
	//
	//   [messageType; byte] [messageLength; uint32] [stream_label; optional]
	//
	dataBytes := append(out[:5:5], streamLabel...)

	// Append the encrypted cipher text
	return sealPayload(encVsn, m.config.Keyring.primaryAEAD(), sendBuf, dataBytes, out)
}

// decryptRemoteState is used to help decrypt the remote state
func (m *Memberlist) decryptRemoteState(bufConn io.Reader, streamLabel string) ([]byte, error) {
	// Read in enough to determine message length
	cipherText := bytes.NewBuffer(nil)
	cipherText.WriteByte(byte(encryptMsg))
	_, err := io.CopyN(cipherText, bufConn, 4)
	if err != nil {
		return nil, err
	}

	// Ensure we aren't asked to download too much. This is to guard against
	// an attack vector where a huge amount of state is sent
	moreBytes := binary.BigEndian.Uint32(cipherText.Bytes()[1:5])
	m.metrics.sample(keySizeRemote, float32(moreBytes))

	if moreBytes > maxPushStateBytes {
		return nil, fmt.Errorf("remote node state is larger than limit (%d)", moreBytes)

	}

	//Start reporting the size before you cross the limit
	if moreBytes > uint32(math.Floor(.6*maxPushStateBytes)) {
		m.logger.Warn("remote node state size is approaching the limit", "size", moreBytes, "limit", maxPushStateBytes)
	}

	// Read in the rest of the payload
	_, err = io.CopyN(cipherText, bufConn, int64(moreBytes))
	if err != nil {
		return nil, err
	}

	// Decrypt the cipherText with some authenticated data
	//
	// Authenticated Data is:
	//
	//   [messageType; byte] [messageLength; uint32] [label_data; optional]
	//
	dataBytes := appendBytes(cipherText.Bytes()[:5], []byte(streamLabel))
	cipherBytes := cipherText.Bytes()[5:]

	// Decrypt the payload
	return openPayload(m.config.Keyring.getAEADs(), cipherBytes, dataBytes)
}

// readStream is used to read messages from a stream connection, decrypting and
// decompressing the stream if necessary. It returns the message type and a
// decoder positioned at the message body.
//
// The provided streamLabel if present will be authenticated during decryption
// of each message.
func (m *Memberlist) readStream(conn net.Conn, streamLabel string) (messageType, *msgpack.Decoder, error) {
	// Created a buffered reader. Every stream message is bounded: encrypted
	// and compressed payloads carry their own limits, and this caps the
	// plaintext form, whose size is otherwise only implied by the headers
	// and the variable-length fields inside it.
	bufConn := bufio.NewReader(io.LimitReader(conn, maxStreamMessageBytes))

	// Read the message type
	b, err := bufConn.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	msgType := messageType(b)
	dec := msgpack.NewStreamDecoder(bufConn)

	// Check if the message is encrypted
	if msgType == encryptMsg {
		if !m.config.EncryptionEnabled() {
			return 0, nil,
				fmt.Errorf("remote state is encrypted and encryption is not configured")
		}

		plain, err := m.decryptRemoteState(bufConn, streamLabel)
		if err != nil {
			return 0, nil, err
		}
		if len(plain) == 0 {
			return 0, nil, errors.New("decrypted message is empty")
		}

		// Reset message type and decoder
		msgType = messageType(plain[0])
		dec = msgpack.NewDecoder(plain[1:])
	} else if m.config.EncryptionEnabled() && m.config.GossipVerifyIncoming {
		return 0, nil,
			fmt.Errorf("encryption is configured but remote state is not encrypted")
	}

	// Check if we have a compressed message
	if msgType == compressMsg {
		var c compress
		if err := c.DecodeMsgpack(dec); err != nil {
			return 0, nil, err
		}
		decomp, err := decompressBuffer(&c, maxDecompressedBytes)
		if err != nil {
			return 0, nil, err
		}
		if len(decomp) == 0 {
			return 0, nil, errors.New("decompressed message is empty")
		}

		// Reset the message type and decoder
		msgType = messageType(decomp[0])
		dec = msgpack.NewDecoder(decomp[1:])
	}

	return msgType, dec, nil
}

// readRemoteState is used to read the remote state from a connection
func (m *Memberlist) readRemoteState(dec *msgpack.Decoder) (bool, []pushNodeState, []byte, error) {
	// Read the push/pull header
	var header pushPullHeader
	if err := header.DecodeMsgpack(dec); err != nil {
		return false, nil, nil, err
	}

	if header.Nodes < 0 || header.Nodes > maxPushStateNodes {
		return false, nil, nil, fmt.Errorf("number of nodes in header (%d) exceeds limit", header.Nodes)
	}
	if header.UserStateLen < 0 || header.UserStateLen > maxPushStateBytes {
		return false, nil, nil, fmt.Errorf("user state length (%d) exceeds limit", header.UserStateLen)
	}

	// Decode the states. Storage grows with the states actually received,
	// never from the declared count: a short header must not buy a large
	// allocation.
	remoteNodes := make([]pushNodeState, 0, min(header.Nodes, pushStatePrealloc))
	for range header.Nodes {
		var n pushNodeState
		if err := n.DecodeMsgpack(dec); err != nil {
			return false, nil, nil, err
		}
		remoteNodes = append(remoteNodes, n)
	}

	// Read the remote user state into a buffer
	var userBuf []byte
	if header.UserStateLen > 0 {
		var err error
		if userBuf, err = dec.ReadRaw(header.UserStateLen); err != nil {
			return false, nil, nil, fmt.Errorf("failed to read full user state (%d bytes): %w", header.UserStateLen, err)
		}
	}

	// For proto versions < 2, there is no port provided. Mask old
	// behavior by using the configured port
	for idx := range remoteNodes {
		if m.ProtocolVersion() < 2 || remoteNodes[idx].Port == 0 {
			remoteNodes[idx].Port = uint16(m.config.BindPort)
		}
	}

	return header.Join, remoteNodes, userBuf, nil
}

// mergeRemoteState is used to merge the remote state with our local state
func (m *Memberlist) mergeRemoteState(join bool, remoteNodes []pushNodeState, userBuf []byte) error {
	// Advisory early check so the merge delegate never sees a
	// protocol-invalid state; re-verified under mergeLock below.
	if err := m.verifyProtocol(remoteNodes); err != nil {
		return err
	}

	// Invoke the merge delegate if any — outside mergeLock: a delegate may
	// re-enter (e.g. call Join synchronously), which reaches this function
	// again and would self-deadlock on a held lock.
	if join && m.config.Merge != nil {
		nodes := make([]*Node, len(remoteNodes))
		for idx, n := range remoteNodes {
			nodes[idx] = &Node{
				Name:  n.Name,
				Addr:  n.Addr,
				Port:  n.Port,
				Meta:  n.Meta,
				State: NodeStateType(n.State),
			}
			// verifyProtocol refused malformed vectors above.
			vsn, _ := parseVsn(n.Vsn)
			nodes[idx].setVersions(vsn)
		}
		var err error
		m.runCallback(func() { err = m.config.Merge.NotifyMerge(nodes) })
		if err != nil {
			return err
		}
	}

	// Serialize protocol verification with the state merge: two concurrent
	// exchanges (parallel push/pull initiations, or concurrent inbound
	// handlers) whose states are individually compatible but mutually
	// incompatible could otherwise both pass verifyProtocol before either
	// merges, admitting a mixed-protocol membership the sequential order
	// would reject. Only network I/O and delegate callbacks stay parallel.
	err := func() error {
		m.mergeLock.Lock()
		defer m.mergeLock.Unlock()

		if err := m.verifyProtocol(remoteNodes); err != nil {
			return err
		}
		m.mergeState(remoteNodes)
		return nil
	}()
	if err != nil {
		return err
	}

	// Invoke the delegate for user state — outside the membership
	// invariant; the app serializes its own state if it needs to.
	if userBuf != nil && m.config.Delegate != nil {
		m.runCallback(func() { m.config.Delegate.MergeRemoteState(userBuf, join) })
	}
	return nil
}

// readUserMsg is used to decode a userMsg from a stream.
func (m *Memberlist) readUserMsg(dec *msgpack.Decoder) error {
	// Read the user message header
	var header userMsgHeader
	if err := header.DecodeMsgpack(dec); err != nil {
		return err
	}

	if header.UserMsgLen < 0 || header.UserMsgLen > maxUserMsgBytes {
		return fmt.Errorf("user message length (%d) exceeds limit", header.UserMsgLen)
	}
	if header.UserMsgLen == 0 {
		return nil
	}

	// Read the user message into a buffer
	userBuf, err := dec.ReadRaw(header.UserMsgLen)
	if err != nil {
		return fmt.Errorf("failed to read full user message (%d bytes): %w", header.UserMsgLen, err)
	}
	if d := m.config.Delegate; d != nil {
		m.runCallback(func() { d.NotifyMsg(userBuf) })
	}
	return nil
}

// sendPingAndWaitForAck makes a stream connection to the given address, sends
// a ping, and waits for an ack. All of this is done as a series of blocking
// operations, given the deadline. The bool return parameter is true if we
// we able to round trip a ping to the other node.
func (m *Memberlist) sendPingAndWaitForAck(a Address, ping ping, deadline time.Time) (bool, error) {
	if a.Name == "" && m.config.RequireNodeNames {
		return false, errNodeNamesAreRequired
	}

	conn, err := m.transport.DialAddressTimeout(a, time.Until(deadline))
	if err != nil {
		// If the node is actually dead we expect this to fail, so we
		// shouldn't spam the logs with it. After this point, errors
		// with the connection are real, unexpected errors and should
		// get propagated up.
		return false, nil
	}
	defer func() {
		_ = conn.Close()
	}()
	_ = conn.SetDeadline(deadline)

	if err = m.rawSendMsgStream(conn, encode(pingMsg, ping), m.config.Label); err != nil {
		return false, err
	}

	msgType, dec, err := m.readStream(conn, m.config.Label)
	if err != nil {
		return false, err
	}

	if msgType != ackRespMsg {
		return false, fmt.Errorf("unexpected msgType (%d) in reply to a ping from %s", msgType, conn.RemoteAddr())
	}

	var ack ackResp
	if err = ack.DecodeMsgpack(dec); err != nil {
		return false, err
	}

	if ack.SeqNo != ping.SeqNo {
		return false, fmt.Errorf("sequence number from ack (%d) doesn't match ping (%d)", ack.SeqNo, ping.SeqNo)
	}

	return true, nil
}
