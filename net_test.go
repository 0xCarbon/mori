// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0xCarbon/mori/internal/msgpack"

	iretry "github.com/0xCarbon/mori/internal/retry"
)

// As a regression we left this test very low-level and network-ey, even after
// we abstracted the transport. We added some basic network-free transport tests
// in transport_test.go to prove that we didn't hard code some network stuff
// outside of NetTransport.

func TestHandleCompoundPing(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf := encode(pingMsg, ping)

	// Make a compound message
	compound := makeCompoundMessage([][]byte{buf, buf, buf})

	// Send compound version
	addr := &net.UDPAddr{IP: net.ParseIP(m.config.BindAddr), Port: m.config.BindPort}
	_, err := udp.WriteTo(compound, addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for responses
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	for range 3 {
		in := make([]byte, 1500)
		n, _, err := udp.ReadFrom(in)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		in = in[0:n]

		msgType := messageType(in[0])
		if msgType != ackRespMsg {
			t.Fatalf("bad response %v", in)
		}

		var ack ackResp
		if err := decode(in[1:], &ack); err != nil {
			t.Fatalf("unexpected err %s", err)
		}

		if ack.SeqNo != 42 {
			t.Fatalf("bad sequence no")
		}
	}

	doneCh <- struct{}{}
}

func TestHandlePing(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf := encode(pingMsg, ping)

	// Send
	addr := &net.UDPAddr{IP: net.ParseIP(m.config.BindAddr), Port: m.config.BindPort}
	_, err := udp.WriteTo(buf, addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := messageType(in[0])
	if msgType != ackRespMsg {
		t.Fatalf("bad response %v", in)
	}

	var ack ackResp
	if err := decode(in[1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 42 {
		t.Fatalf("bad sequence no")
	}

	doneCh <- struct{}{}
}

func TestHandlePing_WrongNode(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping, wrong node!
	ping := ping{
		SeqNo:      42,
		Node:       m.config.Name + "-bad",
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf := encode(pingMsg, ping)

	// Send
	addr := &net.UDPAddr{IP: net.ParseIP(m.config.BindAddr), Port: m.config.BindPort}
	_, err := udp.WriteTo(buf, addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	_ = udp.SetDeadline(time.Now().Add(50 * time.Millisecond))
	in := make([]byte, 1500)
	_, _, err = udp.ReadFrom(in)

	// Should get an i/o timeout
	if err == nil {
		t.Fatalf("expected err %s", err)
	}
}

func TestHandleIndirectPing(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode an indirect ping
	ind := indirectPingReq{
		SeqNo:      100,
		Target:     net.ParseIP(m.config.BindAddr),
		Port:       uint16(m.config.BindPort),
		Node:       m.config.Name,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf := encode(indirectPingMsg, &ind)

	// Send
	addr := &net.UDPAddr{IP: net.ParseIP(m.config.BindAddr), Port: m.config.BindPort}
	_, err := udp.WriteTo(buf, addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := messageType(in[0])
	if msgType != ackRespMsg {
		t.Fatalf("bad response %v", in)
	}

	var ack ackResp
	if err := decode(in[1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 100 {
		t.Fatalf("bad sequence no")
	}

	doneCh <- struct{}{}
}

func TestTCPPing(t *testing.T) {
	var tcp *net.TCPListener
	var tcpAddr *net.TCPAddr
	for port := 60000; port < 61000; port++ {
		tcpAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err == nil {
			tcp = tcpLn
			break
		}
	}
	if tcp == nil {
		t.Fatalf("no tcp listener")
	}

	tcpAddr2 := Address{Addr: tcpAddr.String(), Name: "test"}

	// Note that tcp gets closed in the last test, so we avoid a deferred
	// Close() call here.

	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	pingTimeout := m.config.ProbeInterval
	pingTimeMax := m.config.ProbeInterval + 10*time.Millisecond

	// Do a normal round trip.
	pingOut := ping{SeqNo: 23, Node: "mongo"}
	pingErrCh := make(chan error, 1)
	go func() {
		_ = tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		msgType, dec, err := m.readStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		if msgType != pingMsg {
			pingErrCh <- fmt.Errorf("expecting ping, got message type (%d)", msgType)
			return
		}

		var pingIn ping
		if err := pingIn.DecodeMsgpack(dec); err != nil {
			pingErrCh <- fmt.Errorf("failed to decode ping: %s", err)
			return
		}

		if pingIn.SeqNo != pingOut.SeqNo {
			pingErrCh <- fmt.Errorf("sequence number isn't correct (%d) vs (%d)", pingIn.SeqNo, pingOut.SeqNo)
			return
		}

		if pingIn.Node != pingOut.Node {
			pingErrCh <- fmt.Errorf("node name isn't correct (%s) vs (%s)", pingIn.Node, pingOut.Node)
			return
		}

		ack := ackResp{SeqNo: pingIn.SeqNo}
		out := encode(ackRespMsg, &ack)
		err = m.rawSendMsgStream(conn, out, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send ack: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	deadline := time.Now().Add(pingTimeout)
	didContact, err := m.sendPingAndWaitForAck(tcpAddr2, pingOut, deadline)
	if err != nil {
		t.Fatalf("error trying to ping: %s", err)
	}
	if !didContact {
		t.Fatalf("expected successful ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure a mis-matched sequence number is caught.
	go func() {
		_ = tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		_, dec, err := m.readStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		var pingIn ping
		if err := pingIn.DecodeMsgpack(dec); err != nil {
			pingErrCh <- fmt.Errorf("failed to decode ping: %s", err)
			return
		}

		ack := ackResp{SeqNo: pingIn.SeqNo + 1}
		out := encode(ackRespMsg, &ack)
		err = m.rawSendMsgStream(conn, out, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send ack: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	deadline = time.Now().Add(pingTimeout)
	didContact, err = m.sendPingAndWaitForAck(tcpAddr2, pingOut, deadline)
	if err == nil || !strings.Contains(err.Error(), "sequence number") {
		t.Fatalf("expected an error from mis-matched sequence number")
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure an unexpected message type is handled gracefully.
	go func() {
		_ = tcp.SetDeadline(time.Now().Add(pingTimeMax))
		conn, err := tcp.AcceptTCP()
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to connect: %s", err)
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		_, _, err = m.readStream(conn, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to read ping: %s", err)
			return
		}

		bogus := indirectPingReq{}
		out := encode(indirectPingMsg, &bogus)
		err = m.rawSendMsgStream(conn, out, "")
		if err != nil {
			pingErrCh <- fmt.Errorf("failed to send bogus msg: %s", err)
			return
		}
		pingErrCh <- nil
	}()
	deadline = time.Now().Add(pingTimeout)
	didContact, err = m.sendPingAndWaitForAck(tcpAddr2, pingOut, deadline)
	if err == nil || !strings.Contains(err.Error(), "unexpected msgType") {
		t.Fatalf("expected an error from bogus message")
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if err = <-pingErrCh; err != nil {
		t.Fatal(err)
	}

	// Make sure failed I/O respects the deadline. In this case we try the
	// common case of the receiving node being totally down.
	_ = tcp.Close()
	deadline = time.Now().Add(pingTimeout)
	startPing := time.Now()
	didContact, err = m.sendPingAndWaitForAck(tcpAddr2, pingOut, deadline)
	pingTime := time.Since(startPing)
	if err != nil {
		t.Fatalf("expected no error during ping on closed socket, got: %s", err)
	}
	if didContact {
		t.Fatalf("expected failed ping")
	}
	if pingTime > pingTimeMax {
		t.Fatalf("took too long to fail ping, %9.6f", pingTime.Seconds())
	}
}

func TestTCPPushPull(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	m.nodes = append(m.nodes, &nodeState{
		Name:        "Test 0",
		Addr:        net.ParseIP(m.config.BindAddr),
		Port:        uint16(m.config.BindPort),
		Incarnation: 0,
		State:       StateSuspect,
		StateChange: time.Now().Add(-1 * time.Second),
	})

	addr := net.JoinHostPort(m.config.BindAddr, strconv.Itoa(m.config.BindPort))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	localNodes := make([]pushNodeState, 3)
	localNodes[0].Name = "Test 0"
	localNodes[0].Addr = net.ParseIP(m.config.BindAddr)
	localNodes[0].Port = uint16(m.config.BindPort)
	localNodes[0].Incarnation = 1
	localNodes[0].State = int(StateAlive)
	localNodes[1].Name = "Test 1"
	localNodes[1].Addr = net.ParseIP(m.config.BindAddr)
	localNodes[1].Port = uint16(m.config.BindPort)
	localNodes[1].Incarnation = 1
	localNodes[1].State = int(StateAlive)
	localNodes[2].Name = "Test 2"
	localNodes[2].Addr = net.ParseIP(m.config.BindAddr)
	localNodes[2].Port = uint16(m.config.BindPort)
	localNodes[2].Incarnation = 1
	localNodes[2].State = int(StateAlive)

	// Send our node state
	header := pushPullHeader{Nodes: 3}
	out := header.AppendMsgpack([]byte{byte(pushPullMsg)})
	for i := range header.Nodes {
		out = localNodes[i].AppendMsgpack(out)
	}
	if _, err := conn.Write(out); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Read the message type
	br := bufio.NewReader(conn)
	b, err := br.ReadByte()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	msgType := messageType(b)
	dec := msgpack.NewStreamDecoder(br)

	// Check if we have a compressed message
	if msgType == compressMsg {
		var c compress
		if err := c.DecodeMsgpack(dec); err != nil {
			t.Fatalf("unexpected err %s", err)
		}
		decomp, err := decompressBuffer(&c, maxDecompressedBytes)
		if err != nil {
			t.Fatalf("unexpected err %s", err)
		}

		// Reset the message type and decoder
		msgType = messageType(decomp[0])
		dec = msgpack.NewDecoder(decomp[1:])
	}

	// Quit if not push/pull
	if msgType != pushPullMsg {
		t.Fatalf("bad message type")
	}

	if err := header.DecodeMsgpack(dec); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Allocate space for the transfer
	remoteNodes := make([]pushNodeState, header.Nodes)

	// Try to decode all the states
	for i := 0; i < header.Nodes; i++ {
		if err := remoteNodes[i].DecodeMsgpack(dec); err != nil {
			t.Fatalf("unexpected err %s", err)
		}
	}

	if len(remoteNodes) != 1 {
		t.Fatalf("bad response")
	}

	n := &remoteNodes[0]
	if n.Name != "Test 0" {
		t.Fatalf("bad name")
	}
	if !bytes.Equal(n.Addr, net.ParseIP(m.config.BindAddr)) {
		t.Fatal("bad addr")
	}
	if n.Incarnation != 0 {
		t.Fatal("bad incarnation")
	}
	if NodeStateType(n.State) != StateSuspect {
		t.Fatal("bad state")
	}
}

func TestSendMsg_Piggyback(t *testing.T) {
	m := GetMemberlist(t, nil)
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// Add a message to be broadcast
	a := alive{
		Incarnation: 10,
		Node:        "rand",
		Addr:        []byte{127, 0, 0, 255},
		Meta:        nil,
		Vsn: []uint8{
			ProtocolVersionMin, ProtocolVersionMax, ProtocolVersionMin,
			1, 1, 1,
		},
	}
	m.encodeAndBroadcast("rand", aliveMsg, &a)

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	udpAddr := udp.LocalAddr().(*net.UDPAddr)

	// Encode a ping
	ping := ping{
		SeqNo:      42,
		SourceAddr: udpAddr.IP,
		SourcePort: uint16(udpAddr.Port),
		SourceNode: "test",
	}
	buf := encode(pingMsg, ping)

	// Send
	addr := &net.UDPAddr{IP: net.ParseIP(m.config.BindAddr), Port: m.config.BindPort}
	_, err := udp.WriteTo(buf, addr)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	// Wait for response
	doneCh := make(chan struct{}, 1)
	go func() {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			panic("timeout")
		}
	}()

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	msgType := messageType(in[0])
	if msgType != compoundMsg {
		t.Fatalf("bad response %v", in)
	}

	// get the parts
	trunc, parts, err := decodeCompoundMessage(in[1:])
	if trunc != 0 {
		t.Fatalf("unexpected truncation")
	}
	if len(parts) != 2 {
		t.Fatalf("unexpected parts %v", parts)
	}
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	var ack ackResp
	if err := decode(parts[0][1:], &ack); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if ack.SeqNo != 42 {
		t.Fatalf("bad sequence no")
	}

	var aliveout alive
	if err := decode(parts[1][1:], &aliveout); err != nil {
		t.Fatalf("unexpected err %s", err)
	}

	if aliveout.Node != "rand" || aliveout.Incarnation != 10 {
		t.Fatalf("bad mesg")
	}

	doneCh <- struct{}{}
}

func TestEncryptDecryptState(t *testing.T) {
	state := []byte("this is our internal state...")
	config := &Config{
		SecretKey:       []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		ProtocolVersion: ProtocolVersionMax,
		Logger:          testLogger(t, "local"),
	}
	sink := newRecordingSink()
	config.Metrics = sink

	m, err := Create(config)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	crypt, err := m.encryptLocalState(state, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Create reader, seek past the type byte
	buf := bytes.NewReader(crypt)
	if _, err := buf.Seek(1, 0); err != nil {
		t.Fatalf("err: %v", err)
	}

	plain, err := m.decryptRemoteState(buf, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n := sink.sampleCount("memberlist.size.remote"); n != 1 {
		t.Fatalf("memberlist.size.remote recorded %d samples, want 1", n)
	}

	if !reflect.DeepEqual(state, plain) {
		t.Fatalf("Decrypt failed: %v", plain)
	}
}

func TestRawSendUdp_CRC(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	a := Address{
		Addr: udp.LocalAddr().String(),
		Name: "test",
	}

	// Pass a nil node with no nodes registered, should result in no checksum
	payload := []byte{3, 3, 3, 3}
	if err := m.rawSendMsgPacket(a, nil, payload); err != nil {
		t.Fatalf("err: %v", err)
	}

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 4 {
		t.Fatalf("bad: %v", in)
	}

	// Pass a non-nil node with PMax >= 5, should result in a checksum
	if err := m.rawSendMsgPacket(a, &Node{PMax: 5}, payload); err != nil {
		t.Fatalf("err: %v", err)
	}

	in = make([]byte, 1500)
	n, _, err = udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}

	// Register a node with PMax >= 5 to be looked up, should result in a checksum
	m.nodeMap["127.0.0.1"] = &nodeState{
		PMax: 5,
	}
	if err := m.rawSendMsgPacket(a, nil, payload); err != nil {
		t.Fatal(err)
	}

	in = make([]byte, 1500)
	n, _, err = udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}
}

func TestIngestPacket_CRC(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	a := Address{
		Addr: udp.LocalAddr().String(),
		Name: "test",
	}

	// Get a message with a checksum
	payload := []byte{3, 3, 3, 3}
	if err := m.rawSendMsgPacket(a, &Node{PMax: 5}, payload); err != nil {
		t.Fatal(err)
	}

	in := make([]byte, 1500)
	n, _, err := udp.ReadFrom(in)
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
	in = in[0:n]

	if len(in) != 9 {
		t.Fatalf("bad: %v", in)
	}

	// Corrupt the checksum
	in[1] <<= 1

	logger, logs := bufferLogger()
	m.logger = logger
	m.ingestPacket(in, udp.LocalAddr(), time.Now())

	if !strings.Contains(logs.String(), "invalid checksum") {
		t.Fatalf("bad: %s", logs.String())
	}
}

func TestIngestPacket_ExportedFunc_EmptyMessage(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	udp := listenUDP(t)
	defer func() {
		if err := udp.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	emptyConn := &emptyReadNetConn{}

	type ingestionAwareTransport interface {
		IngestPacket(conn net.Conn, addr net.Addr, now time.Time, shouldClose bool) error
	}

	err := m.transport.(ingestionAwareTransport).IngestPacket(emptyConn, udp.LocalAddr(), time.Now(), true)
	isErr(t, err)
	contains(t, err.Error(), "packet too short")
}

type emptyReadNetConn struct {
	net.Conn
}

func (c *emptyReadNetConn) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (c *emptyReadNetConn) Close() error {
	return nil
}

func TestGossip_MismatchedKeys(t *testing.T) {
	// Create two agents with different gossip keys
	c1 := testConfig(t)
	c1.SecretKey = []byte("4W6DGn2VQVqDEceOdmuRTQ==")

	m1, err := Create(c1)
	noErr(t, err)
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	bindPort := m1.config.BindPort

	c2 := testConfig(t)
	c2.BindPort = bindPort
	c2.SecretKey = []byte("XhX/w702/JKKK7/7OtM9Ww==")

	m2, err := Create(c2)
	noErr(t, err)
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// Make sure we get this error on the joining side
	_, err = m2.Join([]string{c1.Name + "/" + c1.BindAddr})
	if err == nil || !strings.Contains(err.Error(), "no installed keys could decrypt the message") {
		t.Fatalf("bad: %s", err)
	}
}

func listenUDP(t *testing.T) *net.UDPConn {
	var udp *net.UDPConn
	for port := 60000; port < 61000; port++ {
		udpAddr := fmt.Sprintf("127.0.0.1:%d", port)
		udpLn, err := net.ListenPacket("udp", udpAddr)
		if err == nil {
			udp = udpLn.(*net.UDPConn)
			break
		}
	}
	if udp == nil {
		t.Fatalf("no udp listener")
	}
	return udp
}

func TestHandleCommand(t *testing.T) {
	logger, buf := bufferLogger()
	m := Memberlist{
		logger: logger,
	}
	m.handleCommand(nil, &net.TCPAddr{Port: 12345}, time.Now())
	contains(t, buf.String(), "missing message type byte")
}

func TestReadRemoteState_Limits(t *testing.T) {

	mockNet := &MockNetwork{}
	tr := mockNet.NewTransport("node")
	logger, logs := bufferLogger()

	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Logger = logger
		c.Transport = tr
		c.BindAddr = "127.0.0.1"
		c.BindPort = 1
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	addr := joinHostPort(m.config.BindAddr, uint16(m.config.BindPort))

	t.Run("nodes", func(t *testing.T) {
		msg := pushPullHeader{Nodes: 10_000_000, UserStateLen: 100, Join: false}
		buf := encode(pushPullMsg, msg)

		conn, err := tr.DialTimeout(addr, time.Millisecond*100)
		noErr(t, err)

		err = m.rawSendMsgStream(conn, buf, "")
		noErr(t, err)

		// conn closed: get nothing back
		var out []byte
		_, err = conn.Read(out)
		isErr(t, err, "EOF")
		contains(t, logs.String(),
			"number of nodes in header (10000000) exceeds limit")
	})

	t.Run("user_state", func(t *testing.T) {
		msg := pushPullHeader{Nodes: 0, UserStateLen: 30_000_000, Join: false}
		buf := encode(pushPullMsg, msg)

		conn, err := tr.DialTimeout(addr, time.Millisecond*100)
		noErr(t, err)

		err = m.rawSendMsgStream(conn, buf, "")
		noErr(t, err)

		// conn closed: get nothing back
		var out []byte
		_, err = conn.Read(out)
		isErr(t, err, "EOF")
		contains(t, logs.String(),
			"user state length (30000000) exceeds limit")
	})
}

func TestHandleConn_NilConnAfterRemoveLabelHeaderFromStream(t *testing.T) {
	mockNet := &MockNetwork{}

	t1 := mockNet.NewTransport("node1")

	m := GetMemberlist(t, func(c *Config) {
		c.Transport = t1
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	errConn := &errorReadNetConn{
		closed: make(chan struct{}),
	}
	if err := t1.IngestStream(errConn); err != nil {
		t.Fatal(err)
	}

	// The connection must be successfully closed
	<-errConn.closed
}

type errorReadNetConn struct {
	net.Conn
	closed chan struct{}
}

func (c *errorReadNetConn) LocalAddr() net.Addr {
	return &MockAddress{"fake:0", "fake"}
}

func (c *errorReadNetConn) RemoteAddr() net.Addr {
	return &MockAddress{"fake:0", "fake"}
}

func (c *errorReadNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *errorReadNetConn) Read(b []byte) (n int, err error) {
	return 0, fmt.Errorf("test read error")
}

func (c *errorReadNetConn) Close() error {
	close(c.closed)
	return nil
}

// allocatedBytes reports the bytes allocated by the whole process while f
// runs. Used to prove that header-declared lengths do not drive allocation.
func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// streamFrom returns the reading end of an in-memory stream that carries
// payload and then EOF.
func streamFrom(t *testing.T, payload []byte) net.Conn {
	t.Helper()
	r, w := net.Pipe()
	go func() {
		_, _ = w.Write(payload)
		_ = w.Close()
	}()
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// noPanic runs f and fails the test if it panics, so a crash on hostile
// input is reported as a test failure with the panic value.
func noPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked: %v", what, r)
		}
	}()
	f()
}

// TestReadUserMsgLengthBounds covers upstream hashicorp/memberlist#361: a
// stream user message whose header declares a huge or negative length must
// be refused before any buffer is sized from the header.
func TestReadUserMsgLengthBounds(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	for _, length := range []int{30_000_000, -1} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			buf := encode(userMsg, &userMsgHeader{UserMsgLen: length})
			var readErr error
			alloc := allocatedBytes(func() {
				conn := streamFrom(t, append(buf, 1, 2, 3))
				msgType, dec, err := m.readStream(conn, "")
				if err != nil {
					t.Fatalf("readStream: %v", err)
				}
				if msgType != userMsg {
					t.Fatalf("message type %d, want %d", msgType, userMsg)
				}
				readErr = m.readUserMsg(dec)
			})
			if readErr == nil || !strings.Contains(readErr.Error(), "exceeds limit") {
				t.Fatalf("readUserMsg error = %v, want a length-limit refusal", readErr)
			}
			if alloc > 4<<20 {
				t.Fatalf("refusal allocated %d bytes; the header length must not size a buffer", alloc)
			}
		})
	}
}

// TestReadRemoteStateNodeCountDoesNotPreallocate: a push/pull header that
// declares the maximum node count but carries no node payload must not make
// the receiver allocate storage for every declared node up front.
func TestReadRemoteStateNodeCountDoesNotPreallocate(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	buf := encode(pushPullMsg, &pushPullHeader{Nodes: maxPushStateNodes})
	var readErr error
	alloc := allocatedBytes(func() {
		conn := streamFrom(t, buf)
		_, dec, err := m.readStream(conn, "")
		if err != nil {
			t.Fatalf("readStream: %v", err)
		}
		_, _, _, readErr = m.readRemoteState(dec)
	})
	if readErr == nil {
		t.Fatal("truncated push/pull state was accepted")
	}
	if alloc > 8<<20 {
		t.Fatalf("truncated state allocated %d bytes for %d declared nodes", alloc, maxPushStateNodes)
	}
}

// TestReadStreamEmptyInnerPayload covers upstream hashicorp/memberlist#369
// and the equivalent decryption path: a compressed or encrypted stream whose
// inner payload is empty must be an error, never an index panic.
func TestReadStreamEmptyInnerPayload(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		m := GetMemberlist(t, func(c *Config) {
			c.Transport = (&MockNetwork{}).NewTransport("node")
		})
		t.Cleanup(func() { _ = m.Shutdown() })

		buf, err := compressPayload(nil)
		if err != nil {
			t.Fatal(err)
		}
		var readErr error
		noPanic(t, "readStream", func() {
			_, _, readErr = m.readStream(streamFrom(t, buf), "")
		})
		if readErr == nil {
			t.Fatal("empty decompressed stream payload was accepted")
		}
	})

	t.Run("encrypted", func(t *testing.T) {
		key := []byte("0123456789abcdef")
		m := GetMemberlist(t, func(c *Config) {
			c.Transport = (&MockNetwork{}).NewTransport("node")
			c.SecretKey = key
		})
		t.Cleanup(func() { _ = m.Shutdown() })

		stream, err := m.encryptLocalState(nil, "")
		if err != nil {
			t.Fatal(err)
		}
		var readErr error
		noPanic(t, "readStream", func() {
			_, _, readErr = m.readStream(streamFrom(t, stream), "")
		})
		if readErr == nil {
			t.Fatal("empty decrypted stream payload was accepted")
		}
	})
}

// TestHandleCommandRejectsNestedEnvelopes: senders never nest a compressed
// message in a compressed message, or a compound message in a compound
// message. Accepting either lets one packet multiply decompression work and
// recursion depth, so both are refused and nothing inside is queued.
func TestHandleCommandRejectsNestedEnvelopes(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })
	// Stop the packet handler from draining the queues under test.
	_ = m.Shutdown()

	user := []byte{byte(userMsg), 'h', 'i'}
	compress := func(b []byte) []byte {
		out, err := compressPayload(b)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	compound := func(b []byte) []byte { return makeCompoundMessage([][]byte{b}) }

	cases := map[string][]byte{
		"compress-in-compress": compress(compress(user)),
		"compound-in-compound": compound(compound(user)),
	}
	for name, packet := range cases {
		t.Run(name, func(t *testing.T) {
			m.msgQueueLock.Lock()
			m.lowPriorityMsgQueue.Init()
			m.msgQueueLock.Unlock()

			m.handleCommand(packet, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, time.Now())

			m.msgQueueLock.Lock()
			queued := m.lowPriorityMsgQueue.Len()
			m.msgQueueLock.Unlock()
			if queued != 0 {
				t.Fatalf("nested envelope delivered %d message(s); want refusal", queued)
			}
		})
	}

	t.Run("single-envelopes-still-accepted", func(t *testing.T) {
		for _, packet := range [][]byte{compress(user), compound(user), compress(compound(user))} {
			m.msgQueueLock.Lock()
			m.lowPriorityMsgQueue.Init()
			m.msgQueueLock.Unlock()

			m.handleCommand(packet, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, time.Now())

			m.msgQueueLock.Lock()
			queued := m.lowPriorityMsgQueue.Len()
			m.msgQueueLock.Unlock()
			if queued != 1 {
				t.Fatalf("well-formed envelope queued %d messages, want 1", queued)
			}
		}
	})
}

// TestReadStreamBoundsPlainMessage: an unencrypted, uncompressed stream has
// no length prefix, so variable-length fields inside node states could make
// one push/pull arbitrarily large. The plaintext read is capped at
// maxStreamMessageBytes like the compressed form.
func TestReadStreamBoundsPlainMessage(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	// Every field is within wire.MaxFieldBytes; only the total exceeds
	// the stream cap.
	const nodes = maxStreamMessageBytes/(1<<20) + 1
	stream := pushPullHeader{Nodes: nodes}.AppendMsgpack([]byte{byte(pushPullMsg)})
	for i := range nodes {
		n := pushNodeState{Name: fmt.Sprint("peer", i), Addr: []byte{127, 0, 0, 2}, Port: 7946, Meta: make([]byte, 1<<20-64), Vsn: []uint8{1, 5, 2, 0, 0, 0}}
		stream = n.AppendMsgpack(stream)
	}

	_, dec, err := m.readStream(streamFrom(t, stream), "")
	if err != nil {
		t.Fatalf("readStream: %v", err)
	}
	if _, nodes, _, err := m.readRemoteState(dec); err == nil {
		t.Fatalf("stream message above maxStreamMessageBytes was accepted (%d nodes)", len(nodes))
	}
}

// TestSizeLocalGaugeIsMessageSize covers finding F4: memberlist.size.local
// must report the bytes of the local push/pull state sent. It used to read
// bytes 1-4 of the MessagePack encoding as a big-endian length.
func TestSizeLocalGaugeIsMessageSize(t *testing.T) {
	sink := newRecordingSink()
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("local")
		c.Metrics = sink
	})
	t.Cleanup(func() { _ = m.Shutdown() })
	if err := m.setAlive(nil); err != nil {
		t.Fatal(err)
	}

	r, w := net.Pipe()
	received := make(chan int)
	go func() {
		b, _ := io.ReadAll(r)
		received <- len(b)
	}()
	if err := m.sendLocalState(w, false, ""); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	n := <-received
	if got, _ := sink.gauge("memberlist.size.local"); int(got) != n {
		t.Fatalf("memberlist.size.local = %v, want the %d bytes sent", got, n)
	}
}

// TestReadRemoteStateRefusesNamelessStates: a node state without a name is
// meaningless, and accepting it let each 1-byte nil (0xc0) in a push/pull
// stream decode to a full node state (over 100 bytes of memory per input
// byte). The first nameless state fails the exchange.
func TestReadRemoteStateRefusesNamelessStates(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	const n = 1 << 16
	stream := pushPullHeader{Nodes: n}.AppendMsgpack([]byte{byte(pushPullMsg)})
	stream = append(stream, bytes.Repeat([]byte{0xc0}, n)...)
	var readErr error
	alloc := allocatedBytes(func() {
		_, dec, err := m.readStream(streamFrom(t, stream), "")
		noErr(t, err)
		_, _, _, readErr = m.readRemoteState(dec)
	})
	isErr(t, readErr, "nameless node states accepted")
	lessOrEqual(t, alloc, uint64(1<<20), "allocation for %d input bytes", len(stream))
}

// TestSendReliableRefusesOversizedMessages: receivers refuse stream user
// messages above maxUserMsgBytes, so the sender must report the failure
// instead of returning nil for a message that is never delivered.
func TestSendReliableRefusesOversizedMessages(t *testing.T) {
	n := &MockNetwork{}
	m1 := GetMemberlist(t, func(c *Config) { c.Transport = n.NewTransport("node1") })
	t.Cleanup(func() { _ = m1.Shutdown() })
	m2 := GetMemberlist(t, func(c *Config) { c.Transport = n.NewTransport("node2") })
	t.Cleanup(func() { _ = m2.Shutdown() })
	to := &Node{Name: "node2", Addr: net.ParseIP("127.0.0.2"), Port: 1}
	err := m1.SendReliable(to, make([]byte, maxUserMsgBytes+1))
	isErr(t, err, "an undeliverable stream user message was reported as sent")
}

// TestReadStreamAcceptsIncompressibleCompressedState: senders compress
// stream messages whether or not that shrinks them, and LZW expands
// incompressible data (about 1.37x, at most 1.5x). A push/pull whose
// decompressed form is within maxDecompressedBytes must be accepted even
// when its compressed form is larger than that.
func TestReadStreamAcceptsIncompressibleCompressedState(t *testing.T) {
	if raceDetector {
		t.Skip("single-goroutine size check; ~33 MB of input costs 16 s under the race detector")
	}
	m := GetMemberlist(t, func(c *Config) { c.Transport = (&MockNetwork{}).NewTransport("node") })
	t.Cleanup(func() { _ = m.Shutdown() })

	// A large cluster: 20,000 nodes with 512 bytes of random meta and a
	// full 20 MiB of random user state — about 32.7 MB decompressed.
	r := rand.New(rand.NewPCG(1, 2))
	random := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}
	const nodes = 20_000
	user := random(maxPushStateBytes)
	state := pushPullHeader{Nodes: nodes, UserStateLen: len(user)}.AppendMsgpack([]byte{byte(pushPullMsg)})
	for i := range nodes {
		n := pushNodeState{Name: fmt.Sprint("node-", i), Addr: []byte{10, 1, byte(i >> 8), byte(i)}, Port: 7946, Meta: random(512), Incarnation: 1, Vsn: []uint8{1, 5, 2, 0, 0, 0}}
		state = n.AppendMsgpack(state)
	}
	state = append(state, user...)
	lessOrEqual(t, len(state), maxDecompressedBytes, "decompressed size")
	compressed, err := compressPayload(state)
	noErr(t, err)
	if len(compressed) <= maxStreamMessageBytes {
		t.Fatalf("test input compressed to %d bytes; it must exceed the %d-byte plaintext cap", len(compressed), maxStreamMessageBytes)
	}

	_, dec, err := m.readStream(streamFrom(t, compressed), "")
	noErr(t, err, "compressed stream of %d bytes (%d decompressed)", len(compressed), len(state))
	_, got, gotUser, err := m.readRemoteState(dec)
	noErr(t, err)
	equal(t, nodes, len(got))
	isTrue(t, bytes.Equal(user, gotUser), "user state corrupted")
}

// TestReadStreamOversizedDeclaredFields covers issue #21: a stream whose
// compressed buffer or node field declares a large length — within the
// field limits, so only allocation that grows with the bytes received can
// save memory — but carries 1 KiB is refused without allocating the
// declared size.
func TestReadStreamOversizedDeclaredFields(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) { c.Transport = (&MockNetwork{}).NewTransport("node") })
	t.Cleanup(func() { _ = m.Shutdown() })

	str32 := func(b []byte, n uint32) []byte {
		return binary.BigEndian.AppendUint32(append(b, 0xdb), n)
	}
	compressedBuf := []byte{byte(compressMsg), 0x82, 0xa4, 'A', 'l', 'g', 'o', 0x00, 0xa3, 'B', 'u', 'f'}
	compressedBuf = append(str32(compressedBuf, 60<<20), make([]byte, 1024)...)

	nodeMeta := pushPullHeader{Nodes: 1}.AppendMsgpack([]byte{byte(pushPullMsg)})
	nodeMeta = append(nodeMeta, 0x82, 0xa4, 'N', 'a', 'm', 'e', 0xa1, 'p', 0xa4, 'M', 'e', 't', 'a')
	nodeMeta = append(str32(nodeMeta, 1<<20), make([]byte, 1024)...)

	for _, tc := range []struct {
		name   string
		stream []byte
		budget uint64 // well below the declared length
	}{
		{"compress.Buf", compressedBuf, 4 << 20},
		{"pushNodeState.Meta", nodeMeta, 512 << 10},
	} {
		stream := tc.stream
		t.Run(tc.name, func(t *testing.T) {
			var readErr error
			alloc := allocatedBytes(func() {
				msgType, dec, err := m.readStream(streamFrom(t, stream), "")
				if err != nil {
					readErr = err
					return
				}
				if msgType == pushPullMsg {
					_, _, _, readErr = m.readRemoteState(dec)
				}
			})
			isErr(t, readErr)
			lessOrEqual(t, alloc, tc.budget, "allocation for a %d-byte stream", len(stream))
		})
	}
}

// TestEncryptedStreamSenderLimit: receivers refuse encrypted stream
// messages whose ciphertext exceeds maxPushStateBytes, so senders must fail
// with ErrMessageTooLarge rather than send something that is dropped. The
// boundary is exact: the largest user message whose stream (type byte,
// header, payload) encrypts to maxPushStateBytes is delivered, one byte
// more is refused.
func TestEncryptedStreamSenderLimit(t *testing.T) {
	n := &MockNetwork{}
	key := []byte("0123456789abcdef")
	m1 := GetMemberlist(t, func(c *Config) {
		c.Transport = n.NewTransport("node1")
		c.SecretKey = key
		c.EnableCompression = false
	})
	t.Cleanup(func() { _ = m1.Shutdown() })
	d2 := &MockDelegate{}
	m2 := GetMemberlist(t, func(c *Config) {
		c.Transport = n.NewTransport("node2")
		c.SecretKey = key
		c.Delegate = d2
	})
	t.Cleanup(func() { _ = m2.Shutdown() })

	// The stream around a user message of length L is 1 type byte plus the
	// encoded header; its ciphertext is encryptedLength of that total.
	streamLen := func(l int) int { return len(userMsgHeader{UserMsgLen: l}.AppendMsgpack([]byte{byte(userMsg)})) + l }
	largest := maxPushStateBytes - encryptedLength(m1.encryptionVersion(), streamLen(0))
	for encryptedLength(m1.encryptionVersion(), streamLen(largest)) > maxPushStateBytes {
		largest--
	}
	equal(t, maxPushStateBytes, encryptedLength(m1.encryptionVersion(), streamLen(largest)), "boundary derivation")

	to := &Node{Name: "node2", Addr: net.ParseIP("127.0.0.2"), Port: 1}
	errIs(t, m1.SendReliable(to, make([]byte, largest+1)), ErrMessageTooLarge)
	noErr(t, m1.SendReliable(to, make([]byte, largest)), "the largest message within the encrypted limit")
	iretry.Run(t, func(r *iretry.R) {
		msgs := d2.getMessages()
		if len(msgs) != 1 || len(msgs[0]) != largest {
			r.Fatalf("receiver got %d messages, want one of %d bytes", len(msgs), largest)
		}
	})
}

// TestReadStreamPlaintextCapEdge: a plaintext stream message of exactly
// maxStreamMessageBytes (type byte included) is accepted; one byte more is
// refused.
func TestReadStreamPlaintextCapEdge(t *testing.T) {
	if raceDetector {
		t.Skip("single-goroutine size check; two 40 MiB streams are slow under the race detector")
	}
	m := GetMemberlist(t, func(c *Config) {
		c.EnableCompression = false
		c.Transport = (&MockNetwork{}).NewTransport("node")
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	// A push/pull with a full user state and one node whose meta pads the
	// message to the target size (meta stays within wire.MaxFieldBytes by
	// spreading it over several nodes).
	build := func(total int) []byte {
		const nodes = 24
		fixed := func(meta int) int {
			b := pushPullHeader{Nodes: nodes, UserStateLen: maxPushStateBytes}.AppendMsgpack([]byte{byte(pushPullMsg)})
			for i := range nodes {
				b = pushNodeState{Name: fmt.Sprintf("n%02d", i), Addr: []byte{10, 0, 0, 1}, Port: 7946, Meta: make([]byte, meta), Incarnation: 1, Vsn: []uint8{1, 5, 2, 0, 0, 0}}.AppendMsgpack(b)
			}
			return len(b) + maxPushStateBytes
		}
		meta := (total - fixed(0)) / nodes
		for fixed(meta) > total {
			meta--
		}
		b := pushPullHeader{Nodes: nodes, UserStateLen: maxPushStateBytes}.AppendMsgpack([]byte{byte(pushPullMsg)})
		extra := total - fixed(meta) // the remainder goes to the last node
		for i := range nodes {
			l := meta
			if i == nodes-1 {
				l += extra
			}
			b = pushNodeState{Name: fmt.Sprintf("n%02d", i), Addr: []byte{10, 0, 0, 1}, Port: 7946, Meta: make([]byte, l), Incarnation: 1, Vsn: []uint8{1, 5, 2, 0, 0, 0}}.AppendMsgpack(b)
		}
		return append(b, make([]byte, maxPushStateBytes)...)
	}
	for _, tc := range []struct {
		total int
		ok    bool
	}{{maxStreamMessageBytes, true}, {maxStreamMessageBytes + 1, false}} {
		stream := build(tc.total)
		equal(t, tc.total, len(stream), "test stream size")
		_, dec, err := m.readStream(streamFrom(t, stream), "")
		noErr(t, err)
		_, _, _, err = m.readRemoteState(dec)
		if (err == nil) != tc.ok {
			t.Fatalf("%d-byte plaintext message: error %v, want accepted=%v", tc.total, err, tc.ok)
		}
	}
}

// TestStreamCompressionOnlyWhenSmaller: like packets, stream messages go
// out compressed only when that makes them smaller.
func TestStreamCompressionOnlyWhenSmaller(t *testing.T) {
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("node")
		c.EnableCompression = true
	})
	t.Cleanup(func() { _ = m.Shutdown() })

	r := rand.New(rand.NewPCG(5, 6))
	random := make([]byte, 4096)
	for i := range random {
		random[i] = byte(r.Uint32())
	}
	for _, tc := range []struct {
		name    string
		payload []byte
		want    messageType
	}{
		{"incompressible", random, userMsg},
		{"compressible", make([]byte, 4096), compressMsg},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := append(userMsgHeader{UserMsgLen: len(tc.payload)}.AppendMsgpack([]byte{byte(userMsg)}), tc.payload...)
			local, remote := net.Pipe()
			got := make(chan []byte, 1)
			go func() {
				b, _ := io.ReadAll(remote)
				got <- b
			}()
			noErr(t, m.rawSendMsgStream(local, msg, ""))
			_ = local.Close()
			sent := <-got
			equal(t, tc.want, messageType(sent[0]))
			lessOrEqual(t, len(sent), len(msg), "sent size")
		})
	}
}
