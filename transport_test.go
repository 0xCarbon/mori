// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestTransport_Join(t *testing.T) {
	net := &MockNetwork{}

	t1 := net.NewTransport("node1")

	c1 := DefaultLANConfig()
	c1.Name = "node1"
	c1.Transport = t1
	m1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m1.setAlive(nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	m1.schedule()
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	c2 := DefaultLANConfig()
	c2.Name = "node2"
	c2.Transport = net.NewTransport("node2")
	m2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.setAlive(nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	m2.schedule()
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	num, err := m2.Join([]string{c1.Name + "/" + t1.addr.String()})
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(m2.Members()) != 2 {
		t.Fatalf("bad: %v", m2.Members())
	}
	if m2.estNumNodes() != 2 {
		t.Fatalf("bad: %v", m2.Members())
	}

}

func TestTransport_Send(t *testing.T) {
	net := &MockNetwork{}

	t1 := net.NewTransport("node1")
	d1 := &MockDelegate{}

	c1 := DefaultLANConfig()
	c1.Name = "node1"
	c1.Transport = t1
	c1.Delegate = d1
	m1, err := Create(c1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m1.setAlive(nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	m1.schedule()
	defer func() {
		if err := m1.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	c2 := DefaultLANConfig()
	c2.Name = "node2"
	c2.Transport = net.NewTransport("node2")
	m2, err := Create(c2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.setAlive(nil); err != nil {
		t.Fatalf("err: %v", err)
	}
	m2.schedule()
	defer func() {
		if err := m2.Shutdown(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	num, err := m2.Join([]string{c1.Name + "/" + t1.addr.String()})
	if num != 1 {
		t.Fatalf("bad: %d", num)
	}
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := m2.SendTo(t1.addr, []byte("SendTo")); err != nil {
		t.Fatalf("err: %v", err)
	}

	var n1 *Node
	for _, n := range m2.Members() {
		if n.Name == c1.Name {
			n1 = n
			break
		}
	}
	if n1 == nil {
		t.Fatalf("bad")
	}

	if err := m2.SendToUDP(n1, []byte("SendToUDP")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendToTCP(n1, []byte("SendToTCP")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendBestEffort(n1, []byte("SendBestEffort")); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := m2.SendReliable(n1, []byte("SendReliable")); err != nil {
		t.Fatalf("err: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	expected := []string{"SendTo", "SendToUDP", "SendToTCP", "SendBestEffort", "SendReliable"}

	msgs1 := d1.getMessages()

	received := make([]string, len(msgs1))
	for i, bs := range msgs1 {
		received[i] = string(bs)
	}
	// Some of these are UDP so often get re-ordered making the test flaky if we
	// assert send ordering. Sort both slices to be tolerant of re-ordering.
	elementsMatch(t, expected, received)
}

type testCountingWriter struct {
	t        *testing.T
	numCalls *int32
}

func (tw testCountingWriter) Write(p []byte) (n int, err error) {
	atomic.AddInt32(tw.numCalls, 1)
	if !strings.Contains(string(p), "error accepting TCP connection") {
		tw.t.Error("did not receive expected log message")
	}
	tw.t.Log("countingWriter:", string(p))
	return len(p), nil
}

// TestTransport_TcpListenBackoff tests that AcceptTCP() errors in
// NetTransport#tcpListen() do not result in a tight loop and spam the log.
// It runs in a synctest bubble, so the count is exact: with delays doubling
// from 5ms to a 1s cap, errors are logged at 0, 5, 15, 35, 75, 155, 315,
// 635, 1275, 2275 and 3275ms — eleven within 4s — and the loop exits on
// its next wake-up (4275ms) after shutdown is flagged.
func TestTransport_TcpListenBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var numCalls int32
		countingWriter := testCountingWriter{t, &numCalls}
		countingLogger := slog.New(slog.NewTextHandler(countingWriter, nil))
		transport := NetTransport{
			streamCh: make(chan net.Conn),
			logger:   countingLogger,
		}
		transport.wg.Add(1)

		// create a listener that will cause AcceptTCP calls to fail
		listener, err := net.ListenTCP("tcp", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("not able to close the listener: %v", err)
		}
		go transport.tcpListen(listener)

		time.Sleep(4 * time.Second)
		transport.shutdown.Store(1)

		start := time.Now()
		transport.wg.Wait()
		if waited := time.Since(start); waited != 275*time.Millisecond {
			t.Errorf("accept loop exited %v after shutdown, want 275ms (its next wake-up)", waited)
		}
		if n := atomic.LoadInt32(&numCalls); n != 11 {
			t.Errorf("logged %d accept errors in 4s, want 11", n)
		}

		// no connections should have been accepted and sent to the channel
		equal(t, 0, len(transport.streamCh))
	})
}
