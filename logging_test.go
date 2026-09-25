// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// testLogWriter forwards to the test's output until the test ends, then
// discards: background goroutines of a leaked instance must not write to a
// finished test.
type testLogWriter struct {
	mu   sync.Mutex
	w    io.Writer
	done bool
}

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return len(p), nil
	}
	return w.w.Write(p)
}

// testLogger returns a debug-level logger attributed to tb, tagged with
// the node name.
func testLogger(tb testing.TB, node string) *slog.Logger {
	w := &testLogWriter{w: tb.Output()}
	tb.Cleanup(func() {
		w.mu.Lock()
		w.done = true
		w.mu.Unlock()
	})
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", node)
}

// syncBuffer is a bytes.Buffer safe for concurrent writers and readers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// bufferLogger returns a debug-level logger writing text records to a
// concurrency-safe buffer, for tests that assert on log output.
func bufferLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func TestLogging_AddressAttributes(t *testing.T) {
	if got := addrAttr(nil).String(); got != "from=<unknown address>" {
		t.Fatalf("addrAttr(nil) = %s", got)
	}
	addr, err := net.ResolveIPAddr("ip4", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if got := addrAttr(addr).String(); got != "from=127.0.0.1" {
		t.Fatalf("addrAttr = %s", got)
	}
	if got := connAttr(nil).String(); got != "from=<unknown address>" {
		t.Fatalf("connAttr(nil) = %s", got)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if got := connAttr(conn).String(); got != "from="+conn.RemoteAddr().String() {
		t.Fatalf("connAttr = %s", got)
	}
}

// TestLoggerDefaultsToSlogDefault: a nil Config.Logger logs through
// slog.Default().
func TestLoggerDefaultsToSlogDefault(t *testing.T) {
	logger, buf := bufferLogger()
	prev := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(prev) })

	m := GetMemberlist(t, func(c *Config) {
		c.Logger = nil
		c.Transport = (&MockNetwork{}).NewTransport("local")
	})
	t.Cleanup(func() { _ = m.Shutdown() })
	m.handleCommand(nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, time.Now())
	if !strings.Contains(buf.String(), "missing message type byte") {
		t.Fatalf("default logger did not receive the record: %q", buf.String())
	}
}
