// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// TestNetTransportPacketsDoNotAlias: the UDP listener reuses one receive
// buffer, so every delivered packet must own a copy of exactly its bytes.
func TestNetTransportPacketsDoNotAlias(t *testing.T) {
	tr, err := NewNetTransport(&NetTransportConfig{BindAddrs: []string{"127.0.0.1"}, Logger: testLogger(t, "transport")})
	noErr(t, err)
	t.Cleanup(func() { _ = tr.Shutdown() })

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: tr.GetAutoBindPort()})
	noErr(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	first := bytes.Repeat([]byte{'a'}, 1000)
	second := []byte("bb")
	var got [][]byte
	for _, payload := range [][]byte{first, second} {
		_, err := conn.Write(payload)
		noErr(t, err)
		select {
		case p := <-tr.PacketCh():
			got = append(got, p.Buf)
		case <-time.After(5 * time.Second):
			t.Fatal("packet not received")
		}
	}
	equal(t, first, got[0], "first packet changed after the second was read")
	equal(t, second, got[1])
	less(t, cap(got[1]), 64, "a packet must not retain the 64 KiB receive buffer")
}
