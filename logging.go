// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"log/slog"
	"net"
)

// addrAttr is the log attribute for a peer address.
func addrAttr(addr net.Addr) slog.Attr {
	if addr == nil {
		return slog.String("from", "<unknown address>")
	}
	return slog.String("from", addr.String())
}

// connAttr is the log attribute for a stream's remote address.
func connAttr(conn net.Conn) slog.Attr {
	if conn == nil {
		return addrAttr(nil)
	}
	return addrAttr(conn.RemoteAddr())
}
