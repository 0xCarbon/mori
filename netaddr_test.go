// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"net"
	"net/netip"
	"testing"
)

// TestSpecialPurposeBlocks checks addresses at the edges of the RFC 6890
// blocks against values derived by hand from the RFC tables.
func TestSpecialPurposeBlocks(t *testing.T) {
	special := []string{
		"0.0.0.1", "10.0.0.0", "10.255.255.255", "100.64.0.1", "100.127.255.255",
		"127.0.0.1", "169.254.1.1", "172.16.0.1", "172.31.255.255", "192.0.0.8",
		"192.0.2.1", "192.88.99.1", "192.168.1.1", "198.18.0.1", "198.19.255.255",
		"198.51.100.7", "203.0.113.9", "240.0.0.1", "255.255.255.255",
		"::1", "::", "64:ff9b::1.2.3.4", "::ffff:10.0.0.1", "100::1", "2001::1",
		"2001:1ff:ffff::1", "2001:db8::1", "2002::1", "fc00::1", "fdff::1", "fe80::1",
	}
	public := []string{
		"1.1.1.1", "8.8.8.8", "11.0.0.1", "100.63.255.255", "100.128.0.0", "172.15.255.255",
		"172.32.0.0", "192.0.1.1", "192.169.0.0", "198.17.255.255", "198.20.0.0",
		"223.255.255.255", "::ffff:8.8.8.8", "2001:200::1", "2001:4860:4860::8888",
		"2600::1", "fe00::1",
	}
	for _, s := range special {
		if !isSpecialPurpose(netip.MustParseAddr(s)) {
			t.Errorf("%s: want special-purpose", s)
		}
		if isPublicAddress(net.ParseIP(s)) {
			t.Errorf("%s: want not public", s)
		}
	}
	for _, s := range public {
		if isSpecialPurpose(netip.MustParseAddr(s)) {
			t.Errorf("%s: want public", s)
		}
		if !isPublicAddress(net.ParseIP(s)) {
			t.Errorf("%s: isPublicAddress = false", s)
		}
	}
	if isPublicAddress(nil) {
		t.Error("a nil IP is not a public address")
	}
}

func TestPickAdvertiseAddr(t *testing.T) {
	c := func(addr string, bits int, def bool) advertiseCandidate {
		return advertiseCandidate{addr: netip.MustParseAddr(addr), bits: bits, defaultRoute: def}
	}
	for _, tc := range []struct {
		name  string
		cands []advertiseCandidate
		want  string
	}{
		{"none", nil, ""},
		{"only-loopback-and-link-local", []advertiseCandidate{c("127.0.0.1", 8, false), c("169.254.3.4", 16, false), c("fe80::1", 64, false)}, ""},
		{"only-public", []advertiseCandidate{c("8.8.8.8", 24, true), c("2600::1", 64, true)}, ""},
		{"default-route-first", []advertiseCandidate{c("10.0.0.5", 24, false), c("192.168.1.9", 24, true)}, "192.168.1.9"},
		{"ipv4-before-ipv6", []advertiseCandidate{c("fd00::5", 64, false), c("10.1.2.3", 8, false)}, "10.1.2.3"},
		{"smaller-network-first", []advertiseCandidate{c("10.1.2.3", 8, false), c("172.16.4.5", 28, false)}, "172.16.4.5"},
		{"mapped-ipv4", []advertiseCandidate{c("::ffff:10.9.8.7", 104, false)}, "10.9.8.7"},
		{"cgnat", []advertiseCandidate{c("100.64.1.2", 10, false)}, "100.64.1.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pickAdvertiseAddr(tc.cands)
			if tc.want == "" {
				if ok {
					t.Fatalf("picked %s, want none", got)
				}
				return
			}
			if !ok || got != netip.MustParseAddr(tc.want) {
				t.Fatalf("picked %v (%v), want %s", got, ok, tc.want)
			}
		})
	}
}

// TestPrivateAdvertiseAddrOnHost exercises interface discovery on the test
// host: any address it returns must be a forwardable private address.
func TestPrivateAdvertiseAddrOnHost(t *testing.T) {
	a, err := privateAdvertiseAddr()
	if err != nil {
		t.Skipf("host has no private address: %v", err)
	}
	if !isSpecialPurpose(a) || inBlocks(nonForwardableBlocks, a) {
		t.Fatalf("advertise address %s is not a forwardable private address", a)
	}
}
