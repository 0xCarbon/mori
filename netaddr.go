// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"cmp"
	"errors"
	"net"
	"net/netip"
	"slices"
)

// specialPurposeBlocks are the IPv4 and IPv6 special-purpose address blocks
// of RFC 6890 and the registries it established. An address outside every
// block is publicly routable. (go-sockaddr, used before, listed 2001::/16,
// which also covers global unicast such as 2001:4860::/32; this table uses
// the RFC's 2001::/23 and 2001:db8::/32.)
var specialPurposeBlocks = mustPrefixes(
	"0.0.0.0/8",          // "This host on this network" (RFC 1122)
	"10.0.0.0/8",         // Private-Use (RFC 1918)
	"100.64.0.0/10",      // Shared Address Space (RFC 6598)
	"127.0.0.0/8",        // Loopback (RFC 1122)
	"169.254.0.0/16",     // Link Local (RFC 3927)
	"172.16.0.0/12",      // Private-Use (RFC 1918)
	"192.0.0.0/24",       // IETF Protocol Assignments (RFC 6890)
	"192.0.2.0/24",       // Documentation TEST-NET-1 (RFC 5737)
	"192.88.99.0/24",     // 6to4 Relay Anycast (RFC 3068)
	"192.168.0.0/16",     // Private-Use (RFC 1918)
	"198.18.0.0/15",      // Benchmarking (RFC 2544)
	"198.51.100.0/24",    // Documentation TEST-NET-2 (RFC 5737)
	"203.0.113.0/24",     // Documentation TEST-NET-3 (RFC 5737)
	"240.0.0.0/4",        // Reserved (RFC 1112)
	"255.255.255.255/32", // Limited Broadcast (RFC 919)
	"::1/128",            // Loopback (RFC 4291)
	"::/128",             // Unspecified (RFC 4291)
	"64:ff9b::/96",       // IPv4-IPv6 Translation (RFC 6052)
	"::ffff:0:0/96",      // IPv4-mapped (RFC 4291)
	"100::/64",           // Discard-Only (RFC 6666)
	"2001::/23",          // IETF Protocol Assignments (RFC 2928): TEREDO, benchmarking, ORCHID
	"2001:db8::/32",      // Documentation (RFC 3849)
	"2002::/16",          // 6to4 (RFC 3056)
	"fc00::/7",           // Unique-Local (RFC 4193)
	"fe80::/10",          // Linked-Scoped Unicast (RFC 4291)
)

// nonForwardableBlocks are the special-purpose blocks whose addresses a
// router must not forward between external interfaces (RFC 6890
// "Forwardable: False"): they cannot identify this host to its peers.
var nonForwardableBlocks = mustPrefixes(
	"0.0.0.0/8",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"240.0.0.0/4",
	"255.255.255.255/32",
	"::1/128",
	"::/128",
	"::ffff:0:0/96",
	"2001:db8::/32",
	"2001:10::/28",
	"fe80::/10",
)

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}

func inBlocks(blocks []netip.Prefix, a netip.Addr) bool {
	for _, p := range blocks {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// isSpecialPurpose reports whether a lies in an RFC 6890 special-purpose
// block. IPv4-mapped IPv6 addresses are judged as IPv4.
func isSpecialPurpose(a netip.Addr) bool {
	return inBlocks(specialPurposeBlocks, a.Unmap())
}

// isPublicAddress reports whether ip is a valid address outside every
// special-purpose block, i.e. reachable from the public internet.
func isPublicAddress(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	return ok && !isSpecialPurpose(a)
}

// advertiseCandidate is a local interface address considered for
// advertisement when bound to every interface.
type advertiseCandidate struct {
	addr         netip.Addr
	bits         int  // prefix length of the interface network
	defaultRoute bool // the interface carries the default route
}

// errNoPrivateAddress is returned when no interface has a private address.
var errNoPrivateAddress = errors.New("no private IP address found, and explicit IP not provided")

// pickAdvertiseAddr selects the address to advertise from interface
// addresses: forwardable special-purpose (private) addresses only, preferring
// the default-route interface, then IPv4 over IPv6, then the smallest network.
// This is the order of go-sockaddr's GetPrivateIP.
func pickAdvertiseAddr(cands []advertiseCandidate) (netip.Addr, bool) {
	cands = slices.DeleteFunc(slices.Clone(cands), func(c advertiseCandidate) bool {
		a := c.addr.Unmap()
		return !isSpecialPurpose(a) || inBlocks(nonForwardableBlocks, a)
	})
	if len(cands) == 0 {
		return netip.Addr{}, false
	}
	slices.SortStableFunc(cands, func(x, y advertiseCandidate) int {
		if x.defaultRoute != y.defaultRoute {
			if x.defaultRoute {
				return -1
			}
			return 1
		}
		if x4, y4 := x.addr.Unmap().Is4(), y.addr.Unmap().Is4(); x4 != y4 {
			if x4 {
				return -1
			}
			return 1
		}
		return cmp.Compare(y.bits, x.bits) // more prefix bits = smaller network
	})
	return cands[0].addr.Unmap(), true
}

// privateAdvertiseAddr returns the private address of this host to
// advertise when bound to 0.0.0.0. The default-route interface is found by
// asking the kernel which source address it would use to reach a
// documentation address (a UDP "connect" sends no packet).
func privateAdvertiseAddr() (netip.Addr, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return netip.Addr{}, err
	}
	defaultSrc := map[netip.Addr]bool{}
	for _, probe := range []string{"192.0.2.1:9", "[2001:db8::1]:9"} {
		if conn, err := net.Dial("udp", probe); err == nil {
			if ua, ok := conn.LocalAddr().(*net.UDPAddr); ok {
				if a, ok := netip.AddrFromSlice(ua.IP); ok {
					defaultSrc[a.Unmap()] = true
				}
			}
			_ = conn.Close()
		}
	}

	var cands []advertiseCandidate
	var defaultIfaces []int
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		first := len(cands)
		isDefault := false
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			a, ok := netip.AddrFromSlice(ipNet.IP)
			if !ok {
				continue
			}
			bits, _ := ipNet.Mask.Size()
			cands = append(cands, advertiseCandidate{addr: a.Unmap(), bits: bits})
			isDefault = isDefault || defaultSrc[a.Unmap()]
		}
		if isDefault {
			defaultIfaces = append(defaultIfaces, first, len(cands))
		}
	}
	for i := 0; i < len(defaultIfaces); i += 2 {
		for j := defaultIfaces[i]; j < defaultIfaces[i+1]; j++ {
			cands[j].defaultRoute = true
		}
	}
	a, ok := pickAdvertiseAddr(cands)
	if !ok {
		return netip.Addr{}, errNoPrivateAddress
	}
	return a, nil
}
