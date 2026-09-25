// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"net"
	"strings"
	"testing"
)

func Test_IsValidAddressDefaults(t *testing.T) {
	tests := []string{
		"127.0.0.1",
		"127.0.0.5",
		"10.0.0.9",
		"172.16.0.7",
		"192.168.2.1",
		"fe80::aede:48ff:fe00:1122",
		"::1",
	}
	config := DefaultLANConfig()
	for _, ip := range tests {
		localV4 := net.ParseIP(ip)
		if err := config.IPAllowed(localV4); err != nil {
			t.Fatalf("IP %s Localhost Should be accepted for LAN", ip)
		}
	}
	config = DefaultWANConfig()
	for _, ip := range tests {
		localV4 := net.ParseIP(ip)
		if err := config.IPAllowed(localV4); err != nil {
			t.Fatalf("IP %s Localhost Should be accepted for WAN", ip)
		}
	}
}

func Test_IsValidAddressOverride(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		allow   []string
		success []string
		fail    []string
	}{
		{
			name:    "Default, nil allows all",
			allow:   nil,
			success: []string{"127.0.0.5", "10.0.0.9", "192.168.1.7", "::1"},
			fail:    []string{},
		},
		{
			name:    "Only IPv4",
			allow:   []string{"0.0.0.0/0"},
			success: []string{"127.0.0.5", "10.0.0.9", "192.168.1.7"},
			fail:    []string{"fe80::38bc:4dff:fe62:b1ae", "::1"},
		},
		{
			name:    "Only IPv6",
			allow:   []string{"::0/0"},
			success: []string{"fe80::38bc:4dff:fe62:b1ae", "::1"},
			fail:    []string{"127.0.0.5", "10.0.0.9", "192.168.1.7"},
		},
		{
			name:    "Only 127.0.0.0/8 and ::1",
			allow:   []string{"::1/128", "127.0.0.0/8"},
			success: []string{"127.0.0.5", "::1"},
			fail:    []string{"::2", "178.250.0.187", "10.0.0.9", "192.168.1.7", "fe80::38bc:4dff:fe62:b1ae"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			config := DefaultLANConfig()
			var err error
			config.CIDRsAllowed, err = ParseCIDRs(testCase.allow)
			if err != nil {
				t.Fatalf("failed parsing %s", testCase.allow)
			}
			for _, ips := range testCase.success {
				ip := net.ParseIP(ips)
				if err := config.IPAllowed(ip); err != nil {
					t.Fatalf("Test case with %s should pass", ip)
				}
			}
			for _, ips := range testCase.fail {
				ip := net.ParseIP(ips)
				if err := config.IPAllowed(ip); err == nil {
					t.Fatalf("Test case with %s should fail", ip)
				}
			}
		})

	}

}

// TestParseCIDRsReportsEveryInvalidEntry: ParseCIDRs keeps the valid
// networks and reports each invalid entry in one joined error.
func TestParseCIDRsReportsEveryInvalidEntry(t *testing.T) {
	nets, err := ParseCIDRs([]string{"10.0.0.0/8", "bogus", " 192.168.0.0/16 ", "300.1.1.1/8"})
	if len(nets) != 2 {
		t.Fatalf("parsed %d networks, want 2", len(nets))
	}
	if err == nil || !strings.Contains(err.Error(), "invalid cidr: bogus") || !strings.Contains(err.Error(), "invalid cidr: 300.1.1.1/8") {
		t.Fatalf("error %v does not report both invalid entries", err)
	}
	if nets, err := ParseCIDRs(nil); err != nil || len(nets) != 0 {
		t.Fatalf("ParseCIDRs(nil) = %v, %v; want empty, nil", nets, err)
	}
}
