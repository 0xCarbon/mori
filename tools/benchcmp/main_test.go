// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseStripsProcsAndCollectsUnits(t *testing.T) {
	input := `goos: linux
BenchmarkX/a-4   	 100	  12.5 ns/op	  8 B/op	  1 allocs/op
BenchmarkX/a-4   	 100	  13.5 ns/op	  8 B/op	  1 allocs/op
BenchmarkY-16    	  10	  99 ns/op
PASS
`
	s, err := parse(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.order, ","); got != "BenchmarkX/a,BenchmarkY" {
		t.Fatalf("order = %s", got)
	}
	if got := s.units["BenchmarkX/a"]["ns/op"]; len(got) != 2 || got[0] != 12.5 || got[1] != 13.5 {
		t.Fatalf("ns/op samples = %v", got)
	}
	if got := s.units["BenchmarkX/a"]["allocs/op"]; len(got) != 2 {
		t.Fatalf("allocs/op samples = %v", got)
	}
}

func TestMannWhitneyExactAndApproximate(t *testing.T) {
	// Completely separated samples of 5 and 5: exact two-sided p = 2/C(10,5).
	low := []float64{1, 2, 3, 4, 5}
	high := []float64{6, 7, 8, 9, 10}
	if got, want := mannWhitney(low, high), 2.0/252; math.Abs(got-want) > 1e-12 {
		t.Fatalf("exact p = %v, want %v", got, want)
	}
	// Identical distributions are never significant.
	if got := mannWhitney([]float64{1, 1, 1}, []float64{1, 1, 1}); got != 1 {
		t.Fatalf("all-tied p = %v, want 1", got)
	}
	// Interleaved samples are not significant.
	if got := mannWhitney([]float64{1, 3, 5, 7, 9}, []float64{2, 4, 6, 8, 10}); got < 0.5 {
		t.Fatalf("interleaved p = %v, want >= 0.5", got)
	}
	// Ties use the normal approximation and still detect clear separation.
	if got := mannWhitney([]float64{1, 1, 2, 2, 3, 3}, []float64{7, 7, 8, 8, 9, 9}); got > 0.01 {
		t.Fatalf("tied separated p = %v, want <= 0.01", got)
	}
}

func TestRunReportsOnlySignificantChanges(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, values ...string) string {
		var b strings.Builder
		for _, v := range values {
			b.WriteString("BenchmarkFast-4 100 " + v + " ns/op\n")
			b.WriteString("BenchmarkSame-4 100 50 ns/op\n")
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	old := write("old.txt", "100", "101", "102", "103", "104", "105")
	cur := write("new.txt", "50", "51", "52", "53", "54", "55")
	var stdout, stderr bytes.Buffer
	if code := run([]string{old, cur}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "BenchmarkFast") || !strings.Contains(out, "-48.8%") {
		t.Fatalf("missing significant speedup:\n%s", out)
	}
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(line, "BenchmarkSame") && !strings.Contains(line, " ~ ") {
			t.Fatalf("unchanged benchmark reported as a change: %q", line)
		}
	}
	if code := run([]string{old}, &stdout, &stderr); code != 2 {
		t.Fatalf("missing argument exit = %d, want 2", code)
	}
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{old, empty}, &stdout, &stderr); code != 1 {
		t.Fatalf("empty input exit = %d, want 1", code)
	}
}
