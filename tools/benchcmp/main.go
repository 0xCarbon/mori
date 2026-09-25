// SPDX-License-Identifier: Apache-2.0
// Origin: github.com/0xCarbon/decimal tools/benchcmp (same owner).

// Command benchcmp compares two sets of `go test -bench` results using only
// the standard library. It is the repository's dependency-compliant stand-in
// for benchstat: for every benchmark and unit present in both inputs it
// reports the old and new medians, the relative change and a two-sided
// Mann-Whitney U p-value. A change whose p-value exceeds alpha is reported as
// "~" (no significant difference), never as a speedup or slowdown. Rows whose
// old and new observations are all identical (typically 0 allocs/op) are
// omitted.
//
// Usage:
//
//	go run ./tools/benchcmp [-alpha 0.05] old.txt new.txt
//
// Run each side with -count of at least 6 (preferably 10, interleaved) so the
// test has enough power; with fewer samples every row reports "~".
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// samples maps benchmark name -> unit -> observations, in first-seen order.
type samples struct {
	order []string
	units map[string]map[string][]float64
}

var procSuffix = regexp.MustCompile(`-\d+$`)

func parse(r io.Reader) (samples, error) {
	s := samples{units: map[string]map[string][]float64{}}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		if _, err := strconv.ParseUint(fields[1], 10, 64); err != nil {
			continue // not a result line
		}
		name := procSuffix.ReplaceAllString(fields[0], "")
		byUnit, ok := s.units[name]
		if !ok {
			byUnit = map[string][]float64{}
			s.units[name] = byUnit
			s.order = append(s.order, name)
		}
		for i := 2; i+1 < len(fields); i += 2 {
			v, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				return s, fmt.Errorf("benchmark %s: value %q: %w", name, fields[i], err)
			}
			byUnit[fields[i+1]] = append(byUnit[fields[i+1]], v)
		}
	}
	return s, scanner.Err()
}

func median(xs []float64) float64 {
	sorted := slices.Clone(xs)
	slices.Sort(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// mannWhitney returns the two-sided p-value for the null hypothesis that x
// and y come from the same distribution. Without ties it uses the exact U
// distribution; with ties it uses the tie-corrected normal approximation.
func mannWhitney(x, y []float64) float64 {
	n1, n2 := len(x), len(y)
	if n1 == 0 || n2 == 0 {
		return 1
	}
	type obs struct {
		v     float64
		fromX bool
	}
	all := make([]obs, 0, n1+n2)
	for _, v := range x {
		all = append(all, obs{v, true})
	}
	for _, v := range y {
		all = append(all, obs{v, false})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v < all[j].v })
	var rankX, tieTerm float64
	ties := false
	for i := 0; i < len(all); {
		j := i
		for j < len(all) && all[j].v == all[i].v {
			j++
		}
		rank := float64(i+j+1) / 2 // average of 1-based ranks i+1..j
		if t := float64(j - i); t > 1 {
			ties = true
			tieTerm += t*t*t - t
		}
		for k := i; k < j; k++ {
			if all[k].fromX {
				rankX += rank
			}
		}
		i = j
	}
	u := rankX - float64(n1*(n1+1))/2
	if !ties && n1*n2 <= 400 {
		return exactU(u, n1, n2)
	}
	n := float64(n1 + n2)
	mean := float64(n1*n2) / 2
	variance := float64(n1*n2) / 12 * ((n + 1) - tieTerm/(n*(n-1)))
	if variance <= 0 {
		return 1
	}
	z := (math.Abs(u-mean) - 0.5) / math.Sqrt(variance)
	if z < 0 {
		z = 0
	}
	return math.Min(1, math.Erfc(z/math.Sqrt2))
}

// exactU counts the arrangements with a U at least as extreme as observed.
func exactU(u float64, n1, n2 int) float64 {
	// counts[i][j][k]: ways for i x-items and j y-items to reach U=k.
	maxU := n1 * n2
	prev := make([][]float64, n2+1)
	for j := range prev {
		prev[j] = make([]float64, maxU+1)
		prev[j][0] = 1
	}
	for i := 1; i <= n1; i++ {
		cur := make([][]float64, n2+1)
		for j := range cur {
			cur[j] = make([]float64, maxU+1)
			for k := 0; k <= maxU; k++ {
				// Largest item is x (contributes j) or y (contributes 0).
				if k >= j {
					cur[j][k] += prev[j][k-j]
				}
				if j > 0 {
					cur[j][k] += cur[j-1][k]
				}
			}
		}
		prev = cur
	}
	dist := prev[n2]
	var total, tail float64
	mean := float64(maxU) / 2
	deviation := math.Abs(u - mean)
	for k, ways := range dist {
		total += ways
		if math.Abs(float64(k)-mean) >= deviation-1e-9 {
			tail += ways
		}
	}
	return tail / total
}

func constant(x, y []float64) bool {
	for _, v := range slices.Concat(x, y) {
		if v != x[0] {
			return false
		}
	}
	return true
}

func format(v float64) string {
	switch a := math.Abs(v); {
	case a == 0:
		return "0"
	case a >= 100:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case a >= 10:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
}

func compare(w io.Writer, old, cur samples, alpha float64) error {
	out := fmt.Appendf(nil, "%-64s %-10s %12s %12s %9s %8s %s\n", "benchmark", "unit", "old", "new", "delta", "p", "n")
	for _, name := range old.order {
		newUnits, ok := cur.units[name]
		if !ok {
			continue
		}
		units := make([]string, 0, len(old.units[name]))
		for unit := range old.units[name] {
			if _, ok := newUnits[unit]; ok {
				units = append(units, unit)
			}
		}
		slices.Sort(units)
		for _, unit := range units {
			x, y := old.units[name][unit], newUnits[unit]
			if constant(x, y) {
				continue // identical observations carry no comparison
			}
			mo, mn := median(x), median(y)
			p := mannWhitney(x, y)
			delta := "~"
			if p <= alpha && mo != mn {
				if mo == 0 {
					delta = "+inf%"
				} else {
					delta = fmt.Sprintf("%+.1f%%", (mn-mo)/mo*100)
				}
			}
			out = fmt.Appendf(out, "%-64s %-10s %12s %12s %9s %8.3f %d+%d\n", name, unit, format(mo), format(mn), delta, p, len(x), len(y))
		}
	}
	_, err := w.Write(out)
	return err
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("benchcmp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	alpha := flags.Float64("alpha", 0.05, "significance level for reporting a change")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 2 {
		_, _ = fmt.Fprintln(stderr, "usage: benchcmp [-alpha 0.05] old.txt new.txt")
		return 2
	}
	var sets [2]samples
	for i, path := range flags.Args() {
		f, err := os.Open(path)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "benchcmp:", err)
			return 1
		}
		sets[i], err = parse(f)
		_ = f.Close()
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcmp: %s: %v\n", path, err)
			return 1
		}
		if len(sets[i].order) == 0 {
			_, _ = fmt.Fprintf(stderr, "benchcmp: %s: no benchmark result lines\n", path)
			return 1
		}
	}
	if err := compare(stdout, sets[0], sets[1], *alpha); err != nil {
		_, _ = fmt.Fprintln(stderr, "benchcmp:", err)
		return 1
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
