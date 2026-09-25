#!/usr/bin/env bash
# Interleaved A/B benchmark runs of two Mori trees.
#
#   run.sh OLD NEW OUT [ROUNDS]
#
# OLD and NEW are git revisions, or "." for the current working tree. For
# each, the portable internal benchmarks (portable_bench_test.go.txt) and
# the public benchmarks/ package are compiled once into test binaries; the
# binaries then run alternately ROUNDS times (default 10), so drift in the
# machine affects both sides equally. Raw results go to OUT/old.txt and
# OUT/new.txt, the comparison (Mann-Whitney U, p <= 0.05) to OUT/cmp.txt.
set -euo pipefail
old=$1 new=$2 out=$3 rounds=${4:-10}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
work=$(mktemp -d)
trap 'for d in "$work"/tree-*; do [ -d "$d" ] && git -C "$repo" worktree remove --force "$d" 2>/dev/null; done; rm -rf "$work"' EXIT
mkdir -p "$out"
out=$(cd "$out" && pwd)
out=$(cd "$out" && pwd)
export CGO_ENABLED=0 GOTOOLCHAIN=local GOWORK=off

build() { # side rev
	local side=$1 rev=$2 tree
	if [ "$rev" = "." ]; then
		tree="$work/tree-$side"
		mkdir -p "$tree"
		(cd "$repo" && git ls-files -co --exclude-standard | grep -v '^project/' | tar -cf - -T -) | tar -xf - -C "$tree"
	else
		tree="$work/tree-$side"
		git -C "$repo" worktree add -q --detach "$tree" "$rev"
	fi
	cp "$here/portable_bench_test.go.txt" "$tree/zz_portable_bench_test.go"
	mkdir -p "$tree/benchmarks"
	cp "$repo/benchmarks/benchmarks_test.go" "$tree/benchmarks/"
	(cd "$tree" && go test -c -o "$work/$side-mori.test" . && go test -c -o "$work/$side-bench.test" ./benchmarks)
	echo "$side: $rev ($(git -C "$repo" rev-parse --short "${rev/#./HEAD}")$([ "$rev" = . ] && ! git -C "$repo" diff --quiet HEAD && echo +dirty))"
}

{
	echo "go: $(go version)"
	echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //'), $(nproc) threads"
	echo "os: $(uname -srm)"
	build old "$old"
	build new "$new"
	echo "rounds: $rounds interleaved"
} | tee "$out/env.txt"

: > "$out/old.txt"
: > "$out/new.txt"
for i in $(seq "$rounds"); do
	for side in old new; do
		"$work/$side-mori.test" -test.run '^$' -test.bench 'Portable' -test.benchmem -test.count 1 -test.benchtime 2000x 2>/dev/null | grep '^Benchmark' >> "$out/$side.txt"
		"$work/$side-bench.test" -test.run '^$' -test.bench '.' -test.benchmem -test.count 1 -test.benchtime 1s 2>/dev/null | grep '^Benchmark' >> "$out/$side.txt"
	done
	echo "round $i/$rounds done"
done
(cd "$repo" && go run ./tools/benchcmp "$out/old.txt" "$out/new.txt") | tee "$out/cmp.txt"
