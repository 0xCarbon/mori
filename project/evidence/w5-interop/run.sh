#!/usr/bin/env bash
# Mixed-version interop: Mori v0.7.0 (go-msgpack) and the current tree
# (owned codec) in one cluster, for plain, compressed, and encrypted+labeled
# gossip. Each scenario runs two old and two new nodes on loopback; every
# node must see all four members with their updated meta and receive the
# reliable and best-effort messages every other node sent it.
set -euo pipefail
cd "$(dirname "$0")"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
export CGO_ENABLED=0 GOTOOLCHAIN=local GOWORK=off
go build -o "$out/new" ./node
go build -modfile=old.mod -tags moriold -o "$out/old" ./node
echo "go: $(go version); old: github.com/0xCarbon/mori v0.7.0; new: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- ../../.. || echo +dirty)"

# Below the Linux ephemeral range (32768+), so client sockets cannot hold our ports.
base=$((10000 + RANDOM % 20000))
# scenario NAME KA KB KC KD FLAGS...: KA..KD choose the old or new binary.
scenario() {
	local name=$1 ka=$2 kb=$3 kc=$4 kd=$5; shift 5
	local p=$base; base=$((base + 10))
	local A=127.0.0.1:$p B=127.0.0.1:$((p+1)) C=127.0.0.1:$((p+2)) D=127.0.0.1:$((p+3))
	"$out/$ka" -name A -port $p "$@" -update-at 2s -send-at 3s -run 6300ms > "$out/A" &
	sleep 0.3
	"$out/$kb" -name B -port $((p+1)) -join $A "$@" -update-at 2500ms -send-at 3s -run 6s > "$out/B" &
	"$out/$kc" -name C -port $((p+2)) -join $B "$@" -send-at 3s -run 6s > "$out/C" &
	"$out/$kd" -name D -port $((p+3)) -join $C "$@" -send-at 3s -run 6s > "$out/D" &
	wait
	python3 - "$name" "$out" <<'PY'
import json, sys
name, d = sys.argv[1], sys.argv[2]
want_meta = {"A": "updated-A", "B": "updated-B", "C": "meta-C", "D": "meta-D"}
ok = True
for n in "ABCD":
    r = json.loads(open(f"{d}/{n}").read())
    if r["members"] != want_meta:
        ok = False; print(f"  {name}: {n} sees {r['members']}")
    for kind in ("reliable", "besteffort"):
        got = {m for m in r["msgs"] if m.startswith(kind + ":")}
        want = {f"{kind}:{o}->{n}" for o in "ABCD" if o != n}
        if got != want:
            ok = False; print(f"  {name}: {n} {kind} messages {sorted(got)}, want {sorted(want)}")
    gossip = {m for m in r["msgs"] if m.startswith("gossip:")}
    if gossip != {f"gossip:{o}" for o in "ABCD" if o != n}:
        ok = False; print(f"  {name}: {n} gossip broadcasts {sorted(gossip)}")
print(f"{name}: {'PASS' if ok else 'FAIL'} (membership, meta updates, reliable, best-effort and gossip user messages)")
sys.exit(0 if ok else 1)
PY
}

status=0
for mix in "all-old old old old old" "all-new new new new new" "mixed old new old new" "mixed-rev new old new old"; do
	set -- $mix
	tag=$1; shift
	scenario "$tag/plain" "$@" -compress=false || status=1
	scenario "$tag/compressed" "$@" -compress=true || status=1
	scenario "$tag/encrypted-labeled" "$@" -compress=true -secret 000102030405060708090a0b0c0d0e0f -label interop || status=1
done
exit $status
