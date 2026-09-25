# W7 — performance evidence

`run.sh OLD NEW OUT [ROUNDS]` compiles two trees (git revisions, or `.` for
the working tree) into test binaries and runs them alternately, so machine
drift affects both sides equally, then compares with `tools/benchcmp`
(medians, two-sided Mann-Whitney U; `~` means p > 0.05). Two benchmark
sets run on both sides:

- `benchmarks/` (repository package): public API only — end-to-end packet
  ingest through a custom Transport, the broadcast queue at 100 and 10,000
  nodes, NetTransport UDP receive.
- `portable_bench_test.go.txt`: internal paths that exist in every compared
  tree (`handleAlive`, `handlePing`, `mergeRemoteState` of 1,000 nodes, an
  AES-GCM encrypted ping through `ingestPacket`). Messages are hand-encoded,
  so neither codec under comparison produces the input.

Environment for both phases: go1.27.1 linux/amd64, AMD Ryzen 9 5900XT
(16 cores, 32 threads), Linux 7.0.0; 10 interleaved rounds per side; not a
quiet host, so only p ≤ 0.05 rows are claims.

## Phase A — the refactor (W3–W6): 50e61ef → 984744b

Owned codec, treap queue, per-instance telemetry, math/rand/v2 — no
performance work yet. `phase-a-refactor/`.

| Path | Time | Allocations |
| --- | --- | --- |
| handleAlive | −83.5% (4216 → 697 ns) | 50 → 10 |
| mergeRemoteState, 1,000 nodes | −75.6% (1.94 → 0.47 ms) | 26,000 → 5,000 |
| handlePing | −33.6% | 62 → 16 |
| packet ingest, end to end | −12.3% | 61 → 18 |
| broadcast queue, 100 nodes | −57.1% | 102 → 7 |
| broadcast queue, 10,000 nodes | −42.1% | 123 → 54 |
| NetTransport receive | ~ | 5 → 4 |

## Phase B — W7 optimizations: 984744b → W7 commit

- LZW coders pooled (`sync.Pool` + `Reset`); packets of at most 22 bytes
  are not compressed (provably never smaller; see
  `maxIncompressiblePacket`).
- AES-GCM ciphers cached per key in the Keyring; sealing appends in place.
- NetTransport reads into one reused buffer and copies each datagram out
  at its size (was a fresh 64 KiB buffer per datagram).
- Treap seek without allocation (`treapCeil`/`treapNext`).
- Compound messages built with one append-based allocation.

`phase-b-w7/`:

| Path | Time | Bytes/op | Allocations |
| --- | --- | --- | --- |
| handlePing (compression on) | −94.2% (14.5 → 0.84 µs) | 78,487 → 416 | 16 → 10 |
| encrypted ping ingest | −57.4% (2.95 → 1.26 µs) | 3,928 → 656 | 21 → 13 |
| broadcast queue, 10,000 nodes | −37.4% | 5,556 → 1,227 | 54.5 → 7 |
| broadcast queue, 100 nodes | −10.5% | −6% | ~ |
| NetTransport receive, 64 B | −40.0% | 65,654 → 180 | ~ |
| NetTransport receive, 1400 B | −36.8% | 65,654 → 1,524 | ~ |
| handleAlive, mergeRemoteState, packet ingest | ~ | ~ | ~ |

The unchanged rows are paths the W7 changes do not reach, as expected.
