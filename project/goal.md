# Goal: std-only, hardened, performant Mori (v0.8)

Persistent execution plan. Every wave updates the **Status** column and the
**Log** in the same commit that does the work. Resume from the first wave
that is not `done`.

Branch: `goal/stdlib-hardening` (local; not pushed).

## Target state

1. **Dependency policy (mirrors `../decimal`).** The library module imports
   only the Go standard library. Tools and tests may add official
   `golang.org/x/` or `github.com/0xCarbon/` modules only with a written
   reason. `make check-deps` enforces it. No project-authored `unsafe`.
2. **Toolchain.** Go 1.27.1, `CGO_ENABLED=0`, `GOTOOLCHAIN=local`; `make ci`
   is the single local gate and CI runs the same contract.
3. **Security parity or better with upstream** hashicorp/memberlist master
   (fixes #361, #363, #368, #369), plus bounded decoding everywhere input
   size drives allocation, and fuzz targets on every wire entry point.
4. **Owned wire codec.** Hand-written msgpack subset, byte-for-byte
   compatible with go-msgpack v2 `MsgpackHandle{}` output (maps keyed by
   field name, legacy raw strings, no str8/bin), proven by golden vectors
   captured from the real codec and a differential evidence module.
5. **Owned telemetry.** Per-instance `Config.Metrics` sink (no global
   state) and `log/slog` structured logging.
6. **Deterministic, fast tests.** std `testing` only (no testify/goleak);
   `testing/synctest` for timer-driven protocol logic.
7. **Measured performance.** Benchmarks for the hot paths (packet ingest,
   encode/decode, broadcast queue, push/pull merge) with A/B evidence under
   `project/evidence/`.

## Rules (adapted from decimal AGENTS.md)

- RED before GREEN for fixes; keep the failing output as a receipt in the
  commit message or evidence.
- Independent oracles: wire compatibility is proven against go-msgpack
  output (golden vectors + differential module), never against the new
  codec itself.
- Fast paths never own semantics.
- Performance claims need interleaved A/B runs (>= 8+8) compared with
  `tools/benchcmp`; keep command, Go version, CPU and raw output.
- `make ci` passes before every commit.
- Modernization gate (user-directed, `~/.agents/skills/golang-modernization`):
  `go-modernize --golangci-lint <go1.27-built golangci-lint v2> --config
  .golangci.yml .` returns 0 findings / 0 errors at the end of every wave.
  Findings are evidence, not instructions: verify each against version
  gates, nil-vs-empty, aliasing and concurrency semantics before applying.
  Baseline (go 1.25.0 directive): 0 findings.

## Waves

| Wave | Scope | Status |
| --- | --- | --- |
| W0 | Goal doc, branch, baseline measurements | done |
| W1 | Upstream security fixes (#361 #363 #368 #369) + local hardening of the same classes | done |
| W2 | Tooling: go 1.27.1, Makefile `ci`, `tools/checkdeps`, `tools/benchcmp`, CI workflow, AGENTS.md | done |
| W3 | Small deps: go-multierror, sean-/seed, go-sockaddr, miekg/dns, google/btree | done |
| W4 | Telemetry: `Config.Metrics` sink replaces go-metrics; `log/slog` replaces `log` | pending |
| W5 | Wire codec: `internal/msgpack` + `internal/wire`, golden vectors, differential evidence; drop go-msgpack | pending |
| W6 | Tests: testify/goleak -> std; synctest for timer-driven tests; flake removal | pending |
| W7 | Performance: UDP receive buffer reuse, encode/decode allocations, benchmarks + evidence | pending |
| W8 | Fuzz targets, SECURITY.md threat model, README/CHANGELOG v0.8.0, independent review | pending |

## Decisions

- **D1 (W5):** `internal/wire` owns message types and their codec so the
  differential evidence module (path-nested under this module) can import
  it; `internal/msgpack` owns format primitives. Seam bought: the codec is
  testable and fuzzable without a Memberlist instance.
- **D2 (W4):** metric names keep the `memberlist.*` prefix for dashboard
  continuity; only the delivery mechanism changes.
- **D3 (W4):** `Config.Logger *slog.Logger` replaces `LogOutput`/`Logger`;
  nil means `slog.Default()`.
- **D4 (W3):** TCP-first DNS uses `net.Resolver{PreferGo, Dial: tcp}` with
  A+AAAA queries instead of an ANY query (RFC 8482 minimal ANY answers made
  the old path silently empty on modern resolvers).

## Baseline (W0, 2026-09-25, go1.27.1 linux/amd64)

- `go test -count=1 ./...`: 41.8 s wall, 10.6 s user CPU (sleep-dominated).
- Direct deps: 9 (btree, go-metrics, go-msgpack, go-multierror,
  go-sockaddr, miekg/dns, sean-/seed, testify, goleak); indirect: 12.
- Upstream delta since fork point `371698b`: 13 commits; 4 security fixes
  missing in Mori.

## Log

- 2026-09-25 W0: goal created from the fork evaluation.
- 2026-09-25 W1: 9 RED tests (receipt: project/evidence/w1-security/red.txt),
  all GREEN; full suite passes; go-modernize 0/0. Found beyond upstream:
  header-count preallocation (117 MB from a 10-byte header), unbounded
  plaintext streams, nested envelopes, PKCS7 pad panic.
- 2026-09-25 W2: go 1.27.1; `make ci` green (fmt, go fix -diff, vet,
  golangci-lint 2.13.2/go1.27.1, build, cross x5, checkdeps with 21-entry
  ratchet in project/deps-transition.txt, tidy -diff, test, race). go fix
  modernizers applied after review. Fixed a 32-bit test overflow found by
  the new 386 cross vet.

- 2026-09-25 W3: removed go-multierror, errwrap, seed, go-sockaddr,
  miekg/dns (+5 golang.org/x indirects), google/btree; ratchet 21 -> 10.
  RED receipts in project/evidence/w3/: F1 node state, duplicate queue id
  (message dropped without Finished), RFC 8482 ANY lookup, IPv4 4/16-byte
  address conflict. Treap pinned to a sorted-slice oracle (caught an
  empty-stack seek bug before commit). Advertise-address discovery matched
  go-sockaddr on this host (evidence/w3/sockaddr-differential.txt).
  Deflaked TestMemberlist_Join_Cancel and the queue-metrics test (race
  timing), both pre-existing.

## Findings to fix (discovered during the waves)

- F1 (W3): `nodeState.State` shadows the embedded `Node.State`, so the
  `Node` values handed to users (`Members`, `LocalNode`, event callbacks)
  always report `StateAlive`, even in `NotifyLeave`. Inherited from
  upstream. Fixed in W3.
- F2 (W3, fixed): queue id generator rewound mid-GetBroadcasts; btree
  ReplaceOrInsert then dropped a pending broadcast without Finished.
- F3 (W3, fixed): IPv4 4- vs 16-byte forms compared bytewise produced
  spurious address conflicts.
