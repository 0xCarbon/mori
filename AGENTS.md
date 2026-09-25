# AGENTS.md

Rules for humans and coding agents working in this repository. When a rule
and a convenience conflict, the rule wins.

Mori is 0xCarbon's maintained hard fork of `hashicorp/memberlist`: gossip
cluster membership and failure detection (SWIM + Lifeguard). Its primary
consumer is [taba](https://github.com/0xCarbon/taba). The wire protocol stays
compatible with memberlist peers; the Go API is Mori's own.

## Authority

Owner instructions, then this file, then [CHANGELOG.md](CHANGELOG.md) (the
behavioral contract per release), then code comments.

## Toolchain and dependencies

- **Go 1.27.1, `CGO_ENABLED=0`, `GOTOOLCHAIN=local`** for every build, vet
  and test. The one exception is `make race`: the race detector needs cgo.
- **Standard library only.** The root module (library, tests and in-module
  tools) has no `require` line: every package, tests included, imports only
  the standard library and this module. A tool that genuinely needs more
  lives in its own module under `tools/` and may use only official
  `golang.org/x/` or `github.com/0xCarbon/` modules, with a written reason
  the standard library is insufficient. `make check-deps` enforces this.
  Modules under a `testdata/` directory (ignored by the go command) may pin
  what they compare against: `internal/wire/testdata/oracle` runs
  go-msgpack.
- No project-authored `unsafe`, assembly or object files.
- `make ci` passes before every commit. It runs gofmt, `go fix -diff`
  (modernizers), vet, golangci-lint (`.golangci.yml`), build, cross builds,
  the dependency audit, `go mod tidy -diff`, the suite on amd64 and 386,
  the race detector and a benchmark smoke. CI adds a fuzz smoke.

## Wire compatibility

- The msgpack encoding of every message must stay byte-for-byte compatible
  with go-msgpack v2 `MsgpackHandle{}` output: structs as maps keyed by field
  name with entries sorted by name, legacy raw strings (no str8/bin), nil
  byte slices as nil. Message type numbers are append-only.
- Changes to encoding are proven against go-msgpack, never against the new
  code itself: run `make oracle` (the go-msgpack differential, which also
  regenerates `internal/wire/testdata/golden.json`).
- Every length or count read from the network is bounded before it sizes an
  allocation, and every decoder refuses (never panics on) malformed input.

## Validation

- **RED before GREEN.** A fix starts with a test that fails on the baseline
  for the intended reason (wrong value, error identity, allocation or panic),
  never a missing symbol. Put the failing output in the pull request
  description.
- **Independent oracles** for correctness (go-msgpack vectors, RFC tables,
  hand-derived values).
- **Fast paths never own semantics.** An optimized path returns only what
  the general path would, and differential tests pin the two together.
- **Evidence-based performance.** A performance claim needs interleaved A/B
  runs (at least 8+8) compared with `go run ./tools/benchcmp`; put the
  command, Go version, CPU and results in the pull request description.
  Raw run outputs, plans and one-off harnesses do not belong in the tree.
- Tests use the standard `testing` package. Timer-driven protocol logic is
  tested inside `testing/synctest` bubbles with in-memory transports; real
  network tests are reserved for the transport itself.

## Layout

| Path | Contents |
| --- | --- |
| `/` | package `mori`: files named by subject, one `_test.go` per file, plus shared test infrastructure named by subject (`helpers_test.go`, `simnet_test.go`, `fuzz_test.go`) |
| `internal/` | `msgpack` (format) and `wire` (protocol messages, with the go-msgpack oracle under `testdata/`) |
| `tools/` | development tools (`checkdeps`, `benchcmp`) |
| `benchmarks/` | public-API benchmarks |

## Delivery

Work on a branch, commit per coherent change with the gates green, and
describe behavioral changes in the CHANGELOG `Unreleased` section in the same
commit. Never force-push `main`.
