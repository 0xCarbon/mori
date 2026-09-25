# Contributing to Mori

Thank you for contributing. Mori is 0xCarbon's maintained fork of
hashicorp/memberlist; this guide covers how to report problems and what a
change needs before it can merge. The binding engineering rules (toolchain,
dependency policy, wire compatibility, validation) are in
[AGENTS.md](AGENTS.md), which applies to humans and coding agents alike.

## Reporting security issues

Do not open public issues for vulnerabilities. Follow
[SECURITY.md](SECURITY.md).

## Reporting bugs

1. Check the existing issues first.
2. Reproduce the problem on the latest release.
3. Include a minimal, self-contained reproduction — ideally a failing test.
   Timing-dependent behavior can usually be reproduced deterministically
   with `testing/synctest` on the in-memory network in `simnet_test.go`.

## Requesting features

1. Check the existing issues first.
2. Describe the problem, not only the solution you have in mind.
3. Keep the scope narrow. Mori's wire protocol must stay compatible with
   memberlist peers, and the module takes no dependencies beyond the
   standard library; requests that need either to change are unlikely to be
   accepted.

## Pull requests

- **Small is better.** A focused change reviews faster.
- **RED before GREEN.** A bug fix starts with a test that fails on the
  current code for the intended reason; paste that failing output in the
  pull request description.
- **Tests and docs move with the code.** Behavior changes update or add
  tests, godoc comments and README/SECURITY as needed, plus a CHANGELOG
  entry under `Unreleased` (mark breaking changes **BREAKING**).
- **`make ci` passes.** It runs formatting, `go fix -diff`, vet,
  golangci-lint, cross builds, the dependency audit, the tests on amd64 and
  386, the race detector and a benchmark smoke. Changes to `internal/wire`
  or `internal/msgpack` also run `make oracle` (the go-msgpack
  differential).
- **Evidence goes in the pull request, not the tree.** Benchmark
  comparisons (interleaved runs compared with `go run ./tools/benchcmp`),
  fuzz runs and other receipts belong in the description; raw outputs,
  plans and one-off harnesses are not committed.

## Maintainer response time

Mori is maintained on a best-effort basis. Please allow reasonable time for
a response; repeated pings or duplicate issues do not speed it up.

## AI usage

AI-assisted contributions are welcome. Contributors remain responsible for
the final submission. Before opening a pull request:

- review and understand every change;
- verify that it follows [AGENTS.md](AGENTS.md) and the surrounding code;
- make sure the description states what was tested and how.

Do not submit code you cannot explain during review.
