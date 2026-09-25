# W8 — independent review and dispositions

Three reviewers that did not edit the change read `git diff v0.7.0..HEAD`
at cc81e57: (1) the wire codec against go-msgpack's source, (2) concurrency
and security of the library, (3) tests, tooling and documentation. None
found a critical or high issue. Every finding and its disposition:

| # | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| 1 | medium | Push/pull amplification: each 1-byte nil decoded to a ~112-byte node state (plus an escaping temporary and slice growth) | Fixed: nameless states refused, states decoded in place; `TestReadRemoteStateRefusesNamelessStates` (RED in `red.txt`) |
| 2 | medium | `SendReliable` returned nil for >20 MiB messages the receiver refuses | Fixed: `ErrMessageTooLarge` on the sender, doc corrected; `TestSendReliableRefusesOversizedMessages` (RED) |
| 3 | medium | `FuzzReadStream` claimed decryption coverage but had no key | Fixed: plaintext and encrypted+labeled instances and seeds; `TestFuzzStreamSeedsReachMerge` proves both reach the merge |
| 4 | low | 32-bit: map32 count >= 2^31 wrapped negative in stream mode | Fixed: counts above 2^31-1 refused; `TestMapHeaderCountFitsInt` (RED on GOARCH=386); `make ci` now runs the suite on 386 |
| 5 | low | Decoder stricter than go-msgpack for inputs no encoder produces (arrays as bytes, structs as arrays, uint64 > MaxInt64 into int), contrary to the docs | Kept strict; package docs narrowed to state it |
| 6 | low | 40 MiB plaintext/decompressed push/pull cap vs v0.7.0 senders of very large states | Kept (encrypted push/pull was already capped at 20 MiB upstream); README upgrade notes and CHANGELOG state it; senders now report their own oversize state |
| 7 | low | Pooled LZW coders kept their last buffers reachable; a 4 KiB bufio.Writer per Reset | Fixed: coders parked on shared idle endpoints; `lzwBuffer` provides Flush; decompression reads into a sized slice (round trip 4.1 KB/13 allocs -> 2.7 KB/10) |
| 8 | low | Pre-existing race: `rawSendMsgPacket` read `PMax` after releasing the node lock | Fixed: node copied under the lock |
| 9 | low | Test helper treated nil and empty byte slices as equal, unlike testify; helpers marked themselves, not callers, as helpers | Fixed: testify semantics; `t.Helper()` (retry.R gained a no-op Helper) |
| 10 | low | CI did not run `bench-smoke`; docs claimed parity; action SHAs unannotated | Fixed: test job runs `make test-386` and `make bench-smoke`; SHAs annotated |
| 11 | low | checkdeps missed `.S`/`.sx`, cgo sources and `import "C"` | Fixed, with tests |
| 12 | low | Docs: README baseline label ("than v0.7.0") and Go version; stale interop receipt; stacked comment; one-test-file rule vs shared test files; unrecorded fuzz counts | Fixed; interop re-run on the final tree; `fuzz.txt` receipt |
| 13 | low | simnet refuses dials at once where a real unreachable host would time out | Documented in the review only: probes still wait for the probe interval, which the timing tests assert |

While recording `fuzz.txt`, a 60 s run failed once: the fuzz function
rebuilt its instance through a helper that called `f.Helper`, which
`testing` forbids inside a fuzz target. It was a harness defect, not a
Mori one; instances are now rebuilt without a `testing.F`, and the two
artifact inputs were discarded. The final receipt: FuzzIngestPacket 15.9 M,
FuzzReadStream 9.8 M, FuzzDecoder 19.9 M, FuzzDecodeMessages 30.7 M
executions, 60 s each, no failure.

taba (the consumer) was built against this tree from a scratch copy: the
only source change needed is `Config.LogOutput = io.Discard` ->
`Config.Logger = slog.New(slog.DiscardHandler)` (two lines), plus raising
its `go` directive to 1.27.1. Its full suite passed (335 s) except its own
contract test pinning `go 1.26.5`.
