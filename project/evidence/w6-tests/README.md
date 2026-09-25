# W6 — standard-library tests, synctest and the simulated network

## Dependencies

testify and goleak are replaced by `helpers_test.go` (assertion helpers
with testify's argument order; byte slices compared by content as testify
did) and `verifyNoGoroutineLeak` (a `runtime.Stack` diff with a grace
period, self-tested). `go.mod` has no requirements.

## Deterministic timing

`simnet_test.go` is an in-memory network (channels and `net.Pipe`) with UDP
and TCP semantics the protocol relies on: packets to unknown, down or
congested nodes drop silently; dials to them fail with a `dial`
`*net.OpError` (`failedRemote`); an optional one-way latency. A whole
cluster built on it runs inside a `testing/synctest` bubble, where time is
exact and advances only when every goroutine is blocked.

Converted tests now assert exact values instead of real-time windows:

| Test | Before | After | Assertion |
| --- | --- | --- | --- |
| TestSuspicion_Timer | 13.08 s | 0.00 s | fires at exactly 2000/1250/811/500 ms (was ±25 ms) |
| TestMemberList_ProbeNode_Suspect_Dogpile | 7.00 s | 0.01 s | dead exactly at the timeout (603 ms, not the approximated 604) |
| TestMemberlist_EncryptedGossipTransition | 5.31 s | 0.01 s | + negative control: plaintext node cannot join |
| TestTransport_TcpListenBackoff | 4.28 s | 0.00 s | exactly 11 accept errors in 4 s; exit 275 ms after shutdown (was 8 < n < 14) |
| TestMemberlist_delegateMeta_Update | 1.67 s | 0.00 s | both views of both updates |
| TestMemberList_Ping | 1.00 s | 0.00 s | RTT exactly 2 × latency; timeout exactly ProbeTimeout |
| ProbeNode_Suspect / Awareness_{Degraded,MissedNack,OldProtocol} | 1.52 s | 0.00 s | probe lasts exactly (score+1) × ProbeInterval; exact indirect pings and scores |

Suite total (sum of top-level test times): 39.6 s → 6.6 s before the probe
family, measured with `go test -json`, go1.27.1 linux/amd64.

## Mutation check

Two deliberate defects, each reverted after the run:

- `probeNode` ignoring awareness scaling: `TestMemberList_ProbeNode_Awareness_Degraded` fails.
- suspicion timeout rounded up (`math.Ceil`) instead of down: `TestSuspicion_Timer` ("did not fire at 811ms") and the dogpile test ("still suspect at 603ms") fail. The previous ±25 ms real-time assertions could not see a 1 ms shift.

Real-network tests remain for `NetTransport` itself and for end-to-end
joins over loopback.
