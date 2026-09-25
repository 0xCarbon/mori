# Mori [![Go Reference](https://pkg.go.dev/badge/github.com/0xCarbon/mori.svg)](https://pkg.go.dev/github.com/0xCarbon/mori)

Mori is a [Go](https://go.dev) library that manages cluster membership and
member failure detection using a gossip based protocol.

It is 0xCarbon's maintained hard fork of
[hashicorp/memberlist](https://github.com/hashicorp/memberlist), carried
forward as its own module: `github.com/0xCarbon/mori`, package `mori`.

The use cases for such a library are far-reaching: all distributed systems
require membership, and Mori is a re-usable solution to managing cluster
membership and node failure detection.

Mori is eventually consistent but converges quickly on average. The speed at
which it converges can be heavily tuned via various knobs on the protocol.
Node failures are detected and network partitions are partially tolerated by
attempting to communicate to potentially dead nodes through multiple routes.

## Why a fork

Mori exists to serve the [taba](https://github.com/0xCarbon/taba) stack
(membership, consistent hashing, and RPC over WebTransport), which needs:

* A stable, ownable release cadence — the fork point already carries upstream
  fixes that were never released as a memberlist tag: keyring concurrent
  read/write fix (hashicorp#342), nil pointer dereference fix (hashicorp#336),
  broadcast data race fix (hashicorp#273), probe selection for very small
  clusters (hashicorp#350), and remote state header limits (hashicorp#357).
* Room for features upstream does not want or need. Fork-specific changes are
  documented in the [CHANGELOG](CHANGELOG.md).

Upstream `master` is reviewed periodically; fixes that benefit Mori are
ported (with regression tests), not merged wholesale: the code bases have
diverged. Mori's wire protocol stays compatible with memberlist peers.

## Fork-specific features

Beyond the fork point, Mori adds capabilities that are not in upstream
memberlist (see the [CHANGELOG](CHANGELOG.md) for exact semantics):

* **No dependencies.** The module requires nothing but the Go standard
  library (Go 1.27.1): no go-msgpack, go-metrics, go-sockaddr, miekg/dns,
  btree or testify. It owns its MessagePack codec, byte-for-byte compatible
  with go-msgpack (proven by golden vectors, a randomized differential run
  by `make oracle`, and mixed-version clusters against v0.7.0; see the
  v0.8.0 pull request, #22).
* **Hardened input handling.** Every length or count from the network is
  bounded before it can size an allocation, decoders never panic, and the
  packet and stream paths are fuzzed end to end. See
  [SECURITY.md](SECURITY.md) for the threat model and the bounds.
* **Structured logging and per-instance metrics.** `Config.Logger` is a
  `*slog.Logger`; `Config.Metrics` is a `MetricSink` interface owned by the
  instance (no global registry), with the metric list documented on it.
* **Performance.** Handling an alive message is 6× faster with 5× fewer
  allocations than before this release; compressed and encrypted packets reuse pooled LZW coders
  and cached AES-GCM ciphers; the UDP listener no longer allocates 64 KiB per
  datagram (measurements in pull request #22).
* **Configurable node-meta cap** — `Config.MetaMaxSize` sets the producer-side
  limit (bytes) on `Delegate.NodeMeta`, validated fail-fast against the
  transport packet budget at `Create`; oversized meta returns `ErrMetaTooLarge`
  instead of panicking.
* **Parallel anti-entropy** — `Config.PushPullConcurrency` runs full state
  exchanges against multiple random alive peers per cycle, removing
  head-of-line blocking on a slow peer.
* **Context-aware lifecycle** — `LeaveContext(ctx)` and `UpdateNodeContext(ctx)`
  bound the whole operation (internal lock acquisition and broadcast wait);
  `Leave` and `UpdateNode` are thin timeout wrappers over them and now return
  typed sentinel errors (`ErrShutdown`, `ErrMetaTooLarge`, `ErrNoLocalNode`,
  `ErrLeft`, matchable with `errors.Is`) instead of panicking.
* **Async ordered event delivery** — `EventDelegate` and `ConflictDelegate`
  callbacks are dispatched asynchronously in commit order off the node lock, so
  a blocking or re-entrant callback no longer stalls or deadlocks membership
  processing.
* **Deterministic shutdown** — `Shutdown` joins every background goroutine,
  clears pending suspicion timers, and drains the event queue before returning.
* **Transport packet-size awareness** — the optional `MaxPacketSizeTransport`
  interface lets a transport advertise its largest deliverable payload;
  `NetTransport` reports the 65507-byte IPv4 UDP ceiling.

## Upgrading from v0.7

v0.8 changes the API in a few places (full list under **BREAKING** in the
[CHANGELOG](CHANGELOG.md)):

* `Config.LogOutput` is gone and `Config.Logger` is a `*slog.Logger` (nil
  uses `slog.Default()`). Replace `c.LogOutput = io.Discard` with
  `c.Logger = slog.New(slog.DiscardHandler)`. The `LogAddress`/`LogConn`/
  `LogStringAddress` helpers are removed.
* Metrics go to `Config.Metrics` (a `MetricSink`); `Config.MetricLabels` is
  `[]mori.Label`. The CHANGELOG shows a four-method adapter to go-metrics.
* `Config.MsgpackUseNewTimeFormat` is removed (it had no effect).
* `Node.State` now reports the real state in `Members()`, `LocalNode()` and
  event callbacks (it was always `StateAlive`).

The wire protocol is unchanged: v0.7 and v0.8 nodes can run in one cluster
during a rolling upgrade. Two new receive limits can refuse traffic a v0.7
node accepted: stream user messages above 20 MiB (`SendReliable` now fails
with `ErrMessageTooLarge` on the sender), and plaintext or decompressed
push/pull states above 40 MiB — roughly 65,000 nodes with 512-byte meta;
encrypted push/pull was already capped at 20 MiB upstream.

## Migrating from hashicorp/memberlist

The fork point (v0.6.0) was API-compatible with upstream — only the module
path and package identifier changed:

* `import "github.com/hashicorp/memberlist"` → `import "github.com/0xCarbon/mori"`
* `memberlist.Create(...)` → `mori.Create(...)`

No `replace` directive is needed — depend on `github.com/0xCarbon/mori`
directly.

Since v0.7.0 the API has diverged from upstream: new `Config` knobs,
context-aware `Leave`/`UpdateNode`, typed sentinel errors, asynchronous
`EventDelegate` delivery, and (v0.8) slog logging and per-instance metrics.
See [Fork-specific features](#fork-specific-features) and the
[CHANGELOG](CHANGELOG.md) before upgrading across those boundaries.

## Usage

Mori is surprisingly simple to use. An example is shown below:

```go
/* Create the initial membership list from a safe configuration.
   Please reference the godoc for other default config types.
   https://pkg.go.dev/github.com/0xCarbon/mori#Config
*/
list, err := mori.Create(mori.DefaultLocalConfig())
if err != nil {
	panic("Failed to create membership list: " + err.Error())
}

// Join an existing cluster by specifying at least one known member.
n, err := list.Join([]string{"1.2.3.4"})
if err != nil {
	panic("Failed to join cluster: " + err.Error())
}

// Ask for members of the cluster
for _, member := range list.Members() {
	fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
}

// Continue doing whatever you need, mori will maintain membership
// information in the background. Delegates can be used for receiving
// events when members join or leave.
```

The most difficult part is configuration, since there are many available knobs
to tune state propagation delay and convergence times. Mori provides a default
configuration that offers a good starting point, but errs on the side of
caution, choosing values that are optimized for higher convergence at the cost
of higher bandwidth usage.

For complete documentation, see the associated
[Go reference](https://pkg.go.dev/github.com/0xCarbon/mori).

## Protocol

Mori is based on ["SWIM: Scalable Weakly-consistent Infection-style Process
Group Membership Protocol"](http://ieeexplore.ieee.org/document/1028914/),
with the extensions made by hashicorp/memberlist:

* Several extensions are made to increase propagation speed and
  convergence rate.
* Another set of extensions, called Lifeguard, make the protocol more robust
  in the presence of slow message processing (due to factors such as CPU
  starvation, and network delay or loss).

For details on all of these extensions, please read the paper "[Lifeguard :
SWIM-ing with Situational Awareness](https://arxiv.org/abs/1707.00788)", along
with the source.

## Metrics

Set `Config.Metrics` to any implementation of `MetricSink` (four methods,
the shape of go-metrics' labeled calls); nil disables metrics. The metric
names (`memberlist.*`) and kinds are listed on the `MetricSink` type.

## Development

`make ci` runs every gate: gofmt, `go fix -diff`, vet, golangci-lint, build,
cross builds, the dependency audit (`tools/checkdeps`), `go mod tidy -diff`,
the test suite, the race detector and a benchmark smoke. The suite runs in
about five seconds: timer-driven protocol tests use `testing/synctest` on an
in-memory network (`simnet_test.go`) and assert exact timings. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for the
contribution rules.

## License

Mozilla Public License 2.0, inherited from hashicorp/memberlist — see
[LICENSE](LICENSE). Original code copyright HashiCorp, Inc. / IBM Corp.;
fork-specific changes copyright 0xCarbon.
