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

Upstream `master` is merged in periodically when it benefits Mori.

## Fork-specific features

Beyond the fork point, Mori adds capabilities that are not in upstream
memberlist (see the [CHANGELOG](CHANGELOG.md) for exact semantics):

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

## Migrating from hashicorp/memberlist

The fork point (v0.6.0) is API-compatible with upstream — only the module
path and package identifier changed:

* `import "github.com/hashicorp/memberlist"` → `import "github.com/0xCarbon/mori"`
* `memberlist.Create(...)` → `mori.Create(...)`

No `replace` directive is needed — depend on `github.com/0xCarbon/mori`
directly.

Since v0.7.0 the API has diverged from upstream: new `Config` knobs,
context-aware `Leave`/`UpdateNode`, typed sentinel errors, and asynchronous
`EventDelegate` delivery. See [Fork-specific features](#fork-specific-features)
and the [CHANGELOG](CHANGELOG.md) before upgrading across that boundary.

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

## Metrics Emission and Compatibility

This library can emit metrics using either `github.com/armon/go-metrics` or
`github.com/hashicorp/go-metrics`. Choosing between the libraries is
controlled via build tags.

**Build Tags**
* `armonmetrics` - Using this tag will cause metrics to be routed to `armon/go-metrics`
* `hashicorpmetrics` - Using this tag will cause all metrics to be routed to `hashicorp/go-metrics`

If no build tag is specified, the default behavior is to use `armon/go-metrics`,
which is deprecated — new code should build with the `hashicorpmetrics` tag.

## License

Mozilla Public License 2.0, inherited from hashicorp/memberlist — see
[LICENSE](LICENSE). Original code copyright HashiCorp, Inc. / IBM Corp.;
fork-specific changes copyright 0xCarbon.
