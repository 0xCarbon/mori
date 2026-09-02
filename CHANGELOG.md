## Unreleased

### Improvements

### Changes

### Fixed

* `(*Memberlist).Members` and `(*Memberlist).LocalNode` now return snapshot
  copies of `Node` taken under the node lock, instead of pointers into
  live Memberlist-internal state. Callers reading returned fields (`Meta`,
  `Addr`, ...) no longer data race with concurrent alive/update handling;
  slice-typed fields alias buffers that are replaced wholesale, never
  mutated in place, so the copies are safe to read lock-free.

### Security

* Ports of four upstream hashicorp/memberlist security fixes, keeping the
  wire-format semantics unchanged:
  * `readUserMsg` now rejects a `userMsg` header whose `UserMsgLen` is
    negative or exceeds `maxUserMsgBytes` (20 MiB) instead of allocating the
    declared length off the wire (upstream #361).
  * `decompressBuffer` now caps decompressed output at
    `maxDecompressedBytes` (2×`maxPushStateBytes`) via `io.CopyN` and errors
    with "decompressed message is larger than limit" past it, instead of
    draining the LZW reader into an unbounded buffer (upstream #363).
  * `readStream` now treats a compressed message that decompresses to an
    empty payload as an error instead of indexing the empty buffer and
    panicking (upstream #369).
  * Short `Vsn` protocol-version vectors read off the wire are now
    length-checked on all paths: `verifyProtocol` skips nodes with fewer
    than 5 entries and treats fewer than 6 as version-less, and
    `mergeRemoteState`/`aliveNode` only copy versions from slices with at
    least 6 entries, instead of panicking on the short slice (upstream
    #368).

## v0.7.0 (Mori)

This release hardens the node lifecycle, shutdown, and delegate-event
delivery, and contains multiple **BREAKING** behavioral changes (see the
Changes and Fixed sections). Panics on the `Create`/`Leave`/`UpdateNode`
paths are replaced by matchable typed errors (`ErrShutdown`,
`ErrMetaTooLarge`, `ErrNoLocalNode`, `ErrLeft`); `Leave` and `UpdateNode`
gain context-bounded variants (`LeaveContext`/`UpdateNodeContext`) and now
bound the *entire* call by the timeout rather than only the final broadcast
wait. `EventDelegate`/`ConflictDelegate` callbacks move off the internal
node lock onto a single ordered asynchronous dispatcher, fixing
callback-induced membership stalls and deadlocks, and `Shutdown` becomes
fully synchronous — it joins every background goroutine, clears pending
suspicion timers, and drains the event queue before returning. New
configuration adds a producer-side meta cap (`Config.MetaMaxSize`) with
fail-fast transport-budget validation at construction (plus the optional
`MaxPacketSizeTransport` interface) and parallel anti-entropy
(`Config.PushPullConcurrency`).

### Improvements

* Add `(*Memberlist).LeaveContext(ctx)` and `(*Memberlist).UpdateNodeContext(ctx)`:
  context-bounded lifecycle calls that bound the whole operation, including
  internal lock acquisition and the broadcast wait. `Leave`/`UpdateNode`
  become thin timeout wrappers over them.
* Add exported sentinel errors `ErrShutdown`, `ErrMetaTooLarge`,
  `ErrNoLocalNode`, and `ErrLeft` (documented as matchable with
  `errors.Is`), returned by the `Create`/`Leave`/`UpdateNode` lifecycle
  methods and their `Context` variants.
* Add `Config.MetaMaxSize`, a configurable producer-side cap (bytes) on node
  meta from `Delegate.NodeMeta`; `0` keeps the existing 512-byte default.
  `Delegate.NodeMeta` is now offered this effective cap as its `limit`
  argument, and meta exceeding it makes lifecycle calls (`setAlive` /
  `UpdateNode`) return `ErrMetaTooLarge`.
* Add optional `MaxPacketSizeTransport` interface — a `Transport`
  implementing `MaxPacketSize() int` advertises the largest single-`WriteTo`
  payload it can deliver. When present, `Create` validates
  `Config.MetaMaxSize` against it (in addition to `UDPBufferSize`) and the
  outgoing packet budget honors it, unwrapped through the internal
  shim/label transport wrappers.
* Add `Config.PushPullConcurrency` (int): run full state exchanges against
  that many distinct random alive peers in parallel each push/pull cycle,
  removing head-of-line blocking on a slow peer. `0` or `1` preserves
  classic single-peer behavior; the value is clamped to cluster size.
  Trade-offs: per-cycle sync bandwidth multiplies by this factor
  (`pushPullScale` stretching unchanged), `maxPushStateBytes` applies per
  exchange, and raising it fleet-wide can make initiations fail against the
  receiver's `maxPushPullRequests=128` cap ("Too many pending push/pull
  requests").
* New event-queue observability: a `memberlist.event.pending` gauge
  (includes the in-flight event), a `memberlist.event.delivered` counter,
  and WARN logs when queue depth crosses 1,000 / 10,000 / 100,000
  (indicating a slow or blocked event consumer).

### Changes

* **BREAKING:** `EventDelegate` (`NotifyJoin`/`NotifyUpdate`/`NotifyLeave`)
  and `ConflictDelegate.NotifyConflict` are now delivered asynchronously in
  global commit order by a single dispatcher goroutine holding no locks,
  instead of synchronously under the internal node lock. Delivery is no
  longer synchronous with the mutating call (`Create`/`LeaveContext`/
  `UpdateNodeContext` excepted, which confirm delivery), the queue is
  unbounded so undelivered events accumulate in memory, and a panic in a
  callback is now fail-fast (unrecovered). Configured via `Config.Events` /
  `Config.Conflict`.
* **BREAKING:** `(*Memberlist).Leave` and `(*Memberlist).UpdateNode` now
  bound the entire call (including internal `nodeLock` acquisition) by the
  timeout instead of only the final broadcast wait; on expiry they return an
  error wrapping `context.DeadlineExceeded` rather than the old plain strings
  "timeout waiting for leave broadcast" / "timeout waiting for update
  broadcast". A timeout firing before the leave is committed now leaves no
  partial state — a timed-out `Leave` no longer guarantees the local node
  was marked left, and is safe to retry.
* **BREAKING:** `(*Memberlist).UpdateNode` and
  `(*Memberlist).UpdateNodeContext` now fail fast with `ErrLeft` when the
  node has already left the cluster, instead of blocking for the full
  timeout waiting for a self-alive broadcast that is never sent after leave.
* **BREAKING:** `Create` now fail-fasts: it validates that a worst-case
  alive message (name + IPv6 address + meta at the cap + gossip compound/CRC/
  label/encryption envelope, bounded by the 65535 `uint16` protocol ceiling)
  fits the transport packet budget, and on failure returns a config error
  and shuts the transport down instead of constructing a node that would
  silently drop oversized packets at runtime. Absurd `Config.MetaMaxSize`
  caps are rejected against the budget before any slice of that size is
  allocated. Degenerate setups with no packet budget (`UDPBufferSize<=0` and
  no advertised size) skip the check.
* **BREAKING:** `AliveDelegate.NotifyAlive` (`Config.Alive`) is no longer
  invoked for local-origin alive messages (the `setAlive` bootstrap path and
  `UpdateNodeContext`); it now runs only as a synchronous gate on remote
  alive claims.
* **BREAKING:** `(*Memberlist).LeaveContext` / `Leave` now also bound, within
  ctx/timeout, confirmation that the leave event was delivered to the
  `EventDelegate`; on ctx expiry after commit the leave is still committed
  and broadcast (the error only means delivery is unconfirmed), and an
  idempotent retry after the node has already left now blocks on the original
  leave event's delivery via a persisted receipt instead of returning nil
  immediately.
* **BREAKING:** `(*Memberlist).UpdateNodeContext` / `UpdateNode` now also
  bound, within ctx/timeout, confirmation that the resulting membership event
  was delivered to the `EventDelegate`; on ctx expiry after commit the update
  is still committed and broadcast (the error only means delivery is
  unconfirmed).
* **BREAKING:** `(*Memberlist).Shutdown` now seals and fully drains the event
  queue before returning — every committed membership event is delivered to
  the `EventDelegate` / `ConflictDelegate` first, so a blocked event consumer
  now delays `Shutdown`.
* `NetTransport` now implements `MaxPacketSizeTransport`, with
  `MaxPacketSize()` returning 65507 (the IPv4 UDP payload ceiling); this
  clamps the effective packet budget used for both `Create`-time validation
  and packet construction, so a `UDPBufferSize` set above 65507 no longer
  admits undeliverable datagrams.

### Fixed

* **BREAKING:** `(*Memberlist).Shutdown` now blocks until every
  instance-owned background goroutine (stream/packet listeners, packet
  handler, broadcast-queue checker, probe/gossip/push-pull schedule
  triggers, in-flight stream handlers, TCP-ping fallbacks, and
  indirect-ping nack timers) has exited; previously it returned early and
  leaked goroutines that kept contending the node lock and touching the
  transport. Worst-case `Shutdown` latency is now ~2×`TCPTimeout` when a
  stream peer stalls mid push/pull; when `Shutdown` is called re-entrantly
  from a delegate callback it still returns promptly with quiescence
  completing asynchronously.
* **BREAKING:** `(*Memberlist).Shutdown` now stops and clears all pending
  suspicion timers under the node lock and suppresses any suspicion callback
  that slipped past the stop, so a shut-down instance no longer marks nodes
  dead, mutates node state, or delivers a spurious
  `EventDelegate.NotifyLeave` after `Shutdown` returns.
* `(*Memberlist).Leave` no longer panics: calling it after `Shutdown`
  returns `ErrShutdown` instead of `panic("leave after shutdown")`, and when
  the local node is absent from the node map it returns `ErrNoLocalNode`
  instead of a nil-pointer dereference (it previously read
  `state.Incarnation` before the presence check).
* `(*Memberlist).UpdateNode` no longer panics or nil-derefs: it returns
  `ErrShutdown` after `Shutdown`, `ErrNoLocalNode` when the local node is
  missing from the node map, and `ErrMetaTooLarge` when the `Delegate`'s
  `NodeMeta` exceeds `MetaMaxSize` (previously an oversized meta panicked).
* `Create` now returns `ErrMetaTooLarge` instead of panicking when the
  configured `Delegate` returns `NodeMeta` larger than `MetaMaxSize` (the
  size check moved out of `setAlive`'s panic into a returned error).
* An unbounded (zero/negative-timeout) `Leave`/`UpdateNode` broadcast wait
  that can never complete is now released by `Shutdown`, returning an error
  wrapping `ErrShutdown`, instead of blocking forever.
* A blocking or re-entrant `EventDelegate` / `ConflictDelegate` callback no
  longer stalls all membership processing or deadlocks the instance
  (taba#373 / hashicorp#23 class): callbacks may now block and call back into
  `Memberlist` methods (`Members`, `UpdateNode`, `Leave`, `Shutdown`, ...),
  and an unconsumed `ChannelEventDelegate` channel no longer wedges
  membership processing.
* Outgoing packet budgets in gossip and single-message sends now derive from
  `UDPBufferSize` constrained by the transport's advertised `MaxPacketSize`
  (previously they used `UDPBufferSize` alone) and reserve the primary
  message's compound part-length prefix plus the protocol-v5 CRC header, so a
  delegate filling the offered budget no longer builds a compound packet that
  an exact-limit transport rejects/drops.
* `mergeRemoteState` now serializes protocol re-verification and the
  membership state merge under a dedicated internal lock, closing a
  pre-existing TOCTOU where two concurrent push/pull exchanges (parallel
  initiations or concurrent inbound handlers) whose remote states are
  individually compatible but mutually incompatible could both pass
  `verifyProtocol` before either merged, admitting a mixed-protocol
  membership that sequential processing would reject; a merge can now return
  a protocol error under concurrency where it previously would not.

## v0.6.0 (Mori)

First release as Mori, 0xCarbon's maintained hard fork of
hashicorp/memberlist.

### Changes

* Module path renamed to `github.com/0xCarbon/mori`, package `memberlist`
  renamed to `mori`. No API changes.
* Removed HashiCorp release/compliance tooling (`tag.sh`, copywrite,
  CODEOWNERS); default branch renamed to `main`.

### Fork point

Forked from hashicorp/memberlist `master` at `371698b` (post-v0.5.4),
which includes unreleased upstream fixes: keyring concurrent read/write
(hashicorp#342), nil pointer dereference (hashicorp#336), broadcast data race
(hashicorp#273), probe selection for very small clusters (hashicorp#350), and
remote state header limits (hashicorp#357).
