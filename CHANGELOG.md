## Unreleased

### Improvements

* `CONTRIBUTING.md` and an updated pull request template (adapted from
  upstream hashicorp/memberlist #375).

### Changes

### Fixed

* A compressed stream message is read against the largest valid compressed
  form of a `maxDecompressedBytes` message (LZW expands incompressible
  input by about 1.37x, 1.41x at worst in practice, 1.5x in theory), not
  the 40 MiB plaintext cap. Senders compress stream messages whether or not
  that shrinks them, so v0.8.0 refused legitimate unencrypted push/pull
  states from about 28 to 40 MiB with incompressible content. Plaintext
  stream messages keep the 40 MiB cap. Encrypted stream messages remain
  limited to 20 MiB of ciphertext, as upstream.
* Stream messages are sent compressed only when that makes them smaller,
  as packets already were. Incompressible push/pull states no longer grow
  by up to 1.4x on the wire (which, with encryption, could push a state
  that fits uncompressed over the 20 MiB ciphertext limit). Wire
  compatible: receivers accept both forms.
* Senders now refuse, with `ErrMessageTooLarge`, an encrypted stream message
  (push/pull state or `SendReliable`) whose ciphertext exceeds the 20 MiB
  receivers accept; before, the receiver dropped it with an error only it
  logged.

### Security

* Regression tests for issue #21: a stream whose compressed buffer (60 MiB)
  or node meta (1 MiB) declares a length within the field limits but
  carries 1 KiB is refused without allocating the declared size.

## v0.8.0 (Mori)

This release makes Mori a standard-library-only
module, ports the upstream security fixes released after the fork point and
closes further instances of the same classes, replaces go-msgpack with an
owned and fuzzed codec proven byte-compatible (a v0.7 and a v0.8 node can
share a cluster), moves logging to `log/slog` and metrics to a per-instance
sink, and fixes several inherited bugs. It contains **BREAKING** API changes
(logging, metrics, a removed config field) and requires **Go 1.27.1**;
consumers on older toolchains must raise their `go` directive to adopt it.

### Improvements

* Performance, compared with the code before this release (two interleaved
  A/B phases, chained where both apply; go1.27.1; method and results in
  pull request #22): handling an alive message −84%
  time and 50 → 10 allocations; merging a 1,000-node push/pull state −76%;
  handling a ping −96% overall (LZW coders are pooled and packets of at
  most 22 bytes, which LZW provably cannot shrink, skip compression);
  encrypted gossip reuses per-key AES-GCM ciphers (encrypted ping −57%);
  the UDP listener reuses one receive buffer instead of allocating 64 KiB
  per datagram (−37% time, −98% bytes per packet); the broadcast queue at
  10,000 nodes −64% time and 123 → 7 allocations.
* Mori owns its wire codec (`internal/msgpack`, `internal/wire`) instead of
  `github.com/hashicorp/go-msgpack/v2`. The encoding is byte-for-byte what
  go-msgpack's default handle produces, proven by a differential oracle
  (21.6 M randomized agreements across encoding, decoding and alternative
  encodings; `make oracle`) and 144 captured golden vectors. Decoding never panics, checks every length
  against the remaining input (packets) or grows buffers with the bytes
  received (streams), bounds field sizes (1 MiB; 64 MiB for compressed
  payloads) and container nesting, and allocates only for non-empty string
  and byte fields; encoding appends into one buffer without reflection.
* The broadcast queue (`TransmitLimitedQueue`) is an intrusive treap: the
  same ordering and results as before, with no allocation when an entry
  moves to its next transmit tier. It no longer depends on
  `github.com/google/btree`.
* Private advertise-address discovery (bind to `0.0.0.0`) uses `net/netip`
  and the RFC 6890 special-purpose table directly; the default-route
  interface is found with a UDP "connect" probe, so no routing-table parsing
  or `github.com/hashicorp/go-sockaddr` is needed.

### Changes

* **BREAKING:** requires Go 1.27.1 (`go.mod`), matching the 0xCarbon
  ecosystem toolchain; the module has no `require` lines at all.
* **BREAKING:** logging uses `log/slog`. `Config.Logger` is a
  `*slog.Logger` (nil means `slog.Default()`); `Config.LogOutput` is
  removed, as are the `LogAddress`, `LogConn` and `LogStringAddress`
  helpers. `NetTransportConfig.Logger` is a `*slog.Logger` too (nil means
  `slog.Default()`). Records are structured (`error`, `from`, `node`,
  `addr`, ...) and protocol chatter is at Debug, so the default handler no
  longer prints it. To silence Mori, set
  `Config.Logger = slog.New(slog.DiscardHandler)` (was
  `Config.LogOutput = io.Discard`).
* **BREAKING:** metrics go to a per-instance `Config.Metrics` sink
  (interface `MetricSink`) instead of the process-global
  `github.com/hashicorp/go-metrics` registry; nil disables them.
  `Config.MetricLabels` and `NetTransportConfig.MetricLabels` are
  `[]mori.Label`, and `NetTransportConfig.Metrics` feeds the transport's
  counter. Metric names and kinds are unchanged (listed on `MetricSink`).
  To keep sending to go-metrics' global sink, adapt it:

  ```go
  type goMetrics struct{}

  func labels(ls []mori.Label) []metrics.Label {
  	out := make([]metrics.Label, len(ls))
  	for i, l := range ls {
  		out[i] = metrics.Label{Name: l.Name, Value: l.Value}
  	}
  	return out
  }
  func (goMetrics) IncrCounter(k []string, v float32, l []mori.Label) { metrics.IncrCounterWithLabels(k, v, labels(l)) }
  func (goMetrics) SetGauge(k []string, v float32, l []mori.Label)    { metrics.SetGaugeWithLabels(k, v, labels(l)) }
  func (goMetrics) AddSample(k []string, v float32, l []mori.Label)   { metrics.AddSampleWithLabels(k, v, labels(l)) }
  func (goMetrics) MeasureSince(k []string, t time.Time, l []mori.Label) {
  	metrics.MeasureSinceWithLabels(k, t, labels(l))
  }
  ```
* **BREAKING:** `SendReliable` returns the new sentinel `ErrMessageTooLarge`
  for messages above 20 MiB, which receivers refuse, instead of reporting
  success for a message that is never delivered. A node whose own push/pull
  state exceeds the 40 MiB stream limit reports that error when sending it.
* **BREAKING:** `Config.MsgpackUseNewTimeFormat` is removed. No protocol
  message carries a time value, so it never changed the encoding.
* The TCP-first DNS lookup in `Join` queries A and AAAA records through
  `net.Resolver` over TCP instead of sending an ANY query with
  `github.com/miekg/dns`. Resolvers following RFC 8482 answer ANY with a
  single synthesized HINFO record, which made the TCP-first lookup silently
  return nothing and fall back to the system resolver.
* `ParseCIDRs` and `Join` combine multiple errors with `errors.Join` (one
  line per error) instead of `go-multierror`; `Join` wraps each cause with
  `%w`.
* The public-address warning and advertise-address selection treat only
  RFC 6890's `2001::/23` and `2001:db8::/32` as special-purpose within
  `2001::/16`; go-sockaddr's table covered the whole `/16`, including
  global unicast such as `2001:4860::/32`.
* Randomness uses `math/rand/v2` (`github.com/sean-/seed` is gone; the
  global generator has been seeded automatically since Go 1.20).

### Fixed

* `Node.State` now reports the node's actual state everywhere a `Node` is
  handed out (`Members`, `LocalNode`, `EventDelegate`/`ConflictDelegate`
  callbacks). Internal state shadowed the field, so every such `Node`
  reported `StateAlive`, even in `NotifyLeave`. Inherited from upstream.
* A broadcast could be silently dropped without `Finished` being called:
  `GetBroadcasts` briefly empties the queue while sent entries await
  re-insertion, which reset the id generator, so a later broadcast could
  reuse a pending entry's id and replace it in the ordered set. Ids now
  never rewind (except in `Reset`). Inherited from upstream.
* The `memberlist.size.local` gauge reports the size of the local push/pull
  message. It used to decode bytes 1–4 of the MessagePack encoding as a
  big-endian length (111 bytes sent were reported as 2,208,582,100).
  Inherited from upstream.
* A data race between sending a packet and applying an alive message:
  `rawSendMsgPacket` read the destination's protocol version after
  releasing the node lock. Inherited from upstream.
* An alive message carrying a node's IPv4 address in 16-byte form (as
  advertised by nodes bound to `0.0.0.0`) no longer counts as an address
  change against the 4-byte form, which reported a spurious conflict and
  refused the update.

* `(*Memberlist).Members` and `(*Memberlist).LocalNode` now return snapshot
  copies of `Node` taken under the node lock, instead of pointers into
  live Memberlist-internal state. Callers reading returned fields (`Meta`,
  `Addr`, ...) no longer data race with concurrent alive/update handling;
  slice-typed fields alias buffers that are replaced wholesale, never
  mutated in place, so the copies are safe to read lock-free.

### Security

* `SECURITY.md` documents the threat model (adapted from upstream) and every
  input bound Mori enforces. Fuzz targets cover the whole packet path
  (plaintext and encrypted, through the live state machine), the stream
  path including push/pull merge, the message decoders and the MessagePack
  decoder; `make fuzz-smoke` runs them in CI (76.4M executions without a
  failure while preparing this release).

Ports the four upstream hashicorp/memberlist fixes released after the fork
point, and closes further instances of the same classes found while
porting them. Every item is reachable by an unauthenticated peer unless
noted; each has a regression test that failed (wrong error, allocation or
panic) before the fix.

* Stream user messages whose header declares a negative length or more
  than 20 MiB are refused before any buffer is sized (upstream #361).
* Decompression is bounded: stream payloads may expand to at most 40 MiB
  (upstream #363) and packet payloads to at most 1 MiB, since senders only
  compress packets built within the packet budget.
* A compressed or encrypted stream whose inner payload is empty is an error
  instead of an index-out-of-range panic (upstream #369; the decrypted form
  requires a key holder).
* Protocol version vectors (`Vsn`) with 1–5 entries are refused on the
  alive, push/pull merge and protocol-verification paths instead of
  panicking (upstream #368). Empty vectors keep their legacy meaning (all
  versions zero); entries past the sixth are ignored.
* A push/pull header no longer preallocates storage for every declared node:
  a 10-byte header declaring 1,048,576 nodes used to allocate ~117 MB per
  connection before a single node was read. Both header limits are now
  checked before decoding, and storage grows with the states received.
* Unencrypted, uncompressed stream messages are capped at 40 MiB of
  plaintext; before, variable-length fields (for example node meta) made a
  single push/pull unbounded.
* A compressed message nested in a compressed message, or a compound
  message nested in a compound message, is refused. Senders never produce
  either; accepting them multiplied decompression work and recursion depth
  per packet.
* Push/pull node states must carry a node name; a nameless state fails the
  exchange. Before, each 1-byte nil in a push/pull stream decoded to a full
  node state (over 100 bytes of memory per input byte).
* The MessagePack decoder refuses map counts above 2^31-1; on 32-bit
  platforms such a count wrapped negative and decoded as an empty map.
* Version-0 (PKCS7-padded) encrypted payloads with an invalid pad length
  return an error instead of a slice-bounds panic (requires a key holder).

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
