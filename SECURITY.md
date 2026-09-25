# Mori security model

This document describes the security model of Mori: the guarantees it
provides, the configuration required to obtain them, the input bounds it
enforces, and what is explicitly **outside** its threat model. It is
adapted from hashicorp/memberlist's `SECURITY.md` (MPL-2.0), from which Mori
is forked, and extended with Mori's own hardening.

Mori is a library, not a product. It is embedded by higher-level systems such
as [taba](https://github.com/0xCarbon/taba). Some responsibilities described
here (identity, authorization, key distribution, encryption at rest) are
intentionally delegated to the embedding application.

> [!IMPORTANT]
> Mori is **not secure by default**. Without a gossip encryption key
> configured, all membership traffic is transmitted in plaintext and is
> neither encrypted nor authenticated. In that mode, any host that can reach
> the gossip port can read membership information and forge membership
> messages.

## Overview

Mori manages cluster membership and failure detection with a gossip protocol
based on [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
and Lifeguard. Nodes exchange:

- **UDP packets** — probes, acks, nacks and gossiped state changes (`alive`,
  `suspect`, `dead`, user messages), optionally compressed and batched in
  compound messages.
- **TCP streams** — full-state push/pull synchronization, stream user
  messages and TCP fallback pings.

The only security primitive Mori provides is symmetric encryption and
authentication of this traffic with a shared keyring (AES-GCM). It provides
no per-node identity, authentication or authorization.

### Trust model

The trust boundary of a Mori cluster is **possession of a current gossip
key**.

- Every node shares the same symmetric key material (the keyring).
- Any party holding a current key is a fully trusted member. There is no
  cryptographic notion of an individual node's identity.
- Mori is a weakly consistent membership protocol: **not Byzantine fault
  tolerant** and **not a consensus protocol**. One authenticated but
  malicious peer can add or remove nodes (broadcasting `dead`/`suspect`),
  reclaim or impersonate a node's name (with a higher incarnation) and alter
  node metadata.

## Secure configuration

### Requirements

- **Enable gossip encryption.** Set `Config.SecretKey` (or a `Config.Keyring`)
  to a 16-, 24- or 32-byte key (AES-128/192/256). With a key present, Mori
  encrypts and authenticates all UDP gossip and TCP push/pull traffic.
- **Keep verification enforced.** `Config.GossipVerifyIncoming` and
  `Config.GossipVerifyOutgoing` default to `true` and should stay so. They
  exist only to migrate a running cluster from plaintext to encrypted gossip
  (see `TestMemberlist_EncryptedGossipTransition` for the three stages).
- **Protect the key** as a cluster-wide secret, distributed out of band.
- **Rotate keys** with `Keyring.AddKey`, `UseKey` and `RemoveKey`; secondary
  keys stay valid for decryption during a rollout.

### Recommendations

- **Restrict network exposure.** `DefaultLANConfig`/`DefaultWANConfig` bind
  `0.0.0.0:7946` (TCP and UDP). Firewall it to cluster members. Mori logs a
  warning when it advertises a public address without encryption.
- **Use `Config.CIDRsAllowed`** to restrict sources and member addresses
  (defense in depth, not a substitute for encryption).
- **Set `Config.Label`.** With encryption, the label is GCM additional
  authenticated data, so a node rejects messages meant for another logical
  cluster. It is not a secret and protects nothing without encryption.
- **Use `Config.RequireNodeNames`** where appropriate.
- **Validate at the application layer** with the `Alive`, `Conflict` and
  `Merge` delegates.
- **Layer identity and authorization above Mori** (for example mTLS).

## Threat model

With a key set and verification enforced, Mori defends against:

- **Eavesdropping** on gossip in transit (AES-GCM).
- **Tampering** with messages in transit (GCM authentication tags).
- **Forged or injected messages** from parties without a key.
- **Cross-cluster confusion** when a `Label` is configured.
- **Malformed input.** Every decoder refuses malformed input with an error;
  none may panic or allocate beyond the bounds below. This holds **with or
  without encryption**: it is the only protection a plaintext cluster has
  against crafted packets, and the parsers run before authentication only
  where the protocol requires (labels, the encryption envelope).
- **Resource exhaustion by declared sizes.** A length or count read from the
  network never sizes an allocation before it is checked against a bound,
  and stream buffers grow with the bytes actually received:

| Input | Bound |
| --- | --- |
| Encrypted stream message | 20 MiB ciphertext |
| Plaintext stream message (headers, node states, user state) | 40 MiB |
| Compressed stream message (as read) | about 60 MiB (the largest valid LZW form of a 40 MiB payload) |
| Decompressed stream payload | 40 MiB |
| Decompressed packet payload | 1 MiB |
| Push/pull node count | 1,048,576, storage grown per received state |
| Push/pull user state, stream user message | 20 MiB |
| String or byte field in a message | 1 MiB (compressed buffer: 64 MiB) |
| MessagePack nesting in skipped fields | 32 levels |
| Envelope nesting | one compression and one compound level per packet |
| Protocol version vector | 0 or at least 6 entries; 1–5 is refused |
| Concurrent inbound push/pulls | 128 |
| Packet handoff queue | `Config.HandoffQueueDepth` (1024) |

The fuzz targets `FuzzIngestPacket` (the whole packet path, plaintext and
encrypted, through the live state machine), `FuzzReadStream` (stream path,
state decoding and merge), `FuzzDecodeMessages` and `FuzzDecoder` pin these
properties; `make fuzz-smoke` runs them in CI. The CHANGELOG records each
fix of this class, most with the upstream issue it corresponds to.

## Not in the threat model

- **Malicious authenticated peers / key compromise.** A key holder is fully
  trusted and can disrupt the cluster. Mori is not Byzantine fault tolerant.
- **Per-node authentication and authorization.**
- **Key distribution and exchange.**
- **Encryption at rest** of keys or state.
- **Host, process and memory access** (an attacker who can read process
  memory recovers the keys).
- **Metadata confidentiality and traffic analysis**: message sizes, timing,
  framing and the plaintext outer `Label` header are observable.
- **Replay protection as a designed control.** There is no nonce or replay
  cache; incarnation numbers give only incidental resistance.
- **Denial of service beyond the bounds above**: volumetric floods,
  amplification, OS and network exhaustion. A plaintext cluster reachable by
  an attacker can be disrupted at will through well-formed messages.
- **Plaintext operation** (no key, or `GossipVerifyIncoming = false`): no
  guarantee above except the malformed-input and declared-size bounds.
- **Custom transports**: their security is their author's responsibility.

## Network ports

| Port | Protocol | Purpose |
| ---- | -------- | ------- |
| 7946 (default) | TCP | Push/pull synchronization, stream user messages, TCP fallback pings |
| 7946 (default) | UDP | Probes, acks, nacks and gossiped membership changes |

Bind and advertise addresses are set by `Config.BindAddr`/`BindPort` and
`Config.AdvertiseAddr`/`AdvertisePort`.

## Reporting a vulnerability

Do not report security vulnerabilities through public GitHub issues. Use
GitHub's private vulnerability reporting on
[github.com/0xCarbon/mori](https://github.com/0xCarbon/mori/security) so the
maintainers can coordinate a fix and disclosure.
