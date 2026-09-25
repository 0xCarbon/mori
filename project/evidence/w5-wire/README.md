# W5 — owned wire codec: go-msgpack oracle

`internal/wire` (messages) and `internal/msgpack` (format) replace
`github.com/hashicorp/go-msgpack/v2`. This nested module is the independent
oracle: it imports the real go-msgpack and the new codec, and is excluded
from the main module's build and dependency graph.

## What it proves

For 12 message types, boundary-biased random values (integers at every
format edge, strings/bytes of 0/1/15/16/31/32/33/255/256/65535/65536 bytes,
nil vs empty slices, arbitrary non-UTF-8 names):

1. **Encoding**: `wire` bytes == go-msgpack `MsgpackHandle{}` bytes.
2. **Decoding**: both codecs decode go-msgpack's bytes to equal values.
3. **Tolerance**: for alternative encodings of the same message (other
   integer widths, str8/bin families, shuffled keys, unknown keys with nested
   values, duplicate keys, nil for zero values), both codecs agree — equal
   values, or both refuse.

It also writes `internal/wire/testdata/golden.json` (144 vectors, 12 per
type, JSON-lossless values only), which Mori's own tests use after the
dependency is gone.

## Finding

The first run failed 160,000 of 240,000 encodings: go-msgpack writes struct
fields **sorted by name**, not in declaration order (`Node` before `SeqNo`).
The draft codec assumed declaration order; it was fixed before integration.
Key order does not affect decoding or message size, so peers would still
have interoperated, but the byte-for-byte contract (and every golden vector)
would have been false.

## Receipt

`differential.txt`: three seeds × 200,000 messages per type × 3 checks
(21.6 M agreements), go1.27.1 linux/amd64, go-msgpack v2.1.5.

Reproduce: `cd project/evidence/w5-wire && go run . -n 200000 -seed 11`.
