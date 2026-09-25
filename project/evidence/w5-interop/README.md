# W5 — mixed-version interop: Mori v0.7.0 ↔ owned codec

The strongest compatibility claim for the owned wire codec is a live
cluster: nodes built from Mori v0.7.0 (go-msgpack v2.1.5, the codec
memberlist peers use) and from this tree gossip together.

`run.sh` builds one node program twice — `go build -modfile=old.mod -tags
moriold` against `github.com/0xCarbon/mori v0.7.0` from the module proxy,
and `go build` against this tree — then runs four-node loopback clusters in
four compositions (all-old and all-new controls, mixed, mixed reversed) and
three transports each: plain, compressed, and compressed + AES-GCM
encryption + label. Nodes join in a chain across versions, two of them
update their meta mid-run (alive broadcast with a new incarnation), and
every node sends a reliable (TCP) and a best-effort (UDP) user message to
every other node and queues a gossip broadcast.

A scenario passes when every node sees all four members with their final
meta and received every message addressed to it.

## Receipt

`interop.txt`: two consecutive full runs, 12/12 scenarios PASS each,
go1.27.1 linux/amd64, new tree at 9f2e968.

The first harness draft failed for harness reasons (a node leaving before
its peers reported; user broadcasts handed to a single packet; ports inside
the ephemeral range). The all-old control group exposed each of them, which
is why the controls stay in the script.
