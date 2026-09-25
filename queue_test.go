// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// for testing only
func (q *TransmitLimitedQueue) orderedView() []*limitedBroadcast {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.lenLocked())
	q.walkReadOnlyLocked(true, func(cur *limitedBroadcast) bool {
		out = append(out, cur)
		return true
	})

	return out
}

func TestLimitedBroadcastLess(t *testing.T) {
	cases := []struct {
		Name string
		A    *limitedBroadcast // lesser
		B    *limitedBroadcast
	}{
		{
			"diff-transmits",
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
			&limitedBroadcast{transmits: 1, msgLen: 10, id: 100},
		},
		{
			"same-transmits--diff-len",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
		},
		{
			"same-transmits--same-len--diff-id",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 90},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			a, b := c.A, c.B
			if !a.less(b) || b.less(a) {
				t.Fatalf("less(%+v, %+v) is not a strict order", a, b)
			}

			var root *limitedBroadcast
			b.prio, a.prio = 2, 1
			root = treapInsert(root, b)
			root = treapInsert(root, a)
			if got := treapMin(root); got != a {
				t.Fatalf("min = %+v, want %+v", got, a)
			}
			if got := treapMax(root); got != b {
				t.Fatalf("max = %+v, want %+v", got, b)
			}
		})
	}
}

// TestTreapMatchesSortedOracle drives the intrusive treap with random
// inserts and deletes and checks traversal, bounds and seeks against a
// sorted slice after every operation.
func TestTreapMatchesSortedOracle(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var root *limitedBroadcast
	var oracle []*limitedBroadcast
	nextID := int64(0)
	for step := range 20_000 {
		if len(oracle) == 0 || r.IntN(3) != 0 {
			nextID++
			x := &limitedBroadcast{transmits: r.IntN(4), msgLen: int64(r.IntN(8)), id: nextID, prio: r.Uint64()}
			root = treapInsert(root, x)
			i, _ := slices.BinarySearchFunc(oracle, x, cmpBroadcast)
			oracle = slices.Insert(oracle, i, x)
		} else {
			i := r.IntN(len(oracle))
			root = treapDelete(root, oracle[i])
			oracle = slices.Delete(oracle, i, i+1)
		}

		if step%97 != 0 {
			continue
		}
		var got []*limitedBroadcast
		ascendFrom(root, nil, func(x *limitedBroadcast) bool { got = append(got, x); return true })
		if !slices.Equal(got, oracle) {
			t.Fatalf("step %d: ascending traversal differs from the oracle", step)
		}
		got = got[:0]
		descend(root, func(x *limitedBroadcast) bool { got = append(got, x); return true })
		slices.Reverse(got)
		if !slices.Equal(got, oracle) {
			t.Fatalf("step %d: descending traversal differs from the oracle", step)
		}
		if len(oracle) > 0 && (treapMin(root) != oracle[0] || treapMax(root) != oracle[len(oracle)-1]) {
			t.Fatalf("step %d: min/max differ from the oracle", step)
		}
		k := &limitedBroadcast{transmits: r.IntN(5), msgLen: int64(r.IntN(9)), id: r.Int64N(nextID + 2)}
		i, _ := slices.BinarySearchFunc(oracle, k, cmpBroadcast)
		got = got[:0]
		ascendFrom(root, k, func(x *limitedBroadcast) bool { got = append(got, x); return true })
		if !slices.Equal(got, oracle[i:]) {
			t.Fatalf("step %d: ascendFrom(%+v) differs from the oracle", step, k)
		}
	}
}

func cmpBroadcast(a, b *limitedBroadcast) int {
	switch {
	case a.less(b):
		return -1
	case b.less(a):
		return 1
	}
	return 0
}

func TestTransmitLimited_Queue(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 1 }}
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump := q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
	if dump[1].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[2].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}

	// Should invalidate previous message
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump = q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[1].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[2].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
}

func TestTransmitLimited_GetBroadcasts(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 10 }}

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 2 byte overhead per message, should get all 4 messages
	all := q.GetBroadcasts(2, 80)
	require.Equal(t, 4, len(all), "missing messages: %v", prettyPrintMessages(all))

	// 3 byte overhead, should only get 3 messages back
	partial := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial), "missing messages: %v", prettyPrintMessages(partial))
}

func TestTransmitLimited_GetBroadcasts_Limit(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	require.Equal(t, int64(0), q.idGen, "the id generator seed starts at zero")
	require.Equal(t, 2, retransmitLimit(q.RetransmitMult, q.NumNodes()), "sanity check transmit limits")

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	require.Equal(t, int64(4), q.idGen, "we handed out 4 IDs")

	// 3 byte overhead, should only get 3 messages back
	partial1 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial1), "missing messages: %v", prettyPrintMessages(partial1))

	require.Equal(t, int64(4), q.idGen, "id generator never rewinds")

	partial2 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial2), "missing messages: %v", prettyPrintMessages(partial2))

	require.Equal(t, int64(4), q.idGen, "id generator never rewinds")

	// Only two not expired
	partial3 := q.GetBroadcasts(3, 80)
	require.Equal(t, 2, len(partial3), "missing messages: %v", prettyPrintMessages(partial3))

	require.Equal(t, int64(4), q.idGen, "id generator never rewinds, even when the queue empties")

	// Should get nothing
	partial5 := q.GetBroadcasts(3, 80)
	require.Equal(t, 0, len(partial5), "missing messages: %v", prettyPrintMessages(partial5))

	require.Equal(t, int64(4), q.idGen, "id generator never rewinds, even when the queue empties")
}

func prettyPrintMessages(msgs [][]byte) []string {
	var out []string
	for _, msg := range msgs {
		out = append(out, "'"+string(msg)+"'")
	}
	return out
}

func TestTransmitLimited_Prune(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), ch1})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), ch2})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// Keep only 2
	q.Prune(2)

	require.Equal(t, 2, q.NumQueued())

	// Should notify the first two
	select {
	case <-ch1:
	default:
		t.Fatalf("expected invalidation")
	}
	select {
	case <-ch2:
	default:
		t.Fatalf("expected invalidation")
	}

	dump := q.orderedView()

	if dump[0].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[1].b.(*memberlistBroadcast).node != "baz" {
		t.Fatalf("missing baz")
	}
}

func TestTransmitLimited_ordering(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	insert := func(name string, transmits int) {
		q.queueBroadcast(&memberlistBroadcast{name, []byte(name), make(chan struct{})}, transmits)
	}

	insert("node0", 0)
	insert("node1", 10)
	insert("node2", 3)
	insert("node3", 4)
	insert("node4", 7)

	dump := q.orderedView()

	if dump[0].transmits != 10 {
		t.Fatalf("bad val %v, %d", dump[0].b.(*memberlistBroadcast).node, dump[0].transmits)
	}
	if dump[1].transmits != 7 {
		t.Fatalf("bad val %v, %d", dump[7].b.(*memberlistBroadcast).node, dump[7].transmits)
	}
	if dump[2].transmits != 4 {
		t.Fatalf("bad val %v, %d", dump[2].b.(*memberlistBroadcast).node, dump[2].transmits)
	}
	if dump[3].transmits != 3 {
		t.Fatalf("bad val %v, %d", dump[3].b.(*memberlistBroadcast).node, dump[3].transmits)
	}
	if dump[4].transmits != 0 {
		t.Fatalf("bad val %v, %d", dump[4].b.(*memberlistBroadcast).node, dump[4].transmits)
	}
}

// TestQueueIDsStayUniqueAcrossDrain: GetBroadcasts removes every entry it
// sends and re-queues it afterwards, so the queue can be momentarily empty
// while entries are still pending re-insertion. Resetting the id generator
// at that moment let a new broadcast reuse a pending entry's id; with equal
// keys the ordered set replaced the older entry, dropping it without
// Finished ever being called.
func TestQueueIDsStayUniqueAcrossDrain(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 10, NumNodes: func() int { return 10 }}
	aDone := make(chan struct{}, 1)
	q.QueueBroadcast(&memberlistBroadcast{"a", []byte("12345"), aDone})
	if got := q.GetBroadcasts(0, 100); len(got) != 1 {
		t.Fatalf("first drain sent %d messages, want 1", len(got))
	}
	q.QueueBroadcast(&memberlistBroadcast{"b", []byte("67890"), nil})
	if got := q.GetBroadcasts(0, 5); len(got) != 1 {
		t.Fatalf("second drain sent %d messages, want 1", len(got))
	}
	if n := q.NumQueued(); n != 2 {
		select {
		case <-aDone:
			t.Fatalf("queue holds %d entries, want 2: broadcast a was dropped and finished early", n)
		default:
			t.Fatalf("queue holds %d entries, want 2: broadcast a was dropped without Finished", n)
		}
	}
}
