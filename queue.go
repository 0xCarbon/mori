// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"math"
	"math/rand/v2"
	"sync"
)

// TransmitLimitedQueue is used to queue messages to broadcast to
// the cluster (via gossip) but limits the number of transmits per
// message. It also prioritizes messages with lower transmit counts
// (hence newer messages).
type TransmitLimitedQueue struct {
	// NumNodes returns the number of nodes in the cluster. This is
	// used to determine the retransmit count, which is calculated
	// based on the log of this.
	NumNodes func() int

	// RetransmitMult is the multiplier used to determine the maximum
	// number of retransmissions attempted.
	RetransmitMult int

	mu    sync.Mutex
	root  *limitedBroadcast // treap ordered by less, heap-ordered by prio
	n     int
	tm    map[string]*limitedBroadcast
	idGen int64
}

// limitedBroadcast is a queued broadcast and, intrusively, its node in the
// queue's treap: re-queuing an entry at the next transmit tier relinks it
// without allocating.
type limitedBroadcast struct {
	transmits int   // key[0]: Number of transmissions attempted.
	msgLen    int64 // key[1]: copied from len(b.Message())
	id        int64 // key[2]: unique incrementing id stamped at submission time
	b         Broadcast

	name string // set if Broadcast is a NamedBroadcast

	left, right *limitedBroadcast
	prio        uint64
}

// less tests whether b orders before o. It is a strict total order over
// queued entries (ids are unique), so the treap never holds equal keys.
//
// The ordering is
// - [transmits=0, ..., transmits=inf]
// - [transmits=0:len=999, ..., transmits=0:len=2, ...]
// - [transmits=0:len=999,id=999, ..., transmits=0:len=999:id=1, ...]
func (b *limitedBroadcast) less(o *limitedBroadcast) bool {
	if b.transmits != o.transmits {
		return b.transmits < o.transmits
	}
	if b.msgLen != o.msgLen {
		return b.msgLen > o.msgLen
	}
	return b.id > o.id
}

// treapInsert links x into the treap rooted at t and returns the new root.
// x must not be linked already.
func treapInsert(t, x *limitedBroadcast) *limitedBroadcast {
	if t == nil {
		x.left, x.right = nil, nil
		return x
	}
	if x.prio > t.prio {
		x.left, x.right = treapSplit(t, x)
		return x
	}
	if x.less(t) {
		t.left = treapInsert(t.left, x)
	} else {
		t.right = treapInsert(t.right, x)
	}
	return t
}

// treapSplit partitions t into the entries ordering before k and the rest.
func treapSplit(t, k *limitedBroadcast) (lo, hi *limitedBroadcast) {
	if t == nil {
		return nil, nil
	}
	if t.less(k) {
		t.right, hi = treapSplit(t.right, k)
		return t, hi
	}
	lo, t.left = treapSplit(t.left, k)
	return lo, t
}

// treapMerge joins two treaps where every entry of lo orders before hi.
func treapMerge(lo, hi *limitedBroadcast) *limitedBroadcast {
	switch {
	case lo == nil:
		return hi
	case hi == nil:
		return lo
	case lo.prio > hi.prio:
		lo.right = treapMerge(lo.right, hi)
		return lo
	default:
		hi.left = treapMerge(lo, hi.left)
		return hi
	}
}

// treapDelete unlinks x (which must be linked in t) and returns the new root.
func treapDelete(t, x *limitedBroadcast) *limitedBroadcast {
	if t == x {
		r := treapMerge(t.left, t.right)
		x.left, x.right = nil, nil
		return r
	}
	if x.less(t) {
		t.left = treapDelete(t.left, x)
	} else {
		t.right = treapDelete(t.right, x)
	}
	return t
}

// ascendFrom calls f for each entry not ordering before k, in ascending
// order, until f returns false. A nil k visits every entry.
func ascendFrom(t, k *limitedBroadcast, f func(*limitedBroadcast) bool) {
	var stack []*limitedBroadcast
	for t != nil || len(stack) > 0 {
		for t != nil {
			if k != nil && t.less(k) {
				t = t.right // t and its left subtree order before k
				continue
			}
			stack = append(stack, t)
			t = t.left
		}
		if len(stack) == 0 {
			return // every remaining entry orders before k
		}
		t = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !f(t) {
			return
		}
		t = t.right
		k = nil // everything after an included entry is included
	}
}

// descend calls f for each entry in descending order until f returns false.
func descend(t *limitedBroadcast, f func(*limitedBroadcast) bool) {
	var stack []*limitedBroadcast
	for t != nil || len(stack) > 0 {
		for t != nil {
			stack = append(stack, t)
			t = t.right
		}
		t = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !f(t) {
			return
		}
		t = t.left
	}
}

func treapMin(t *limitedBroadcast) *limitedBroadcast {
	for t != nil && t.left != nil {
		t = t.left
	}
	return t
}

func treapMax(t *limitedBroadcast) *limitedBroadcast {
	for t != nil && t.right != nil {
		t = t.right
	}
	return t
}

// walkReadOnlyLocked calls f for each item in the queue traversing it in
// natural order (by less) when reverse=false and the opposite when true. You
// must hold the mutex.
//
// This method panics if you attempt to mutate the item during traversal.
func (q *TransmitLimitedQueue) walkReadOnlyLocked(reverse bool, f func(*limitedBroadcast) bool) {
	iter := func(cur *limitedBroadcast) bool {
		prevTransmits := cur.transmits
		prevMsgLen := cur.msgLen
		prevID := cur.id

		keepGoing := f(cur)

		if prevTransmits != cur.transmits || prevMsgLen != cur.msgLen || prevID != cur.id {
			panic("edited queue while walking read only")
		}
		return keepGoing
	}

	if reverse {
		descend(q.root, iter) // end with transmit 0
	} else {
		ascendFrom(q.root, nil, iter) // start with transmit 0
	}
}

// Broadcast is something that can be broadcasted via gossip to
// the memberlist cluster.
type Broadcast interface {
	// Invalidates checks if enqueuing the current broadcast
	// invalidates a previous broadcast
	Invalidates(b Broadcast) bool

	// Returns a byte form of the message
	Message() []byte

	// Finished is invoked when the message will no longer
	// be broadcast, either due to invalidation or to the
	// transmit limit being reached
	Finished()
}

// NamedBroadcast is an optional extension of the Broadcast interface that
// gives each message a unique string name, and that is used to optimize
//
// You shoud ensure that Invalidates() checks the same uniqueness as the
// example below:
//
//	func (b *foo) Invalidates(other Broadcast) bool {
//		nb, ok := other.(NamedBroadcast)
//		if !ok {
//			return false
//		}
//		return b.Name() == nb.Name()
//	}
//
// Invalidates() isn't currently used for NamedBroadcasts, but that may change
// in the future.
type NamedBroadcast interface {
	Broadcast
	// The unique identity of this broadcast message.
	Name() string
}

// UniqueBroadcast is an optional interface that indicates that each message is
// intrinsically unique and there is no need to scan the broadcast queue for
// duplicates.
//
// You should ensure that Invalidates() always returns false if implementing
// this interface. Invalidates() isn't currently used for UniqueBroadcasts, but
// that may change in the future.
type UniqueBroadcast interface {
	Broadcast
	// UniqueBroadcast is just a marker method for this interface.
	UniqueBroadcast()
}

// QueueBroadcast is used to enqueue a broadcast
func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.queueBroadcast(b, 0)
}

// lazyInit initializes internal data structures the first time they are
// needed.  You must already hold the mutex.
func (q *TransmitLimitedQueue) lazyInit() {
	if q.tm == nil {
		q.tm = make(map[string]*limitedBroadcast)
	}
}

// queueBroadcast is like QueueBroadcast but you can use a nonzero value for
// the initial transmit tier assigned to the message. This is meant to be used
// for unit testing.
func (q *TransmitLimitedQueue) queueBroadcast(b Broadcast, initialTransmits int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.lazyInit()

	// Ids are unique for the queue's lifetime (only Reset rewinds them):
	// the ordering relies on them to break ties, and an entry pending
	// re-insertion inside GetBroadcasts still holds its id while the queue
	// looks empty. 2^63 submissions cannot be exhausted.
	q.idGen++
	id := q.idGen

	lb := &limitedBroadcast{
		transmits: initialTransmits,
		msgLen:    int64(len(b.Message())),
		id:        id,
		b:         b,
	}
	unique := false
	if nb, ok := b.(NamedBroadcast); ok {
		lb.name = nb.Name()
	} else if _, ok := b.(UniqueBroadcast); ok {
		unique = true
	}

	// Check if this message invalidates another.
	if lb.name != "" {
		if old, ok := q.tm[lb.name]; ok {
			old.b.Finished()
			q.deleteItem(old)
		}
	} else if !unique {
		// Slow path, hopefully nothing hot hits this.
		var remove []*limitedBroadcast
		ascendFrom(q.root, nil, func(cur *limitedBroadcast) bool {
			// Special Broadcasts can only invalidate each other.
			switch cur.b.(type) {
			case NamedBroadcast:
				// noop
			case UniqueBroadcast:
				// noop
			default:
				if b.Invalidates(cur.b) {
					cur.b.Finished()
					remove = append(remove, cur)
				}
			}
			return true
		})
		for _, cur := range remove {
			q.deleteItem(cur)
		}
	}

	// Append to the relevant queue.
	q.addItem(lb)
}

// deleteItem removes the given item from the overall datastructure. You
// must already hold the mutex.
func (q *TransmitLimitedQueue) deleteItem(cur *limitedBroadcast) {
	q.root = treapDelete(q.root, cur)
	q.n--
	if cur.name != "" {
		delete(q.tm, cur.name)
	}
}

// addItem adds the given item into the overall datastructure. You must already
// hold the mutex.
func (q *TransmitLimitedQueue) addItem(cur *limitedBroadcast) {
	cur.prio = rand.Uint64()
	q.root = treapInsert(q.root, cur)
	q.n++
	if cur.name != "" {
		q.tm[cur.name] = cur
	}
}

// getTransmitRange returns a pair of min/max values for transmit values
// represented by the current queue contents. Both values represent actual
// transmit values on the interval [0, len). You must already hold the mutex.
func (q *TransmitLimitedQueue) getTransmitRange() (minTransmit, maxTransmit int) {
	if q.lenLocked() == 0 {
		return 0, 0
	}
	return treapMin(q.root).transmits, treapMax(q.root).transmits
}

// GetBroadcasts is used to get a number of broadcasts, up to a byte limit
// and applying a per-message overhead as provided.
func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Fast path the default case
	if q.lenLocked() == 0 {
		return nil
	}

	transmitLimit := retransmitLimit(q.RetransmitMult, q.NumNodes())

	var (
		bytesUsed int
		toSend    [][]byte
		reinsert  []*limitedBroadcast
	)

	// Visit fresher items first, but only look at stuff that will fit.
	// We'll go tier by tier, grabbing the largest items first.
	minTr, maxTr := q.getTransmitRange()
	for transmits := minTr; transmits <= maxTr; /*do not advance automatically*/ {
		free := int64(limit - bytesUsed - overhead)
		if free <= 0 {
			break // bail out early
		}

		// Search for the least element on a given tier (by transmit count) as
		// defined in the limitedBroadcast.Less function that will fit into our
		// remaining space.
		greaterOrEqual := limitedBroadcast{
			transmits: transmits,
			msgLen:    free,
			id:        math.MaxInt64,
		}
		var keep *limitedBroadcast
		ascendFrom(q.root, &greaterOrEqual, func(cur *limitedBroadcast) bool {
			if cur.transmits != transmits {
				return false // past this tier
			}
			// Check if this is within our limits
			if int64(len(cur.b.Message())) > free {
				// If this happens it's a bug in the datastructure or
				// surrounding use doing something like having len(Message())
				// change over time. There's enough going on here that it's
				// probably sane to just skip it and move on for now.
				return true
			}
			keep = cur
			return false
		})
		if keep == nil {
			// No more items of an appropriate size in the tier.
			transmits++
			continue
		}

		msg := keep.b.Message()

		// Add to slice to send
		bytesUsed += overhead + len(msg)
		toSend = append(toSend, msg)

		// Check if we should stop transmission
		q.deleteItem(keep)
		if keep.transmits+1 >= transmitLimit {
			keep.b.Finished()
		} else {
			// We need to bump this item down to another transmit tier, but
			// because it would be in the same direction that we're walking the
			// tiers, we will have to delay the reinsertion until we are
			// finished our search. Otherwise we'll possibly re-add the message
			// when we ascend to the next tier.
			keep.transmits++
			reinsert = append(reinsert, keep)
		}
	}

	for _, cur := range reinsert {
		q.addItem(cur)
	}

	return toSend
}

// NumQueued returns the number of queued messages
func (q *TransmitLimitedQueue) NumQueued() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lenLocked()
}

// lenLocked returns the length of the overall queue datastructure. You must
// hold the mutex.
func (q *TransmitLimitedQueue) lenLocked() int {
	return q.n
}

// Reset clears all the queued messages. Should only be used for tests.
func (q *TransmitLimitedQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.walkReadOnlyLocked(false, func(cur *limitedBroadcast) bool {
		cur.b.Finished()
		return true
	})

	q.root = nil
	q.n = 0
	q.tm = nil
	q.idGen = 0
}

// Prune will retain the maxRetain latest messages, and the rest
// will be discarded. This can be used to prevent unbounded queue sizes
func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Do nothing if queue size is less than the limit
	for q.n > maxRetain {
		cur := treapMax(q.root)
		cur.b.Finished()
		q.deleteItem(cur)
	}
}
