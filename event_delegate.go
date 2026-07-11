// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

// EventDelegate is a simpler delegate that is used only to receive
// notifications about members joining and leaving.
//
// Delivery contract: events are enqueued at the commit point of the state
// change (under the internal node lock) and delivered by a single dispatcher
// goroutine holding no locks. Delivery is therefore single-threaded and in
// GLOBAL COMMIT ORDER (a fortiori per-node ordered: join, then updates,
// then leave), but asynchronous with respect to the mutating call — except
// where a receipt restores synchrony: Create returns after the local join
// was delivered, and LeaveContext/UpdateNodeContext confirm delivery within
// their ctx. Node values passed to callbacks are private snapshots taken at
// commit time.
//
// Callbacks MAY block and MAY call back into Memberlist (Members,
// UpdateNode, Leave, Shutdown, ...) without deadlocking. A blocked callback
// does not stall membership processing; it delays subsequent event delivery
// (the queue is unbounded — memory grows), delays Shutdown's final drain,
// and costs lifecycle callers only their ctx budget. Callbacks must not
// synchronously wait for a lifecycle call issued from a goroutine they
// spawned — that reintroduces a delivery cycle bounded only by that call's
// ctx. A panic in a callback is not recovered (fail-fast).
type EventDelegate interface {
	// NotifyJoin is invoked when a node is detected to have joined.
	// The Node argument must not be modified.
	NotifyJoin(*Node)

	// NotifyLeave is invoked when a node is detected to have left.
	// The Node argument must not be modified.
	NotifyLeave(*Node)

	// NotifyUpdate is invoked when a node is detected to have
	// updated, usually involving the meta data. The Node argument
	// must not be modified.
	NotifyUpdate(*Node)
}

// ChannelEventDelegate is used to enable an application to receive
// events about joins and leaves over a channel instead of a direct
// function call.
//
// Care must be taken that events are processed in a timely manner from
// the channel: an unconsumed channel no longer blocks membership processing
// (it blocks only the event dispatcher), but undelivered events accumulate
// in memory and Shutdown's final drain waits for the channel to be read.
type ChannelEventDelegate struct {
	Ch chan<- NodeEvent
}

// NodeEventType are the types of events that can be sent from the
// ChannelEventDelegate.
type NodeEventType int

const (
	NodeJoin NodeEventType = iota
	NodeLeave
	NodeUpdate
)

// NodeEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified. It is passed
// as a pointer to avoid unnecessary copies. If you wish to modify the node,
// make a copy first.
type NodeEvent struct {
	Event NodeEventType
	Node  *Node
}

func (c *ChannelEventDelegate) NotifyJoin(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeJoin, &node}
}

func (c *ChannelEventDelegate) NotifyLeave(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeLeave, &node}
}

func (c *ChannelEventDelegate) NotifyUpdate(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeUpdate, &node}
}
