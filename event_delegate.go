// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

// EventDelegate is a simpler delegate that is used only to receive
// notifications about members joining and leaving. The methods in this
// delegate may be called by multiple goroutines, but never concurrently.
// This allows you to reason about ordering.
//
// The callbacks run synchronously while memberlist holds its internal node
// lock, which is what serializes them. They must therefore return promptly
// and must not block: a blocked callback stalls all membership processing,
// including the ctx bound of LeaveContext/UpdateNodeContext. They also must
// not call back into Memberlist methods that acquire the node lock
// (Members, NumMembers, UpdateNode, Leave, ...) or they will deadlock;
// hand off to another goroutine instead.
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
// the channel, since this delegate will block until an event can be sent.
// An unconsumed channel blocks membership processing entirely (see the
// EventDelegate contract): size the channel buffer for the expected burst
// of membership changes and keep a consumer running for the lifetime of
// the memberlist instance.
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
