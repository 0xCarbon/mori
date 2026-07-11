// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"context"
	"fmt"
	"slices"

	metrics "github.com/hashicorp/go-metrics/compat"
)

// Event delivery (issue #6).
//
// EventDelegate and ConflictDelegate callbacks used to run synchronously
// under the nodeLock write lock. Instead, state mutations enqueue an event
// at their commit point (still under nodeLock, so queue order IS commit
// order — a global total order), and a single dispatcher goroutine delivers
// the callbacks holding no locks. A blocking or re-entrant consumer can no
// longer stall membership processing or deadlock the instance; the cost is
// that delivery is asynchronous with respect to the mutating call, except
// where an explicit receipt restores synchrony (Create's self-join, and the
// ctx-bounded barriers in LeaveContext/UpdateNodeContext).

type eventKind uint8

const (
	eventJoin eventKind = iota
	eventUpdate
	eventLeave
	eventConflict
)

// eventReceipt lets a lifecycle caller wait for the delivery of the event
// its mutation produced. done is closed exactly once by the dispatcher
// after the callback returns — a persistent, broadcast-style completion
// that idempotent retries can re-observe. attached is set under nodeLock
// when the event is actually enqueued; conditional emitters can return
// without emitting (no delegate configured, no-op update), in which case
// every barrier treats the receipt as trivially satisfied.
type eventReceipt struct {
	done     chan struct{}
	attached bool
}

func newEventReceipt() *eventReceipt {
	return &eventReceipt{done: make(chan struct{})}
}

// queuedEvent is a committed membership event awaiting delivery. node (and
// other, for conflicts) are deep copies: Node's two mutable aliases (Addr,
// Meta) are cloned, because the map's backing arrays keep mutating under
// nodeLock after the event is queued.
type queuedEvent struct {
	kind    eventKind
	node    Node
	other   *Node // eventConflict only: the rejected claimant
	seq     uint64
	receipt *eventReceipt
}

// cloneNode deep-copies a Node's mutable aliases.
func cloneNode(n *Node) Node {
	c := *n
	c.Addr = slices.Clone(n.Addr)
	c.Meta = slices.Clone(n.Meta)
	return c
}

// eventPendingWarnThresholds are the queue depths at which an upward
// crossing logs a warning about a slow event consumer.
var eventPendingWarnThresholds = []int{1_000, 10_000, 100_000}

// enqueueEventLocked commits an event to the delivery queue. The caller
// must hold nodeLock (write): that is what makes queue order equal commit
// order. Lock order: nodeLock -> eventMu.
func (m *Memberlist) enqueueEventLocked(kind eventKind, node *Node, other *Node, r *eventReceipt) {
	ev := queuedEvent{kind: kind, node: cloneNode(node)}
	if other != nil {
		o := cloneNode(other)
		ev.other = &o
	}
	if r != nil {
		r.attached = true
		ev.receipt = r
	}

	m.eventMu.Lock()
	m.eventSeq++
	ev.seq = m.eventSeq
	m.events = append(m.events, ev)
	m.eventPending++
	pending := m.eventPending
	m.eventMu.Unlock()

	metrics.SetGaugeWithLabels([]string{"memberlist", "event", "pending"}, float32(pending), m.metricLabels)
	for _, th := range eventPendingWarnThresholds {
		if pending == th+1 {
			m.logger.Printf("[WARN] memberlist: event queue depth crossed %d: event consumer is slow or blocked", th)
		}
	}

	// Coalescing wake: a dropped token is fine, the dispatcher drains the
	// whole queue on every pass.
	select {
	case m.eventWake <- struct{}{}:
	default:
	}
}

// eventDispatch is the single event-delivery goroutine. It pops one event
// at a time (so eventPending always reflects undelivered work, including
// the in-flight event), invokes the consumer callback holding no locks,
// and completes the receipt. It exits when admission is sealed and the
// queue is drained. Panics in consumer callbacks are deliberately not
// recovered (fail-fast): before #6 they unwound a protocol goroutine and
// crashed the process; masking them here would silently complete receipts
// for callbacks that never ran.
func (m *Memberlist) eventDispatch() {
	for {
		m.eventMu.Lock()
		if len(m.events) == 0 {
			if m.eventsSealed {
				m.eventMu.Unlock()
				return
			}
			m.eventMu.Unlock()
			<-m.eventWake
			continue
		}
		ev := m.events[0]
		m.events[0] = queuedEvent{}
		m.events = m.events[1:]
		if len(m.events) == 0 {
			m.events = nil // release the drained backing array
		}
		m.eventMu.Unlock()

		m.deliverEvent(ev)

		m.eventMu.Lock()
		m.eventPending--
		pending := m.eventPending
		m.eventMu.Unlock()
		metrics.SetGaugeWithLabels([]string{"memberlist", "event", "pending"}, float32(pending), m.metricLabels)
		metrics.IncrCounterWithLabels([]string{"memberlist", "event", "delivered"}, 1, m.metricLabels)
	}
}

// deliverEvent invokes the consumer callback for one event, bracketed as a
// delegate callback (so Shutdown/lifecycle calls made from inside it are
// detected as re-entrant), holding no Memberlist locks.
func (m *Memberlist) deliverEvent(ev queuedEvent) {
	m.runCallback(func() {
		switch ev.kind {
		case eventJoin:
			m.config.Events.NotifyJoin(&ev.node)
		case eventUpdate:
			m.config.Events.NotifyUpdate(&ev.node)
		case eventLeave:
			m.config.Events.NotifyLeave(&ev.node)
		case eventConflict:
			m.config.Conflict.NotifyConflict(&ev.node, ev.other)
		}
	})
	if ev.receipt != nil {
		close(ev.receipt.done)
	}
}

// inCallback reports whether the calling goroutine is inside a delegate
// callback. Unlike inReentrantContext it deliberately ignores tracked
// background goroutines: only literal callback context may bypass event
// delivery barriers, otherwise ordinary goroutines would silently lose
// their delivery guarantee.
func (m *Memberlist) inCallback() bool {
	id := goid()
	m.trackedMu.Lock()
	depth := m.callbackGoids[id]
	m.trackedMu.Unlock()
	return depth > 0
}

// waitEventReceipt blocks until the event carrying r is delivered, ctx is
// done, or the instance shuts down. Unattached receipts (no event emitted)
// are trivially satisfied. Callback context skips the wait: the dispatcher
// executing the calling callback can never deliver the awaited event while
// the callback is still running. A completed delivery wins the race against
// both ctx and shutdown.
func (m *Memberlist) waitEventReceipt(ctx context.Context, r *eventReceipt, what string) error {
	if r == nil || !r.attached {
		return nil
	}
	if m.inCallback() {
		return nil
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		select {
		case <-r.done:
			return nil
		default:
		}
		return fmt.Errorf("memberlist: %s committed; event delivery unconfirmed: %w", what, ctx.Err())
	case <-m.shutdownCh:
		select {
		case <-r.done:
			return nil
		default:
		}
		return fmt.Errorf("memberlist: %s event delivery interrupted: %w", what, ErrShutdown)
	}
}
