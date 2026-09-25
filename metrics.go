// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"slices"
	"time"
)

// Label is a metric dimension.
type Label struct {
	Name  string
	Value string
}

// MetricSink receives Mori's telemetry. Its method set mirrors the
// labeled calls of github.com/hashicorp/go-metrics, so an adapter to that
// package (or to Prometheus or OpenTelemetry) is a few lines.
//
// Keys are the metric name split on '.', for example
// ["memberlist", "udp", "sent"]. Implementations must be safe for concurrent
// use and must not block: they are called on protocol paths. They must not
// modify key or labels, and must copy them to retain them.
//
// Emitted metrics (name: kind, labels beyond Config.MetricLabels):
//
//	memberlist.tcp.accept        counter  inbound stream connections
//	memberlist.tcp.connect       counter  outbound push/pull connections
//	memberlist.tcp.sent          counter  bytes written to streams
//	memberlist.udp.sent          counter  bytes written as packets
//	memberlist.udp.received      counter  bytes read as packets (NetTransport)
//	memberlist.msg.alive         counter  alive messages applied
//	memberlist.msg.suspect       counter  suspect messages applied
//	memberlist.msg.dead          counter  dead messages applied
//	memberlist.degraded.probe    counter  probes run with a degraded interval
//	memberlist.degraded.timeout  counter  suspicions that timed out short of confirmations
//	memberlist.event.delivered   counter  events delivered to the delegates
//	memberlist.event.pending     gauge    undelivered events, including the in-flight one
//	memberlist.health.score      gauge    awareness score (0 is healthy)
//	memberlist.node.instances    gauge    known nodes; label node_state
//	memberlist.size.local        gauge    bytes of local push/pull state
//	memberlist.size.remote       sample   bytes of remote push/pull state
//	memberlist.queue.broadcasts  sample   queued broadcasts, every QueueCheckInterval
//	memberlist.probeNode         timer    duration of one probe
//	memberlist.gossip            timer    duration of one gossip round
//	memberlist.pushPullNode      timer    duration of one push/pull exchange
type MetricSink interface {
	IncrCounter(key []string, val float32, labels []Label)
	SetGauge(key []string, val float32, labels []Label)
	AddSample(key []string, val float32, labels []Label)
	// MeasureSince records the time elapsed since start as a sample in
	// milliseconds.
	MeasureSince(key []string, start time.Time, labels []Label)
}

// Metric keys, allocated once.
var (
	keyTCPAccept       = []string{"memberlist", "tcp", "accept"}
	keyTCPConnect      = []string{"memberlist", "tcp", "connect"}
	keyTCPSent         = []string{"memberlist", "tcp", "sent"}
	keyUDPSent         = []string{"memberlist", "udp", "sent"}
	keyUDPReceived     = []string{"memberlist", "udp", "received"}
	keyMsgAlive        = []string{"memberlist", "msg", "alive"}
	keyMsgSuspect      = []string{"memberlist", "msg", "suspect"}
	keyMsgDead         = []string{"memberlist", "msg", "dead"}
	keyDegradedProbe   = []string{"memberlist", "degraded", "probe"}
	keyDegradedTimeout = []string{"memberlist", "degraded", "timeout"}
	keyEventDelivered  = []string{"memberlist", "event", "delivered"}
	keyEventPending    = []string{"memberlist", "event", "pending"}
	keyHealthScore     = []string{"memberlist", "health", "score"}
	keyNodeInstances   = []string{"memberlist", "node", "instances"}
	keySizeLocal       = []string{"memberlist", "size", "local"}
	keySizeRemote      = []string{"memberlist", "size", "remote"}
	keyQueueBroadcasts = []string{"memberlist", "queue", "broadcasts"}
	keyProbeNode       = []string{"memberlist", "probeNode"}
	keyGossip          = []string{"memberlist", "gossip"}
	keyPushPullNode    = []string{"memberlist", "pushPullNode"}
)

// nodeStates lists every node state, indexed by its value.
var nodeStates = [...]NodeStateType{StateAlive, StateSuspect, StateDead, StateLeft}

// telemetry binds an optional sink to an instance's constant labels. A nil
// *telemetry and the zero value both discard everything.
type telemetry struct {
	sink   MetricSink
	labels []Label
	// stateLabels[s] is labels plus node_state=s, for the instances gauge.
	stateLabels [len(nodeStates)][]Label
}

func newTelemetry(sink MetricSink, labels []Label) *telemetry {
	t := &telemetry{sink: sink, labels: slices.Clip(slices.Clone(labels))}
	for i, s := range nodeStates {
		t.stateLabels[i] = append(slices.Clone(t.labels), Label{Name: "node_state", Value: s.metricsString()})
	}
	return t
}

func (t *telemetry) counter(key []string, val float32) {
	if t != nil && t.sink != nil {
		t.sink.IncrCounter(key, val, t.labels)
	}
}

func (t *telemetry) gauge(key []string, val float32) {
	if t != nil && t.sink != nil {
		t.sink.SetGauge(key, val, t.labels)
	}
}

func (t *telemetry) sample(key []string, val float32) {
	if t != nil && t.sink != nil {
		t.sink.AddSample(key, val, t.labels)
	}
}

// since records the time elapsed since start; use as
// defer m.metrics.since(key, time.Now()).
func (t *telemetry) since(key []string, start time.Time) {
	if t != nil && t.sink != nil {
		t.sink.MeasureSince(key, start, t.labels)
	}
}

// nodeInstances sets the instances gauge for every node state.
func (t *telemetry) nodeInstances(counts [len(nodeStates)]int) {
	if t == nil || t.sink == nil {
		return
	}
	for i, n := range counts {
		t.sink.SetGauge(keyNodeInstances, float32(n), t.stateLabels[i])
	}
}
