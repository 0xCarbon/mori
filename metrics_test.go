// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingSink is a MetricSink that records the last value (gauges), the
// sum (counters) and every observation (samples, timers), keyed by the
// dotted name plus ";name=value" labels.
type recordingSink struct {
	mu       sync.Mutex
	counters map[string]float32
	gauges   map[string]float32
	samples  map[string][]float32
}

func newRecordingSink() *recordingSink {
	return &recordingSink{counters: map[string]float32{}, gauges: map[string]float32{}, samples: map[string][]float32{}}
}

func metricName(key []string, labels []Label) string {
	name := strings.Join(key, ".")
	for _, l := range labels {
		name += ";" + l.Name + "=" + l.Value
	}
	return name
}

func (s *recordingSink) IncrCounter(key []string, val float32, labels []Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[metricName(key, labels)] += val
}

func (s *recordingSink) SetGauge(key []string, val float32, labels []Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gauges[metricName(key, labels)] = val
}

func (s *recordingSink) AddSample(key []string, val float32, labels []Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := metricName(key, labels)
	s.samples[name] = append(s.samples[name], val)
}

func (s *recordingSink) MeasureSince(key []string, start time.Time, labels []Label) {
	s.AddSample(key, float32(time.Since(start).Seconds()*1000), labels)
}

func (s *recordingSink) gauge(name string) (float32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.gauges[name]
	return v, ok
}

func (s *recordingSink) counter(name string) float32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[name]
}

func (s *recordingSink) sampleCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.samples[name])
}

// TestTelemetryLabelsAreIsolated: the instance labels are copied, so a
// caller mutating Config.MetricLabels after Create, or a sink appending to
// the labels it receives, cannot change what later calls report.
func TestTelemetryLabelsAreIsolated(t *testing.T) {
	sink := newRecordingSink()
	labels := []Label{{Name: "cluster", Value: "a"}}
	tm := newTelemetry(sink, labels)
	labels[0].Value = "mutated"

	tm.counter(keyMsgAlive, 1)
	got := tm.labels
	_ = append(got, Label{Name: "sink", Value: "appended"})
	tm.counter(keyMsgAlive, 1)
	if v := sink.counter("memberlist.msg.alive;cluster=a"); v != 2 {
		t.Fatalf("counter = %v, want 2 under the original label", v)
	}

	tm.nodeInstances([len(nodeStates)]int{3, 1, 0, 2})
	for i, want := range []float32{3, 1, 0, 2} {
		name := "memberlist.node.instances;cluster=a;node_state=" + nodeStates[i].metricsString()
		if v, ok := sink.gauge(name); !ok || v != want {
			t.Fatalf("%s = %v (%v), want %v", name, v, ok, want)
		}
	}
}

// TestTelemetryNilSinkDiscards: the zero configuration emits nothing and
// does not allocate on the metric path.
func TestTelemetryNilSinkDiscards(t *testing.T) {
	tm := newTelemetry(nil, []Label{{Name: "a", Value: "b"}})
	allocs := testing.AllocsPerRun(100, func() {
		tm.counter(keyUDPSent, 1)
		tm.gauge(keyHealthScore, 1)
		tm.sample(keySizeRemote, 1)
		tm.since(keyGossip, time.Time{})
		tm.nodeInstances([len(nodeStates)]int{})
	})
	if allocs != 0 {
		t.Fatalf("nil sink allocated %v times per run", allocs)
	}
}

// TestMetricsReachInstanceSink: a Memberlist reports through its own sink
// with its own labels, with no process-global state.
func TestMetricsReachInstanceSink(t *testing.T) {
	sinkA, sinkB := newRecordingSink(), newRecordingSink()
	a := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("a")
		c.Metrics = sinkA
		c.MetricLabels = []Label{{Name: "instance", Value: "a"}}
	})
	t.Cleanup(func() { _ = a.Shutdown() })
	b := GetMemberlist(t, func(c *Config) {
		c.Transport = (&MockNetwork{}).NewTransport("b")
		c.Metrics = sinkB
	})
	t.Cleanup(func() { _ = b.Shutdown() })

	a.aliveNode(&alive{Incarnation: 1, Node: "peer", Addr: []byte{10, 0, 0, 1}, Port: 7946, Vsn: []uint8{1, 5, 2, 0, 0, 0}}, false)
	if v := sinkA.counter("memberlist.msg.alive;instance=a"); v != 1 {
		t.Fatalf("instance a alive counter = %v, want 1", v)
	}
	sinkB.mu.Lock()
	n := len(sinkB.counters)
	sinkB.mu.Unlock()
	if n != 0 {
		t.Fatalf("instance b recorded %d counters from instance a", n)
	}
	if !slices.Equal(a.metrics.labels, []Label{{Name: "instance", Value: "a"}}) {
		t.Fatalf("instance labels = %v", a.metrics.labels)
	}
}
