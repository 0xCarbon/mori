// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestSuspicion_remainingSuspicionTime(t *testing.T) {
	cases := []struct {
		n        int32
		k        int32
		elapsed  time.Duration
		min      time.Duration
		max      time.Duration
		expected time.Duration
	}{
		{0, 3, 0, 2 * time.Second, 30 * time.Second, 30 * time.Second},
		{1, 3, 2 * time.Second, 2 * time.Second, 30 * time.Second, 14 * time.Second},
		{2, 3, 3 * time.Second, 2 * time.Second, 30 * time.Second, 4810 * time.Millisecond},
		{3, 3, 4 * time.Second, 2 * time.Second, 30 * time.Second, -2 * time.Second},
		{4, 3, 5 * time.Second, 2 * time.Second, 30 * time.Second, -3 * time.Second},
		{5, 3, 10 * time.Second, 2 * time.Second, 30 * time.Second, -8 * time.Second},
	}
	for i, c := range cases {
		remaining := remainingSuspicionTime(c.n, c.k, c.elapsed, c.min, c.max)
		if remaining != c.expected {
			t.Errorf("case %d: remaining %9.6f != expected %9.6f", i, remaining.Seconds(), c.expected.Seconds())
		}
	}
}

// TestSuspicion_Timer runs in a synctest bubble, so time is exact: each
// case checks that the timer fires at precisely the timeout derived by
// hand from the Lifeguard formula (max - log(n+1)/log(k+1) * (max - min),
// floored to the millisecond), not earlier, and never again.
func TestSuspicion_Timer(t *testing.T) {
	const k = 3
	const min = 500 * time.Millisecond
	const max = 2 * time.Second

	type pair struct {
		from    string
		newInfo bool
	}
	cases := []struct {
		numConfirmations int
		from             string
		confirmations    []pair
		expected         time.Duration
	}{
		{0, "me", []pair{}, max},
		// n=1: 2000 - log(2)/log(4)*1500 = 1250ms.
		{1, "me", []pair{{"me", false}, {"foo", true}}, 1250 * time.Millisecond},
		{1, "me", []pair{{"me", false}, {"foo", true}, {"foo", false}, {"foo", false}}, 1250 * time.Millisecond},
		// n=2: 2000 - log(3)/log(4)*1500 = 811.28ms, floored to 811ms.
		{2, "me", []pair{{"me", false}, {"foo", true}, {"bar", true}}, 811 * time.Millisecond},
		// n=k: the minimum.
		{3, "me", []pair{{"me", false}, {"foo", true}, {"bar", true}, {"baz", true}}, min},
		{3, "me", []pair{{"me", false}, {"foo", true}, {"bar", true}, {"baz", true}, {"zoo", false}}, min},
	}
	for i, c := range cases {
		synctest.Test(t, func(t *testing.T) {
			ch := make(chan time.Duration, 1)
			start := time.Now()
			f := func(numConfirmations int) {
				if numConfirmations != c.numConfirmations {
					t.Errorf("case %d: bad %d != %d", i, numConfirmations, c.numConfirmations)
				}
				ch <- time.Since(start)
			}

			// Create the timer and add the requested confirmations, spaced
			// out so the elapsed time is part of every recomputation.
			s := newSuspicion(c.from, k, min, max, f)
			const step = 25 * time.Millisecond
			for _, p := range c.confirmations {
				time.Sleep(step)
				if s.Confirm(p.from) != p.newInfo {
					t.Fatalf("case %d: newInfo mismatch for %s", i, p.from)
				}
			}

			// One nanosecond before the timeout it has not fired.
			time.Sleep(c.expected - time.Since(start) - time.Nanosecond)
			synctest.Wait()
			select {
			case d := <-ch:
				t.Fatalf("case %d: fired early at %v, want %v", i, d, c.expected)
			default:
			}

			// At the timeout it fires, exactly then.
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			select {
			case d := <-ch:
				if d != c.expected {
					t.Fatalf("case %d: fired at %v, want %v", i, d, c.expected)
				}
			default:
				t.Fatalf("case %d: did not fire at %v", i, c.expected)
			}

			// A late confirmation (negative remaining time) must not fire
			// it again.
			s.Confirm("late")
			time.Sleep(c.expected)
			synctest.Wait()
			select {
			case d := <-ch:
				t.Fatalf("case %d: fired again at %v", i, d)
			default:
			}
		})
	}
}

func TestSuspicion_Timer_ZeroK(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan time.Duration, 1)
		start := time.Now()
		f := func(int) { ch <- time.Since(start) }

		// With no expected confirmations the timer uses the minimum.
		s := newSuspicion("me", 0, 25*time.Millisecond, 30*time.Second, f)
		if s.Confirm("foo") {
			t.Fatalf("should not provide new information")
		}
		if d := <-ch; d != 25*time.Millisecond {
			t.Fatalf("fired at %v, want 25ms", d)
		}
	})
}

func TestSuspicion_Timer_Immediate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan time.Duration, 1)
		start := time.Now()
		f := func(int) { ch <- time.Since(start) }

		// A confirmation after the new timeout has already passed fires
		// at once (from its own goroutine).
		s := newSuspicion("me", 1, 100*time.Millisecond, 30*time.Second, f)
		time.Sleep(200 * time.Millisecond)
		s.Confirm("foo")
		synctest.Wait()
		select {
		case d := <-ch:
			if d != 200*time.Millisecond {
				t.Fatalf("fired at %v, want 200ms (at the confirmation)", d)
			}
		default:
			t.Fatal("should have fired")
		}
	})
}
