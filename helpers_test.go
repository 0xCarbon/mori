// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

package mori

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Assertion helpers built on the standard library. Each stops the test on
// failure. msgAndArgs is an optional format string and its arguments.

// fataler is satisfied by *testing.T, *testing.B and the retry package's R.
type fataler interface {
	Fatalf(format string, args ...any)
}

func helper(t fataler) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
}

func describe(msgAndArgs []any) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if format, ok := msgAndArgs[0].(string); ok {
		return fmt.Sprintf(format, msgAndArgs[1:]...) + ": "
	}
	return fmt.Sprint(msgAndArgs...) + ": "
}

func noErr(t fataler, err error, msgAndArgs ...any) {
	helper(t)
	if err != nil {
		t.Fatalf("%sunexpected error: %v", describe(msgAndArgs), err)
	}
}

func isErr(t fataler, err error, msgAndArgs ...any) {
	helper(t)
	if err == nil {
		t.Fatalf("%sexpected an error, got nil", describe(msgAndArgs))
	}
}

func errIs(t fataler, err, target error, msgAndArgs ...any) {
	helper(t)
	if !errors.Is(err, target) {
		t.Fatalf("%serror %v does not match %v", describe(msgAndArgs), err, target)
	}
}

// equalValues compares like reflect.DeepEqual, except that byte slices are
// equal when their contents are (nil equals empty).
func equalValues(want, got any) bool {
	if wb, ok := want.([]byte); ok {
		gb, ok := got.([]byte)
		return ok && bytes.Equal(wb, gb)
	}
	return reflect.DeepEqual(want, got)
}

func equal(t fataler, want, got any, msgAndArgs ...any) {
	helper(t)
	if !equalValues(want, got) {
		t.Fatalf("%snot equal:\nwant: %#v\n got: %#v", describe(msgAndArgs), want, got)
	}
}

func isTrue(t fataler, cond bool, msgAndArgs ...any) {
	helper(t)
	if !cond {
		t.Fatalf("%sexpected true", describe(msgAndArgs))
	}
}

func isFalse(t fataler, cond bool, msgAndArgs ...any) {
	helper(t)
	if cond {
		t.Fatalf("%sexpected false", describe(msgAndArgs))
	}
}

func contains(t fataler, s, substr string) {
	helper(t)
	if !strings.Contains(s, substr) {
		t.Fatalf("%q does not contain %q", s, substr)
	}
}

func isZero(t fataler, v any, msgAndArgs ...any) {
	helper(t)
	if v != nil && !reflect.ValueOf(v).IsZero() {
		t.Fatalf("%sexpected the zero value, got %#v", describe(msgAndArgs), v)
	}
}

func isNil(t fataler, v any) {
	helper(t)
	if v == nil {
		return
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if rv.IsNil() {
			return
		}
	}
	t.Fatalf("expected nil, got %#v", v)
}

func hasLen(t fataler, v any, n int, msgAndArgs ...any) {
	helper(t)
	if l := reflect.ValueOf(v).Len(); l != n {
		t.Fatalf("%slength %d, want %d: %#v", describe(msgAndArgs), l, n, v)
	}
}

func notEmpty(t fataler, v any, msgAndArgs ...any) {
	helper(t)
	if reflect.ValueOf(v).Len() == 0 {
		t.Fatalf("%sexpected a non-empty value", describe(msgAndArgs))
	}
}

func less[T cmp.Ordered](t fataler, a, b T, msgAndArgs ...any) {
	helper(t)
	if a >= b {
		t.Fatalf("%s%v is not less than %v", describe(msgAndArgs), a, b)
	}
}

func lessOrEqual[T cmp.Ordered](t fataler, a, b T, msgAndArgs ...any) {
	helper(t)
	if a > b {
		t.Fatalf("%s%v is greater than %v", describe(msgAndArgs), a, b)
	}
}

func greaterOrEqual[T cmp.Ordered](t fataler, a, b T, msgAndArgs ...any) {
	helper(t)
	if a < b {
		t.Fatalf("%s%v is less than %v", describe(msgAndArgs), a, b)
	}
}

func positive[T cmp.Ordered](t fataler, a T, msgAndArgs ...any) {
	helper(t)
	var zero T
	if a <= zero {
		t.Fatalf("%s%v is not positive", describe(msgAndArgs), a)
	}
}

// elementsMatch reports whether two slices hold the same elements with the
// same multiplicity, in any order.
func elementsMatch[T any](t fataler, want, got []T, msgAndArgs ...any) {
	helper(t)
	ok := len(want) == len(got)
	used := make([]bool, len(got))
	for _, w := range want {
		if !ok {
			break
		}
		found := false
		for i, g := range got {
			if !used[i] && equalValues(w, g) {
				used[i], found = true, true
				break
			}
		}
		ok = found
	}
	if !ok {
		t.Fatalf("%selements differ:\nwant: %#v\n got: %#v", describe(msgAndArgs), want, got)
	}
}

func panics(t fataler, f func(), msgAndArgs ...any) {
	helper(t)
	defer func() {
		if recover() == nil {
			t.Fatalf("%sexpected a panic", describe(msgAndArgs))
		}
	}()
	f()
}

// goroutineIDs returns the ids of every live goroutine except the caller.
func goroutineIDs() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	ids := map[string]string{}
	for i, g := range strings.Split(string(buf), "\n\n") {
		if i == 0 {
			continue // the calling goroutine
		}
		header, _, _ := strings.Cut(g, "\n")
		fields := strings.Fields(header) // "goroutine 12 [chan receive]:"
		if len(fields) >= 2 {
			ids[fields[1]] = g
		}
	}
	return ids
}

// newGoroutines returns the stacks of live goroutines absent from before.
func newGoroutines(before map[string]string) []string {
	var fresh []string
	for id, stack := range goroutineIDs() {
		if _, ok := before[id]; !ok {
			fresh = append(fresh, stack)
		}
	}
	return fresh
}

// verifyNoGoroutineLeak records the live goroutines and returns a check
// that fails the test if goroutines started since remain alive after a
// grace period. Standard-library replacement for goleak.VerifyNone with
// IgnoreCurrent.
func verifyNoGoroutineLeak(t *testing.T) func() {
	t.Helper()
	before := goroutineIDs()
	return func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			leaked := newGoroutines(before)
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d goroutine(s) leaked:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestGoroutineLeakCheckDetectsLeaks: the leak check sees a goroutine
// started after the snapshot, and stops seeing it once it exits.
func TestGoroutineLeakCheckDetectsLeaks(t *testing.T) {
	before := goroutineIDs()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-stop
	}()
	leaked := newGoroutines(before)
	if len(leaked) != 1 || !strings.Contains(leaked[0], "TestGoroutineLeakCheckDetectsLeaks") {
		t.Fatalf("leak check found %d goroutines, want the one started by this test: %v", len(leaked), leaked)
	}
	close(stop)
	<-done
	for deadline := time.Now().Add(5 * time.Second); len(newGoroutines(before)) != 0; {
		if time.Now().After(deadline) {
			t.Fatalf("exited goroutine still reported: %v", newGoroutines(before))
		}
		time.Sleep(time.Millisecond)
	}
}
