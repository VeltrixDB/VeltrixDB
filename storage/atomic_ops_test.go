package storage

import (
	"math"
	"strconv"
	"sync"
	"testing"
)

// TestCAS_Success: Put "v1"; CAS(expected="v1", new="v2") returns CASSuccess; Get returns "v2"
func TestCAS_Success(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "cas-success"
	if err := se.Put(key, []byte("v1"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	result, err := se.CompareAndSwap(key, []byte("v1"), []byte("v2"), -1)
	if err != nil {
		t.Fatalf("CAS error: %v", err)
	}
	if result != CASSuccess {
		t.Fatalf("expected CASSuccess, got %d", result)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected value %q, got %q", "v2", string(got))
	}
}

// TestCAS_Mismatch: Put "v1"; CAS(expected="wrong", new="v2") returns CASMismatch; value unchanged
func TestCAS_Mismatch(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "cas-mismatch"
	if err := se.Put(key, []byte("v1"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	result, err := se.CompareAndSwap(key, []byte("wrong"), []byte("v2"), -1)
	if err != nil {
		t.Fatalf("CAS error: %v", err)
	}
	if result != CASMismatch {
		t.Fatalf("expected CASMismatch, got %d", result)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get after CAS mismatch: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("expected value unchanged %q, got %q", "v1", string(got))
	}
}

// TestCAS_KeyNotFound: CAS on missing key returns CASKeyNotFound
func TestCAS_KeyNotFound(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	result, err := se.CompareAndSwap("nonexistent-key", []byte("v1"), []byte("v2"), -1)
	if err != nil {
		t.Fatalf("CAS error: %v", err)
	}
	if result != CASKeyNotFound {
		t.Fatalf("expected CASKeyNotFound, got %d", result)
	}
}

// TestIncrement_Basic: Put "5"; Increment(3) returns 8; Get returns "8"
func TestIncrement_Basic(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "incr-basic"
	if err := se.Put(key, []byte("5"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	newVal, err := se.Increment(key, 3, -1)
	if err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if newVal != 8 {
		t.Fatalf("expected 8, got %d", newVal)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "8" {
		t.Fatalf("expected stored value %q, got %q", "8", string(got))
	}
}

// TestIncrement_FromNothing: Increment on missing key (starts at 0); returns delta
func TestIncrement_FromNothing(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "incr-new"
	newVal, err := se.Increment(key, 42, -1)
	if err != nil {
		t.Fatalf("Increment on new key: %v", err)
	}
	if newVal != 42 {
		t.Fatalf("expected 42, got %d", newVal)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "42" {
		t.Fatalf("expected stored %q, got %q", "42", string(got))
	}
}

// TestIncrement_Overflow: Increment MaxInt64 by 1 should saturate (not panic/wrap)
func TestIncrement_Overflow(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "incr-overflow"
	maxStr := strconv.FormatInt(math.MaxInt64, 10)
	if err := se.Put(key, []byte(maxStr), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	newVal, err := se.Increment(key, 1, -1)
	if err != nil {
		t.Fatalf("Increment overflow: %v", err)
	}
	// Saturating: result should remain at MaxInt64, not wrap to negative
	if newVal != math.MaxInt64 {
		t.Fatalf("expected MaxInt64=%d (saturating), got %d", int64(math.MaxInt64), newVal)
	}
}

// TestDecrement_Basic: Put "10"; Decrement(3) returns 7
func TestDecrement_Basic(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "decr-basic"
	if err := se.Put(key, []byte("10"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	newVal, err := se.Decrement(key, 3, -1)
	if err != nil {
		t.Fatalf("Decrement: %v", err)
	}
	if newVal != 7 {
		t.Fatalf("expected 7, got %d", newVal)
	}
}

// TestDecrement_BelowZero: Decrement below zero should use saturating arithmetic
func TestDecrement_BelowZero(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "decr-belowzero"
	minStr := strconv.FormatInt(math.MinInt64, 10)
	if err := se.Put(key, []byte(minStr), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	newVal, err := se.Decrement(key, 1, -1)
	if err != nil {
		t.Fatalf("Decrement below zero: %v", err)
	}
	// Saturating: result should stay at MinInt64
	if newVal != math.MinInt64 {
		t.Fatalf("expected MinInt64=%d (saturating), got %d", int64(math.MinInt64), newVal)
	}
}

// TestSetIfNotExists_Created: Key absent -> SetNXCreated; value readable
func TestSetIfNotExists_Created(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "setnx-created"
	result, err := se.SetIfNotExists(key, []byte("hello"), -1)
	if err != nil {
		t.Fatalf("SetIfNotExists: %v", err)
	}
	if result != SetNXCreated {
		t.Fatalf("expected SetNXCreated, got %d", result)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get after SETNX: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", string(got))
	}
}

// TestSetIfNotExists_Exists: Key present -> SetNXExists; value unchanged
func TestSetIfNotExists_Exists(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "setnx-exists"
	if err := se.Put(key, []byte("original"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	result, err := se.SetIfNotExists(key, []byte("new"), -1)
	if err != nil {
		t.Fatalf("SetIfNotExists: %v", err)
	}
	if result != SetNXExists {
		t.Fatalf("expected SetNXExists, got %d", result)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("expected value unchanged %q, got %q", "original", string(got))
	}
}

// TestAtomicOps_Concurrent: 64 goroutines each Increment the same key by 1;
// final value == 64 (tests that the shard lock serializes correctly).
func TestAtomicOps_Concurrent(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "concurrent-incr"
	// Seed the key at 0 so it definitely exists before the goroutines race.
	if err := se.Put(key, []byte("0"), -1); err != nil {
		t.Fatalf("Put seed: %v", err)
	}

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := se.Increment(key, 1, -1); err != nil {
				// Use t.Logf instead of t.Errorf to avoid data race on t
				t.Logf("Increment error: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	n, err := strconv.ParseInt(string(got), 10, 64)
	if err != nil {
		t.Fatalf("parse final value %q: %v", string(got), err)
	}
	if n != workers {
		t.Fatalf("expected final value %d, got %d", workers, n)
	}
}
