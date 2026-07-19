package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestLocalFSColdTier_PutGet: Put value; Get returns same bytes.
func TestLocalFSColdTier_PutGet(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	handle := "abcdef01"
	want := []byte("hello-tiered-value")
	if err := tier.Put(handle, want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := tier.Get(handle)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

// TestLocalFSColdTier_Delete: Put; Delete; Get returns error.
func TestLocalFSColdTier_Delete(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	handle := "deadbeef"
	if err := tier.Put(handle, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tier.Delete(handle); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := tier.Get(handle); err == nil {
		t.Fatal("expected error after Delete, got nil")
	}
}

// TestLocalFSColdTier_LargeValue: Put 5 MB value; Get returns same bytes.
func TestLocalFSColdTier_LargeValue(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	large := bytes.Repeat([]byte("X"), 5*1024*1024)
	handle := "00large0"
	if err := tier.Put(handle, large); err != nil {
		t.Fatalf("Put large value: %v", err)
	}
	got, err := tier.Get(handle)
	if err != nil {
		t.Fatalf("Get large value: %v", err)
	}
	if !bytes.Equal(got, large) {
		t.Fatalf("large value mismatch: len(got)=%d, len(want)=%d", len(got), len(large))
	}
}

// TestLocalFSColdTier_ConcurrentPut: 50 goroutines Put different handles
// concurrently; all readable afterwards.
func TestLocalFSColdTier_ConcurrentPut(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	const n = 50
	handles := make([]string, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		handles[i] = fmt.Sprintf("%08x", i+1)
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			if err := tier.Put(handles[i], values[i]); err != nil {
				t.Errorf("Put %s: %v", handles[i], err)
			}
		}()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		got, err := tier.Get(handles[i])
		if err != nil {
			t.Errorf("Get %s: %v", handles[i], err)
			continue
		}
		if !bytes.Equal(got, values[i]) {
			t.Errorf("handle %s: expected %q, got %q", handles[i], values[i], got)
		}
	}
}

// TestPutGetColdTier: SetColdTier(localFS); PutColdTier(key, val);
// GetColdTier(handle) returns val.
// Not parallel — mutates global SetColdTier.
func TestPutGetColdTier(t *testing.T) {
	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	SetColdTier(tier)
	t.Cleanup(func() { SetColdTier(nil) })

	key := "mykey"
	val := []byte("cold-storage-value")
	handle, err := PutColdTier(key, val)
	if err != nil {
		t.Fatalf("PutColdTier: %v", err)
	}
	if handle == "" {
		t.Fatal("PutColdTier returned empty handle")
	}

	got, err := GetColdTier(handle)
	if err != nil {
		t.Fatalf("GetColdTier: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

// TestTieredStats: Verify demotions/hits/misses counters increment correctly.
// Not parallel — mutates global SetColdTier and global tierStats atomics.
func TestTieredStats(t *testing.T) {
	dir := t.TempDir()
	tier, err := NewLocalFSColdTier(dir)
	if err != nil {
		t.Fatalf("NewLocalFSColdTier: %v", err)
	}

	// Use a fresh engine just for GetTieredStats.
	se := newTestEngine(t)

	SetColdTier(tier)
	t.Cleanup(func() { SetColdTier(nil) })

	// Reset counters by using a fresh engine's stats call as baseline.
	_ = se.GetTieredStats() // just to confirm it doesn't panic

	// Capture baseline.
	before := se.GetTieredStats()

	// Demote a value.
	handle, err := PutColdTier("stats-key", []byte("stats-val"))
	if err != nil {
		t.Fatalf("PutColdTier: %v", err)
	}

	// Successful Get — should count as a hit.
	if _, err := GetColdTier(handle); err != nil {
		t.Fatalf("GetColdTier: %v", err)
	}

	// Get on a non-existent handle — should count as a miss.
	_, _ = GetColdTier("ffffffff")

	after := se.GetTieredStats()

	if after.Demotions <= before.Demotions {
		t.Errorf("expected Demotions to increase, before=%d after=%d", before.Demotions, after.Demotions)
	}
	if after.HitCount <= before.HitCount {
		t.Errorf("expected HitCount to increase, before=%d after=%d", before.HitCount, after.HitCount)
	}
	if after.MissCount <= before.MissCount {
		t.Errorf("expected MissCount to increase, before=%d after=%d", before.MissCount, after.MissCount)
	}
}
