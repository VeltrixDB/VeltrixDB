package storage

// engine_test.go — comprehensive unit tests for StorageEngine.
//
// Each test uses a fresh temporary directory; the test cleanup removes it.
// Multi-disk tests create 2 independent dirs to exercise shard distribution.

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// newTestEngine2Disks creates an engine with 2 independent temp directories.
func newTestEngine2Disks(t *testing.T) *StorageEngine {
	t.Helper()
	dir0, err := os.MkdirTemp("", "veltrix-disk0-")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dir1, err := os.MkdirTemp("", "veltrix-disk1-")
	if err != nil {
		os.RemoveAll(dir0)
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir0)
		os.RemoveAll(dir1)
	})

	cfg := DefaultStorageConfig()
	cfg.DataDirPath = ""
	cfg.DataDirPaths = []string{dir0, dir1}
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false

	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	return se
}

// ── Basic CRUD ────────────────────────────────────────────────────────────────

func TestEngine_BasicPutGet(t *testing.T) {
	se := newTestEngine(t)

	key := "hello"
	val := []byte("world")

	if err := se.Put(key, val, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get = %q, want %q", got, val)
	}
}

func TestEngine_Delete(t *testing.T) {
	se := newTestEngine(t)

	key := "todelete"
	if err := se.Put(key, []byte("v"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := se.Get(key)
	if err == nil {
		t.Errorf("expected error after Delete, got nil")
	}
}

func TestEngine_KeyNotFound(t *testing.T) {
	se := newTestEngine(t)
	_, err := se.Get("does-not-exist-at-all")
	if err == nil {
		t.Error("expected error for missing key, got nil")
	}
}

func TestEngine_EmptyValue(t *testing.T) {
	se := newTestEngine(t)
	key := "empty-val"
	if err := se.Put(key, []byte{}, -1); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %d bytes", len(got))
	}
}

func TestEngine_UnicodeKey(t *testing.T) {
	se := newTestEngine(t)
	key := "日本語キー"
	val := []byte("unicode value")
	if err := se.Put(key, val, -1); err != nil {
		t.Fatalf("Put unicode: %v", err)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get unicode: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("unicode round-trip: got %q, want %q", got, val)
	}
}

func TestEngine_Overwrite(t *testing.T) {
	se := newTestEngine(t)
	key := "overwrite-key"
	if err := se.Put(key, []byte("v1"), -1); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := se.Put(key, []byte("v2"), -1); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("overwrite: got %q, want %q", got, "v2")
	}
}

// ── TTL ───────────────────────────────────────────────────────────────────────

func TestEngine_TTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TTL test in short mode")
	}
	se := newTestEngine(t)

	key := "ttl-key"
	// TTL of 1 second
	if err := se.Put(key, []byte("transient"), 1); err != nil {
		t.Fatalf("Put TTL=1: %v", err)
	}
	// Should be readable immediately.
	if _, err := se.Get(key); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	// Wait for TTL to expire.
	time.Sleep(1500 * time.Millisecond)
	// Evict from cache so Get falls through to the index TTL check.
	se.cache.Evict(key)
	// After expiry and cache eviction, the engine must return an error.
	_, err := se.Get(key)
	if err == nil {
		t.Error("expected error after TTL expiry, got nil")
	}
}

// ── Large value ───────────────────────────────────────────────────────────────

func TestEngine_LargeValue(t *testing.T) {
	se := newTestEngine(t)

	key := "large-key"
	val := make([]byte, 1<<20) // 1 MB
	for i := range val {
		val[i] = byte(i & 0xFF)
	}
	if err := se.Put(key, val, -1); err != nil {
		t.Fatalf("Put 1MB: %v", err)
	}
	got, err := se.Get(key)
	if err != nil {
		t.Fatalf("Get 1MB: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("large value round-trip failed: lengths got=%d want=%d", len(got), len(val))
	}
}

// ── MultiPut / MultiGet ───────────────────────────────────────────────────────

func TestEngine_MultiPut(t *testing.T) {
	se := newTestEngine(t)

	const n = 1000
	reqs := make([]MultiPutRequest, n)
	for i := 0; i < n; i++ {
		reqs[i] = MultiPutRequest{
			Key:   fmt.Sprintf("mp-key-%d", i),
			Value: []byte(fmt.Sprintf("mp-val-%d", i)),
			TTL:   -1,
		}
	}
	errs := se.MultiPut(reqs)
	for i, err := range errs {
		if err != nil {
			t.Errorf("MultiPut[%d] = %v", i, err)
		}
	}

	// Verify all keys are readable.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("mp-key-%d", i)
		want := []byte(fmt.Sprintf("mp-val-%d", i))
		got, err := se.Get(key)
		if err != nil {
			t.Errorf("Get mp-key-%d: %v", i, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get mp-key-%d = %q, want %q", i, got, want)
		}
	}
}

func TestEngine_MultiGet(t *testing.T) {
	se := newTestEngine(t)

	// Put some keys.
	for i := 0; i < 10; i++ {
		if err := se.Put(fmt.Sprintf("mg-%d", i), []byte(fmt.Sprintf("v%d", i)), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	keys := make([]string, 15)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("mg-%d", i)
	}
	// 5 missing keys.
	for i := 10; i < 15; i++ {
		keys[i] = fmt.Sprintf("missing-%d", i)
	}

	results := se.MultiGet(keys)
	if len(results) != 15 {
		t.Fatalf("MultiGet returned %d results, want 15", len(results))
	}

	// First 10 should be found.
	for i := 0; i < 10; i++ {
		r := results[i]
		if !r.Found || r.Err != nil {
			t.Errorf("MultiGet[%d]: expected found, got found=%v err=%v", i, r.Found, r.Err)
		}
		want := []byte(fmt.Sprintf("v%d", i))
		if !bytes.Equal(r.Value, want) {
			t.Errorf("MultiGet[%d] = %q, want %q", i, r.Value, want)
		}
	}

	// Last 5 should not be found.
	for i := 10; i < 15; i++ {
		r := results[i]
		if r.Found {
			t.Errorf("MultiGet[%d]: expected not-found, got found=true", i)
		}
		if r.Err == nil {
			t.Errorf("MultiGet[%d]: expected error for missing key, got nil", i)
		}
	}
}

// ── Crash recovery ────────────────────────────────────────────────────────────

func TestEngine_CrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "veltrix-crash-")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false

	// Phase 1: write keys.
	se1, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("new engine 1: %v", err)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("recover-key-%d", i)
		val := []byte(fmt.Sprintf("recover-val-%d", i))
		if err := se1.Put(key, val, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := se1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and verify.
	se2, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("new engine 2: %v", err)
	}
	t.Cleanup(func() { se2.Close() })

	// Wait for WAL replay to complete.
	<-se2.ReplayDone

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("recover-key-%d", i)
		want := []byte(fmt.Sprintf("recover-val-%d", i))
		got, err := se2.Get(key)
		if err != nil {
			t.Errorf("Get after recovery key=%s: %v", key, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("recovered key=%s got=%q want=%q", key, got, want)
		}
	}
}

// ── Multi-disk ────────────────────────────────────────────────────────────────

func TestEngine_MultiDisk(t *testing.T) {
	se := newTestEngine2Disks(t)

	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("multi-disk-key-%d", i)
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := se.Put(key, val, -1); err != nil {
			t.Fatalf("Put key %d: %v", i, err)
		}
	}

	// Verify all keys are readable (shard distribution across 2 disks is transparent).
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("multi-disk-key-%d", i)
		want := []byte(fmt.Sprintf("val-%d", i))
		got, err := se.Get(key)
		if err != nil {
			t.Errorf("Get multi-disk key %d: %v", i, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("multi-disk key %d: got %q want %q", i, got, want)
		}
	}

	// The index should contain exactly n keys across both disks.
	size := se.GetIndexSize()
	if size != n {
		t.Errorf("expected %d keys in index, got %d", n, size)
	}
}

// ── Concurrent access ─────────────────────────────────────────────────────────

func TestEngine_Concurrent(t *testing.T) {
	se := newTestEngine(t)

	const goroutines = 32
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines*opsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("conc-g%d-k%d", g, i)
				val := []byte(fmt.Sprintf("v-%d-%d", g, i))

				if err := se.Put(key, val, -1); err != nil {
					errCh <- fmt.Errorf("Put g=%d k=%d: %w", g, i, err)
					return
				}
				got, err := se.Get(key)
				if err != nil {
					errCh <- fmt.Errorf("Get g=%d k=%d: %w", g, i, err)
					return
				}
				if !bytes.Equal(got, val) {
					errCh <- fmt.Errorf("Get g=%d k=%d: got %q, want %q", g, i, got, val)
					return
				}
				if i%5 == 0 {
					if err := se.Delete(key); err != nil {
						errCh <- fmt.Errorf("Delete g=%d k=%d: %w", g, i, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// ── Version monotone ──────────────────────────────────────────────────────────

func TestEngine_VersionMonotone(t *testing.T) {
	se := newTestEngine(t)

	var prev uint64
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("ver-key-%d", i)
		if err := se.Put(key, []byte("v"), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		cur := se.GetVersion()
		if cur <= prev {
			t.Errorf("version not monotone: prev=%d cur=%d after %d puts", prev, cur, i+1)
		}
		prev = cur
	}
}

// ── Atomic operations ─────────────────────────────────────────────────────────

func TestEngine_CompareAndSwap(t *testing.T) {
	se := newTestEngine(t)

	key := "cas-key"
	v1 := []byte("v1")
	v2 := []byte("v2")

	if err := se.Put(key, v1, -1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Mismatch: expected "wrong" but actual is "v1".
	res, err := se.CompareAndSwap(key, []byte("wrong"), v2, -1)
	if err != nil {
		t.Fatalf("CAS mismatch: %v", err)
	}
	if res != CASMismatch {
		t.Errorf("CAS with wrong expected = %v, want CASMismatch", res)
	}

	// Value must still be v1.
	got, _ := se.Get(key)
	if !bytes.Equal(got, v1) {
		t.Errorf("after CASMismatch: got %q, want %q", got, v1)
	}

	// Success: swap v1 → v2.
	res, err = se.CompareAndSwap(key, v1, v2, -1)
	if err != nil {
		t.Fatalf("CAS success: %v", err)
	}
	if res != CASSuccess {
		t.Errorf("CAS with correct expected = %v, want CASSuccess", res)
	}

	got, _ = se.Get(key)
	if !bytes.Equal(got, v2) {
		t.Errorf("after CASSuccess: got %q, want %q", got, v2)
	}
}

func TestEngine_SetIfNotExists(t *testing.T) {
	se := newTestEngine(t)

	key := "setnx-key"
	val := []byte("setnx-val")

	// First call on a missing key must create it.
	res, err := se.SetIfNotExists(key, val, -1)
	if err != nil {
		t.Fatalf("SetNX first: %v", err)
	}
	if res != SetNXCreated {
		t.Errorf("first SetNX = %v, want SetNXCreated", res)
	}

	// Second call on an existing key must return Exists.
	res, err = se.SetIfNotExists(key, []byte("other"), -1)
	if err != nil {
		t.Fatalf("SetNX second: %v", err)
	}
	if res != SetNXExists {
		t.Errorf("second SetNX = %v, want SetNXExists", res)
	}

	// Value must still be original.
	got, _ := se.Get(key)
	if !bytes.Equal(got, val) {
		t.Errorf("after SetNXExists: got %q, want %q", got, val)
	}
}
