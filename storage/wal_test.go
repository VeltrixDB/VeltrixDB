package storage

// wal_test.go — unit tests for the WAL group-commit behaviour.
//
// Tests exercise the WriteAheadLog in isolation (no full StorageEngine) by
// calling newWriteAheadLog directly and submitting WALEntry values.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestWAL creates a temporary WAL and registers cleanup.
func newTestWAL(t *testing.T, flushWindow time.Duration, maxBatch int) (*WriteAheadLog, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "veltrix-wal-")
	if err != nil {
		t.Fatalf("tmp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	var flushCount atomic.Uint64
	wal, err := newWriteAheadLog(dir, &flushCount, flushWindow, maxBatch, 0)
	if err != nil {
		t.Fatalf("newWriteAheadLog: %v", err)
	}
	t.Cleanup(func() { wal.close() })
	return wal, dir
}

// dummyEntry returns a minimal WALEntry for testing.
func dummyEntry(key string, value []byte) *WALEntry {
	return &WALEntry{
		Timestamp:   time.Now().UnixNano(),
		Key:         key,
		KeyLen:      uint32(len(key)),
		Value:       value,
		ValueLen:    uint32(len(value)),
		Checksum:    computeCRC32C(value),
		Version:     1,
		IsTombstone: false,
	}
}

// TestWAL_GroupCommit verifies that concurrent appends share a single fdatasync.
//
// With a 5 ms window and 50 goroutines each appending one entry, the flusher
// should batch most of them into far fewer fdatasyncs than the entry count.
func TestWAL_GroupCommit(t *testing.T) {
	const n = 50
	flushWindow := 5 * time.Millisecond
	// Build a WAL with an explicit counter we can inspect.
	dir, err := os.MkdirTemp("", "veltrix-wal-gc-")
	if err != nil {
		t.Fatalf("tmp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	var flushCount atomic.Uint64
	wal, err := newWriteAheadLog(dir, &flushCount, flushWindow, 1024, 0)
	if err != nil {
		t.Fatalf("newWriteAheadLog: %v", err)
	}
	t.Cleanup(func() { wal.close() })

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			e := dummyEntry(fmt.Sprintf("key-%d", i), []byte("value"))
			if err := wal.append(e); err != nil {
				t.Errorf("append %d: %v", i, err)
			}
		}()
	}
	wg.Wait()

	flushes := flushCount.Load()
	// All entries written.
	_, entries := wal.GetStats()
	if entries != n {
		t.Errorf("expected %d entries written, got %d", n, entries)
	}
	// Group commit: the flusher should have batched writes into significantly fewer
	// fdatasyncs than 50.  In practice the 5 ms window collapses 50 concurrent
	// goroutines into 1-5 flushes.  We allow up to n/2 as a conservative bound.
	if flushes >= n/2 {
		t.Errorf("group commit not working: %d flushes for %d entries (expected << %d)", flushes, n, n)
	}
}

// TestWAL_ImmediateMode verifies that flushWindow=0 flushes each batch
// immediately (no deliberate wait for stragglers).  Because the channel is
// drained on each flush, sequential appends produce one fdatasync each.
func TestWAL_ImmediateMode(t *testing.T) {
	const n = 10
	dir, err := os.MkdirTemp("", "veltrix-wal-imm-")
	if err != nil {
		t.Fatalf("tmp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	var flushCount atomic.Uint64
	wal, err := newWriteAheadLog(dir, &flushCount, 0 /*immediate*/, 1024, 0)
	if err != nil {
		t.Fatalf("newWriteAheadLog: %v", err)
	}
	t.Cleanup(func() { wal.close() })

	// Sequential appends — each one should complete its own flush.
	for i := 0; i < n; i++ {
		e := dummyEntry(fmt.Sprintf("imm-key-%d", i), []byte("val"))
		if err := wal.append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Every sequential append in immediate mode triggers its own fdatasync.
	flushes := flushCount.Load()
	if flushes < n {
		t.Errorf("immediate mode: expected at least %d flushes, got %d", n, flushes)
	}
}

// TestWAL_Serialization writes a single entry and verifies the WAL file
// contains the expected pipe-delimited fields.
func TestWAL_Serialization(t *testing.T) {
	wal, dir := newTestWAL(t, 0 /*immediate*/, 1024)

	key := "serialize-test-key"
	value := []byte("hello-world")
	entry := &WALEntry{
		Timestamp:   12345678,
		Key:         key,
		KeyLen:      uint32(len(key)),
		Value:       value,
		ValueLen:    uint32(len(value)),
		Checksum:    computeCRC32C(value),
		Version:     7,
		IsTombstone: false,
	}
	if err := wal.append(entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}

	content := string(data)
	// The WAL file must contain the key and the "7|" version.
	for _, want := range []string{key, "|7|"} {
		found := false
		for i := 0; i <= len(content)-len(want); i++ {
			if content[i:i+len(want)] == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("WAL missing %q in:\n%s", want, content)
		}
	}
}

// TestWAL_ConcurrentWrites verifies that 100 goroutines each writing 100
// entries all succeed and the total entry count is correct.
func TestWAL_ConcurrentWrites(t *testing.T) {
	wal, _ := newTestWAL(t, 2*time.Millisecond, 4096)

	const goroutines = 100
	const perGoroutine = 100
	total := goroutines * perGoroutine

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, total)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				e := dummyEntry(key, []byte("v"))
				if err := wal.append(e); err != nil {
					errCh <- fmt.Errorf("goroutine %d entry %d: %w", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	_, entries := wal.GetStats()
	if entries != uint64(total) {
		t.Errorf("expected %d entries, got %d", total, entries)
	}
}

// TestWAL_GetStats verifies that GetStats returns non-zero bytes and entries
// after writing.
func TestWAL_GetStats(t *testing.T) {
	wal, _ := newTestWAL(t, 0, 1024)

	e := dummyEntry("stats-key", []byte("stats-value"))
	if err := wal.append(e); err != nil {
		t.Fatalf("append: %v", err)
	}

	bytes, entries := wal.GetStats()
	if entries == 0 {
		t.Error("expected entries > 0")
	}
	if bytes == 0 {
		t.Error("expected bytes > 0")
	}
}

// TestWAL_TombstoneEntry verifies that a tombstone entry is accepted and
// serialized without error.
func TestWAL_TombstoneEntry(t *testing.T) {
	wal, dir := newTestWAL(t, 0, 1024)

	e := &WALEntry{
		Timestamp:   time.Now().UnixNano(),
		Key:         "dead-key",
		KeyLen:      8,
		IsTombstone: true,
		Version:     99,
	}
	if err := wal.append(e); err != nil {
		t.Fatalf("tombstone append: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Tombstone field is the second pipe-delimited field and must be "1".
	content := string(data)
	if len(content) < 3 {
		t.Fatalf("WAL file too short: %q", content)
	}
	// Find first '|' and check the byte after it is '1'.
	for i, ch := range content {
		if ch == '|' {
			if i+1 < len(content) && content[i+1] != '1' {
				t.Errorf("tombstone field should be '1', got '%c' in %q", content[i+1], content)
			}
			break
		}
	}
}
