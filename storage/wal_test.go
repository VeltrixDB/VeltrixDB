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

// TestWAL_Serialization round-trips every record shape through both
// encodings: what the flusher writes, the replay decoder must return field
// for field.
func TestWAL_Serialization(t *testing.T) {
	entries := []*WALEntry{
		{Timestamp: 12345678, Key: "inline", Value: []byte("hello-world"), ValueLen: 11,
			Checksum: computeCRC32C([]byte("hello-world")), Version: 7},
		{Timestamp: 2, Key: "kvsep", ValueLen: 300, Checksum: 0xABCD, Version: 8,
			VLogOffset: 1 << 40, Packed: true, DiskValueLen: 120, XformFlags: FlagCompressed | FlagEncrypted},
		{Timestamp: 3, Key: "gone", IsTombstone: true, Version: 9},
		{Timestamp: 4, Key: "", Value: []byte("empty key"), ValueLen: 9,
			Checksum: computeCRC32C([]byte("empty key")), Version: 10},
	}
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacyText=%v", text), func(t *testing.T) {
			wal, dir := newTestWAL(t, 0 /*immediate*/, 1024)
			wal.legacyText = text
			for _, e := range entries {
				e.KeyLen = uint32(len(e.Key))
				if err := wal.append(e); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			got, err := replayWAL(filepath.Join(dir, "wal.log"))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(entries) {
				t.Fatalf("replayed %d records, want %d", len(got), len(entries))
			}
			for i, e := range entries {
				g := got[i]
				wantDisk := e.DiskValueLen
				if wantDisk == 0 {
					wantDisk = e.ValueLen
				}
				if g.key != e.Key || g.isTombstone != e.IsTombstone || g.version != e.Version ||
					g.timestampNs != e.Timestamp || g.crc != e.Checksum || g.valueLen != e.ValueLen ||
					g.vlogOffset != e.VLogOffset || g.packed != e.Packed || g.diskLen != wantDisk ||
					g.xflags != e.XformFlags || string(g.value) != string(e.Value) {
					t.Errorf("record %d:\n got %+v\nwant %+v", i, g, e)
				}
			}
		})
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

	// Decode rather than grep: records are binary, and a scan for '|' used to
	// trip over whatever the timestamp bytes happened to be (flaky).
	got, err := replayWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || !got[0].isTombstone || got[0].key != "dead-key" || got[0].version != 99 {
		t.Fatalf("replayed %+v, want one tombstone for dead-key at version 99", got)
	}
}
