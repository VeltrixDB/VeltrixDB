package storage

import (
	"errors"
	"testing"
)

// GetNoIO must answer everything Get answers without disk, report needIO for
// exactly the keys whose value only the VLog has, and GetNoIO+GetAfterNoIO
// must count as one read — the network front-end splits every cache-missing
// GET this way.
func TestGetNoIO(t *testing.T) {
	se, err := NewStorageEngine(testStorageConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { se.Close() })

	if err := se.Put("k", []byte("v1"), -1); err != nil {
		t.Fatal(err)
	}

	// Cached (Put is write-through): answered, no I/O.
	v, needIO, err := se.GetNoIO("k")
	if err != nil || needIO || string(v) != "v1" {
		t.Fatalf("cached: GetNoIO = %q, needIO=%v, %v; want \"v1\", false, nil", v, needIO, err)
	}

	// Absent: answered from the index, no I/O.
	if _, needIO, err := se.GetNoIO("missing"); needIO || !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("absent: needIO=%v err=%v; want false, ErrKeyNotFound", needIO, err)
	}

	// Only on disk: needIO, and nothing recorded until GetAfterNoIO.
	se.cache.Evict("k")
	reads, vlogReads := se.metrics.Reads.Load(), se.metrics.VLogReads.Load()
	v, needIO, err = se.GetNoIO("k")
	if err != nil || !needIO || v != nil {
		t.Fatalf("on disk: GetNoIO = %q, needIO=%v, %v; want nil, true, nil", v, needIO, err)
	}
	if got := se.metrics.Reads.Load(); got != reads {
		t.Fatalf("GetNoIO that needs I/O counted a read: %d → %d", reads, got)
	}
	if got := se.metrics.VLogReads.Load(); got != vlogReads {
		t.Fatalf("GetNoIO touched the VLog: VLogReads %d → %d", vlogReads, got)
	}
	v, err = se.GetAfterNoIO("k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("GetAfterNoIO = %q, %v; want \"v1\", nil", v, err)
	}
	if got := se.metrics.Reads.Load(); got != reads+1 {
		t.Fatalf("GetNoIO+GetAfterNoIO counted %d reads, want 1", got-reads)
	}
	if got := se.metrics.VLogReads.Load(); got != vlogReads+1 {
		t.Fatalf("GetAfterNoIO VLog reads = %d, want 1", got-vlogReads)
	}

	// GetAfterNoIO re-populated the cache: the next probe needs no I/O.
	if _, needIO, _ := se.GetNoIO("k"); needIO {
		t.Fatal("after GetAfterNoIO the key should be cached")
	}
}
