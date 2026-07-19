package storage

import (
	"errors"
	"fmt"
	"testing"
)

// TestDiskHealth_BreakerTripsAndFailsFast: 5 consecutive errors trip the
// breaker; writes to that disk then fail fast with ErrDiskFailed and the node
// reports Degraded.
func TestDiskHealth_BreakerTripsAndFailsFast(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	defer se.Close()

	if se.Degraded() {
		t.Fatal("fresh engine reports degraded")
	}

	ioErr := errors.New("input/output error")
	for i := 0; i < diskFailThreshold-1; i++ {
		se.noteDiskError(0, "test", ioErr)
	}
	if se.diskIsFailed(0) {
		t.Fatal("breaker tripped before threshold")
	}
	// A success in between resets the streak.
	se.noteDiskOK(0)
	for i := 0; i < diskFailThreshold-1; i++ {
		se.noteDiskError(0, "test", ioErr)
	}
	if se.diskIsFailed(0) {
		t.Fatal("streak did not reset on success")
	}
	se.noteDiskError(0, "test", ioErr)
	if !se.diskIsFailed(0) {
		t.Fatal("breaker did not trip at threshold")
	}
	if !se.Degraded() {
		t.Fatal("Degraded() false with a failed disk")
	}
	if fd := se.FailedDisks(); len(fd) != 1 || fd[0] != 0 {
		t.Fatalf("FailedDisks = %v, want [0]", fd)
	}

	// Single-disk engine: every write routes to disk 0 and must fail fast.
	err = se.Put("k", []byte("v"), -1)
	if !errors.Is(err, ErrDiskFailed) {
		t.Fatalf("Put on failed disk: err = %v, want ErrDiskFailed", err)
	}
	if n := se.metrics.DiskFailures.Load(); n != 1 {
		t.Fatalf("DiskFailures metric = %d, want 1", n)
	}
}

// TestDiskHealth_ReadsStillServedFromCache: a failed disk must not take down
// cached reads.
func TestDiskHealth_ReadsStillServedFromCache(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	defer se.Close()

	for i := 0; i < 10; i++ {
		if err := se.Put(fmt.Sprintf("ck%d", i), []byte("cached"), -1); err != nil {
			t.Fatal(err)
		}
	}
	// Trip the breaker manually.
	for i := 0; i < diskFailThreshold; i++ {
		se.noteDiskError(0, "test", errors.New("io"))
	}
	// Values were cached by Put — reads must still succeed.
	for i := 0; i < 10; i++ {
		v, err := se.Get(fmt.Sprintf("ck%d", i))
		if err != nil || string(v) != "cached" {
			t.Fatalf("cached read after disk failure: v=%q err=%v", v, err)
		}
	}
}
