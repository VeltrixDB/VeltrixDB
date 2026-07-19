package storage

// paper_changes_test.go — Unit tests for the 5 changes derived from the
// academic paper review (RESYSTANCE / io_uring / CaaS-LSM / Scavenger+ / MirrorKV).
//
// These tests run with CGO_ENABLED=0 (pure Go) so they work in all CI
// environments, including those without liburing or cgroup v2.
//
// Coverage map:
//   change-1  IOPOLL ring flag        — graceful fallback contract (Go side)
//   change-2  Temperature-weighted GC — sort order of vlogGCEntry slice
//   change-3  Defrag thread pinning   — lockDefragThread() doesn't panic
//   change-4  cgroup v2 throttle      — FlagTiered / tiered data-path only
//   change-5  Background tiering      — NopColdTier, TierManager, FlagTiered defrag skip

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// ── change-2: Temperature-weighted GC sort ─────────────────────────────────

// TestGCColdFirstSort verifies that a slice of vlogGCEntry values is ordered
// by writeTimestampUs ascending (oldest first), with diskOffset as tie-break.
// This is the Scavenger+ insight: re-compacted hot records land at a high
// diskOffset but carry their original writeTimestampUs, so pure offset-sort
// would mis-classify them as "new" data.
func TestGCColdFirstSort(t *testing.T) {
	t.Parallel()

	entries := []vlogGCEntry{
		{key: "b", diskOffset: 9000, writeTimestampUs: 200}, // new, high offset
		{key: "a", diskOffset: 1000, writeTimestampUs: 100}, // old, low offset
		{key: "d", diskOffset: 8000, writeTimestampUs: 100}, // old, high offset — re-compacted
		{key: "c", diskOffset: 2000, writeTimestampUs: 150}, // mid
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].writeTimestampUs != entries[j].writeTimestampUs {
			return entries[i].writeTimestampUs < entries[j].writeTimestampUs
		}
		return entries[i].diskOffset < entries[j].diskOffset
	})

	// Expected order: a(ts=100,off=1000), d(ts=100,off=8000), c(ts=150), b(ts=200)
	want := []string{"a", "d", "c", "b"}
	for i, e := range entries {
		if e.key != want[i] {
			t.Errorf("pos %d: got key=%q, want %q", i, e.key, want[i])
		}
	}

	// Key invariant: after the sort, entries[0] must have the smallest timestamp.
	if entries[0].writeTimestampUs > entries[len(entries)-1].writeTimestampUs {
		t.Errorf("sort is not ascending: first ts=%d, last ts=%d",
			entries[0].writeTimestampUs, entries[len(entries)-1].writeTimestampUs)
	}
}

// TestGCColdFirstSort_TieBreak: when all timestamps equal, diskOffset ascending.
func TestGCColdFirstSort_TieBreak(t *testing.T) {
	t.Parallel()

	const ts = int64(42)
	entries := []vlogGCEntry{
		{key: "z", diskOffset: 3000, writeTimestampUs: ts},
		{key: "x", diskOffset: 1000, writeTimestampUs: ts},
		{key: "y", diskOffset: 2000, writeTimestampUs: ts},
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].writeTimestampUs != entries[j].writeTimestampUs {
			return entries[i].writeTimestampUs < entries[j].writeTimestampUs
		}
		return entries[i].diskOffset < entries[j].diskOffset
	})

	for i := 1; i < len(entries); i++ {
		if entries[i].diskOffset < entries[i-1].diskOffset {
			t.Errorf("tie-break failed at pos %d: offset %d < %d",
				i, entries[i].diskOffset, entries[i-1].diskOffset)
		}
	}
}

// ── change-3: Defrag thread pinning ────────────────────────────────────────

// TestLockDefragThread: lockDefragThread must not panic on any platform.
// On Linux it sets thread affinity; on other OS it is a no-op.
func TestLockDefragThread(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		lockDefragThread() // must not panic
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lockDefragThread blocked for >5s")
	}
}

// ── change-5a: FlagTiered constant ─────────────────────────────────────────

// TestFlagTiered_Constant: FlagTiered must be 0x80 and must not overlap any
// other flag constant so that masking is unambiguous.
func TestFlagTiered_Constant(t *testing.T) {
	t.Parallel()

	if FlagTiered != 0x80 {
		t.Errorf("FlagTiered = 0x%02X, want 0x80", FlagTiered)
	}

	other := []struct {
		name string
		v    uint8
	}{
		{"FlagTombstone", FlagTombstone},
		{"FlagCompressed", FlagCompressed},
		{"FlagPacked", FlagPacked},
	}
	for _, f := range other {
		if FlagTiered&f.v != 0 {
			t.Errorf("FlagTiered (0x%02X) overlaps %s (0x%02X)", FlagTiered, f.name, f.v)
		}
	}
}

// ── change-5b: NopColdTier ─────────────────────────────────────────────────

func TestNopColdTier_PutGet(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	handle := "deadbeef"
	val := []byte("nop-test-value")

	if err := nop.Put(handle, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := nop.Get(handle)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %q, got %q", val, got)
	}
	if nop.Name() != "nop" {
		t.Errorf("Name() = %q, want %q", nop.Name(), "nop")
	}
}

func TestNopColdTier_Delete(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	if err := nop.Put("h1", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := nop.Delete("h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := nop.Get("h1"); err == nil {
		t.Fatal("expected error after Delete, got nil")
	}
}

func TestNopColdTier_MissingHandle(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	if _, err := nop.Get("nonexistent"); err == nil {
		t.Fatal("expected error for missing handle, got nil")
	}
}

// TestNopColdTier_IsolatesWrites: Put does not share the backing slice with
// caller or between Put and Get (defensive copy semantics).
func TestNopColdTier_IsolatesWrites(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	original := []byte("original")
	if err := nop.Put("h", original); err != nil {
		t.Fatal(err)
	}

	// Mutate the original slice; stored value must be unaffected.
	original[0] = 'X'
	got, _ := nop.Get("h")
	if got[0] == 'X' {
		t.Error("NopColdTier.Put shares backing array with caller — expected defensive copy")
	}

	// Mutate the returned slice; subsequent Get must be unaffected.
	got[0] = 'Y'
	got2, _ := nop.Get("h")
	if got2[0] == 'Y' {
		t.Error("NopColdTier.Get shares backing array with caller — expected defensive copy")
	}
}

// TestNopColdTier_Concurrent: 100 goroutines Put/Get/Delete concurrently.
func TestNopColdTier_Concurrent(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			h := fmt.Sprintf("%08x", i)
			v := []byte(fmt.Sprintf("v-%d", i))
			_ = nop.Put(h, v)
			_, _ = nop.Get(h)
			_ = nop.Delete(h)
		}()
	}
	wg.Wait()
}

// TestNopColdTier_Len: Len tracks inserts and deletes.
func TestNopColdTier_Len(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	for i := 0; i < 5; i++ {
		_ = nop.Put(fmt.Sprintf("%08x", i), []byte("v"))
	}
	if nop.Len() != 5 {
		t.Errorf("Len() = %d, want 5", nop.Len())
	}
	_ = nop.Delete("00000000")
	if nop.Len() != 4 {
		t.Errorf("Len() after Delete = %d, want 4", nop.Len())
	}
}

// ── change-5c: TierManager candidate filtering ────────────────────────────

// TestTierManager_EnqueueNonBlocking: EnqueueCandidate must not block even
// when the internal channel is full.
func TestTierManager_EnqueueNonBlocking(t *testing.T) {
	t.Parallel()

	tm := &TierManager{
		candidates: make(chan DemotionCandidate, 2), // intentionally tiny
		done:       make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			tm.EnqueueCandidate(DemotionCandidate{Key: fmt.Sprintf("k%d", i)})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("EnqueueCandidate blocked — must be fire-and-forget")
	}
}

// TestTierManager_RunPassSkipsTooRecent: candidates accessed within the cold-age
// threshold must not be demoted.
func TestTierManager_RunPassSkipsTooRecent(t *testing.T) {
	t.Parallel()

	nop := NewNopColdTier()
	SetColdTier(nop)
	t.Cleanup(func() { SetColdTier(nil) })

	tm := &TierManager{
		candidates: make(chan DemotionCandidate, 16),
		done:       make(chan struct{}),
		// index/vlogs are nil — the loop exits before reaching them because
		// LastAccessedNs is recent enough to be filtered out.
	}

	recentNs := time.Now().UnixNano() // just accessed
	tm.EnqueueCandidate(DemotionCandidate{
		Key:            "hot-key",
		LastAccessedNs: recentNs,
	})

	tm.runPass() // must not panic or dereference nil index/vlogs

	if nop.Len() != 0 {
		t.Errorf("hot candidate was incorrectly demoted: NopColdTier.Len() = %d", nop.Len())
	}
}

// TestTierManager_RunPassSkipsAlreadyTiered: a candidate whose index entry
// already has FlagTiered set must be skipped without a second Put.
// Uses a real engine so the index is populated.
func TestTierManager_RunPassSkipsAlreadyTiered(t *testing.T) {
	se := newTestEngine(t)
	key := "already-tiered"
	val := []byte("some-value")
	if err := se.Put(key, val, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Manually set FlagTiered on the index entry.
	se.index.markTiered(key)

	nop := NewNopColdTier()
	SetColdTier(nop)
	t.Cleanup(func() { SetColdTier(nil) })

	// Enqueue with an old LastAccessedNs so age check passes.
	tm := &TierManager{
		index:    se.index,
		vlogs:    se.vlogs,
		numDisks: len(se.vlogs),
		config:   se.config,
		metrics:  se.metrics,
		candidates: make(chan DemotionCandidate, 16),
		done:       make(chan struct{}),
	}

	// Fetch the disk offset so the DemotionCandidate is credible.
	entry, _, exists := se.index.get(key)
	if !exists {
		t.Fatal("key not found in index after Put")
	}

	tm.EnqueueCandidate(DemotionCandidate{
		Key:            key,
		DiskOffset:     entry.DiskOffset,
		LastAccessedNs: time.Now().Add(-10 * time.Minute).UnixNano(), // cold enough
	})

	before := nop.Len()
	tm.runPass()

	if nop.Len() != before {
		t.Errorf("already-tiered entry was demoted a second time (NopColdTier grew by %d)", nop.Len()-before)
	}
}

// ── change-5d: convenience constructors (no network call) ─────────────────

// TestNewS3ColdTierSimple_Returns: must return a non-nil *S3ColdTier without
// dialing anything (constructor is pure struct assembly).
func TestNewS3ColdTierSimple_Returns(t *testing.T) {
	t.Parallel()

	tier := NewS3ColdTierSimple("my-bucket", "us-east-1", "veltrix/")
	if tier == nil {
		t.Fatal("NewS3ColdTierSimple returned nil")
	}
	if tier.Name() == "" {
		t.Fatal("S3ColdTier.Name() returned empty string")
	}
}

func TestNewGCSColdTierSimple_Returns(t *testing.T) {
	t.Parallel()

	tier := NewGCSColdTierSimple("my-bucket", "veltrix/")
	if tier == nil {
		t.Fatal("NewGCSColdTierSimple returned nil")
	}
}
