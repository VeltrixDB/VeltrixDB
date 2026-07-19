package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newPITRTestEngine creates an engine rooted at dir with continuous WAL
// archiving into archiveDir (fast 10 ms archive interval for tests).
func newPITRTestEngine(t *testing.T, dir, archiveDir string) (*StorageEngine, *WALArchiver) {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	cfg.BloomFilterShardBits = 1 << 18
	cfg.ArchiveDir = archiveDir
	cfg.ArchiveIntervalMs = 10

	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("new engine at %s: %v", dir, err)
	}
	arch, err := StartWALArchiver(se)
	if err != nil {
		se.Close()
		t.Fatalf("StartWALArchiver: %v", err)
	}
	if arch == nil {
		se.Close()
		t.Fatal("StartWALArchiver returned nil with ArchiveDir set")
	}
	return se, arch
}

// TestPITR_ArchiveSegmentsUnderWrites: writes with archiving on produce
// well-formed segments — sidecar bounds are sane, CRCs match the segment
// files, sequence numbers are monotonic, and every write is archived.
func TestPITR_ArchiveSegmentsUnderWrites(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	archiveDir := t.TempDir()
	se, arch := newPITRTestEngine(t, dataDir, archiveDir)
	defer se.Close()

	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		if err := se.Put(fmt.Sprintf("arch-key-%03d", i), []byte(fmt.Sprintf("val-%d", i)), -1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	arch.Stop() // final pass: everything durable is archived

	segs, err := ListArchiveSegments(archiveDir)
	if err != nil {
		t.Fatalf("ListArchiveSegments: %v", err)
	}
	if len(segs) == 0 {
		t.Fatal("no archive segments produced")
	}

	totalEntries := 0
	var lastSeq uint64
	var maxVersion uint64
	for _, s := range segs {
		if s.Seq <= lastSeq {
			t.Errorf("segment seq not monotonic: %d after %d", s.Seq, lastSeq)
		}
		lastSeq = s.Seq

		if s.FirstVersion > s.LastVersion {
			t.Errorf("seg %d: FirstVersion %d > LastVersion %d", s.Seq, s.FirstVersion, s.LastVersion)
		}
		if s.FirstUnixNs > s.LastUnixNs {
			t.Errorf("seg %d: FirstUnixNs %d > LastUnixNs %d", s.Seq, s.FirstUnixNs, s.LastUnixNs)
		}
		if s.LastVersion > maxVersion {
			maxVersion = s.LastVersion
		}

		data, err := os.ReadFile(s.SegPath)
		if err != nil {
			t.Fatalf("read segment %s: %v", s.SegPath, err)
		}
		if int64(len(data)) != s.SizeBytes {
			t.Errorf("seg %d: size %d != meta %d", s.Seq, len(data), s.SizeBytes)
		}
		if got := fmt.Sprintf("%08x", computeCRC32C(data)); got != s.CRC32C {
			t.Errorf("seg %d: CRC %s != meta %s", s.Seq, got, s.CRC32C)
		}

		// Segment must parse fully as self-contained records with values.
		recs, consumed := parseWALBuffer(data)
		if consumed != len(data) {
			t.Errorf("seg %d: only %d of %d bytes parse", s.Seq, consumed, len(data))
		}
		if len(recs) != s.Entries {
			t.Errorf("seg %d: parsed %d records, meta says %d", s.Seq, len(recs), s.Entries)
		}
		for _, r := range recs {
			if r.vlogOffset != 0 {
				t.Errorf("seg %d: record for %q not self-contained (vlogOffset=%d)", s.Seq, r.key, r.vlogOffset)
			}
			if !r.isTombstone && len(r.value) == 0 {
				t.Errorf("seg %d: record for %q has no inline value", s.Seq, r.key)
			}
		}
		totalEntries += s.Entries
	}

	if totalEntries < numKeys {
		t.Errorf("archived %d entries, want >= %d", totalEntries, numKeys)
	}
	if maxVersion < uint64(numKeys) {
		t.Errorf("archive max version %d < %d writes", maxVersion, numKeys)
	}
	if arch.EntriesArchived.Load() != uint64(totalEntries) {
		t.Errorf("EntriesArchived counter %d != sidecar sum %d", arch.EntriesArchived.Load(), totalEntries)
	}
}

// writeFakeArchiveSegment fabricates a segment + sidecar for pruning tests.
func writeFakeArchiveSegment(t *testing.T, diskDir string, seq uint64, size int, createdNs int64) {
	t.Helper()
	payload := make([]byte, size)
	meta := ArchiveSegmentMeta{
		DiskIdx:   0,
		Seq:       seq,
		CreatedNs: createdNs,
		Entries:   1,
		SizeBytes: int64(size),
		CRC32C:    fmt.Sprintf("%08x", computeCRC32C(payload)),
	}
	base := filepath.Join(diskDir, archiveSegName(seq))
	if err := os.WriteFile(base+".wal", payload, 0644); err != nil {
		t.Fatalf("write fake segment: %v", err)
	}
	data, _ := json.Marshal(&meta)
	if err := os.WriteFile(base+".json", data, 0644); err != nil {
		t.Fatalf("write fake sidecar: %v", err)
	}
}

// TestPITR_ArchivePruning: MaxArchiveBytes and MaxArchiveAgeSec retention
// remove oldest segments first and keep the rest intact.
func TestPITR_ArchivePruning(t *testing.T) {
	t.Parallel()

	// Bytes-based pruning: 5 × 100 B segments, cap 250 B → oldest 3 pruned.
	root := t.TempDir()
	diskDir := filepath.Join(root, "disk0")
	if err := os.MkdirAll(diskDir, 0755); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-time.Hour).UnixNano()
	for seq := uint64(1); seq <= 5; seq++ {
		writeFakeArchiveSegment(t, diskDir, seq, 100, base+int64(seq)*int64(time.Minute))
	}

	a := &WALArchiver{dir: root, maxBytes: 250}
	a.prune()

	segs, err := ListArchiveSegments(root)
	if err != nil {
		t.Fatalf("ListArchiveSegments: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("bytes pruning: want 2 surviving segments, got %d", len(segs))
	}
	if segs[0].Seq != 4 || segs[1].Seq != 5 {
		t.Errorf("bytes pruning kept wrong segments: seq %d, %d (want 4, 5)", segs[0].Seq, segs[1].Seq)
	}

	// Age-based pruning: 2 old + 2 recent segments, maxAge 30 min → old pruned.
	root2 := t.TempDir()
	diskDir2 := filepath.Join(root2, "disk0")
	if err := os.MkdirAll(diskDir2, 0755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	writeFakeArchiveSegment(t, diskDir2, 1, 50, now-2*int64(time.Hour))
	writeFakeArchiveSegment(t, diskDir2, 2, 50, now-int64(time.Hour))
	writeFakeArchiveSegment(t, diskDir2, 3, 50, now-int64(time.Minute))
	writeFakeArchiveSegment(t, diskDir2, 4, 50, now)

	a2 := &WALArchiver{dir: root2, maxAge: 30 * time.Minute}
	a2.prune()

	segs2, err := ListArchiveSegments(root2)
	if err != nil {
		t.Fatalf("ListArchiveSegments: %v", err)
	}
	if len(segs2) != 2 {
		t.Fatalf("age pruning: want 2 surviving segments, got %d", len(segs2))
	}
	if segs2[0].Seq != 3 || segs2[1].Seq != 4 {
		t.Errorf("age pruning kept wrong segments: seq %d, %d (want 3, 4)", segs2[0].Seq, segs2[1].Seq)
	}
	// Both .wal and .json of pruned segments must be gone.
	for _, seq := range []uint64{1, 2} {
		b := filepath.Join(diskDir2, archiveSegName(seq))
		if _, err := os.Stat(b + ".wal"); !os.IsNotExist(err) {
			t.Errorf("pruned segment %d .wal still exists", seq)
		}
		if _, err := os.Stat(b + ".json"); !os.IsNotExist(err) {
			t.Errorf("pruned segment %d .json still exists", seq)
		}
	}
}

// TestPITR_ParseTarget: --until syntax parsing.
func TestPITR_ParseTarget(t *testing.T) {
	t.Parallel()

	tgt, err := ParsePITRTarget("version:42")
	if err != nil || tgt.Version != 42 || !tgt.Time.IsZero() {
		t.Errorf("version:42 → %+v, %v", tgt, err)
	}
	tgt, err = ParsePITRTarget("2026-07-02T10:00:00Z")
	if err != nil || tgt.Version != 0 || tgt.Time.IsZero() {
		t.Errorf("RFC3339 → %+v, %v", tgt, err)
	}
	for _, bad := range []string{"", "version:", "version:0", "yesterday", "version:abc"} {
		if _, err := ParsePITRTarget(bad); err == nil {
			t.Errorf("ParsePITRTarget(%q): want error", bad)
		}
	}
}

// TestPITR_RestoreE2E: writes with archiving on → full backup midway → more
// writes (overwrites + deletes) → restore-pitr to a middle point → reopen →
// verify the exact mid-point state. Also covers timestamp targets, restore to
// the final version (tombstone replay), and the live-data-dir refusal.
func TestPITR_RestoreE2E(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	archiveDir := t.TempDir()
	se, arch := newPITRTestEngine(t, dataDir, archiveDir)

	const numKeys = 50
	key := func(i int) string { return fmt.Sprintf("pitr-key-%03d", i) }

	// Phase 1: initial values (versions 1..50).
	for i := 0; i < numKeys; i++ {
		if err := se.Put(key(i), []byte(fmt.Sprintf("v1-%d", i)), -1); err != nil {
			t.Fatalf("phase1 Put %d: %v", i, err)
		}
	}

	// Full base backup midway.
	backupDir := t.TempDir()
	be := NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// Phase 2: overwrite everything (versions 51..100).
	for i := 0; i < numKeys; i++ {
		if err := se.Put(key(i), []byte(fmt.Sprintf("v2-%d", i)), -1); err != nil {
			t.Fatalf("phase2 Put %d: %v", i, err)
		}
	}
	cutVersion := se.GetVersion()
	cutTime := time.Now()
	time.Sleep(20 * time.Millisecond) // keep phase-3 timestamps strictly after cutTime

	// Phase 3: overwrite half, delete some (versions 101..130).
	for i := 0; i < 25; i++ {
		if err := se.Put(key(i), []byte(fmt.Sprintf("v3-%d", i)), -1); err != nil {
			t.Fatalf("phase3 Put %d: %v", i, err)
		}
	}
	for i := 25; i < 30; i++ {
		if err := se.Delete(key(i)); err != nil {
			t.Fatalf("phase3 Delete %d: %v", i, err)
		}
	}
	finalVersion := se.GetVersion()

	arch.Stop()
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if cutVersion <= manifest.EngineVersion {
		t.Fatalf("test setup: cutVersion %d must be after base backup version %d", cutVersion, manifest.EngineVersion)
	}

	verify := func(t *testing.T, dir string, want func(i int) (string, bool)) {
		t.Helper()
		se2 := newTestEngineWithDir(t, dir)
		defer se2.Close()
		<-se2.ReplayDone
		for i := 0; i < numKeys; i++ {
			wantVal, exists := want(i)
			got, err := se2.Get(key(i))
			if !exists {
				if err == nil {
					t.Errorf("key %d: want deleted, got %q", i, string(got))
				}
				continue
			}
			if err != nil {
				t.Errorf("key %d: %v", i, err)
				continue
			}
			if string(got) != wantVal {
				t.Errorf("key %d: want %q, got %q", i, wantVal, string(got))
			}
		}
	}

	// Restore to the phase-2 boundary by VERSION: every key must be v2,
	// nothing from phase 3 (no v3 values, no deletes).
	restoreV := t.TempDir()
	applied, err := RestorePITR(backupDir, archiveDir, PITRTarget{Version: cutVersion}, []string{restoreV})
	if err != nil {
		t.Fatalf("RestorePITR (version): %v", err)
	}
	if applied != numKeys {
		t.Errorf("version restore: applied %d archived entries, want %d (phase-2 writes)", applied, numKeys)
	}
	verify(t, restoreV, func(i int) (string, bool) { return fmt.Sprintf("v2-%d", i), true })

	// Restore to the same point by TIMESTAMP.
	restoreT := t.TempDir()
	if _, err := RestorePITR(backupDir, archiveDir, PITRTarget{Time: cutTime}, []string{restoreT}); err != nil {
		t.Fatalf("RestorePITR (time): %v", err)
	}
	verify(t, restoreT, func(i int) (string, bool) { return fmt.Sprintf("v2-%d", i), true })

	// Restore to the FINAL version: phase-3 overwrites and deletes replayed.
	restoreF := t.TempDir()
	if _, err := RestorePITR(backupDir, archiveDir, PITRTarget{Version: finalVersion}, []string{restoreF}); err != nil {
		t.Fatalf("RestorePITR (final): %v", err)
	}
	verify(t, restoreF, func(i int) (string, bool) {
		switch {
		case i < 25:
			return fmt.Sprintf("v3-%d", i), true
		case i < 30:
			return "", false // deleted in phase 3
		default:
			return fmt.Sprintf("v2-%d", i), true
		}
	})

	// Refusal: the destination now contains engine files — a second restore
	// into it (as into any live/used data dir) must be rejected.
	if _, err := RestorePITR(backupDir, archiveDir, PITRTarget{Version: cutVersion}, []string{restoreV}); err == nil {
		t.Error("RestorePITR into a non-empty data dir: want refusal error, got nil")
	}

	// A target at or before the base backup is unreachable via PITR.
	if _, err := RestorePITR(backupDir, archiveDir, PITRTarget{Version: manifest.EngineVersion}, []string{t.TempDir()}); err == nil {
		t.Error("RestorePITR with target <= base version: want error, got nil")
	}
}
