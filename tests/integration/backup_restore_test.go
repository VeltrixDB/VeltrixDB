package integration_test

// backup_restore_test.go — Integration tests for VeltrixDB backup and restore.
//
// All tests use the storage engine directly (no TCP server process) for speed
// and to exercise the backup/restore API at the storage layer.

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// newTestEngineInDir creates a StorageEngine in dir, registers a cleanup
// function that calls engine.Close(), and returns the engine.
func newTestEngineInDir(t *testing.T, dir string) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false

	se, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("newTestEngineInDir: %v", err)
	}
	<-se.ReplayDone
	return se
}

// mustMkdirTemp creates a temp dir and registers cleanup.
func mustMkdirTemp(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatalf("mkdirtemp %s: %v", prefix, err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestIntegration_FullBackupRestore writes 200 keys, takes a full backup,
// restores to a new data dir, and verifies all keys and values match exactly.
func TestIntegration_FullBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full backup/restore test in short mode")
	}

	srcDir := mustMkdirTemp(t, "veltrix-backup-src-")
	backupDir := mustMkdirTemp(t, "veltrix-backup-full-")
	restoreDir := mustMkdirTemp(t, "veltrix-backup-restore-")

	// ── Phase 1: write 200 keys ───────────────────────────────────────────────
	se := newTestEngineInDir(t, srcDir)

	const numKeys = 200
	written := make(map[string]string, numKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("backup-key-%04d", i)
		val := fmt.Sprintf("backup-val-%04d-padding-%s", i, makeAlphanumPadding(i%64))
		written[key] = val
		if err := se.Put(key, []byte(val), -1); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// ── Phase 2: full backup ──────────────────────────────────────────────────
	be := storage.NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	if manifest.NumDisks < 1 {
		t.Fatalf("manifest.NumDisks = %d, want >= 1", manifest.NumDisks)
	}
	if manifest.BackupID == "" {
		t.Fatal("manifest.BackupID is empty")
	}
	if manifest.Timestamp == 0 {
		t.Fatal("manifest.Timestamp is zero")
	}

	// Close source engine before restore (restore is offline-only).
	if err := se.Close(); err != nil {
		t.Fatalf("close src engine: %v", err)
	}

	// ── Phase 3: restore ─────────────────────────────────────────────────────
	if err := storage.Restore(
		[]*storage.BackupManifest{manifest},
		[]string{backupDir},
		[]string{restoreDir},
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// ── Phase 4: verify ───────────────────────────────────────────────────────
	se2 := newTestEngineInDir(t, restoreDir)
	defer se2.Close()

	for key, wantVal := range written {
		got, err := se2.Get(key)
		if err != nil {
			t.Errorf("get %s after restore: %v", key, err)
			continue
		}
		if string(got) != wantVal {
			t.Errorf("get %s: want %q, got %q", key, wantVal, got)
		}
	}
}

// TestIntegration_IncrementalBackupRestore writes 100 keys, takes a full
// backup, writes 50 more keys, takes an incremental backup, restores the chain,
// and verifies all 150 keys are readable.
func TestIntegration_IncrementalBackupRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping incremental backup/restore test in short mode")
	}

	srcDir := mustMkdirTemp(t, "veltrix-incr-src-")
	fullBackupDir := mustMkdirTemp(t, "veltrix-incr-full-")
	incrBackupDir := mustMkdirTemp(t, "veltrix-incr-incr-")
	restoreDir := mustMkdirTemp(t, "veltrix-incr-restore-")

	se := newTestEngineInDir(t, srcDir)

	// Write the first 100 keys.
	const base = 100
	written := make(map[string]string, 150)
	for i := 0; i < base; i++ {
		key := fmt.Sprintf("incr-key-%04d", i)
		val := fmt.Sprintf("incr-val-%04d", i)
		written[key] = val
		if err := se.Put(key, []byte(val), -1); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Full backup.
	be := storage.NewBackupEngine(se)
	fullManifest, err := be.FullBackup(fullBackupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// Write 50 more keys AFTER the full backup.
	const delta = 50
	for i := base; i < base+delta; i++ {
		key := fmt.Sprintf("incr-key-%04d", i)
		val := fmt.Sprintf("incr-val-%04d", i)
		written[key] = val
		if err := se.Put(key, []byte(val), -1); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Incremental backup relative to the full backup.
	incrManifest, err := be.IncrementalBackup(incrBackupDir, fullManifest)
	if err != nil {
		t.Fatalf("IncrementalBackup: %v", err)
	}

	if err := se.Close(); err != nil {
		t.Fatalf("close se: %v", err)
	}

	// Restore the full+incremental chain.
	if err := storage.Restore(
		[]*storage.BackupManifest{fullManifest, incrManifest},
		[]string{fullBackupDir, incrBackupDir},
		[]string{restoreDir},
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	se2 := newTestEngineInDir(t, restoreDir)
	defer se2.Close()

	for key, wantVal := range written {
		got, err := se2.Get(key)
		if err != nil {
			t.Errorf("get %s after incr restore: %v", key, err)
			continue
		}
		if string(got) != wantVal {
			t.Errorf("get %s: want %q, got %q", key, wantVal, got)
		}
	}
}

// TestIntegration_BackupWhileWriting runs 4 writer goroutines while a full
// backup is in progress, then restores and verifies that all keys written
// BEFORE the backup completed are readable.
func TestIntegration_BackupWhileWriting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent-write backup test in short mode")
	}

	srcDir := mustMkdirTemp(t, "veltrix-bww-src-")
	backupDir := mustMkdirTemp(t, "veltrix-bww-backup-")
	restoreDir := mustMkdirTemp(t, "veltrix-bww-restore-")

	se := newTestEngineInDir(t, srcDir)

	// Pre-write a known set of keys BEFORE starting the backup, so we can
	// definitively assert they survive the restore.
	const preKeys = 50
	preWritten := make(map[string]string, preKeys)
	for i := 0; i < preKeys; i++ {
		key := fmt.Sprintf("pre-key-%04d", i)
		val := fmt.Sprintf("pre-val-%04d", i)
		preWritten[key] = val
		if err := se.Put(key, []byte(val), -1); err != nil {
			t.Fatalf("pre-put %s: %v", key, err)
		}
	}

	// Start 4 concurrent writer goroutines.
	var wg sync.WaitGroup
	var writersDone atomic.Bool
	const writers = 4
	const keysPerWriter = 100

	for g := 0; g < writers; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < keysPerWriter; k++ {
				if writersDone.Load() {
					return
				}
				key := fmt.Sprintf("concurrent-g%d-k%04d", g, k)
				val := fmt.Sprintf("concurrent-val-%d-%04d", g, k)
				_ = se.Put(key, []byte(val), -1)
			}
		}()
	}

	// Run the backup while writes are in flight.
	be := storage.NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		writersDone.Store(true)
		wg.Wait()
		t.Fatalf("FullBackup during concurrent writes: %v", err)
	}

	// Stop writers.
	writersDone.Store(true)
	wg.Wait()

	if err := se.Close(); err != nil {
		t.Fatalf("close se: %v", err)
	}

	// Restore.
	if err := storage.Restore(
		[]*storage.BackupManifest{manifest},
		[]string{backupDir},
		[]string{restoreDir},
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	se2 := newTestEngineInDir(t, restoreDir)
	defer se2.Close()

	// All pre-backup keys must be readable.
	for key, wantVal := range preWritten {
		got, err := se2.Get(key)
		if err != nil {
			t.Errorf("get pre-key %s after restore: %v", key, err)
			continue
		}
		if string(got) != wantVal {
			t.Errorf("get pre-key %s: want %q, got %q", key, wantVal, got)
		}
	}
}

// TestIntegration_ManifestValidation creates a backup and verifies the manifest
// has non-zero NumDisks, a non-empty BackupID, and a non-zero Timestamp.
func TestIntegration_ManifestValidation(t *testing.T) {
	srcDir := mustMkdirTemp(t, "veltrix-manifest-src-")
	backupDir := mustMkdirTemp(t, "veltrix-manifest-backup-")

	se := newTestEngineInDir(t, srcDir)

	// Write a few keys.
	for i := 0; i < 10; i++ {
		if err := se.Put(fmt.Sprintf("m-key-%d", i), []byte("value"), -1); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	be := storage.NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("close se: %v", err)
	}

	// Verify in-memory manifest.
	if manifest.NumDisks < 1 {
		t.Errorf("manifest.NumDisks = %d, want >= 1", manifest.NumDisks)
	}
	if manifest.BackupID == "" {
		t.Errorf("manifest.BackupID is empty")
	}
	if manifest.Timestamp == 0 {
		t.Errorf("manifest.Timestamp is zero")
	}
	if manifest.Type != storage.BackupTypeFull {
		t.Errorf("manifest.Type = %q, want %q", manifest.Type, storage.BackupTypeFull)
	}

	// Verify round-trip through ReadManifest.
	m2, err := storage.ReadManifest(backupDir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m2.BackupID != manifest.BackupID {
		t.Errorf("ReadManifest BackupID mismatch: got %q, want %q", m2.BackupID, manifest.BackupID)
	}
	if m2.Timestamp != manifest.Timestamp {
		t.Errorf("ReadManifest Timestamp mismatch: got %d, want %d", m2.Timestamp, manifest.Timestamp)
	}
	if m2.NumDisks != manifest.NumDisks {
		t.Errorf("ReadManifest NumDisks mismatch: got %d, want %d", m2.NumDisks, manifest.NumDisks)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// makeAlphanumPadding returns a string of n repeating alphanumeric characters.
func makeAlphanumPadding(n int) string {
	if n <= 0 {
		return "x"
	}
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = alpha[i%len(alpha)]
	}
	return string(buf)
}
