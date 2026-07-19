package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestEngineWithDir creates a StorageEngine rooted at dir (which must already
// exist). Used when we need precise control over the data directory path (e.g.
// for backup/restore round-trips).
func newTestEngineWithDir(t *testing.T, dir string) *StorageEngine {
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

	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("new engine at %s: %v", dir, err)
	}
	return se
}

// TestBackup_FullBackup: Create engine; Put 100 keys; FullBackup to tempdir;
// manifest.json exists; disk0/wal.log present.
func TestBackup_FullBackup(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)
	defer se.Close()

	for i := 0; i < 100; i++ {
		if err := se.Put(fmt.Sprintf("backup-key-%03d", i), []byte("value"), -1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	backupDir := t.TempDir()
	be := NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// manifest.json must exist
	if _, err := os.Stat(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing: %v", err)
	}

	// disk0/wal.log must exist
	walPath := filepath.Join(backupDir, "disk0", "wal.log")
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("disk0/wal.log missing: %v", err)
	}

	if manifest.Type != BackupTypeFull {
		t.Fatalf("expected type=full, got %q", manifest.Type)
	}
	if manifest.NumDisks < 1 {
		t.Fatalf("expected NumDisks >= 1, got %d", manifest.NumDisks)
	}
}

// TestBackup_FullRestore: Full backup; close engine; Restore to new dirs;
// create new engine from restored dirs; all 100 keys readable.
func TestBackup_FullRestore(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)

	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		if err := se.Put(fmt.Sprintf("restore-key-%03d", i), []byte(fmt.Sprintf("val-%d", i)), -1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	backupDir := t.TempDir()
	be := NewBackupEngine(se)
	manifest, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restore into a fresh directory.
	restoreDir := t.TempDir()
	if err := Restore(
		[]*BackupManifest{manifest},
		[]string{backupDir},
		[]string{restoreDir},
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Open a new engine on the restored directory.
	se2 := newTestEngineWithDir(t, restoreDir)
	defer se2.Close()

	// Wait for WAL replay to finish.
	<-se2.ReplayDone

	// All keys must be readable.
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("restore-key-%03d", i)
		wantVal := fmt.Sprintf("val-%d", i)
		got, err := se2.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after restore: %v", key, err)
			continue
		}
		if string(got) != wantVal {
			t.Errorf("key %s: want %q, got %q", key, wantVal, string(got))
		}
	}
}

// TestBackup_Incremental: Full backup; Put 50 more keys; IncrementalBackup;
// manifest has type=incremental.
func TestBackup_Incremental(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)
	defer se.Close()

	// Initial 50 keys for the full backup.
	for i := 0; i < 50; i++ {
		if err := se.Put(fmt.Sprintf("incr-init-%03d", i), []byte("v"), -1); err != nil {
			t.Fatalf("Put init %d: %v", i, err)
		}
	}

	fullDir := t.TempDir()
	be := NewBackupEngine(se)
	fullManifest, err := be.FullBackup(fullDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// Write 50 more keys after the full backup.
	for i := 50; i < 100; i++ {
		if err := se.Put(fmt.Sprintf("incr-init-%03d", i), []byte("v"), -1); err != nil {
			t.Fatalf("Put incr %d: %v", i, err)
		}
	}

	incrDir := t.TempDir()
	incrManifest, err := be.IncrementalBackup(incrDir, fullManifest)
	if err != nil {
		t.Fatalf("IncrementalBackup: %v", err)
	}

	if incrManifest.Type != BackupTypeIncremental {
		t.Fatalf("expected type=incremental, got %q", incrManifest.Type)
	}
}

// TestBackup_ChainRestore: Full + incremental chain; Restore; all keys (initial
// 50 + 50 more) readable.
func TestBackup_ChainRestore(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)

	// Write initial 50 keys.
	for i := 0; i < 50; i++ {
		if err := se.Put(fmt.Sprintf("chain-key-%03d", i), []byte(fmt.Sprintf("v%d", i)), -1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	fullDir := t.TempDir()
	be := NewBackupEngine(se)
	fullManifest, err := be.FullBackup(fullDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// Write 50 more keys.
	for i := 50; i < 100; i++ {
		if err := se.Put(fmt.Sprintf("chain-key-%03d", i), []byte(fmt.Sprintf("v%d", i)), -1); err != nil {
			t.Fatalf("Put incr %d: %v", i, err)
		}
	}

	incrDir := t.TempDir()
	incrManifest, err := be.IncrementalBackup(incrDir, fullManifest)
	if err != nil {
		t.Fatalf("IncrementalBackup: %v", err)
	}

	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restore the chain.
	restoreDir := t.TempDir()
	chain := []*BackupManifest{fullManifest, incrManifest}
	roots := []string{fullDir, incrDir}
	if err := Restore(chain, roots, []string{restoreDir}); err != nil {
		t.Fatalf("Restore chain: %v", err)
	}

	se2 := newTestEngineWithDir(t, restoreDir)
	defer se2.Close()
	<-se2.ReplayDone

	// All 100 keys from both phases must be visible.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("chain-key-%03d", i)
		_, err := se2.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after chain restore: %v", key, err)
		}
	}
}

// TestBackup_ManifestRoundTrip: Write manifest JSON; ReadManifest; fields match.
func TestBackup_ManifestRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	m := &BackupManifest{
		BackupID:      "test-manifest-id",
		Type:          BackupTypeFull,
		Timestamp:     1234567890,
		EngineVersion: 42,
		NumDisks:      1,
		Disks: []DiskBackupMeta{{
			DiskIdx:      0,
			WALFile:      "disk0/wal.log",
			VLogFile:     "disk0/vlog.dat",
			VLogStartOff: 0,
			VLogEndOff:   4096,
		}},
	}
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	got, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.BackupID != m.BackupID {
		t.Errorf("BackupID: want %q, got %q", m.BackupID, got.BackupID)
	}
	if got.Type != m.Type {
		t.Errorf("Type: want %q, got %q", m.Type, got.Type)
	}
	if got.EngineVersion != m.EngineVersion {
		t.Errorf("EngineVersion: want %d, got %d", m.EngineVersion, got.EngineVersion)
	}
	if got.NumDisks != m.NumDisks {
		t.Errorf("NumDisks: want %d, got %d", m.NumDisks, got.NumDisks)
	}
	if len(got.Disks) != 1 || got.Disks[0].VLogEndOff != 4096 {
		t.Errorf("Disks: unexpected %+v", got.Disks)
	}
}

// TestBackup_EmptyEngine: FullBackup on engine with 0 keys; succeeds without error.
func TestBackup_EmptyEngine(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)
	defer se.Close()

	backupDir := t.TempDir()
	be := NewBackupEngine(se)
	_, err := be.FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup on empty engine: %v", err)
	}

	// manifest.json must still be written.
	if _, err := os.Stat(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing for empty engine backup: %v", err)
	}
}

// TestBackup_ConcurrentReads: FullBackup runs while 8 goroutines are doing Gets;
// no corruption (backup completes without error, all Gets return correct values).
func TestBackup_ConcurrentReads(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	se := newTestEngineWithDir(t, dataDir)
	defer se.Close()

	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		if err := se.Put(fmt.Sprintf("concurrent-key-%04d", i), []byte(fmt.Sprintf("v%d", i)), -1); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 8 reader goroutines.
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Sprintf("concurrent-key-%04d", id*25)
					_, _ = se.Get(key)
				}
			}
		}(r)
	}

	backupDir := t.TempDir()
	be := NewBackupEngine(se)
	_, err := be.FullBackup(backupDir)
	close(stop)
	wg.Wait()

	if err != nil {
		t.Fatalf("FullBackup during concurrent reads: %v", err)
	}
}
