package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Regressions for the single-engine-per-data-dir lock (dirlock.go).
//
// Before the lock, veltrixdb-backup / veltrix-repair pointed at a live
// server's --data-dirs opened a second engine on the same files, and the live
// wal.log and vlog_active.dat were truncated to 0 bytes.

// abandonEngineForCrashTest releases se's data-dir locks WITHOUT closing it,
// so a test can open a second engine on the same dir to simulate a process
// kill (the kernel drops a dead process's flocks). Only for "dirty restart"
// tests where se1 is idle and never used again.
func abandonEngineForCrashTest(se *StorageEngine) {
	releaseDirLocks(se.dirLocks)
	se.dirLocks = nil
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

func TestDataDirLock_SecondEngineOnSameDirRefused(t *testing.T) {
	dir := t.TempDir()
	se1, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatalf("first NewStorageEngine: %v", err)
	}
	<-se1.ReplayDone

	want := map[string][]byte{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("live-key-%d", i)
		v := []byte(fmt.Sprintf("live-value-%d", i))
		if err := se1.Put(k, v, -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	walPath := filepath.Join(dir, "wal.log")
	vlogPath := filepath.Join(dir, "vlog_active.dat")
	walBefore, vlogBefore := fileSize(t, walPath), fileSize(t, vlogPath)
	if walBefore == 0 || vlogBefore == 0 {
		t.Fatalf("precondition: wal=%d vlog=%d bytes, want both > 0", walBefore, vlogBefore)
	}

	se2, err := NewStorageEngine(testStorageConfig(dir))
	if err == nil {
		<-se2.ReplayDone
		se2.Close()
		t.Fatalf("second NewStorageEngine on a live data dir succeeded; want ErrDataDirLocked "+
			"(wal.log %d -> %d bytes, vlog_active.dat %d -> %d bytes)",
			walBefore, fileSize(t, walPath), vlogBefore, fileSize(t, vlogPath))
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("second NewStorageEngine error = %v, want errors.Is(ErrDataDirLocked)", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Errorf("error %q does not name the locked dir %s", err, dir)
	}

	// The refused open must not have touched the live files.
	if got := fileSize(t, walPath); got != walBefore {
		t.Errorf("wal.log changed size %d -> %d after a refused open", walBefore, got)
	}
	if got := fileSize(t, vlogPath); got != vlogBefore {
		t.Errorf("vlog_active.dat changed size %d -> %d after a refused open", vlogBefore, got)
	}
	// And the live engine keeps working.
	for k, v := range want {
		se1.cache.Evict(k) // force the VLog read path
		got, err := se1.Get(k)
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("live engine Get %s = %q, %v; want %q", k, got, err, v)
		}
	}
	if err := se1.Put("after-refusal", []byte("ok"), -1); err != nil {
		t.Fatalf("live engine Put after refusal: %v", err)
	}

	// Close releases the lock: a reopen (restart) must succeed and see the data.
	if err := se1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	se3, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatalf("reopen after Close: %v", err)
	}
	t.Cleanup(func() { se3.Close() })
	<-se3.ReplayDone
	for k, v := range want {
		got, err := se3.Get(k)
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("after reopen Get %s = %q, %v; want %q", k, got, err, v)
		}
	}
}

// A multi-disk open that fails on its second dir must release the lock it
// already took on the first, or that dir stays unopenable for the process.
func TestDataDirLock_PartialFailureReleasesEarlierDirs(t *testing.T) {
	a, b, c := t.TempDir(), t.TempDir(), t.TempDir()
	seAB, err := NewStorageEngine(testStorageConfig(a, b))
	if err != nil {
		t.Fatalf("open [a b]: %v", err)
	}
	<-seAB.ReplayDone
	t.Cleanup(func() { seAB.Close() })

	if se, err := NewStorageEngine(testStorageConfig(c, b)); err == nil {
		se.Close()
		t.Fatal("open [c b] succeeded while b is held")
	} else if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("open [c b] error = %v, want ErrDataDirLocked", err)
	}

	seC, err := NewStorageEngine(testStorageConfig(c))
	if err != nil {
		t.Fatalf("open [c] after the failed [c b] open: %v — lock on c leaked", err)
	}
	<-seC.ReplayDone
	seC.Close()
}

// The same directory listed twice is a misconfiguration (two "disks" sharing
// one wal.log); the lock turns it into a startup error.
func TestDataDirLock_DuplicateDirRejected(t *testing.T) {
	d := t.TempDir()
	se, err := NewStorageEngine(testStorageConfig(d, d))
	if err == nil {
		se.Close()
		t.Fatal("NewStorageEngine with the same dir twice succeeded")
	}
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("error = %v, want ErrDataDirLocked", err)
	}
}

// Restore writes wal.log / vlog_active.dat into its destination dirs; doing
// that under a running engine would corrupt it, so Restore takes the lock too.
func TestDataDirLock_RestoreIntoLiveDirRefused(t *testing.T) {
	src := t.TempDir()
	se, err := NewStorageEngine(testStorageConfig(src))
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	<-se.ReplayDone
	if err := se.Put("k", []byte("v"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	backupDir := t.TempDir()
	m, err := NewBackupEngine(se).FullBackup(backupDir)
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}

	// se is live on src: restoring into src must be refused.
	err = Restore([]*BackupManifest{m}, []string{backupDir}, []string{src})
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("Restore into a live dir: err = %v, want ErrDataDirLocked", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Into a fresh dir it works, and the restored dir opens afterwards.
	dst := t.TempDir()
	if err := Restore([]*BackupManifest{m}, []string{backupDir}, []string{dst}); err != nil {
		t.Fatalf("Restore into a fresh dir: %v", err)
	}
	se2, err := NewStorageEngine(testStorageConfig(dst))
	if err != nil {
		t.Fatalf("open restored dir: %v", err)
	}
	t.Cleanup(func() { se2.Close() })
	<-se2.ReplayDone
	if got, err := se2.Get("k"); err != nil || string(got) != "v" {
		t.Fatalf("restored Get = %q, %v", got, err)
	}
}
