package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VeltrixDB/veltrixdb/storage"
)

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("stat wal.log: %v", err)
	}
	return fi.Size()
}

// Regression: veltrixdb-backup pointed at a live server's --data-dirs opened a
// second engine on the same files and truncated the live wal.log and
// vlog_active.dat. openEngine must now refuse, and say what to do instead.
func TestOpenEngine_RefusesLiveServerDirs(t *testing.T) {
	dir := t.TempDir()

	// The "live server".
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.CacheMaxSizeMB = 16
	cfg.BloomFilterShardBits = 1 << 12
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	live, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("open live engine: %v", err)
	}
	<-live.ReplayDone
	for _, k := range []string{"a", "b", "c"} {
		if err := live.Put(k, []byte("value-"+k), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	before := walSize(t, dir)

	se, err := openEngine(dir, 16)
	if err == nil {
		se.Close()
		t.Fatalf("openEngine on a live server's data dir succeeded (wal.log %d -> %d bytes); want a refusal",
			before, walSize(t, dir))
	}
	if !errors.Is(err, storage.ErrDataDirLocked) {
		t.Fatalf("openEngine error = %v, want errors.Is(storage.ErrDataDirLocked)", err)
	}
	msg := err.Error()
	for _, want := range []string{"POST", "/admin/backup", "veltrix", "backup", "STOPPED"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message does not mention %q:\n%s", want, msg)
		}
	}
	if got := walSize(t, dir); got != before {
		t.Errorf("live wal.log changed %d -> %d bytes after the refused open", before, got)
	}
	if v, err := live.Get("b"); err != nil || string(v) != "value-b" {
		t.Errorf("live engine Get = %q, %v", v, err)
	}

	// Once the server is stopped the offline tool works, and backs it up.
	if err := live.Close(); err != nil {
		t.Fatalf("Close live: %v", err)
	}
	se, err = openEngine(dir, 16)
	if err != nil {
		t.Fatalf("openEngine on a stopped server's dir: %v", err)
	}
	defer se.Close()
	<-se.ReplayDone
	m, err := storage.NewBackupEngine(se).FullBackup(t.TempDir())
	if err != nil {
		t.Fatalf("FullBackup: %v", err)
	}
	if m.NumDisks != 1 {
		t.Errorf("manifest NumDisks = %d, want 1", m.NumDisks)
	}
}

func TestExplainLocked_PassesOtherErrorsThrough(t *testing.T) {
	other := errors.New("disk on fire")
	if got := explainLocked(other); got != other {
		t.Fatalf("explainLocked changed an unrelated error: %v", got)
	}
}
