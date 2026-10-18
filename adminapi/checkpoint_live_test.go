package adminapi

// checkpoint_live_test.go — POST /admin/checkpoint and /admin/backup against a
// real engine: writes acknowledged after them survive a crash (the runtime
// checkpoint used to rename its file over wal.log under the flusher, which
// kept appending to the unlinked inode — storage/wal_checkpoint_live.go).

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VeltrixDB/veltrixdb/storage"
)

func openRealEngine(t *testing.T, dir string) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.BloomFilterShardBits = 1 << 12
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	se, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	return se
}

// crashCopy copies dir as a power cut would leave it (the engine keeps
// running); the copy has no LOCK file, so a second engine can open it.
func crashCopy(t *testing.T, dir string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		out := filepath.Join(dst, rel)
		if fi.IsDir() {
			return os.MkdirAll(out, 0755)
		}
		if fi.Name() == "LOCK" {
			return nil
		}
		in, err := os.Open(p)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		defer in.Close()
		f, err := os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, in)
		return err
	})
	if err != nil {
		t.Fatalf("copy %s: %v", dir, err)
	}
	return dst
}

func TestAdminCheckpointAndBackup_LaterWritesSurviveCrash(t *testing.T) {
	dir := t.TempDir()
	se := openRealEngine(t, dir)
	defer se.Close()
	mux := http.NewServeMux()
	Register(mux, se, "/admin", storage.NewBackupEngine(se))

	want := map[string]string{}
	put := func(k, v string) {
		if err := se.Put(k, []byte(v), -1); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
		want[k] = v
	}
	for i := 0; i < 50; i++ {
		put(fmt.Sprintf("before-%02d", i), "v0")
	}
	if rec := do(t, mux, http.MethodPost, "/admin/checkpoint", ""); rec.Code != http.StatusOK {
		t.Fatalf("POST /admin/checkpoint: %d %s", rec.Code, rec.Body.String())
	}
	for i := 0; i < 50; i++ {
		put(fmt.Sprintf("after-ckpt-%02d", i), "v1")
	}
	put("before-00", "overwritten")
	body := fmt.Sprintf(`{"type":"full","dest_dir":%q}`, t.TempDir())
	if rec := do(t, mux, http.MethodPost, "/admin/backup", body); rec.Code != http.StatusOK {
		t.Fatalf("POST /admin/backup: %d %s", rec.Code, rec.Body.String())
	}
	for i := 0; i < 50; i++ {
		put(fmt.Sprintf("after-backup-%02d", i), "v2")
	}
	if err := se.Delete("before-01"); err != nil {
		t.Fatal(err)
	}
	want["before-01"] = ""

	crashed := openRealEngine(t, crashCopy(t, dir))
	defer crashed.Close()
	var lost []string
	for k, v := range want {
		got, err := crashed.Get(k)
		if v == "" {
			if err == nil {
				lost = append(lost, k+" (deleted, came back)")
			}
			continue
		}
		if err != nil || string(got) != v {
			lost = append(lost, k)
		}
	}
	if len(lost) > 0 {
		n := len(lost)
		if n > 10 {
			lost = lost[:10]
		}
		t.Fatalf("%d acknowledged writes lost after a crash following /admin/checkpoint and /admin/backup: %s",
			n, strings.Join(lost, ", "))
	}
}
