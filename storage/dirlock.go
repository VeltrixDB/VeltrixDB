package storage

// dirlock.go — one engine per data directory.
//
// NewStorageEngine takes an exclusive, non-blocking advisory lock on a LOCK
// file in every data directory before it touches any other file there, and
// Close releases it. A second engine on the same directory — another process
// (veltrixdb-backup or veltrix-repair pointed at a live server's --data-dirs)
// or a second NewStorageEngine in the same process — fails fast with
// ErrDataDirLocked instead of opening its own WAL / VLog over the live files.
// Before this lock existed, exactly that truncated the live wal.log and
// vlog_active.dat to 0 bytes.
//
// The lock is advisory: it stops VeltrixDB engines, not `rm` or `cp`. It is
// released by the kernel when the process exits (including SIGKILL / OOM), so
// a crash never leaves a stale lock behind; the LOCK file itself is left in
// place on purpose (unlinking it would race a concurrent opener). Its
// contents (pid, start time) are informational only.
//
// Platform: flock(2) on darwin / *BSD / linux (dirlock_flock.go). Elsewhere a
// best-effort in-process registry (dirlock_other.go) — it catches a second
// engine in the same process but not one in another process.

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrDataDirLocked is returned (wrapped) by NewStorageEngine and Restore when
// another engine holds a data directory's LOCK file. Match with errors.Is.
var ErrDataDirLocked = errors.New("data directory is locked by another VeltrixDB engine")

// DirLockFileName is the advisory lock file created in every data directory.
const DirLockFileName = "LOCK"

// errLockHeld is the platform layer's "someone else holds it" result.
var errLockHeld = errors.New("lock held")

// errLockUnsupported means the filesystem cannot lock (e.g. some network
// filesystems return ENOLCK / EOPNOTSUPP). The engine then starts unlocked
// with a warning rather than refusing to serve.
var errLockUnsupported = errors.New("locking not supported by filesystem")

type dirLock struct {
	dir  string
	f    *os.File
	held bool // false when the filesystem could not lock (errLockUnsupported)
}

// lockDataDirs locks every dir in order. On any failure it releases the locks
// it already took and returns the error, so the caller holds all or none.
func lockDataDirs(dirs []string) ([]*dirLock, error) {
	locks := make([]*dirLock, 0, len(dirs))
	for _, d := range dirs {
		l, err := lockDataDir(d)
		if err != nil {
			releaseDirLocks(locks)
			return nil, err
		}
		locks = append(locks, l)
	}
	return locks, nil
}

func releaseDirLocks(locks []*dirLock) {
	for i := len(locks) - 1; i >= 0; i-- {
		if locks[i] != nil {
			locks[i].release()
		}
	}
}

func lockDataDir(dir string) (*dirLock, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("data dir %s: %w", dir, err)
	}
	path := filepath.Join(dir, DirLockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("data dir %s: open %s: %w", dir, DirLockFileName, err)
	}
	l := &dirLock{dir: dir, f: f}
	switch err := tryLockFile(f, dir); {
	case err == nil:
		l.held = true
	case errors.Is(err, errLockHeld):
		holder := readLockHolder(path)
		f.Close()
		return nil, fmt.Errorf("%w: %s (%s%s) — only one engine may open a data directory at a time",
			ErrDataDirLocked, dir, path, holder)
	case errors.Is(err, errLockUnsupported):
		log.Printf("[dirlock] WARNING: %s: %v — starting WITHOUT the single-engine "+
			"lock; never run a second engine (veltrixdb-backup, veltrix-repair) on this directory "+
			"while the server is up", path, err)
		// Keep the fd open anyway so release() is uniform.
	default:
		f.Close()
		return nil, fmt.Errorf("data dir %s: lock %s: %w", dir, DirLockFileName, err)
	}
	if l.held {
		// Informational only: the lock is the flock, not the contents.
		info := fmt.Sprintf("pid %d since %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
		if err := f.Truncate(0); err == nil {
			_, _ = f.WriteAt([]byte(info), 0)
		}
	}
	return l, nil
}

func (l *dirLock) release() {
	if l.f == nil {
		return
	}
	if l.held {
		unlockFile(l.f, l.dir)
		l.held = false
	}
	l.f.Close()
	l.f = nil
}

// readLockHolder returns ", held by pid N since T" from the LOCK file, or ""
// when it is empty or unreadable.
func readLockHolder(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(b))
	if s == "" || len(s) > 128 {
		return ""
	}
	return ", held by " + s
}
