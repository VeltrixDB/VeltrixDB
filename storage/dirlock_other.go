//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd)

package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// Best-effort fallback for platforms without flock(2) in package syscall
// (windows, solaris, aix, plan9, wasm …): an in-process registry keyed by the
// cleaned absolute directory path. It stops a second engine in the SAME
// process; it does NOT stop another process. Production targets Linux.

var (
	inprocDirLocksMu sync.Mutex
	inprocDirLocks   = map[string]bool{}
)

func dirLockKey(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(dir)
}

func tryLockFile(_ *os.File, dir string) error {
	k := dirLockKey(dir)
	inprocDirLocksMu.Lock()
	defer inprocDirLocksMu.Unlock()
	if inprocDirLocks[k] {
		return errLockHeld
	}
	inprocDirLocks[k] = true
	return nil
}

func unlockFile(_ *os.File, dir string) {
	inprocDirLocksMu.Lock()
	delete(inprocDirLocks, dirLockKey(dir))
	inprocDirLocksMu.Unlock()
}
