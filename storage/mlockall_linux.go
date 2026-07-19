//go:build linux

package storage

import (
	"fmt"
	"syscall"
)

// mlockall flags (from <sys/mman.h>).
const (
	mclCurrent = 0x1 // MCL_CURRENT — lock all currently mapped pages
	mclFuture  = 0x2 // MCL_FUTURE  — lock all pages mapped in the future
	mclOnFault = 0x4 // MCL_ONFAULT — only lock pages when they are faulted in
	                 //               (Linux 4.4+; avoids pre-faulting 64 GB on startup)

	// rlimitMemlock is RLIMIT_MEMLOCK = 8 on Linux.  Go's syscall package does
	// not export this constant (it was omitted from the generated zerrors file).
	rlimitMemlock = 8
)

// LockProcessMemory calls mlockall(MCL_CURRENT|MCL_FUTURE|MCL_ONFAULT).
//
// Why lock memory for a 480 GB RAM machine
// ──────────────────────────────────────────
// At 1B keys + 256 GB LIRS cache, VeltrixDB's RSS exceeds 320 GB.  Without
// mlockall, the kernel may swap out cold pages under memory pressure from
// other processes.  A single swap-in during a P99-sensitive read adds
// ~10 ms (disk seek latency).
//
// MCL_ONFAULT defers page faulting — pages are locked in RAM only when they
// are first accessed, not all at once during startup.  This prevents a 64 GB
// page-fault storm at boot.
//
// Requirements: CAP_IPC_LOCK privilege or root.  If locking fails with
// EPERM, the engine continues normally but logs a warning.
func LockProcessMemory() error {
	_, _, errno := syscall.Syscall(
		syscall.SYS_MLOCKALL,
		uintptr(mclCurrent|mclFuture|mclOnFault),
		0, 0,
	)
	if errno == 0 {
		return nil
	}
	// EPERM is non-fatal: engine still works, just without memory locking.
	if errno == syscall.EPERM {
		return fmt.Errorf("mlockall: EPERM — needs CAP_IPC_LOCK or root (continuing without memory lock)")
	}
	return fmt.Errorf("mlockall: %w", errno)
}

// SetMemoryRLimitLock raises RLIMIT_MEMLOCK to `bytes` so that subsequent
// mlockall / mlock calls can lock that much memory.
// On production GKE nodes, set to 80% of physical RAM
// (e.g. 0.8 × 480 GB = 384 GB).
func SetMemoryRLimitLock(bytes uint64) error {
	lim := syscall.Rlimit{Cur: bytes, Max: bytes}
	if err := syscall.Setrlimit(rlimitMemlock, &lim); err != nil {
		return fmt.Errorf("setrlimit MEMLOCK: %w", err)
	}
	return nil
}
