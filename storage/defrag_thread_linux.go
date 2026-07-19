//go:build linux

package storage

import (
	"runtime"
	"syscall"
)

// lockDefragThread pins the calling goroutine to its OS thread and lowers the
// thread's scheduling priority (nice +10) so the kernel prefers request-handler
// goroutines over GC work.  This is CaaS-LSM phase 1: CPU isolation without
// extracting defrag into a separate process.
//
// Must be called at the start of a goroutine that will run GC work.
// The goroutine must NOT be unlocked for its lifetime (do not call
// runtime.UnlockOSThread) — the nice value is per-thread and unlocking would
// reassign the goroutine to an un-niced thread.
func lockDefragThread() {
	runtime.LockOSThread()
	// Lower niceness to +10 so the Linux CFS scheduler deprioritises this
	// thread behind any non-niced goroutine.  Errors are silently ignored:
	// the caller may lack CAP_SYS_NICE, which is fine — we still get the
	// LockOSThread benefit (no goroutine migration, predictable CPU affinity).
	_ = syscall.Setpriority(syscall.PRIO_PROCESS, 0, 10)
}
