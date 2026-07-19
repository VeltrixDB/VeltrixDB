package storage

// disk_health.go — automatic disk-failure detection and degraded mode.
//
// Previously an I/O-failing disk surfaced only as per-request errors: every
// Put/Get routed to it kept hitting the dead device (and its timeouts)
// forever, and nothing marked the node degraded.  This file adds a small
// consecutive-error breaker per disk:
//
//   - diskFailThreshold consecutive I/O errors on a disk's WAL/VLog trip the
//     breaker: the disk is marked FAILED, writes routed to it fail fast with
//     ErrDiskFailed, and compaction/GC skips it.
//   - Reads still try the device (data may be partially readable) and are
//     served from cache when possible.
//   - Any subsequent successful I/O on the disk resets the streak; a failed
//     disk is NOT auto-unfailed — replacing it requires a node drain/restart
//     (see docs/DR_RUNBOOK.md), which matches how operators actually swap
//     hardware.
//   - Degraded() feeds /readyz so orchestrators (K8s) stop routing new
//     traffic to a node with a dead disk; FailedDisks feeds INFO + metrics.

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
)

// diskFailThreshold is the number of CONSECUTIVE I/O errors that trip the
// breaker. Transient hiccups reset on the next success; a dead device
// produces an unbroken streak within milliseconds.
const diskFailThreshold = 5

// ErrDiskFailed is returned for writes routed to a disk whose breaker has
// tripped. Clients should treat it as a retriable node-degraded condition
// (retry against another replica).
var ErrDiskFailed = errors.New("disk marked FAILED — node is degraded, retry on another replica")

// diskHealth tracks one disk's breaker state.
type diskHealth struct {
	failed atomic.Bool
	streak atomic.Int32
}

// initDiskHealth sizes the tracker; called once from NewStorageEngine.
func (se *StorageEngine) initDiskHealth(n int) {
	se.diskHealth = make([]diskHealth, n)
}

// noteDiskError records an I/O failure on disk i and trips the breaker after
// diskFailThreshold consecutive errors.
func (se *StorageEngine) noteDiskError(i int, op string, err error) {
	if i < 0 || i >= len(se.diskHealth) {
		return
	}
	dh := &se.diskHealth[i]
	if dh.failed.Load() {
		return
	}
	if streak := dh.streak.Add(1); int(streak) >= diskFailThreshold {
		if dh.failed.CompareAndSwap(false, true) {
			se.metrics.DiskFailures.Add(1)
			log.Printf("[disk] disk=%d marked FAILED after %d consecutive %s errors (last: %v) — "+
				"writes to this disk now fail fast; node reports degraded on /readyz",
				i, streak, op, err)
		}
	}
}

// noteDiskOK resets the consecutive-error streak after a successful I/O.
func (se *StorageEngine) noteDiskOK(i int) {
	if i < 0 || i >= len(se.diskHealth) {
		return
	}
	dh := &se.diskHealth[i]
	if dh.streak.Load() != 0 && !dh.failed.Load() {
		dh.streak.Store(0)
	}
}

// diskIsFailed reports whether disk i's breaker has tripped.
func (se *StorageEngine) diskIsFailed(i int) bool {
	return i >= 0 && i < len(se.diskHealth) && se.diskHealth[i].failed.Load()
}

// FailedDisks returns the indices of disks currently marked FAILED.
func (se *StorageEngine) FailedDisks() []int {
	var out []int
	for i := range se.diskHealth {
		if se.diskHealth[i].failed.Load() {
			out = append(out, i)
		}
	}
	return out
}

// Degraded reports whether any disk has failed — surfaced on /readyz so
// orchestrators drain traffic from the node.
func (se *StorageEngine) Degraded() bool {
	for i := range se.diskHealth {
		if se.diskHealth[i].failed.Load() {
			return true
		}
	}
	return false
}

// degradedError decorates ErrDiskFailed with the disk index for logs/clients.
func degradedError(disk int) error {
	return fmt.Errorf("%w (disk=%d)", ErrDiskFailed, disk)
}
