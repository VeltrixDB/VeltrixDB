package storage

// tombstone_replicated.go — tombstone retention policy that respects replica lag.
//
// Vanilla tombstone GC drops tombstones after GCGracePeriodSec.  In a replicated
// deployment that period must be at least as long as the longest replica lag,
// otherwise a slow replica that hasn't yet seen the delete would resurrect the
// key on next anti-entropy ("zombie data").
//
// This file adds a "minimum replica acknowledgement" check.  A tombstone is
// reaped when
//   (a) it is older than GCGracePeriodSec (unchanged rule — also protects the
//       repl-ship catch-up feed and single-node / raft deployments), AND
//   (b) every replica that reported a watermark has acknowledged a write
//       timestamp >= the tombstone's, OR the tombstone is older than
//       2 × GCGracePeriodSec (upper bound: a replica that never catches up
//       delays reaping by at most one extra grace period).
//
// When no replica has ever reported a watermark (single node, raft mode,
// replication not wired) rule (b) is skipped: grace period alone.
//
// Wire flow (cmd/server, --mode=replicated):
//   - replication.ReplicationEngine.SetWatermarkObserver → every second,
//     StorageEngine.SetReplicaWatermark(replicaID, watermarkUs), where the
//     watermark is the timestamp of the oldest write the replica has not
//     acked (or now, when caught up) minus a safety margin.
//   - Defragmenter.reapExpiredTombstones calls canReapTombstone for every
//     tombstone past the grace period.
//
// Watermarks live in memory: after a restart they are empty until the
// replication engine reports again (≤ 1 s).

import (
	"sync"
	"sync/atomic"
	"time"
)

// TombstoneCoordinator tracks per-replica acknowledgement timestamps and
// computes the minimum-acked watermark used by the tombstone reaper.
type TombstoneCoordinator struct {
	mu        sync.RWMutex
	watermark map[string]int64 // replicaID → max acked WriteTimestampUs

	// disabled: when no replicas have ever reported a watermark we behave
	// as a single-node deployment and skip the watermark check entirely.
	hasReplicas atomic.Bool
}

// NewTombstoneCoordinator returns a fresh coordinator. The engine creates one
// at startup and exposes it via SetReplicaWatermark / GetReplicaWatermarks.
func NewTombstoneCoordinator() *TombstoneCoordinator {
	return &TombstoneCoordinator{watermark: map[string]int64{}}
}

// SetReplicaWatermark records that replicaID has durably applied every write
// up to (and including) writeTimestampUs.  Monotonic per replica — older
// updates are ignored.  Called from the replication subsystem.
func (c *TombstoneCoordinator) SetReplicaWatermark(replicaID string, writeTimestampUs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.watermark[replicaID]; ok && cur >= writeTimestampUs {
		return
	}
	c.watermark[replicaID] = writeTimestampUs
	c.hasReplicas.Store(true)
}

// MinWatermarkUs returns the lowest acknowledgement timestamp across all
// known replicas.  Returns math.MaxInt64 when no replicas are tracked
// (effectively a no-op for single-node).
func (c *TombstoneCoordinator) MinWatermarkUs() int64 {
	if !c.hasReplicas.Load() {
		return 1<<63 - 1
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var min int64 = 1<<63 - 1
	for _, v := range c.watermark {
		if v < min {
			min = v
		}
	}
	return min
}

// Snapshot returns a copy of the per-replica watermark map for admin views.
func (c *TombstoneCoordinator) Snapshot() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int64, len(c.watermark))
	for k, v := range c.watermark {
		out[k] = v
	}
	return out
}

// SetReplicaWatermark is the engine-level helper.
func (se *StorageEngine) SetReplicaWatermark(replicaID string, writeTimestampUs int64) {
	se.tombstones.SetReplicaWatermark(replicaID, writeTimestampUs)
}

// CanReapTombstone returns true when a tombstone with the given timestamp
// can be physically removed without risking zombie data on a lagging replica.
//
// The tombstone must be older than the grace period, and every known replica
// must have acknowledged a writeTimestamp ≥ this one unless the tombstone is
// older than twice the grace period.  Single-node deployments (no replicas
// tracked) skip the replica check entirely.
func (se *StorageEngine) CanReapTombstone(tombstoneWriteTsUs int64, nowUs int64, gracePeriodSec int64) bool {
	return canReapTombstone(se.tombstones, tombstoneWriteTsUs, nowUs, gracePeriodSec)
}

// canReapTombstone implements the rule documented at the top of this file.
// tc may be nil (grace period only).
func canReapTombstone(tc *TombstoneCoordinator, tombstoneWriteTsUs, nowUs, gracePeriodSec int64) bool {
	graceUs := gracePeriodSec * 1_000_000
	age := nowUs - tombstoneWriteTsUs
	if age < graceUs {
		return false
	}
	if tc == nil || age >= 2*graceUs {
		return true
	}
	return tc.MinWatermarkUs() >= tombstoneWriteTsUs
}

// ReplicatedTombstoneStats exposes the watermark snapshot via the admin API.
type ReplicatedTombstoneStats struct {
	Replicas       map[string]int64 // replicaID → ackedWriteTimestampUs
	MinWatermarkUs int64
	NowUs          int64
}

func (se *StorageEngine) ReplicatedTombstoneStats() ReplicatedTombstoneStats {
	return ReplicatedTombstoneStats{
		Replicas:       se.tombstones.Snapshot(),
		MinWatermarkUs: se.tombstones.MinWatermarkUs(),
		NowUs:          time.Now().UnixMicro(),
	}
}
