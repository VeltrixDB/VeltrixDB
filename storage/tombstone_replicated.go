package storage

// tombstone_replicated.go — tombstone retention policy that respects replica lag.
//
// Vanilla tombstone GC drops tombstones after GCGracePeriodSec.  In a replicated
// deployment that period must be at least as long as the longest replica lag,
// otherwise a slow replica that hasn't yet seen the delete would resurrect the
// key on next anti-entropy ("zombie data").
//
// This file adds a "minimum replica acknowledgement" check: a tombstone is
// reaped only when EITHER
//   (a) GCGracePeriodSec has elapsed, AND
//   (b) every known replica has reported `LastSeenWriteTimestampUs >= the
//       tombstone's timestamp` via the replication acknowledgement channel.
//
// When (b) cannot be satisfied (replica unreachable, replication subsystem not
// wired), the engine falls back to (a) alone — the same behaviour we had
// before this change. Operators who run replication should wire the
// SetReplicaWatermark API from the replication package.
//
// Wire flow:
//   - Replication subsystem calls SetReplicaWatermark(replicaID, timestamp)
//     periodically (after each replica acks a batch).
//   - The defragmenter's tombstone-reap loop calls minReplicaWatermark()
//     and skips any tombstone whose timestamp > min watermark.

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
// Two conditions must BOTH hold:
//   1. The tombstone is older than the configured grace period (existing rule).
//   2. Every known replica has acknowledged a writeTimestamp ≥ this one.
//
// Single-node deployments (no replicas tracked) skip rule (2) entirely.
func (se *StorageEngine) CanReapTombstone(tombstoneWriteTsUs int64, nowUs int64, gracePeriodSec int64) bool {
	if nowUs-tombstoneWriteTsUs < gracePeriodSec*1_000_000 {
		return false
	}
	if se.tombstones.MinWatermarkUs() < tombstoneWriteTsUs {
		return false
	}
	return true
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
