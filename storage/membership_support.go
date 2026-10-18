package storage

// membership_support.go — engine helpers for online cluster membership
// (cmd/server/membership*.go, --mode=replicated).

import "time"

// PresentOrDeleted reports whether the index holds a live (unexpired) entry
// for key, or a tombstone that has not been reaped yet.  A replicated-mode
// node receiving a migrated copy of a key (put-if-absent) skips it when this
// is true: the replication stream already delivered a write or a delete of
// that key, which is at least as new as the copy.
func (se *StorageEngine) PresentOrDeleted(key string) bool {
	shard, _ := se.index.shardFor(key)
	shard.mu.RLock()
	entry, ok := shard.entries.get(key, fnv64a(key))
	shard.mu.RUnlock()
	if !ok {
		return false
	}
	if entry.IsTombstone() {
		return true
	}
	return !entry.IsExpired(time.Now().UnixMicro())
}

// ForgetReplicaWatermark drops a replica that left the cluster from the
// tombstone-GC watermarks, so its last watermark no longer holds back
// tombstone reaping (storage/tombstone_replicated.go).
func (se *StorageEngine) ForgetReplicaWatermark(replicaID string) {
	c := se.tombstones
	c.mu.Lock()
	delete(c.watermark, replicaID)
	c.hasReplicas.Store(len(c.watermark) > 0)
	c.mu.Unlock()
}
