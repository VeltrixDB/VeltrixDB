package storage

// tombstone_checkpoint.go — replay of tombstone records for keys the index
// does not hold, and the replication origin bit on tombstones.
//
// A clean shutdown's checkpoint (writeWALCheckpoint) has one record per key.
// In replicated mode it keeps the tombstones a replica may still need
// (checkpointKeepsTombstone), and such a key has NO live record before its
// tombstone in the new wal.log — replayMarkTombstone, which only updates an
// existing entry, would drop it, and the replication catch-up feed
// (ChangesSince / ScanChanges) would lose the delete.

// replayTombstoneRecord applies a WAL tombstone record at replay: the usual
// version-aware tombstone when the key has an entry, otherwise a bare
// tombstone entry carrying the record's timestamp (reaped by the defragmenter
// like any other).  A live write that landed during replay wins because it
// has an entry (replayMarkTombstone compares timestamps).  origin is the
// record's FlagReplicaApplied bit.
func (si *shardedIndex) replayTombstoneRecord(key string, tsUs int64, origin uint8) {
	h := fnv64a(key)
	shardID := uint16(h & (numShards - 1))
	shard := &si.shards[shardID]
	shard.mu.Lock()
	if _, ok := shard.entries.get(key, h); ok {
		shard.mu.Unlock()
		si.replayMarkTombstone(key, tsUs)
		if origin != 0 {
			si.setReplicaApplied(key, tsUs)
		}
		return
	}
	e := IndexEntry{
		KeyHash:       h,
		KeySize:       uint32(len(key)),
		SchemaVersion: CurrentSchemaVersion,
		ShardID:       shardID,
	}
	e.MarkTombstone(tsUs)
	e.Flags |= origin
	shard.entries.swap(key, h, &e)
	shard.mu.Unlock()
	si.noteWrite(key)
}

// setReplicaApplied sets FlagReplicaApplied on key's tombstone written at
// tsUs (a delete applied from a replication peer).  A later write — any
// other timestamp — is left alone.
func (si *shardedIndex) setReplicaApplied(key string, tsUs int64) {
	h := fnv64a(key)
	shard := &si.shards[uint16(h&(numShards-1))]
	shard.mu.Lock()
	shard.entries.update(key, h, func(e *IndexEntry) bool {
		if !e.IsTombstone() || e.WriteTimestampUs != tsUs {
			return false
		}
		e.Flags |= FlagReplicaApplied
		return true
	})
	shard.mu.Unlock()
}
