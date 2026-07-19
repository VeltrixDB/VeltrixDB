package storage

// atomic_ops.go — Compare-and-swap, increment/decrement, and set-if-not-exists.
//
// All four operations need a serialised read-modify-write on a single key.
// We achieve that by holding the per-shard write lock for the duration of the
// RMW.  This is acceptable because:
//
//   1. Atomic ops are rare relative to PUT/GET — typically counters and locks.
//   2. The shard lock is one of 1024, so even a 10 ms hold only stalls 1/1024
//      of the keyspace for that duration (random-distributed reads see ~10 µs
//      tail).
//   3. The alternative (optimistic CAS via IndexEntry pointer) requires the
//      VLog read to happen outside the lock, which can race with concurrent
//      Puts and produce stale "expected" comparisons.
//
// To avoid deadlocking when we call internal Put/Delete logic, we directly
// mutate shard.entries / shard.dirtyValues under the held lock and call the
// WAL+VLog persistence helpers without re-acquiring the shard lock. The
// helpers below — durablePersistKVSep and durablePersistInline — replicate
// what engine.Put does internally but skip the shardedIndex.put step.

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// CASResult enumerates outcomes of CompareAndSwap.
type CASResult int

const (
	CASSuccess     CASResult = iota // value was equal to expected; new value durably written
	CASMismatch                     // value did not match expected; nothing was written
	CASKeyNotFound                  // key did not exist; nothing was written (use SetIfNotExists)
)

// SetNXResult enumerates outcomes of SetIfNotExists.
type SetNXResult int

const (
	SetNXCreated SetNXResult = iota // key did not exist before; new value written
	SetNXExists                     // key already existed; nothing was written
)

// CompareAndSwap atomically replaces the value of key with newValue iff the
// current value byte-equals expected. Returns CASSuccess on a swap,
// CASMismatch on a value mismatch, CASKeyNotFound when the key is absent.
//
// The shard's write lock is held for the entire RMW: read current value,
// compare, persist new value to WAL+VLog, update the in-memory IndexEntry.
// During the durable persist (~10 ms group-commit window) the shard is
// momentarily unreadable; for atomic ops this is the price of correctness.
func (se *StorageEngine) CompareAndSwap(key string, expected, newValue []byte, ttl int32) (CASResult, error) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return 0, errors.New("CAS requires KV-separation mode")
	}
	shard, shardID := se.index.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	cur, found := se.readUnderShardLockKVSep(shard, key)
	if !found {
		return CASKeyNotFound, nil
	}
	if !bytes.Equal(cur, expected) {
		return CASMismatch, nil
	}
	if err := se.persistAtomicKVSep(shard, shardID, key, newValue, ttl); err != nil {
		return 0, err
	}
	return CASSuccess, nil
}

// Increment atomically adds delta to the int64 value stored at key. Treats a
// missing key as zero (creates it with value=delta). Returns the new value.
// Errors if the existing value is not a valid int64 string.
func (se *StorageEngine) Increment(key string, delta int64, ttl int32) (int64, error) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return 0, errors.New("INCR requires KV-separation mode")
	}
	shard, shardID := se.index.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	var cur int64
	if curBytes, found := se.readUnderShardLockKVSep(shard, key); found {
		n, err := strconv.ParseInt(string(curBytes), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("INCR: existing value is not an int64: %w", err)
		}
		cur = n
	}
	// Saturating arithmetic avoids silent overflow for monotonic counters.
	newVal := cur + delta
	if delta > 0 && newVal < cur {
		newVal = int64(^uint64(0) >> 1) // MaxInt64
	} else if delta < 0 && newVal > cur {
		newVal = -int64(^uint64(0)>>1) - 1 // MinInt64
	}
	newBytes := []byte(strconv.FormatInt(newVal, 10))
	if err := se.persistAtomicKVSep(shard, shardID, key, newBytes, ttl); err != nil {
		return 0, err
	}
	return newVal, nil
}

// Decrement is sugar for Increment with negative delta.
func (se *StorageEngine) Decrement(key string, delta int64, ttl int32) (int64, error) {
	return se.Increment(key, -delta, ttl)
}

// SetIfNotExists atomically sets key=value iff key does not currently exist
// (or is tombstoned). Returns SetNXCreated on success, SetNXExists if a live
// entry already exists.
func (se *StorageEngine) SetIfNotExists(key string, value []byte, ttl int32) (SetNXResult, error) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return 0, errors.New("SETNX requires KV-separation mode")
	}
	shard, shardID := se.index.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, ok := shard.entries[key]; ok && !entry.IsTombstone() {
		nowUs := time.Now().UnixMicro()
		if !entry.IsExpired(nowUs) {
			return SetNXExists, nil
		}
		// Expired: treat as not-found and overwrite.
	}
	if err := se.persistAtomicKVSep(shard, shardID, key, value, ttl); err != nil {
		return 0, err
	}
	return SetNXCreated, nil
}

// readUnderShardLockKVSep returns the current value of key while the caller
// holds shard.mu (read or write lock). Looks at, in order: dirtyValues,
// LIRS cache, and finally the VLog (which may issue a disk read while the
// shard lock is held — costly but rare for atomic ops).
//
// Returns (nil, false) when the key is absent, tombstoned, or expired.
func (se *StorageEngine) readUnderShardLockKVSep(shard *indexShard, key string) ([]byte, bool) {
	entry, ok := shard.entries[key]
	if !ok || entry.IsTombstone() {
		return nil, false
	}
	nowUs := time.Now().UnixMicro()
	if entry.IsExpired(nowUs) {
		return nil, false
	}
	if v, ok := shard.dirtyValues[key]; ok {
		return v, true
	}
	if v, hit := se.cache.Get(key); hit {
		return v, true
	}
	if entry.DiskOffset > 0 && len(se.vlogs) > 0 {
		vlogIdx := int(entry.SegmentID) % len(se.vlogs)
		v, err := se.vlogs[vlogIdx].ReadValue(int64(entry.DiskOffset), entry.ValueSize)
		if err != nil {
			return nil, false
		}
		return v, true
	}
	return nil, false
}

// persistAtomicKVSep performs the WAL+VLog durability for an atomic op while
// the caller holds shard.mu (write lock). It directly mutates shard.entries
// to install the new IndexEntry without re-acquiring the shard lock — that
// would deadlock — and updates the LIRS cache.
//
// On WAL or VLog error nothing is written to the index, so the previous
// committed state remains intact (caller still holds the lock).
//
// Note: WAL group-commit + VLog group-commit run concurrently. Both fdatasyncs
// may take ~10 ms; the caller's shard lock is held for that duration. This is
// the price of strictly serialisable atomic ops; for higher contention move
// the workload to a non-atomic path.
func (se *StorageEngine) persistAtomicKVSep(shard *indexShard, shardID uint16, key string, value []byte, ttl int32) error {
	now := time.Now()
	nowUs := now.UnixMicro()
	version := se.version.Add(1)
	crc := computeCRC32C(value)
	diskIdx := diskForShard(shardID, len(se.wals))

	walEntry := walEntryPool.Get().(*WALEntry)
	walEntry.Timestamp = now.UnixNano()
	walEntry.KeyLen = uint32(len(key))
	walEntry.Key = key
	walEntry.ValueLen = uint32(len(value))
	walEntry.Value = value
	walEntry.Checksum = crc
	walEntry.Version = version
	walEntry.IsTombstone = false
	walEntry.ReplicationID = 0
	walEntry.Packed = false

	if old, ok := shard.entries[key]; ok && !old.IsTombstone() && old.DiskOffset > 0 {
		se.vlogs[int(old.SegmentID)%len(se.vlogs)].MarkDead(old.ValueSize, old.IsPacked())
	}

	vlogOffset, vlogRp, err := se.vlogs[diskIdx].beginAppend(value)
	if err != nil {
		walEntryPool.Put(walEntry)
		return fmt.Errorf("atomic vlog append: %w", err)
	}
	walEntry.VLogOffset = vlogOffset
	walEntry.Value = nil // value is durable in VLog
	walRp := se.wals[diskIdx].beginAppend(walEntry)
	walEntryPool.Put(walEntry)

	walErr := <-*walRp
	walRespPool.Put(walRp)
	vlogErr := <-*vlogRp
	vlogRespPool.Put(vlogRp)
	if walErr != nil {
		return fmt.Errorf("atomic WAL append: %w", walErr)
	}
	if vlogErr != nil {
		return fmt.Errorf("atomic VLog fdatasync: %w", vlogErr)
	}

	// Build the new IndexEntry and install it directly under the held lock.
	entry := &IndexEntry{
		KeyHash:          fnv64a(key),
		DiskOffset:       uint64(vlogOffset),
		SegmentID:        uint32(diskIdx),
		ValueSize:        uint32(len(value)),
		UncompressedSize: uint32(len(value)),
		KeySize:          uint32(len(key)),
		WriteTimestampUs: nowUs,
		CRC32C:           crc,
		SchemaVersion:    CurrentSchemaVersion,
		ShardID:          shardID,
	}
	if ttl > 0 {
		entry.Flags |= FlagHasTTL
		entry.TTLExpiryUs = nowUs + int64(ttl)*1_000_000
	}
	shard.entries[key] = entry
	if se.index.ordered != nil {
		// Caller holds shard.mu — matches the indexShard.mu → oiNode.mu lock
		// order documented in ordered_index.go.
		se.index.ordered.Insert(key)
	}
	if shard.bloom != nil {
		shard.bloom.Add(fnv64a(key))
	}
	delete(shard.dirtyValues, key) // value is in VLog, not RAM

	se.cache.Put(key, value)
	se.metrics.Writes.Add(1)
	se.metrics.AtomicOps.Add(1)
	elapsed := time.Since(now)
	se.metrics.WritesLatencyNs.Store(elapsed.Nanoseconds())
	se.metrics.ObserveWriteLatency(elapsed.Seconds())
	return nil
}
