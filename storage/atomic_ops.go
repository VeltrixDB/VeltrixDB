package storage

// atomic_ops.go — Compare-and-swap, increment/decrement, and set-if-not-exists.
//
// All four operations need a serialised read-modify-write on a single key.
// We achieve that by holding the per-shard write lock for the duration of the
// RMW.  This is acceptable because:
//
//   1. Atomic ops are rare relative to PUT/GET — typically counters and locks.
//   2. The shard lock is one of 8192, so even a 10 ms hold only stalls 1/8192
//      of the keyspace for that duration (invariant 30).
//   3. The alternative (optimistic CAS via IndexEntry pointer) requires the
//      VLog read to happen outside the lock, which can race with concurrent
//      Puts and produce stale "expected" comparisons.
//
// To avoid deadlocking when we call internal Put/Delete logic, we directly
// mutate shard.entries / shard.dirtyValues under the held lock and call the
// WAL+VLog persistence helpers without re-acquiring the shard lock. The
// helpers below — durablePersistKVSep and durablePersistInline — replicate
// what engine.Put does internally — including the compress → encrypt value
// transform on write and its inverse on read (invariant 29) — but skip the
// shardedIndex.put step.

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

	cur, found, err := se.readUnderShardLockKVSep(shard, key)
	if err != nil {
		return 0, fmt.Errorf("CAS read: %w", err)
	}
	if !found {
		se.audit.Log(AuditRecord{Op: "CAS", Key: key, Status: "not_found"})
		return CASKeyNotFound, nil
	}
	if !bytes.Equal(cur, expected) {
		se.audit.Log(AuditRecord{Op: "CAS", Key: key, Status: "mismatch"})
		return CASMismatch, nil
	}
	if err := se.persistAtomicKVSep(shard, shardID, key, newValue, ttl, "CAS"); err != nil {
		return 0, err
	}
	return CASSuccess, nil
}

// Increment atomically adds delta to the int64 value stored at key. Treats a
// missing key as zero (creates it with value=delta). Returns the new value.
// Errors if the existing value is not a valid int64 string.
func (se *StorageEngine) Increment(key string, delta int64, ttl int32) (int64, error) {
	return se.increment(key, delta, ttl, "INCR")
}

// increment is Increment with the op name the audit log records (INCR/DECR).
func (se *StorageEngine) increment(key string, delta int64, ttl int32, op string) (int64, error) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return 0, errors.New("INCR requires KV-separation mode")
	}
	shard, shardID := se.index.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	var cur int64
	curBytes, found, err := se.readUnderShardLockKVSep(shard, key)
	if err != nil {
		return 0, fmt.Errorf("INCR read: %w", err)
	}
	if found {
		n, err := strconv.ParseInt(string(curBytes), 10, 64)
		if err != nil {
			err = fmt.Errorf("%s: existing value is not an int64: %w", op, err)
			se.audit.Log(AuditRecord{Op: op, Key: key, Status: "err", Err: err.Error()})
			return 0, err
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
	if err := se.persistAtomicKVSep(shard, shardID, key, newBytes, ttl, op); err != nil {
		return 0, err
	}
	return newVal, nil
}

// Decrement is sugar for Increment with negative delta.
func (se *StorageEngine) Decrement(key string, delta int64, ttl int32) (int64, error) {
	return se.increment(key, -delta, ttl, "DECR")
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

	if entry, ok := shard.entries.get(key, fnv64a(key)); ok && !entry.IsTombstone() {
		nowUs := time.Now().UnixMicro()
		if !entry.IsExpired(nowUs) {
			se.audit.Log(AuditRecord{Op: "SETNX", Key: key, Status: "exists"})
			return SetNXExists, nil
		}
		// Expired: treat as not-found and overwrite.
	}
	if err := se.persistAtomicKVSep(shard, shardID, key, value, ttl, "SETNX"); err != nil {
		return 0, err
	}
	return SetNXCreated, nil
}

// readUnderShardLockKVSep returns the current value of key while the caller
// holds shard.mu (read or write lock). Looks at, in order: dirtyValues,
// LIRS cache, and finally the VLog (which may issue a disk read while the
// shard lock is held — costly but rare for atomic ops).
//
// Returns (nil, false, nil) when the key is absent, tombstoned, or expired,
// and an error when a live value cannot be read or decoded — the caller must
// not treat that as "absent" (INCR would reset the counter to delta, SETNX /
// CAS would act on a key that exists).
func (se *StorageEngine) readUnderShardLockKVSep(shard *indexShard, key string) ([]byte, bool, error) {
	entry, ok := shard.entries.get(key, fnv64a(key))
	if !ok || entry.IsTombstone() {
		return nil, false, nil
	}
	nowUs := time.Now().UnixMicro()
	if entry.IsExpired(nowUs) {
		return nil, false, nil
	}
	if v, ok := shard.dirtyValues[key]; ok {
		return v, true, nil
	}
	if v, hit := se.cache.Get(key); hit {
		return v, true, nil
	}
	if entry.DiskOffset > 0 && len(se.vlogs) > 0 {
		vlogIdx := int(entry.SegmentID) % len(se.vlogs)
		v, err := se.vlogs[vlogIdx].ReadValue(int64(entry.DiskOffset), entry.ValueSize)
		if err != nil {
			return nil, false, fmt.Errorf("vlog read: %w", err)
		}
		// ReadValue returns the on-disk blob; undo compress/encrypt exactly as
		// Get does (invariant 29). Returning the raw blob made INCR fail to
		// parse, and CAS mismatch, on any transformed value after a cache miss.
		v, err = decodeStoredValue(&entry, v)
		if err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	return nil, false, nil
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
//
// op names the operation for the audit log (CAS, INCR, DECR, SETNX). Every
// outcome is audited; on success one CDC "PUT" event is emitted, while the
// shard lock is still held, so CDC order for the key matches install order.
// Both sinks are non-blocking, so this adds no wait under the lock.
func (se *StorageEngine) persistAtomicKVSep(shard *indexShard, shardID uint16, key string, value []byte, ttl int32, op string) (err error) {
	defer func() {
		if err != nil {
			se.audit.Log(AuditRecord{Op: op, Key: key, Status: "err", Err: err.Error()})
		}
	}()
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
	// The TTL the new IndexEntry gets below must reach the WAL too, or replay
	// brings the key back immortal (always assigned: the entry is pooled).
	ttlExpiryUs := walTTLExpiryUs(nowUs, ttl)
	walEntry.TTLExpiryUs = ttlExpiryUs

	// Value transform: compress, then encrypt — the same chokepoint as Put
	// (invariant 29). Done before MarkDead so a transform error leaves the
	// old record's live accounting untouched.
	writeBytes, xflags, xerr := se.transformForWrite(value)
	if xerr != nil {
		walEntryPool.Put(walEntry)
		return fmt.Errorf("atomic value transform: %w", xerr)
	}

	if old, ok := shard.entries.get(key, fnv64a(key)); ok && !old.IsTombstone() && old.DiskOffset > 0 {
		se.vlogs[int(old.SegmentID)%len(se.vlogs)].MarkDead(old.ValueSize, old.IsPacked())
	}

	vlogOffset, vlogRp, err := se.vlogs[diskIdx].beginAppend(writeBytes)
	if err != nil {
		walEntryPool.Put(walEntry)
		return fmt.Errorf("atomic vlog append: %w", err)
	}
	walEntry.VLogOffset = vlogOffset
	walEntry.Value = nil // value is durable in VLog
	// On-disk length + transform flags must reach the WAL, or replay rebuilds
	// the entry with the plaintext length and no flags (invariant 29).
	walEntry.DiskValueLen = uint32(len(writeBytes))
	walEntry.XformFlags = xflags
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
		ValueSize:        uint32(len(writeBytes)), // on-disk blob length
		UncompressedSize: uint32(len(value)),      // plaintext length
		KeySize:          uint32(len(key)),
		WriteTimestampUs: nowUs,
		CRC32C:           crc,
		SchemaVersion:    CurrentSchemaVersion,
		ShardID:          shardID,
		Flags:            xflags,
	}
	if ttlExpiryUs > 0 {
		entry.Flags |= FlagHasTTL
		entry.TTLExpiryUs = ttlExpiryUs
	}
	// swap, not a blind store: a new or resurrected key must bump keyCount
	// exactly as shardedIndex.put does, or size() drifts low on every
	// SETNX/INCR/CAS that creates a key.
	if old, hadOld := shard.entries.swap(key, fnv64a(key), entry); !hadOld || old.IsTombstone() {
		se.index.keyCount.Add(1)
		if se.index.ordered != nil { // live keys are already present — see shardedIndex.put
			// Caller holds shard.mu — matches the indexShard.mu → oiNode.mu lock
			// order documented in ordered_index.go.
			se.index.ordered.Insert(key)
		}
	}
	if shard.bloom != nil {
		shard.bloom.Add(fnv64a(key))
	}
	delete(shard.dirtyValues, key) // value is in VLog, not RAM

	se.cache.PutWithExpiry(key, value, entry.CacheExpiryUs())
	se.metrics.Writes.Add(1)
	se.metrics.AtomicOps.Add(1)
	elapsed := time.Since(now)
	se.metrics.WritesLatencyNs.Store(elapsed.Nanoseconds())
	se.metrics.ObserveWriteLatency(elapsed.Seconds())
	se.audit.Log(AuditRecord{Op: op, Key: key, Status: "ok"})
	se.cdc.Broadcast(CDCEvent{Op: "PUT", Key: key, Value: value, Timestamp: nowUs})
	return nil
}
