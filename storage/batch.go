package storage

import (
	"fmt"
	"sync"
	"time"
)

// MultiPutRequest is a single entry in a vectorized write batch.
type MultiPutRequest struct {
	Key   string
	Value []byte
	TTL   int32 // seconds; -1 = immortal
}

// MultiGetResult holds the outcome for one key in a vectorized read.
type MultiGetResult struct {
	Key   string
	Value []byte
	Found bool
	Err   error
}

// MultiPut executes all writes, grouped by disk.
//
// KV-separation path: all writes to the same NVMe disk are staged via
// VLogBatcher and committed with a single fdatasync per disk — instead of
// one fdatasync per entry.  At 1024-entry batches this reduces fdatasync
// calls from 1024 to numDisks (typically 1–8), recovering 18K+ writes/s.
//
// Non-KV-separation path: grouped by shard, goroutines run in parallel.
func (se *StorageEngine) MultiPut(reqs []MultiPutRequest) []error {
	errs := make([]error, len(reqs))
	if len(reqs) == 0 {
		return errs
	}

	if se.config.KeyValueSeparation && len(se.vlogs) > 0 {
		// Secondary-index maintenance for batched writes (TXN commit, MPUT,
		// WriteBatcher). Zero cost when no index rules are registered. Old
		// values are captured before the batch so the diff-apply can remove
		// obsolete entries. The fallback path below needs none of this — it
		// funnels through se.Put, which maintains indexes itself.
		maintainIdx := indexRulesActive()
		var oldVals [][]byte
		if maintainIdx {
			oldVals = make([][]byte, len(reqs))
			for i, r := range reqs {
				if !isInternalIndexKey(r.Key) {
					oldVals[i], _ = se.Get(r.Key)
				}
			}
		}
		errs = se.multiPutKVSep(reqs, errs)
		if maintainIdx {
			for i, r := range reqs {
				if errs[i] != nil || isInternalIndexKey(r.Key) {
					continue
				}
				se.applySecondaryIndexes(r.Key, oldVals[i], r.Value)
			}
		}
		return errs
	}

	groups := make(map[uint16][]int, numShards)
	for i, r := range reqs {
		sid := uint16(fnv64a(r.Key) & (numShards - 1))
		groups[sid] = append(groups[sid], i)
	}

	var wg sync.WaitGroup
	wg.Add(len(groups))
	for sid, idxs := range groups {
		sid, idxs := sid, idxs
		go func() {
			defer wg.Done()
			_ = sid
			for _, i := range idxs {
				errs[i] = se.Put(reqs[i].Key, reqs[i].Value, reqs[i].TTL)
			}
		}()
	}
	wg.Wait()
	return errs
}

// multiPutKVSep is the hot path for MultiPut when KeyValueSeparation=true.
//
// All writes routed to the same disk share one VLogBatcher → one fdatasync.
// Each disk's work runs in its own goroutine so all disks proceed in parallel.
//
// Write order per disk (mirrors engine.Put — VLog before WAL):
//  1. VLogBatcher.Stage for each entry — fills 4 KB packed blocks in memory,
//     reserves offsets atomically. No disk I/O yet.
//  2. Build all WAL entries with VLogOffset + Packed already known. The WAL
//     header carries the packed bit so replay restores it.
//  3. Submit WAL appendAll AND VLogBatcher.Flush concurrently in two
//     goroutines, then wait for both. Effective P99 = max(WAL_fsync, VLog_fsync)
//     instead of the sum — same trick as engine.Put.
//  4. Index + cache update for entries that survived both phases.
//
// Why VLog before WAL (not after, like the previous version):
// ──────────────────────────────────────────────────────────
// With WAL-first, the WAL had to embed value bytes (vlogOffset=0) because
// the VLog offset wasn't known yet. After a crash, replay re-wrote those
// values to the VLog through the legacy unpacked path — orphaning the
// original packed records as garbage and regressing density until the next
// GC pass.
//
// With VLog-first, the WAL stores vlogOffset + packed flag. Replay sees
// vlogOffset > 0 and reuses the original packed VLog record directly —
// FlagPacked survives crash recovery. Same correctness story as engine.Put.
//
// Failure semantics:
//   - Both succeed → record is durable, index updated.
//   - VLog Flush fails → all entries on this disk fail; their staged bytes
//     are orphaned (next GC pass reclaims them).
//   - Per-entry WAL fails → that one entry's index update is skipped; its
//     VLog bytes are orphaned (next GC pass reclaims them).
//   - Both pending at crash time → client never received OK; replay sees no
//     WAL record → VLog bytes orphaned (next GC pass reclaims them).
func (se *StorageEngine) multiPutKVSep(reqs []MultiPutRequest, errs []error) []error {
	numDisks := len(se.vlogs)
	nowUs := time.Now().UnixMicro()
	nowNs := nowUs * 1000

	byDisk := make([][]int, numDisks)
	for i, r := range reqs {
		sid := uint16(fnv64a(r.Key) & (numShards - 1))
		d := diskForShard(sid, numDisks)
		byDisk[d] = append(byDisk[d], i)
	}

	var wg sync.WaitGroup
	for d, idxs := range byDisk {
		if len(idxs) == 0 {
			continue
		}
		d, idxs := d, idxs
		wg.Add(1)
		go func() {
			defer wg.Done()

			vl := se.vlogs[d]
			wal := se.wals[d]
			batcher := vl.NewBatcher()

			// Pre-compute CRC and shard for every entry in this disk's slice.
			type prepped struct {
				crc     uint32
				shardID uint16
			}
			prep := make([]prepped, len(idxs))
			for j, i := range idxs {
				r := reqs[i]
				prep[j] = prepped{
					crc:     computeCRC32C(r.Value),
					shardID: uint16(fnv64a(r.Key) & (numShards - 1)),
				}
			}

			// Phase 1: stage all values into the VLog batcher (in memory) AND
			// build the matching WAL entries with vlogOffset + packed bits.
			// MarkDead the old entry (if any) — it's superseded as soon as the
			// new write makes it durable; on Flush failure we just over-counted
			// dead bytes for one cycle, which the next GC pass corrects.
			type stagedEntry struct {
				reqIdx  int
				offset  int64
				shardID uint16
				crc     uint32
				packed  bool
			}
			committed := make([]stagedEntry, 0, len(idxs))
			walEntries := make([]*WALEntry, 0, len(idxs))
			walEntryIdx := make([]int, 0, len(idxs)) // walEntries[k] → original reqs index
			for j, i := range idxs {
				r := reqs[i]
				if old, _, exists := se.index.get(r.Key); exists && !old.IsTombstone() && old.DiskOffset > 0 {
					vl.MarkDead(old.ValueSize, old.IsPacked())
				}
				offset, err := batcher.Stage(r.Value)
				if err != nil {
					errs[i] = fmt.Errorf("vlog stage: %w", err)
					continue
				}
				isPacked := vlogHeaderBytes+len(r.Value) <= vlogBlockSize
				committed = append(committed, stagedEntry{
					reqIdx:  i,
					offset:  offset,
					shardID: prep[j].shardID,
					crc:     prep[j].crc,
					packed:  isPacked,
				})
				walEntries = append(walEntries, &WALEntry{
					Timestamp:  nowNs,
					Key:        r.Key,
					KeyLen:     uint32(len(r.Key)),
					ValueLen:   uint32(len(r.Value)),
					Value:      nil, // value lives in VLog after Flush
					Checksum:   prep[j].crc,
					Version:    se.version.Add(1),
					VLogOffset: offset,
					Packed:     isPacked,
				})
				walEntryIdx = append(walEntryIdx, i)
			}

			// Phase 2: WAL appendAll and VLog Flush run concurrently.
			// Effective fsync wait = max(WAL_fsync, VLog_fsync), not the sum.
			var (
				walErrs       []error
				vlogFlushErr  error
				phase2Wg      sync.WaitGroup
			)
			if len(walEntries) > 0 {
				phase2Wg.Add(2)
				go func() {
					defer phase2Wg.Done()
					walErrs = wal.appendAll(walEntries)
				}()
				go func() {
					defer phase2Wg.Done()
					vlogFlushErr = batcher.Flush()
				}()
				phase2Wg.Wait()
			}

			// Phase 3: apply errors. VLog Flush failure poisons every staged
			// entry on this disk; per-entry WAL failure poisons just that one.
			if vlogFlushErr != nil {
				for _, s := range committed {
					errs[s.reqIdx] = fmt.Errorf("vlog flush: %w", vlogFlushErr)
				}
				return
			}
			for k, werr := range walErrs {
				if werr != nil {
					errs[walEntryIdx[k]] = fmt.Errorf("WAL append: %w", werr)
				}
			}

			// Phase 4: index + cache update for entries that survived both.
			survived := 0
			for _, s := range committed {
				if errs[s.reqIdx] != nil {
					continue
				}
				r := reqs[s.reqIdx]
				entry := &IndexEntry{
					KeyHash:          fnv64a(r.Key),
					DiskOffset:       uint64(s.offset),
					SegmentID:        uint32(d),
					ValueSize:        uint32(len(r.Value)),
					UncompressedSize: uint32(len(r.Value)),
					KeySize:          uint32(len(r.Key)),
					WriteTimestampUs: nowUs,
					CRC32C:           s.crc,
					SchemaVersion:    CurrentSchemaVersion,
					ShardID:          s.shardID,
				}
				if s.packed {
					entry.Flags |= FlagPacked
				}
				if r.TTL > 0 {
					entry.Flags |= FlagHasTTL
					entry.TTLExpiryUs = nowUs + int64(r.TTL)*1_000_000
				}
				se.index.put(r.Key, entry, nil)
				se.cache.Put(r.Key, r.Value)
				survived++
			}
			se.metrics.VLogWrites.Add(uint64(survived))
			se.metrics.Writes.Add(uint64(survived))
		}()
	}
	wg.Wait()
	return errs
}

// MultiGet executes all reads concurrently, grouped by shard.
//
// Keys in the same shard are read sequentially under a single RLock, while
// different shards are read in parallel.  Returns one result per key in the
// same order as the input slice.
func (se *StorageEngine) MultiGet(keys []string) []MultiGetResult {
	results := make([]MultiGetResult, len(keys))
	if len(keys) == 0 {
		return results
	}

	groups := make(map[uint16][]int, numShards)
	for i, key := range keys {
		sid := uint16(fnv64a(key) & (numShards - 1))
		groups[sid] = append(groups[sid], i)
	}

	var wg sync.WaitGroup
	wg.Add(len(groups))
	for sid, idxs := range groups {
		sid, idxs := sid, idxs
		go func() {
			defer wg.Done()
			_ = sid
			for _, i := range idxs {
				val, err := se.Get(keys[i])
				results[i] = MultiGetResult{
					Key:   keys[i],
					Value: val,
					Found: err == nil && val != nil,
					Err:   err,
				}
			}
		}()
	}
	wg.Wait()
	return results
}
