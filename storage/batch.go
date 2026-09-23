package storage

import (
	"fmt"
	"runtime"
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
//  1. VLogBatcher.Stage for each entry — fills 4 KB packed blocks inside one
//     contiguous in-memory extent. No disk I/O and no offset reservation yet.
//  2. VLogBatcher.Commit reserves the extent with a single vl.end.Add, making
//     every staged offset absolute; then build all WAL entries with
//     VLogOffset + Packed already known. The WAL header carries the packed bit
//     so replay restores it.
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

			// Phase 1: stage all values into the VLog batcher (in memory).
			// MarkDead the old entry (if any) — it's superseded as soon as the
			// new write makes it durable; on Flush failure we just over-counted
			// dead bytes for one cycle, which the next GC pass corrects.
			type stagedEntry struct {
				reqIdx  int
				offset  int64 // relative to the batch extent until Commit below
				shardID uint16
				crc     uint32
				packed  bool
				diskLen uint32 // on-disk blob length (post compress/encrypt)
				xflags  uint8  // FlagCompressed | FlagEncrypted
			}
			committed := make([]stagedEntry, 0, len(idxs))
			for j, i := range idxs {
				r := reqs[i]
				if old, _, exists := se.index.get(r.Key); exists && !old.IsTombstone() && old.DiskOffset > 0 {
					vl.MarkDead(old.ValueSize, old.IsPacked())
				}
				// Same compress→encrypt pipeline as the single-key Put path.
				// Skipping it here used to mean every batched write (MPUT, the
				// WriteBatcher, the server's PUT coalescing) stored plaintext
				// on disk even with --encrypt-at-rest on — silently, because
				// FlagEncrypted is per-record so reads simply never decrypted.
				writeBytes, xflags, xerr := se.transformForWrite(r.Value)
				if xerr != nil {
					errs[i] = xerr
					continue
				}
				relOff, isPacked, err := batcher.Stage(writeBytes)
				if err != nil {
					errs[i] = fmt.Errorf("vlog stage: %w", err)
					continue
				}
				committed = append(committed, stagedEntry{
					reqIdx:  i,
					offset:  relOff,
					shardID: prep[j].shardID,
					crc:     prep[j].crc,
					packed:  isPacked,
					diskLen: uint32(len(writeBytes)),
					xflags:  xflags,
				})
			}

			// Phase 1b: reserve the whole extent in one vl.end.Add and turn the
			// relative offsets absolute, then build the WAL entries — they carry
			// the absolute vlogOffset so replay can reuse the packed record
			// in place.
			//
			// The reservation has to happen here rather than inside Stage: the
			// batcher reserves its exact total so that all its blocks are
			// contiguous on disk, which is what lets Flush write them with a
			// single pwrite instead of one per 4 KB block.
			base := batcher.Commit()
			walEntries := make([]*WALEntry, 0, len(committed))
			for k := range committed {
				committed[k].offset += base
				s := committed[k]
				r := reqs[s.reqIdx]
				// Pooled, as the single-key Put path already does: these
				// die once appendAll has serialized them, and allocating one
				// per key cost 1024 allocations per 1024-entry batch.
				we := walEntryPool.Get().(*WALEntry)
				we.Timestamp = nowNs
				we.Key = r.Key
				we.KeyLen = uint32(len(r.Key))
				// ValueLen is the plaintext length; DiskValueLen is what
				// actually landed in the VLog. Replay needs both.
				we.ValueLen = uint32(len(r.Value))
				we.Value = nil // value lives in VLog after Flush
				we.Checksum = s.crc
				we.ReplicationID = 0
				we.IsTombstone = false
				we.Version = se.version.Add(1)
				we.VLogOffset = s.offset
				we.Packed = s.packed
				we.DiskValueLen = s.diskLen
				we.XformFlags = s.xflags
				walEntries = append(walEntries, we)
			}

			// Phase 2: WAL appendAll and VLog Flush run concurrently.
			// Effective fsync wait = max(WAL_fsync, VLog_fsync), not the sum.
			var (
				walErrs      []error
				vlogFlushErr error
				phase2Wg     sync.WaitGroup
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

				// appendAll has serialized every entry, so the envelopes can
				// go back. Done here rather than after phase 4 because
				// nothing below reads them.
				for _, we := range walEntries {
					walEntryPool.Put(we)
				}
			}

			// Phase 3: apply errors. VLog Flush failure poisons every staged
			// entry on this disk; per-entry WAL failure poisons just that one.
			if vlogFlushErr != nil {
				for _, s := range committed {
					errs[s.reqIdx] = fmt.Errorf("vlog flush: %w", vlogFlushErr)
				}
				return
			}
			// walEntries[k] is 1:1 with committed[k].
			for k, werr := range walErrs {
				if werr != nil {
					errs[committed[k].reqIdx] = fmt.Errorf("WAL append: %w", werr)
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
					KeyHash:    fnv64a(r.Key),
					DiskOffset: uint64(s.offset),
					SegmentID:  uint32(d),
					// ValueSize is the on-disk blob (what ReadValue pulls);
					// UncompressedSize is the plaintext the read path restores.
					ValueSize:        s.diskLen,
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
				entry.Flags |= s.xflags
				if r.TTL > 0 {
					entry.Flags |= FlagHasTTL
					entry.TTLExpiryUs = nowUs + int64(r.TTL)*1_000_000
				}
				se.index.put(r.Key, entry, nil)
				// Write-around: refresh the key if it is already cached (else a
				// reader would serve the superseded value), but do not insert
				// it. MultiPut backs bulk ingestion, and inserting there did two
				// unwanted things — it evicted the read working set with data
				// nobody had asked for, and it took the per-shard LIRS mutex
				// for a full insert-plus-eviction-scan while readers needed that
				// same mutex to serve a hit. Measured with 8 writers and 64
				// concurrent readers: batch P50 5.7 -> 3.2 ms, P99 13.8 -> 9.2 ms,
				// which closes essentially the whole read/write contention gap.
				// The single-key Put path stays write-through: one interactive
				// write is far more likely to be read back than one of a
				// thousand keys in a bulk batch.
				se.cache.PutIfPresent(r.Key, r.Value)
				survived++
			}
			se.metrics.VLogWrites.Add(uint64(survived))
			se.metrics.Writes.Add(uint64(survived))
		}()
	}
	wg.Wait()
	return errs
}

// multiGetParallelThreshold is the batch size below which MultiGet stays on
// the calling goroutine. Below it, goroutine scheduling costs more than the
// per-key work it parallelises.
const multiGetParallelThreshold = 16

// MultiGet reads many keys concurrently.
//
// Work is split into GOMAXPROCS contiguous chunks rather than grouped by index
// shard. The previous implementation built a map[uint16][]int preallocated for
// `numShards` (8192) entries on every call and launched one goroutine per
// distinct shard. Both were costly and neither bought anything:
//
//   - The 8192-entry map dominated the allocation profile: a 256-key batch
//     spent ~573 KB and ~1046 allocations to produce 256 results.
//   - Up to one goroutine per key was spawned, each performing a single Get.
//   - The grouping was meant to amortise a shard lock across same-shard keys,
//     but the body calls se.Get(), which takes and releases the shard lock
//     itself per key. Same-shard keys shared nothing, so grouping was pure
//     overhead.
//
// Chunking gives the same parallelism with a bounded, CPU-proportional
// goroutine count and no intermediate map. Result ordering is unchanged: each
// worker writes only its own disjoint index range, so no synchronisation is
// needed on `results`.
func (se *StorageEngine) MultiGet(keys []string) []MultiGetResult {
	results := make([]MultiGetResult, len(keys))
	if len(keys) == 0 {
		return results
	}

	read := func(i int) {
		val, err := se.Get(keys[i])
		results[i] = MultiGetResult{
			Key:   keys[i],
			Value: val,
			Found: err == nil && val != nil,
			Err:   err,
		}
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(keys) {
		workers = len(keys)
	}
	// Small batches are not worth the scheduling round-trip.
	if workers <= 1 || len(keys) < multiGetParallelThreshold {
		for i := range keys {
			read(i)
		}
		return results
	}

	chunk := (len(keys) + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < len(keys); start += chunk {
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				read(i)
			}
		}(start, end)
	}
	wg.Wait()
	return results
}
