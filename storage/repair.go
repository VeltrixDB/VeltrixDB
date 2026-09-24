package storage

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

// Repair for value-transform metadata lost by pre-fix builds.
//
// # What went wrong
//
// Before the fix, the WAL recorded only the plaintext length and no transform
// flags. Replay therefore rebuilt IndexEntry with:
//
//   - ValueSize        = whatever the WAL's valueLen field held
//   - UncompressedSize = the same value
//   - Flags            = missing FlagCompressed / FlagEncrypted
//
// The on-disk VLog bytes were always correct — only the metadata describing
// them was lost. That is what makes this repairable in place rather than by
// re-ingestion.
//
// Two distinct symptoms follow, depending on how the node last shut down:
//
//   - Clean shutdown (checkpoint): the checkpoint wrote entry.ValueSize into
//     the valueLen field, so ValueSize came back CORRECT but the flags were
//     still lost. Reads succeed and silently return the raw compressed or
//     encrypted blob. No error, no metric — the dangerous case.
//   - Crash (dirty WAL): valueLen was the plaintext length, so ValueSize came
//     back WRONG. Reads fail loudly with a CRC32C mismatch.
//
// # Why this repair is exact, not heuristic
//
// IndexEntry.CRC32C is the CRC32C of the PLAINTEXT value (Put computes it from
// its `value` argument before any transform runs) and it survived the bug
// untouched. So the repair does not have to guess which transforms applied: it
// enumerates the four possible interpretations of the on-disk blob and accepts
// only the one whose reconstruction hashes to the CRC the entry already
// carries. Anything else is reported and left alone.
//
// The record's true on-disk length likewise comes from the VLog record header
// (readRecordAtOffset), never from the ValueSize field under repair.

// transformRepairSampleLimit caps how many example keys a report retains, so
// a repair over a billion keys does not build a billion-element slice.
const transformRepairSampleLimit = 20

// TransformRepairReport summarises one scan or repair pass.
type TransformRepairReport struct {
	// DryRun records whether the pass mutated anything.
	DryRun bool

	EntriesScanned int // live, VLog-backed entries examined
	AlreadyHealthy int // metadata already agreed with the on-disk bytes
	Repaired       int // metadata corrected

	// Breakdown of what the repaired entries turned out to be.
	RepairedCompressed int
	RepairedEncrypted  int
	RepairedSizeOnly   int // flags were right, ValueSize/UncompressedSize were not

	// Entries that could not be resolved. Each is left untouched.
	BadMagic        int // no valid VLog record at DiskOffset
	BlobCRCMismatch int // record header's own CRC check failed — real corruption
	Unresolved      int // no interpretation reproduced IndexEntry.CRC32C

	// SampleRepaired / SampleUnresolved hold up to transformRepairSampleLimit
	// keys each, for an operator to spot-check by hand.
	SampleRepaired   []string
	SampleUnresolved []string
}

// NeedsRepair reports whether the scan found anything to fix.
func (r *TransformRepairReport) NeedsRepair() bool { return r.Repaired > 0 }

// HasUnresolved reports whether anything could not be explained. Unresolved
// entries are the ones worth escalating: they are neither healthy nor fixable
// by this tool.
func (r *TransformRepairReport) HasUnresolved() bool {
	return r.BadMagic > 0 || r.BlobCRCMismatch > 0 || r.Unresolved > 0
}

func (r *TransformRepairReport) String() string {
	mode := "repair"
	if r.DryRun {
		mode = "scan (dry run)"
	}
	s := fmt.Sprintf(
		"%s: scanned=%d healthy=%d repaired=%d (compressed=%d encrypted=%d size-only=%d) "+
			"bad-magic=%d blob-crc-mismatch=%d unresolved=%d",
		mode, r.EntriesScanned, r.AlreadyHealthy, r.Repaired,
		r.RepairedCompressed, r.RepairedEncrypted, r.RepairedSizeOnly,
		r.BadMagic, r.BlobCRCMismatch, r.Unresolved)
	if len(r.SampleRepaired) > 0 {
		s += fmt.Sprintf("\n  sample repaired keys:   %v", r.SampleRepaired)
	}
	if len(r.SampleUnresolved) > 0 {
		s += fmt.Sprintf("\n  sample unresolved keys: %v", r.SampleUnresolved)
	}
	return s
}

// repairCandidate is one interpretation of an on-disk blob.
type repairCandidate struct {
	plain []byte
	flags uint8 // transform bits this interpretation implies
}

// resolveBlob returns the interpretation of blob whose plaintext matches
// wantCRC, or ok=false when none does.
//
// Candidates are tried cheapest-first. Decryption is only attempted when a key
// is loaded; AES-GCM is authenticated, so a wrong guess fails cleanly rather
// than yielding plausible garbage. Compression is self-describing via its
// algorithm-prefix byte. The CRC check is what makes the result definitive —
// without it, a plaintext value whose first byte happened to be 0x01 could be
// mistaken for a flate blob.
func resolveBlob(blob []byte, wantCRC uint32) (repairCandidate, bool) {
	try := func(c repairCandidate) (repairCandidate, bool) {
		if c.plain != nil && computeCRC32C(c.plain) == wantCRC {
			return c, true
		}
		return repairCandidate{}, false
	}

	// 1. Stored as-is.
	if c, ok := try(repairCandidate{plain: blob, flags: 0}); ok {
		return c, true
	}
	// 2. Compressed only.
	if dec, ok := DecompressUnknownSize(blob); ok {
		if c, ok := try(repairCandidate{plain: dec, flags: FlagCompressed}); ok {
			return c, true
		}
	}
	if EncryptionEnabled() {
		pt, err := Decrypt(blob)
		if err == nil {
			// 3. Encrypted only.
			if c, ok := try(repairCandidate{plain: pt, flags: FlagEncrypted}); ok {
				return c, true
			}
			// 4. Compressed, then encrypted — the full pipeline.
			if dec, ok := DecompressUnknownSize(pt); ok {
				if c, ok := try(repairCandidate{plain: dec, flags: FlagCompressed | FlagEncrypted}); ok {
					return c, true
				}
			}
		}
	}
	return repairCandidate{}, false
}

// RepairTransformMetadata walks every live VLog-backed index entry, works out
// the true on-disk length and transform flags from the bytes themselves, and
// corrects the IndexEntry where they disagree.
//
// With dryRun=true nothing is mutated — the report says what a real run would
// change. Run a scan first.
//
// The corrected metadata only becomes durable when the engine writes its next
// checkpoint, which Close() does. The intended sequence, implemented by
// cmd/veltrix-repair, is: open engine → wait for ReplayDone → back up the WAL
// files → RepairTransformMetadata(false) → Close().
//
// Safe to run repeatedly: an entry whose metadata already matches its bytes is
// counted healthy and left alone.
//
// This must NOT run concurrently with client traffic. It takes each shard's
// write lock only per-entry, so a concurrent Put could interleave; the repair
// would then be operating on a superseded offset and its CRC check would
// simply fail the entry into Unresolved. Not corrupting, but noisy and
// incomplete — run it offline.
func (se *StorageEngine) RepairTransformMetadata(dryRun bool) (*TransformRepairReport, error) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return nil, fmt.Errorf("repair: only meaningful with key-value separation enabled")
	}

	rep := &TransformRepairReport{DryRun: dryRun}
	var mu sync.Mutex // guards rep

	numDisks := len(se.vlogs)
	var wg sync.WaitGroup
	// One goroutine per disk, each walking only the shards that route to it,
	// so the VLog reads spread across all NVMe queues instead of serialising.
	for d := 0; d < numDisks; d++ {
		wg.Add(1)
		go func(diskIdx int) {
			defer wg.Done()
			local := TransformRepairReport{}
			vl := se.vlogs[diskIdx]

			for i := diskIdx; i < numShards; i += numDisks {
				shard := &se.index.shards[i]

				// Snapshot the shard's keys under a read lock; do the VLog I/O
				// outside it so a slow disk never blocks the whole shard.
				shard.mu.RLock()
				keys := make([]string, 0, shard.entries.len())
				shard.entries.rangeAll(func(k string, e *IndexEntry) bool {
					if e.IsTombstone() || e.DiskOffset == 0 || e.Flags&FlagTiered != 0 {
						return true
					}
					keys = append(keys, k)
					return true
				})
				shard.mu.RUnlock()

				for _, key := range keys {
					entry, _, ok := se.index.get(key)
					if !ok || entry.IsTombstone() || entry.DiskOffset == 0 {
						continue
					}
					local.EntriesScanned++

					blob, storedCRC, err := vl.readRecordAtOffset(int64(entry.DiskOffset))
					if err != nil {
						local.BadMagic++
						addSample(&local.SampleUnresolved, key)
						continue
					}
					if computeCRC32C(blob) != storedCRC {
						// The record disagrees with its own header checksum:
						// genuine bit-rot or a bad offset, not a metadata bug.
						local.BlobCRCMismatch++
						addSample(&local.SampleUnresolved, key)
						continue
					}

					cand, ok := resolveBlob(blob, entry.CRC32C)
					if !ok {
						local.Unresolved++
						addSample(&local.SampleUnresolved, key)
						continue
					}

					wantValueSize := uint32(len(blob))
					wantUncompressed := uint32(len(cand.plain))
					curFlags := entry.Flags & (FlagCompressed | FlagEncrypted)

					if curFlags == cand.flags &&
						entry.ValueSize == wantValueSize &&
						entry.UncompressedSize == wantUncompressed {
						local.AlreadyHealthy++
						continue
					}

					local.Repaired++
					switch {
					case curFlags == cand.flags:
						local.RepairedSizeOnly++
					default:
						if cand.flags&FlagCompressed != 0 {
							local.RepairedCompressed++
						}
						if cand.flags&FlagEncrypted != 0 {
							local.RepairedEncrypted++
						}
					}
					addSample(&local.SampleRepaired, key)

					if dryRun {
						continue
					}
					se.index.repairTransformMetadata(key, entry.DiskOffset,
						wantValueSize, wantUncompressed, cand.flags)
				}
			}

			mu.Lock()
			mergeRepairReport(rep, &local)
			mu.Unlock()
		}(d)
	}
	wg.Wait()

	sort.Strings(rep.SampleRepaired)
	sort.Strings(rep.SampleUnresolved)
	return rep, nil
}

func addSample(dst *[]string, key string) {
	if len(*dst) < transformRepairSampleLimit {
		*dst = append(*dst, key)
	}
}

func mergeRepairReport(dst, src *TransformRepairReport) {
	dst.EntriesScanned += src.EntriesScanned
	dst.AlreadyHealthy += src.AlreadyHealthy
	dst.Repaired += src.Repaired
	dst.RepairedCompressed += src.RepairedCompressed
	dst.RepairedEncrypted += src.RepairedEncrypted
	dst.RepairedSizeOnly += src.RepairedSizeOnly
	dst.BadMagic += src.BadMagic
	dst.BlobCRCMismatch += src.BlobCRCMismatch
	dst.Unresolved += src.Unresolved
	for _, k := range src.SampleRepaired {
		addSample(&dst.SampleRepaired, k)
	}
	for _, k := range src.SampleUnresolved {
		addSample(&dst.SampleUnresolved, k)
	}
}

// repairTransformMetadata rewrites the transform-describing fields of an entry
// under the shard write lock.
//
// expectOffset guards against a concurrent Put having replaced the entry
// between the read and this write: if DiskOffset has moved, the bytes the
// repair inspected are no longer the ones this key points at, so the update is
// dropped. The new write came from fixed code and needs no repair anyway.
func (si *shardedIndex) repairTransformMetadata(
	key string, expectOffset uint64, valueSize, uncompressedSize uint32, xflags uint8,
) bool {
	shard, _ := si.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	applied := false
	shard.entries.update(key, fnv64a(key), func(entry *IndexEntry) bool {
		if entry.IsTombstone() || entry.DiskOffset != expectOffset {
			return false
		}
		entry.ValueSize = valueSize
		entry.UncompressedSize = uncompressedSize
		entry.Flags &^= FlagCompressed | FlagEncrypted
		entry.Flags |= xflags
		applied = true
		return true
	})
	return applied
}

// ── Startup health check ─────────────────────────────────────────────────────

// transformHealthSampleSize bounds the startup check. Large enough to catch a
// systemically damaged index with near-certainty (damage from the pre-fix bug
// affects every transformed record, not a scattered few), small enough that
// the I/O is irrelevant next to WAL replay.
const transformHealthSampleSize = 512

// CheckTransformMetadataHealth samples live entries and reports how many carry
// transform metadata that disagrees with their on-disk bytes.
//
// This exists because upgrading does NOT repair data damaged by the pre-fix
// build: the flags were never written to the WAL, so a fixed binary rebuilds
// the same wrong index and goes on silently returning compressed or encrypted
// blobs to clients. Nothing in the read path can notice — a blob whose length
// matches its header passes every CRC check there is.
//
// So the engine checks itself at startup and says so, loudly, rather than
// waiting for someone to notice their values look like binary garbage.
//
// Sampled and bounded: reads at most transformHealthSampleSize records. The
// damage is systemic when present, so a sample is enough to raise the alarm;
// veltrix-repair --scan is the exhaustive answer.
func (se *StorageEngine) CheckTransformMetadataHealth() (checked, damaged int) {
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return 0, 0
	}

	for i := range se.index.shards {
		if checked >= transformHealthSampleSize {
			break
		}
		shard := &se.index.shards[i]

		shard.mu.RLock()
		var keys []string
		shard.entries.rangeAll(func(k string, e *IndexEntry) bool {
			if e.IsTombstone() || e.DiskOffset == 0 || e.Flags&FlagTiered != 0 {
				return true
			}
			keys = append(keys, k)
			if len(keys) >= 8 { // a few per shard, spread across the keyspace
				return false
			}
			return true
		})
		shard.mu.RUnlock()

		for _, key := range keys {
			if checked >= transformHealthSampleSize {
				break
			}
			entry, _, ok := se.index.get(key)
			if !ok || entry.IsTombstone() || entry.DiskOffset == 0 {
				continue
			}
			vl := se.vlogs[int(entry.SegmentID)%len(se.vlogs)]
			blob, storedCRC, err := vl.readRecordAtOffset(int64(entry.DiskOffset))
			if err != nil || computeCRC32C(blob) != storedCRC {
				// Unreadable or genuinely corrupt — the scrubber's business,
				// not this check's. Don't inflate the damage count with it.
				continue
			}
			checked++
			cand, resolved := resolveBlob(blob, entry.CRC32C)
			if !resolved {
				continue // unresolvable; reported by veltrix-repair --scan
			}
			if cand.flags != entry.Flags&(FlagCompressed|FlagEncrypted) ||
				entry.ValueSize != uint32(len(blob)) {
				damaged++
			}
		}
	}
	return checked, damaged
}

// reportTransformHealth runs the startup check and escalates if it finds
// anything. Called once after WAL replay.
func (se *StorageEngine) reportTransformHealth() {
	checked, damaged := se.CheckTransformMetadataHealth()
	if checked == 0 {
		return
	}
	se.metrics.TransformMetadataDamaged.Store(uint64(damaged))
	if damaged == 0 {
		return
	}
	log.Printf("[repair] ****** VALUE-TRANSFORM METADATA DAMAGE DETECTED ******")
	log.Printf("[repair] %d of %d sampled records carry transform metadata that "+
		"disagrees with their on-disk bytes.", damaged, checked)
	log.Printf("[repair] This node is very likely returning raw compressed or " +
		"encrypted blobs to clients for affected keys, WITHOUT any error.")
	log.Printf("[repair] Cause: written by a build predating the value-transform " +
		"WAL fix. Upgrading does not repair it — the metadata was never stored.")
	log.Printf("[repair] Fix: stop this node and run")
	log.Printf("[repair]     veltrix-repair --data-dirs <dirs> --scan     # assess")
	log.Printf("[repair]     veltrix-repair --data-dirs <dirs> --repair   # fix")
	log.Printf("[repair] Metric: veltrixdb_storage_transform_metadata_damaged")
	log.Printf("[repair] ********************************************************")
}
