package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// walReplayEntry is one parsed record from an on-disk WAL file.
type walReplayEntry struct {
	key         string
	value       []byte // non-nil only when value bytes were written into the WAL
	isTombstone bool
	version     uint64
	timestampNs int64
	crc         uint32
	valueLen    uint32
	vlogOffset  int64 // >0: value lives in VLog at this offset; 0: value bytes in WAL
	packed      bool  // 8-field format only: VLog record at vlogOffset is part of a packed 4 KB block
	// diskLen is the on-disk blob length (post-compression, post-encryption).
	// Equals valueLen for untransformed records and for pre-10-field WALs.
	diskLen uint32
	// xflags carries FlagCompressed|FlagEncrypted so replay can restore them
	// onto the rebuilt IndexEntry; without them the read path would hand back
	// ciphertext. Zero for pre-10-field WALs.
	xflags uint8
}

// replayWAL reads the WAL file at walPath and returns all valid, fully-written
// entries in the order they were appended. A partial entry at the tail — from a
// crash mid-write — is silently dropped; all entries before it are returned.
//
// Returns nil, nil when the file does not exist (fresh install, nothing to replay).
//
// WAL record formats:
//
//	6-field (legacy): timestamp|tombstone|key|valueLen|crc32hex|version\n
//	                  [raw value bytes]\n   ← present when !tombstone && valueLen>0
//
//	7-field (legacy KV-sep): timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset\n
//	                         [raw value bytes]\n  ← present only when vlogOffset==0 && !tombstone && valueLen>0
//
//	8-field (legacy): timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset|packed\n
//	                  [raw value bytes]\n  ← present only when vlogOffset==0 && !tombstone && valueLen>0
//	The 'packed' field ('0' or '1') restores FlagPacked onto the rebuilt
//	IndexEntry so MarkDead later subtracts the correct footprint.
//
//	10-field (current): ...|vlogOffset|packed|diskLen|xflags\n
//	                    [raw value bytes]\n  ← same condition as above
//	'diskLen' is the on-disk blob length after compression + encryption and
//	'xflags' (hex) carries FlagCompressed|FlagEncrypted. valueLen remains the
//	PLAINTEXT length. Shorter records parse with diskLen=valueLen and
//	xflags=0 — correct for them, since those builds wrote values
//	untransformed through this path.
func replayWAL(walPath string) ([]walReplayEntry, error) {
	f, err := os.Open(walPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20) // 1 MB read buffer
	var entries []walReplayEntry

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			// EOF or I/O error: entries parsed so far are valid.
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		// SplitN with n=10 handles the 6-, 7-, 8- and 10-field formats:
		// shorter lines just leave the higher indices unset.
		parts := strings.SplitN(line, "|", 10)
		if len(parts) < 6 {
			break // malformed header — stop, treat remainder as corrupt tail
		}

		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			break
		}
		isTombstone := parts[1] == "1"
		key := parts[2]
		valueLen, err := strconv.ParseUint(parts[3], 10, 32)
		if err != nil {
			break
		}
		crcU64, err := strconv.ParseUint(parts[4], 16, 32)
		if err != nil {
			break
		}
		version, err := strconv.ParseUint(parts[5], 10, 64)
		if err != nil {
			break
		}

		var vlogOffset int64
		if len(parts) >= 7 {
			off, err := strconv.ParseInt(parts[6], 10, 64)
			if err != nil {
				break
			}
			vlogOffset = off
		}
		var packed bool
		if len(parts) >= 8 {
			// 8th field is "0" or "1"; anything else leaves packed=false.
			packed = parts[7] == "1"
		}

		// Fields 9 and 10 (diskLen, xflags) describe the value transform.
		// Absent in older WALs: default diskLen to the plaintext length and
		// xflags to 0, which is exactly right for records written before the
		// batched/replay paths applied transforms.
		diskLen := uint32(valueLen)
		var xflags uint8
		if len(parts) >= 10 {
			dl, err := strconv.ParseUint(parts[8], 10, 32)
			if err != nil {
				break
			}
			xf, err := strconv.ParseUint(parts[9], 16, 8)
			if err != nil {
				break
			}
			if dl > 0 {
				diskLen = uint32(dl)
			}
			xflags = uint8(xf)
		}

		var value []byte
		// Value bytes follow in the WAL when:
		//   • not a tombstone
		//   • valueLen > 0
		//   • vlogOffset == 0  (KV-sep entries with vlogOffset>0 have no WAL bytes)
		if !isTombstone && valueLen > 0 && vlogOffset == 0 {
			value = make([]byte, valueLen)
			if _, err := io.ReadFull(br, value); err != nil {
				break // truncated — drop this entry and everything after
			}
			// Consume the '\n' delimiter that follows the value bytes.
			if b, err := br.ReadByte(); err != nil || b != '\n' {
				break
			}
			if computeCRC32C(value) != uint32(crcU64) {
				break // corrupted entry
			}
		}

		entries = append(entries, walReplayEntry{
			key:         key,
			value:       value,
			isTombstone: isTombstone,
			version:     version,
			timestampNs: ts,
			crc:         uint32(crcU64),
			valueLen:    uint32(valueLen),
			vlogOffset:  vlogOffset,
			packed:      packed,
			diskLen:     diskLen,
			xflags:      xflags,
		})
	}

	return entries, nil
}

// vlogReplayPending tracks a single old-format WAL entry that needs a VLog
// offset resolved before it can be committed to the index.
type vlogReplayPending struct {
	key     string
	entry   *IndexEntry
	respPtr *chan error
}

// applyWALReplay rebuilds the in-memory index for one disk from WAL entries.
//
// Each entry is routed by shard→disk: entries whose shard maps to a different
// disk are skipped (they will be handled by the replay call for that disk).
//
// KV-separation (kvSep=true):
//   - Entry with vlogOffset > 0  → IndexEntry built directly; no VLog I/O.
//   - Entry with value bytes (old WAL format) → value re-appended to vl via
//     beginAppend (non-blocking WriteAt + enqueued fdatasync). All such
//     submissions are batched: beginAppend for every entry is called before
//     any response is awaited, so the VLog flusher goroutine covers them all
//     in a handful of fdatasync calls instead of one per entry.
//
// Non-KV-separation (kvSep=false):
//   - Value stored as dirty in the index (same RAM layout as a live Put).
//
// Later entries in the WAL always supersede earlier entries for the same key
// because se.version is a monotonic counter: the highest version wins.
//
// Returns the maximum version seen; the caller stores this in se.version so
// new writes start from a strictly higher version than any replayed entry.
// transform is the engine's compress→encrypt pipeline (se.transformForWrite).
// It is only consulted on the legacy inline-value path, where plaintext bytes
// come out of the WAL and have to be written into the VLog fresh: without it a
// legacy WAL replayed onto a node running --encrypt-at-rest would land
// plaintext on disk. May be nil (tests, non-transforming configs).
func applyWALReplay(
	entries []walReplayEntry,
	index *shardedIndex,
	vl *VLog,
	diskIdx int,
	numDisks int,
	kvSep bool,
	transform func([]byte) ([]byte, uint8, error),
) uint64 {
	var maxVersion uint64

	// pending collects old-format KV-sep entries whose VLog fdatasync is in
	// flight.  All beginAppend calls are submitted before any response is read
	// so the VLog flusher can batch them into the fewest possible fdatasyncs.
	var pending []vlogReplayPending

	for _, e := range entries {
		shardID := uint16(fnv64a(e.key) & (numShards - 1))
		if diskForShard(shardID, numDisks) != diskIdx {
			continue
		}
		if e.version > maxVersion {
			maxVersion = e.version
		}

		nowUs := e.timestampNs / 1000

		if e.isTombstone {
			index.replayMarkTombstone(e.key, nowUs)
			continue
		}

		entry := &IndexEntry{
			KeyHash: fnv64a(e.key),
			// ValueSize is the ON-DISK blob length — the byte count ReadValue
			// must pull from the VLog. UncompressedSize is the plaintext
			// length the read path decompresses back to. They differ whenever
			// compression or encryption applied, which is why the WAL carries
			// both; conflating them made every transformed record fail its
			// CRC check after a crash restart.
			ValueSize:        e.diskLen,
			UncompressedSize: e.valueLen,
			KeySize:          uint32(len(e.key)),
			WriteTimestampUs: nowUs,
			CRC32C:           e.crc,
			SchemaVersion:    CurrentSchemaVersion,
			ShardID:          shardID,
		}
		if e.packed {
			entry.Flags |= FlagPacked
		}
		// Restore the transform bits so Get() decrypts/decompresses this
		// record exactly as it would have before the restart.
		entry.Flags |= e.xflags & (FlagCompressed | FlagEncrypted)

		switch {
		case kvSep && vl != nil && e.vlogOffset > 0:
			// New WAL format: VLog offset stored directly — no VLog I/O needed.
			entry.DiskOffset = uint64(e.vlogOffset)
			entry.SegmentID = uint32(diskIdx)
			index.replayPut(e.key, entry, nil)

		case kvSep && vl != nil && len(e.value) > 0:
			// Old WAL format: value bytes in WAL.  Submit the write non-blocking
			// (beginAppend does WriteAt + enqueues an fdatasync request) and
			// record the response channel for collection below.  Old VLog content
			// below the replay watermark is orphaned GC garbage.
			//
			// The WAL bytes are plaintext, so they go through the same
			// compress→encrypt pipeline a live Put would use before landing in
			// the VLog — otherwise replaying a legacy WAL on an encrypted node
			// would silently write plaintext to disk.
			writeBytes := e.value
			if transform != nil {
				tb, xf, terr := transform(e.value)
				if terr != nil {
					continue // cannot seal the value; key absent after restart
				}
				writeBytes = tb
				entry.Flags |= xf
			}
			entry.ValueSize = uint32(len(writeBytes))
			entry.UncompressedSize = uint32(len(e.value))
			offset, rp, err := vl.beginAppend(writeBytes)
			if err != nil {
				continue // disk error; key absent from index after restart
			}
			entry.DiskOffset = uint64(offset)
			entry.SegmentID = uint32(diskIdx)
			pending = append(pending, vlogReplayPending{key: e.key, entry: entry, respPtr: rp})

		case !kvSep && len(e.value) > 0:
			// Non-KV-sep: store value as dirty in the index (same as normal Put).
			index.replayPut(e.key, entry, e.value)
		}
	}

	// Collect all pending VLog fdatasync responses.  Because all beginAppend
	// calls were submitted above before any response is read, the VLog flusher
	// goroutine sees the full batch and covers it with far fewer fdatasyncs
	// (one per flush window, not one per entry).
	for _, p := range pending {
		if err := <-*p.respPtr; err == nil {
			index.replayPut(p.key, p.entry, nil)
		}
		vlogRespPool.Put(p.respPtr)
	}

	return maxVersion
}

// walPathForDir returns the WAL file path for a given disk directory.
// Mirrors the path used by newWriteAheadLog so replay and open agree.
func walPathForDir(dir string) string {
	return filepath.Join(dir, "wal.log")
}

// writeWALCheckpoint writes a compacted WAL containing exactly one record per
// live key assigned to diskIdx.  It is called by Close() instead of truncating
// the WAL to zero, so keys are never lost across clean restarts.
//
// Format is identical to the normal 10-field WAL so replayWAL/applyWALReplay
// can parse it without any changes.  For KV-sep entries the vlogOffset field
// is set (no value bytes written); for non-KV-sep entries value bytes follow.
//
// The write is crash-safe: we write to walPath+".ckpt", fdatasync, then
// rename to walPath — so a crash mid-write leaves the old WAL intact.
//
// Must be called after the WAL flusher goroutine has stopped (w.close()).
func writeWALCheckpoint(walPath string, index *shardedIndex, diskIdx, numDisks int, kvSep bool, version uint64) error {
	tmpPath := walPath + ".ckpt"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal checkpoint create: %w", err)
	}

	bw := bufio.NewWriterSize(f, 1<<20) // 1 MB write buffer
	now := time.Now().UnixNano()
	var writeErr error
	var buf []byte // reused across entries to avoid per-entry allocation

	for i := diskIdx; i < numShards && writeErr == nil; i += numDisks {
		shard := &index.shards[i]
		shard.mu.RLock()
		shard.entries.rangeAll(func(key string, entry *IndexEntry) bool {
			if entry.IsTombstone() {
				return true // deleted keys are not checkpointed
			}

			ts := entry.WriteTimestampUs * 1000 // μs → ns (WAL stores nanoseconds)
			if ts == 0 {
				ts = now
			}

			// Serialise in the same 10-field pipe-delimited format as
			// wal.serialize():
			//   timestamp|0|key|valueLen|crcHex|version|vlogOffset|packed|diskLen|xflags\n
			//
			// valueLen is the PLAINTEXT length (UncompressedSize) and diskLen
			// the on-disk blob (ValueSize); they differ under compression or
			// encryption. Emitting ValueSize as valueLen and dropping the
			// packed/compressed/encrypted flags — as this did while it wrote
			// 7 fields — meant a clean shutdown produced a checkpoint that
			// replayed into entries with the wrong read length and no
			// transform bits, i.e. unreadable values and a distorted GCRatio.
			plainLen := entry.UncompressedSize
			if plainLen == 0 {
				plainLen = entry.ValueSize
			}
			buf = buf[:0]
			buf = strconv.AppendInt(buf, ts, 10)
			buf = append(buf, '|', '0', '|')
			buf = append(buf, key...)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, uint64(plainLen), 10)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, uint64(entry.CRC32C), 16)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, version, 10)
			buf = append(buf, '|')

			if kvSep && entry.DiskOffset > 0 {
				// Value is durable in VLog — header-only record, no value bytes.
				buf = strconv.AppendInt(buf, int64(entry.DiskOffset), 10)
				buf = append(buf, '|')
				if entry.IsPacked() {
					buf = append(buf, '1')
				} else {
					buf = append(buf, '0')
				}
				// Same rollback insurance as wal.serialize: only emit the
				// transform fields when they carry information, so an
				// untransformed checkpoint stays readable by a pre-fix binary.
				xf := entry.Flags & (FlagCompressed | FlagEncrypted)
				if xf != 0 || entry.ValueSize != plainLen {
					buf = append(buf, '|')
					buf = strconv.AppendUint(buf, uint64(entry.ValueSize), 10)
					buf = append(buf, '|')
					buf = strconv.AppendUint(buf, uint64(xf), 16)
				}
				buf = append(buf, '\n')
				_, writeErr = bw.Write(buf)
			} else {
				// Non-KV-sep (or missing vlogOffset): embed value bytes from the
				// dirty map.  If the dirty value is gone (already flushed to segment
				// before Close was called) we have no bytes to write — skip.
				dirtyVal := shard.dirtyValues[key]
				if len(dirtyVal) == 0 {
					return true
				}
				// Dirty values are held in RAM as plaintext, so this record is
				// untransformed regardless of what the IndexEntry says: rewrite
				// the length field to match the bytes that actually follow.
				buf = buf[:0]
				buf = strconv.AppendInt(buf, ts, 10)
				buf = append(buf, '|', '0', '|')
				buf = append(buf, key...)
				buf = append(buf, '|')
				buf = strconv.AppendUint(buf, uint64(len(dirtyVal)), 10)
				buf = append(buf, '|')
				buf = strconv.AppendUint(buf, uint64(computeCRC32C(dirtyVal)), 16)
				buf = append(buf, '|')
				buf = strconv.AppendUint(buf, version, 10)
				// vlogOffset=0 → value bytes follow; unpacked, untransformed.
				buf = append(buf, '|', '0', '|', '0', '|')
				buf = strconv.AppendUint(buf, uint64(len(dirtyVal)), 10)
				buf = append(buf, '|', '0', '\n')
				if _, writeErr = bw.Write(buf); writeErr == nil {
					if _, writeErr = bw.Write(dirtyVal); writeErr == nil {
						writeErr = bw.WriteByte('\n')
					}
				}
			}
			return true
		})
		shard.mu.RUnlock()
	}

	if writeErr == nil {
		writeErr = bw.Flush()
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	f.Close()
	if writeErr != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("wal checkpoint write: %w", writeErr)
	}
	// Atomic rename: on Linux this is a single syscall — crash between Sync and
	// Rename leaves walPath+".ckpt" on disk; the next startup finds it, ignores
	// it (not named wal.log), and replays the old WAL intact.
	return os.Rename(tmpPath, walPath)
}

// maxVLogEndFromWAL returns one byte past the highest VLog position referenced
// by any parsed WAL entry belonging to diskIdx, or 0 when none do.
//
// This exists to close a restart race in raw block-device mode. A block device
// reports Stat().Size()==0, so newVLog cannot infer where prior writes ended
// and provisionally sets vl.end to vlogStart (4096). The authoritative seed is
// index.maxVLogEndOffset(), but the index is only rebuilt by the BACKGROUND
// replay goroutine — and the server starts accepting writes the moment
// NewStorageEngine returns. Any Put landing in that window reserves offsets
// from 4096 and overwrites live records the index still points at.
//
// Computing the seed straight from the parsed WAL entries needs no index, so
// it can run synchronously before the engine is handed out. The post-replay
// index-derived seed still runs afterwards; SetEndAtLeast is monotonic, so the
// two are complementary rather than conflicting.
//
// The alignment arithmetic mirrors shardedIndex.maxVLogEndOffset exactly:
// packed records round their end up to the enclosing 4 KB block; unpacked
// records own a whole aligned span. When in doubt this rounds UP — reserving
// too much VLog costs a little space, reserving too little corrupts data.
func maxVLogEndFromWAL(entries []walReplayEntry, diskIdx, numDisks int) uint64 {
	var maxEnd uint64
	for _, e := range entries {
		if e.vlogOffset <= 0 {
			continue // tombstone, or legacy inline value with no VLog position yet
		}
		shardID := uint16(fnv64a(e.key) & (numShards - 1))
		if diskForShard(shardID, numDisks) != diskIdx {
			continue
		}
		rawLen := uint64(vlogHeaderBytes) + uint64(e.diskLen)
		end := uint64(e.vlogOffset) + rawLen
		if e.packed {
			end = (end + uint64(vlogBlockSize) - 1) &^ uint64(vlogBlockSize-1)
		} else {
			alignedLen := (rawLen + uint64(vlogBlockSize) - 1) &^ uint64(vlogBlockSize-1)
			end = uint64(e.vlogOffset) + alignedLen
		}
		if end > maxEnd {
			maxEnd = end
		}
	}
	return maxEnd
}
