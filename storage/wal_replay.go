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
//	8-field (current): timestamp|tombstone|key|valueLen|crc32hex|version|vlogOffset|packed\n
//	                   [raw value bytes]\n  ← present only when vlogOffset==0 && !tombstone && valueLen>0
//	The trailing 'packed' field ('0' or '1') restores FlagPacked onto the
//	rebuilt IndexEntry so MarkDead later subtracts the correct footprint.
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

		// SplitN with n=8 handles 6-field, 7-field, and 8-field formats:
		// shorter lines just leave the higher indices unset.
		parts := strings.SplitN(line, "|", 8)
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
		if len(parts) == 8 {
			// 8th field is "0" or "1"; anything else is a parse failure.
			switch parts[7] {
			case "0":
				packed = false
			case "1":
				packed = true
			default:
				break
			}
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
func applyWALReplay(
	entries []walReplayEntry,
	index *shardedIndex,
	vl *VLog,
	diskIdx int,
	numDisks int,
	kvSep bool,
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
			KeyHash:          fnv64a(e.key),
			ValueSize:        e.valueLen,
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
			offset, rp, err := vl.beginAppend(e.value)
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
// Format is identical to the normal 7-field WAL so replayWAL/applyWALReplay
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
		for key, entry := range shard.entries {
			if entry.IsTombstone() {
				continue // deleted keys are not checkpointed
			}

			ts := entry.WriteTimestampUs * 1000 // μs → ns (WAL stores nanoseconds)
			if ts == 0 {
				ts = now
			}

			// Serialise in the same 7-field pipe-delimited format as wal.serialize():
			// timestamp|0|key|valueLen|crcHex|version|vlogOffset\n
			buf = buf[:0]
			buf = strconv.AppendInt(buf, ts, 10)
			buf = append(buf, '|', '0', '|')
			buf = append(buf, key...)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, uint64(entry.ValueSize), 10)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, uint64(entry.CRC32C), 16)
			buf = append(buf, '|')
			buf = strconv.AppendUint(buf, version, 10)
			buf = append(buf, '|')

			if kvSep && entry.DiskOffset > 0 {
				// Value is durable in VLog — header-only record, no value bytes.
				buf = strconv.AppendInt(buf, int64(entry.DiskOffset), 10)
				buf = append(buf, '\n')
				_, writeErr = bw.Write(buf)
			} else {
				// Non-KV-sep (or missing vlogOffset): embed value bytes from the
				// dirty map.  If the dirty value is gone (already flushed to segment
				// before Close was called) we have no bytes to write — skip.
				dirtyVal := shard.dirtyValues[key]
				if len(dirtyVal) == 0 {
					continue
				}
				buf = append(buf, '0', '\n') // vlogOffset=0 → value bytes follow
				if _, writeErr = bw.Write(buf); writeErr == nil {
					if _, writeErr = bw.Write(dirtyVal); writeErr == nil {
						writeErr = bw.WriteByte('\n')
					}
				}
			}
		}
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
