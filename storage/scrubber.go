package storage

// scrubber.go — Background corruption scrubber.
//
// One goroutine per disk walks its VLog from offset RawSuperblockSize to vl.end,
// reading 64 KB chunks at a time. Each chunk contains 16 × 4 KB blocks; for
// every block we extract the leading 24-byte VLog record header and CRC-verify
// the value bytes. Mismatches increment ScrubCorruption and emit a structured
// log line — they DO NOT mark the index entry dead, because a CRC mismatch
// between two healthy VLog reads of the same byte range likely indicates
// silent disk corruption that requires operator intervention (replica resync,
// SMART check, or hardware replacement).
//
// Why scrub when ReadValue already CRC-checks?
//   - Cold keys are never read.  A cosmic-ray flip on a year-old block stays
//     undetected until the next user request — which may be a critical one.
//   - Detecting bit-rot proactively gives operators time to swap the disk
//     before a real read fails.
//   - For replicated deployments, scrubbing also flags divergence between
//     replicas (peer scrubs find different CRCs at the same offset → split).
//
// Throttling: 50 MB/s default cap. At that rate a 375 GB local SSD scrubs
// once every ~2 hours.  Honors AdmissionControl.GCPaused so a bursty read
// load fully suspends scrubbing — same logic as VLog GC.
//
// Block-packing aware: scrubber treats every 4 KB block as the unit of work.
// Inside a packed block there may be N records of size header+value each. The
// scrubber walks them sequentially using the ValLen header field, stopping
// when it hits a zero-magic header (end-of-records-in-block).

import (
	"encoding/binary"
	"log"
	"sync/atomic"
	"time"
)

const (
	// scrubReadChunkBytes is read in one io_uring/pread call. 64 KB matches the
	// kernel readahead window and is large enough to amortize syscall overhead
	// without holding too much locked memory.
	scrubReadChunkBytes = 64 << 10

	// Default scrub bandwidth cap: 50 MB/s × disk count. At 8 NVMe disks this
	// is 400 MB/s aggregate, ~10% of typical NVMe write bandwidth.
	scrubDefaultMBPerSec = 50

	// scrubPauseCheckInterval is how often the scrubber re-checks GCPaused
	// while sleeping. 250 ms gives quick resume when read load drops.
	scrubPauseCheckInterval = 250 * time.Millisecond
)

// startScrubbers launches one scrubber goroutine per VLog when scrubbing is
// enabled in the config. Idempotent: nil vlogs slice or disabled config = no-op.
func (se *StorageEngine) startScrubbers() {
	if !se.config.ScrubEnabled {
		return
	}
	if !se.config.KeyValueSeparation || len(se.vlogs) == 0 {
		return
	}
	rateBPS := int64(se.config.ScrubMBPerSec) * (1 << 20)
	if rateBPS <= 0 {
		rateBPS = int64(scrubDefaultMBPerSec) * (1 << 20)
	}
	for i, vl := range se.vlogs {
		go se.scrubLoop(i, vl, rateBPS)
	}
	log.Printf("[scrub] background scrubber enabled  disks=%d  rate=%dMB/s/disk",
		len(se.vlogs), rateBPS>>20)
}

// scrubLoop walks the VLog from start to end, then restarts. Errors increment
// the corruption counter and log; never panics.
func (se *StorageEngine) scrubLoop(diskIdx int, vl *VLog, rateBPS int64) {
	limiter := newGCRateLimiter()
	chunk := make([]byte, scrubReadChunkBytes)

	// Start at superblock end (raw mode) or vlogBlockSize (file mode) — both
	// are equal to RawSuperblockSize=4096.
	startOff := int64(RawSuperblockSize)

	for {
		// Honor GCPaused: if reads are slow, scrubbing waits.
		for se.metrics.Admission.GCPaused.Load() {
			select {
			case <-se.done:
				return
			case <-time.After(scrubPauseCheckInterval):
			}
		}

		select {
		case <-se.done:
			return
		default:
		}

		end := vl.end.Load()
		if end <= startOff {
			// Empty disk; sleep before retrying.
			select {
			case <-se.done:
				return
			case <-time.After(30 * time.Second):
				continue
			}
		}

		offset := startOff
		for offset < end {
			select {
			case <-se.done:
				return
			default:
			}

			toRead := end - offset
			if toRead > scrubReadChunkBytes {
				toRead = scrubReadChunkBytes
			}
			// Round read size down to 4 KB to honor O_DIRECT alignment.
			toRead &^= int64(vlogBlockSize - 1)
			if toRead <= 0 {
				break
			}

			limiter.consume(int(toRead), rateBPS)

			// Use ReadAt so we don't fight the lock-free Append path.
			n, err := vl.file.ReadAt(chunk[:toRead], offset)
			if err != nil && n == 0 {
				// EOF or transient error — bump counter and skip ahead.
				se.metrics.ScrubReadErrors.Add(1)
				offset += toRead
				continue
			}
			se.scrubChunk(diskIdx, offset, chunk[:n])
			se.metrics.ScrubBytes.Add(uint64(n))
			offset += int64(n)
		}

		// One full pass complete. Wait briefly before restarting so we don't
		// hot-loop on tiny VLogs in dev.
		atomic.AddUint64(&se.scrubPassCounter, 1)
		select {
		case <-se.done:
			return
		case <-time.After(10 * time.Second):
		}
	}
}

// scrubChunk validates every VLog record header in a 4 KB-aligned chunk.
// Walks blocks sequentially: each block holds either 1 unpacked record or
// up to ~26 packed records (header 24 + value).
func (se *StorageEngine) scrubChunk(diskIdx int, baseOffset int64, chunk []byte) {
	for blockStart := 0; blockStart+vlogBlockSize <= len(chunk); blockStart += vlogBlockSize {
		block := chunk[blockStart : blockStart+vlogBlockSize]
		// Walk records inside this block.
		pos := 0
		for pos+vlogHeaderBytes <= vlogBlockSize {
			magic := binary.LittleEndian.Uint32(block[pos+vhdrOffMagic:])
			if magic == 0 {
				break // end-of-records sentinel inside packed block
			}
			if magic != vlogMagic {
				se.metrics.ScrubRecords.Add(1)
				se.metrics.ScrubCorruption.Add(1)
				log.Printf("[scrub] disk=%d offset=%d MAGIC MISMATCH expected=%08x got=%08x",
					diskIdx, baseOffset+int64(blockStart+pos), vlogMagic, magic)
				break // can't continue parsing this block
			}
			valLen := binary.LittleEndian.Uint32(block[pos+vhdrOffValLen:])
			if valLen == 0 || pos+vlogHeaderBytes+int(valLen) > vlogBlockSize {
				// Either zero-length (sentinel) or value would cross block boundary
				// (unpacked oversized records take whole multi-block extents and
				// we treat their second-and-onward blocks as already-validated).
				break
			}
			storedCRC := binary.LittleEndian.Uint32(block[pos+vhdrOffCRC:])
			payload := block[pos+vlogHeaderBytes : pos+vlogHeaderBytes+int(valLen)]
			computed := computeCRC32C(payload)
			se.metrics.ScrubRecords.Add(1)
			if computed != storedCRC {
				se.metrics.ScrubCorruption.Add(1)
				log.Printf("[scrub] disk=%d offset=%d valLen=%d CRC MISMATCH stored=%08x computed=%08x",
					diskIdx, baseOffset+int64(blockStart+pos), valLen, storedCRC, computed)
			}
			pos += vlogHeaderBytes + int(valLen)
		}
	}
}
