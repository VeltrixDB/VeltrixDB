package storage

import (
	"bufio"
	"encoding/binary"
	"io"
)

// wal_codec.go — binary, key-safe WAL record framing.
//
// The original WAL format was a pipe-delimited text line
// (`timestamp|tombstone|key|valueLen|crc|version|vlogOffset|packed\n`).
// That format is NOT binary-safe: a key containing '|' shifts every field and
// a key containing '\n' breaks record framing, so replay silently discards the
// offending record AND every record after it — a real data-loss-on-recovery
// bug because Put accepts arbitrary key bytes.
//
// This codec replaces the writer side with a length-prefixed, CRC-guarded
// binary record that carries an explicit keyLen, so keys may contain ANY bytes.
// Readers auto-detect the format from the first byte of each record:
//
//	0xB6            → binary record (this codec)
//	ASCII digit ... → legacy text record (timestamp is a decimal number)
//
// Legacy WALs written by older builds therefore keep replaying unchanged; new
// records are always written in the safe binary form.
//
// On-disk record:
//
//	magic     1  byte   = walBinaryMagic (0xB6)
//	bodyLen   4  bytes  uint32 LE   (length of body)
//	body      bodyLen bytes
//	bodyCRC   4  bytes  uint32 LE   (CRC32C of body — torn-write / corruption guard)
//
// body:
//
//	flags      1  byte   (bit0=tombstone, bit1=packed)
//	timestamp  8  bytes  int64  LE  (nanoseconds)
//	version    8  bytes  uint64 LE
//	valueCRC   4  bytes  uint32 LE  (CRC32C of the logical value)
//	vlogOffset 8  bytes  int64  LE  (>0 ⇒ value lives in the VLog, no inline bytes)
//	keyLen     4  bytes  uint32 LE
//	valueLen   4  bytes  uint32 LE  (logical value length — always the true size)
//	key        keyLen  bytes
//	value      valueLen bytes       (inlined only when !tombstone && valueLen>0 && vlogOffset==0)
const (
	walBinaryMagic byte = 0xB6
	walBodyFixed        = 1 + 8 + 8 + 4 + 8 + 4 + 4 // 37: flags..valueLen
	// walMaxBodyLen caps the declared body length so a corrupted length field
	// can't trigger a huge allocation. 256 MiB comfortably exceeds any single
	// key+value while still bounding damage from a garbage length.
	walMaxBodyLen = 256 << 20

	walFlagTombstone = 1 << 0
	walFlagPacked    = 1 << 1
)

// encodeWALRecord appends one binary WAL record to dst and returns the grown
// slice. valueLen is the LOGICAL value length and is always recorded, even in
// KV-separation mode where the bytes live in the VLog (vlogOffset > 0) and are
// not passed here. Value bytes are inlined only when vlogOffset == 0 and value
// is non-empty; the caller must then pass value with len(value) == valueLen
// (the non-KV-sep contract), mirroring the legacy text format's semantics.
func encodeWALRecord(dst []byte, tsNs int64, tombstone, packed bool, key string, valueLen uint32, value []byte, valueCRC uint32, version uint64, vlogOffset int64) []byte {
	if tombstone {
		valueLen = 0
	}
	inlineValue := !tombstone && len(value) > 0 && vlogOffset == 0

	bodyLen := walBodyFixed + len(key)
	if inlineValue {
		bodyLen += len(value)
	}

	var hdr [5]byte
	hdr[0] = walBinaryMagic
	binary.LittleEndian.PutUint32(hdr[1:], uint32(bodyLen))
	dst = append(dst, hdr[:]...)

	bodyStart := len(dst)
	var flags byte
	if tombstone {
		flags |= walFlagTombstone
	}
	if packed {
		flags |= walFlagPacked
	}
	dst = append(dst, flags)

	var num [8]byte
	binary.LittleEndian.PutUint64(num[:], uint64(tsNs))
	dst = append(dst, num[:]...)
	binary.LittleEndian.PutUint64(num[:], version)
	dst = append(dst, num[:]...)
	binary.LittleEndian.PutUint32(num[:4], valueCRC)
	dst = append(dst, num[:4]...)
	binary.LittleEndian.PutUint64(num[:], uint64(vlogOffset))
	dst = append(dst, num[:]...)
	binary.LittleEndian.PutUint32(num[:4], uint32(len(key)))
	dst = append(dst, num[:4]...)
	binary.LittleEndian.PutUint32(num[:4], valueLen)
	dst = append(dst, num[:4]...)

	dst = append(dst, key...)
	if inlineValue {
		dst = append(dst, value...)
	}

	bodyCRC := computeCRC32C(dst[bodyStart:])
	binary.LittleEndian.PutUint32(num[:4], bodyCRC)
	dst = append(dst, num[:4]...)
	return dst
}

// decodeWALBody parses a validated body buffer into its fields. It assumes the
// body length and CRC have already been checked by the caller.
func decodeWALBody(body []byte) (tsNs int64, tombstone, packed bool, key string, value []byte, valueCRC uint32, valueLen uint32, version uint64, vlogOffset int64, ok bool) {
	if len(body) < walBodyFixed {
		return
	}
	flags := body[0]
	tombstone = flags&walFlagTombstone != 0
	packed = flags&walFlagPacked != 0
	tsNs = int64(binary.LittleEndian.Uint64(body[1:9]))
	version = binary.LittleEndian.Uint64(body[9:17])
	valueCRC = binary.LittleEndian.Uint32(body[17:21])
	vlogOffset = int64(binary.LittleEndian.Uint64(body[21:29]))
	keyLen := binary.LittleEndian.Uint32(body[29:33])
	valueLen = binary.LittleEndian.Uint32(body[33:37])

	pos := walBodyFixed
	if uint64(pos)+uint64(keyLen) > uint64(len(body)) {
		return
	}
	key = string(body[pos : pos+int(keyLen)])
	pos += int(keyLen)

	if !tombstone && valueLen > 0 && vlogOffset == 0 {
		if uint64(pos)+uint64(valueLen) > uint64(len(body)) {
			return
		}
		value = body[pos : pos+int(valueLen)]
	}
	ok = true
	return
}

// readBinaryWALRecord reads one binary record from br. The magic byte has
// already been consumed by the caller (that is how the caller decided to take
// the binary path). It returns io.EOF-style errors as a false ok so the
// streaming replayer treats a torn or corrupt tail exactly like the legacy
// path does: stop and keep everything parsed so far.
func readBinaryWALRecord(br *bufio.Reader) (walReplayEntry, bool) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return walReplayEntry{}, false
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
	if bodyLen < walBodyFixed || bodyLen > walMaxBodyLen {
		return walReplayEntry{}, false // corrupt length — treat as tail
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(br, body); err != nil {
		return walReplayEntry{}, false
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
		return walReplayEntry{}, false
	}
	if computeCRC32C(body) != binary.LittleEndian.Uint32(crcBuf[:]) {
		return walReplayEntry{}, false // corrupted record — stop
	}

	tsNs, tombstone, packed, key, value, valueCRC, valueLen, version, vlogOffset, ok := decodeWALBody(body)
	if !ok {
		return walReplayEntry{}, false
	}
	// Inline values are verified against their CRC, mirroring the legacy reader.
	if len(value) > 0 && computeCRC32C(value) != valueCRC {
		return walReplayEntry{}, false
	}
	var valCopy []byte
	if len(value) > 0 {
		// body is reused per-record; copy the value out before returning.
		valCopy = append([]byte(nil), value...)
	}
	return walReplayEntry{
		key:         key,
		value:       valCopy,
		isTombstone: tombstone,
		version:     version,
		timestampNs: tsNs,
		crc:         valueCRC,
		valueLen:    valueLen,
		vlogOffset:  vlogOffset,
		packed:      packed,
	}, true
}

// decodeBinaryWALRecord parses one binary record from the front of data. It
// returns the record, the number of bytes consumed, and ok=false when the
// buffer holds only a partial or corrupt record (the caller then stops and
// retries the tail on a later pass, exactly like the legacy buffer parser).
func decodeBinaryWALRecord(data []byte) (archivedWALRecord, int, bool) {
	if len(data) < 5 {
		return archivedWALRecord{}, 0, false
	}
	bodyLen := binary.LittleEndian.Uint32(data[1:5])
	if bodyLen < walBodyFixed || bodyLen > walMaxBodyLen {
		return archivedWALRecord{}, 0, false
	}
	total := 5 + int(bodyLen) + 4
	if len(data) < total {
		return archivedWALRecord{}, 0, false // partial tail
	}
	body := data[5 : 5+int(bodyLen)]
	if computeCRC32C(body) != binary.LittleEndian.Uint32(data[5+int(bodyLen):total]) {
		return archivedWALRecord{}, 0, false
	}
	tsNs, tombstone, _, key, value, valueCRC, valueLen, version, vlogOffset, ok := decodeWALBody(body)
	if !ok {
		return archivedWALRecord{}, 0, false
	}
	return archivedWALRecord{
		timestampNs: tsNs,
		isTombstone: tombstone,
		key:         key,
		valueLen:    valueLen,
		crc:         valueCRC,
		version:     version,
		vlogOffset:  vlogOffset,
		value:       value,
	}, total, true
}
