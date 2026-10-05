package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
)

// wal_format.go — the on-disk WAL record encodings, and the ONE decoder every
// reader goes through (crash replay, the PITR archiver, PITR restore).
//
// # Binary records (written by default)
//
//	off  size  field
//	  0     1  magic 0xB1 — never a valid first byte of a text record, which
//	           always starts with an ASCII digit
//	  1     1  format version (1)
//	  2     2  flags: bit0 tombstone, bit1 packed, bit2 value bytes inline
//	  4     4  key length
//	  8     4  valueLen — PLAINTEXT length
//	 12     4  diskLen  — on-disk blob length after compress + encrypt
//	 16     4  CRC32C of the plaintext value (WALEntry.Checksum)
//	 20     1  xflags: FlagCompressed | FlagEncrypted
//	 21     3  zero
//	 24     8  timestamp, UnixNano
//	 32     8  engine version
//	 40     8  vlogOffset (0 = value inline, or none)
//	 48     …  key bytes, then value bytes when the inline flag is set
//	  end   4  CRC32C of every byte above
//
// All integers little-endian.
//
// # Version 2: records that carry a TTL
//
// A key written with a TTL gets a version-2 record (byte 1 = 2): the same
// 48 bytes, then
//
//	 48     8  TTL expiry — absolute Unix MICROSECONDS, int64 > 0
//	           (IndexEntry.TTLExpiryUs semantics: the deadline, not a duration)
//	 56     …  key bytes, then value bytes when the inline flag is set
//	  end   4  CRC32C of every byte above
//
// Records without a TTL are still written as version 1, byte for byte as
// before, so existing WAL files and the no-TTL hot path are unchanged; the
// decoder accepts both versions in any mix. Replay installs FlagHasTTL +
// TTLExpiryUs from the field, and a record already past its expiry at replay
// is applied as an expiry (the key is tombstoned, as the TTL scanner would),
// never resurrected. Before this field existed the WAL carried no TTL at all,
// so every TTL'd key came back immortal after any restart.
//
// A build without version-2 support stops decoding at the first such record —
// it fails that build's version check, which it treats like a torn tail — and
// drops it and every record after it. See "Mixed files and rollback".
//
// # Why it replaced the text format
//
// The text format was `ts|tomb|key|valueLen|crc|version|vlogOffset|packed|
// diskLen|xflags\n[value\n]`, parsed by splitting on '|' and reading lines on
// '\n'. Keys are arbitrary bytes. One key containing either delimiter ended
// replay at that record, and replay treats "cannot parse" as "torn tail" — so
// every acknowledged write after it was silently discarded on the next crash
// restart (TestWAL_CrashReplay_KeysWithDelimiters reproduces it). The header
// line also had no checksum at all: only inline values were CRC-checked, so a
// KV-separation record — the default, header only — could come back with a
// flipped digit in its vlogOffset and nothing would notice.
//
// Binary records are length-prefixed, so key bytes are never interpreted, and
// every record is checksummed end to end. They are also cheaper to produce
// (fixed-width puts instead of strconv) and to parse.
//
// # Mixed files and rollback
//
// The decoder reads both encodings, record by record, keyed on the first
// byte, so an upgraded node simply keeps appending binary records after the
// text ones already in wal.log. A pre-binary build cannot read binary
// records, and no build older than the TTL field can read a version-2 record
// or a text record with the 11th (TTL) field: both end its replay there, and
// everything after is lost. To roll back to ANY older build, restart the new
// build once with --wal-format=text-legacy and stop cleanly: the checkpoint
// rewrites wal.log as 8/10-field text with no TTL field, which every build
// since the text format reads. The price is that keys written with a TTL come
// back immortal on the older build — exactly what that build did to them on
// every restart anyway. --wal-format=text keeps the TTL field and is therefore
// only readable by builds with this change.

const (
	walBinMagic      = 0xB1
	walBinVersion    = 1 // record without a TTL
	walBinVersionTTL = 2 // record with the 8-byte TTL expiry at offset 48
	walBinHeaderSize = 48
	walBinTTLSize    = 8
	walBinTrailer    = 4

	walBinFlagTombstone = 1 << 0
	walBinFlagPacked    = 1 << 1
	walBinFlagInline    = 1 << 2

	// walMaxRecordField bounds key and value lengths read from disk before
	// anything is allocated for them, so a corrupt length field cannot ask
	// for gigabytes. 1 GiB is far above any value the server accepts.
	walMaxRecordField = 1 << 30
)

// errWALTail marks where decoding stopped short of a clean end of input: a
// record torn by a crash mid-write, or bytes that fail their checksum.
// Replay treats both the same way the text format always did — everything
// before is valid, nothing from here on is trusted.
var errWALTail = errors.New("wal: torn or corrupt record")

// appendWALRecordBinary appends one binary record for entry to buf.
func appendWALRecordBinary(buf []byte, entry *WALEntry) []byte {
	inline := !entry.IsTombstone && len(entry.Value) > 0 && entry.VLogOffset == 0
	var flags uint16
	if entry.IsTombstone {
		flags |= walBinFlagTombstone
	}
	if entry.Packed {
		flags |= walBinFlagPacked
	}
	if inline {
		flags |= walBinFlagInline
	}
	diskLen := entry.DiskValueLen
	if diskLen == 0 {
		diskLen = entry.ValueLen // no transform applied — on-disk == plaintext
	}

	start := len(buf)
	var h [walBinHeaderSize + walBinTTLSize]byte
	hdrLen := walBinHeaderSize
	h[0] = walBinMagic
	h[1] = walBinVersion
	if entry.TTLExpiryUs > 0 {
		h[1] = walBinVersionTTL
		binary.LittleEndian.PutUint64(h[walBinHeaderSize:], uint64(entry.TTLExpiryUs))
		hdrLen += walBinTTLSize
	}
	binary.LittleEndian.PutUint16(h[2:], flags)
	binary.LittleEndian.PutUint32(h[4:], uint32(len(entry.Key)))
	binary.LittleEndian.PutUint32(h[8:], entry.ValueLen)
	binary.LittleEndian.PutUint32(h[12:], diskLen)
	binary.LittleEndian.PutUint32(h[16:], entry.Checksum)
	h[20] = entry.XformFlags
	binary.LittleEndian.PutUint64(h[24:], uint64(entry.Timestamp))
	binary.LittleEndian.PutUint64(h[32:], entry.Version)
	binary.LittleEndian.PutUint64(h[40:], uint64(entry.VLogOffset))
	buf = append(buf, h[:hdrLen]...)
	buf = append(buf, entry.Key...)
	if inline {
		buf = append(buf, entry.Value...)
	}
	return binary.LittleEndian.AppendUint32(buf, computeCRC32C(buf[start:]))
}

// walReader decodes WAL records of either encoding from a byte stream.
type walReader struct {
	br  *bufio.Reader
	off int64 // bytes consumed by fully decoded records
	hdr [walBinHeaderSize + walBinTTLSize]byte
}

func newWALReader(r io.Reader) *walReader {
	return &walReader{br: bufio.NewReaderSize(r, 1<<20)}
}

// next returns the next record. io.EOF means the input ended cleanly on a
// record boundary; errWALTail means it did not (torn tail or corruption);
// any other error is an I/O error. After an error, off is the byte offset of
// the first record not returned.
func (r *walReader) next() (walReplayEntry, error) {
	for {
		b, err := r.br.Peek(1)
		if err != nil {
			if err == io.EOF {
				return walReplayEntry{}, io.EOF
			}
			return walReplayEntry{}, err
		}
		switch {
		case b[0] == walBinMagic:
			return r.nextBinary()
		case b[0] == '\n' || b[0] == '\r':
			// Blank lines between text records were always tolerated.
			r.br.ReadByte()
			r.off++
		default:
			return r.nextText()
		}
	}
}

func (r *walReader) nextBinary() (walReplayEntry, error) {
	if _, err := io.ReadFull(r.br, r.hdr[:walBinHeaderSize]); err != nil {
		return walReplayEntry{}, tailOrIOErr(err)
	}
	hdrLen := walBinHeaderSize
	switch r.hdr[1] {
	case walBinVersion:
	case walBinVersionTTL:
		if _, err := io.ReadFull(r.br, r.hdr[walBinHeaderSize:]); err != nil {
			return walReplayEntry{}, tailOrIOErr(err)
		}
		hdrLen += walBinTTLSize
	default:
		return walReplayEntry{}, errWALTail
	}
	h := r.hdr[:hdrLen]
	flags := binary.LittleEndian.Uint16(h[2:])
	keyLen := binary.LittleEndian.Uint32(h[4:])
	valueLen := binary.LittleEndian.Uint32(h[8:])
	inline := flags&walBinFlagInline != 0
	if keyLen > walMaxRecordField || (inline && valueLen > walMaxRecordField) {
		return walReplayEntry{}, errWALTail
	}
	bodyLen := int(keyLen)
	if inline {
		bodyLen += int(valueLen)
	}
	rec := make([]byte, hdrLen+bodyLen+walBinTrailer)
	copy(rec, h)
	if _, err := io.ReadFull(r.br, rec[hdrLen:]); err != nil {
		return walReplayEntry{}, tailOrIOErr(err)
	}
	crcAt := len(rec) - walBinTrailer
	if computeCRC32C(rec[:crcAt]) != binary.LittleEndian.Uint32(rec[crcAt:]) {
		return walReplayEntry{}, errWALTail
	}

	body := rec[hdrLen:crcAt]
	e := walReplayEntry{
		key:         string(body[:keyLen]),
		isTombstone: flags&walBinFlagTombstone != 0,
		packed:      flags&walBinFlagPacked != 0,
		valueLen:    valueLen,
		diskLen:     binary.LittleEndian.Uint32(h[12:]),
		crc:         binary.LittleEndian.Uint32(h[16:]),
		xflags:      h[20],
		timestampNs: int64(binary.LittleEndian.Uint64(h[24:])),
		version:     binary.LittleEndian.Uint64(h[32:]),
		vlogOffset:  int64(binary.LittleEndian.Uint64(h[40:])),
	}
	if hdrLen > walBinHeaderSize {
		if ttl := int64(binary.LittleEndian.Uint64(h[walBinHeaderSize:])); ttl > 0 {
			e.ttlExpiryUs = ttl
		}
	}
	if inline {
		e.value = body[keyLen:]
		if computeCRC32C(e.value) != e.crc {
			return walReplayEntry{}, errWALTail
		}
	}
	if e.diskLen == 0 {
		e.diskLen = e.valueLen
	}
	r.off += int64(len(rec))
	return e, nil
}

func tailOrIOErr(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return errWALTail
	}
	return err
}

// nextText decodes one legacy text record. The 6-, 7-, 8-, 10- and 11-field
// layouts are all accepted:
//
//	6:  timestamp|tombstone|key|valueLen|crc32hex|version
//	7:  …|vlogOffset
//	8:  …|vlogOffset|packed
//	10: …|vlogOffset|packed|diskLen|xflags
//	11: …|vlogOffset|packed|diskLen|xflags|ttlExpiryUs
//
// ttlExpiryUs (decimal, absolute Unix µs) is written only for keys with a
// TTL, and never under --wal-format=text-legacy. Parsers older than it split
// on at most 10 fields, so they read "xflags|ttl" as the xflags field, fail
// to parse it and stop replay there — which is why rollback uses text-legacy.
//
// followed by "value\n" when !tombstone && valueLen > 0 && vlogOffset == 0.
// Shorter layouts parse with diskLen = valueLen and xflags = 0 — correct for
// them, since those builds wrote values untransformed through this path.
func (r *walReader) nextText() (walReplayEntry, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return walReplayEntry{}, errWALTail // header without its newline
		}
		return walReplayEntry{}, err
	}
	consumed := int64(len(line))
	line = strings.TrimRight(line, "\r\n")

	parts := strings.SplitN(line, "|", 11)
	if len(parts) < 6 {
		return walReplayEntry{}, errWALTail
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return walReplayEntry{}, errWALTail
	}
	valueLen, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return walReplayEntry{}, errWALTail
	}
	crc, err := strconv.ParseUint(parts[4], 16, 32)
	if err != nil {
		return walReplayEntry{}, errWALTail
	}
	version, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return walReplayEntry{}, errWALTail
	}
	e := walReplayEntry{
		key:         parts[2],
		isTombstone: parts[1] == "1",
		version:     version,
		timestampNs: ts,
		crc:         uint32(crc),
		valueLen:    uint32(valueLen),
		diskLen:     uint32(valueLen),
	}
	if len(parts) >= 7 {
		off, err := strconv.ParseInt(parts[6], 10, 64)
		if err != nil {
			return walReplayEntry{}, errWALTail
		}
		e.vlogOffset = off
	}
	if len(parts) >= 8 {
		e.packed = parts[7] == "1"
	}
	if len(parts) >= 10 {
		dl, err := strconv.ParseUint(parts[8], 10, 32)
		if err != nil {
			return walReplayEntry{}, errWALTail
		}
		xf, err := strconv.ParseUint(parts[9], 16, 8)
		if err != nil {
			return walReplayEntry{}, errWALTail
		}
		if dl > 0 {
			e.diskLen = uint32(dl)
		}
		e.xflags = uint8(xf)
	}
	if len(parts) >= 11 {
		ttl, err := strconv.ParseInt(parts[10], 10, 64)
		if err != nil {
			return walReplayEntry{}, errWALTail
		}
		if ttl > 0 {
			e.ttlExpiryUs = ttl
		}
	}

	if !e.isTombstone && valueLen > 0 && e.vlogOffset == 0 {
		if valueLen > walMaxRecordField {
			return walReplayEntry{}, errWALTail
		}
		e.value = make([]byte, valueLen)
		if _, err := io.ReadFull(r.br, e.value); err != nil {
			return walReplayEntry{}, tailOrIOErr(err)
		}
		if b, err := r.br.ReadByte(); err != nil || b != '\n' {
			return walReplayEntry{}, errWALTail
		}
		if computeCRC32C(e.value) != e.crc {
			return walReplayEntry{}, errWALTail
		}
		consumed += int64(valueLen) + 1
	}
	r.off += consumed
	return e, nil
}
