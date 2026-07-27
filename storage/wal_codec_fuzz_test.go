package storage

import (
	"bufio"
	"bytes"
	"testing"
)

// FuzzWALCodecRoundTrip fuzzes the binary WAL codec: for arbitrary key/value
// bytes and flags, an encoded record must decode back to the same fields via
// BOTH readers (the streaming replayWAL reader and the buffer parser), and the
// consumed length must be exact. This is the property that the old text format
// violated for keys containing '|' or '\n'.
func FuzzWALCodecRoundTrip(f *testing.F) {
	// Seeds, including the bytes that broke the old text format.
	f.Add("plain", []byte("value"), false, false, int64(123), uint64(1), int64(0))
	f.Add("pipe|key", []byte("v|a|l"), false, false, int64(1), uint64(2), int64(0))
	f.Add("newline\nkey", []byte("li\nne"), false, false, int64(1), uint64(3), int64(0))
	f.Add("", []byte(nil), true, false, int64(9), uint64(4), int64(0))               // tombstone, empty key
	f.Add("kvsep", []byte("ignored"), false, true, int64(5), uint64(6), int64(4096)) // vlogOffset>0
	f.Add("\x00\xff\x7f", []byte("\x00\x01\xff"), false, false, int64(-1), uint64(0), int64(0))

	f.Fuzz(func(t *testing.T, key string, value []byte, tombstone, packed bool, ts int64, version uint64, vlogOffset int64) {
		// vlogOffset can't be negative on disk; normalise like the engine does.
		if vlogOffset < 0 {
			vlogOffset = -vlogOffset
		}
		valueCRC := computeCRC32C(value)
		valueLen := uint32(len(value))

		enc := encodeWALRecord(nil, ts, tombstone, packed, key, valueLen, value, valueCRC, version, vlogOffset)

		inlined := !tombstone && len(value) > 0 && vlogOffset == 0

		// 1) Streaming reader: a magic byte then the record.
		br := bufio.NewReader(bytes.NewReader(enc))
		marker, err := br.ReadByte()
		if err != nil || marker != walBinaryMagic {
			t.Fatalf("encoded record does not start with the binary magic")
		}
		rec, ok := readBinaryWALRecord(br)
		if !ok {
			t.Fatalf("readBinaryWALRecord failed to parse a well-formed record")
		}
		if rec.key != key {
			t.Fatalf("streaming key mismatch: got %q want %q", rec.key, key)
		}
		if rec.isTombstone != tombstone {
			t.Fatalf("streaming tombstone flag mismatch")
		}
		if rec.version != version || rec.timestampNs != ts {
			t.Fatalf("streaming version/ts mismatch")
		}
		if inlined && !bytes.Equal(rec.value, value) {
			t.Fatalf("streaming value mismatch: got %q want %q", rec.value, value)
		}
		if br.Buffered() != 0 {
			t.Fatalf("streaming reader left %d trailing bytes", br.Buffered())
		}

		// 2) Buffer parser: must consume the whole record, exactly.
		got, consumed, ok := decodeBinaryWALRecord(enc)
		if !ok {
			t.Fatalf("decodeBinaryWALRecord failed on a well-formed record")
		}
		if consumed != len(enc) {
			t.Fatalf("consumed %d of %d bytes", consumed, len(enc))
		}
		if got.key != key || got.isTombstone != tombstone || got.version != version {
			t.Fatalf("buffer decode field mismatch")
		}
		if inlined && !bytes.Equal(got.value, value) {
			t.Fatalf("buffer decode value mismatch")
		}
	})
}

// FuzzWALCodecCorruptionSafe fuzzes arbitrary bytes prefixed with the binary
// magic: the parser must NEVER panic and must reject anything it cannot fully
// and correctly parse (torn/corrupt tails are dropped, not mis-decoded). A
// clean "ok=false" or a byte-exact valid decode are both acceptable; a panic or
// an over-read is not.
func FuzzWALCodecCorruptionSafe(f *testing.F) {
	f.Add([]byte{walBinaryMagic, 0, 0, 0, 0})
	f.Add([]byte{walBinaryMagic, 0xff, 0xff, 0xff, 0xff}) // absurd length
	f.Add([]byte("random garbage that is not a record"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Prepend the magic so the buffer parser takes the binary branch.
		buf := append([]byte{walBinaryMagic}, data...)

		// Must not panic, and must not claim to consume more than it was given.
		rec, consumed, ok := decodeBinaryWALRecord(buf)
		if consumed < 0 || consumed > len(buf) {
			t.Fatalf("consumed=%d out of range for len=%d", consumed, len(buf))
		}
		if ok {
			// If it parsed, re-encoding must reproduce a record the parser reads
			// back identically (internal consistency of a "valid" decode).
			re := encodeWALRecord(nil, rec.timestampNs, rec.isTombstone, false, rec.key,
				rec.valueLen, rec.value, rec.crc, rec.version, rec.vlogOffset)
			rec2, c2, ok2 := decodeBinaryWALRecord(re)
			if !ok2 || c2 != len(re) || rec2.key != rec.key {
				t.Fatalf("re-encode of an accepted record did not round-trip")
			}
		}

		// The streaming reader must also never panic on the same bytes.
		br := bufio.NewReader(bytes.NewReader(buf))
		if b, err := br.ReadByte(); err == nil && b == walBinaryMagic {
			_, _ = readBinaryWALRecord(br) // ok/!ok both fine; must not panic
		}
	})
}
