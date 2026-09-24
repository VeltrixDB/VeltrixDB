package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// copyCrashImage copies a live engine's data directory. Every Put has been
// fdatasync'd to both WAL and VLog before it returned, so the copy is exactly
// what a power cut at this instant would leave — without the clean-shutdown
// checkpoint that Close() writes.
func copyCrashImage(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatalf("copy crash image: %v", err)
	}
	return dst
}

// TestWAL_CrashReplay_KeysWithDelimiters: keys may contain any bytes. The
// text WAL split its header on '|' and records on '\n', so one key holding
// either byte ended replay at that record and silently dropped every write
// after it.
func TestWAL_CrashReplay_KeysWithDelimiters(t *testing.T) {
	dir := t.TempDir()
	cfg := testStorageConfig(dir)
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()

	want := map[string][]byte{}
	put := func(k string, v []byte) {
		if err := se.Put(k, v, -1); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
		want[k] = v
	}
	put("before", []byte("v0"))
	put("pipe|in|key", []byte("v1"))
	put("newline\nin key", []byte("v2"))
	put("both|\n|", []byte("v3"))
	put("\x00binary\xff", []byte("v4"))
	for i := 0; i < 50; i++ {
		put(fmt.Sprint("after-", i), []byte(fmt.Sprint("val-", i)))
	}
	if err := se.Delete("before"); err != nil {
		t.Fatal(err)
	}
	delete(want, "before")

	img := copyCrashImage(t, dir)
	se2, err := NewStorageEngine(testStorageConfig(img))
	if err != nil {
		t.Fatal(err)
	}
	defer se2.Close()
	<-se2.ReplayDone

	for k, v := range want {
		got, err := se2.Get(k)
		if err != nil {
			t.Errorf("after crash replay Get(%q): %v", k, err)
			continue
		}
		if !bytes.Equal(got, v) {
			t.Errorf("after crash replay Get(%q) = %q, want %q", k, got, v)
		}
	}
	if _, err := se2.Get("before"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("deleted key resurrected by replay: err=%v", err)
	}
}

func binaryRecords(n int) ([]byte, []int) {
	var buf []byte
	var ends []int
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprint("value-", i))
		buf = appendWALRecordBinary(buf, &WALEntry{
			Timestamp: int64(i + 1), Key: fmt.Sprint("k|", i), Value: v,
			ValueLen: uint32(len(v)), Checksum: computeCRC32C(v), Version: uint64(i + 1),
		})
		ends = append(ends, len(buf))
	}
	return buf, ends
}

// TestWAL_BinaryTornTail cuts the file at every byte of the final record: a
// crash mid-write. Replay must return exactly the complete records before it,
// never an error and never a half record.
func TestWAL_BinaryTornTail(t *testing.T) {
	buf, ends := binaryRecords(3)
	path := filepath.Join(t.TempDir(), "wal.log")
	for cut := ends[1]; cut <= ends[2]; cut++ {
		if err := os.WriteFile(path, buf[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := replayWAL(path)
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		want := 2
		if cut == ends[2] {
			want = 3
		}
		if len(got) != want {
			t.Fatalf("cut=%d: replayed %d records, want %d", cut, len(got), want)
		}
	}
}

// TestWAL_BinaryCorruptionStopsReplay flips each byte of the middle record in
// turn. The text format checksummed only inline values, so a flipped digit in
// a header — a vlogOffset, a version — replayed as a different, valid-looking
// record. Every binary byte is covered by the record CRC.
func TestWAL_BinaryCorruptionStopsReplay(t *testing.T) {
	buf, ends := binaryRecords(3)
	path := filepath.Join(t.TempDir(), "wal.log")
	for i := ends[0]; i < ends[1]; i++ {
		bad := append([]byte(nil), buf...)
		bad[i] ^= 0x40
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := replayWAL(path)
		if err != nil {
			t.Fatalf("flip@%d: %v", i, err)
		}
		if len(got) != 1 || got[0].key != "k|0" {
			t.Fatalf("flip@%d: replayed %d records, want only the one before the damage", i, len(got))
		}
	}
}

// TestWAL_MixedTextThenBinary is the upgrade path: a wal.log written by a
// text-format build, appended to by a binary one. Replay reads straight
// through the switch.
func TestWAL_MixedTextThenBinary(t *testing.T) {
	var buf []byte
	for i := 0; i < 3; i++ {
		v := []byte(fmt.Sprint("t", i))
		buf = appendWALRecordText(buf, &WALEntry{Timestamp: int64(i + 1), Key: fmt.Sprint("text-", i),
			Value: v, ValueLen: uint32(len(v)), Checksum: computeCRC32C(v), Version: uint64(i + 1)})
	}
	bin, _ := binaryRecords(3)
	buf = append(buf, bin...)
	path := filepath.Join(t.TempDir(), "wal.log")
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := replayWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 || got[2].key != "text-2" || got[3].key != "k|0" || string(got[5].value) != "value-2" {
		t.Fatalf("mixed replay returned %d records: %+v", len(got), got)
	}
}

// TestWAL_TextFormatRollback: with WALFormat "text", a clean shutdown must
// leave a wal.log a pre-binary build can read — text records only.
func TestWAL_TextFormatRollback(t *testing.T) {
	dir := t.TempDir()
	cfg := testStorageConfig(dir)
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := se.Put(fmt.Sprint("rb-", i), []byte("v"), -1); err != nil {
			t.Fatal(err)
		}
	}
	se.Close() // binary checkpoint

	cfg = testStorageConfig(dir)
	cfg.WALFormat = "text"
	se, err = NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	se.Close() // text checkpoint

	data, err := os.ReadFile(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.IndexByte(data, walBinMagic) >= 0 {
		t.Fatal("text-format checkpoint still contains binary records")
	}
	se, err = NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()
	<-se.ReplayDone
	for i := 0; i < 20; i++ {
		if _, err := se.Get(fmt.Sprint("rb-", i)); err != nil {
			t.Fatalf("after text checkpoint: %v", err)
		}
	}
}

func TestWAL_InvalidFormatRejected(t *testing.T) {
	cfg := testStorageConfig(t.TempDir())
	cfg.WALFormat = "json"
	if se, err := NewStorageEngine(cfg); err == nil {
		se.Close()
		t.Fatal("WALFormat=json accepted")
	}
}

// TestPITR_ArchivesCompressedValues: the archiver used to read the VLog with
// the PLAINTEXT length. For a compressed record that is the wrong length, the
// read failed its CRC, and the write was counted "skipped" — absent from the
// archive with nothing but a counter to show for it.
func TestPITR_ArchivesCompressedValues(t *testing.T) {
	dir, archiveDir := t.TempDir(), t.TempDir()
	cfg := testStorageConfig(dir)
	cfg.ArchiveDir = archiveDir
	cfg.ArchiveIntervalMs = 10
	cfg.Compression = "flate"
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	arch, err := StartWALArchiver(se)
	if err != nil || arch == nil {
		t.Fatalf("StartWALArchiver: %v", err)
	}
	want := map[string]string{}
	for i := 0; i < 20; i++ {
		k, v := fmt.Sprint("z-", i), strings.Repeat(fmt.Sprint("compress me ", i, " "), 64)
		if err := se.Put(k, []byte(v), -1); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	arch.Stop()
	defer se.Close()
	if n := arch.EntriesSkipped.Load(); n != 0 {
		t.Fatalf("archiver skipped %d compressed records", n)
	}
	segs, err := ListArchiveSegments(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range segs {
		data, err := os.ReadFile(s.SegPath)
		if err != nil {
			t.Fatal(err)
		}
		recs, _ := parseWALBuffer(data)
		for _, r := range recs {
			got[r.key] = string(r.value)
		}
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("archived %q: got %d bytes, want the %d-byte plaintext", k, len(got[k]), len(v))
		}
	}
}

func benchWALEntry() *WALEntry {
	return &WALEntry{Timestamp: 1_700_000_000_000_000_000, Key: "user:0000000000123456789", KeyLen: 24,
		ValueLen: 128, Checksum: 0xDEADBEEF, Version: 123456789, VLogOffset: 987654321, Packed: true}
}

// BenchmarkWAL_Encode is the per-record cost on the group-commit path, for a
// KV-separation header-only record (the default shape).
func BenchmarkWAL_Encode(b *testing.B) {
	for _, text := range []bool{true, false} {
		b.Run(map[bool]string{true: "text", false: "binary"}[text], func(b *testing.B) {
			e := benchWALEntry()
			buf := make([]byte, 0, 256)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf = appendWALRecordFor(buf[:0], e, text)
			}
			b.SetBytes(int64(len(buf)))
		})
	}
}

// BenchmarkWAL_Replay is restart cost: decoding 100K records.
func BenchmarkWAL_Replay(b *testing.B) {
	for _, text := range []bool{true, false} {
		b.Run(map[bool]string{true: "text", false: "binary"}[text], func(b *testing.B) {
			var data []byte
			e := benchWALEntry()
			for i := 0; i < 100_000; i++ {
				e.Version = uint64(i)
				data = appendWALRecordFor(data, e, text)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				recs, n := parseWALBuffer(data)
				if n != len(data) || len(recs) != 100_000 {
					b.Fatalf("parsed %d/%d", len(recs), n)
				}
			}
		})
	}
}
