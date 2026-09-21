package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Regressions for the value-transform pipeline (compress → encrypt) and the
// metadata it must carry across a restart.
//
// Every test here failed before the fix, and each one failed for a reason the
// pre-existing suite could not see: the older crash-recovery tests all used
// values like "recover-val-7" — 13 bytes, below the 256-byte compression
// threshold — so no test ever put a transformed value through a restart.

// compressibleValue returns a value that is both over the compression
// threshold and highly redundant, so MaybeCompress reliably succeeds.
func compressibleValue(tag string) []byte {
	return []byte(strings.Repeat("veltrix-"+tag+"-payload:", 40)) // ~900 B
}

// transformTestEngine builds a single-disk engine on dir with tight flush
// windows and no background noise.
func transformTestEngine(t *testing.T, dir string) *StorageEngine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	cfg.DefragInterval = time.Hour // keep GC out of these assertions

	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	return se
}

// enableTestEncryption installs a deterministic AES-256 key for the duration
// of the test.
func enableTestEncryption(t *testing.T) {
	t.Helper()
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x37}, 32))
	t.Setenv(EncryptionKeyEnvVar, key)
	enc, err := loadEncryptor("")
	if err != nil {
		t.Fatalf("load encryptor: %v", err)
	}
	setEncryptor(enc)
	t.Cleanup(func() { setEncryptor(nil) })
	if !EncryptionEnabled() {
		t.Fatal("EncryptionEnabled() = false after installing a key")
	}
}

// ── Bug: transform metadata lost across restart ──────────────────────────────

// Before the fix the WAL recorded only the PLAINTEXT length and no transform
// flags. Replay therefore rebuilt IndexEntry.ValueSize from the plaintext
// length while the VLog held a compressed blob of a different size, so
// ReadValue pulled the wrong byte count and failed its CRC check. Every
// compressed key became unreadable after a restart — and zstd compression is
// on by default.
func TestCrashRecovery_CompressedValueSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	se1 := transformTestEngine(t, dir)
	want := map[string][]byte{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("compressed-key-%d", i)
		v := compressibleValue(fmt.Sprintf("%d", i))
		if err := se1.Put(k, v, -1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		want[k] = v
	}
	if err := se1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	se2 := transformTestEngine(t, dir)
	t.Cleanup(func() { se2.Close() })

	for k, v := range want {
		got, err := se2.Get(k)
		if err != nil {
			t.Fatalf("Get %s after restart: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after restart: got %d bytes, want %d", k, len(got), len(v))
		}
	}
}

// Same failure mode, via encryption rather than compression: the ciphertext is
// plaintext+28 bytes, so a ValueSize taken from the plaintext length reads
// short and fails CRC. The flags matter independently — without
// FlagEncrypted restored, Get would hand back raw ciphertext.
func TestCrashRecovery_EncryptedValueSurvivesRestart(t *testing.T) {
	enableTestEncryption(t)
	dir := t.TempDir()

	se1 := transformTestEngine(t, dir)
	want := map[string][]byte{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("encrypted-key-%d", i)
		v := []byte(fmt.Sprintf("secret-value-%d", i))
		if err := se1.Put(k, v, -1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		want[k] = v
	}
	if err := se1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	se2 := transformTestEngine(t, dir)
	t.Cleanup(func() { se2.Close() })

	for k, v := range want {
		got, err := se2.Get(k)
		if err != nil {
			t.Fatalf("Get %s after restart: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after restart: got %q, want %q", k, got, v)
		}
	}
}

// The two tests above restart through Close(), which rebuilds the WAL as a
// checkpoint. This one restarts WITHOUT Close() — an unclean shutdown that
// leaves the real WAL records in place — so it covers the ordinary
// serialize/replay path rather than the checkpoint writer.
func TestCrashRecovery_DirtyShutdownKeepsTransformMetadata(t *testing.T) {
	enableTestEncryption(t)
	dir := t.TempDir()

	// Deliberately not closed: simulates a process kill. Put has already
	// returned, so WAL and VLog are both fdatasync'd and the WAL is intact
	// (no checkpoint was written over it).
	se1 := transformTestEngine(t, dir)
	want := map[string][]byte{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("dirty-key-%d", i)
		v := compressibleValue(fmt.Sprintf("dirty%d", i))
		if err := se1.Put(k, v, -1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		want[k] = v
	}

	se2 := transformTestEngine(t, dir)
	t.Cleanup(func() { se2.Close() })

	for k, v := range want {
		got, err := se2.Get(k)
		if err != nil {
			t.Fatalf("Get %s after dirty restart: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s after dirty restart: got %d bytes, want %d", k, len(got), len(v))
		}
	}
}

// ── Bug: batched writes skipped the transform pipeline entirely ──────────────

// MultiPut backs MPUT, the WriteBatcher and the server's PUT coalescing — the
// paths the README tells users to prefer. Before the fix none of them called
// the compress/encrypt pipeline, so --encrypt-at-rest silently stored
// plaintext. Reads still worked (FlagEncrypted is per-record, so nothing tried
// to decrypt), which is what made it invisible.
//
// This asserts on the bytes actually on disk, not on round-trip behaviour.
func TestMultiPut_EncryptsValuesOnDisk(t *testing.T) {
	enableTestEncryption(t)
	dir := t.TempDir()

	se := transformTestEngine(t, dir)

	const marker = "TOP-SECRET-PLAINTEXT-MARKER"
	reqs := make([]MultiPutRequest, 0, 8)
	for i := 0; i < 8; i++ {
		reqs = append(reqs, MultiPutRequest{
			Key:   fmt.Sprintf("mput-secret-%d", i),
			Value: []byte(fmt.Sprintf("%s-%d", marker, i)),
			TTL:   -1,
		})
	}
	for i, err := range se.MultiPut(reqs) {
		if err != nil {
			t.Fatalf("MultiPut[%d]: %v", i, err)
		}
	}

	// Round-trip must still work.
	for _, r := range reqs {
		got, err := se.Get(r.Key)
		if err != nil {
			t.Fatalf("Get %s: %v", r.Key, err)
		}
		if !bytes.Equal(got, r.Value) {
			t.Fatalf("Get %s = %q, want %q", r.Key, got, r.Value)
		}
	}

	// The index must say these records are encrypted.
	for _, r := range reqs {
		entry, _, ok := se.index.get(r.Key)
		if !ok {
			t.Fatalf("index entry missing for %s", r.Key)
		}
		if !entry.IsEncrypted() {
			t.Errorf("key %s: FlagEncrypted not set on a MultiPut write", r.Key)
		}
	}

	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// And the plaintext must not be findable anywhere in the VLog.
	vlogPath := filepath.Join(dir, "vlog_active.dat")
	raw, err := os.ReadFile(vlogPath)
	if err != nil {
		t.Fatalf("read vlog %s: %v", vlogPath, err)
	}
	if bytes.Contains(raw, []byte(marker)) {
		t.Fatalf("plaintext marker %q found in %s — MultiPut wrote unencrypted values "+
			"despite encryption being enabled", marker, vlogPath)
	}
}

// Batched writes must also survive a restart with their transform metadata,
// for the same reason single Put must.
func TestMultiPut_TransformedValuesSurviveRestart(t *testing.T) {
	enableTestEncryption(t)
	dir := t.TempDir()

	se1 := transformTestEngine(t, dir)
	reqs := make([]MultiPutRequest, 0, 32)
	for i := 0; i < 32; i++ {
		reqs = append(reqs, MultiPutRequest{
			Key:   fmt.Sprintf("mput-restart-%d", i),
			Value: compressibleValue(fmt.Sprintf("mp%d", i)),
			TTL:   -1,
		})
	}
	for i, err := range se1.MultiPut(reqs) {
		if err != nil {
			t.Fatalf("MultiPut[%d]: %v", i, err)
		}
	}
	if err := se1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	se2 := transformTestEngine(t, dir)
	t.Cleanup(func() { se2.Close() })

	for _, r := range reqs {
		got, err := se2.Get(r.Key)
		if err != nil {
			t.Fatalf("Get %s after restart: %v", r.Key, err)
		}
		if !bytes.Equal(got, r.Value) {
			t.Fatalf("key %s after restart: got %d bytes, want %d", r.Key, len(got), len(r.Value))
		}
	}
}

// transformForWrite is the single chokepoint every VLog write path must use.
func TestTransformForWrite_OrderAndFlags(t *testing.T) {
	dir := t.TempDir()
	se := transformTestEngine(t, dir)
	t.Cleanup(func() { se.Close() })

	// Compression only (no key installed).
	val := compressibleValue("order")
	out, flags, err := se.transformForWrite(val)
	if err != nil {
		t.Fatalf("transformForWrite: %v", err)
	}
	if flags&FlagCompressed == 0 {
		t.Errorf("FlagCompressed not set for a %d-byte redundant value", len(val))
	}
	if flags&FlagEncrypted != 0 {
		t.Error("FlagEncrypted set with no key installed")
	}
	if len(out) >= len(val) {
		t.Errorf("compressed length %d not smaller than input %d", len(out), len(val))
	}

	// With encryption on, the blob must be sealed as well — and compression
	// must still have run first, so the result stays well under the plaintext
	// size plus the 28-byte AEAD overhead.
	enableTestEncryption(t)
	out2, flags2, err := se.transformForWrite(val)
	if err != nil {
		t.Fatalf("transformForWrite (encrypted): %v", err)
	}
	if flags2&FlagCompressed == 0 || flags2&FlagEncrypted == 0 {
		t.Errorf("flags = 0x%02x, want both FlagCompressed and FlagEncrypted", flags2)
	}
	if len(out2) >= len(val) {
		t.Errorf("compress-then-encrypt produced %d bytes for a %d-byte value — "+
			"suggests encryption ran before compression", len(out2), len(val))
	}
}

// ── Bug: GC mislabelled oversized relocations as packed ──────────────────────

// Stage falls back to an unpacked, dedicated span for records that exceed a
// 4 KB block. Reporting those as packed makes MarkDead subtract only
// header+value instead of the whole aligned span, so liveBytes drifts
// permanently high and GCRatio under-reports garbage.
func TestVLogBatcher_StageReportsPackedAccurately(t *testing.T) {
	dir := t.TempDir()
	vl, err := newVLog(0, dir, "", time.Millisecond)
	if err != nil {
		t.Fatalf("newVLog: %v", err)
	}
	t.Cleanup(func() { vl.close() })

	b := vl.NewBatcher()

	// Small record: packs.
	if _, packed, err := b.Stage([]byte("small")); err != nil {
		t.Fatalf("Stage small: %v", err)
	} else if !packed {
		t.Error("Stage reported packed=false for a small record")
	}

	// Oversized record: cannot share a block.
	big := bytes.Repeat([]byte("x"), vlogBlockSize) // header+value > 4 KB
	if _, packed, err := b.Stage(big); err != nil {
		t.Fatalf("Stage oversized: %v", err)
	} else if packed {
		t.Errorf("Stage reported packed=true for a %d-byte record (block size %d) — "+
			"MarkDead would subtract the wrong footprint", len(big), vlogBlockSize)
	}

	if err := b.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// ── Bug: raw-mode VLog cursor seeded only after async replay ─────────────────

// maxVLogEndFromWAL derives the VLog write cursor from parsed WAL entries, with
// no index — that is what lets the seed run synchronously, before the engine
// starts serving. In raw block-device mode Stat().Size() is 0, so without this
// the first Put after a restart reserves from offset 4096 and overwrites live
// records while background replay is still running.
func TestMaxVLogEndFromWAL(t *testing.T) {
	// Pick two keys that both route to disk 0 of a 1-disk layout (trivially
	// true) and give them known offsets.
	entries := []walReplayEntry{
		// Unpacked record: owns its aligned span.
		{key: "k-unpacked", vlogOffset: 8192, diskLen: 100, packed: false},
		// Packed record living inside the block at 4096.
		{key: "k-packed", vlogOffset: 4096 + 152, diskLen: 64, packed: true},
		// Tombstone / no VLog position: ignored.
		{key: "k-none", vlogOffset: 0, diskLen: 0},
	}

	got := maxVLogEndFromWAL(entries, 0, 1)

	// Unpacked at 8192 with 24+100=124 raw bytes → one 4 KB block → 12288.
	// Packed record ends inside the 4096 block → rounds to 8192.
	const want = 12288
	if got != want {
		t.Fatalf("maxVLogEndFromWAL = %d, want %d", got, want)
	}

	// No entries for this disk → 0, so the caller leaves vl.end alone.
	if got := maxVLogEndFromWAL(nil, 0, 1); got != 0 {
		t.Fatalf("maxVLogEndFromWAL(nil) = %d, want 0", got)
	}
}

// The seed must be monotonic and must never pull the cursor backwards on a
// file-backed VLog, where vl.end already reflects the real file size.
func TestSetEndAtLeast_NeverRetreats(t *testing.T) {
	dir := t.TempDir()
	vl, err := newVLog(0, dir, "", time.Millisecond)
	if err != nil {
		t.Fatalf("newVLog: %v", err)
	}
	t.Cleanup(func() { vl.close() })

	if _, _, err := vl.beginAppend([]byte("something")); err != nil {
		t.Fatalf("beginAppend: %v", err)
	}
	high := vl.end.Load()

	vl.SetEndAtLeast(1) // far below the current cursor
	if got := vl.end.Load(); got != high {
		t.Fatalf("SetEndAtLeast(1) moved end from %d to %d", high, got)
	}

	vl.SetEndAtLeast(high + vlogBlockSize)
	if got := vl.end.Load(); got != high+vlogBlockSize {
		t.Fatalf("SetEndAtLeast did not advance: got %d, want %d", got, high+vlogBlockSize)
	}
}
