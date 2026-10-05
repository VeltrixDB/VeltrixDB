package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Regressions for "the WAL does not carry a key's TTL". Before the fix the
// binary record header and the text format had no TTL field and replay
// rebuilt every IndexEntry without FlagHasTTL, so after ANY restart — crash
// replay or clean-shutdown checkpoint — every TTL'd key became immortal, and
// a key that had already expired came back to life.

const walTTLTestSeconds = 3600

// ttlFixture writes one TTL'd key through every write path that can set a
// TTL, plus an immortal control key, and returns how to read each TTL back.
type ttlProbe struct {
	name string
	ttl  func(se *StorageEngine) int32 // remaining seconds; -1 immortal; 0/-2 absent
}

func writeTTLFixture(t *testing.T, se *StorageEngine, kvSep bool) []ttlProbe {
	t.Helper()
	ttl := int32(walTTLTestSeconds)
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	keyTTL := func(k string) func(*StorageEngine) int32 {
		return func(se *StorageEngine) int32 { return se.GetTTLForKey(k) }
	}

	must("Put", se.Put("put-ttl", []byte("v"), ttl))
	must("Put immortal", se.Put("put-immortal", []byte("v"), -1))
	errs := se.MultiPut([]MultiPutRequest{
		{Key: "mput-ttl", Value: []byte("v"), TTL: ttl},
		{Key: "mput-immortal", Value: []byte("v"), TTL: -1},
	})
	for _, err := range errs {
		must("MultiPut", err)
	}
	must("PutNS", se.PutNS("tenant", "ns-ttl", []byte("v"), ttl))
	must("HSet", se.HSet("h", "f-ttl", []byte("v"), ttl))
	must("HSet immortal", se.HSet("h", "f-expire", []byte("v"), -1))
	must("HExpire", se.HExpire("h", "f-expire", ttl))

	txn := se.BeginTxn()
	txn.Set("txn-ttl", []byte("v"), ttl)
	must("Txn.Commit", txn.Commit())

	probes := []ttlProbe{
		{"Put", keyTTL("put-ttl")},
		{"MultiPut", keyTTL("mput-ttl")},
		{"PutNS", keyTTL(nsKey("tenant", "ns-ttl"))},
		{"HSet", func(se *StorageEngine) int32 { return se.HTTL("h", "f-ttl") }},
		{"HExpire", func(se *StorageEngine) int32 { return se.HTTL("h", "f-expire") }},
		{"Txn", keyTTL("txn-ttl")},
	}
	if kvSep { // the atomic ops require KV separation
		if _, err := se.SetIfNotExists("setnx-ttl", []byte("v"), ttl); err != nil {
			t.Fatalf("SetIfNotExists: %v", err)
		}
		if _, err := se.Increment("incr-ttl", 5, ttl); err != nil {
			t.Fatalf("Increment: %v", err)
		}
		must("Put cas", se.Put("cas-ttl", []byte("old"), -1))
		if r, err := se.CompareAndSwap("cas-ttl", []byte("old"), []byte("new"), ttl); err != nil || r != CASSuccess {
			t.Fatalf("CompareAndSwap: %v %v", r, err)
		}
		probes = append(probes,
			ttlProbe{"SETNX", keyTTL("setnx-ttl")},
			ttlProbe{"INCR", keyTTL("incr-ttl")},
			ttlProbe{"CAS", keyTTL("cas-ttl")},
		)
	}
	return probes
}

func checkTTLFixture(t *testing.T, se *StorageEngine, probes []ttlProbe, when string) {
	t.Helper()
	for _, p := range probes {
		got := p.ttl(se)
		if got < walTTLTestSeconds-60 || got > walTTLTestSeconds {
			t.Errorf("%s: %s TTL = %d s, want ~%d s (-1 means it came back immortal)",
				when, p.name, got, walTTLTestSeconds)
		}
	}
	for _, k := range []string{"put-immortal", "mput-immortal"} {
		if got := se.GetTTLForKey(k); got != -1 {
			t.Errorf("%s: immortal key %q TTL = %d, want -1", when, k, got)
		}
		if _, err := se.Get(k); err != nil {
			t.Errorf("%s: Get(%q): %v", when, k, err)
		}
	}
	if v, err := se.Get("put-ttl"); err != nil || string(v) != "v" {
		t.Errorf("%s: Get(put-ttl) = %q, %v", when, v, err)
	}
}

func openReplayed(t *testing.T, cfg *StorageConfig) *StorageEngine {
	t.Helper()
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	return se
}

// TestWALTTL_SurvivesRestart: every TTL-setting write path, crash replay and
// clean (checkpoint) restart, binary and both text encodings, KV-separation
// on and off.
func TestWALTTL_SurvivesRestart(t *testing.T) {
	for _, format := range []string{"binary", "text"} {
		for _, kvSep := range []bool{true, false} {
			format, kvSep := format, kvSep
			t.Run(fmt.Sprintf("%s/kvsep=%v", format, kvSep), func(t *testing.T) {
				t.Parallel()
				mkcfg := func(dir string) *StorageConfig {
					cfg := testStorageConfig(dir)
					cfg.WALFormat = format
					cfg.KeyValueSeparation = kvSep
					return cfg
				}
				dir := t.TempDir()
				se := openReplayed(t, mkcfg(dir))
				probes := writeTTLFixture(t, se, kvSep)
				checkTTLFixture(t, se, probes, "before restart")

				// Dirty restart: what a power cut leaves (no checkpoint).
				img := copyCrashImage(t, dir)
				crashed := openReplayed(t, mkcfg(img))
				checkTTLFixture(t, crashed, probes, "after crash replay")
				crashed.Close()

				// Clean restart: Close rewrites wal.log as a checkpoint.
				if err := se.Close(); err != nil {
					t.Fatal(err)
				}
				clean := openReplayed(t, mkcfg(dir))
				checkTTLFixture(t, clean, probes, "after clean restart")
				// …and a second clean restart replays a checkpoint written
				// from replayed state.
				clean.Close()
				again := openReplayed(t, mkcfg(dir))
				defer again.Close()
				checkTTLFixture(t, again, probes, "after second clean restart")
			})
		}
	}
}

// TestWALTTL_ExpiredKeyStaysGone: a key overwritten with a 1 s TTL, which
// then expires while the node is down, must not come back — neither as the
// TTL'd value nor as the older immortal value it superseded.
func TestWALTTL_ExpiredKeyStaysGone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	se := openReplayed(t, testStorageConfig(dir))
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(se.Put("old-then-ttl", []byte("immortal-old"), -1))
	must(se.Put("old-then-ttl", []byte("short"), 1))
	must(se.Put("ttl-only", []byte("short"), 1))
	for _, err := range se.MultiPut([]MultiPutRequest{{Key: "mput-short", Value: []byte("short"), TTL: 1}}) {
		must(err)
	}
	must(se.HSet("h", "short", []byte("short"), 1))
	must(se.Put("keeper", []byte("k"), -1))

	img := copyCrashImage(t, dir) // crash image taken while still live
	if err := se.Close(); err != nil {
		t.Fatal(err) // checkpoint taken while still live, too
	}
	time.Sleep(1100 * time.Millisecond)

	check := func(se *StorageEngine, when string) {
		t.Helper()
		for _, k := range []string{"old-then-ttl", "ttl-only", "mput-short"} {
			if v, err := se.Get(k); err == nil {
				t.Errorf("%s: expired key %q served %q", when, k, v)
			} else if !errors.Is(err, ErrKeyNotFound) && !errors.Is(err, ErrKeyExpired) {
				t.Errorf("%s: Get(%q): unexpected error %v", when, k, err)
			}
			if ttl := se.GetTTLForKey(k); ttl != 0 {
				t.Errorf("%s: expired key %q TTL = %d, want 0 (absent)", when, k, ttl)
			}
		}
		if v, _ := se.HGet("h", "short"); v != nil {
			t.Errorf("%s: expired hash field served %q", when, v)
		}
		if n := se.HLen("h"); n != 0 {
			t.Errorf("%s: HLen = %d, want 0", when, n)
		}
		for _, k := range se.ScanKeys() {
			if k != "keeper" {
				t.Errorf("%s: ScanKeys returned expired key %q", when, k)
			}
		}
		if v, err := se.Get("keeper"); err != nil || string(v) != "k" {
			t.Errorf("%s: Get(keeper) = %q, %v", when, v, err)
		}
	}

	crashed := openReplayed(t, testStorageConfig(img))
	check(crashed, "after crash replay")
	crashed.Close()
	// A checkpoint written by the replayed engine must not resurrect it either.
	again := openReplayed(t, testStorageConfig(img))
	check(again, "after crash replay + clean restart")
	again.Close()

	clean := openReplayed(t, testStorageConfig(dir))
	defer clean.Close()
	check(clean, "after clean restart")
}

// TestWALTTL_CheckpointDropsExpired: a key that has already expired when the
// checkpoint is written is left out of it.
func TestWALTTL_CheckpointDropsExpired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	se := openReplayed(t, testStorageConfig(dir))
	if err := se.Put("gone", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := se.Put("kept", []byte("v"), walTTLTestSeconds); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := se.Close(); err != nil {
		t.Fatal(err)
	}
	recs, err := replayWAL(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].key != "kept" || recs[0].ttlExpiryUs == 0 {
		t.Fatalf("checkpoint records = %+v, want only \"kept\" with its TTL", recs)
	}
}

// TestWALTTL_V1RecordsByteIdentical: a record without a TTL must be encoded
// exactly as before the TTL field existed — existing WAL files, and the
// no-TTL hot path, are unchanged. The expected bytes are built by hand from
// the documented v1 layout, not by the encoder under test.
func TestWALTTL_V1RecordsByteIdentical(t *testing.T) {
	e := &WALEntry{Timestamp: 1_700_000_000_123_456_789, Key: "user:42", KeyLen: 7,
		Value: []byte("hello"), ValueLen: 5, Checksum: computeCRC32C([]byte("hello")),
		Version: 77, XformFlags: 0}
	var want []byte
	var h [48]byte
	h[0], h[1] = 0xB1, 1
	binary.LittleEndian.PutUint16(h[2:], walBinFlagInline)
	binary.LittleEndian.PutUint32(h[4:], 7)
	binary.LittleEndian.PutUint32(h[8:], 5)
	binary.LittleEndian.PutUint32(h[12:], 5)
	binary.LittleEndian.PutUint32(h[16:], e.Checksum)
	binary.LittleEndian.PutUint64(h[24:], uint64(e.Timestamp))
	binary.LittleEndian.PutUint64(h[32:], 77)
	want = append(want, h[:]...)
	want = append(want, "user:42hello"...)
	want = binary.LittleEndian.AppendUint32(want, computeCRC32C(want))
	if got := appendWALRecordBinary(nil, e); !bytes.Equal(got, want) {
		t.Fatalf("v1 binary record changed:\n got %x\nwant %x", got, want)
	}

	kv := &WALEntry{Timestamp: 5, Key: "k", ValueLen: 128, Checksum: 0xABC, Version: 9,
		VLogOffset: 4096, Packed: true}
	if got, want := string(appendWALRecordText(nil, kv)), "5|0|k|128|abc|9|4096|1\n"; got != want {
		t.Fatalf("TTL-free text record changed: got %q want %q", got, want)
	}
	xf := &WALEntry{Timestamp: 5, Key: "k", ValueLen: 128, DiskValueLen: 60, XformFlags: FlagCompressed,
		Checksum: 0xABC, Version: 9, VLogOffset: 4096}
	if got, want := string(appendWALRecordText(nil, xf)), "5|0|k|128|abc|9|4096|0|60|2\n"; got != want {
		t.Fatalf("TTL-free 10-field text record changed: got %q want %q", got, want)
	}

	// And a v1 file replays to the same entries it always did: no TTL.
	path := filepath.Join(t.TempDir(), "wal.log")
	data := append(appendWALRecordBinary(nil, e), appendWALRecordText(nil, kv)...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err := replayWAL(path)
	if err != nil || len(recs) != 2 {
		t.Fatalf("replay: %d records, %v", len(recs), err)
	}
	for _, r := range recs {
		if r.ttlExpiryUs != 0 {
			t.Errorf("v1 record %q replayed with TTL %d", r.key, r.ttlExpiryUs)
		}
	}
	if string(recs[0].value) != "hello" || recs[0].version != 77 || recs[1].vlogOffset != 4096 || !recs[1].packed {
		t.Fatalf("v1 replay fields wrong: %+v", recs)
	}
}

// TestWALTTL_V1WALReplaysOnEngine: a wal.log written entirely in the
// pre-TTL format — what every existing deployment has on disk — replays
// onto an engine as immortal keys, as it always did.
func TestWALTTL_V1WALReplaysOnEngine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var data []byte
	for i := 0; i < 10; i++ {
		v := []byte(fmt.Sprint("val-", i))
		e := &WALEntry{Timestamp: time.Now().UnixNano(), Key: fmt.Sprint("k", i), KeyLen: 2,
			Value: v, ValueLen: uint32(len(v)), Checksum: computeCRC32C(v), Version: uint64(i + 1)}
		if i%2 == 0 {
			data = appendWALRecordBinary(data, e)
		} else {
			data = appendWALRecordText(data, e)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "wal.log"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	se := openReplayed(t, testStorageConfig(dir))
	defer se.Close()
	for i := 0; i < 10; i++ {
		k := fmt.Sprint("k", i)
		if v, err := se.Get(k); err != nil || string(v) != fmt.Sprint("val-", i) {
			t.Errorf("Get(%q) = %q, %v", k, v, err)
		}
		if ttl := se.GetTTLForKey(k); ttl != -1 {
			t.Errorf("v1 key %q TTL = %d, want -1", k, ttl)
		}
	}
}

// TestWALTTL_FormatRoundTrip: both encodings carry the absolute expiry, a
// version-2 binary record is otherwise the v1 layout, and a mixed file of
// text / text+TTL / v1 / v2 records replays in order.
func TestWALTTL_FormatRoundTrip(t *testing.T) {
	const exp = int64(1_900_000_000_000_000) // absolute µs
	mk := func(k string, ttl int64, kvsep bool) *WALEntry {
		e := &WALEntry{Timestamp: 1, Key: k, KeyLen: uint32(len(k)), Version: 3, TTLExpiryUs: ttl}
		if kvsep {
			e.ValueLen, e.DiskValueLen, e.Checksum, e.VLogOffset, e.Packed = 128, 90, 7, 8192, true
			e.XformFlags = FlagCompressed
		} else {
			e.Value = []byte("inline")
			e.ValueLen, e.Checksum = 6, computeCRC32C(e.Value)
		}
		return e
	}

	v2 := appendWALRecordBinary(nil, mk("b", exp, false))
	if v2[1] != walBinVersionTTL || len(v2) != walBinHeaderSize+walBinTTLSize+1+6+walBinTrailer {
		t.Fatalf("TTL record: version %d, %d bytes", v2[1], len(v2))
	}
	if got := int64(binary.LittleEndian.Uint64(v2[48:])); got != exp {
		t.Fatalf("TTL field at offset 48 = %d, want %d", got, exp)
	}
	line := string(appendWALRecordText(nil, mk("t", exp, true)))
	if want := "1|0|t|128|7|3|8192|1|90|2|" + strconv.FormatInt(exp, 10) + "\n"; line != want {
		t.Fatalf("text TTL record = %q, want %q", line, want)
	}
	// TTL forces fields 9–10 out even for an untransformed record.
	if got := string(appendWALRecordText(nil, &WALEntry{Timestamp: 1, Key: "u", ValueLen: 4, Checksum: 1,
		Version: 3, VLogOffset: 4096, TTLExpiryUs: exp})); got != "1|0|u|4|1|3|4096|0|4|0|"+strconv.FormatInt(exp, 10)+"\n" {
		t.Fatalf("untransformed text TTL record = %q", got)
	}

	var data []byte
	data = appendWALRecordText(data, mk("text-v1", 0, false))
	data = appendWALRecordText(data, mk("text-ttl", exp, false))
	data = appendWALRecordText(data, mk("text-ttl-kvsep", exp+1, true))
	data = appendWALRecordBinary(data, mk("bin-v1", 0, true))
	data = appendWALRecordBinary(data, mk("bin-ttl", exp+2, false))
	data = appendWALRecordBinary(data, mk("bin-ttl-kvsep", exp+3, true))
	data = appendWALRecordText(data, mk("text-after", 0, true))
	path := filepath.Join(t.TempDir(), "wal.log")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err := replayWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key string
		ttl int64
	}{{"text-v1", 0}, {"text-ttl", exp}, {"text-ttl-kvsep", exp + 1}, {"bin-v1", 0},
		{"bin-ttl", exp + 2}, {"bin-ttl-kvsep", exp + 3}, {"text-after", 0}}
	if len(recs) != len(want) {
		t.Fatalf("mixed replay: %d records, want %d", len(recs), len(want))
	}
	for i, w := range want {
		r := recs[i]
		if r.key != w.key || r.ttlExpiryUs != w.ttl {
			t.Errorf("record %d = %q ttl %d, want %q ttl %d", i, r.key, r.ttlExpiryUs, w.key, w.ttl)
		}
		if strings.HasSuffix(w.key, "kvsep") || w.key == "bin-v1" || w.key == "text-after" {
			if r.vlogOffset != 8192 || !r.packed || r.diskLen != 90 || r.xflags != FlagCompressed {
				t.Errorf("record %q lost KV-sep fields: %+v", r.key, r)
			}
		} else if string(r.value) != "inline" {
			t.Errorf("record %q value = %q", r.key, r.value)
		}
	}
	// parseWALBuffer (the PITR archiver's decoder) agrees and consumes it all.
	if prs, n := parseWALBuffer(data); n != len(data) || len(prs) != len(want) {
		t.Fatalf("parseWALBuffer: %d records, consumed %d of %d", len(prs), n, len(data))
	}
}

// TestWALTTL_V2TornAndCorrupt: a version-2 record torn anywhere — including
// inside the TTL field — or damaged is a torn tail like any other; an unknown
// version still is too.
func TestWALTTL_V2TornAndCorrupt(t *testing.T) {
	good := appendWALRecordBinary(nil, &WALEntry{Timestamp: 1, Key: "a", Value: []byte("x"), ValueLen: 1,
		Checksum: computeCRC32C([]byte("x")), Version: 1})
	rec := appendWALRecordBinary(nil, &WALEntry{Timestamp: 2, Key: "b", Value: []byte("y"), ValueLen: 1,
		Checksum: computeCRC32C([]byte("y")), Version: 2, TTLExpiryUs: 123456789})
	decode := func(data []byte) []walReplayEntry {
		recs, _ := parseWALBuffer(data)
		return recs
	}
	for cut := 1; cut < len(rec); cut++ {
		if got := decode(append(append([]byte{}, good...), rec[:cut]...)); len(got) != 1 {
			t.Fatalf("torn at %d: %d records, want 1", cut, len(got))
		}
	}
	for i := 0; i < len(rec); i++ {
		bad := append([]byte{}, rec...)
		bad[i] ^= 0x10
		if i == 0 {
			continue // magic flipped: decodes as text and fails — still a tail
		}
		if got := decode(append(append([]byte{}, good...), bad...)); len(got) != 1 {
			t.Fatalf("flip@%d: %d records, want 1", i, len(got))
		}
	}
	unk := append([]byte{}, rec...)
	unk[1] = 3
	if got := decode(append(append([]byte{}, good...), unk...)); len(got) != 1 {
		t.Fatalf("unknown version accepted: %d records", len(got))
	}
}

// TestWALTTL_TextLegacyRollback: --wal-format=text-legacy is the rollback
// mode. Its checkpoint must be readable by a parser that predates the TTL
// field (SplitN on 10 fields, as every older build does), so it carries no
// TTL — and --wal-format=text, which keeps the TTL, must not be mistaken for
// rollback-safe.
func TestWALTTL_TextLegacyRollback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		format      string
		wantTTLKept bool
	}{{"text-legacy", false}, {"text", true}} {
		dir := t.TempDir()
		se := openReplayed(t, testStorageConfig(dir))
		if err := se.Put("ttl", []byte("v"), walTTLTestSeconds); err != nil {
			t.Fatal(err)
		}
		if err := se.Put("plain", []byte("v"), -1); err != nil {
			t.Fatal(err)
		}
		se.Close() // binary checkpoint

		cfg := testStorageConfig(dir)
		cfg.WALFormat = tc.format
		se = openReplayed(t, cfg)
		se.Close() // text checkpoint

		data, err := os.ReadFile(filepath.Join(dir, "wal.log"))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.IndexByte(data, walBinMagic) >= 0 {
			t.Fatalf("%s checkpoint contains binary records", tc.format)
		}
		oldParserOK := true
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			parts := strings.SplitN(line, "|", 10) // the pre-TTL parser's split
			if len(parts) == 10 {
				if _, err := strconv.ParseUint(parts[9], 16, 8); err != nil {
					oldParserOK = false // an older build stops replay here
				}
			}
		}
		if oldParserOK == tc.wantTTLKept {
			t.Errorf("%s: pre-TTL parser can read checkpoint = %v, want %v", tc.format, oldParserOK, !tc.wantTTLKept)
		}

		se = openReplayed(t, testStorageConfig(dir))
		got := se.GetTTLForKey("ttl")
		if tc.wantTTLKept && got < walTTLTestSeconds-60 {
			t.Errorf("%s: TTL after restart = %d, want ~%d", tc.format, got, walTTLTestSeconds)
		}
		if !tc.wantTTLKept && got != -1 {
			t.Errorf("%s: TTL after restart = %d, want -1 (documented: dropped)", tc.format, got)
		}
		if _, err := se.Get("plain"); err != nil {
			t.Errorf("%s: Get(plain): %v", tc.format, err)
		}
		se.Close()
	}
}

// TestWALTTL_PITRKeepsTTL: the archiver copies each record's TTL into its
// self-contained archive record, and a point-in-time restore replays it.
func TestWALTTL_PITRKeepsTTL(t *testing.T) {
	t.Parallel()
	dataDir, archiveDir := t.TempDir(), t.TempDir()
	se, arch := newPITRTestEngine(t, dataDir, archiveDir)
	if err := se.Put("base", []byte("b"), -1); err != nil {
		t.Fatal(err)
	}
	backupDir := t.TempDir()
	if _, err := NewBackupEngine(se).FullBackup(backupDir); err != nil {
		t.Fatalf("FullBackup: %v", err)
	}
	if err := se.Put("ttl-put", []byte("v"), walTTLTestSeconds); err != nil {
		t.Fatal(err)
	}
	for _, err := range se.MultiPut([]MultiPutRequest{{Key: "ttl-mput", Value: []byte("v"), TTL: walTTLTestSeconds}}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := se.Put("short", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := se.Put("plain", []byte("v"), -1); err != nil {
		t.Fatal(err)
	}
	target := se.GetVersion()
	arch.Stop()
	if err := se.Close(); err != nil {
		t.Fatal(err)
	}

	segs, err := ListArchiveSegments(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	archived := map[string]int64{}
	for _, s := range segs {
		data, err := os.ReadFile(s.SegPath)
		if err != nil {
			t.Fatal(err)
		}
		recs, _ := parseWALBuffer(data)
		for _, r := range recs {
			archived[r.key] = r.ttlExpiryUs
		}
	}
	for _, k := range []string{"ttl-put", "ttl-mput", "short"} {
		if archived[k] == 0 {
			t.Errorf("archive record for %q has no TTL", k)
		}
	}
	if archived["plain"] != 0 {
		t.Errorf("immortal key archived with TTL %d", archived["plain"])
	}

	time.Sleep(1100 * time.Millisecond) // "short" expires before the restore
	restored := t.TempDir()
	if _, err := RestorePITR(backupDir, archiveDir, PITRTarget{Version: target}, []string{restored}); err != nil {
		t.Fatalf("RestorePITR: %v", err)
	}
	se2 := openReplayed(t, testStorageConfig(restored))
	defer se2.Close()
	for _, k := range []string{"ttl-put", "ttl-mput"} {
		if got := se2.GetTTLForKey(k); got < walTTLTestSeconds-60 || got > walTTLTestSeconds {
			t.Errorf("restored %q TTL = %d, want ~%d", k, got, walTTLTestSeconds)
		}
	}
	if v, err := se2.Get("short"); err == nil {
		t.Errorf("restored expired key served %q", v)
	}
	if got := se2.GetTTLForKey("plain"); got != -1 {
		t.Errorf("restored immortal key TTL = %d", got)
	}
	if _, err := se2.Get("base"); err != nil {
		t.Errorf("restored base key: %v", err)
	}
}
