package main

// raft_snapshot_test.go — raftFSM.CaptureSnapshot / RestoreSnapshot against
// real engines (raft_snapshot.go): consistency of a capture taken while
// Apply runs, restore = snapshot exactly (from any prior state, after an
// interrupted restore, without the ordered index), the v1.16 formats, and
// the size of the Apply stall.

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

type memSink struct{ recs [][]byte }

func (s *memSink) Write(r []byte) error {
	s.recs = append(s.recs, append([]byte(nil), r...))
	return nil
}

var errInjectedCrash = errors.New("injected crash")

type memSource struct {
	recs   [][]byte
	i      int
	failAt int // > 0: Next fails at this record (a crash mid-restore)
}

func (s *memSource) Next() ([]byte, error) {
	if s.failAt > 0 && s.i == s.failAt {
		return nil, errInjectedCrash
	}
	if s.i >= len(s.recs) {
		return nil, io.EOF
	}
	r := s.recs[s.i]
	s.i++
	return append([]byte(nil), r...), nil
}

func snapEngineCfg(t testing.TB, dir string, mut func(*storage.StorageConfig)) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.ScrubEnabled = false
	cfg.BloomFilterShardBits = 1 << 12
	cfg.DataDirPath = dir
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.RaftAppliedIndex = true
	if mut != nil {
		mut(cfg)
	}
	e, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("storage init: %v", err)
	}
	<-e.ReplayDone
	if _, _, err := e.RebuildSearchIndexes(); err != nil {
		t.Fatal(err)
	}
	return e
}

func applyCmd(t testing.TB, f *raftFSM, c fsmCmd) {
	t.Helper()
	b, err := encodeCmd(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(b); err != nil {
		t.Fatal(err)
	}
}

func preload(t testing.TB, e *storage.StorageEngine, n int) {
	t.Helper()
	val := bytes.Repeat([]byte("p"), 64)
	for i := 0; i < n; i += 1000 {
		var reqs []storage.MultiPutRequest
		for j := i; j < i+1000 && j < n; j++ {
			reqs = append(reqs, storage.MultiPutRequest{Key: fmt.Sprintf("pre%07d", j), Value: val, TTL: -1})
		}
		for _, err := range e.MultiPut(reqs) {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// randomOp returns a mixed, partly non-idempotent command: puts (some with
// TTL), deletes of preloaded and written keys, INCR, hash, list and set ops,
// namespace puts, MultiPut, vectors and records with vectors.
func randomOp(r *rand.Rand, i int) fsmCmd {
	k := fmt.Sprintf("w%04d", r.Intn(2000))
	switch r.Intn(12) {
	case 0:
		return fsmCmd{Op: opDelete, Key: fmt.Sprintf("pre%07d", r.Intn(50_000))}
	case 1:
		return fsmCmd{Op: opDelete, Key: k}
	case 2:
		return fsmCmd{Op: opIncr, Key: "ctr" + k[1:3], Delta: int64(r.Intn(5) + 1), TTL: -1}
	case 3:
		return fsmCmd{Op: opHSet, Key: "h" + k[1:3], Field: k, Value: []byte(fmt.Sprint(i)), TTL: -1}
	case 4:
		return fsmCmd{Op: opLPush, Key: "list" + k[1:2], Value: []byte(fmt.Sprint(i))}
	case 5:
		return fsmCmd{Op: opSAdd, Key: "set" + k[1:2], Field: k}
	case 6:
		return fsmCmd{Op: opNSPut, Ns: "tenant", Key: k, Value: []byte(fmt.Sprint(i)), TTL: -1}
	case 7:
		return fsmCmd{Op: opPut, Key: k, Value: []byte(fmt.Sprintf("ttl-%d", i)), TTL: 600}
	case 8:
		return fsmCmd{Op: opMultiPut, Entries: []storage.MultiPutRequest{
			{Key: k, Value: []byte(fmt.Sprint(i)), TTL: -1},
			{Key: k + "b", Value: []byte(fmt.Sprint(i)), TTL: -1}}}
	case 9:
		return fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("doc%03d", r.Intn(200)), Vec: vecFor(r.Intn(1000))}
	case 10:
		return fsmCmd{Op: opDelete, Key: fmt.Sprintf("doc%03d", r.Intn(200))}
	default:
		return fsmCmd{Op: opPut, Key: k, Value: []byte(fmt.Sprintf(`{"color":"c%d","i":%d}`, r.Intn(4), i)), TTL: -1}
	}
}

func requireSameKeyspace(t *testing.T, what string, want, got *storage.StorageEngine) {
	t.Helper()
	requireSameKeyspaceTol(t, what, want, got, 3)
}

// requireSameKeyspaceTol: TTLs equal within tol seconds.
func requireSameKeyspaceTol(t *testing.T, what string, want, got *storage.StorageEngine, tol float64) {
	t.Helper()
	if d := diffDumpsTol(dumpEngine(want), dumpEngine(got), tol); len(d) > 0 {
		if len(d) > 15 {
			d = append(d[:15], fmt.Sprintf("… %d more", len(d)-15))
		}
		t.Fatalf("%s: keyspace differs: %s", what, strings.Join(d, "; "))
	}
}

// TestRaftFSMSnapshot_ConsistentUnderConcurrentApply: a capture runs while an
// applier hammers the engine with mixed, non-idempotent commands.  The
// snapshot, restored into an engine that holds unrelated junk, equals — key
// for key, with TTLs — a reference engine that applied exactly the commands
// up to the fence's index.  The applier is stalled only for the fence.
func TestRaftFSMSnapshot_ConsistentUnderConcurrentApply(t *testing.T) {
	root := t.TempDir()
	src := snapEngineCfg(t, filepath.Join(root, "src"), nil)
	defer src.Close()
	preload(t, src, 50_000)
	f := newRaftFSM(src)
	applyCmd(t, f, fsmCmd{Op: opIdxCreate, Key: "bycolor", Field: "color"})
	for i := 0; i < 50; i++ {
		applyCmd(t, f, fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("doc%03d", i), Vec: vecFor(i)})
	}
	setup := []fsmCmd{{Op: opIdxCreate, Key: "bycolor", Field: "color"}}
	for i := 0; i < 50; i++ {
		setup = append(setup, fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("doc%03d", i), Vec: vecFor(i)})
	}

	var smMu sync.Mutex
	var applied []fsmCmd
	var maxWait atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(1))
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c := randomOp(r, i)
			b, _ := encodeCmd(c)
			w := time.Now()
			smMu.Lock()
			if d := int64(time.Since(w)); d > maxWait.Load() {
				maxWait.Store(d)
			}
			_ = f.Apply(b)
			applied = append(applied, c)
			smMu.Unlock()
		}
	}()
	time.Sleep(50 * time.Millisecond)

	sink := &memSink{}
	var fenceHold time.Duration
	var idx uint64
	t0 := time.Now()
	err := f.CaptureSnapshot(sink, func(fn func()) (uint64, error) {
		smMu.Lock()
		defer smMu.Unlock()
		s := time.Now()
		idx = uint64(len(applied))
		fn()
		fenceHold = time.Since(s)
		return idx, nil
	})
	capture := time.Since(t0)
	close(stop)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	smMu.Lock()
	total := len(applied)
	upTo := append([]fsmCmd(nil), applied[:idx]...)
	smMu.Unlock()
	t.Logf("capture %v (%d records), applies during it: %d of %d, fence hold %v, max applier wait %v",
		capture, len(sink.recs), idx, total, fenceHold, time.Duration(maxWait.Load()))
	if idx < 100 {
		t.Fatalf("only %d commands applied before the fence — no concurrency exercised", idx)
	}
	if total <= int(idx) {
		t.Log("note: no command applied after the fence")
	}

	// Reference: the same setup + preload + commands up to the fence.
	ref := snapEngineCfg(t, filepath.Join(root, "ref"), nil)
	defer ref.Close()
	preload(t, ref, 50_000)
	rf := newRaftFSM(ref)
	for _, c := range setup {
		applyCmd(t, rf, c)
	}
	for _, c := range upTo {
		applyCmd(t, rf, c)
	}

	// Target: an engine full of other state.
	dst := snapEngineCfg(t, filepath.Join(root, "dst"), nil)
	defer dst.Close()
	df := newRaftFSM(dst)
	for i := 0; i < 3000; i++ {
		applyCmd(t, df, fsmCmd{Op: opPut, Key: fmt.Sprintf("junk%05d", i), Value: []byte("j"), TTL: -1})
		if i%10 == 0 {
			applyCmd(t, df, fsmCmd{Op: opPut, Key: fmt.Sprintf("pre%07d", i), Value: []byte("stale"), TTL: 60})
		}
	}
	applyCmd(t, df, fsmCmd{Op: opVSet, Ns: "other", Key: "zz", Vec: vecFor(9)[:4]})
	applyCmd(t, df, fsmCmd{Op: opIdxCreate, Key: "stale", Field: "x"})

	if err := df.RestoreSnapshot(&memSource{recs: sink.recs}, false); err != nil {
		t.Fatal(err)
	}
	// The reference re-applied the TTL'd puts later than the original did
	// (slow under -race), so its relative TTLs are younger by that much;
	// the restored copy carries the original absolute expiries.
	requireSameKeyspaceTol(t, "restored vs reference", ref, dst, 3+time.Since(t0).Seconds())
	if got, want := dst.ListFieldIndexes(), ref.ListFieldIndexes(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("index definitions %v, want %v", got, want)
	}
	// The keyspaces (every "@vec/" key and value included) are equal above;
	// this checks the restored RAM index answers.  HNSW is approximate and
	// the two graphs were built in different orders (the reference in apply
	// order, with update tombstones; the restore from the keys), so with the
	// default beam their top-k may legitimately differ in the tail (seen 1 in
	// 20 under load: doc036 0.9928 vs doc003 0.9902).  A beam wider than the
	// namespace (≤ 200 ids + tombstones) makes both searches exhaustive.
	exact := storage.VectorSearchOptions{Ef: 1024}
	for _, q := range []int{3, 17, 42} {
		a, _ := ref.SearchVectorWithOptions("emb", vecFor(q), 5, exact)
		b, _ := dst.SearchVectorWithOptions("emb", vecFor(q), 5, exact)
		if fmt.Sprint(canonMatches(a)) != fmt.Sprint(canonMatches(b)) {
			t.Fatalf("vector search differs: ref %v, restored %v", a, b)
		}
	}
	if res, err := dst.SearchVector("other", vecFor(9)[:4], 3); err == nil && len(res) > 0 {
		t.Fatalf("a vector of a namespace absent from the snapshot is still searchable: %v", res)
	}
	if fenceHold > capture/2 && capture > 50*time.Millisecond {
		t.Fatalf("the fence held Apply for %v of a %v capture", fenceHold, capture)
	}
}

// TestRaftFSMSnapshot_InterruptedRestoreRedone: a restore that dies halfway
// (the process crashed) leaves a mixed keyspace; on restart the restore is
// redone from the snapshot and the result is exactly the snapshot.
func TestRaftFSMSnapshot_InterruptedRestoreRedone(t *testing.T) {
	root := t.TempDir()
	src := snapEngineCfg(t, filepath.Join(root, "src"), nil)
	defer src.Close()
	sf := newRaftFSM(src)
	for i := 0; i < 2000; i++ {
		applyCmd(t, sf, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%05d", i), Value: []byte(fmt.Sprint("new", i)), TTL: int32(600 * (i % 2))})
	}
	for i := 0; i < 30; i++ {
		applyCmd(t, sf, fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("k%05d", i), Vec: vecFor(i)})
	}
	sink := &memSink{}
	if err := sf.CaptureSnapshot(sink, func(fn func()) (uint64, error) { fn(); return 1, nil }); err != nil {
		t.Fatal(err)
	}

	dstDir := filepath.Join(root, "dst")
	dst := snapEngineCfg(t, dstDir, nil)
	df := newRaftFSM(dst)
	for i := 1000; i < 4000; i++ {
		applyCmd(t, df, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%05d", i), Value: []byte("old"), TTL: -1})
	}
	for i := 3000; i < 3030; i++ {
		applyCmd(t, df, fsmCmd{Op: opVSet, Ns: "emb", Key: fmt.Sprintf("k%05d", i), Vec: vecFor(i)})
	}
	err := df.RestoreSnapshot(&memSource{recs: sink.recs, failAt: len(sink.recs) / 2}, false)
	if !errors.Is(err, errInjectedCrash) {
		t.Fatalf("restore error %v, want the injected crash", err)
	}
	if d := diffDumps(dumpEngine(src), dumpEngine(dst)); len(d) == 0 {
		t.Fatal("half a restore already equals the snapshot — the crash was not mid-restore")
	}
	_ = dst.Close() // the crash; every write the restore made is durable

	dst = snapEngineCfg(t, dstDir, nil)
	defer dst.Close()
	if err := newRaftFSM(dst).RestoreSnapshot(&memSource{recs: sink.recs}, false); err != nil {
		t.Fatal(err)
	}
	requireSameKeyspace(t, "after the redone restore", src, dst)
	a, _ := src.SearchVector("emb", vecFor(5), 3)
	b, _ := dst.SearchVector("emb", vecFor(5), 3)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("vector search differs after the redone restore: %v vs %v", a, b)
	}
}

// TestRaftFSMSnapshot_NoOrderedIndex: with --no-ordered-index the capture
// walks shards (unsorted) and the restore uses a key set; both directions
// between ordered and unordered engines give the exact keyspace.
func TestRaftFSMSnapshot_NoOrderedIndex(t *testing.T) {
	root := t.TempDir()
	noOrd := func(c *storage.StorageConfig) { c.DisableOrderedIndex = true }
	for _, tc := range []struct {
		name           string
		srcMut, dstMut func(*storage.StorageConfig)
		wantSorted     bool
	}{
		{"unordered->ordered", noOrd, nil, false},
		{"ordered->unordered", nil, noOrd, true},
		{"unordered->unordered", noOrd, noOrd, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := snapEngineCfg(t, filepath.Join(root, tc.name, "src"), tc.srcMut)
			defer src.Close()
			sf := newRaftFSM(src)
			for i := 0; i < 500; i++ {
				applyCmd(t, sf, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%04d", i), Value: []byte(fmt.Sprint(i)), TTL: int32(300 * (i % 3))})
			}
			sink := &memSink{}
			if err := sf.CaptureSnapshot(sink, func(fn func()) (uint64, error) { fn(); return 1, nil }); err != nil {
				t.Fatal(err)
			}
			if sorted := sink.recs[0][2] == 1; sorted != tc.wantSorted {
				t.Fatalf("sorted=%v, want %v", sorted, tc.wantSorted)
			}
			dst := snapEngineCfg(t, filepath.Join(root, tc.name, "dst"), tc.dstMut)
			defer dst.Close()
			df := newRaftFSM(dst)
			for i := 250; i < 900; i++ {
				applyCmd(t, df, fsmCmd{Op: opPut, Key: fmt.Sprintf("k%04d", i), Value: []byte("old"), TTL: -1})
			}
			if err := df.RestoreSnapshot(&memSource{recs: sink.recs}, false); err != nil {
				t.Fatal(err)
			}
			requireSameKeyspace(t, tc.name, src, dst)
		})
	}
}

// TestRaftFSMSnapshot_V116Formats: a v1.16 snapshot blob (gob stream of
// key/value pairs — sent by a v1.16 leader, or migrated from
// raft_snapshot.gob) restores to exactly its keys; and the blob this build
// encodes for a v1.16 follower holds exactly the live keys.
func TestRaftFSMSnapshot_V116Formats(t *testing.T) {
	root := t.TempDir()
	var blob bytes.Buffer
	enc := gob.NewEncoder(&blob)
	want := map[string]string{}
	for i := 0; i < 300; i++ {
		k, v := fmt.Sprintf("old%03d", i), fmt.Sprint("v", i)
		want[k] = v
		_ = enc.Encode(snapshotEntry{Key: k, Value: []byte(v)})
	}
	dst := snapEngineCfg(t, filepath.Join(root, "dst"), nil)
	defer dst.Close()
	df := newRaftFSM(dst)
	for i := 200; i < 600; i++ {
		applyCmd(t, df, fsmCmd{Op: opPut, Key: fmt.Sprintf("old%03d", i), Value: []byte("stale"), TTL: 100})
	}
	if err := df.RestoreSnapshot(&memSource{recs: [][]byte{blob.Bytes()}}, true); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for k, e := range dumpEngine(dst) {
		got[k] = e.val
		if e.ttl != -1 {
			t.Fatalf("%s has TTL %d after a v1.16 restore (its format has none)", k, e.ttl)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("restored %d keys, want %d", len(got), len(want))
	}

	// Encode for a v1.16 follower: last record per key wins, deletes and
	// expired keys dropped.
	src := snapEngineCfg(t, filepath.Join(root, "src"), nil)
	defer src.Close()
	sf := newRaftFSM(src)
	for i := 0; i < 100; i++ {
		applyCmd(t, sf, fsmCmd{Op: opPut, Key: fmt.Sprintf("n%03d", i), Value: []byte(fmt.Sprint(i)), TTL: -1})
	}
	sink := &memSink{}
	_ = sf.CaptureSnapshot(sink, func(fn func()) (uint64, error) { fn(); return 1, nil })
	sink.recs = append(sink.recs[:len(sink.recs)-1], // overlay edits before the defs record
		encodeDelRec("n000"),
		encodePutRec(nil, storage.SnapshotKV{Key: "n001", Value: []byte("over")}),
		encodePutRec(nil, storage.SnapshotKV{Key: "gone", Value: []byte("x"), ExpiryUs: 1}),
		sink.recs[len(sink.recs)-1])
	var out bytes.Buffer
	if err := sf.EncodeLegacySnapshot(&memSource{recs: sink.recs}, &out, 1<<20); err != nil {
		t.Fatal(err)
	}
	dec := gob.NewDecoder(&out)
	m := map[string]string{}
	for {
		var e snapshotEntry
		if err := dec.Decode(&e); err != nil {
			break
		}
		m[e.Key] = string(e.Value)
	}
	if _, ok := m["n000"]; ok || m["n001"] != "over" || len(m) != 99 {
		t.Fatalf("legacy blob: %d keys, n001=%q, n000 present=%v", len(m), m["n001"], m["n000"] != "")
	}
	if err := sf.EncodeLegacySnapshot(&memSource{recs: sink.recs}, io.Discard, 100); err == nil {
		t.Fatal("the size limit was not enforced")
	}
}

// TestRaftFSMSnapshot_TTLKeptAndExpiredSkipped: absolute expiries survive a
// restore exactly; a key that expired between capture and restore is not
// restored.
func TestRaftFSMSnapshot_TTLKeptAndExpiredSkipped(t *testing.T) {
	root := t.TempDir()
	src := snapEngineCfg(t, filepath.Join(root, "src"), nil)
	defer src.Close()
	sf := newRaftFSM(src)
	applyCmd(t, sf, fsmCmd{Op: opPut, Key: "long", Value: []byte("1"), TTL: 3600})
	applyCmd(t, sf, fsmCmd{Op: opPut, Key: "short", Value: []byte("2"), TTL: 1})
	applyCmd(t, sf, fsmCmd{Op: opPut, Key: "forever", Value: []byte("3"), TTL: -1})
	applyCmd(t, sf, fsmCmd{Op: opHSet, Key: "h", Field: "f", Value: []byte("4"), TTL: 1200})
	srcLong, _, _ := src.ReadForSnapshot("long")
	sink := &memSink{}
	if err := sf.CaptureSnapshot(sink, func(fn func()) (uint64, error) { fn(); return 1, nil }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // "short" expires
	dst := snapEngineCfg(t, filepath.Join(root, "dst"), nil)
	defer dst.Close()
	applyCmd(t, newRaftFSM(dst), fsmCmd{Op: opPut, Key: "short", Value: []byte("2"), TTL: -1})
	if err := newRaftFSM(dst).RestoreSnapshot(&memSource{recs: sink.recs}, false); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := dst.ReadForSnapshot("long")
	if !ok || got.ExpiryUs != srcLong.ExpiryUs {
		t.Fatalf("long: expiry %d, want exactly %d", got.ExpiryUs, srcLong.ExpiryUs)
	}
	// Expired before the restore: never readable.  (The restore may keep its
	// entry: expiry in raft state is judged at the raft clock, which is the
	// snapshot's — before the deadline — until the next command; invariant
	// 66.  Every later command sees it expired.)
	if v, err := dst.Get("short"); err == nil {
		t.Fatalf("a key that expired before the restore is readable: %q", v)
	}
	if f, ok, _ := dst.ReadForSnapshot("forever"); !ok || f.ExpiryUs != 0 {
		t.Fatalf("forever: ok=%v expiry=%d", ok, f.ExpiryUs)
	}
	if ttl := dst.HTTL("h", "f"); ttl < 1100 || ttl > 1200 {
		t.Fatalf("hash field TTL %d", ttl)
	}
}

// canonMatches orders search results by score, ties by ID (two documents may
// hold identical vectors, and the order of a tie is unspecified).
func canonMatches(m []storage.VectorMatch) []storage.VectorMatch {
	out := append([]storage.VectorMatch(nil), m...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out
}
