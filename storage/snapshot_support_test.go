package storage

// snapshot_support_test.go — the engine primitives behind raft snapshots
// (snapshot_support.go): the write tracker, uncached snapshot reads, and raw
// restore writes.

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func newSnapEngine(t *testing.T) *StorageEngine {
	t.Helper()
	se, err := NewStorageEngine(testStorageConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	t.Cleanup(func() { se.Close() })
	return se
}

// Every kind of write is noted, only after it is installed, and Drain
// starts a new set.
func TestWriteTracker_NotesEveryWritePath(t *testing.T) {
	se := newSnapEngine(t)
	if err := se.Put("before", []byte("x"), -1); err != nil {
		t.Fatal(err)
	}
	tr, err := se.TrackWrites(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := se.TrackWrites(0); err == nil {
		t.Fatal("a second tracker was allowed")
	}
	_ = se.Put("p", []byte("1"), -1)
	_ = se.Delete("before")
	_ = se.MultiPut([]MultiPutRequest{{Key: "m1", Value: []byte("1"), TTL: -1}, {Key: "m2", Value: []byte("2"), TTL: 5}})
	_, _ = se.Increment("ctr", 3, -1)
	_ = se.HSet("h", "f", []byte("v"), -1)
	keys, overflow := tr.Drain()
	sort.Strings(keys)
	want := []string{"before", "ctr", "h" + hashFieldSep + "f", "m1", "m2", "p"}
	if overflow || fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("drained %q (overflow %v), want %q", keys, overflow, want)
	}
	if keys, _ := tr.Drain(); len(keys) != 0 {
		t.Fatalf("second drain returned %q", keys)
	}
	tr.Close()
	_ = se.Put("after", []byte("x"), -1)
	if keys, _ := tr.Drain(); len(keys) != 0 {
		t.Fatalf("closed tracker noted %q", keys)
	}
	if _, err := se.TrackWrites(0); err != nil {
		t.Fatalf("tracker not released by Close: %v", err)
	}
}

func TestWriteTracker_Overflow(t *testing.T) {
	se := newSnapEngine(t)
	tr, _ := se.TrackWrites(3)
	defer tr.Close()
	for i := 0; i < 5; i++ {
		_ = se.Put(fmt.Sprint("k", i), []byte("v"), -1)
	}
	if _, overflow := tr.Drain(); !overflow {
		t.Fatal("no overflow past the limit")
	}
}

// ReadForSnapshot returns the absolute expiry, treats expired keys as
// absent without tombstoning them, and never fills the cache.
func TestReadForSnapshot(t *testing.T) {
	se := newSnapEngine(t)
	_ = se.Put("ttl", []byte("v"), 100)
	_ = se.Put("short", []byte("v"), 1)
	_ = se.Put("immortal", []byte("w"), -1)
	kv, ok, err := se.ReadForSnapshot("ttl")
	now := time.Now().UnixMicro()
	if err != nil || !ok || string(kv.Value) != "v" || kv.ExpiryUs < now+99_000_000 || kv.ExpiryUs > now+100_000_000 {
		t.Fatalf("ttl: %+v ok=%v err=%v", kv, ok, err)
	}
	if kv, ok, _ := se.ReadForSnapshot("immortal"); !ok || kv.ExpiryUs != 0 {
		t.Fatalf("immortal: %+v ok=%v", kv, ok)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, ok, _ := se.ReadForSnapshot("short"); ok {
		t.Fatal("an expired key was returned")
	}
	if _, ok, _ := se.ReadForSnapshot("missing"); ok {
		t.Fatal("a missing key was returned")
	}
}

// Raw restore writes: an absolute expiry is installed exactly; deleting a
// record does not cascade to its vector (the snapshot states that key
// itself); an index entry written raw is not re-derived; a put of an
// already-expired key deletes it.
func TestRestorer_RawWrites(t *testing.T) {
	se := newSnapEngine(t)
	if err := se.RegisterVectorNamespace("emb", 2); err != nil {
		t.Fatal(err)
	}
	if err := se.Put("rec", []byte("r"), -1); err != nil {
		t.Fatal(err)
	}
	if err := se.PutVector("emb", "rec", []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	_ = se.Put("old", []byte("x"), -1)

	r := se.NewRestorer()
	exp := time.Now().Add(time.Hour).UnixMicro()
	if err := r.Put(SnapshotKV{Key: "exact", Value: []byte("e"), ExpiryUs: exp}); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete("rec"); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(SnapshotKV{Key: "old", Value: []byte("gone"), ExpiryUs: 1}); err != nil {
		t.Fatal(err)
	}
	if err := r.Finish(nil); err != nil {
		t.Fatal(err)
	}
	if kv, ok, _ := se.ReadForSnapshot("exact"); !ok || kv.ExpiryUs != exp {
		t.Fatalf("exact: %+v ok=%v, want expiry %d", kv, ok, exp)
	}
	if _, err := se.Get(VectorPersistKey("emb", "rec")); err != nil {
		t.Fatalf("a raw delete of the record cascaded to its vector: %v", err)
	}
	if _, err := se.Get("old"); err == nil {
		t.Fatal("an expired restore put left the key in place")
	}
	if !r.SearchTouched() {
		// Only the record changed; the vector key was not written.
		t.Log("no search key touched")
	}
	// The vector is still searchable (its key is live, the RAM index intact).
	if res, err := se.SearchVector("emb", []float32{1, 0}, 1); err != nil || len(res) != 1 || res[0].ID != "rec" {
		t.Fatalf("vector search after restore: %v %v", res, err)
	}
}

// After a restore that changed search keys, the RAM indexes are exactly the
// restored keys (a vector deleted raw is gone from search, one written raw
// is found).
func TestRestorer_RebuildsSearch(t *testing.T) {
	se := newSnapEngine(t)
	_ = se.RegisterVectorNamespace("emb", 2)
	_ = se.PutVector("emb", "a", []float32{1, 0})
	r := se.NewRestorer()
	_ = r.Delete(VectorPersistKey("emb", "a"))
	_ = r.Put(SnapshotKV{Key: VectorPersistKey("emb", "b"), Value: encodeVector([]float32{0, 1})})
	if err := r.Finish(nil); err != nil {
		t.Fatal(err)
	}
	res, err := se.SearchVector("emb", []float32{0, 1}, 5)
	if err != nil || len(res) != 1 || res[0].ID != "b" {
		t.Fatalf("search after restore: %v %v", res, err)
	}
}
