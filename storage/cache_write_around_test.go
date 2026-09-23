package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// multiPutKVSep refreshes the cache write-around: it updates a key that is
// already resident and does not insert one that is not. The update half is the
// correctness half — dropping it would leave a reader serving the superseded
// value indefinitely, and nothing else in the engine would notice, because a
// cache hit returns before the index is ever consulted.
func TestMultiPut_OverwriteOfCachedKeyIsNotStale(t *testing.T) {
	se, err := NewStorageEngine(testStorageConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	<-se.ReplayDone

	const n = 64
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("cached-%d", i)
	}

	v1 := func(i int) []byte { return []byte(fmt.Sprintf("v1-%d", i)) }
	v2 := func(i int) []byte { return []byte(fmt.Sprintf("v2-value-%d", i)) }

	// Write, then read, so every key is resident in the LIRS cache.
	first := make([]MultiPutRequest, n)
	for i := range first {
		first[i] = MultiPutRequest{Key: keys[i], Value: v1(i), TTL: -1}
	}
	for i, err := range se.MultiPut(first) {
		if err != nil {
			t.Fatalf("MultiPut[%d]: %v", i, err)
		}
	}
	for i, k := range keys {
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if !bytes.Equal(got, v1(i)) {
			t.Fatalf("key %s: got %q, want %q", k, got, v1(i))
		}
	}

	// Overwrite through the batch path. Values differ in length so a stale
	// read is unambiguous.
	second := make([]MultiPutRequest, n)
	for i := range second {
		second[i] = MultiPutRequest{Key: keys[i], Value: v2(i), TTL: -1}
	}
	for i, err := range se.MultiPut(second) {
		if err != nil {
			t.Fatalf("MultiPut overwrite[%d]: %v", i, err)
		}
	}
	for i, k := range keys {
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s after overwrite: %v", k, err)
		}
		if !bytes.Equal(got, v2(i)) {
			t.Fatalf("key %s: got %q, want %q — the batch path left a stale "+
				"value in the cache", k, got, v2(i))
		}
	}
}

// PutIfPresent must not resurrect a key the cache does not hold: that is the
// whole point of write-around, and inserting there would put bulk-ingest data
// back in the way of the read working set.
func TestLIRSCache_PutIfPresent(t *testing.T) {
	c := NewLIRSCache(8, 0.9)

	if c.PutIfPresent("absent", []byte("x")) {
		t.Error("PutIfPresent reported an update for a key the cache never held")
	}
	if _, ok := c.Get("absent"); ok {
		t.Error("PutIfPresent inserted a key it should have skipped")
	}

	c.Put("present", []byte("old"))
	if !c.PutIfPresent("present", []byte("new-and-longer")) {
		t.Fatal("PutIfPresent reported no update for a resident key")
	}
	got, ok := c.Get("present")
	if !ok {
		t.Fatal("resident key vanished after PutIfPresent")
	}
	if !bytes.Equal(got, []byte("new-and-longer")) {
		t.Fatalf("got %q, want %q", got, "new-and-longer")
	}
}
