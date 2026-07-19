package storage

// ordered_index_test.go — tests for the ordered key index (skiplist) and the
// RangeScan / ScanCursor engine APIs.
//
// Covers: ordering + boundary + limit + reverse semantics, delete and TTL
// interaction, concurrent put/scan under -race, cursor pagination, the
// DisableOrderedIndex config toggle, and crash-recovery rebuild of the
// ordered view after an engine reopen (WAL replay path).

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// newOrderedTestEngine creates a single-disk engine rooted at dir.  Unlike
// newTestEngine it takes the directory explicitly so reopen tests can point a
// second engine at the same data.
func newOrderedTestEngine(t *testing.T, dir string, mutate func(*StorageConfig)) *StorageEngine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = dir
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = 16
	cfg.NumShards = 1024
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	if mutate != nil {
		mutate(cfg)
	}
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return se
}

func tempDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "veltrix-ordered-")
	if err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func scanKeysOf(kvs []KV) []string {
	keys := make([]string, len(kvs))
	for i, kv := range kvs {
		keys[i] = kv.Key
	}
	return keys
}

func assertKeys(t *testing.T, got []KV, want ...string) {
	t.Helper()
	gk := scanKeysOf(got)
	if len(gk) != len(want) {
		t.Fatalf("got %d keys %v, want %d keys %v", len(gk), gk, len(want), want)
	}
	for i := range want {
		if gk[i] != want[i] {
			t.Fatalf("key[%d] = %q, want %q (got %v)", i, gk[i], want[i], want)
		}
	}
}

// ── Skiplist unit tests (no engine) ──────────────────────────────────────────

func TestOrderedKeyIndex_InsertRemoveContains(t *testing.T) {
	oi := newOrderedKeyIndex()

	if oi.contains("a") {
		t.Fatal("empty index should not contain anything")
	}
	if !oi.Insert("b") || !oi.Insert("a") || !oi.Insert("c") {
		t.Fatal("fresh inserts must return true")
	}
	if oi.Insert("b") {
		t.Fatal("duplicate insert must return false")
	}
	if !oi.contains("a") || !oi.contains("b") || !oi.contains("c") {
		t.Fatal("inserted keys must be present")
	}
	if got := oi.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	if !oi.Remove("b") {
		t.Fatal("remove of present key must return true")
	}
	if oi.Remove("b") {
		t.Fatal("remove of absent key must return false")
	}
	if oi.contains("b") {
		t.Fatal("removed key must be absent")
	}
	// Re-insert after remove must work (node reuse / marked-node retry path).
	if !oi.Insert("b") {
		t.Fatal("re-insert after remove must return true")
	}
	if !oi.contains("b") {
		t.Fatal("re-inserted key must be present")
	}
}

func TestOrderedKeyIndex_AscendDescendBounds(t *testing.T) {
	oi := newOrderedKeyIndex()
	for _, k := range []string{"b", "d", "f", "h"} {
		oi.Insert(k)
	}

	var got []string
	walk := func(k string) bool { got = append(got, k); return true }

	// [start, end) ascending: start inclusive, end exclusive.
	got = nil
	oi.ascend("b", "f", walk)
	if fmt.Sprint(got) != fmt.Sprint([]string{"b", "d"}) {
		t.Fatalf("ascend[b,f) = %v, want [b d]", got)
	}
	// start between keys; end == "" is unbounded.
	got = nil
	oi.ascend("c", "", walk)
	if fmt.Sprint(got) != fmt.Sprint([]string{"d", "f", "h"}) {
		t.Fatalf("ascend[c,∞) = %v, want [d f h]", got)
	}
	// Empty range.
	got = nil
	oi.ascend("x", "", walk)
	if len(got) != 0 {
		t.Fatalf("ascend[x,∞) = %v, want empty", got)
	}
	// Descend: end == "" means from the max; start inclusive.
	got = nil
	oi.descend("d", "", walk)
	if fmt.Sprint(got) != fmt.Sprint([]string{"h", "f", "d"}) {
		t.Fatalf("descend[d,∞) = %v, want [h f d]", got)
	}
	// Descend with exclusive upper bound.
	got = nil
	oi.descend("", "f", walk)
	if fmt.Sprint(got) != fmt.Sprint([]string{"d", "b"}) {
		t.Fatalf("descend[,f) = %v, want [d b]", got)
	}
	// Early termination.
	got = nil
	oi.ascend("", "", func(k string) bool { got = append(got, k); return len(got) < 2 })
	if len(got) != 2 {
		t.Fatalf("ascend early-stop returned %v, want 2 keys", got)
	}
}

// TestOrderedKeyIndex_ConcurrentStress hammers the skiplist with concurrent
// inserts, removes, and full-range scans.  Run with -race.
func TestOrderedKeyIndex_ConcurrentStress(t *testing.T) {
	oi := newOrderedKeyIndex()
	const writers = 8
	const perWriter = 2000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Scanners: continuously verify ascending order while writers churn.
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				prev := ""
				oi.ascend("", "", func(k string) bool {
					if prev != "" && k <= prev {
						t.Errorf("scan out of order: %q after %q", k, prev)
						return false
					}
					prev = k
					return true
				})
			}
		}()
	}

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("k%08d", rng.Intn(perWriter*writers))
				if rng.Intn(2) == 0 {
					oi.Insert(k)
				} else {
					oi.Remove(k)
				}
			}
		}(w)
	}

	// Let writers and scanners overlap briefly, then stop the scanners and
	// wait for everyone.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	<-time.After(10 * time.Millisecond)
	close(stop)
	<-done

	// Final pass must be strictly sorted and duplicate-free.
	prev := ""
	oi.ascend("", "", func(k string) bool {
		if prev != "" && k <= prev {
			t.Fatalf("final scan out of order: %q after %q", k, prev)
		}
		prev = k
		return true
	})
}

// ── Engine RangeScan / ScanCursor ─────────────────────────────────────────────

func TestRangeScan_OrderBoundsLimitReverse(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	// Insert out of order so ordering cannot come from insertion sequence.
	for _, i := range []int{7, 2, 9, 0, 5, 3, 8, 1, 6, 4} {
		k := fmt.Sprintf("k%02d", i)
		if err := se.Put(k, []byte("v"+k), -1); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Full ascending scan.
	kvs, err := se.RangeScan("", "", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09")
	for _, kv := range kvs {
		if !bytes.Equal(kv.Value, []byte("v"+kv.Key)) {
			t.Fatalf("value for %s = %q, want %q", kv.Key, kv.Value, "v"+kv.Key)
		}
	}

	// Bounds: start inclusive, end exclusive.
	kvs, err = se.RangeScan("k03", "k07", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "k03", "k04", "k05", "k06")

	// Limit.
	kvs, err = se.RangeScan("k03", "k07", 2, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "k03", "k04")

	// Reverse: descending from just below end.
	kvs, err = se.RangeScan("k03", "k07", 0, true)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "k06", "k05", "k04", "k03")

	// Reverse + limit.
	kvs, err = se.RangeScan("", "", 3, true)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "k09", "k08", "k07")

	// Empty range.
	kvs, err = se.RangeScan("x", "z", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(kvs) != 0 {
		t.Fatalf("empty range returned %v", scanKeysOf(kvs))
	}
}

func TestScanCursor_Pagination(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	const n = 10
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("p%02d", i)
		if err := se.Put(k, []byte("v"), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var all []string
	cursor := ""
	pages := 0
	for {
		kvs, next, err := se.ScanCursor(cursor, 3)
		if err != nil {
			t.Fatalf("ScanCursor: %v", err)
		}
		all = append(all, scanKeysOf(kvs)...)
		pages++
		if next == "" {
			break
		}
		cursor = next
		if pages > n {
			t.Fatal("cursor did not terminate")
		}
	}
	if pages != 4 { // 3+3+3+1
		t.Fatalf("pages = %d, want 4", pages)
	}
	if !sort.StringsAreSorted(all) || len(all) != n {
		t.Fatalf("paginated keys = %v, want %d sorted keys", all, n)
	}
	for i, k := range all {
		if want := fmt.Sprintf("p%02d", i); k != want {
			t.Fatalf("all[%d] = %q, want %q", i, k, want)
		}
	}
}

func TestRangeScan_DeleteAndTTL(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	if err := se.Put("d1", []byte("v1"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Put("d2", []byte("v2"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := se.Put("d3", []byte("v3"), 1); err != nil { // 1 s TTL
		t.Fatalf("Put: %v", err)
	}

	// Delete removes the key from scans immediately.
	if err := se.Delete("d2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	kvs, err := se.RangeScan("d", "e", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "d1", "d3")

	// TTL expiry: after the TTL elapses the key must be skipped (lazy path —
	// no background scanner tick needed) and lazily tombstoned, which also
	// removes it from the ordered index.
	time.Sleep(1100 * time.Millisecond)
	kvs, err = se.RangeScan("d", "e", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "d1")
	if se.index.ordered.contains("d3") {
		t.Fatal("expired key should have been lazily removed from the ordered index")
	}

	// Re-put after delete: key reappears.
	if err := se.Put("d2", []byte("v2b"), -1); err != nil {
		t.Fatalf("re-Put: %v", err)
	}
	kvs, err = se.RangeScan("d", "e", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "d1", "d2")
	if !bytes.Equal(kvs[1].Value, []byte("v2b")) {
		t.Fatalf("re-put value = %q, want v2b", kvs[1].Value)
	}
}

func TestRangeScan_MultiPutAndAtomicOpsVisible(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	reqs := []MultiPutRequest{
		{Key: "m2", Value: []byte("v2"), TTL: -1},
		{Key: "m1", Value: []byte("v1"), TTL: -1},
		{Key: "m3", Value: []byte("v3"), TTL: -1},
	}
	for i, err := range se.MultiPut(reqs) {
		if err != nil {
			t.Fatalf("MultiPut[%d]: %v", i, err)
		}
	}
	// Keys created through the atomic-op path (direct shard.entries install)
	// must also appear in ordered scans.
	if res, err := se.SetIfNotExists("m0", []byte("v0"), -1); err != nil || res != SetNXCreated {
		t.Fatalf("SetIfNotExists: res=%v err=%v", res, err)
	}

	kvs, err := se.RangeScan("m", "n", 0, false)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	assertKeys(t, kvs, "m0", "m1", "m2", "m3")
}

// TestRangeScan_ConcurrentPutScan runs writers and scanners in parallel.
// Run with -race.  Scans must always return strictly ascending,
// duplicate-free keys with self-consistent values.  (Same-key Get-vs-Delete
// concurrency is not exercised here: engine.Get reads IndexEntry.Flags
// without the shard lock — a pre-existing engine pattern — so delete-vs-scan
// concurrency is covered at the skiplist level by
// TestOrderedKeyIndex_ConcurrentStress instead.)
func TestRangeScan_ConcurrentPutScan(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	const writers = 4
	const perWriter = 60
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				kvs, err := se.RangeScan("c", "d", 0, false)
				if err != nil {
					t.Errorf("RangeScan: %v", err)
					return
				}
				prev := ""
				for _, kv := range kvs {
					if prev != "" && kv.Key <= prev {
						t.Errorf("scan out of order: %q after %q", kv.Key, prev)
						return
					}
					prev = kv.Key
					if !bytes.Equal(kv.Value, []byte("v-"+kv.Key)) {
						t.Errorf("value mismatch for %s: %q", kv.Key, kv.Value)
						return
					}
				}
			}
		}()
	}

	var writerWg sync.WaitGroup
	for w := 0; w < writers; w++ {
		writerWg.Add(1)
		go func(w int) {
			defer writerWg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 100))
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("c%02d-%04d", w, rng.Intn(perWriter))
				if err := se.Put(k, []byte("v-"+k), -1); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	writerWg.Wait()
	close(stop)
	wg.Wait()
}

func TestRangeScan_DisabledToggle(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), func(cfg *StorageConfig) {
		cfg.DisableOrderedIndex = true
	})
	defer se.Close()

	if err := se.Put("k1", []byte("v1"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := se.RangeScan("", "", 0, false); !errors.Is(err, ErrOrderedIndexDisabled) {
		t.Fatalf("RangeScan err = %v, want ErrOrderedIndexDisabled", err)
	}
	if _, _, err := se.ScanCursor("", 10); !errors.Is(err, ErrOrderedIndexDisabled) {
		t.Fatalf("ScanCursor err = %v, want ErrOrderedIndexDisabled", err)
	}
	// Point lookups are unaffected by the toggle.
	if v, err := se.Get("k1"); err != nil || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get = %q, %v", v, err)
	}
}

// TestRangeScan_RecoveryRebuild verifies the ordered index is rebuilt from
// the WAL on startup: keys written (and one deleted) before shutdown must
// scan in order after reopening the engine on the same data directory.
func TestRangeScan_RecoveryRebuild(t *testing.T) {
	dir := tempDataDir(t)

	se := newOrderedTestEngine(t, dir, nil)
	for _, i := range []int{4, 1, 3, 0, 2} {
		k := fmt.Sprintf("r%02d", i)
		if err := se.Put(k, []byte("v"+k), -1); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := se.Delete("r03"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	se2 := newOrderedTestEngine(t, dir, nil)
	defer se2.Close()
	<-se2.ReplayDone // ordered index is rebuilt by the async post-replay pass

	kvs, err := se2.RangeScan("", "", 0, false)
	if err != nil {
		t.Fatalf("RangeScan after reopen: %v", err)
	}
	assertKeys(t, kvs, "r00", "r01", "r02", "r04")
	for _, kv := range kvs {
		if !bytes.Equal(kv.Value, []byte("v"+kv.Key)) {
			t.Fatalf("value for %s after reopen = %q, want %q", kv.Key, kv.Value, "v"+kv.Key)
		}
	}
	// Reverse scan works off the rebuilt index too.
	kvs, err = se2.RangeScan("", "", 2, true)
	if err != nil {
		t.Fatalf("reverse RangeScan after reopen: %v", err)
	}
	assertKeys(t, kvs, "r04", "r02")
}

func TestRangeScanNS_NamespaceIsolation(t *testing.T) {
	se := newOrderedTestEngine(t, tempDataDir(t), nil)
	defer se.Close()

	for _, k := range []string{"b", "a", "c"} {
		if err := se.PutNS("ns1", k, []byte("ns1-"+k), -1); err != nil {
			t.Fatalf("PutNS: %v", err)
		}
	}
	if err := se.PutNS("ns2", "a", []byte("ns2-a"), -1); err != nil {
		t.Fatalf("PutNS: %v", err)
	}

	kvs, err := se.RangeScanNS("ns1", "", "", 0, false)
	if err != nil {
		t.Fatalf("RangeScanNS: %v", err)
	}
	assertKeys(t, kvs, "a", "b", "c") // prefix stripped, ns2 excluded
	for _, kv := range kvs {
		if !bytes.Equal(kv.Value, []byte("ns1-"+kv.Key)) {
			t.Fatalf("value for %s = %q", kv.Key, kv.Value)
		}
	}
}
