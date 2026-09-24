//go:build cgo && go1.21

package storage

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestEntryTable_NativeMatchesMap drives the native table and the map table
// with the same random operation stream and requires identical answers after
// every step. The map is the oracle: it is the implementation every other
// test in this package already validates.
//
// The stream is built to hit what an open-addressing table gets wrong:
//   - forced hash collisions (distinct keys sharing one hash), so a lookup
//     that trusted the hash alone would return the wrong entry;
//   - the empty key, and one key larger than the 64 KiB scan buffer;
//   - heavy delete churn, which exercises backward-shift deletion across
//     wrap-around and arena compaction.
func TestEntryTable_NativeMatchesMap(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 42, 1234} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runEntryTableModel(t, seed, 20000)
		})
	}
}

func runEntryTableModel(t *testing.T, seed int64, ops int) {
	rng := rand.New(rand.NewSource(seed))
	nat := newNativeTable()
	defer nat.free()
	ref := newMapTable()

	keys := make([]string, 0, 3000)
	hashes := make(map[string]uint64, 3002)
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("k:%d:%x", i, rng.Int63())
		keys = append(keys, k)
		// Every run of 8 consecutive keys shares ONE hash, so a table that
		// matched on the hash alone would hand back a neighbour's entry.
		hashes[k] = fnv64a(fmt.Sprint("group", i/8))
	}
	for _, k := range []string{"", strings.Repeat("B", 100<<10)} {
		keys = append(keys, k)
		hashes[k] = fnv64a(k)
	}
	hashOf := func(k string) uint64 { return hashes[k] }

	for step := 0; step < ops; step++ {
		k := keys[rng.Intn(len(keys))]
		h := hashOf(k)
		switch op := rng.Intn(10); {
		case op < 5:
			e := IndexEntry{
				DiskOffset:       rng.Uint64(),
				ValueSize:        rng.Uint32(),
				WriteTimestampUs: rng.Int63(),
				CRC32C:           rng.Uint32(),
				Flags:            uint8(rng.Intn(256)),
			}
			e._reserved = [8]byte{9, 9, 9, 9, 9, 9, 9, 9} // must be ignored
			o1, had1 := nat.swap(k, h, &e)
			o2, had2 := ref.swap(k, h, &e)
			o2._reserved = [8]byte{} // the map keeps what it was given
			if had1 != had2 || o1 != o2 {
				t.Fatalf("step %d swap(%q): native=(%v,%+v) map=(%v,%+v)", step, trunc(k), had1, o1, had2, o2)
			}
		case op < 8:
			d1, d2 := nat.del(k, h), ref.del(k, h)
			if d1 != d2 {
				t.Fatalf("step %d del(%q): native=%v map=%v", step, trunc(k), d1, d2)
			}
		default:
			bump := func(e *IndexEntry) bool { e.ValueSize++; return rng.Intn(2) == 0 }
			// Same decision on both sides: re-seed the coin per call.
			s := rng.Int63()
			rng.Seed(s)
			u1 := nat.update(k, h, bump)
			rng.Seed(s)
			u2 := ref.update(k, h, bump)
			if u1 != u2 {
				t.Fatalf("step %d update(%q): native=%v map=%v", step, trunc(k), u1, u2)
			}
		}
		g1, ok1 := nat.get(k, h)
		g2, ok2 := ref.get(k, h)
		g2._reserved = [8]byte{}
		if ok1 != ok2 || g1 != g2 {
			t.Fatalf("step %d get(%q): native=(%v,%+v) map=(%v,%+v)", step, trunc(k), ok1, g1, ok2, g2)
		}
		if nat.len() != ref.len() {
			t.Fatalf("step %d len: native=%d map=%d", step, nat.len(), ref.len())
		}
	}

	// Full walks must agree too.
	collect := func(tb entryTable) []string {
		var out []string
		tb.rangeAll(func(k string, e *IndexEntry) bool {
			out = append(out, fmt.Sprintf("%s|%d|%d", trunc(k), e.DiskOffset, e.ValueSize))
			return true
		})
		sort.Strings(out)
		return out
	}
	a, b := collect(nat), collect(ref)
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatalf("rangeAll differs: native has %d entries, map has %d", len(a), len(b))
	}
	n := 0
	nat.rangeEntries(func(e *IndexEntry) bool { n++; return true })
	if n != ref.len() {
		t.Fatalf("rangeEntries visited %d, want %d", n, ref.len())
	}
}

// TestEntryTable_NativeEarlyStopAndFree checks the two lifecycle edges: a
// range callback that stops early must not keep going, and a freed table
// must behave as empty (the engine relies on that after Close).
func TestEntryTable_NativeEarlyStopAndFree(t *testing.T) {
	nat := newNativeTable().(*nativeTable)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprint("key", i)
		nat.swap(k, fnv64a(k), &IndexEntry{ValueSize: uint32(i)})
	}
	seen := 0
	nat.rangeAll(func(string, *IndexEntry) bool { seen++; return seen < 10 })
	if seen != 10 {
		t.Fatalf("early stop: callback ran %d times, want 10", seen)
	}
	if nat.bytes() == 0 {
		t.Fatal("1000 inserts allocated nothing")
	}
	nat.free()
	nat.free() // idempotent
	if nat.bytes() != 0 {
		t.Fatalf("freed table still reports %d bytes", nat.bytes())
	}
	if _, ok := nat.get("key1", fnv64a("key1")); ok || nat.len() != 0 {
		t.Fatalf("freed table is not empty")
	}
	nat.rangeAll(func(string, *IndexEntry) bool { t.Fatal("range over freed table"); return false })
}

// TestEntryTable_DeleteAllReleasesMemory: a shard that empties out must give
// its slots and arena back, so churned-out shards cost nothing.
func TestEntryTable_DeleteAllReleasesMemory(t *testing.T) {
	nat := newNativeTable().(*nativeTable)
	defer nat.free()
	for i := 0; i < 50000; i++ {
		k := fmt.Sprint("churn-", i)
		nat.swap(k, fnv64a(k), &IndexEntry{})
	}
	if nat.bytes() == 0 {
		t.Fatal("inserts allocated nothing")
	}
	for i := 0; i < 50000; i++ {
		k := fmt.Sprint("churn-", i)
		if !nat.del(k, fnv64a(k)) {
			t.Fatalf("del %q: absent", k)
		}
	}
	if got := nat.bytes(); got != 0 {
		t.Fatalf("empty table still holds %d bytes", got)
	}
}

func trunc(k string) string {
	if len(k) > 24 {
		return fmt.Sprintf("%s…(%d)", k[:24], len(k))
	}
	return k
}

// BenchmarkIndex_Footprint measures what the two index implementations cost
// at scale: resident bytes per key and the stop-the-world-inclusive time of a
// full GC with the index live. Run with:
//
//	go test ./storage -run '^$' -bench Index_Footprint -benchtime 1x
//
// Keys are 24-byte "user:%019d" strings, the shape the load tests use.
func BenchmarkIndex_Footprint(b *testing.B) {
	for _, impl := range []string{"map", "native"} {
		b.Run(impl, func(b *testing.B) {
			b.Setenv(IndexImplEnv, impl)
			const n = 2_000_000
			runtime.GC()
			var m0 runtime.MemStats
			runtime.ReadMemStats(&m0)
			c0 := NativeIndexBytes()
			rss0 := maxRSSBytes()

			si := newShardedIndex()
			si.ordered = nil // measure the Index Vault alone, not the skiplist
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("user:%019d", i)
				si.put(k, &IndexEntry{DiskOffset: uint64(i) * 4096, ValueSize: 128}, nil)
			}

			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			goBytes := float64(m1.HeapAlloc) - float64(m0.HeapAlloc)
			cBytes := float64(NativeIndexBytes() - c0)

			start := time.Now()
			for i := 0; i < 5; i++ {
				runtime.GC()
			}
			gcMs := float64(time.Since(start).Microseconds()) / 5 / 1000

			b.ReportMetric(goBytes/n, "goheap-B/key")
			b.ReportMetric(cBytes/n, "offheap-B/key")
			b.ReportMetric((goBytes+cBytes)/n, "total-B/key")
			b.ReportMetric(gcMs, "fullGC-ms")
			// Peak RSS growth is the honest comparison: the map's figure
			// includes the GC headroom its heap needs, the native one does
			// not need any. Process-wide, so run one impl per process:
			//   -bench 'Index_Footprint/map$'  and  -bench 'Index_Footprint/native$'
			b.ReportMetric(float64(maxRSSBytes()-rss0)/n, "peakRSS-B/key")
			runtime.KeepAlive(si)
			si.close()
		})
	}
}

// BenchmarkIndex_Get is the hot-path cost the native table adds: one cgo
// transition per cache-miss lookup, versus a Go map probe.
func BenchmarkIndex_Get(b *testing.B) {
	for _, impl := range []string{"map", "native"} {
		b.Run(impl, func(b *testing.B) {
			b.Setenv(IndexImplEnv, impl)
			const n = 1_000_000
			si := newShardedIndex()
			si.ordered = nil
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("user:%019d", i)
				si.put(keys[i], &IndexEntry{DiskOffset: uint64(i)}, nil)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					k := keys[i%n]
					if _, _, ok := si.getHashed(k, fnv64a(k)); !ok {
						b.Fatal("missing key")
					}
					i += 7919
				}
			})
			b.StopTimer()
			si.close()
		})
	}
}

// maxRSSBytes is the process's peak resident set size. getrusage reports
// ru_maxrss in bytes on Darwin and in KiB on Linux.
func maxRSSBytes() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "linux" {
		return int64(ru.Maxrss) * 1024
	}
	return int64(ru.Maxrss)
}
