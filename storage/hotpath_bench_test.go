package storage

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/tracing"
)

// The tracing package defaults to AlwaysSample, but cmd/server configures
// RateSampler(0.01). Benchmarking against the package default would measure a
// 100%-sampled trace pipeline that production never runs, so mirror the
// server's configuration here.
func init() {
	// The engine logs per-pass GC and bloom lines. They interleave with
	// benchmark output on the same stream and make the results unparseable by
	// benchstat, so discard them — but only under -bench, so `go test` output
	// is untouched. Flags are not parsed yet at init time, hence the os.Args
	// scan.
	for _, a := range os.Args {
		if strings.HasPrefix(a, "-test.bench") {
			log.SetOutput(io.Discard)
			break
		}
	}

	tracing.Configure(tracing.Configuration{
		ServiceName:   "bench",
		Sampler:       tracing.RateSampler(0.01),
		SlowThreshold: 50 * time.Millisecond,
		Retain:        1024,
	})
}

// Benchmarks for the paths that actually run per client request.
//
// Scope note: on macOS fdatasync is plain fsync(2), which returns at the drive
// cache in ~0.02 ms without flushing it — so the write benchmarks here are
// dominated by the group-commit window and, if anything, FLATTER than Linux
// NVMe (~0.2 ms of real round trip). They say nothing useful about Linux
// write throughput in either direction. What IS portable is
// everything CPU-bound: cache lookups, index lookups, hashing, and the
// allocation behaviour of each path. Those are what these measure.

func benchEngine(b *testing.B, cacheMB uint32) *StorageEngine {
	return benchEngineCfg(b, cacheMB, false)
}

// benchEngineCfg builds an engine with the ordered key index optionally
// disabled, so the cost it adds to the write path can be measured directly.
func benchEngineCfg(b *testing.B, cacheMB uint32, disableOrdered bool) *StorageEngine {
	b.Helper()
	cfg := DefaultStorageConfig()
	cfg.DisableOrderedIndex = disableOrdered
	cfg.DataDirPath = b.TempDir()
	cfg.DataDirPaths = nil
	cfg.CacheMaxSizeMB = cacheMB
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	cfg.ScrubEnabled = false
	cfg.DefragInterval = time.Hour
	// Blooms default to 4 M bits × 8192 shards = 4 GB, which dwarfs a
	// benchmark's working set and distorts both memory and startup. Size them
	// for the benchmark instead.
	cfg.BloomFilterShardBits = 1 << 12

	se, err := NewStorageEngine(cfg)
	if err != nil {
		b.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	b.Cleanup(func() { se.Close() })
	return se
}

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:key:%08d", i)
	}
	return keys
}

// seed loads keys through MultiPut. Seeding with individual Put costs one
// fdatasync per key, which makes setup dwarf the measurement; MultiPut
// amortises them into one flush per batch.
func seed(b *testing.B, se *StorageEngine, keys []string, val []byte) {
	b.Helper()
	const chunk = 2048
	for i := 0; i < len(keys); i += chunk {
		end := i + chunk
		if end > len(keys) {
			end = len(keys)
		}
		reqs := make([]MultiPutRequest, 0, end-i)
		for _, k := range keys[i:end] {
			reqs = append(reqs, MultiPutRequest{Key: k, Value: val, TTL: -1})
		}
		for _, err := range se.MultiPut(reqs) {
			if err != nil {
				b.Fatalf("seed: %v", err)
			}
		}
	}
}

// ── Read path ────────────────────────────────────────────────────────────────

// BenchmarkGet_CacheHit is the single most important number in the engine:
// >95% of reads are meant to land here.
func BenchmarkGet_CacheHit(b *testing.B) {
	se := benchEngine(b, 256)
	keys := benchKeys(10000)
	val := make([]byte, 128)
	seed(b, se, keys, val)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			if _, err := se.Get(keys[rng.Intn(len(keys))]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGet_Miss_NotFound is the negative-lookup path the per-shard bloom
// exists to accelerate.
func BenchmarkGet_Miss_NotFound(b *testing.B) {
	se := benchEngine(b, 256)
	keys := benchKeys(10000)
	val := make([]byte, 128)
	seed(b, se, keys, val)
	absent := make([]string, 10000)
	for i := range absent {
		absent[i] = fmt.Sprintf("absent:key:%08d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			_, _ = se.Get(absent[rng.Intn(len(absent))])
		}
	})
}

// BenchmarkGet_CacheMiss_VLogRead forces the disk path by using a cache far
// smaller than the working set.
func BenchmarkGet_CacheMiss_VLogRead(b *testing.B) {
	se := benchEngine(b, 1)
	keys := benchKeys(20000)
	val := make([]byte, 128)
	seed(b, se, keys, val)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			_, _ = se.Get(keys[rng.Intn(len(keys))])
		}
	})
}

func BenchmarkMultiGet_256(b *testing.B) {
	se := benchEngine(b, 256)
	keys := benchKeys(10000)
	val := make([]byte, 128)
	seed(b, se, keys, val)
	batch := keys[:256]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.MultiGet(batch)
	}
}

// ── Index / hashing primitives ───────────────────────────────────────────────

// BenchmarkIndexGet isolates the Index Vault lookup from cache and disk.
func BenchmarkIndexGet(b *testing.B) {
	si := newShardedIndex()
	keys := benchKeys(100000)
	for i, k := range keys {
		si.put(k, &IndexEntry{KeyHash: fnv64a(k), DiskOffset: uint64(i + 1)}, nil)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			si.get(keys[rng.Intn(len(keys))])
		}
	})
}

// BenchmarkFNV64a measures the hash on the hot path. Get calls it up to three
// times per lookup (shardFor, the bloom probe, and the miss-accounting path).
func BenchmarkFNV64a(b *testing.B) {
	key := "bench:key:00012345"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkU64 = fnv64a(key)
	}
}

var sinkU64 uint64

// ── Write path ───────────────────────────────────────────────────────────────

// BenchmarkPut is fdatasync-bound on macOS; read it for allocation counts, not
// for throughput.
func BenchmarkPut(b *testing.B) {
	se := benchEngine(b, 256)
	val := make([]byte, 128)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := fmt.Sprintf("w:%d", rng.Int63())
			if err := se.Put(k, val, -1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMultiPut_1024(b *testing.B) {
	se := benchEngine(b, 256)
	val := make([]byte, 128)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqs := make([]MultiPutRequest, 1024)
		for j := range reqs {
			reqs[j] = MultiPutRequest{Key: fmt.Sprintf("mp:%d:%d", i, j), Value: val, TTL: -1}
		}
		for _, err := range se.MultiPut(reqs) {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// ── Ordered-index cost ───────────────────────────────────────────────────────
//
// Every Put maintains the RangeScan/ScanCursor skiplist, whether or not the
// workload ever range-scans. These pairs measure what that costs so the
// DisableOrderedIndex trade can be made on numbers.

func benchMultiPutOrdered(b *testing.B, disableOrdered bool) {
	se := benchEngineCfg(b, 256, disableOrdered)
	val := make([]byte, 128)
	reqs := make([]MultiPutRequest, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range reqs {
			reqs[j] = MultiPutRequest{Key: fmt.Sprintf("oi:%d:%d", i, j), Value: val, TTL: -1}
		}
		for _, err := range se.MultiPut(reqs) {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMultiPut_OrderedIndexOn(b *testing.B)  { benchMultiPutOrdered(b, false) }
func BenchmarkMultiPut_OrderedIndexOff(b *testing.B) { benchMultiPutOrdered(b, true) }

func benchPutOrdered(b *testing.B, disableOrdered bool) {
	se := benchEngineCfg(b, 256, disableOrdered)
	val := make([]byte, 128)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			if err := se.Put(fmt.Sprintf("oi1:%d", rng.Int63()), val, -1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPut_OrderedIndexOn(b *testing.B)  { benchPutOrdered(b, false) }
func BenchmarkPut_OrderedIndexOff(b *testing.B) { benchPutOrdered(b, true) }
