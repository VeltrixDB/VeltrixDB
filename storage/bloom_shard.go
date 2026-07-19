package storage

// bloom_shard.go — Lock-free per-shard Bloom filter for negative-lookup acceleration.
//
// One filter per indexShard (1024 filters total). Sized at construction; bits
// are atomic uint64 words so Add and MayContain do not contend on a mutex.
//
// Why per-shard, not global:
//   - The shardedIndex already partitions keys by shard; a global filter wouldn't
//     give any extra information beyond what each shard already knows.
//   - Sizing per shard scales with key count: 1B keys / 1024 shards = ~1M keys/shard,
//     so each shard's filter only needs ~1M-key capacity, not 1B.
//   - Cache locality: when a shard's hot path is on one CPU, the bloom bits live
//     in that CPU's L2/L3 — global filter would be cache-line-shared across cores.
//
// Why not the existing types.go BloomFilter:
//   - It uses sync.RWMutex per access — defeats the point at multi-M ops/sec.
//   - We need lock-free atomic Or/Load on uint64 words.
//
// Hashing: derived from FNV-1a 64-bit (already used for shard routing). Two
// hash positions h1, h2 are extracted from the 64-bit FNV; k probes use double
// hashing h_i = h1 + i × h2.
//
// Maintenance: Add() never clears bits, so deletes accumulate stale bits over
// time. Rebuild() walks the live index and rebuilds the filter from scratch;
// the defragmenter triggers Rebuild on every full GC pass for shards it visits.
//
// False-positive math (k optimal = (m/n) × ln 2):
//   bits_per_key=10 → FP ~1%   (k=7)
//   bits_per_key=14 → FP ~0.1% (k=10)
//   bits_per_key=4  → FP ~7%   (k=3)
//
// Default: 4M bits per shard at 10 bits/key — sized for ~400K keys/shard at
// ~1% FP rate. Memory: 1024 × 512 KB = 512 MB. Acceptable for n2-highmem-64.
// Operators with > 400K keys/shard can raise BloomFilterBitsPerShard at cost
// of memory.

import (
	"sync/atomic"
	"unsafe"
)

// shardBloom is a fixed-size lock-free Bloom filter.
//
// `bits` is a power-of-2-word array; the bit count is len(bits) × 64 and must
// be a power of 2 so position lookup uses a single AND.
type shardBloom struct {
	bits []atomic.Uint64
	mask uint64 // bit count - 1; must be (len(bits)*64) - 1 with power-of-2 word count
	k    uint8  // hash count, capped at 16
}

// newShardBloom allocates a filter with at least bitCount bits, rounded up to
// the next power of 2. k is the number of hash probes per Add/MayContain.
func newShardBloom(bitCount uint64, k uint8) *shardBloom {
	// Round bitCount up to next power-of-2 word boundary (64-bit words).
	if bitCount < 64 {
		bitCount = 64
	}
	words := (bitCount + 63) >> 6 // bits/64
	// Round up to power of 2.
	pow2 := uint64(1)
	for pow2 < words {
		pow2 <<= 1
	}
	totalBits := pow2 * 64
	if k == 0 {
		k = 7
	}
	if k > 16 {
		k = 16
	}
	return &shardBloom{
		bits: make([]atomic.Uint64, pow2),
		mask: totalBits - 1,
		k:    k,
	}
}

// hashes returns h1, h2 from the 64-bit FNV-1a we already compute for routing.
// Splitting a single 64-bit hash into two 32-bit halves and using double hashing
// gives independent-enough probes for Bloom (Kirsch–Mitzenmacher).
func (b *shardBloom) hashes(fullHash uint64) (uint32, uint32) {
	return uint32(fullHash), uint32(fullHash >> 32)
}

// Add sets the k bits corresponding to fullHash.
// Concurrent Adds and MayContains are safe.
func (b *shardBloom) Add(fullHash uint64) {
	h1, h2 := b.hashes(fullHash)
	for i := uint8(0); i < b.k; i++ {
		pos := uint64(h1) + uint64(i)*uint64(h2)
		pos &= b.mask
		word := pos >> 6
		bit := uint64(1) << (pos & 63)
		// Atomic OR via CAS — no atomic.Uint64.Or in stdlib (Go 1.23+ has it).
		// Fall back to CAS loop; in practice contention is negligible because
		// each bit position is only touched by writers to one shard.
		// IMPORTANT: break (not return) so we proceed to set the next probe bit;
		// returning here would leave only the first probe set and break the
		// "no false negatives" invariant on subsequent MayContain checks.
		for {
			old := b.bits[word].Load()
			if old&bit != 0 {
				break // bit already set — advance to next probe
			}
			if b.bits[word].CompareAndSwap(old, old|bit) {
				break // success — advance to next probe
			}
		}
	}
}

// MayContain returns true if all k bits for fullHash are set.
// false is a definitive negative; true means "may exist, must check the index".
func (b *shardBloom) MayContain(fullHash uint64) bool {
	h1, h2 := b.hashes(fullHash)
	for i := uint8(0); i < b.k; i++ {
		pos := uint64(h1) + uint64(i)*uint64(h2)
		pos &= b.mask
		word := pos >> 6
		bit := uint64(1) << (pos & 63)
		if b.bits[word].Load()&bit == 0 {
			return false
		}
	}
	return true
}

// Reset zeros all bits. Used by Rebuild.
func (b *shardBloom) Reset() {
	for i := range b.bits {
		b.bits[i].Store(0)
	}
}

// SizeBytes returns the in-memory footprint of the filter.
func (b *shardBloom) SizeBytes() int {
	return len(b.bits) * int(unsafe.Sizeof(uint64(0)))
}

// shardBloomEnabled is set by the engine constructor when blooms are configured.
// Atomic load on the read hot path avoids a cfg dereference.
var shardBloomEnabled atomic.Bool
