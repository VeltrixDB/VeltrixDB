package storage

import "sync/atomic"

// StripedCounter is a monotonically increasing counter split over
// cache-line-padded stripes, for the counters every Get bumps (Reads,
// CacheHits, CacheMisses). A single atomic.Uint64 there is one cache line
// that every core writes on every read: profiling a read-heavy in-process
// load put CacheHits.Add + Reads.Add at 14% of CPU, and dropping them raised
// reads/s by 30–55%. Load sums the stripes; it is only called on a scrape,
// INFO or /admin/stats, never on the hot path.
//
// AddAt picks the stripe from a hint the caller already has — the key's
// fnv64a hash — so concurrent reads of different keys land on different
// lines. The value is exact (every Add is counted once); a Load concurrent
// with Adds may miss the in-flight ones, like any atomic counter read.
type StripedCounter struct {
	stripes [counterStripes]paddedCounter
}

const counterStripes = 64 // power of two

// paddedCounter fills 128 bytes: the cache line on Apple silicon, two on
// x86-64 (where the adjacent-line prefetcher pairs them anyway).
type paddedCounter struct {
	v atomic.Uint64
	_ [120]byte
}

// AddAt adds n to the stripe selected by hint. Bits 13+ are used because the
// low 13 bits of fnv64a already pick the index shard; any bits are correct.
func (c *StripedCounter) AddAt(hint, n uint64) {
	c.stripes[(hint>>13)&(counterStripes-1)].v.Add(n)
}

// Load returns the sum of all stripes.
func (c *StripedCounter) Load() uint64 {
	var sum uint64
	for i := range c.stripes {
		sum += c.stripes[i].v.Load()
	}
	return sum
}
