package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestStripedCounter_ConcurrentAddsSumExactly(t *testing.T) {
	var c StripedCounter
	const goroutines, perG = 32, 10000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				c.AddAt(fnv64a(string(rune('a'+g))+string(rune(i))), 1)
			}
		}(g)
	}
	wg.Wait()
	if got := c.Load(); got != goroutines*perG {
		t.Fatalf("Load = %d, want %d", got, goroutines*perG)
	}
}

func TestStripedCounter_StripesArePadded(t *testing.T) {
	if s := unsafe.Sizeof(paddedCounter{}); s != 128 {
		t.Fatalf("paddedCounter is %d bytes, want 128 (one stripe per cache line)", s)
	}
	// Different keys must spread over stripes, or the striping buys nothing.
	var c StripedCounter
	for i := 0; i < 4096; i++ {
		c.AddAt(fnv64a(string(rune(i))+"key"), 1)
	}
	used := 0
	for i := range c.stripes {
		if c.stripes[i].v.Load() > 0 {
			used++
		}
	}
	if used < counterStripes*3/4 {
		t.Fatalf("4096 keys used %d of %d stripes", used, counterStripes)
	}
}

// BenchmarkReadCounter compares one shared atomic with the striped counter
// under parallel increments, as Get does.
func BenchmarkReadCounter(b *testing.B) {
	b.Run("atomic", func(b *testing.B) {
		var c atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
			}
		})
	})
	b.Run("striped", func(b *testing.B) {
		var c StripedCounter
		var seed atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			h := fnv64a(string(rune(seed.Add(1))))
			for pb.Next() {
				h = h*6364136223846793005 + 1442695040888963407 // a different key each read
				c.AddAt(h, 1)
			}
		})
	})
}
