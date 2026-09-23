package storage

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// BenchmarkMultiPut_Concurrent mirrors what `loadtest --batch-size=1024
// --concurrency=8` does, but at the engine level with no TCP or protocol in
// the way. Comparing the two isolates engine cost from server cost.
//
// It reports the per-batch latency distribution, because that is what a client
// issuing one MPut actually waits for — a mean over 1024 keys hides it.
func BenchmarkMultiPut_Concurrent(b *testing.B) {
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			se := benchEngine(b, 512)
			val := make([]byte, 128)

			var mu sync.Mutex
			lat := make([]time.Duration, 0, b.N*workers)

			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					reqs := make([]MultiPutRequest, 1024)
					local := make([]time.Duration, 0, b.N)
					for i := 0; i < b.N; i++ {
						for j := range reqs {
							reqs[j] = MultiPutRequest{
								Key: fmt.Sprintf("cmp:%d:%d:%d", w, i, j), Value: val, TTL: -1,
							}
						}
						t0 := time.Now()
						se.MultiPut(reqs)
						local = append(local, time.Since(t0))
					}
					mu.Lock()
					lat = append(lat, local...)
					mu.Unlock()
				}(w)
			}
			wg.Wait()
			b.StopTimer()

			if len(lat) == 0 {
				return
			}
			sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
			p := func(q float64) float64 {
				idx := int(float64(len(lat)-1) * q)
				return float64(lat[idx].Microseconds()) / 1000.0
			}
			b.ReportMetric(p(0.50), "P50-ms/batch")
			b.ReportMetric(p(0.99), "P99-ms/batch")
		})
	}
}
