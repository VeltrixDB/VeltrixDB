// Package annds loads ANN benchmark datasets and scores search results.
//
// Datasets are fvecs files (per vector: int32 dim, then dim float32,
// little-endian), as written by scripts/ann-dataset.py from the
// ann-benchmarks HDF5 files. Ground truth is computed here, exactly, by
// cosine similarity: ann-benchmarks' own neighbours cover the full base set
// and are wrong for the subsets these tools load.
package annds

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
)

// ReadFvecs reads up to max vectors (max ≤ 0 = all) from path.
func ReadFvecs(path string, max int) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var out [][]float32
	var hdr [4]byte
	for max <= 0 || len(out) < max {
		if _, err := io.ReadFull(r, hdr[:]); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("%s: vector %d header: %w", path, len(out), err)
		}
		d := int(int32(binary.LittleEndian.Uint32(hdr[:])))
		if d <= 0 || d > 1<<16 {
			return nil, fmt.Errorf("%s: vector %d: bad dim %d", path, len(out), d)
		}
		buf := make([]byte, 4*d)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("%s: vector %d body: %w", path, len(out), err)
		}
		v := make([]float32, d)
		for i := range v {
			v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
		}
		out = append(out, v)
	}
	if len(out) > 0 {
		for i, v := range out {
			if len(v) != len(out[0]) {
				return nil, fmt.Errorf("%s: vector %d has dim %d, first has %d", path, i, len(v), len(out[0]))
			}
		}
	}
	return out, nil
}

// Normalize returns a unit-length copy of v (zero vectors stay zero).
func Normalize(v []float32) []float32 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	out := make([]float32, len(v))
	if s == 0 {
		return out
	}
	inv := float32(1 / math.Sqrt(s))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

// GroundTruth returns, for each query, the indices of its k most
// cosine-similar base vectors, best first. Brute force, parallel over
// queries.
func GroundTruth(base, queries [][]float32, k int) [][]int {
	nb := make([][]float32, len(base))
	for i, v := range base {
		nb[i] = Normalize(v)
	}
	out := make([][]int, len(queries))
	var wg sync.WaitGroup
	work := make(chan int)
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			type hit struct {
				i int
				s float32
			}
			for qi := range work {
				q := Normalize(queries[qi])
				best := make([]hit, 0, k+1)
				for i, v := range nb {
					var s float32
					for j := range q {
						s += q[j] * v[j]
					}
					if len(best) < k || s > best[len(best)-1].s {
						best = append(best, hit{i, s})
						sort.Slice(best, func(a, b int) bool { return best[a].s > best[b].s })
						if len(best) > k {
							best = best[:k]
						}
					}
				}
				ids := make([]int, len(best))
				for j, h := range best {
					ids[j] = h.i
				}
				out[qi] = ids
			}
		}()
	}
	for qi := range queries {
		work <- qi
	}
	close(work)
	wg.Wait()
	return out
}

// Recall is |got ∩ want[:k]| / k averaged over queries; got holds base
// indices per query.
func Recall(got, want [][]int, k int) float64 {
	hits, total := 0, 0
	for qi := range want {
		in := map[int]bool{}
		for _, i := range got[qi] {
			in[i] = true
		}
		n := k
		if n > len(want[qi]) {
			n = len(want[qi])
		}
		for _, i := range want[qi][:n] {
			total++
			if in[i] {
				hits++
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// Percentile returns the p-th percentile (0–100) of ds (sorted in place).
func Percentile(ds []float64, p float64) float64 {
	if len(ds) == 0 {
		return 0
	}
	sort.Float64s(ds)
	i := int(p / 100 * float64(len(ds)-1))
	return ds[i]
}
