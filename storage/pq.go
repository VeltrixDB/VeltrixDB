package storage

// pq.go — product quantization (Jégou, Douze, Schmid, TPAMI 2011) for the
// HNSW vector index.
//
// A dim-dimensional vector is cut into m contiguous subvectors; each
// subspace has its own codebook of up to 256 centroids learned with k-means,
// and a vector is stored as m one-byte centroid indices. A 768-dim vector
// with m = 96 takes 96 bytes (float32: 3072, int8: 772).
//
// Scoring is asymmetric (ADC): per query, table[s][c] = dot(q_s, centroid_s_c)
// is computed once (256 × dim multiply-adds), after which a vector's
// approximate similarity is the sum of m table lookups. The engine re-ranks
// the best candidates against the full-precision vectors persisted in the
// VLog (vector_index.go), so PQ error only affects which candidates survive
// the graph walk, not the returned scores.
//
// Subspaces need not divide dim evenly: boundaries spread the remainder over
// the first subspaces.

import (
	"errors"
	"math"
	"math/rand"
	"runtime"
	"sync"
)

const (
	pqKsub          = 256   // centroids per subspace (one byte per code)
	pqIters         = 12    // Lloyd iterations
	pqMaxTrainN     = 10000 // training sample cap
	pqDefaultTrainN = 10000 // vectors a namespace collects before training
	pqMinTrainN     = 256   // below this k-means has fewer points than centroids
)

// pqCodebook is one trained product quantizer.
type pqCodebook struct {
	dim    int
	m      int
	ksub   int         // centroids per subspace (≤ 256; fewer if trained on less data)
	bounds []int       // subspace s spans [bounds[s], bounds[s+1])
	cents  [][]float32 // cents[s] is ksub × sublen(s), row-major
}

// defaultPQSubspaces picks m for dim: 8 dimensions per subspace (the common
// choice for 128–1536-dim embeddings), at least 1.
func defaultPQSubspaces(dim int) int {
	m := dim / 8
	if m < 1 {
		m = 1
	}
	return m
}

func pqBounds(dim, m int) []int {
	b := make([]int, m+1)
	base, rem := dim/m, dim%m
	for s := 0; s < m; s++ {
		w := base
		if s < rem {
			w++
		}
		b[s+1] = b[s] + w
	}
	return b
}

// trainPQ learns an m-subspace codebook from samples (all of length dim).
func trainPQ(samples [][]float32, dim, m int, seed int64) (*pqCodebook, error) {
	if m < 1 || m > dim {
		return nil, errors.New("pq: subspaces must be in [1, dim]")
	}
	if len(samples) < pqMinTrainN {
		return nil, errors.New("pq: not enough training vectors")
	}
	r := rand.New(rand.NewSource(seed))
	if len(samples) > pqMaxTrainN {
		r.Shuffle(len(samples), func(i, j int) { samples[i], samples[j] = samples[j], samples[i] })
		samples = samples[:pqMaxTrainN]
	}
	ksub := pqKsub
	if len(samples) < ksub {
		ksub = len(samples)
	}
	cb := &pqCodebook{dim: dim, m: m, ksub: ksub, bounds: pqBounds(dim, m), cents: make([][]float32, m)}

	// Subspaces are independent: train them in parallel.
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	for s := 0; s < m; s++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(s int, seed int64) {
			defer wg.Done()
			defer func() { <-sem }()
			cb.cents[s] = kmeansSub(samples, cb.bounds[s], cb.bounds[s+1], ksub, rand.New(rand.NewSource(seed)))
		}(s, r.Int63())
	}
	wg.Wait()
	return cb, nil
}

// kmeansSub runs Lloyd's k-means on columns [lo, hi) of samples.
func kmeansSub(samples [][]float32, lo, hi, k int, r *rand.Rand) []float32 {
	w := hi - lo
	n := len(samples)
	cents := make([]float32, k*w)
	for c, i := range r.Perm(n)[:k] { // distinct random samples as seeds
		copy(cents[c*w:(c+1)*w], samples[i][lo:hi])
	}
	assign := make([]int, n)
	sums := make([]float64, k*w)
	counts := make([]int, k)
	for it := 0; it < pqIters; it++ {
		for i, v := range samples {
			assign[i] = nearestCentroid(v[lo:hi], cents, k, w)
		}
		for i := range sums {
			sums[i] = 0
		}
		for i := range counts {
			counts[i] = 0
		}
		for i, v := range samples {
			c := assign[i]
			counts[c]++
			row := sums[c*w : (c+1)*w]
			for j, x := range v[lo:hi] {
				row[j] += float64(x)
			}
		}
		for c := 0; c < k; c++ {
			row := cents[c*w : (c+1)*w]
			if counts[c] == 0 { // empty cluster: re-seed from a random point
				copy(row, samples[r.Intn(n)][lo:hi])
				continue
			}
			inv := 1 / float64(counts[c])
			for j := range row {
				row[j] = float32(sums[c*w+j] * inv)
			}
		}
	}
	return cents
}

// nearestCentroid returns the index of the centroid closest (L2) to v.
func nearestCentroid(v, cents []float32, k, w int) int {
	best, bestD := 0, float32(math.MaxFloat32)
	for c := 0; c < k; c++ {
		row := cents[c*w : (c+1)*w]
		var d float32
		for j, x := range v {
			t := x - row[j]
			d += t * t
		}
		if d < bestD {
			best, bestD = c, d
		}
	}
	return best
}

// encode returns v's m-byte code.
func (cb *pqCodebook) encode(v []float32) []uint8 {
	code := make([]uint8, cb.m)
	for s := 0; s < cb.m; s++ {
		lo, hi := cb.bounds[s], cb.bounds[s+1]
		code[s] = uint8(nearestCentroid(v[lo:hi], cb.cents[s], cb.ksub, hi-lo))
	}
	return code
}

// decode reconstructs the approximate vector a code stands for.
func (cb *pqCodebook) decode(code []uint8) []float32 {
	out := make([]float32, cb.dim)
	for s := 0; s < cb.m; s++ {
		lo, hi := cb.bounds[s], cb.bounds[s+1]
		w := hi - lo
		c := int(code[s])
		copy(out[lo:hi], cb.cents[s][c*w:(c+1)*w])
	}
	return out
}

// table builds the ADC lookup table for query q: m × ksub dot products.
func (cb *pqCodebook) table(q []float32) []float32 {
	t := make([]float32, cb.m*cb.ksub)
	for s := 0; s < cb.m; s++ {
		lo, hi := cb.bounds[s], cb.bounds[s+1]
		w := hi - lo
		qs := q[lo:hi]
		cents := cb.cents[s]
		row := t[s*cb.ksub : (s+1)*cb.ksub]
		for c := 0; c < cb.ksub; c++ {
			row[c] = dot(qs, cents[c*w:(c+1)*w])
		}
	}
	return t
}

// adcScore sums the table entries a code selects.
func (cb *pqCodebook) adcScore(table []float32, code []uint8) float32 {
	var s float32
	k := cb.ksub
	for i, c := range code {
		s += table[i*k+int(c)]
	}
	return s
}

// bytes is the codebook's own RAM.
func (cb *pqCodebook) bytes() int64 {
	var b int64
	for _, c := range cb.cents {
		b += int64(len(c)) * 4
	}
	return b
}
