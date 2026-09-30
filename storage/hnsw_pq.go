package storage

// hnsw_pq.go — product-quantization lifecycle for a VectorIndex.
//
// A PQ namespace needs training data before it can encode anything, so it
// starts out storing float32 vectors like an unquantized one. Once it holds
// pqTrainAt live vectors, a background goroutine samples them, trains a
// codebook (pq.go) and encodes every float32 node. Encoding runs outside the
// index lock; only the final code swap takes the write lock, so searches are
// not stalled for the training's seconds. Nodes inserted afterwards are
// encoded while their insert is planned (under the read lock).
//
// A node therefore holds exactly one of vec (float32), code (int8) or pqc
// (PQ); sim / score / vecOf dispatch on which one is set, so an index can mix
// float32 and PQ nodes during the transition.

import (
	"log"
	"time"
)

// pqTrainThreshold is how many live vectors trigger training.
func (vi *VectorIndex) pqTrainThreshold() int {
	if vi.pqTrainAt >= pqMinTrainN {
		return vi.pqTrainAt
	}
	return pqDefaultTrainN
}

// pqSamplesLocked returns up to pqMaxTrainN live float32 vectors (shared,
// not copied: vectors are immutable). Caller must hold vi.mu.
func (vi *VectorIndex) pqSamplesLocked() [][]float32 {
	var out [][]float32
	for _, n := range vi.nodes {
		if !n.deleted && n.vec != nil {
			out = append(out, n.vec)
			if len(out) == pqMaxTrainN {
				break
			}
		}
	}
	return out
}

// maybeTrainPQLocked starts background training once enough vectors have
// arrived. Caller must hold vi.mu exclusively.
func (vi *VectorIndex) maybeTrainPQLocked() {
	if !vi.pqMode || vi.pq != nil || vi.pqTraining || vi.live < vi.pqTrainThreshold() {
		return
	}
	vi.pqTraining = true
	samples := vi.pqSamplesLocked()
	go vi.trainPQBackground(samples)
}

func (vi *VectorIndex) trainPQBackground(samples [][]float32) {
	t0 := time.Now()
	cb, err := trainPQ(samples, vi.dim, vi.pqM, int64(len(samples)))
	if err != nil {
		vi.mu.Lock()
		vi.pqTraining = false
		vi.mu.Unlock()
		log.Printf("[vector] pq training failed: %v", err)
		return
	}

	// Encode a snapshot of the float32 nodes without holding the lock.
	type job struct {
		n    *hnswNode
		vec  []float32
		code []uint8
	}
	vi.mu.RLock()
	var jobs []job
	for _, n := range vi.nodes {
		if n.vec != nil {
			jobs = append(jobs, job{n: n, vec: n.vec})
		}
	}
	vi.mu.RUnlock()
	for i := range jobs {
		jobs[i].code = cb.encode(jobs[i].vec)
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.pq = cb
	vi.pqTraining = false
	for _, j := range jobs {
		if j.n.vec != nil { // still float32 (nodes replaced by a compaction are dropped anyway)
			j.n.pqc, j.n.vec = j.code, nil
		}
	}
	vi.encodePQLocked() // stragglers inserted during the encode pass
	log.Printf("[vector] pq codebook trained: dim=%d m=%d ksub=%d samples=%d nodes=%d in %s",
		vi.dim, cb.m, cb.ksub, len(samples), len(jobs), time.Since(t0).Round(time.Millisecond))
}

// encodePQLocked PQ-encodes every remaining float32 node. Caller must hold
// vi.mu exclusively (or own a private index).
func (vi *VectorIndex) encodePQLocked() {
	if vi.pq == nil {
		return
	}
	for _, n := range vi.nodes {
		if n.vec != nil {
			n.pqc, n.vec = vi.pq.encode(n.vec), nil
		}
	}
}

// trainPQPrivate trains and encodes synchronously; for an index no other
// goroutine can see yet (namespace re-encode).
func (vi *VectorIndex) trainPQPrivate() {
	if !vi.pqMode || vi.pq != nil || vi.live < vi.pqTrainThreshold() {
		return
	}
	cb, err := trainPQ(vi.pqSamplesLocked(), vi.dim, vi.pqM, int64(vi.live))
	if err != nil {
		log.Printf("[vector] pq training failed: %v", err)
		return
	}
	vi.pq = cb
	vi.encodePQLocked()
}
