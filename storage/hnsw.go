package storage

// hnsw.go — Hierarchical Navigable Small World graph (Malkov & Yashunin,
// arXiv:1603.09320) for approximate nearest-neighbour vector search.
//
// Replaces the brute-force scan that previously backed SearchVector: query
// cost drops from O(N×D) to roughly O(log N × M × D) at >0.95 recall for the
// default parameters. All vectors are L2-normalized before insertion, so
// similarity is the plain dot product (cosine).
//
// Design notes:
//   - Level assignment is DETERMINISTIC per id (hash-derived, not RNG), so
//     every replica that inserts the same ids builds a structurally similar
//     graph regardless of insert order or process restarts.
//   - Updates replace the vector in place and keep existing edges; the graph
//     re-optimises on subsequent inserts. Restart rebuild produces a fresh
//     optimal graph (RebuildVectorIndexes).
//   - Deletes tombstone the node: it stays as a routing waypoint but is
//     filtered from results. Live count is tracked separately.
//   - Concurrency: one RWMutex per index — writers exclusive, searches shared.

import (
	"container/heap"
	"hash/fnv"
	"math"
)

const (
	hnswM              = 16  // max out-edges per node on layers ≥ 1
	hnswMmax0          = 32  // max out-edges on layer 0
	hnswEfConstruction = 200 // beam width while inserting
	hnswEfSearch       = 64  // beam width while querying (raised to k when k larger)
)

// hnswInvLogM = 1/ln(M) — the level multiplier from the paper.
var hnswInvLogM = 1.0 / math.Log(float64(hnswM))

type hnswNode struct {
	id      string
	vec     []float32
	level   int
	deleted bool
	// neighbors[l] lists node indices adjacent at layer l (0..level).
	neighbors [][]int32
}

// hnswLevelForID derives the node's top layer deterministically from its id:
// a 64-bit hash → uniform (0,1) → exponential level distribution.
func hnswLevelForID(id string) int {
	h := fnv.New64a()
	h.Write([]byte(id))
	x := h.Sum64()
	// fmix64 finalizer for avalanche, then map the top 53 bits to (0,1).
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	u := float64(x>>11)/float64(1<<53) + 1e-18 // avoid ln(0)
	return int(-math.Log(u) * hnswInvLogM)
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// simHeap is a heap of (similarity, node index) pairs. minFirst selects
// between a min-heap (results set — evict worst) and a max-heap (candidate
// frontier — expand best).
type simItem struct {
	sim float32
	idx int32
}
type simHeap struct {
	items    []simItem
	minFirst bool
}

func (h *simHeap) Len() int { return len(h.items) }
func (h *simHeap) Less(i, j int) bool {
	if h.minFirst {
		return h.items[i].sim < h.items[j].sim
	}
	return h.items[i].sim > h.items[j].sim
}
func (h *simHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *simHeap) Push(x interface{}) { h.items = append(h.items, x.(simItem)) }
func (h *simHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}

// searchLayer runs a beam search of width ef at the given layer, starting from
// entry points eps. Returns up to ef (similarity, index) pairs, best-first
// unspecified (callers sort/select). Caller must hold vi.mu (read or write).
func (vi *VectorIndex) searchLayer(q []float32, eps []int32, ef, layer int) []simItem {
	visited := make(map[int32]struct{}, ef*4)
	candidates := &simHeap{minFirst: false} // frontier: best-first expansion
	results := &simHeap{minFirst: true}     // keep-best-ef: worst on top

	for _, ep := range eps {
		if _, ok := visited[ep]; ok {
			continue
		}
		visited[ep] = struct{}{}
		s := dot(q, vi.nodes[ep].vec)
		heap.Push(candidates, simItem{s, ep})
		heap.Push(results, simItem{s, ep})
	}

	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(simItem)
		if results.Len() >= ef && c.sim < results.items[0].sim {
			break // best remaining candidate is worse than the worst kept result
		}
		node := vi.nodes[c.idx]
		if layer < len(node.neighbors) {
			for _, nb := range node.neighbors[layer] {
				if _, ok := visited[nb]; ok {
					continue
				}
				visited[nb] = struct{}{}
				s := dot(q, vi.nodes[nb].vec)
				if results.Len() < ef || s > results.items[0].sim {
					heap.Push(candidates, simItem{s, nb})
					heap.Push(results, simItem{s, nb})
					if results.Len() > ef {
						heap.Pop(results)
					}
				}
			}
		}
	}
	return results.items
}

// greedyDescend walks from ep down through layers (top..targetLayer+1) taking
// the locally best neighbor at each step. Caller must hold vi.mu.
func (vi *VectorIndex) greedyDescend(q []float32, ep int32, fromLayer, toLayer int) int32 {
	cur := ep
	curSim := dot(q, vi.nodes[cur].vec)
	for l := fromLayer; l > toLayer; l-- {
		for improved := true; improved; {
			improved = false
			node := vi.nodes[cur]
			if l >= len(node.neighbors) {
				break
			}
			for _, nb := range node.neighbors[l] {
				if s := dot(q, vi.nodes[nb].vec); s > curSim {
					curSim, cur = s, nb
					improved = true
				}
			}
		}
	}
	return cur
}

// selectTopM returns the indices of the up-to-m most similar items.
func selectTopM(items []simItem, m int) []int32 {
	// Partial selection via a min-heap of size m.
	h := &simHeap{minFirst: true}
	for _, it := range items {
		if h.Len() < m {
			heap.Push(h, it)
		} else if it.sim > h.items[0].sim {
			heap.Pop(h)
			heap.Push(h, it)
		}
	}
	out := make([]int32, h.Len())
	for i := range out {
		out[i] = h.items[i].idx
	}
	return out
}

// insertHNSW adds (or updates) a vector. Caller must hold vi.mu exclusively.
func (vi *VectorIndex) insertHNSW(id string, vec []float32) {
	if i, exists := vi.byID[id]; exists {
		n := vi.nodes[i]
		n.vec = vec
		if n.deleted {
			n.deleted = false
			vi.live++
		}
		return
	}

	level := hnswLevelForID(id)
	idx := int32(len(vi.nodes))
	node := &hnswNode{
		id:        id,
		vec:       vec,
		level:     level,
		neighbors: make([][]int32, level+1),
	}
	vi.nodes = append(vi.nodes, node)
	vi.byID[id] = idx
	vi.live++

	if idx == 0 {
		vi.entry = 0
		vi.maxLevel = level
		return
	}

	ep := vi.entry
	// Phase 1: greedy descend from the top of the graph to level+1.
	if vi.maxLevel > level {
		ep = vi.greedyDescend(vec, ep, vi.maxLevel, level)
	}

	// Phase 2: beam-connect on each layer from min(level, maxLevel) down to 0.
	startLayer := level
	if vi.maxLevel < startLayer {
		startLayer = vi.maxLevel
	}
	eps := []int32{ep}
	for l := startLayer; l >= 0; l-- {
		found := vi.searchLayer(vec, eps, hnswEfConstruction, l)
		mmax := hnswM
		if l == 0 {
			mmax = hnswMmax0
		}
		selected := selectTopM(found, hnswM)
		node.neighbors[l] = append([]int32(nil), selected...)

		// Bidirectional links with degree pruning on the neighbor side.
		for _, nb := range selected {
			nbNode := vi.nodes[nb]
			if l >= len(nbNode.neighbors) {
				continue
			}
			nbNode.neighbors[l] = append(nbNode.neighbors[l], idx)
			if len(nbNode.neighbors[l]) > mmax {
				// Keep the mmax most similar to the NEIGHBOR itself.
				items := make([]simItem, len(nbNode.neighbors[l]))
				for i, x := range nbNode.neighbors[l] {
					items[i] = simItem{dot(nbNode.vec, vi.nodes[x].vec), x}
				}
				nbNode.neighbors[l] = selectTopM(items, mmax)
			}
		}

		// Next layer starts from everything we found here.
		eps = eps[:0]
		for _, it := range found {
			eps = append(eps, it.idx)
		}
	}

	if level > vi.maxLevel {
		vi.maxLevel = level
		vi.entry = idx
	}
}

// searchHNSW returns the top-k live ids by cosine similarity.
// Caller must hold vi.mu (read suffices).
func (vi *VectorIndex) searchHNSW(q []float32, k int) []VectorMatch {
	if len(vi.nodes) == 0 || vi.live == 0 {
		return nil
	}
	ef := hnswEfSearch
	if k > ef {
		ef = k
	}
	// Over-fetch when tombstones exist so filtering can still fill k.
	if vi.live < len(vi.nodes) {
		ef += len(vi.nodes) - vi.live
	}

	ep := vi.greedyDescend(q, vi.entry, vi.maxLevel, 0)
	found := vi.searchLayer(q, []int32{ep}, ef, 0)

	items := make([]simItem, 0, len(found))
	for _, it := range found {
		if !vi.nodes[it.idx].deleted {
			items = append(items, it)
		}
	}
	// Sort best-first via heap drain.
	h := &simHeap{items: items, minFirst: false}
	heap.Init(h)
	n := k
	if k <= 0 || k > h.Len() {
		n = h.Len()
	}
	out := make([]VectorMatch, 0, n)
	for len(out) < n && h.Len() > 0 {
		it := heap.Pop(h).(simItem)
		out = append(out, VectorMatch{ID: vi.nodes[it.idx].id, Score: it.sim})
	}
	return out
}

// removeHNSW tombstones id. Caller must hold vi.mu exclusively.
func (vi *VectorIndex) removeHNSW(id string) {
	if i, ok := vi.byID[id]; ok {
		if !vi.nodes[i].deleted {
			vi.nodes[i].deleted = true
			vi.live--
		}
		delete(vi.byID, id)
	}
}

// hnswStatsBytes is a rough memory estimate for the admin API (vectors +
// adjacency), avoiding a full graph walk.
func (vi *VectorIndex) hnswStatsBytes() int64 {
	var b int64
	for _, n := range vi.nodes {
		b += int64(len(n.vec)) * 4
		for _, adj := range n.neighbors {
			b += int64(len(adj)) * 4
		}
	}
	return b
}

