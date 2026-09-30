package storage

// hnsw.go — Hierarchical Navigable Small World graph (Malkov & Yashunin,
// arXiv:1603.09320) for approximate nearest-neighbour vector search.
//
// Query cost is roughly O(log N × M × D) at >0.95 recall for the default
// parameters. All vectors are L2-normalized before insertion, so similarity
// is the plain dot product (cosine).
//
// Design notes:
//   - Level assignment is DETERMINISTIC per id (hash-derived, not RNG), so
//     every replica that inserts the same ids builds a structurally similar
//     graph regardless of insert order or process restarts.
//   - Neighbours are chosen with the paper's diversity heuristic (Algorithm
//     4, keepPrunedConnections): a candidate closer to an already-selected
//     neighbour than to the base node is skipped first, so clustered data
//     keeps long-range edges. Plain top-M selection loses recall there.
//   - An update tombstones the old node and inserts a fresh one, so the
//     new position gets its own edges. Rewriting the vector in place would
//     leave edges that point at the old neighbourhood.
//   - Deletes tombstone the node: it stays as a routing waypoint but never
//     enters a result set. Search does NOT widen ef by the tombstone count
//     (that degrades to a full scan as deletes pile up); tombstones are
//     traversed but not counted, and compaction (below) removes them.
//   - Compaction: once tombstones exceed hnswCompactRatio of all nodes (and
//     at least hnswCompactMinDead), a background goroutine rebuilds a fresh
//     graph from the live nodes, replays the writes that landed meanwhile
//     (vi.pending) and swaps it in under the write lock. vi.epoch counts the
//     swaps; node indices are only stable within one epoch.
//   - Quantization (VectorIndex.quant): each node keeps int8 codes plus one
//     float32 scale instead of float32 components — dim+4 bytes instead of
//     4×dim. The graph is built and walked on these approximate similarities;
//     the engine re-ranks the survivors against the full float32 vectors it
//     persists in the VLog (vector_index.go), so RAM holds only the codes.
//   - Concurrency: one RWMutex per index. An insert runs its expensive
//     candidate search under the READ lock (planInsert) and takes the write
//     lock only to link the node (commitInsert), so searches and other
//     inserts' planning proceed in parallel. A plan made in an older epoch
//     is discarded and redone.

import (
	"hash/fnv"
	"math"
	"sort"
	"sync"
)

const (
	hnswM              = 16  // max out-edges per node on layers ≥ 1
	hnswMmax0          = 32  // max out-edges on layer 0
	hnswEfConstruction = 200 // beam width while inserting
	hnswEfSearch       = 64  // beam width while querying (raised to k when k larger)

	// Compaction trigger: tombstones must be at least this many AND more
	// than this fraction of all nodes.
	hnswCompactMinDead = 1024
	hnswCompactRatio   = 0.3

	// A filtered search stops after visiting this many nodes per unit of ef
	// (plus hnswFilterVisitBase). Without a cap, a filter that matches
	// almost nothing walks the whole graph, evaluating the filter per node.
	hnswFilterVisitPerEf = 20
	hnswFilterVisitBase  = 10000
)

// hnswInvLogM = 1/ln(M) — the level multiplier from the paper.
var hnswInvLogM = 1.0 / math.Log(float64(hnswM))

type hnswNode struct {
	id  string
	vec []float32 // never mutated after insert (updates add a new node); nil when quantized
	// Quantized form (VectorIndex.quant): vec[i] ≈ code[i] × scale.
	code  []int8
	scale float32
	// Product-quantized form (VectorIndex.pq, pq.go): one centroid index per
	// subspace. Set once the namespace's codebook is trained.
	pqc     []uint8
	level   int
	deleted bool
	// neighbors[l] lists node indices adjacent at layer l (0..level).
	neighbors [][]int32
}

// hnswOp is a write recorded while a compaction is running; it is replayed
// onto the fresh graph before the swap.
type hnswOp struct {
	id  string
	vec []float32 // nil = delete
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

// dot is unrolled by 4 with independent accumulators, which breaks the
// add-dependency chain and lets the compiler drop bounds checks (b is
// re-sliced to len(a)). About 2–3× the plain loop on amd64/arm64.
func dot(a, b []float32) float32 {
	n := len(a)
	b = b[:n]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= n; i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	for ; i < n; i++ {
		s0 += a[i] * b[i]
	}
	return (s0 + s1) + (s2 + s3)
}

// dotF32I8 is dot with an int8 right-hand side, unrolled like dot.
func dotF32I8(a []float32, c []int8) float32 {
	n := len(a)
	c = c[:n]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= n; i += 4 {
		s0 += a[i] * float32(c[i])
		s1 += a[i+1] * float32(c[i+1])
		s2 += a[i+2] * float32(c[i+2])
		s3 += a[i+3] * float32(c[i+3])
	}
	for ; i < n; i++ {
		s0 += a[i] * float32(c[i])
	}
	return (s0 + s1) + (s2 + s3)
}

// quantizeInt8 maps v to int8 codes with one scale chosen so the largest
// |component| becomes ±127 (symmetric scalar quantization).
func quantizeInt8(v []float32) ([]int8, float32) {
	var maxAbs float32
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	code := make([]int8, len(v))
	if maxAbs == 0 {
		return code, 0
	}
	scale := maxAbs / 127
	for i, x := range v {
		code[i] = int8(math.Round(float64(x / scale)))
	}
	return code, scale
}

// dequantizeInt8 is the inverse of quantizeInt8 (up to rounding).
func dequantizeInt8(code []int8, scale float32) []float32 {
	out := make([]float32, len(code))
	for i, c := range code {
		out[i] = float32(c) * scale
	}
	return out
}

// sim is the similarity of q to node idx: exact for float32 nodes,
// approximate for quantized ones. PQ nodes are decoded, so this is for
// node-to-node comparisons during construction; query paths use a scorer.
// Caller must hold vi.mu.
func (vi *VectorIndex) sim(q []float32, idx int32) float32 {
	n := vi.nodes[idx]
	switch {
	case n.code != nil:
		return dotF32I8(q, n.code) * n.scale
	case n.pqc != nil:
		return dot(q, vi.pq.decode(n.pqc))
	}
	return dot(q, n.vec)
}

// vecOf returns node idx's vector, decoding a quantized node (allocates).
// Caller must hold vi.mu.
func (vi *VectorIndex) vecOf(idx int32) []float32 {
	n := vi.nodes[idx]
	switch {
	case n.code != nil:
		return dequantizeInt8(n.code, n.scale)
	case n.pqc != nil:
		return vi.pq.decode(n.pqc)
	}
	return n.vec
}

// queryScorer scores one query against many nodes. For a PQ index it holds
// the query's ADC table, built once instead of per node.
type queryScorer struct {
	q     []float32
	table []float32 // nil unless vi.pq != nil
}

// scorer prepares q for a search. Caller must hold vi.mu.
func (vi *VectorIndex) scorer(q []float32) *queryScorer {
	sc := &queryScorer{q: q}
	if vi.pq != nil {
		sc.table = vi.pq.table(q)
	}
	return sc
}

// score is sim through a scorer. Caller must hold vi.mu.
func (vi *VectorIndex) score(sc *queryScorer, idx int32) float32 {
	n := vi.nodes[idx]
	if n.pqc != nil {
		return vi.pq.adcScore(sc.table, n.pqc)
	}
	return vi.sim(sc.q, idx)
}

// simItem is one (similarity, node index) pair.
type simItem struct {
	sim float32
	idx int32
}

// maxSimHeap keeps the most similar item on top (the candidate frontier).
type maxSimHeap []simItem

func (h *maxSimHeap) push(it simItem) {
	*h = append(*h, it)
	s := *h
	for i := len(s) - 1; i > 0; {
		p := (i - 1) / 2
		if s[p].sim >= s[i].sim {
			break
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func (h *maxSimHeap) pop() simItem {
	s := *h
	top := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(s) && s[l].sim > s[m].sim {
			m = l
		}
		if r < len(s) && s[r].sim > s[m].sim {
			m = r
		}
		if m == i {
			break
		}
		s[i], s[m] = s[m], s[i]
		i = m
	}
	*h = s
	return top
}

// minSimHeap keeps the least similar item on top (a bounded result set).
type minSimHeap []simItem

func (h *minSimHeap) push(it simItem) {
	*h = append(*h, it)
	s := *h
	for i := len(s) - 1; i > 0; {
		p := (i - 1) / 2
		if s[p].sim <= s[i].sim {
			break
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func (h *minSimHeap) pop() simItem {
	s := *h
	top := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(s) && s[l].sim < s[m].sim {
			m = l
		}
		if r < len(s) && s[r].sim < s[m].sim {
			m = r
		}
		if m == i {
			break
		}
		s[i], s[m] = s[m], s[i]
		i = m
	}
	*h = s
	return top
}

// visitedSet is a generation-stamped mark array: a node is visited in the
// current search iff marks[i] == gen. Bumping gen clears the set in O(1),
// so a pooled set costs no per-search allocation (the old map[int32] did).
type visitedSet struct {
	marks []uint32
	gen   uint32
}

var visitedPool = sync.Pool{New: func() interface{} { return &visitedSet{} }}

func getVisited(n int) *visitedSet {
	v := visitedPool.Get().(*visitedSet)
	if cap(v.marks) < n {
		v.marks = make([]uint32, n+n/4)
		v.gen = 0
	}
	v.marks = v.marks[:cap(v.marks)]
	v.gen++
	if v.gen == 0 { // wrapped: stale marks could collide with the new gen
		for i := range v.marks {
			v.marks[i] = 0
		}
		v.gen = 1
	}
	return v
}

// visit marks i and reports whether it was unvisited.
func (v *visitedSet) visit(i int32) bool {
	if v.marks[i] == v.gen {
		return false
	}
	v.marks[i] = v.gen
	return true
}

// searchLayer runs a beam search of width ef at the given layer, starting from
// entry points eps. Every reachable node is traversed, but only nodes for
// which accept(idx) is true (nil = every node) enter the result set.
// maxVisit > 0 stops the walk after that many visited nodes. Returns up to ef
// accepted (similarity, index) pairs in unspecified order. Caller must hold
// vi.mu (read or write).
func (vi *VectorIndex) searchLayer(sc *queryScorer, eps []int32, ef, layer int, accept func(int32) bool, maxVisit int) []simItem {
	visited := getVisited(len(vi.nodes))
	defer visitedPool.Put(visited)

	candidates := make(maxSimHeap, 0, ef*2) // frontier: best-first expansion
	results := make(minSimHeap, 0, ef+1)    // keep-best-ef: worst on top
	offer := func(s float32, idx int32) {
		if accept == nil || accept(idx) {
			results.push(simItem{s, idx})
			if len(results) > ef {
				results.pop()
			}
		}
	}

	nVisited := 0
	for _, ep := range eps {
		if !visited.visit(ep) {
			continue
		}
		nVisited++
		s := vi.score(sc, ep)
		candidates.push(simItem{s, ep})
		offer(s, ep)
	}

	for len(candidates) > 0 {
		c := candidates.pop()
		if len(results) >= ef && c.sim < results[0].sim {
			break // best remaining candidate is worse than the worst kept result
		}
		for _, nb := range vi.nbrs(c.idx, layer) {
			if !visited.visit(nb) {
				continue
			}
			nVisited++
			s := vi.score(sc, nb)
			if len(results) < ef || s > results[0].sim {
				candidates.push(simItem{s, nb})
				offer(s, nb)
			}
		}
		if maxVisit > 0 && nVisited >= maxVisit {
			break
		}
	}
	return results
}

// greedyDescend walks from ep down through layers (top..targetLayer+1) taking
// the locally best neighbor at each step. Upper layers are routing-only, so
// tombstoned nodes are valid stepping stones. Caller must hold vi.mu.
func (vi *VectorIndex) greedyDescend(sc *queryScorer, ep int32, fromLayer, toLayer int) int32 {
	cur := ep
	curSim := vi.score(sc, cur)
	for l := fromLayer; l > toLayer; l-- {
		for improved := true; improved; {
			improved = false
			for _, nb := range vi.nbrs(cur, l) {
				if s := vi.score(sc, nb); s > curSim {
					curSim, cur = s, nb
					improved = true
				}
			}
		}
	}
	return cur
}

// selectNeighbors picks up to m of cands (similarities are to the base node)
// with the paper's heuristic: walking candidates best-first, one is kept only
// if it is more similar to the base than to every neighbour already kept.
// Discarded candidates then back-fill any remaining slots
// (keepPrunedConnections), so a node never ends up under-connected.
// Caller must hold vi.mu.
func (vi *VectorIndex) selectNeighbors(cands []simItem, m int) []int32 {
	if len(cands) <= m {
		out := make([]int32, len(cands))
		for i, c := range cands {
			out[i] = c.idx
		}
		return out
	}
	sorted := append([]simItem(nil), cands...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].sim > sorted[j].sim })

	selected := make([]int32, 0, m)
	selVecs := make([][]float32, 0, m) // decoded once each
	var pruned []int32
	for _, c := range sorted {
		if len(selected) >= m {
			break
		}
		cv := vi.vecOf(c.idx)
		diverse := true
		for _, sv := range selVecs {
			if dot(cv, sv) > c.sim {
				diverse = false
				break
			}
		}
		if diverse {
			selected = append(selected, c.idx)
			selVecs = append(selVecs, cv)
		} else {
			pruned = append(pruned, c.idx)
		}
	}
	for _, p := range pruned {
		if len(selected) >= m {
			break
		}
		selected = append(selected, p)
	}
	return selected
}

// isLive reports whether node idx is not tombstoned (the default layer-0
// acceptance rule). Caller must hold vi.mu.
func (vi *VectorIndex) isLive(idx int32) bool { return !vi.nodes[idx].deleted }

// hnswPlan is the read-only half of an insert: the neighbours chosen per
// layer, valid only for the epoch (and non-empty graph) it was computed in.
type hnswPlan struct {
	epoch uint64
	empty bool      // graph had no nodes when planned
	links [][]int32 // links[l] for l in 0..min(level, maxLevel)
	// PQ code computed while planning (outside the write lock), valid for
	// codebook pqcb only.
	pqc  []uint8
	pqcb *pqCodebook
}

// planInsert searches for the new node's neighbours without mutating the
// graph. Caller must hold vi.mu (read suffices).
func (vi *VectorIndex) planInsert(vec []float32, level int) hnswPlan {
	p := hnswPlan{epoch: vi.epoch}
	if vi.pq != nil {
		p.pqcb, p.pqc = vi.pq, vi.pq.encode(vec)
	}
	if len(vi.nodes) == 0 {
		p.empty = true
		return p
	}
	sc := vi.scorer(vec)
	ep := vi.entry
	// Phase 1: greedy descend from the top of the graph to level+1.
	if vi.maxLevel > level {
		ep = vi.greedyDescend(sc, ep, vi.maxLevel, level)
	}
	// Phase 2: beam search on each layer from min(level, maxLevel) down to 0.
	startLayer := level
	if vi.maxLevel < startLayer {
		startLayer = vi.maxLevel
	}
	p.links = make([][]int32, startLayer+1)
	eps := []int32{ep}
	for l := startLayer; l >= 0; l-- {
		var found []simItem
		if l == 0 {
			// Layer 0 carries the result edges: don't spend them on
			// tombstones, unless nothing live is reachable at all.
			found = vi.searchLayer(sc, eps, hnswEfConstruction, 0, vi.isLive, 0)
			if len(found) == 0 {
				found = vi.searchLayer(sc, eps, hnswEfConstruction, 0, nil, 0)
			}
		} else {
			found = vi.searchLayer(sc, eps, hnswEfConstruction, l, nil, 0)
		}
		p.links[l] = vi.selectNeighbors(found, hnswM)
		if len(found) > 0 {
			eps = eps[:0]
			for _, it := range found {
				eps = append(eps, it.idx)
			}
		}
	}
	return p
}

// commitInsert links a planned node into the graph. If id already exists its
// old node is tombstoned first (an update). Caller must hold vi.mu
// exclusively and must have checked p.epoch == vi.epoch.
func (vi *VectorIndex) commitInsert(id string, vec []float32, level int, p hnswPlan) {
	if vi.compacting {
		vi.pending = append(vi.pending, hnswOp{id: id, vec: vec})
	}
	if old, exists := vi.byID[id]; exists && !vi.nodes[old].deleted {
		vi.nodes[old].deleted = true
		vi.live--
	}

	idx := int32(len(vi.nodes))
	node := &hnswNode{
		id:        id,
		level:     level,
		neighbors: make([][]int32, level+1),
	}
	switch {
	case vi.quant:
		node.code, node.scale = quantizeInt8(vec)
	case vi.pq != nil:
		if p.pqcb == vi.pq {
			node.pqc = p.pqc
		} else {
			node.pqc = vi.pq.encode(vec) // codebook trained after planning
		}
	default:
		node.vec = vec // also a PQ namespace before its codebook is trained
	}
	vi.nodes = append(vi.nodes, node)
	vi.byID[id] = idx
	vi.live++
	if vi.adj0 != nil {
		vi.adj0.ensure(int(idx) + 1)
	}

	if idx == 0 {
		vi.entry = 0
		vi.maxLevel = level
		return
	}

	for l, selected := range p.links {
		if l > level {
			break
		}
		vi.setNbrs(idx, l, selected)
		mmax := hnswM
		if l == 0 {
			mmax = hnswMmax0
		}
		// Bidirectional links with heuristic pruning on the neighbour side.
		for _, nb := range selected {
			if l > vi.nodes[nb].level {
				continue
			}
			cur := vi.nbrs(nb, l)
			list := make([]int32, len(cur), len(cur)+1)
			copy(list, cur) // cur may alias the disk graph's mapping
			list = append(list, idx)
			if len(list) > mmax {
				base := vi.vecOf(nb)
				items := make([]simItem, len(list))
				for i, x := range list {
					items[i] = simItem{vi.sim(base, x), x}
				}
				list = vi.selectNeighbors(items, mmax)
			}
			vi.setNbrs(nb, l, list)
		}
	}

	if level > vi.maxLevel {
		vi.maxLevel = level
		vi.entry = idx
	}
}

// insertHNSW adds (or updates) a vector in one step. Caller must hold vi.mu
// exclusively. Used for private graphs (compaction) and tests; concurrent
// callers use insert.
func (vi *VectorIndex) insertHNSW(id string, vec []float32) {
	level := hnswLevelForID(id)
	vi.commitInsert(id, vec, level, vi.planInsert(vec, level))
}

// insert adds (or updates) a vector, planning under the read lock and
// linking under the write lock. Caller must NOT hold vi.mu.
func (vi *VectorIndex) insert(id string, vec []float32) {
	level := hnswLevelForID(id)
	for {
		vi.mu.RLock()
		p := vi.planInsert(vec, level)
		vi.mu.RUnlock()

		vi.mu.Lock()
		// Stale plan: a compaction renumbered the nodes, or the graph was
		// empty when planned and another insert has since seeded it.
		if p.epoch != vi.epoch || (p.empty && len(vi.nodes) > 0) {
			vi.mu.Unlock()
			continue
		}
		vi.commitInsert(id, vec, level, p)
		vi.maybeCompactLocked()
		vi.maybeTrainPQLocked()
		vi.mu.Unlock()
		return
	}
}

// remove tombstones id. Caller must NOT hold vi.mu.
func (vi *VectorIndex) remove(id string) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.removeHNSW(id)
	vi.maybeCompactLocked()
}

// removeHNSW tombstones id. Caller must hold vi.mu exclusively.
func (vi *VectorIndex) removeHNSW(id string) {
	if vi.compacting {
		vi.pending = append(vi.pending, hnswOp{id: id})
	}
	if i, ok := vi.byID[id]; ok {
		if !vi.nodes[i].deleted {
			vi.nodes[i].deleted = true
			vi.live--
		}
		delete(vi.byID, id)
	}
}

// maybeCompactLocked starts a background compaction when tombstones cross
// the threshold. Caller must hold vi.mu exclusively.
func (vi *VectorIndex) maybeCompactLocked() {
	dead := len(vi.nodes) - vi.live
	if vi.compacting || dead < hnswCompactMinDead || float64(dead) <= hnswCompactRatio*float64(len(vi.nodes)) {
		return
	}
	vi.compacting = true
	snap := vi.liveSnapshotLocked()
	go vi.compact(snap)
}

// liveSnapshotLocked returns the live (id, vec) pairs. Float32 vectors are
// shared, not copied: they are immutable once inserted. Quantized nodes are
// decoded; re-quantizing a decoded vector reproduces its codes. Caller must
// hold vi.mu.
func (vi *VectorIndex) liveSnapshotLocked() []hnswOp {
	snap := make([]hnswOp, 0, vi.live)
	for i, n := range vi.nodes {
		if !n.deleted {
			snap = append(snap, hnswOp{id: n.id, vec: vi.vecOf(int32(i))})
		}
	}
	return snap
}

// compact builds a fresh graph from snap without holding vi.mu, then replays
// the writes recorded meanwhile and swaps the fresh graph in.
func (vi *VectorIndex) compact(snap []hnswOp) {
	fresh := vi.emptyLike(len(snap))
	for _, op := range snap {
		fresh.insertHNSW(op.id, op.vec)
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()
	for _, op := range vi.pending {
		if op.vec == nil {
			fresh.removeHNSW(op.id)
		} else {
			fresh.insertHNSW(op.id, op.vec)
		}
	}
	oldAdj := vi.adj0
	vi.nodes, vi.byID = fresh.nodes, fresh.byID
	vi.entry, vi.maxLevel, vi.live = fresh.entry, fresh.maxLevel, fresh.live
	vi.adj0, vi.adjDir = fresh.adj0, fresh.adjDir
	if oldAdj != nil {
		oldAdj.close() // write lock held: no search can still be reading it
	}
	vi.pending = nil
	vi.compacting = false
	vi.epoch++
	vi.compactions++
	// A codebook trained while the fresh graph was being built left its
	// nodes as float32.
	vi.encodePQLocked()
}

// emptyLike returns an empty index with vi's settings and codebook (and its
// own disk-graph file when vi has one).
func (vi *VectorIndex) emptyLike(capacity int) *VectorIndex {
	fresh := &VectorIndex{
		dim: vi.dim, quant: vi.quant,
		pqMode: vi.pqMode, pqM: vi.pqM, pqTrainAt: vi.pqTrainAt, pq: vi.pq,
		adjDir: vi.adjDir,
		byID:   make(map[string]int32, capacity),
	}
	if vi.adjDir != "" {
		if a, err := newMappedAdj(vi.adjDir); err == nil {
			fresh.adj0 = a
		} else {
			fresh.adjDir = ""
		}
	}
	return fresh
}

// retire releases an index that has been replaced in the registry. It waits
// for in-flight searches (the write lock), then unmaps the disk graph and
// empties the index, so a search that picked it up just before the swap
// finds no nodes rather than a closed mapping.
func (vi *VectorIndex) retire() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	if vi.adj0 != nil {
		vi.adj0.close()
		vi.adj0 = nil
	}
	vi.nodes, vi.byID, vi.live = nil, map[string]int32{}, 0
	vi.epoch++
}

// searchHNSW returns the top-k live ids by cosine similarity.
// Caller must hold vi.mu (read suffices).
func (vi *VectorIndex) searchHNSW(q []float32, k int) []VectorMatch {
	return vi.searchFiltered(q, k, 0, nil)
}

// searchFiltered returns the top-k live ids by cosine similarity among those
// for which accept(id) is true (nil = all). ef ≤ 0 selects the default beam
// width; it is raised to k when k is larger. k ≤ 0 returns every matching
// live vector (exact scan). Caller must hold vi.mu (read suffices).
func (vi *VectorIndex) searchFiltered(q []float32, k, ef int, accept func(id string) bool) []VectorMatch {
	if len(vi.nodes) == 0 || vi.live == 0 {
		return nil
	}
	if k <= 0 {
		return vi.bruteForce(q, 0, nil, accept)
	}
	if ef <= 0 {
		ef = hnswEfSearch
	}
	if k > ef {
		ef = k
	}

	accept0 := vi.isLive
	maxVisit := 0
	if accept != nil {
		accept0 = func(idx int32) bool {
			n := vi.nodes[idx]
			return !n.deleted && accept(n.id)
		}
		maxVisit = hnswFilterVisitBase + hnswFilterVisitPerEf*ef
	}

	sc := vi.scorer(q)
	ep := vi.greedyDescend(sc, vi.entry, vi.maxLevel, 0)
	found := vi.searchLayer(sc, []int32{ep}, ef, 0, accept0, maxVisit)
	return topMatches(vi, found, k)
}

// bruteForce scores candidates exactly: the nodes of ids when ids != nil,
// else every node. Tombstoned and non-accepted nodes are skipped. k ≤ 0
// returns all of them. Caller must hold vi.mu.
func (vi *VectorIndex) bruteForce(q []float32, k int, ids []string, accept func(id string) bool) []VectorMatch {
	var items []simItem
	sc := vi.scorer(q)
	score := func(idx int32) {
		n := vi.nodes[idx]
		if n.deleted || (accept != nil && !accept(n.id)) {
			return
		}
		items = append(items, simItem{vi.score(sc, idx), idx})
	}
	if ids != nil {
		for _, id := range ids {
			if idx, ok := vi.byID[id]; ok {
				score(idx)
			}
		}
	} else {
		for i := range vi.nodes {
			score(int32(i))
		}
	}
	return topMatches(vi, items, k)
}

// topMatches sorts items best-first and returns the first k (all if k ≤ 0).
func topMatches(vi *VectorIndex, items []simItem, k int) []VectorMatch {
	sort.Slice(items, func(i, j int) bool { return items[i].sim > items[j].sim })
	if k > 0 && k < len(items) {
		items = items[:k]
	}
	out := make([]VectorMatch, len(items))
	for i, it := range items {
		out[i] = VectorMatch{ID: vi.nodes[it.idx].id, Score: it.sim}
	}
	return out
}

// hnswStatsBytes is a rough memory estimate for the admin API (vectors +
// adjacency), avoiding a full graph walk.
func (vi *VectorIndex) hnswStatsBytes() int64 {
	var b int64
	if vi.pq != nil {
		b += vi.pq.bytes()
	}
	for _, n := range vi.nodes {
		b += int64(len(n.vec))*4 + int64(len(n.code)) + int64(len(n.pqc))
		if n.code != nil {
			b += 4 // scale
		}
		for l, adj := range n.neighbors {
			if l == 0 && vi.adj0 != nil {
				continue // on the mapped file, not the heap (diskGraphBytes)
			}
			b += int64(len(adj)) * 4
		}
	}
	return b
}

// nbrs returns node idx's adjacency at layer l. With a disk graph, layer 0
// is a view into the mapped file, valid only while vi.mu is held. Caller
// must hold vi.mu.
func (vi *VectorIndex) nbrs(idx int32, l int) []int32 {
	if l == 0 && vi.adj0 != nil {
		return vi.adj0.get(int(idx))
	}
	n := vi.nodes[idx]
	if l >= len(n.neighbors) {
		return nil
	}
	return n.neighbors[l]
}

// setNbrs replaces node idx's adjacency at layer l. Caller must hold vi.mu
// exclusively (or own a private index).
func (vi *VectorIndex) setNbrs(idx int32, l int, list []int32) {
	if l == 0 && vi.adj0 != nil {
		vi.adj0.set(int(idx), list)
		return
	}
	vi.nodes[idx].neighbors[l] = list
}
