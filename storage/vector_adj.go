package storage

// vector_adj.go — layer-0 HNSW adjacency in a memory-mapped file
// ("GRAPH disk").
//
// Layer 0 holds every node and up to hnswMmax0 (32) edges each, so it is
// almost all of a graph's edge memory; the upper layers hold ~1/M of the
// nodes and stay on the heap. With a disk graph each node's layer-0 list is
// a fixed-size record in a file mapped MAP_SHARED:
//
//	record i: [int32 count][hnswMmax0 × int32 neighbour index] = 132 bytes
//
// Pages are file-backed, so under memory pressure the kernel writes them
// back to NVMe and drops them instead of the process being OOM-killed, and
// they are not in the Go heap (no GC scanning of 32 × N edges). Hot parts of
// the graph stay cached by the page cache, the way DiskANN relies on it.
//
// The file is created in the engine's data dir and unlinked straight after
// mapping: it is scratch space, never read after a restart (the graph is
// rebuilt from the persisted vectors), and cannot be left behind by a crash.
//
// Growth remaps the file, which invalidates earlier views, so ensure / set
// run only under the index write lock and get's views must not outlive the
// lock they were taken under.

const adjRecordInts = 1 + hnswMmax0
const adjRecordBytes = 4 * adjRecordInts

// mappedAdj is the layer-0 adjacency store of one VectorIndex.
type mappedAdj struct {
	region adjRegion // platform mapping (vector_adj_unix.go / _other.go)
	cap    int       // records the mapping holds
}

func newMappedAdj(dir string) (*mappedAdj, error) {
	r, err := newAdjRegion(dir)
	if err != nil {
		return nil, err
	}
	return &mappedAdj{region: r}, nil
}

// ensure grows the mapping to hold at least n records.
func (a *mappedAdj) ensure(n int) {
	if n <= a.cap {
		return
	}
	c := a.cap * 2
	if c < 1024 {
		c = 1024
	}
	for c < n {
		c *= 2
	}
	if err := a.region.resize(c * adjRecordBytes); err != nil {
		panic("vector disk graph: " + err.Error())
	}
	a.cap = c
}

// get returns record i's neighbours as a view into the mapping.
func (a *mappedAdj) get(i int) []int32 {
	ints := a.region.ints()
	rec := ints[i*adjRecordInts : (i+1)*adjRecordInts]
	return rec[1 : 1+int(rec[0])]
}

// set writes record i. len(list) ≤ hnswMmax0 (pruning guarantees it).
func (a *mappedAdj) set(i int, list []int32) {
	if len(list) > hnswMmax0 {
		list = list[:hnswMmax0]
	}
	ints := a.region.ints()
	rec := ints[i*adjRecordInts : (i+1)*adjRecordInts]
	rec[0] = int32(len(list))
	copy(rec[1:], list)
}

// bytes is the size of the mapping (on the file, not the heap).
func (a *mappedAdj) bytes() int64 { return int64(a.cap) * adjRecordBytes }

func (a *mappedAdj) close() { a.region.close() }
