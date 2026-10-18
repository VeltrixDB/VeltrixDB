package storage

// VectorCount is one vector namespace's size, for /metrics.
type VectorCount struct {
	Namespace  string
	Live       int // live vectors
	Tombstones int // deleted / updated nodes awaiting compaction
}

// VectorCounts returns the live and tombstoned vector counts of every
// vector namespace. Unlike VectorIndexStats (which walks every graph node to
// size the RAM), it is O(namespaces), so a Prometheus scrape can call it on
// any index size.
func (se *StorageEngine) VectorCounts() []VectorCount {
	r := &se.vectors
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]VectorCount, 0, len(r.m))
	for ns, vi := range r.m {
		vi.mu.RLock()
		out = append(out, VectorCount{Namespace: ns, Live: vi.live, Tombstones: len(vi.nodes) - vi.live})
		vi.mu.RUnlock()
	}
	return out
}
