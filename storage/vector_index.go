package storage

// vector_index.go — in-memory HNSW vector index with cosine similarity.
//
//   - Queries run on a Hierarchical Navigable Small World graph (hnsw.go):
//     ~O(log N × M × D) per query at >0.95 recall (M=16, ef=64) instead of
//     the previous brute-force O(N×D) scan.
//   - Vectors are kept in RAM only.  Persistence is via the regular Put
//     path (one VLog entry per vector encoded as float32 little-endian).
//     On startup, RebuildVectorIndex() walks the namespace and refills.
//
// API:
//
//   se.RegisterVectorNamespace("docs", 768)
//   se.PutVector("docs", "doc-42", []float32{...})
//   results := se.SearchVector("docs", queryVec, 10)  // top-10 by cosine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
)

// vectorKeyPrefix is the reserved keyspace where vectors are persisted:
// "@vec/<ns>/<id>" → little-endian float32 blob (already L2-normalized).
const vectorKeyPrefix = "@vec/"

// VectorIndex is one in-memory HNSW graph of (id, vector) tuples (hnsw.go).
type VectorIndex struct {
	dim      int
	mu       sync.RWMutex
	nodes    []*hnswNode
	byID     map[string]int32 // id → node index (live entries only)
	entry    int32            // graph entry point
	maxLevel int
	live     int // non-tombstoned count
}

// VectorMatch is one search result.
type VectorMatch struct {
	ID    string
	Score float32 // cosine similarity in [-1, 1]
}

// vectorIndexes is the global per-namespace map.
var (
	vectorMu      sync.RWMutex
	vectorIndexes = map[string]*VectorIndex{}
)

// RegisterVectorNamespace creates an empty index of the given dimensionality.
// Calling twice for the same namespace is a no-op when the dim matches; an
// error otherwise.
func (se *StorageEngine) RegisterVectorNamespace(ns string, dim int) error {
	if dim <= 0 || dim > 4096 {
		return fmt.Errorf("vector dim %d out of range [1, 4096]", dim)
	}
	vectorMu.Lock()
	defer vectorMu.Unlock()
	if existing, ok := vectorIndexes[ns]; ok {
		if existing.dim == dim {
			return nil
		}
		return fmt.Errorf("vector namespace %q already exists with dim %d (got %d)", ns, existing.dim, dim)
	}
	vectorIndexes[ns] = &VectorIndex{
		dim:  dim,
		byID: map[string]int32{},
	}
	return nil
}

// PutVector inserts or updates a vector.  The vector is also persisted as a
// regular VeltrixDB key under the reserved prefix "@vec/<ns>/<id>" so it
// survives restarts (RebuildVectorIndex re-loads from there).
func (se *StorageEngine) PutVector(ns, id string, vec []float32) error {
	vectorMu.RLock()
	vi, ok := vectorIndexes[ns]
	vectorMu.RUnlock()
	if !ok {
		return fmt.Errorf("vector namespace %q not registered — call RegisterVectorNamespace first", ns)
	}
	if len(vec) != vi.dim {
		return fmt.Errorf("vector dim mismatch: index=%d got=%d", vi.dim, len(vec))
	}
	// L2-normalize so cosine reduces to a dot product.
	normalized := make([]float32, vi.dim)
	var sumSq float64
	for _, v := range vec {
		sumSq += float64(v) * float64(v)
	}
	norm := float32(math.Sqrt(sumSq))
	if norm == 0 {
		return errors.New("zero vector cannot be indexed")
	}
	for i, v := range vec {
		normalized[i] = v / norm
	}

	// Persist to VLog under the reserved prefix.
	persistKey := fmt.Sprintf("%s%s/%s", vectorKeyPrefix, ns, id)
	persistVal := encodeVector(normalized)
	if err := se.Put(persistKey, persistVal, -1); err != nil {
		return fmt.Errorf("vector persist: %w", err)
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.insertHNSW(id, normalized)
	return nil
}

// SearchVector returns the top-k IDs by cosine similarity to query via the
// HNSW graph.  k=0 → return all live vectors sorted descending.
func (se *StorageEngine) SearchVector(ns string, query []float32, k int) ([]VectorMatch, error) {
	vectorMu.RLock()
	vi, ok := vectorIndexes[ns]
	vectorMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("vector namespace %q not registered", ns)
	}
	if len(query) != vi.dim {
		return nil, fmt.Errorf("query dim mismatch: index=%d got=%d", vi.dim, len(query))
	}

	// Normalize query so dot product == cosine.
	var sumSq float64
	for _, v := range query {
		sumSq += float64(v) * float64(v)
	}
	norm := float32(math.Sqrt(sumSq))
	if norm == 0 {
		return nil, errors.New("zero query vector")
	}
	q := make([]float32, vi.dim)
	for i, v := range query {
		q[i] = v / norm
	}

	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return vi.searchHNSW(q, k), nil
}

// DeleteVector removes a vector from the index. The persisted key is also deleted.
func (se *StorageEngine) DeleteVector(ns, id string) error {
	vectorMu.RLock()
	vi, ok := vectorIndexes[ns]
	vectorMu.RUnlock()
	if !ok {
		return nil
	}
	persistKey := fmt.Sprintf("%s%s/%s", vectorKeyPrefix, ns, id)
	_ = se.Delete(persistKey)

	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.removeHNSW(id)
	return nil
}

// VectorStats exposes counters for monitoring and the admin API.
type VectorStats struct {
	Namespace string
	Dim       int
	Count     int
}

// VectorIndexStats returns one entry per registered namespace.
func (se *StorageEngine) VectorIndexStats() []VectorStats {
	vectorMu.RLock()
	defer vectorMu.RUnlock()
	out := make([]VectorStats, 0, len(vectorIndexes))
	for ns, vi := range vectorIndexes {
		vi.mu.RLock()
		out = append(out, VectorStats{Namespace: ns, Dim: vi.dim, Count: vi.live})
		vi.mu.RUnlock()
	}
	return out
}

// load inserts an already-normalized vector into the in-RAM index without
// re-persisting it. Used by RebuildVectorIndexes on startup.
func (vi *VectorIndex) load(id string, vec []float32) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.insertHNSW(id, vec)
}

// RebuildVectorIndexes reloads every persisted "@vec/<ns>/<id>" key into the
// in-RAM vector indexes, auto-registering each namespace with the dimension
// inferred from the blob length (4 bytes per float32). Call once at startup
// AFTER WAL replay has finished (<-se.ReplayDone). Returns the number of
// vectors loaded. Keys with corrupt blobs or dimension mismatches are skipped
// with a log line rather than failing the whole rebuild.
func (se *StorageEngine) RebuildVectorIndexes() (int, error) {
	keys := se.scanKeysWithPrefix(vectorKeyPrefix)
	loaded := 0
	for _, k := range keys {
		rest := k[len(vectorKeyPrefix):]
		slash := strings.Index(rest, "/")
		if slash <= 0 || slash == len(rest)-1 {
			continue // malformed persisted key
		}
		ns, id := rest[:slash], rest[slash+1:]
		val, err := se.Get(k)
		if err != nil || len(val) == 0 || len(val)%4 != 0 {
			continue
		}
		dim := len(val) / 4
		if err := se.RegisterVectorNamespace(ns, dim); err != nil {
			log.Printf("[vector] rebuild: skip %q: %v", k, err)
			continue
		}
		vec, err := decodeVector(val, dim)
		if err != nil {
			continue
		}
		vectorMu.RLock()
		vi := vectorIndexes[ns]
		vectorMu.RUnlock()
		vi.load(id, vec) // persisted blobs are already L2-normalized
		loaded++
	}
	return loaded, nil
}

// VectorPersistKey returns the reserved KV key under which a vector is
// persisted ("@vec/<ns>/<id>").  Exposed for the distributed coordinator,
// which replicates vectors as plain KV writes of this key.
func VectorPersistKey(ns, id string) string {
	return fmt.Sprintf("%s%s/%s", vectorKeyPrefix, ns, id)
}

// IsVectorKey reports whether key lives in the reserved vector keyspace.
func IsVectorKey(key string) bool { return strings.HasPrefix(key, vectorKeyPrefix) }

// LoadVectorBlob parses a persisted "@vec/<ns>/<id>" value and loads it into
// the in-RAM vector index WITHOUT re-persisting it, auto-registering the
// namespace from the blob length.  Used by the replication/raft apply paths so
// a replica's searchable index stays in sync with vector writes it receives as
// plain KV operations.
func (se *StorageEngine) LoadVectorBlob(persistKey string, val []byte) error {
	if !IsVectorKey(persistKey) {
		return fmt.Errorf("not a vector key: %q", persistKey)
	}
	rest := persistKey[len(vectorKeyPrefix):]
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return fmt.Errorf("malformed vector key: %q", persistKey)
	}
	ns, id := rest[:slash], rest[slash+1:]
	if len(val) == 0 || len(val)%4 != 0 {
		return fmt.Errorf("malformed vector blob for %q: %d bytes", persistKey, len(val))
	}
	dim := len(val) / 4
	if err := se.RegisterVectorNamespace(ns, dim); err != nil {
		return err
	}
	vec, err := decodeVector(val, dim)
	if err != nil {
		return err
	}
	vectorMu.RLock()
	vi := vectorIndexes[ns]
	vectorMu.RUnlock()
	vi.load(id, vec) // persisted blobs are already L2-normalized
	return nil
}

// encodeVector serializes []float32 as little-endian uint32 bits.
func encodeVector(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(f))
	}
	return buf
}

func decodeVector(buf []byte, dim int) ([]float32, error) {
	if len(buf) != 4*dim {
		return nil, fmt.Errorf("vector blob len %d != 4*dim (%d)", len(buf), 4*dim)
	}
	out := make([]float32, dim)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	return out, nil
}
