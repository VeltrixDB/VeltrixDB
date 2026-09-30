package storage

// vector_index.go — in-memory HNSW vector index with cosine similarity.
//
//   - Queries run on a Hierarchical Navigable Small World graph (hnsw.go):
//     ~O(log N × M × D) per query at >0.95 recall (M=16, ef=64).
//   - Vectors are kept in RAM only.  Persistence is via the regular Put
//     path (one VLog entry per vector encoded as float32 little-endian).
//     On startup, RebuildVectorIndexes() walks the namespace and refills.
//   - Indexes belong to the StorageEngine (se.vectors), not the package, so
//     two engines in one process never see each other's vectors.
//   - The RAM index is derived state: every Put / Delete of an "@vec/" key,
//     whatever path it arrives by (PutVector, replication, raft, partition
//     transfer, backup restore), updates it through the engine's reserved-key
//     hook (search_hooks.go).
//   - Quantized namespaces (CreateVectorNamespace with QuantInt8) keep int8
//     codes in RAM — dim+4 bytes per vector instead of 4×dim — and re-rank the
//     top vectorRerankFactor×k graph candidates against the full float32
//     vectors read back from the VLog (LIRS-cached). A namespace's settings
//     persist under "@vecns/<ns>".
//
// Filtered search: a vector's metadata is the ordinary KV record whose key
// equals the vector id (PUT doc-42 {"lang":"en"} alongside VSET doc-42 ...).
// A VectorFilter is a QUERY-style predicate ("field op value", query.go) on
// that record:
//   - op "=" on a field with a secondary index (CreateFieldIndex): the index
//     yields an allow-list; up to vectorBruteForceMax ids are scored exactly,
//     larger lists restrict the HNSW walk to members.
//   - otherwise: the HNSW walk evaluates the predicate (one Get) only for
//     nodes good enough to enter the result set.
//
// API:
//
//   se.RegisterVectorNamespace("docs", 768)
//   se.PutVector("docs", "doc-42", []float32{...})
//   results := se.SearchVector("docs", queryVec, 10)  // top-10 by cosine
//   results = se.SearchVectorWithOptions("docs", queryVec, 10,
//       VectorSearchOptions{Filter: &VectorFilter{"lang", "=", "en"}})

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
)

// vectorKeyPrefix is the reserved keyspace where vectors are persisted:
// "@vec/<ns>/<id>" → little-endian float32 blob (already L2-normalized).
const vectorKeyPrefix = "@vec/"

// vectorNSConfigPrefix is where namespace settings persist:
// "@vecns/<ns>" → JSON vectorNSConfig. Partition transfer never migrates
// these keys (cluster.pinnedKeyPrefixes): every node needs them.
const vectorNSConfigPrefix = "@vecns/"

// vectorBruteForceMax is the allow-list size at or below which a filtered
// search scores the allowed vectors exactly instead of walking the graph:
// for a selective filter an exact scan is both cheaper and 100 % recall.
// A var only so tests can exercise the graph path with small data.
var vectorBruteForceMax = 2048

// vectorRerankFactor is how many graph candidates per requested result a
// quantized search re-scores against the full-precision vectors.
const vectorRerankFactor = 4

// Quantization modes for VectorNamespaceOptions.Quantization.
const (
	QuantNone = "none" // float32 components in RAM (default)
	QuantInt8 = "int8" // int8 codes + scale in RAM, float32 re-rank from disk
)

// VectorNamespaceOptions configures CreateVectorNamespace.
type VectorNamespaceOptions struct {
	Quantization string // QuantNone ("" means the same) or QuantInt8
}

// vectorNSConfig is the persisted form of a namespace's settings.
type vectorNSConfig struct {
	Dim   int    `json:"dim"`
	Quant string `json:"quant"`
}

// VectorIndex is one in-memory HNSW graph of (id, vector) tuples (hnsw.go).
type VectorIndex struct {
	dim      int
	quant    bool // int8 codes instead of float32 components (hnsw.go)
	mu       sync.RWMutex
	nodes    []*hnswNode
	byID     map[string]int32 // id → node index (live entries only)
	entry    int32            // graph entry point
	maxLevel int
	live     int // non-tombstoned count

	epoch       uint64   // bumped by every compaction swap (hnsw.go)
	compacting  bool     // a background compaction is building a new graph
	pending     []hnswOp // writes to replay onto the compacted graph
	compactions uint64   // completed compactions, for stats
}

// ErrVectorNamespaceNotFound is returned (wrapped) when a search names a
// vector namespace this engine does not have. Distributed search treats it
// as "no vectors on this node" rather than a failure.
var ErrVectorNamespaceNotFound = errors.New("vector namespace not registered")

// VectorMatch is one search result.
type VectorMatch struct {
	ID    string
	Score float32 // cosine similarity in [-1, 1]
}

// VectorFilter restricts a search to vectors whose KV record (the key equal
// to the vector id) satisfies "Field Op Value". Ops are those of QUERY:
// = != > < >= <= contains.
type VectorFilter struct {
	Field string
	Op    string
	Value string
}

// VectorSearchOptions tunes SearchVectorWithOptions.
type VectorSearchOptions struct {
	// Ef is the query beam width; 0 selects the default (64). Higher is
	// slower with better recall. Always raised to k when k is larger.
	Ef int
	// Filter, when non-nil, restricts results to matching records.
	Filter *VectorFilter
}

// vectorRegistry maps namespace → index. Zero value is ready to use.
type vectorRegistry struct {
	mu sync.RWMutex
	m  map[string]*VectorIndex
}

func (r *vectorRegistry) get(ns string) (*VectorIndex, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vi, ok := r.m[ns]
	return vi, ok
}

// RegisterVectorNamespace creates an empty index of the given dimensionality.
// Calling twice for the same namespace is a no-op when the dim matches; an
// error otherwise.
// A namespace created this way stores float32 vectors; use
// CreateVectorNamespace for a quantized one.
func (se *StorageEngine) RegisterVectorNamespace(ns string, dim int) error {
	if err := validateVectorNS(ns, dim); err != nil {
		return err
	}
	if vi, ok := se.vectors.get(ns); ok {
		return checkDim(ns, vi, dim)
	}
	r := &se.vectors
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.m[ns]; ok {
		return checkDim(ns, existing, dim)
	}
	if r.m == nil {
		r.m = map[string]*VectorIndex{}
	}
	r.m[ns] = &VectorIndex{dim: dim, byID: map[string]int32{}}
	return nil
}

func validateVectorNS(ns string, dim int) error {
	if dim <= 0 || dim > 4096 {
		return fmt.Errorf("vector dim %d out of range [1, 4096]", dim)
	}
	if ns == "" || strings.Contains(ns, "/") {
		return fmt.Errorf("vector namespace %q must be non-empty and contain no '/'", ns)
	}
	return nil
}

func checkDim(ns string, vi *VectorIndex, dim int) error {
	if vi.dim != dim {
		return fmt.Errorf("vector namespace %q already exists with dim %d (got %d)", ns, vi.dim, dim)
	}
	return nil
}

func parseQuant(q string) (bool, error) {
	switch strings.ToLower(q) {
	case "", QuantNone:
		return false, nil
	case QuantInt8:
		return true, nil
	}
	return false, fmt.Errorf("unknown quantization %q (want %s or %s)", q, QuantNone, QuantInt8)
}

// VectorNSConfigKey is the reserved key holding namespace ns's settings.
func VectorNSConfigKey(ns string) string { return vectorNSConfigPrefix + ns }

// EncodeVectorNamespaceConfig validates (ns, dim, opts) against this engine's
// existing namespaces and returns the value to store under
// VectorNSConfigKey(ns). The server's coordinator writes that key through the
// normal replicated Put path, so every node applies the same settings.
// Changing an existing namespace's quantization is allowed (its vectors are
// re-encoded); changing its dimension is not.
func (se *StorageEngine) EncodeVectorNamespaceConfig(ns string, dim int, opts VectorNamespaceOptions) ([]byte, error) {
	if err := validateVectorNS(ns, dim); err != nil {
		return nil, err
	}
	quant, err := parseQuant(opts.Quantization)
	if err != nil {
		return nil, err
	}
	if vi, ok := se.vectors.get(ns); ok {
		if err := checkDim(ns, vi, dim); err != nil {
			return nil, err
		}
	}
	cfg := vectorNSConfig{Dim: dim, Quant: QuantNone}
	if quant {
		cfg.Quant = QuantInt8
	}
	return json.Marshal(cfg)
}

// CreateVectorNamespace creates (or reconfigures) namespace ns and persists
// its settings, so restarts and replicas rebuild it the same way.
func (se *StorageEngine) CreateVectorNamespace(ns string, dim int, opts VectorNamespaceOptions) error {
	val, err := se.EncodeVectorNamespaceConfig(ns, dim, opts)
	if err != nil {
		return err
	}
	if err := se.Put(VectorNSConfigKey(ns), val, -1); err != nil {
		return err
	}
	vi, ok := se.vectors.get(ns)
	if !ok {
		return fmt.Errorf("vector namespace %q: settings did not apply", ns)
	}
	if want, _ := parseQuant(opts.Quantization); vi.quant != want {
		return fmt.Errorf("vector namespace %q: settings did not apply", ns)
	}
	return nil
}

// applyVectorNSConfig registers or reconfigures a namespace from its
// persisted settings (the "@vecns/" Put hook and startup rebuild).
func (se *StorageEngine) applyVectorNSConfig(ns string, val []byte) error {
	var cfg vectorNSConfig
	if err := json.Unmarshal(val, &cfg); err != nil {
		return fmt.Errorf("vector namespace %q: bad settings: %w", ns, err)
	}
	if err := validateVectorNS(ns, cfg.Dim); err != nil {
		return err
	}
	quant, err := parseQuant(cfg.Quant)
	if err != nil {
		return err
	}
	r := &se.vectors
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*VectorIndex{}
	}
	existing, ok := r.m[ns]
	if !ok {
		r.m[ns] = &VectorIndex{dim: cfg.Dim, quant: quant, byID: map[string]int32{}}
		return nil
	}
	if err := checkDim(ns, existing, cfg.Dim); err != nil {
		return err
	}
	if existing.quant == quant {
		return nil
	}
	// Re-encode from the persisted full-precision vectors. The registry write
	// lock is held throughout, so a concurrent vector write's hook waits and
	// then lands in the new index rather than being lost with the old one.
	fresh := &VectorIndex{dim: cfg.Dim, quant: quant, byID: map[string]int32{}}
	prefix := VectorPersistKey(ns, "")
	for _, k := range se.scanKeysWithPrefix(prefix) {
		blob, err := se.Get(k)
		if err != nil {
			continue
		}
		vec, err := decodeVector(blob, cfg.Dim)
		if err != nil {
			continue
		}
		fresh.insertHNSW(k[len(prefix):], vec)
	}
	r.m[ns] = fresh
	log.Printf("[vector] namespace %q re-encoded (quantization=%s, %d vectors)", ns, cfg.Quant, fresh.live)
	return nil
}

// normalizeVector returns v / |v|, so cosine reduces to a dot product.
func normalizeVector(v []float32) ([]float32, error) {
	var sumSq float64
	for _, x := range v {
		sumSq += float64(x) * float64(x)
	}
	norm := float32(math.Sqrt(sumSq))
	if norm == 0 || math.IsNaN(float64(norm)) || math.IsInf(float64(norm), 0) {
		return nil, errors.New("vector must be non-zero and finite")
	}
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x / norm
	}
	return out, nil
}

// PutVector inserts or updates a vector.  The vector is also persisted as a
// regular VeltrixDB key under the reserved prefix "@vec/<ns>/<id>" so it
// survives restarts (RebuildVectorIndexes re-loads from there).
func (se *StorageEngine) PutVector(ns, id string, vec []float32) error {
	vi, ok := se.vectors.get(ns)
	if !ok {
		return fmt.Errorf("vector namespace %q not registered — call RegisterVectorNamespace first", ns)
	}
	if len(vec) != vi.dim {
		return fmt.Errorf("vector dim mismatch: index=%d got=%d", vi.dim, len(vec))
	}
	if id == "" {
		return errors.New("empty vector id")
	}
	normalized, err := normalizeVector(vec)
	if err != nil {
		return err
	}

	// Persist to VLog under the reserved prefix; the Put hook
	// (search_hooks.go) inserts it into the RAM index.
	if err := se.Put(VectorPersistKey(ns, id), encodeVector(normalized), -1); err != nil {
		return fmt.Errorf("vector persist: %w", err)
	}
	return nil
}

// SearchVector returns the top-k IDs by cosine similarity to query via the
// HNSW graph.  k=0 → return all live vectors sorted descending.
func (se *StorageEngine) SearchVector(ns string, query []float32, k int) ([]VectorMatch, error) {
	return se.SearchVectorWithOptions(ns, query, k, VectorSearchOptions{})
}

// SearchVectorWithOptions is SearchVector with a beam width and an optional
// metadata filter (see the file comment for how filters execute).
func (se *StorageEngine) SearchVectorWithOptions(ns string, query []float32, k int, opts VectorSearchOptions) ([]VectorMatch, error) {
	vi, ok := se.vectors.get(ns)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrVectorNamespaceNotFound, ns)
	}
	if len(query) != vi.dim {
		return nil, fmt.Errorf("query dim mismatch: index=%d got=%d", vi.dim, len(query))
	}
	if opts.Ef < 0 {
		return nil, fmt.Errorf("ef %d must be ≥ 0", opts.Ef)
	}
	q, err := normalizeVector(query)
	if err != nil {
		return nil, err
	}

	var accept func(id string) bool
	var allow []string
	indexed := false
	if f := opts.Filter; f != nil {
		pred, err := BuildFieldPredicate(f.Field, f.Op, f.Value)
		if err != nil {
			return nil, err
		}
		// The secondary index is best-effort (secondary_index.go), so every
		// candidate is still verified against its live record.
		matches := func(id string) bool {
			val, err := se.Get(id)
			return err == nil && pred(id, val)
		}
		accept = matches
		if f.Op == "=" {
			if idxName, ok := se.findFieldIndexFor(f.Field); ok {
				indexed = true
				allow = se.LookupBySecondary(idxName, f.Value)
				set := make(map[string]struct{}, len(allow))
				for _, id := range allow {
					set[id] = struct{}{}
				}
				accept = func(id string) bool {
					_, in := set[id]
					return in && matches(id)
				}
			}
		}
	}

	// Quantized scores are approximate: fetch more candidates and re-rank
	// them against the full-precision vectors.
	kk, ef := k, opts.Ef
	if vi.quant && k > 0 {
		kk = k * vectorRerankFactor
		// Never narrower than the default beam: raising ef only to kk once
		// set it BELOW hnswEfSearch for small k (4×10 = 40 < 64), so int8
		// namespaces searched with a smaller beam than float32 ones.
		if ef <= 0 {
			ef = hnswEfSearch
		}
		if ef < kk {
			ef = kk
		}
	}
	var hits []VectorMatch
	vi.mu.RLock()
	switch {
	case indexed && len(allow) == 0:
	case indexed && len(allow) <= vectorBruteForceMax:
		hits = vi.bruteForce(q, kk, allow, accept)
	default:
		hits = vi.searchFiltered(q, kk, ef, accept)
	}
	vi.mu.RUnlock()
	if vi.quant {
		hits = se.rerankVectors(ns, vi.dim, q, hits, k)
	}
	return hits, nil
}

// rerankVectors re-scores hits exactly against the persisted float32
// vectors and returns the best k (all if k ≤ 0). A hit whose vector can no
// longer be read (deleted meanwhile) is dropped.
func (se *StorageEngine) rerankVectors(ns string, dim int, q []float32, hits []VectorMatch, k int) []VectorMatch {
	out := hits[:0]
	for _, h := range hits {
		blob, err := se.Get(VectorPersistKey(ns, h.ID))
		if err != nil {
			continue
		}
		vec, err := decodeVector(blob, dim)
		if err != nil {
			continue
		}
		out = append(out, VectorMatch{ID: h.ID, Score: dot(q, vec)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

// DeleteVector removes a vector from the index. The persisted key is also deleted.
func (se *StorageEngine) DeleteVector(ns, id string) error {
	vi, ok := se.vectors.get(ns)
	if !ok {
		return nil
	}
	if err := se.Delete(VectorPersistKey(ns, id)); err != nil && !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("vector delete: %w", err)
	}
	vi.remove(id)
	return nil
}

// VectorStats exposes counters for monitoring and the admin API.
type VectorStats struct {
	Namespace    string
	Dim          int
	Quantization string // QuantNone or QuantInt8
	Count        int    // live vectors
	Tombstones   int    // deleted/updated nodes awaiting compaction
	Compactions  uint64 // completed graph compactions
	Bytes        int64  // approximate RAM for vectors + adjacency
}

// VectorIndexStats returns one entry per registered namespace.
func (se *StorageEngine) VectorIndexStats() []VectorStats {
	r := &se.vectors
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]VectorStats, 0, len(r.m))
	for ns, vi := range r.m {
		vi.mu.RLock()
		quant := QuantNone
		if vi.quant {
			quant = QuantInt8
		}
		out = append(out, VectorStats{
			Namespace:    ns,
			Dim:          vi.dim,
			Quantization: quant,
			Count:        vi.live,
			Tombstones:   len(vi.nodes) - vi.live,
			Compactions:  vi.compactions,
			Bytes:        vi.hnswStatsBytes(),
		})
		vi.mu.RUnlock()
	}
	return out
}

// RebuildVectorIndexes rebuilds every search index from the persisted keys
// (see RebuildSearchIndexes) and returns the number of vectors loaded. Call
// once at startup AFTER WAL replay has finished (<-se.ReplayDone).
func (se *StorageEngine) RebuildVectorIndexes() (int, error) {
	n, _, err := se.RebuildSearchIndexes()
	return n, err
}

// VectorPersistKey returns the reserved KV key under which a vector is
// persisted ("@vec/<ns>/<id>").  Exposed for the distributed coordinator,
// which replicates vectors as plain KV writes of this key.
func VectorPersistKey(ns, id string) string {
	return vectorKeyPrefix + ns + "/" + id
}

// IsVectorKey reports whether key lives in the reserved vector keyspace.
func IsVectorKey(key string) bool { return strings.HasPrefix(key, vectorKeyPrefix) }

// splitVectorKey parses "@vec/<ns>/<id>".
func splitVectorKey(persistKey string) (ns, id string, err error) {
	if !IsVectorKey(persistKey) {
		return "", "", fmt.Errorf("not a vector key: %q", persistKey)
	}
	rest := persistKey[len(vectorKeyPrefix):]
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("malformed vector key: %q", persistKey)
	}
	return rest[:slash], rest[slash+1:], nil
}

// LoadVectorBlob parses a persisted "@vec/<ns>/<id>" value and loads it into
// the in-RAM vector index WITHOUT re-persisting it, auto-registering the
// namespace from the blob length.  Used by startup rebuild and by the
// replication/raft apply paths so a replica's searchable index stays in sync
// with vector writes it receives as plain KV operations.
func (se *StorageEngine) LoadVectorBlob(persistKey string, val []byte) error {
	ns, id, err := splitVectorKey(persistKey)
	if err != nil {
		return err
	}
	if len(val) == 0 || len(val)%4 != 0 {
		return fmt.Errorf("malformed vector blob for %q: %d bytes", persistKey, len(val))
	}
	dim := len(val) / 4
	vi, ok := se.vectors.get(ns)
	if !ok {
		if err := se.RegisterVectorNamespace(ns, dim); err != nil {
			return err
		}
		vi, _ = se.vectors.get(ns)
	}
	if err := checkDim(ns, vi, dim); err != nil {
		return err
	}
	vec, err := decodeVector(val, dim)
	if err != nil {
		return err
	}
	vi.insert(id, vec) // persisted blobs are already L2-normalized
	return nil
}

// UnloadVectorKey drops the vector persisted at "@vec/<ns>/<id>" from the
// in-RAM index WITHOUT touching the KV key — the Delete hook's half of
// LoadVectorBlob.
func (se *StorageEngine) UnloadVectorKey(persistKey string) error {
	ns, id, err := splitVectorKey(persistKey)
	if err != nil {
		return err
	}
	if vi, ok := se.vectors.get(ns); ok {
		vi.remove(id)
	}
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
