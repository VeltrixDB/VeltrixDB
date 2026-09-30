package main

// search_fanout.go — distributed vector / text / hybrid search.
//
// After a rebalance the TransferAgent leaves each key on its ring owner only,
// so in raft and replicated modes one node may hold just part of a vector or
// text namespace. The ring routes every derived-index key as the record it
// describes (cluster.RoutingKey), so a node always holds a record together
// with its vector, text and secondary-index entries and can evaluate a
// filter against its own data. A search therefore:
//
//   - vector: runs locally and on every non-failed peer, then merges by
//     cosine score (deduplicating ids a fully-replicated cluster returns
//     more than once);
//   - text: first sums every node's BM25 statistics for the query terms, then
//     has every node score with the cluster-wide totals, so scores from
//     different nodes are comparable before the merge;
//   - hybrid: builds the two global ranked lists as above and fuses them with
//     RRF (storage/hybrid.go) on the coordinating node.
//
// QUERY and IDXQUERY fan out the same way (merged and deduplicated by key).
//
// Peers are queried on their transfer listener (POST /internal/search), which
// carries the cluster's TLS / mTLS settings and the optional HMAC cluster
// secret. If a peer fails or times out the search FAILS, naming the peer — a
// silently partial result looks like a correct one. --search-allow-partial
// returns what the reachable nodes have instead (logged and counted in
// partialSearches). A node the failure detector has marked failed is not
// queried at all. --search-fanout=false keeps every search local (for
// clusters that never rebalance, where each node already holds everything).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

const searchPath = "/internal/search"

// defaultSearchTimeout bounds one peer's answer to one search phase.
const defaultSearchTimeout = 2 * time.Second

// partialSearches counts searches answered without every peer
// (--search-allow-partial).
var partialSearches atomic.Uint64

// searchRequest is one node-to-node search call.
type searchRequest struct {
	Kind   string                `json:"kind"` // "vector" | "text" | "textstats" | "query" | "idxquery"
	NS     string                `json:"ns"`
	K      int                   `json:"k"`
	Ef     int                   `json:"ef,omitempty"`
	Vec    []float32             `json:"vec,omitempty"`
	Query  string                `json:"q,omitempty"`
	Filter *storage.VectorFilter `json:"filter,omitempty"`
	Stats  *storage.TextStats    `json:"stats,omitempty"`
	// idxquery: index name and value (K = limit). query: NS + Filter
	// (field op value), K = limit.
	Index string `json:"index,omitempty"`
	Value string `json:"value,omitempty"`
}

// searchResponse is a peer's answer.
type searchResponse struct {
	Hits  []storage.VectorMatch `json:"hits,omitempty"`
	Stats *storage.TextStats    `json:"stats,omitempty"`
	Keys  []string              `json:"keys,omitempty"` // idxquery
	KVs   []storage.NSEntry     `json:"kvs,omitempty"`  // query
	// Missing: the peer has no vector namespace of that name.
	Missing bool `json:"missing,omitempty"`
}

// runLocalSearch executes req against this node's engine only. While the
// startup rebuild is running the search indexes are incomplete, so vector /
// text searches fail (a peer's failure fails the fan-out too) unless
// allowPartial.
func runLocalSearch(engine *storage.StorageEngine, req searchRequest) (searchResponse, error) {
	return runLocalSearchOpt(engine, req, false)
}

func runLocalSearchOpt(engine *storage.StorageEngine, req searchRequest, allowPartial bool) (searchResponse, error) {
	if (req.Kind == "vector" || req.Kind == "text" || req.Kind == "textstats") && !allowPartial {
		if err := engine.CheckSearchReady(); err != nil {
			return searchResponse{}, err
		}
	}
	switch req.Kind {
	case "vector":
		hits, err := engine.SearchVectorWithOptions(req.NS, req.Vec, req.K,
			storage.VectorSearchOptions{Ef: req.Ef, Filter: req.Filter})
		if errors.Is(err, storage.ErrVectorNamespaceNotFound) {
			return searchResponse{Missing: true}, nil
		}
		return searchResponse{Hits: hits}, err
	case "text":
		hits, err := engine.SearchTextWithStats(req.NS, req.Query, req.K, req.Filter, req.Stats)
		return searchResponse{Hits: hits}, err
	case "textstats":
		st := engine.TextSearchStats(req.NS, req.Query)
		return searchResponse{Stats: &st}, nil
	case "idxquery":
		keys := engine.LookupBySecondary(req.Index, req.Value)
		if req.K > 0 && len(keys) > req.K {
			keys = keys[:req.K]
		}
		return searchResponse{Keys: keys}, nil
	case "query":
		if req.Filter == nil {
			return searchResponse{}, errors.New("query: missing predicate")
		}
		kvs, err := engine.QueryNS(req.NS, req.Filter.Field, req.Filter.Op, req.Filter.Value, req.K)
		return searchResponse{KVs: kvs}, err
	}
	return searchResponse{}, fmt.Errorf("unknown search kind %q", req.Kind)
}

// newSearchHandler serves POST /internal/search for peers.
func newSearchHandler(engine *storage.StorageEngine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req searchRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := runLocalSearch(engine, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

// searchReady refuses searches while this node's indexes are still being
// rebuilt after a restart, unless --search-allow-partial.
func (c *coordinator) searchReady() error {
	if c.searchAllowPartial {
		return nil
	}
	return c.engine.CheckSearchReady()
}

// searchPeers returns the nodes a search fans out to (none = local only).
func (c *coordinator) searchPeers() []string {
	if c.mode == modeStandalone || c.ta == nil || c.pm == nil || !c.searchFanout {
		return nil
	}
	return c.pm.SearchPeers(c.localID)
}

// fanout sends req to every peer in parallel and returns their answers. A
// failed peer fails the whole call unless --search-allow-partial is set, in
// which case it is logged, counted and left out.
func (c *coordinator) fanout(peers []string, req searchRequest) ([]searchResponse, error) {
	timeout := c.searchTimeout
	if timeout <= 0 {
		timeout = defaultSearchTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var mu sync.Mutex
	var out []searchResponse
	var failed []string
	var wg sync.WaitGroup
	for _, id := range peers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			var resp searchResponse
			err := c.ta.PostJSON(ctx, id, searchPath, req, &resp)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = append(failed, fmt.Sprintf("%s (%v)", id, err))
				return
			}
			out = append(out, resp)
		}(id)
	}
	wg.Wait()
	if len(failed) == 0 {
		return out, nil
	}
	sort.Strings(failed)
	if !c.searchAllowPartial {
		return nil, fmt.Errorf("search incomplete: %d of %d peers did not answer: %s (retry, or start the server with --search-allow-partial)",
			len(failed), len(peers), strings.Join(failed, "; "))
	}
	partialSearches.Add(1)
	log.Printf("[search] partial %s result without %s", req.Kind, strings.Join(failed, "; "))
	return out, nil
}

// mergeHits unions best-first lists (keeping each id's best score) and
// returns the top k (all if k ≤ 0).
func mergeHits(lists [][]storage.VectorMatch, k int) []storage.VectorMatch {
	best := map[string]float32{}
	for _, l := range lists {
		for _, h := range l {
			if s, ok := best[h.ID]; !ok || h.Score > s {
				best[h.ID] = h.Score
			}
		}
	}
	out := make([]storage.VectorMatch, 0, len(best))
	for id, s := range best {
		out = append(out, storage.VectorMatch{ID: id, Score: s})
	}
	storage.SortMatches(out)
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

// VSearch is a (possibly distributed) vector search.
func (c *coordinator) VSearch(ns string, q []float32, k int, opts storage.VectorSearchOptions) ([]storage.VectorMatch, error) {
	if err := c.searchReady(); err != nil {
		return nil, err
	}
	peers := c.searchPeers()
	if len(peers) == 0 {
		return c.engine.SearchVectorWithOptions(ns, q, k, opts)
	}
	req := searchRequest{Kind: "vector", NS: ns, K: k, Ef: opts.Ef, Vec: q, Filter: opts.Filter}
	local, err := runLocalSearchOpt(c.engine, req, c.searchAllowPartial)
	if err != nil {
		return nil, err // bad query (dim, filter): same answer on every node
	}
	resps, err := c.fanout(peers, req)
	if err != nil {
		return nil, err
	}
	lists := [][]storage.VectorMatch{local.Hits}
	found := !local.Missing
	for _, r := range resps {
		lists = append(lists, r.Hits)
		found = found || !r.Missing
	}
	if !found {
		return nil, fmt.Errorf("%w: %q", storage.ErrVectorNamespaceNotFound, ns)
	}
	return mergeHits(lists, k), nil
}

// TSearch is a (possibly distributed) BM25 search.
func (c *coordinator) TSearch(ns, query string, k int, filter *storage.VectorFilter) ([]storage.VectorMatch, error) {
	if err := c.searchReady(); err != nil {
		return nil, err
	}
	peers := c.searchPeers()
	if len(peers) == 0 {
		return c.engine.SearchText(ns, query, k, filter)
	}
	if !storage.HasSearchTerms(query) {
		return nil, errors.New("query has no searchable terms")
	}
	// Phase 1: cluster-wide statistics.
	st := c.engine.TextSearchStats(ns, query)
	statResps, err := c.fanout(peers, searchRequest{Kind: "textstats", NS: ns, Query: query})
	if err != nil {
		return nil, err
	}
	for _, r := range statResps {
		if r.Stats != nil {
			st.Add(*r.Stats)
		}
	}
	// Phase 2: every node scores with them.
	req := searchRequest{Kind: "text", NS: ns, K: k, Query: query, Filter: filter, Stats: &st}
	local, err := runLocalSearchOpt(c.engine, req, c.searchAllowPartial)
	if err != nil {
		return nil, err
	}
	resps, err := c.fanout(peers, req)
	if err != nil {
		return nil, err
	}
	lists := [][]storage.VectorMatch{local.Hits}
	for _, r := range resps {
		lists = append(lists, r.Hits)
	}
	return mergeHits(lists, k), nil
}

// IdxQuery is a (possibly distributed) secondary-index lookup. Result order
// is unspecified, as for LookupBySecondary; limit ≤ 0 = unlimited.
func (c *coordinator) IdxQuery(name, value string, limit int) ([]string, error) {
	local, _ := runLocalSearchOpt(c.engine, searchRequest{Kind: "idxquery", Index: name, Value: value, K: limit}, c.searchAllowPartial)
	peers := c.searchPeers()
	if len(peers) == 0 {
		return local.Keys, nil
	}
	resps, err := c.fanout(peers, searchRequest{Kind: "idxquery", Index: name, Value: value, K: limit})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, keys := range append([][]string{local.Keys}, keysOf(resps)...) {
		for _, k := range keys {
			if !seen[k] && (limit <= 0 || len(out) < limit) {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out, nil
}

func keysOf(resps []searchResponse) [][]string {
	out := make([][]string, len(resps))
	for i, r := range resps {
		out[i] = r.Keys
	}
	return out
}

// Query is a (possibly distributed) QUERY <ns> WHERE field op value.
func (c *coordinator) Query(ns, field, op, value string, limit int) ([]storage.NSEntry, error) {
	peers := c.searchPeers()
	if len(peers) == 0 {
		return c.engine.QueryNS(ns, field, op, value, limit)
	}
	req := searchRequest{Kind: "query", NS: ns, K: limit, Filter: &storage.VectorFilter{Field: field, Op: op, Value: value}}
	local, err := runLocalSearchOpt(c.engine, req, c.searchAllowPartial)
	if err != nil {
		return nil, err
	}
	resps, err := c.fanout(peers, req)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []storage.NSEntry
	add := func(kvs []storage.NSEntry) {
		for _, e := range kvs {
			if !seen[e.Key] && (limit <= 0 || len(out) < limit) {
				seen[e.Key] = true
				out = append(out, e)
			}
		}
	}
	add(local.KVs)
	for _, r := range resps {
		add(r.KVs)
	}
	return out, nil
}

// HSearch is a (possibly distributed) hybrid search.
func (c *coordinator) HSearch(ns string, vec []float32, query string, k int, opts storage.HybridOptions) ([]storage.VectorMatch, error) {
	if err := c.searchReady(); err != nil {
		return nil, err
	}
	if len(c.searchPeers()) == 0 {
		return c.engine.SearchHybrid(ns, vec, query, k, opts)
	}
	if err := storage.ValidateHybrid(k, opts); err != nil {
		return nil, err
	}
	cand := storage.HybridCandidates(k, opts)
	var vecHits, textHits []storage.VectorMatch
	ran := false
	if len(vec) > 0 {
		hits, err := c.VSearch(ns, vec, cand, storage.VectorSearchOptions{Ef: opts.Ef, Filter: opts.Filter})
		switch {
		case err == nil:
			vecHits, ran = hits, true
		case !errors.Is(err, storage.ErrVectorNamespaceNotFound):
			return nil, err
		}
	}
	if storage.HasSearchTerms(query) {
		hits, err := c.TSearch(ns, query, cand, opts.Filter)
		if err != nil {
			return nil, err
		}
		textHits, ran = hits, true
	}
	if !ran {
		return nil, fmt.Errorf("hybrid search on %q needs a query vector for an existing vector namespace or a text query", ns)
	}
	return storage.FuseRRF(vecHits, textHits, opts, k), nil
}

// loadClusterSecret reads the transfer-listener secret from path, or from
// VELTRIXDB_CLUSTER_SECRET when path is empty. Surrounding whitespace is
// trimmed; an empty result means no signing. A secret shorter than 16 bytes
// is refused.
func loadClusterSecret(path string) ([]byte, error) {
	raw := os.Getenv("VELTRIXDB_CLUSTER_SECRET")
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		raw = string(b)
	}
	secret := strings.TrimSpace(raw)
	if secret != "" && len(secret) < 16 {
		return nil, errors.New("cluster secret must be at least 16 bytes")
	}
	return []byte(secret), nil
}

// VCreate creates or reconfigures a vector namespace on every node by writing
// its settings key through the replicated Put path.
func (c *coordinator) VCreate(ns string, dim int, opts storage.VectorNamespaceOptions) error {
	val, err := c.engine.EncodeVectorNamespaceConfig(ns, dim, opts)
	if err != nil {
		return err
	}
	return c.Put(storage.VectorNSConfigKey(ns), val, -1)
}

// TSet stores a text document (indexed by every node that applies it).
func (c *coordinator) TSet(ns, id, text string) error {
	if err := storage.ValidateTextDoc(ns, id, text); err != nil {
		return err
	}
	return c.Put(storage.TextPersistKey(ns, id), []byte(text), -1)
}

// TDel deletes a text document.
func (c *coordinator) TDel(ns, id string) error {
	if err := storage.ValidateTextDoc(ns, id, ""); err != nil {
		return err
	}
	return c.Delete(storage.TextPersistKey(ns, id))
}
