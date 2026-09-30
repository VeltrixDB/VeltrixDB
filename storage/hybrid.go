package storage

// hybrid.go — hybrid (vector + BM25) search by Reciprocal Rank Fusion.
//
// A hybrid query runs a vector search and a BM25 text search over the same
// namespace name (vectors under "@vec/<ns>/", text under "@txt/<ns>/"), each
// for Candidates results, and fuses the two ranked lists with RRF
// (Cormack et al., SIGIR 2009):
//
//	score(id) = α / (rrfK + rank_vec(id)) + (1 − α) / (rrfK + rank_text(id))
//
// with 1-based ranks and a missing list contributing 0. RRF uses ranks, not
// raw scores, so cosine similarities and BM25 scores never need to be put on
// one scale. α = 0.5 weighs the two equally; α = 1 is vector-only, α = 0
// text-only. The fused score is what results carry.

import "fmt"

const (
	rrfK                 = 60 // the constant from the RRF paper
	hybridMinCandidates  = 50
	hybridCandidatesPerK = 4
	hybridDefaultAlpha   = 0.5
	hybridMaxCandidates  = 10000
)

// HybridOptions tunes SearchHybrid.
type HybridOptions struct {
	// Alpha is the vector list's weight in [0, 1]; the text list gets
	// 1 − Alpha. nil selects the default (0.5) — a pointer so the zero
	// value of HybridOptions is not silently text-only.
	Alpha *float64
	// Ef is the vector search beam width (0 = default).
	Ef int
	// Candidates is how many results each list contributes before fusion;
	// 0 selects max(50, 4k).
	Candidates int
	// Filter restricts both lists to matching records.
	Filter *VectorFilter
}

// alpha resolves the vector weight.
func (o HybridOptions) alpha() float64 {
	if o.Alpha == nil {
		return hybridDefaultAlpha
	}
	return *o.Alpha
}

// HybridCandidates returns the per-list candidate count for k.
func HybridCandidates(k int, opts HybridOptions) int {
	if opts.Candidates > 0 {
		if opts.Candidates > hybridMaxCandidates {
			return hybridMaxCandidates
		}
		return opts.Candidates
	}
	c := hybridCandidatesPerK * k
	if c < hybridMinCandidates {
		c = hybridMinCandidates
	}
	return c
}

// ValidateHybrid checks k and the options.
func ValidateHybrid(k int, opts HybridOptions) error {
	if k <= 0 {
		return fmt.Errorf("k must be > 0")
	}
	if a := opts.alpha(); !(a >= 0 && a <= 1) {
		return fmt.Errorf("alpha %g must be in [0, 1]", a)
	}
	if opts.Ef < 0 || opts.Candidates < 0 {
		return fmt.Errorf("ef and candidates must be ≥ 0")
	}
	return nil
}

// FuseRRF merges two best-first lists by weighted Reciprocal Rank Fusion
// and returns the top k. The options supply the weight (Alpha).
func FuseRRF(vecHits, textHits []VectorMatch, opts HybridOptions, k int) []VectorMatch {
	alpha := opts.alpha()
	scores := map[string]float64{}
	for i, h := range vecHits {
		scores[h.ID] += alpha / float64(rrfK+i+1)
	}
	for i, h := range textHits {
		scores[h.ID] += (1 - alpha) / float64(rrfK+i+1)
	}
	out := make([]VectorMatch, 0, len(scores))
	for id, s := range scores {
		if s > 0 {
			out = append(out, VectorMatch{ID: id, Score: float32(s)})
		}
	}
	sortMatches(out)
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

// SearchHybrid runs vector and BM25 search on namespace ns and fuses them.
// Either half may be absent — no vector namespace, no text namespace, or an
// empty query / nil vector — and the other half is then returned alone
// (still RRF-scored). It is an error only when neither half can run.
func (se *StorageEngine) SearchHybrid(ns string, vec []float32, query string, k int, opts HybridOptions) ([]VectorMatch, error) {
	if err := ValidateHybrid(k, opts); err != nil {
		return nil, err
	}
	cand := HybridCandidates(k, opts)
	vecHits, textHits, err := se.hybridLists(ns, vec, query, cand, opts)
	if err != nil {
		return nil, err
	}
	return FuseRRF(vecHits, textHits, opts, k), nil
}

// hybridLists runs the two halves of a hybrid search locally.
func (se *StorageEngine) hybridLists(ns string, vec []float32, query string, cand int, opts HybridOptions) (vecHits, textHits []VectorMatch, err error) {
	ranVec, ranText := false, false
	if len(vec) > 0 {
		if _, ok := se.vectors.get(ns); ok {
			vecHits, err = se.SearchVectorWithOptions(ns, vec, cand, VectorSearchOptions{Ef: opts.Ef, Filter: opts.Filter})
			if err != nil {
				return nil, nil, err
			}
			ranVec = true
		}
	}
	if len(uniqueTerms(query)) > 0 {
		textHits, err = se.SearchText(ns, query, cand, opts.Filter)
		if err != nil {
			return nil, nil, err
		}
		ranText = true
	}
	if !ranVec && !ranText {
		return nil, nil, fmt.Errorf("hybrid search on %q needs a query vector for an existing vector namespace or a text query", ns)
	}
	return vecHits, textHits, nil
}
