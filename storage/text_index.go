package storage

// text_index.go — in-memory BM25 full-text index.
//
// Documents persist as ordinary keys "@txt/<ns>/<id>" → UTF-8 text; the
// per-namespace inverted index in RAM is derived state, maintained by the
// reserved-key Put / Delete hook (search_hooks.go) and rebuilt at startup
// (RebuildSearchIndexes). The document id is the same id space as vectors
// and KV records: PUT doc-42 {...}, VSET doc-42 ..., TSET doc-42 ... all
// describe one record, which is what hybrid search (hybrid.go) fuses on.
//
// Scoring is Okapi BM25 (k1 = 1.2, b = 0.75) with
// idf = ln(1 + (N − df + 0.5) / (df + 0.5)). The corpus statistics (N, total
// length, per-term df) are passed in as TextStats, so a distributed search
// can score every node's documents with the cluster-wide statistics
// (coordinator: gather stats, sum, then search) and get comparable scores.
//
// Tokenizer: Unicode-aware — lowercases, splits on anything that is not a
// letter, digit or combining mark, and drops nothing (no stop words, no stemming), so it
// behaves the same for any language written with word separators. Scripts
// without spaces (CJK, Thai) are indexed as whole runs.

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// textKeyPrefix is the reserved keyspace for text documents.
const textKeyPrefix = "@txt/"

const (
	bm25K1 = 1.2
	bm25B  = 0.75

	// maxTextDocBytes bounds one document; larger texts belong in chunks.
	maxTextDocBytes = 1 << 20
)

// TextStats are the corpus statistics BM25 scores with.
type TextStats struct {
	Docs     int64            `json:"n"`   // documents in the corpus
	TotalLen int64            `json:"len"` // sum of document lengths (tokens)
	DF       map[string]int64 `json:"df"`  // query term → documents containing it
}

// Add accumulates o into s (summing per-node statistics).
func (s *TextStats) Add(o TextStats) {
	s.Docs += o.Docs
	s.TotalLen += o.TotalLen
	if s.DF == nil {
		s.DF = map[string]int64{}
	}
	for t, n := range o.DF {
		s.DF[t] += n
	}
}

// textDoc is one indexed document.
type textDoc struct {
	length int               // tokens
	terms  map[string]uint32 // term → frequency in this document
}

// TextIndex is one namespace's inverted index.
type TextIndex struct {
	mu       sync.RWMutex
	docs     map[string]*textDoc
	postings map[string]map[string]uint32 // term → doc id → tf
	totalLen int64
}

func newTextIndex() *TextIndex {
	return &TextIndex{docs: map[string]*textDoc{}, postings: map[string]map[string]uint32{}}
}

// textRegistry maps namespace → index. Zero value is ready to use.
type textRegistry struct {
	mu sync.RWMutex
	m  map[string]*TextIndex
}

func (r *textRegistry) get(ns string) (*TextIndex, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ti, ok := r.m[ns]
	return ti, ok
}

func (r *textRegistry) getOrCreate(ns string) *TextIndex {
	if ti, ok := r.get(ns); ok {
		return ti
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ti, ok := r.m[ns]; ok {
		return ti
	}
	if r.m == nil {
		r.m = map[string]*TextIndex{}
	}
	ti := newTextIndex()
	r.m[ns] = ti
	return ti
}

// Tokenize splits text into lowercase runs of letters, digits and combining
// marks. Marks must stay inside the word: Indic vowel signs and viramas
// (e.g. the "ु" in "दुनिया") are category M, not L, and splitting on them
// breaks every word of those scripts into fragments.
func Tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsMark(r)
	})
}

// HasSearchTerms reports whether query contains at least one token.
func HasSearchTerms(query string) bool { return len(Tokenize(query)) > 0 }

// uniqueTerms returns the distinct tokens of text in first-seen order.
func uniqueTerms(text string) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range Tokenize(text) {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// put indexes (or re-indexes) document id.
func (ti *TextIndex) put(id, text string) {
	toks := Tokenize(text)
	terms := make(map[string]uint32, len(toks))
	for _, t := range toks {
		terms[t]++
	}
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.removeLocked(id)
	ti.docs[id] = &textDoc{length: len(toks), terms: terms}
	ti.totalLen += int64(len(toks))
	for t, tf := range terms {
		p := ti.postings[t]
		if p == nil {
			p = map[string]uint32{}
			ti.postings[t] = p
		}
		p[id] = tf
	}
}

func (ti *TextIndex) remove(id string) {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.removeLocked(id)
}

func (ti *TextIndex) removeLocked(id string) {
	d, ok := ti.docs[id]
	if !ok {
		return
	}
	for t := range d.terms {
		if p := ti.postings[t]; p != nil {
			delete(p, id)
			if len(p) == 0 {
				delete(ti.postings, t)
			}
		}
	}
	ti.totalLen -= int64(d.length)
	delete(ti.docs, id)
}

// stats returns this index's statistics for terms.
func (ti *TextIndex) stats(terms []string) TextStats {
	ti.mu.RLock()
	defer ti.mu.RUnlock()
	st := TextStats{Docs: int64(len(ti.docs)), TotalLen: ti.totalLen, DF: make(map[string]int64, len(terms))}
	for _, t := range terms {
		st.DF[t] = int64(len(ti.postings[t]))
	}
	return st
}

// search scores every document containing at least one term with BM25 under
// st and returns the top k (all if k ≤ 0) accepted ones.
func (ti *TextIndex) search(terms []string, k int, st TextStats, accept func(id string) bool) []VectorMatch {
	if st.Docs == 0 || len(terms) == 0 {
		return nil
	}
	avgdl := float64(st.TotalLen) / float64(st.Docs)
	if avgdl <= 0 {
		avgdl = 1
	}
	ti.mu.RLock()
	scores := map[string]float64{}
	for _, t := range terms {
		p := ti.postings[t]
		if len(p) == 0 {
			continue
		}
		df := float64(st.DF[t])
		if df <= 0 {
			df = float64(len(p))
		}
		idf := math.Log(1 + (float64(st.Docs)-df+0.5)/(df+0.5))
		for id, tf := range p {
			dl := float64(ti.docs[id].length)
			f := float64(tf)
			scores[id] += idf * f * (bm25K1 + 1) / (f + bm25K1*(1-bm25B+bm25B*dl/avgdl))
		}
	}
	ti.mu.RUnlock()

	all := make([]VectorMatch, 0, len(scores))
	for id, sc := range scores {
		all = append(all, VectorMatch{ID: id, Score: float32(sc)})
	}
	sortMatches(all)
	if accept == nil {
		if k > 0 && len(all) > k {
			all = all[:k]
		}
		return all
	}
	// Filter best-first so the (one Get per id) check runs only until k
	// matches are found.
	out := all[:0]
	for _, m := range all {
		if accept(m.ID) {
			out = append(out, m)
			if k > 0 && len(out) == k {
				break
			}
		}
	}
	return out
}

// SortMatches orders best-first, ties by id so results are deterministic.
func SortMatches(m []VectorMatch) { sortMatches(m) }

// sortMatches orders best-first, ties by id so results are deterministic.
func sortMatches(m []VectorMatch) {
	sort.Slice(m, func(i, j int) bool {
		if m[i].Score != m[j].Score {
			return m[i].Score > m[j].Score
		}
		return m[i].ID < m[j].ID
	})
}

// TextPersistKey returns the reserved key a text document persists under.
func TextPersistKey(ns, id string) string { return textKeyPrefix + ns + "/" + id }

// IsTextKey reports whether key lives in the reserved text keyspace.
func IsTextKey(key string) bool { return strings.HasPrefix(key, textKeyPrefix) }

func splitTextKey(key string) (ns, id string, err error) {
	if !IsTextKey(key) {
		return "", "", fmt.Errorf("not a text key: %q", key)
	}
	rest := key[len(textKeyPrefix):]
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("malformed text key: %q", key)
	}
	return rest[:slash], rest[slash+1:], nil
}

// ValidateTextDoc checks a document before it is written.
func ValidateTextDoc(ns, id, text string) error {
	if ns == "" || strings.Contains(ns, "/") {
		return fmt.Errorf("text namespace %q must be non-empty and contain no '/'", ns)
	}
	if id == "" {
		return errors.New("empty document id")
	}
	if len(text) > maxTextDocBytes {
		return fmt.Errorf("document is %d bytes; the limit is %d", len(text), maxTextDocBytes)
	}
	return nil
}

// PutText stores and indexes text as document id of namespace ns. The
// namespace is created on first use.
func (se *StorageEngine) PutText(ns, id, text string) error {
	if err := ValidateTextDoc(ns, id, text); err != nil {
		return err
	}
	// The Put hook (search_hooks.go) indexes it.
	return se.Put(TextPersistKey(ns, id), []byte(text), -1)
}

// DeleteText removes document id from namespace ns.
func (se *StorageEngine) DeleteText(ns, id string) error {
	if err := se.Delete(TextPersistKey(ns, id)); err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if ti, ok := se.texts.get(ns); ok {
		ti.remove(id)
	}
	return nil
}

// loadTextDoc indexes a persisted "@txt/" value (Put hook, startup rebuild).
func (se *StorageEngine) loadTextDoc(key string, val []byte) error {
	ns, id, err := splitTextKey(key)
	if err != nil {
		return err
	}
	se.texts.getOrCreate(ns).put(id, string(val))
	return nil
}

// unloadTextDoc drops a document from the RAM index (Delete hook).
func (se *StorageEngine) unloadTextDoc(key string) error {
	ns, id, err := splitTextKey(key)
	if err != nil {
		return err
	}
	if ti, ok := se.texts.get(ns); ok {
		ti.remove(id)
	}
	return nil
}

// TextSearchStats returns this node's statistics for query's terms in ns
// (zero stats for an unknown namespace). Distributed search sums these over
// all nodes and passes the total to SearchTextWithStats.
func (se *StorageEngine) TextSearchStats(ns, query string) TextStats {
	terms := uniqueTerms(query)
	ti, ok := se.texts.get(ns)
	if !ok {
		st := TextStats{DF: map[string]int64{}}
		for _, t := range terms {
			st.DF[t] = 0
		}
		return st
	}
	return ti.stats(terms)
}

// SearchText returns the top-k documents of ns for query by BM25, scored
// with this node's own statistics. filter (optional) is checked against the
// KV record whose key is the document id, as in vector search.
func (se *StorageEngine) SearchText(ns, query string, k int, filter *VectorFilter) ([]VectorMatch, error) {
	return se.SearchTextWithStats(ns, query, k, filter, nil)
}

// SearchTextWithStats is SearchText scored with the given corpus statistics
// (nil = this node's own).
func (se *StorageEngine) SearchTextWithStats(ns, query string, k int, filter *VectorFilter, st *TextStats) ([]VectorMatch, error) {
	terms := uniqueTerms(query)
	if len(terms) == 0 {
		return nil, errors.New("query has no searchable terms")
	}
	accept, err := se.recordFilter(filter)
	if err != nil {
		return nil, err
	}
	ti, ok := se.texts.get(ns)
	if !ok {
		return nil, nil
	}
	stats := ti.stats(terms)
	if st != nil {
		stats = *st
	}
	return ti.search(terms, k, stats, accept), nil
}

// recordFilter builds the per-id predicate for a VectorFilter (nil → nil).
// Every candidate is checked against its live record.
func (se *StorageEngine) recordFilter(f *VectorFilter) (func(id string) bool, error) {
	if f == nil {
		return nil, nil
	}
	pred, err := BuildFieldPredicate(f.Field, f.Op, f.Value)
	if err != nil {
		return nil, err
	}
	return func(id string) bool {
		val, err := se.Get(id)
		return err == nil && pred(id, val)
	}, nil
}

// TextStatsSummary exposes one namespace's size for monitoring.
type TextStatsSummary struct {
	Namespace string
	Docs      int
	Terms     int
}

// TextIndexStats returns one entry per text namespace.
func (se *StorageEngine) TextIndexStats() []TextStatsSummary {
	r := &se.texts
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TextStatsSummary, 0, len(r.m))
	for ns, ti := range r.m {
		ti.mu.RLock()
		out = append(out, TextStatsSummary{Namespace: ns, Docs: len(ti.docs), Terms: len(ti.postings)})
		ti.mu.RUnlock()
	}
	return out
}
