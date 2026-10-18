package metrics

import (
	"sort"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// SearchKind is the search type of one client request.
type SearchKind uint8

const (
	SearchVector SearchKind = iota
	SearchText
	SearchHybrid
	numSearchKinds
)

var searchKindNames = [numSearchKinds]string{"vector", "text", "hybrid"}

// SearchOutcome classifies how one search request ended.
type SearchOutcome uint8

const (
	SearchOK SearchOutcome = iota
	// SearchFailedRebuilding: refused while the startup index rebuild runs
	// (storage.ErrSearchRebuilding).
	SearchFailedRebuilding
	// SearchFailedIncomplete: a peer did not answer the fan-out and
	// --search-allow-partial is off.
	SearchFailedIncomplete
	// SearchFailedOther: every other error (bad query, unknown namespace,
	// filter error, ...).
	SearchFailedOther
	numSearchOutcomes
)

var searchReasonNames = [numSearchOutcomes]string{"", "rebuilding", "incomplete", "other"}

// maxNamespaceSeries caps the per-namespace vector series; namespaces past
// the cap (in name order) are only in the totals.
const maxNamespaceSeries = 64

// SearchMetrics counts client vector / text / hybrid searches and reports
// the search indexes' state. Every series it touches per request is created
// up front, so ObserveSearch is a few atomic adds and allocates nothing.
// A nil *SearchMetrics ignores observations.
type SearchMetrics struct {
	nodeID  string
	engine  *storage.StorageEngine
	partial func() uint64

	requests [numSearchKinds]prometheus.Counter
	errors   [numSearchKinds][numSearchOutcomes]prometheus.Counter // [k][SearchOK] unused
	latency  [numSearchKinds]prometheus.Histogram

	partialDesc    *prometheus.Desc
	ready          *prometheus.Desc
	rebuildLoaded  *prometheus.Desc
	rebuildTotal   *prometheus.Desc
	vecNamespaces  *prometheus.Desc
	vectors        *prometheus.Desc
	vecTombstones  *prometheus.Desc
	nsVectors      *prometheus.Desc
	textNamespaces *prometheus.Desc
	textDocs       *prometheus.Desc
}

// NewSearchMetrics builds the search collector for engine. partial returns
// the number of searches answered without every peer (--search-allow-partial);
// nil reports 0.
func NewSearchMetrics(nodeID string, engine *storage.StorageEngine, partial func() uint64) *SearchMetrics {
	const ns, sub = "veltrixdb", "search"
	desc := func(name, help string, extra ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(ns, sub, name), help, append([]string{"node"}, extra...), nil)
	}
	m := &SearchMetrics{
		nodeID:  nodeID,
		engine:  engine,
		partial: partial,

		partialDesc: desc("partial_total",
			"Searches answered without every peer because --search-allow-partial is set (a peer failed or timed out). Each is also logged."),
		ready: desc("ready",
			"1 when the search indexes hold every persisted vector and document; 0 while the startup (or snapshot-restore) rebuild runs and searches are refused."),
		rebuildLoaded: desc("rebuild_loaded_keys",
			"Vector and text keys loaded by the current or last search index rebuild."),
		rebuildTotal: desc("rebuild_total_keys",
			"Vector and text keys the current or last search index rebuild has to load (0 until its scan finishes)."),
		vecNamespaces: desc("vector_namespaces", "Vector namespaces on this node."),
		vectors:       desc("vectors", "Live vectors on this node, all namespaces."),
		vecTombstones: desc("vector_tombstones",
			"Deleted or updated HNSW nodes awaiting graph compaction, all namespaces."),
		nsVectors: desc("namespace_vectors",
			"Live vectors per vector namespace (the first 64 namespaces in name order; the rest are only in veltrixdb_search_vectors).", "namespace"),
		textNamespaces: desc("text_namespaces", "Full-text (BM25) namespaces on this node."),
		textDocs:       desc("text_documents", "Indexed text documents on this node, all namespaces."),
	}
	buckets := []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	for k := SearchKind(0); k < numSearchKinds; k++ {
		kind := searchKindNames[k]
		m.requests[k] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "requests_total",
			Help:        "Client vector / text / hybrid search requests handled by this node (as coordinator), failed ones included.",
			ConstLabels: prometheus.Labels{"node": nodeID, "kind": kind},
		})
		for o := SearchFailedRebuilding; o < numSearchOutcomes; o++ {
			m.errors[k][o] = prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: ns, Subsystem: sub, Name: "errors_total",
				Help:        "Search requests that returned an error, by reason: rebuilding (refused during the index rebuild), incomplete (a peer did not answer the fan-out), other.",
				ConstLabels: prometheus.Labels{"node": nodeID, "kind": kind, "reason": searchReasonNames[o]},
			})
		}
		m.latency[k] = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "latency_seconds",
			Help:        "Search request latency in seconds on the coordinating node, fan-out included.",
			Buckets:     buckets,
			ConstLabels: prometheus.Labels{"node": nodeID, "kind": kind},
		})
	}
	return m
}

// ObserveSearch records one search request that took d and ended with
// outcome. Safe on a nil receiver; does not allocate.
func (m *SearchMetrics) ObserveSearch(kind SearchKind, d time.Duration, outcome SearchOutcome) {
	if m == nil || kind >= numSearchKinds {
		return
	}
	m.requests[kind].Inc()
	m.latency[kind].Observe(d.Seconds())
	if outcome != SearchOK && outcome < numSearchOutcomes {
		m.errors[kind][outcome].Inc()
	}
}

// Describe implements prometheus.Collector.
func (m *SearchMetrics) Describe(ch chan<- *prometheus.Desc) {
	for k := SearchKind(0); k < numSearchKinds; k++ {
		m.requests[k].Describe(ch)
		m.latency[k].Describe(ch)
		for o := SearchFailedRebuilding; o < numSearchOutcomes; o++ {
			m.errors[k][o].Describe(ch)
		}
	}
	for _, d := range []*prometheus.Desc{
		m.partialDesc, m.ready, m.rebuildLoaded, m.rebuildTotal,
		m.vecNamespaces, m.vectors, m.vecTombstones, m.nsVectors,
		m.textNamespaces, m.textDocs,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector. Its cost is O(namespaces).
func (m *SearchMetrics) Collect(ch chan<- prometheus.Metric) {
	for k := SearchKind(0); k < numSearchKinds; k++ {
		m.requests[k].Collect(ch)
		m.latency[k].Collect(ch)
		for o := SearchFailedRebuilding; o < numSearchOutcomes; o++ {
			m.errors[k][o].Collect(ch)
		}
	}
	gauge := func(d *prometheus.Desc, v float64, extra ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, append([]string{m.nodeID}, extra...)...)
	}
	var partial uint64
	if m.partial != nil {
		partial = m.partial()
	}
	ch <- prometheus.MustNewConstMetric(m.partialDesc, prometheus.CounterValue, float64(partial), m.nodeID)

	ready, loaded, total := m.engine.SearchIndexStatus()
	r := 0.0
	if ready {
		r = 1
	}
	gauge(m.ready, r)
	gauge(m.rebuildLoaded, float64(loaded))
	gauge(m.rebuildTotal, float64(total))

	vc := m.engine.VectorCounts()
	sort.Slice(vc, func(i, j int) bool { return vc[i].Namespace < vc[j].Namespace })
	var live, tomb int
	for i, c := range vc {
		live += c.Live
		tomb += c.Tombstones
		if i < maxNamespaceSeries {
			gauge(m.nsVectors, float64(c.Live), c.Namespace)
		}
	}
	gauge(m.vecNamespaces, float64(len(vc)))
	gauge(m.vectors, float64(live))
	gauge(m.vecTombstones, float64(tomb))

	ts := m.engine.TextIndexStats()
	var docs int
	for _, t := range ts {
		docs += t.Docs
	}
	gauge(m.textNamespaces, float64(len(ts)))
	gauge(m.textDocs, float64(docs))
}
