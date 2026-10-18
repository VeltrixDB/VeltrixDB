package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
	"github.com/prometheus/client_golang/prometheus"
)

func testEngine(t *testing.T) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.ScrubEnabled = false
	cfg.BloomFilterShardBits = 1 << 12 // invariant 39
	cfg.WALFlushWindowMs = 1
	cfg.VLogFlushWindowMs = 1
	se, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	<-se.ReplayDone
	return se
}

// ObserveSearch is on the client search path: it must not allocate, and a
// nil receiver (metrics not wired, e.g. in-process tests) must be a no-op.
func TestSearchMetrics_ObserveDoesNotAllocate(t *testing.T) {
	m := NewSearchMetrics("n1", nil, nil)
	allocs := testing.AllocsPerRun(1000, func() {
		m.ObserveSearch(SearchVector, 3*time.Millisecond, SearchOK)
		m.ObserveSearch(SearchHybrid, time.Millisecond, SearchFailedIncomplete)
	})
	if allocs != 0 {
		t.Fatalf("ObserveSearch allocates %.1f times per call pair", allocs)
	}
	var nilM *SearchMetrics
	nilM.ObserveSearch(SearchText, time.Millisecond, SearchFailedOther)
}

// sample returns the value of the series name{labels...} (labels given as
// name, value pairs; unlisted labels are ignored) and whether it exists.
func sample(t *testing.T, g prometheus.Gatherer, name string, labels ...string) (float64, bool) {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			for i := 0; i+1 < len(labels); i += 2 {
				found := false
				for _, l := range m.GetLabel() {
					found = found || (l.GetName() == labels[i] && l.GetValue() == labels[i+1])
				}
				if !found {
					continue next
				}
			}
			switch {
			case m.Gauge != nil:
				return m.GetGauge().GetValue(), true
			case m.Counter != nil:
				return m.GetCounter().GetValue(), true
			case m.Histogram != nil:
				return float64(m.GetHistogram().GetSampleCount()), true
			}
		}
	}
	return 0, false
}

func seriesCount(t *testing.T, g prometheus.Gatherer, name string) int {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func expect(t *testing.T, g prometheus.Gatherer, want float64, name string, labels ...string) {
	t.Helper()
	got, ok := sample(t, g, name, labels...)
	if !ok {
		t.Fatalf("%s%v not served", name, labels)
	}
	if got != want {
		t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func TestSearchMetrics_Collect(t *testing.T) {
	se := testEngine(t)
	if err := se.RegisterVectorNamespace("docs", 3); err != nil {
		t.Fatal(err)
	}
	if err := se.PutVector("docs", "a", []float32{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := se.PutVector("docs", "b", []float32{0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := se.PutText("notes", "n1", "hello search world"); err != nil {
		t.Fatal(err)
	}
	var partial uint64 = 7
	m := NewSearchMetrics("n1", se, func() uint64 { return partial })
	m.ObserveSearch(SearchVector, 2*time.Millisecond, SearchOK)
	m.ObserveSearch(SearchVector, 2*time.Millisecond, SearchFailedRebuilding)
	m.ObserveSearch(SearchText, time.Millisecond, SearchFailedOther)

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(m)
	expect(t, reg, 2, "veltrixdb_search_namespace_vectors", "namespace", "docs", "node", "n1")
	expect(t, reg, 2, "veltrixdb_search_vectors", "node", "n1")
	expect(t, reg, 1, "veltrixdb_search_vector_namespaces")
	expect(t, reg, 1, "veltrixdb_search_text_documents")
	expect(t, reg, 7, "veltrixdb_search_partial_total")
	expect(t, reg, 1, "veltrixdb_search_ready")
	expect(t, reg, 2, "veltrixdb_search_requests_total", "kind", "vector")
	expect(t, reg, 1, "veltrixdb_search_requests_total", "kind", "text")
	expect(t, reg, 0, "veltrixdb_search_requests_total", "kind", "hybrid")
	expect(t, reg, 2, "veltrixdb_search_latency_seconds", "kind", "vector")
	expect(t, reg, 1, "veltrixdb_search_errors_total", "kind", "vector", "reason", "rebuilding")
	expect(t, reg, 0, "veltrixdb_search_errors_total", "kind", "vector", "reason", "incomplete")
	expect(t, reg, 1, "veltrixdb_search_errors_total", "kind", "text", "reason", "other")
	if n := seriesCount(t, reg, "veltrixdb_search_errors_total"); n != 9 {
		t.Fatalf("errors_total series = %d, want 9 (3 kinds x 3 reasons)", n)
	}

	// While a rebuild is pending the node reports not ready.
	se.BeginSearchRebuild()
	expect(t, reg, 0, "veltrixdb_search_ready")
}

func TestSearchMetrics_NamespaceSeriesCapped(t *testing.T) {
	se := testEngine(t)
	const n = maxNamespaceSeries + 6
	for i := 0; i < n; i++ {
		ns := fmt.Sprintf("ns%03d", i)
		if err := se.RegisterVectorNamespace(ns, 2); err != nil {
			t.Fatal(err)
		}
		if err := se.PutVector(ns, "v", []float32{1, 2}); err != nil {
			t.Fatal(err)
		}
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewSearchMetrics("n1", se, nil))
	if got := seriesCount(t, reg, "veltrixdb_search_namespace_vectors"); got != maxNamespaceSeries {
		t.Fatalf("namespace series = %d, want %d", got, maxNamespaceSeries)
	}
	expect(t, reg, n, "veltrixdb_search_vector_namespaces")
	expect(t, reg, n, "veltrixdb_search_vectors")
}

type fakeRaft struct{ st consensus.SnapshotStatus }

func (f fakeRaft) Term() uint64                             { return 9 }
func (f fakeRaft) IsLeader() bool                           { return true }
func (f fakeRaft) SnapshotStatus() consensus.SnapshotStatus { return f.st }

func TestRaftCollector(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewRaftCollector("n1", fakeRaft{consensus.SnapshotStatus{
		LastIncludedIndex: 100, CommitIndex: 150, LastApplied: 140, RetainedLog: 50,
		Captures: 3, Installs: 1, ChunksSent: 12, ChunksReceived: 4,
		LastCapture:     1500 * time.Millisecond,
		CaptureFailures: 2, SnapshotsSent: 5, TransferFailures: 6,
		InstallFailures: 7, RestoreFailures: 8,
	}}, 8192))
	expect(t, reg, 9, "veltrixdb_raft_term", "node", "n1")
	expect(t, reg, 1, "veltrixdb_raft_is_leader")
	expect(t, reg, 150, "veltrixdb_raft_commit_index")
	expect(t, reg, 140, "veltrixdb_raft_applied_index")
	expect(t, reg, 100, "veltrixdb_raft_snapshot_index")
	expect(t, reg, 50, "veltrixdb_raft_log_retained_entries")
	expect(t, reg, 8192, "veltrixdb_raft_snapshot_threshold_entries")
	expect(t, reg, 3, "veltrixdb_raft_snapshot_captures_total")
	expect(t, reg, 1, "veltrixdb_raft_snapshot_installs_total")
	expect(t, reg, 12, "veltrixdb_raft_snapshot_chunks_sent_total")
	expect(t, reg, 4, "veltrixdb_raft_snapshot_chunks_received_total")
	expect(t, reg, 1.5, "veltrixdb_raft_snapshot_last_capture_seconds")
	expect(t, reg, 2, "veltrixdb_raft_snapshot_capture_failures_total")
	expect(t, reg, 5, "veltrixdb_raft_snapshot_sent_total")
	expect(t, reg, 6, "veltrixdb_raft_snapshot_transfer_failures_total")
	expect(t, reg, 7, "veltrixdb_raft_snapshot_install_failures_total")
	expect(t, reg, 8, "veltrixdb_raft_snapshot_restore_failures_total")
}

// The replication recovery counters and gauges are exported.
func TestVeltrixCollector_ReplicationRecovery(t *testing.T) {
	se := testEngine(t)
	var rm replication.ReplicationMetrics
	rm.CatchUpGaps.Store(1)
	rm.FullResyncs.Store(2)
	rm.StatePersists.Store(3)
	rm.StatePersistErrors.Store(4)
	rm.ReplicasResyncPending.Store(5)
	rm.ReplicasDeleteGap.Store(6)
	reg := prometheus.NewPedanticRegistry()
	pm := cluster.NewPartitionMap(cluster.DefaultClusterConfig())
	fd := cluster.NewFailureDetector(pm, cluster.DefaultFailureDetectorConfig())
	reg.MustRegister(NewVeltrixCollector("n1", se, pm, fd, &rm))
	expect(t, reg, 1, "veltrixdb_replication_catchup_gaps_total", "node", "n1")
	expect(t, reg, 2, "veltrixdb_replication_full_resyncs_total")
	expect(t, reg, 3, "veltrixdb_replication_state_persists_total")
	expect(t, reg, 4, "veltrixdb_replication_state_persist_errors_total")
	expect(t, reg, 5, "veltrixdb_replication_replicas_resync_pending")
	expect(t, reg, 6, "veltrixdb_replication_replicas_delete_gap")
}

// The standard collectors must not clash with any VeltrixDB name, and must
// carry the node label.
func TestRegisterRuntimeCollectors(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	RegisterRuntimeCollectors(reg, "n1")
	reg.MustRegister(NewHardwareCollector("n1", nil))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, mf := range mfs {
		seen[mf.GetName()] = true
		if strings.HasPrefix(mf.GetName(), "go_") || strings.HasPrefix(mf.GetName(), "process_") {
			for _, m := range mf.GetMetric() {
				ok := false
				for _, l := range m.GetLabel() {
					ok = ok || (l.GetName() == "node" && l.GetValue() == "n1")
				}
				if !ok {
					t.Fatalf("%s has no node label", mf.GetName())
				}
			}
		}
	}
	for _, n := range []string{"go_goroutines", "go_memstats_heap_inuse_bytes", "process_start_time_seconds"} {
		if !seen[n] {
			t.Errorf("%s not gathered", n)
		}
	}
}
