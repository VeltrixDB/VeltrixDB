package metrics

import (
	"fmt"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// VeltrixCollector implements prometheus.Collector and exposes all internal
// metrics from every layer: storage read/write/delete paths, cache, eviction,
// cluster node management, resharding, failure detection, and replication.
type VeltrixCollector struct {
	nodeID      string
	engine      *storage.StorageEngine
	pm          *cluster.PartitionMap
	fd          *cluster.FailureDetector
	replMetrics *replication.ReplicationMetrics // optional, may be nil

	// ── Storage: write path ───────────────────────────────────────────────────
	writes                 *prometheus.Desc
	writesLatencyNs        *prometheus.Desc
	walFlushes             *prometheus.Desc
	compactionRuns         *prometheus.Desc
	sstableCreations       *prometheus.Desc
	writeBackpressureEvents  *prometheus.Desc
	writeAdmissionThrottles  *prometheus.Desc // admission control throttle events

	// ── Storage: read path ────────────────────────────────────────────────────
	reads          *prometheus.Desc
	readsLatencyNs *prometheus.Desc
	bloomFalsePos  *prometheus.Desc

	// ── Storage: delete path ──────────────────────────────────────────────────
	deletes             *prometheus.Desc
	deletesLatencyNs    *prometheus.Desc
	defragRuns          *prometheus.Desc
	tombstonesCollected *prometheus.Desc

	// ── Cache ─────────────────────────────────────────────────────────────────
	cacheHits      *prometheus.Desc
	cacheMisses    *prometheus.Desc
	cacheHitRate   *prometheus.Desc
	cacheSizeBytes *prometheus.Desc
	cacheMaxBytes  *prometheus.Desc

	// ── Eviction ──────────────────────────────────────────────────────────────
	evictionsTotal      *prometheus.Desc // label: type={lirs,ttl,defrag}
	evictionLatencyNs   *prometheus.Desc

	// ── Index ─────────────────────────────────────────────────────────────────
	indexSize *prometheus.Desc

	// ── Cluster: node management + resharding ─────────────────────────────────
	nodeAdditions       *prometheus.Desc
	nodeRemovals        *prometheus.Desc
	failureDetections   *prometheus.Desc
	rebalances          *prometheus.Desc // resharding operations
	partitionMigrations *prometheus.Desc // partitions moved during reshard
	metaUpdateTs        *prometheus.Desc
	nodeCount           *prometheus.Desc
	clusterVersion      *prometheus.Desc

	// ── Failure detection ─────────────────────────────────────────────────────
	fdNodesDetected    *prometheus.Desc
	fdNodesRecovered   *prometheus.Desc
	fdFalsePositives   *prometheus.Desc
	fdRecoveryAttempts *prometheus.Desc
	fdRecoverySuccess  *prometheus.Desc
	fdRecoveryFailures *prometheus.Desc

	// ── VLog (WiscKey KV separation) ──────────────────────────────────────────
	vlogWrites          *prometheus.Desc // counter, per-node
	vlogReads           *prometheus.Desc // counter, per-node
	vlogGCRuns          *prometheus.Desc // counter, per-node
	vlogGCBytes         *prometheus.Desc // counter, bytes reclaimed by GC, per-node
	vlogGarbageRatio    *prometheus.Desc // gauge, per-disk (label: disk)
	vlogFileBytes       *prometheus.Desc // gauge, per-disk (label: disk)
	// GC diagnostic counters — expose why compactVLog is or is not making progress.
	vlogGCSkippedRatio  *prometheus.Desc // counter: exited because GCRatio < threshold
	vlogGCSkippedPaused *prometheus.Desc // counter: exited because GCPaused was true
	vlogGCSkippedEmpty  *prometheus.Desc // counter: exited because no candidates found
	vlogGCReadErrors    *prometheus.Desc // counter: ReadValue failures inside GC loop
	vlogGCCASFails      *prometheus.Desc // counter: CAS misses (concurrent Put won)
	vlogGCCandidates    *prometheus.Desc // counter: total candidates scanned
	vlogGCEmergencyRuns  *prometheus.Desc // counter: GC bypassed admission-control pause (garbage ≥ 65%)
	vlogBlkDiscardErrors *prometheus.Desc // counter: BLKDISCARD ioctl failures (missing SYS_RAWIO cap)

	// ── Replication ───────────────────────────────────────────────────────────
	replWrites            *prometheus.Desc
	replFailures          *prometheus.Desc
	replLagBytes          *prometheus.Desc
	replLagNs             *prometheus.Desc
	replConflicts         *prometheus.Desc
	replVectorClockUpdates *prometheus.Desc
	replAntiEntropyRuns   *prometheus.Desc

	// ── WAL I/O ───────────────────────────────────────────────────────────────
	walWriteBytes    *prometheus.Desc // counter: bytes written to WAL across all disks
	walBatchEntries  *prometheus.Desc // counter: WAL entries batched (/ walFlushes = avg batch)

	// ── VLog I/O (per-disk) ───────────────────────────────────────────────────
	vlogWriteBytesDisk *prometheus.Desc // counter: raw value bytes written (disk label)
	vlogReadBytesDisk  *prometheus.Desc // counter: raw value bytes read (disk label)

	// ── WriteBatcher ──────────────────────────────────────────────────────────
	writerBatcherDropped *prometheus.Desc // counter: sync fallbacks when channel saturated

	// ── Admission control (GC CAS backpressure) ──────────────────────────────
	gcCASThrottles *prometheus.Desc // counter: Put() calls delayed by GC CAS backpressure

	// ── Server: connections & network ────────────────────────────────────────
	activeConnections *prometheus.Desc // gauge: open TCP connections right now
	networkBytesIn    *prometheus.Desc // counter: bytes received from clients
	networkBytesOut   *prometheus.Desc // counter: bytes sent to clients

	// ── Pipeline coalescing ───────────────────────────────────────────────────
	multiPutBatches *prometheus.Desc // counter: MultiPut batch dispatches
	multiPutEntries *prometheus.Desc // counter: entries inside MultiPut batches
	multiGetBatches *prometheus.Desc // counter: MultiGet batch dispatches
	multiGetEntries *prometheus.Desc // counter: keys inside MultiGet batches

	// ── Namespaces ────────────────────────────────────────────────────────────
	namespaceKeys *prometheus.Desc // gauge: live key count per namespace (label: namespace)

	// ── Slow-disk detection ──────────────────────────────────────────────────
	vlogDiskWriteLatencyEWMA *prometheus.Desc // gauge, per-disk seconds
	vlogDiskReadLatencyEWMA  *prometheus.Desc // gauge, per-disk seconds
	vlogDiskSlow             *prometheus.Desc // gauge 0|1, per-disk

	// ── Bloom filter ────────────────────────────────────────────────────────
	bloomSkipped *prometheus.Desc // counter: negative Gets shortcut by the bloom

	// ── Scrubber ────────────────────────────────────────────────────────────
	scrubRecords    *prometheus.Desc // counter: VLog records inspected
	scrubCorruption *prometheus.Desc // counter: CRC32C/magic mismatches
	scrubBytes      *prometheus.Desc // counter: bytes read by the scrubber
	scrubReadErrors *prometheus.Desc // counter: pread errors during scrub

	// ── Atomic ops ──────────────────────────────────────────────────────────
	atomicOps *prometheus.Desc // counter: CAS+INCR+DECR+SETNX successful operations

	// Latency histograms — proper P50/P95/P99 distributions.
	// These implement prometheus.Collector; we delegate Describe/Collect to them
	// so they live inside the same custom registry as the rest of VeltrixDB metrics.
	writeLatencyHist  prometheus.Histogram
	readLatencyHist   prometheus.Histogram
	deleteLatencyHist prometheus.Histogram
}

// NewVeltrixCollector creates a collector that wraps every metric layer.
// nodeID is the local node identifier used as a Prometheus label.
// replMetrics may be nil if the replication engine is not yet wired.
func NewVeltrixCollector(
	nodeID string,
	engine *storage.StorageEngine,
	pm *cluster.PartitionMap,
	fd *cluster.FailureDetector,
	replMetrics *replication.ReplicationMetrics,
) *VeltrixCollector {
	const ns = "veltrixdb"

	label := []string{"node"}

	desc := func(subsystem, name, help string, extraLabels ...string) *prometheus.Desc {
		labels := append(label, extraLabels...)
		return prometheus.NewDesc(
			prometheus.BuildFQName(ns, subsystem, name),
			help, labels, nil,
		)
	}

	// ── Latency histograms ─────────────────────────────────────────────────────
	// Buckets chosen for NVMe targets:
	//   Read:  P99 < 5 ms cache-miss path (cache hits < 1 µs)
	//   Write: P99 < 20 ms (WAL 2ms window + VLog fdatasync 0.2ms + overhead)
	writeBuckets := []float64{
		0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
	}
	readBuckets := []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1,
	}
	writeHist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   ns,
		Subsystem:   "storage",
		Name:        "write_latency_seconds",
		Help:        "Write (PUT) end-to-end latency in seconds — WAL fdatasync + VLog append.",
		Buckets:     writeBuckets,
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	readHist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   ns,
		Subsystem:   "storage",
		Name:        "read_latency_seconds",
		Help:        "Read (GET) end-to-end latency in seconds — LIRS cache hit or VLog pread.",
		Buckets:     readBuckets,
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	deleteHist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   ns,
		Subsystem:   "storage",
		Name:        "delete_latency_seconds",
		Help:        "Delete (DEL) end-to-end latency in seconds — tombstone WAL write.",
		Buckets:     writeBuckets,
		ConstLabels: prometheus.Labels{"node": nodeID},
	})
	engine.SetLatencyObservers(writeHist.Observe, readHist.Observe, deleteHist.Observe)

	return &VeltrixCollector{
		nodeID:      nodeID,
		engine:      engine,
		pm:          pm,
		fd:          fd,
		replMetrics: replMetrics,

		writeLatencyHist:  writeHist,
		readLatencyHist:   readHist,
		deleteLatencyHist: deleteHist,

		// write path
		writes:                  desc("storage", "writes_total", "Total PUT operations completed."),
		writesLatencyNs:         desc("storage", "write_latency_nanoseconds", "Last observed write latency in nanoseconds."),
		walFlushes:              desc("storage", "wal_flushes_total", "Total WAL fdatasync flushes."),
		compactionRuns:          desc("storage", "compaction_runs_total", "Total memtable-to-SSTable compaction runs."),
		sstableCreations:        desc("storage", "sstable_creations_total", "Total SSTable files created."),
		writeBackpressureEvents:  desc("storage", "write_backpressure_events_total", "PUT operations delayed by back-pressure due to oversized dirty memtable."),
		writeAdmissionThrottles:  desc("storage", "write_admission_throttles_total", "PUT operations delayed by admission control (read P99 EWMA > 4ms)."),

		// read path
		reads:          desc("storage", "reads_total", "Total GET operations completed."),
		readsLatencyNs: desc("storage", "read_latency_nanoseconds", "Last observed read latency in nanoseconds."),
		bloomFalsePos:  desc("storage", "bloom_false_positives_total", "Bloom filter false positives causing unnecessary disk reads."),

		// delete path
		deletes:             desc("storage", "deletes_total", "Total DEL operations completed (tombstone written)."),
		deletesLatencyNs:    desc("storage", "delete_latency_nanoseconds", "Last observed delete latency in nanoseconds."),
		defragRuns:          desc("storage", "defrag_runs_total", "Total defragmenter (tombstone GC) compaction passes."),
		tombstonesCollected: desc("storage", "tombstones_collected_total", "Tombstones physically removed after GC grace period."),

		// cache
		cacheHits:      desc("cache", "hits_total", "LIRS cache hits."),
		cacheMisses:    desc("cache", "misses_total", "LIRS cache misses (fell through to index/disk)."),
		cacheHitRate:   desc("cache", "hit_rate", "Cache hit rate as a fraction [0,1]."),
		cacheSizeBytes: desc("cache", "size_bytes", "Current LIRS cache resident size in bytes."),
		cacheMaxBytes:  desc("cache", "max_size_bytes", "Configured LIRS cache capacity in bytes."),

		// eviction — single desc with a 'type' label covers all three eviction kinds
		evictionsTotal:    desc("cache", "evictions_total", "Cache evictions partitioned by eviction trigger.", "type"),
		evictionLatencyNs: desc("cache", "eviction_latency_nanoseconds", "Last observed eviction latency in nanoseconds."),

		// index
		indexSize: desc("storage", "index_size_keys", "Number of live keys in the Index Vault."),

		// cluster node management + resharding
		nodeAdditions:       desc("cluster", "node_additions_total", "Nodes added to the cluster."),
		nodeRemovals:        desc("cluster", "node_removals_total", "Nodes removed from the cluster."),
		failureDetections:   desc("cluster", "failure_detections_total", "Nodes marked FAILED by the partition map."),
		rebalances:          desc("cluster", "rebalances_total", "Partition rebalance (reshard) operations executed."),
		partitionMigrations: desc("cluster", "partition_migrations_total", "Partitions reassigned across nodes during resharding."),
		metaUpdateTs:        desc("cluster", "metadata_last_update_timestamp_seconds", "Unix timestamp of the last partition-map metadata update."),
		nodeCount:           desc("cluster", "nodes_total", "Current number of registered cluster nodes."),
		clusterVersion:      desc("cluster", "partition_map_version", "Monotonically increasing partition map version."),

		// failure detection
		fdNodesDetected:    desc("failure_detector", "nodes_failed_total", "Nodes detected as failed by the heartbeat monitor."),
		fdNodesRecovered:   desc("failure_detector", "nodes_recovered_total", "Nodes that recovered after being marked failed."),
		fdFalsePositives:   desc("failure_detector", "false_positives_total", "Suspected failures that turned out to be false alarms."),
		fdRecoveryAttempts: desc("failure_detector", "recovery_attempts_total", "Ping attempts made toward failed nodes."),
		fdRecoverySuccess:  desc("failure_detector", "recovery_success_total", "Recovery pings that succeeded."),
		fdRecoveryFailures: desc("failure_detector", "recovery_failures_total", "Recovery pings that permanently failed."),

		// vlog
		vlogWrites:          desc("vlog", "writes_total", "Total values appended to VLog files (KV-separation enabled)."),
		vlogReads:           desc("vlog", "reads_total", "Total values read from VLog files."),
		vlogGCRuns:          desc("vlog", "gc_runs_total", "Total VLog defragmentation (GC) passes that reclaimed ≥1 byte."),
		vlogGCBytes:         desc("vlog", "gc_bytes_total", "Total bytes reclaimed by VLog GC across all disks."),
		vlogGarbageRatio:    desc("vlog", "garbage_ratio", "Fraction of VLog bytes that are dead (garbage). Triggers GC when > VLogGCThreshold.", "disk"),
		vlogFileBytes:       desc("vlog", "file_bytes", "Current VLog file size in bytes per disk.", "disk"),
		vlogGCSkippedRatio:  desc("vlog", "gc_skipped_ratio_total", "Times compactVLog exited early: GCRatio was below the threshold."),
		vlogGCSkippedPaused: desc("vlog", "gc_skipped_paused_total", "Times compactVLog exited early: GCPaused was true (read latency EWMA above 4ms)."),
		vlogGCSkippedEmpty:  desc("vlog", "gc_skipped_empty_total", "Times compactVLog exited early: zero live candidates found below the GC horizon."),
		vlogGCReadErrors:    desc("vlog", "gc_read_errors_total", "ReadValue failures inside the GC candidate loop (skipped candidates)."),
		vlogGCCASFails:      desc("vlog", "gc_cas_fails_total", "CAS misses in GC: a concurrent Put updated the entry before GC could move it."),
		vlogGCCandidates:    desc("vlog", "gc_candidates_total", "Cumulative live VLog entries scanned as GC candidates across all disks."),
		vlogGCEmergencyRuns:  desc("vlog", "gc_emergency_runs_total", "Times compactVLog bypassed the admission-control GC-pause because garbage ratio crossed the emergency threshold (65%). A persistent non-zero rate signals write throughput exceeds sustainable GC bandwidth."),
		vlogBlkDiscardErrors: desc("vlog", "blkdiscard_errors_total", "BLKDISCARD ioctl failures in punchDeadHead. Non-zero means the container lacks CAP_SYS_RAWIO — NVMe TRIM is silently skipped and the raw VLog head will grow without reclaim. Fix: add SYS_RAWIO to securityContext.capabilities.add."),

		// replication
		replWrites:             desc("replication", "writes_total", "Write operations dispatched to replicas."),
		replFailures:           desc("replication", "failures_total", "Replication attempts that failed."),
		replLagBytes:           desc("replication", "lag_bytes", "Total replication lag across all replicas in bytes."),
		replLagNs:              desc("replication", "lag_nanoseconds", "Maximum replication lag across all replicas in nanoseconds."),
		replConflicts:          desc("replication", "conflict_resolutions_total", "Write conflicts resolved via vector-clock comparison."),
		replVectorClockUpdates: desc("replication", "vector_clock_updates_total", "Vector clock increments (causal ordering events)."),
		replAntiEntropyRuns:    desc("replication", "anti_entropy_runs_total", "Anti-entropy full-state sync rounds completed."),

		// wal i/o
		walWriteBytes:   desc("storage", "wal_write_bytes_total", "Bytes written to WAL files across all disks (includes WAL framing overhead)."),
		walBatchEntries: desc("storage", "wal_batch_entries_total", "Total WAL entries flushed. Divide by wal_flushes_total for average batch size."),

		// vlog i/o per disk
		vlogWriteBytesDisk: desc("vlog", "write_bytes_total", "Raw value bytes appended to VLog per disk (excludes header and 4K alignment padding).", "disk"),
		vlogReadBytesDisk:  desc("vlog", "read_bytes_total", "Raw value bytes returned from VLog reads per disk.", "disk"),

		// write batcher
		writerBatcherDropped: desc("storage", "write_batcher_dropped_total", "Put() calls that bypassed WriteBatcher and executed synchronously because the async channel was saturated."),

		// admission control
		gcCASThrottles: desc("storage", "gc_cas_throttles_total", "Put() calls delayed 1ms by GC CAS backpressure (>20% CAS fail rate in last GC batch)."),

		// server: connections + network
		activeConnections: desc("server", "active_connections", "Current number of open TCP client connections."),
		networkBytesIn:    desc("server", "network_bytes_in_total", "Bytes received from clients across all connections."),
		networkBytesOut:   desc("server", "network_bytes_out_total", "Bytes sent to clients across all connections."),

		// pipeline coalescing
		multiPutBatches: desc("server", "multi_put_batches_total", "Number of vectorized MultiPut batches dispatched via pipeline coalescing."),
		multiPutEntries: desc("server", "multi_put_entries_total", "Total PUT entries inside all vectorized MultiPut batches."),
		multiGetBatches: desc("server", "multi_get_batches_total", "Number of vectorized MultiGet batches dispatched via pipeline coalescing."),
		multiGetEntries: desc("server", "multi_get_entries_total", "Total GET keys inside all vectorized MultiGet batches."),

		// namespaces
		namespaceKeys: desc("storage", "namespace_keys", "Live key count per namespace. Only namespaces with at least one key appear.", "namespace"),

		// slow-disk detection
		vlogDiskWriteLatencyEWMA: desc("vlog", "disk_write_latency_ewma_seconds", "Per-disk VLog write latency EWMA (α=1/8) sampled every 32nd write.", "disk"),
		vlogDiskReadLatencyEWMA:  desc("vlog", "disk_read_latency_ewma_seconds", "Per-disk VLog read latency EWMA (α=1/8) sampled every 32nd read.", "disk"),
		vlogDiskSlow:             desc("vlog", "disk_slow", "1 when this disk's combined latency EWMA is ≥ 5× the cluster median, else 0. Persistent slow=1 indicates failing or contended NVMe — investigate before user-visible degradation.", "disk"),

		// bloom filter
		bloomSkipped: desc("storage", "bloom_filter_skipped_total", "Negative Get operations short-circuited by the per-shard Bloom filter (no shard lock taken, no map lookup).  A high value means the filter is paying for itself; combined with bloom_false_positives_total it gives the practical FP rate."),

		// scrubber
		scrubRecords:    desc("scrub", "records_total", "VLog records inspected by the background scrubber across all disks."),
		scrubCorruption: desc("scrub", "corruption_total", "CRC32C or magic-mismatch detections by the background scrubber. ANY non-zero rate indicates silent disk corruption — investigate immediately."),
		scrubBytes:      desc("scrub", "bytes_total", "VLog bytes read by the background scrubber."),
		scrubReadErrors: desc("scrub", "read_errors_total", "Transient pread failures during scrubbing."),

		// atomic ops
		atomicOps: desc("storage", "atomic_ops_total", "Successful atomic operations (CAS, INCR, DECR, SETNX) committed to durable storage."),
	}
}

// Describe sends all metric descriptors to Prometheus.
func (c *VeltrixCollector) Describe(ch chan<- *prometheus.Desc) {
	// Latency histograms self-describe.
	c.writeLatencyHist.Describe(ch)
	c.readLatencyHist.Describe(ch)
	c.deleteLatencyHist.Describe(ch)

	descs := []*prometheus.Desc{
		// write path
		c.writes, c.writesLatencyNs, c.walFlushes, c.compactionRuns, c.sstableCreations,
		c.writeBackpressureEvents, c.writeAdmissionThrottles,
		// read path
		c.reads, c.readsLatencyNs, c.bloomFalsePos,
		// delete path
		c.deletes, c.deletesLatencyNs, c.defragRuns, c.tombstonesCollected,
		// cache
		c.cacheHits, c.cacheMisses, c.cacheHitRate, c.cacheSizeBytes, c.cacheMaxBytes,
		c.evictionsTotal, c.evictionLatencyNs,
		// index
		c.indexSize,
		// vlog
		c.vlogWrites, c.vlogReads, c.vlogGCRuns, c.vlogGCBytes,
		c.vlogGarbageRatio, c.vlogFileBytes,
		c.vlogGCSkippedRatio, c.vlogGCSkippedPaused, c.vlogGCSkippedEmpty,
		c.vlogGCReadErrors, c.vlogGCCASFails, c.vlogGCCandidates,
		c.vlogGCEmergencyRuns, c.vlogBlkDiscardErrors,
		// cluster
		c.nodeAdditions, c.nodeRemovals, c.failureDetections,
		c.rebalances, c.partitionMigrations, c.metaUpdateTs, c.nodeCount, c.clusterVersion,
		// failure detector
		c.fdNodesDetected, c.fdNodesRecovered, c.fdFalsePositives,
		c.fdRecoveryAttempts, c.fdRecoverySuccess, c.fdRecoveryFailures,
		// replication
		c.replWrites, c.replFailures, c.replLagBytes, c.replLagNs,
		c.replConflicts, c.replVectorClockUpdates, c.replAntiEntropyRuns,
		// wal i/o
		c.walWriteBytes, c.walBatchEntries,
		// vlog i/o per disk
		c.vlogWriteBytesDisk, c.vlogReadBytesDisk,
		// write batcher + admission
		c.writerBatcherDropped, c.gcCASThrottles,
		// server
		c.activeConnections, c.networkBytesIn, c.networkBytesOut,
		c.multiPutBatches, c.multiPutEntries, c.multiGetBatches, c.multiGetEntries,
		// namespaces
		c.namespaceKeys,
		// slow-disk + bloom + scrubber + atomic
		c.vlogDiskWriteLatencyEWMA, c.vlogDiskReadLatencyEWMA, c.vlogDiskSlow,
		c.bloomSkipped,
		c.scrubRecords, c.scrubCorruption, c.scrubBytes, c.scrubReadErrors,
		c.atomicOps,
	}
	for _, d := range descs {
		ch <- d
	}
}

// Collect reads all atomic counters/gauges and emits current values.
func (c *VeltrixCollector) Collect(ch chan<- prometheus.Metric) {
	// Latency histograms self-collect (_bucket, _sum, _count series).
	c.writeLatencyHist.Collect(ch)
	c.readLatencyHist.Collect(ch)
	c.deleteLatencyHist.Collect(ch)

	nodeID := c.nodeID

	m := c.engine.GetMetrics()
	em := m.EvictionMetrics
	cs := c.engine.GetCacheStats()
	cm := c.pm.GetMetrics()
	fdm := c.fd.GetMetrics()

	counter := func(desc *prometheus.Desc, v uint64, extraLabels ...string) {
		lbls := append([]string{nodeID}, extraLabels...)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(v), lbls...)
	}
	gauge := func(desc *prometheus.Desc, v float64, extraLabels ...string) {
		lbls := append([]string{nodeID}, extraLabels...)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, lbls...)
	}

	// ── Storage: write path ───────────────────────────────────────────────────
	counter(c.writes, m.Writes.Load())
	gauge(c.writesLatencyNs, float64(m.WritesLatencyNs.Load()))
	counter(c.walFlushes, m.WALFlushes.Load())
	counter(c.compactionRuns, m.CompactionRuns.Load())
	counter(c.sstableCreations, m.SSTableCreations.Load())
	counter(c.writeBackpressureEvents, m.BackPressureEvents.Load())
	counter(c.writeAdmissionThrottles, m.Admission.WriteThrottleEvents.Load())

	// ── Storage: read path ────────────────────────────────────────────────────
	counter(c.reads, m.Reads.Load())
	gauge(c.readsLatencyNs, float64(m.ReadsLatencyNs.Load()))
	counter(c.bloomFalsePos, m.BloomFilterFalsePos.Load())

	// ── Storage: delete path ──────────────────────────────────────────────────
	counter(c.deletes, m.Deletes.Load())
	gauge(c.deletesLatencyNs, float64(m.DeletesLatencyNs.Load()))
	counter(c.defragRuns, m.DefragRuns.Load())
	counter(c.tombstonesCollected, m.TombstonesCollected.Load())

	// ── Cache ─────────────────────────────────────────────────────────────────
	counter(c.cacheHits, m.CacheHits.Load())
	counter(c.cacheMisses, m.CacheMisses.Load())
	gauge(c.cacheHitRate, cs.HitRate)
	gauge(c.cacheSizeBytes, float64(cs.CurrentSizeBytes))
	gauge(c.cacheMaxBytes, float64(cs.MaxSizeBytes))

	// ── Eviction (labelled by type) ───────────────────────────────────────────
	counter(c.evictionsTotal, em.LIRSEvictions.Load(), "lirs")
	counter(c.evictionsTotal, em.TTLEvictions.Load(), "ttl")
	counter(c.evictionsTotal, em.DefragEvictions.Load(), "defrag")
	gauge(c.evictionLatencyNs, float64(em.EvictionLatencyNs.Load()))

	// ── Index ─────────────────────────────────────────────────────────────────
	gauge(c.indexSize, float64(c.engine.GetIndexSize()))

	// ── VLog ─────────────────────────────────────────────────────────────────
	counter(c.vlogWrites, m.VLogWrites.Load())
	counter(c.vlogReads, m.VLogReads.Load())
	counter(c.vlogGCRuns, m.VLogGCRuns.Load())
	counter(c.vlogGCBytes, m.VLogGCBytes.Load())
	counter(c.vlogGCSkippedRatio, m.VLogGCSkippedRatio.Load())
	counter(c.vlogGCSkippedPaused, m.VLogGCSkippedPaused.Load())
	counter(c.vlogGCSkippedEmpty, m.VLogGCSkippedEmpty.Load())
	counter(c.vlogGCReadErrors, m.VLogGCReadErrors.Load())
	counter(c.vlogGCCASFails, m.VLogGCCASFails.Load())
	counter(c.vlogGCCandidates, m.VLogGCCandidates.Load())
	counter(c.vlogGCEmergencyRuns, m.VLogGCEmergencyRuns.Load())
	counter(c.vlogBlkDiscardErrors, m.VLogBlkDiscardErrors.Load())
	for _, vs := range c.engine.GetVLogStats() {
		diskLabel := fmt.Sprintf("%d", vs.DiskIdx)
		gauge(c.vlogGarbageRatio, vs.GarbageRatio, diskLabel)
		gauge(c.vlogFileBytes, float64(vs.FileBytes), diskLabel)
		gauge(c.vlogDiskWriteLatencyEWMA, vs.WriteLatencyEWMAs, diskLabel)
		gauge(c.vlogDiskReadLatencyEWMA, vs.ReadLatencyEWMAs, diskLabel)
		var slow float64
		if vs.Slow {
			slow = 1
		}
		gauge(c.vlogDiskSlow, slow, diskLabel)
	}

	// ── Cluster: node management + resharding ─────────────────────────────────
	counter(c.nodeAdditions, cm.NodeAdditions.Load())
	counter(c.nodeRemovals, cm.NodeRemovals.Load())
	counter(c.failureDetections, cm.FailureDetections.Load())
	counter(c.rebalances, cm.Rebalances.Load())
	counter(c.partitionMigrations, cm.PartitionMigrations.Load())
	gauge(c.metaUpdateTs, float64(cm.MetaUpdateTime.Load())/1e9) // ns → seconds
	gauge(c.nodeCount, float64(c.pm.GetNodeCount()))
	gauge(c.clusterVersion, float64(c.pm.GetVersion()))

	// ── Failure detection ─────────────────────────────────────────────────────
	counter(c.fdNodesDetected, fdm.NodesDetected.Load())
	counter(c.fdNodesRecovered, fdm.NodesRecovered.Load())
	counter(c.fdFalsePositives, fdm.FalsePositives.Load())
	counter(c.fdRecoveryAttempts, fdm.RecoveryAttempts.Load())
	counter(c.fdRecoverySuccess, fdm.RecoverySuccess.Load())
	counter(c.fdRecoveryFailures, fdm.RecoveryFailures.Load())

	// ── Replication ───────────────────────────────────────────────────────────
	if c.replMetrics != nil {
		counter(c.replWrites, c.replMetrics.ReplicatedWrites.Load())
		counter(c.replFailures, c.replMetrics.FailedReplications.Load())
		gauge(c.replLagBytes, float64(c.replMetrics.ReplicaLagBytes.Load()))
		gauge(c.replLagNs, float64(c.replMetrics.ReplicaLagNs.Load()))
		counter(c.replConflicts, c.replMetrics.ConflictResolutions.Load())
		counter(c.replVectorClockUpdates, c.replMetrics.VectorClockUpdates.Load())
		counter(c.replAntiEntropyRuns, c.replMetrics.AntiEntropyRuns.Load())
	}

	// ── WAL I/O ───────────────────────────────────────────────────────────────
	walBytes, walEntries := c.engine.GetWALTotals()
	counter(c.walWriteBytes, walBytes)
	counter(c.walBatchEntries, walEntries)

	// ── VLog I/O (per disk) ───────────────────────────────────────────────────
	for _, vs := range c.engine.GetVLogStats() {
		diskLabel := fmt.Sprintf("%d", vs.DiskIdx)
		counter(c.vlogWriteBytesDisk, uint64(vs.WriteBytes), diskLabel)
		counter(c.vlogReadBytesDisk, uint64(vs.ReadBytes), diskLabel)
	}

	// ── WriteBatcher + Admission control ─────────────────────────────────────
	counter(c.writerBatcherDropped, c.engine.GetBatcherDropped())
	counter(c.gcCASThrottles, m.Admission.GCCASThrottles.Load())

	// ── Server: connections + network + coalescing ────────────────────────────
	gauge(c.activeConnections, float64(m.ActiveConnections.Load()))
	counter(c.networkBytesIn, m.NetworkBytesIn.Load())
	counter(c.networkBytesOut, m.NetworkBytesOut.Load())
	counter(c.multiPutBatches, m.MultiPutBatches.Load())
	counter(c.multiPutEntries, m.MultiPutEntries.Load())
	counter(c.multiGetBatches, m.MultiGetBatches.Load())
	counter(c.multiGetEntries, m.MultiGetEntries.Load())

	// ── Namespaces (live keys per namespace) ──────────────────────────────────
	for _, ns := range c.engine.ListNamespaces() {
		gauge(c.namespaceKeys, float64(ns.KeyCount), ns.Namespace)
	}

	// ── Bloom filter + scrubber + atomic ops counters ─────────────────────────
	counter(c.bloomSkipped, m.BloomFilterSkipped.Load())
	counter(c.scrubRecords, m.ScrubRecords.Load())
	counter(c.scrubCorruption, m.ScrubCorruption.Load())
	counter(c.scrubBytes, m.ScrubBytes.Load())
	counter(c.scrubReadErrors, m.ScrubReadErrors.Load())
	counter(c.atomicOps, m.AtomicOps.Load())
}
