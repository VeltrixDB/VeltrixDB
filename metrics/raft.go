package metrics

import (
	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/prometheus/client_golang/prometheus"
)

// RaftSource is the part of *consensus.RaftNode the raft collector reads.
type RaftSource interface {
	Term() uint64
	IsLeader() bool
	SnapshotStatus() consensus.SnapshotStatus
}

// RaftCollector exports a raft node's position (term, commit and applied
// index, retained log) and its snapshot counters (RaftNode.SnapshotStatus).
// Registered only in --mode=raft. A scrape takes the raft lock twice, briefly.
type RaftCollector struct {
	nodeID    string
	rn        RaftSource
	threshold uint64

	term, isLeader, commitIndex, appliedIndex     *prometheus.Desc
	snapIndex, snapTerm, retainedLog, threshDesc  *prometheus.Desc
	captures, installs, chunksSent, chunksRecv    *prometheus.Desc
	legacySent, lastCapture, lastFence, lastBytes *prometheus.Desc
	captureFailures, snapshotsSent                *prometheus.Desc
	transferFailures, installFailures             *prometheus.Desc
	restoreFailures                               *prometheus.Desc
}

// NewRaftCollector builds the collector. snapshotThreshold is the configured
// --raft-snapshot-threshold (0 = automatic snapshots off).
func NewRaftCollector(nodeID string, rn RaftSource, snapshotThreshold uint64) *RaftCollector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("veltrixdb", "raft", name), help, []string{"node"}, nil)
	}
	return &RaftCollector{
		nodeID:    nodeID,
		rn:        rn,
		threshold: snapshotThreshold,

		term:         d("term", "Current raft term."),
		isLeader:     d("is_leader", "1 when this node is the raft leader, else 0."),
		commitIndex:  d("commit_index", "Highest log index known to be committed."),
		appliedIndex: d("applied_index", "Highest log index applied to the state machine."),
		snapIndex:    d("snapshot_index", "Last log index covered by this node's snapshot (lastIncludedIndex)."),
		snapTerm:     d("snapshot_term", "Term of the last log index covered by this node's snapshot."),
		retainedLog:  d("log_retained_entries", "Log entries retained after the snapshot. Compaction keeps this near or below the snapshot threshold; steady growth past it means snapshots are not completing."),
		threshDesc:   d("snapshot_threshold_entries", "Configured --raft-snapshot-threshold (retained entries that trigger a capture); 0 = automatic snapshots off."),
		captures:     d("snapshot_captures_total", "Snapshots this node captured."),
		installs:     d("snapshot_installs_total", "Snapshots this node installed from a leader (chunked or single-shot)."),
		chunksSent:   d("snapshot_chunks_sent_total", "InstallSnapshot chunks this node sent as leader."),
		chunksRecv:   d("snapshot_chunks_received_total", "InstallSnapshot chunks this node accepted as follower."),
		legacySent:   d("snapshot_legacy_sent_total", "Single-shot InstallSnapshot RPCs sent to followers that do not support chunks (v1.16 and older)."),
		lastCapture:  d("snapshot_last_capture_seconds", "Duration of the last snapshot capture."),
		lastFence:    d("snapshot_last_apply_fence_seconds", "Apply stall (fence) of the last snapshot capture."),
		lastBytes:    d("snapshot_last_bytes", "File size of the last captured snapshot in bytes."),

		captureFailures:  d("snapshot_capture_failures_total", "Snapshot captures that ended without a new snapshot (error, or aborted by an install or shutdown); retried with backoff."),
		snapshotsSent:    d("snapshot_sent_total", "Whole-snapshot transfers to a follower that completed (chunked: install confirmed; single-shot: accepted)."),
		transferFailures: d("snapshot_transfer_failures_total", "Snapshot transfers to a follower that ended without an install: send / read / follower install failure, impossible single-shot (over 256 MB), or aborted by a leader change."),
		installFailures:  d("snapshot_install_failures_total", "Snapshots received from a leader that this node did not install (verification, persist or install failure)."),
		restoreFailures:  d("snapshot_restore_failures_total", "Failed state-machine restore attempts of a snapshot install (retried; the state machine is not usable until one succeeds)."),
	}
}

// Describe implements prometheus.Collector.
func (c *RaftCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.term, c.isLeader, c.commitIndex, c.appliedIndex,
		c.snapIndex, c.snapTerm, c.retainedLog, c.threshDesc,
		c.captures, c.installs, c.chunksSent, c.chunksRecv,
		c.legacySent, c.lastCapture, c.lastFence, c.lastBytes,
		c.captureFailures, c.snapshotsSent, c.transferFailures,
		c.installFailures, c.restoreFailures,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *RaftCollector) Collect(ch chan<- prometheus.Metric) {
	g := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, c.nodeID)
	}
	cnt := func(d *prometheus.Desc, v uint64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), c.nodeID)
	}
	st := c.rn.SnapshotStatus()
	leader := 0.0
	if c.rn.IsLeader() {
		leader = 1
	}
	g(c.term, float64(c.rn.Term()))
	g(c.isLeader, leader)
	g(c.commitIndex, float64(st.CommitIndex))
	g(c.appliedIndex, float64(st.LastApplied))
	g(c.snapIndex, float64(st.LastIncludedIndex))
	g(c.snapTerm, float64(st.LastIncludedTerm))
	g(c.retainedLog, float64(st.RetainedLog))
	g(c.threshDesc, float64(c.threshold))
	cnt(c.captures, st.Captures)
	cnt(c.installs, st.Installs)
	cnt(c.chunksSent, st.ChunksSent)
	cnt(c.chunksRecv, st.ChunksReceived)
	cnt(c.legacySent, st.LegacySent)
	g(c.lastCapture, st.LastCapture.Seconds())
	g(c.lastFence, st.LastApplyFence.Seconds())
	g(c.lastBytes, float64(st.LastSnapshotBytes))
	cnt(c.captureFailures, st.CaptureFailures)
	cnt(c.snapshotsSent, st.SnapshotsSent)
	cnt(c.transferFailures, st.TransferFailures)
	cnt(c.installFailures, st.InstallFailures)
	cnt(c.restoreFailures, st.RestoreFailures)
}
