package main

import (
	"github.com/VeltrixDB/veltrixdb/cluster"
	veltrixmetrics "github.com/VeltrixDB/veltrixdb/metrics"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// newMetricsRegistry builds the registry /metrics serves: the VeltrixDB
// collector, the hardware collector (veltrixdb_runtime_* / _process_* /
// _system_* / _disk_*, which the Grafana dashboards read), the standard
// go_* and process_* collectors, search metrics and, in raft mode, raft
// metrics. It also hands the search metrics to coord, so it must run before
// the client listeners start.
func newMetricsRegistry(nodeID string, engine *storage.StorageEngine, pm *cluster.PartitionMap,
	fd *cluster.FailureDetector, replMetrics *replication.ReplicationMetrics,
	coord *coordinator, raftSnapThreshold uint64, dataDirs []string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(veltrixmetrics.NewVeltrixCollector(nodeID, engine, pm, fd, replMetrics))
	reg.MustRegister(veltrixmetrics.NewHardwareCollector(nodeID, veltrixmetrics.DevicesFromDataDirs(dataDirs)))
	veltrixmetrics.RegisterRuntimeCollectors(reg, nodeID)

	sm := veltrixmetrics.NewSearchMetrics(nodeID, engine, partialSearches.Load)
	reg.MustRegister(sm)
	if coord != nil {
		coord.searchMetrics = sm
		if coord.raft != nil {
			reg.MustRegister(veltrixmetrics.NewRaftCollector(nodeID, coord.raft, raftSnapThreshold))
		}
	}
	return reg
}
