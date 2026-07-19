package main

// admin_cluster.go — /admin/cluster topology endpoint.
//
// Exposes the live state of the distributed layer as JSON so operators and the
// cluster-aware client can discover roles, the Raft leader, partition
// ownership, the fencing epoch, and replication lag.  The client also uses this
// endpoint to bootstrap consistent-hash routing (see client/client.go).

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

// topologyResponse is the JSON returned by /admin/cluster.
type topologyResponse struct {
	NodeID      string          `json:"node_id"`
	Mode        string          `json:"mode"`
	Epoch       uint64          `json:"epoch"`
	Partitions  uint32          `json:"partition_count"`
	Consistency string          `json:"consistency,omitempty"`
	Raft        *raftStatus     `json:"raft,omitempty"`
	Replication []replicaStatus `json:"replication,omitempty"`
	Nodes       []nodeEntry     `json:"nodes"`
}

type raftStatus struct {
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
	IsLeader bool   `json:"is_leader"`
}

type replicaStatus struct {
	NodeID        string `json:"node_id"`
	State         string `json:"state"`
	LastAckSeqNum uint64 `json:"last_ack_seq"`
	LagBytes      uint64 `json:"lag_bytes"`
	LagNs         int64  `json:"lag_ns"`
}

type nodeEntry struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Port    int    `json:"port"`
	Rack    string `json:"rack,omitempty"`
	State   string `json:"state"`
}

func modeName(m deployMode) string {
	switch m {
	case modeRaft:
		return "raft"
	case modeReplicated:
		return "replicated"
	default:
		return "standalone"
	}
}

// buildTopology snapshots the live cluster state into a topologyResponse.
func buildTopology(coord *coordinator, pm *cluster.PartitionMap) topologyResponse {
	resp := topologyResponse{
		NodeID:     coord.localID,
		Mode:       modeName(coord.mode),
		Epoch:      pm.Epoch(),
		Partitions: pm.PartitionCount(),
	}
	if resp.NodeID == "" {
		for id := range pm.GetNodeStats() {
			resp.NodeID = id
			break
		}
	}
	for id, st := range pm.GetNodeStats() {
		resp.Nodes = append(resp.Nodes, nodeEntry{
			NodeID:  id,
			Address: st.Address,
			Port:    st.Port,
			Rack:    st.Rack,
			State:   st.State,
		})
	}
	switch coord.mode {
	case modeRaft:
		if coord.raft != nil {
			resp.Raft = &raftStatus{
				Role:     coord.raft.Role(),
				Term:     coord.raft.Term(),
				LeaderID: coord.raft.GetLeaderID(),
				IsLeader: coord.raft.IsLeader(),
			}
		}
	case modeReplicated:
		resp.Consistency = consistencyName(coord.consistency)
		if coord.repl != nil {
			for id, lag := range coord.repl.GetReplicaLag() {
				resp.Replication = append(resp.Replication, replicaStatus{
					NodeID:        id,
					State:         lag.State,
					LastAckSeqNum: lag.LastAckSeqNum,
					LagBytes:      lag.LagBytes,
					LagNs:         lag.LagNs,
				})
			}
		}
	}
	return resp
}

// topologyJSONLine returns the topology as a single-line JSON string, used by
// the TOPOLOGY text-protocol command so a cluster-aware client can discover the
// node set (and, in raft mode, the leader) over the storage port alone.
func (c *coordinator) topologyJSONLine() string {
	if c.pm == nil {
		return "{}"
	}
	b, err := json.Marshal(buildTopology(c, c.pm))
	if err != nil {
		return "{}"
	}
	return string(b)
}

// registerClusterAdmin mounts GET <prefix>/cluster (and /topology alias).
func registerClusterAdmin(mux *http.ServeMux, prefix string, coord *coordinator, pm *cluster.PartitionMap) {
	prefix = strings.TrimRight(prefix, "/")
	h := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(buildTopology(coord, pm))
	}
	mux.HandleFunc(prefix+"/cluster", h)
	mux.HandleFunc(prefix+"/topology", h)
}
