package main

// cluster_setup.go — builds the distributed layer (Raft or replication) and the
// coordinator that fronts the storage engine, for --mode=raft / replicated.
//
// Address convention (keeps the --peers flag to a single host:port per node):
// given a node's client storage address host:P, its sibling listeners are
// derived by fixed port offsets so a cluster started with matching client
// ports "just works":
//
//	replication server : host:(P+1)   (matches replication.AddReplica's port+1)
//	raft RPC server     : host:(P+2)
//	gossip listener     : host:(P+3)
//
// The local node may override any of these via --raft-addr / --repl-addr /
// --gossip-addr; peers always use the derived offsets.

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

const (
	offReplication = 1
	offRaft        = 2
	offGossip      = 3
	// offTransfer is +5, not +4: the integration harness (and several deploy
	// templates) put the metrics listener at clientPort+4.
	offTransfer = 5
)

// peerSpec is one entry parsed from --peers ("id@host:port,...").
type peerSpec struct {
	id         string
	clientAddr string // "host:port"
	host       string
	clientPort int
}

// parsePeers parses the --peers flag.  Entries whose id equals selfID are
// dropped so a shared flag value can be reused across all nodes.
func parsePeers(spec, selfID string) ([]peerSpec, error) {
	var out []peerSpec
	for _, raw := range strings.Split(spec, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		at := strings.Index(raw, "@")
		if at < 0 {
			return nil, fmt.Errorf("bad --peers entry %q (want id@host:port)", raw)
		}
		id := raw[:at]
		hostPort := raw[at+1:]
		host, portStr, err := net.SplitHostPort(hostPort)
		if err != nil {
			return nil, fmt.Errorf("bad --peers address %q: %w", hostPort, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("bad --peers port in %q: %w", hostPort, err)
		}
		if id == selfID {
			continue
		}
		out = append(out, peerSpec{id: id, clientAddr: hostPort, host: host, clientPort: port})
	}
	return out, nil
}

// deriveAddr returns host:(port+off) for a "host:port" address, preserving an
// empty host (so ":9000" → ":9002" binds on all interfaces).
func deriveAddr(addr string, off int) string {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	port, _ := strconv.Atoi(portStr)
	return net.JoinHostPort(host, strconv.Itoa(port+off))
}

// nodeSalt derives per-node high bits for request-id uniqueness.
func nodeSalt(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64() << 20
}

// clusterParams carries everything buildCoordinator needs.
type clusterParams struct {
	mode        deployMode
	nodeID      string
	clientAddr  string // this node's --addr
	raftAddr    string // override, "" → derived
	replAddr    string // override, "" → derived
	consistency replication.ConsistencyLevel
	replFactor  int
	dataDir     string
	peers       []peerSpec

	// inter-node TLS
	tlsCert  string
	tlsKey   string
	tlsCA    string
	mutual   bool
}

// buildCoordinator constructs the coordinator for raft/replicated mode and
// returns a cleanup func plus, for replicated mode, the replication metrics to
// wire into the Prometheus collector.
func buildCoordinator(p clusterParams) (*coordinator, func(), *replication.ReplicationMetrics, error) {
	switch p.mode {
	case modeRaft:
		return buildRaftCoordinator(p)
	case modeReplicated:
		return buildReplicatedCoordinator(p)
	default:
		return nil, func() {}, nil, fmt.Errorf("buildCoordinator: mode not distributed")
	}
}

func buildRaftCoordinator(p clusterParams) (*coordinator, func(), *replication.ReplicationMetrics, error) {
	// Transport peer map: nodeID → raft RPC address.
	transPeers := make(map[string]string, len(p.peers))
	peerIDs := make([]string, 0, len(p.peers))
	peerClientAddr := map[string]string{p.nodeID: p.clientAddr}
	for _, pr := range p.peers {
		transPeers[pr.id] = deriveAddr(pr.clientAddr, offRaft)
		peerIDs = append(peerIDs, pr.id)
		peerClientAddr[pr.id] = pr.clientAddr
	}

	tlsOpts := consensus.TLSOptions{
		Enabled:  p.tlsCert != "" && p.tlsKey != "",
		CertFile: p.tlsCert,
		KeyFile:  p.tlsKey,
		CAFile:   p.tlsCA,
	}

	transport, err := consensus.NewTCPTransportTLS(transPeers, tlsOpts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("raft transport: %w", err)
	}

	fsm := newRaftFSM(pCurrentEngine)

	raftDir := p.dataDir + "/raft"
	node, err := consensus.NewRaftNode(p.nodeID, peerIDs, raftDir, fsm, transport)
	if err != nil {
		_ = transport.Close()
		return nil, nil, nil, fmt.Errorf("raft node: %w", err)
	}

	raftListen := p.raftAddr
	if raftListen == "" {
		raftListen = deriveAddr(p.clientAddr, offRaft)
	}
	rpcSrv, err := consensus.NewRPCServerTLS(raftListen, node, tlsOpts)
	if err != nil {
		node.Stop()
		return nil, nil, nil, fmt.Errorf("raft rpc server: %w", err)
	}
	go rpcSrv.ListenAndServe()
	log.Printf("[raft] mode active  node=%s  rpc=%s  peers=%v", p.nodeID, raftListen, peerIDs)

	c := &coordinator{
		mode:           modeRaft,
		engine:         pCurrentEngine,
		localID:        p.nodeID,
		raft:           node,
		fsm:            fsm,
		nodeSalt:       nodeSalt(p.nodeID),
		peerClientAddr: peerClientAddr,
	}
	cleanup := func() {
		rpcSrv.Stop()
		node.Stop()
	}
	return c, cleanup, nil, nil
}

func buildReplicatedCoordinator(p clusterParams) (*coordinator, func(), *replication.ReplicationMetrics, error) {
	cfg := replication.DefaultReplicationConfig()
	cfg.ConsistencyLevel = p.consistency
	cfg.ReplicationFactor = p.replFactor
	if p.tlsCert != "" && p.tlsKey != "" {
		cfg.TLS = &replication.TransportTLSConfig{
			TLSEnabled:        true,
			CertFile:          p.tlsCert,
			KeyFile:           p.tlsKey,
			CAFile:            p.tlsCA,
			RequireClientCert: p.mutual,
		}
	}

	re := replication.NewReplicationEngine(p.nodeID, cfg)
	re.Start()

	// Apply received ops to the local engine.
	applyFn := func(op *replication.WriteOperation) error {
		if op.IsTombstone {
			return pCurrentEngine.Delete(op.Key)
		}
		if err := pCurrentEngine.Put(op.Key, op.Value, op.TTL); err != nil {
			return err
		}
		// Vector writes replicate as plain KV on the reserved "@vec/" prefix;
		// refresh this replica's in-RAM searchable index as well.
		if storage.IsVectorKey(op.Key) {
			if err := pCurrentEngine.LoadVectorBlob(op.Key, op.Value); err != nil {
				log.Printf("[repl] vector index refresh %q: %v", op.Key, err)
			}
		}
		return nil
	}
	replListen := p.replAddr
	if replListen == "" {
		replListen = deriveAddr(p.clientAddr, offReplication)
	}
	if err := re.StartReplicationServer(replListen, applyFn); err != nil {
		_ = re.Close()
		return nil, nil, nil, fmt.Errorf("replication server: %w", err)
	}

	for _, pr := range p.peers {
		if err := re.AddReplica(pr.id, pr.host, pr.clientPort); err != nil {
			log.Printf("[repl] add replica %s: %v", pr.id, err)
		}
	}
	log.Printf("[repl] mode active  node=%s  server=%s  consistency=%s  replicas=%d",
		p.nodeID, replListen, consistencyName(p.consistency), len(p.peers))

	c := &coordinator{
		mode:        modeReplicated,
		engine:      pCurrentEngine,
		localID:     p.nodeID,
		repl:        re,
		consistency: p.consistency,
		replFactor:  p.replFactor,
		replTimeout: int(cfg.ReplicationTimeout.Milliseconds()),
	}
	cleanup := func() { _ = re.Close() }
	return c, cleanup, re.GetMetrics(), nil
}

// pCurrentEngine is set by main() before buildCoordinator is called.  A package
// variable avoids threading the engine through several signatures purely for
// construction; it is written once at startup and read once.
var pCurrentEngine *storage.StorageEngine

func consistencyName(l replication.ConsistencyLevel) string {
	switch l {
	case replication.EventualConsistency:
		return "eventual"
	case replication.QuorumConsistency:
		return "quorum"
	case replication.StrongConsistency:
		return "strong"
	default:
		return "unknown"
	}
}

// parseConsistency maps the --consistency flag to a replication level.
func parseConsistency(s string) (replication.ConsistencyLevel, error) {
	switch s {
	case "", "eventual", "async":
		return replication.EventualConsistency, nil
	case "quorum":
		return replication.QuorumConsistency, nil
	case "strong", "sync":
		return replication.StrongConsistency, nil
	default:
		return replication.EventualConsistency, fmt.Errorf("unknown --consistency %q (want eventual|quorum|strong)", s)
	}
}
