package main

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VeltrixDB/veltrixdb/adminapi"
	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/hardware"
	veltrixmetrics "github.com/VeltrixDB/veltrixdb/metrics"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/security"
	"github.com/VeltrixDB/veltrixdb/storage"
	"github.com/VeltrixDB/veltrixdb/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	binCmdPut  = 0x01
	binCmdGet  = 0x02
	binCmdDel  = 0x03
	binCmdPing = 0x04
	binCmdInfo = 0x05
	binCmdMPut = 0x06 // vectorized multi-put  (batch frame)
	binCmdMGet = 0x07 // vectorized multi-get  (batch frame)
	binCmdAuth = 0x09 // AUTH: [1B 0x09][2B userLen][4B passLen][user][pass]

	// Namespace commands (0x0A–0x0F).
	// All NS frames use a 9-byte header: [1B cmd][2B nsLen LE][2B keyLen LE][4B aux LE]
	// NSPUT uses aux = TTL (signed int32); NSSCAN uses aux = limit; others aux = 0.
	// NSPUT additionally has a 4-byte TTL prefix making its effective header 13 bytes:
	// [1B cmd][2B nsLen LE][2B keyLen LE][4B valLen LE][4B ttl LE signed]
	binCmdNSPut  = 0x0A // NSPUT:  13-byte header + ns + key + value
	binCmdNSGet  = 0x0B // NSGET:   9-byte header + ns + key
	binCmdNSDel  = 0x0C // NSDEL:   9-byte header + ns + key
	binCmdNSDrop = 0x0D // NSDROP:  9-byte header + ns  (drops all keys in namespace)
	binCmdNSScan = 0x0E // NSSCAN:  9-byte header + ns + prefix  (aux=limit)
	binCmdNSList = 0x0F // NSLIST:  9-byte header (no body)

	// Hash field commands (per-field TTL, 0x10–0x17).
	binCmdHSet    = 0x10 // HSET:    13-byte header + key + field + value
	binCmdHGet    = 0x11 // HGET:     9-byte header + key + field
	binCmdHDel    = 0x12 // HDEL:     9-byte header + key + field
	binCmdHGetAll = 0x13 // HGETALL:  7-byte header + key
	binCmdHKeys   = 0x14 // HKEYS:    7-byte header + key
	binCmdHLen    = 0x15 // HLEN:     7-byte header + key
	binCmdHExpire = 0x16 // HEXPIRE:  9-byte header + key + field
	binCmdHTTL    = 0x17 // HTTL:     9-byte header + key + field

	// Atomic ops (0x18–0x1B).
	// CAS:   [1B 0x18][2B keyLen][4B expectedLen] + [4B newLen][4B ttl] + key + expected + new
	// INCR:  [1B 0x19][2B keyLen][4B 0]            + [8B delta int64] + [4B ttl] + key
	// DECR:  [1B 0x1A][2B keyLen][4B 0]            + [8B delta int64] + [4B ttl] + key
	// SETNX: [1B 0x1B][2B keyLen][4B valLen]       + [4B ttl] + key + value
	binCmdCAS   = 0x18
	binCmdIncr  = 0x19
	binCmdDecr  = 0x1A
	binCmdSetNX = 0x1B

	// Range scans / transactions / indexes / vectors / query (0x20–0x29).
	// Frame layouts are documented on their handlers in ext_ops.go.
	binCmdRange     = 0x20 // RANGE:    [1B][2B startLen][4B limit] + [2B endLen][1B flags] + start + end
	binCmdScanCur   = 0x21 // SCANCUR:  [1B][2B cursorLen][4B limit] + cursor
	binCmdTxn       = 0x22 // TXN:      [1B][2B 0][4B opCount] + ops (one-shot optimistic transaction)
	binCmdIdxCreate = 0x23 // IDXCREATE:[1B][2B nameLen][4B fieldLen] + name + field
	binCmdIdxDrop   = 0x24 // IDXDROP:  [1B][2B nameLen][4B 0] + name
	binCmdIdxQuery  = 0x25 // IDXQUERY: [1B][2B nameLen][4B limit] + [2B valueLen] + name + value
	binCmdVSet      = 0x26 // VSET:     [1B][2B keyLen][4B dim] + key + dim×4B float32 LE
	binCmdVSearch   = 0x27 // VSEARCH:  [1B][2B k][4B dim] + dim×4B float32 LE
	binCmdQuery     = 0x28 // QUERY:    [1B][2B nsLen][4B limit] + [2B fieldLen][2B opLen][2B valLen] + ns+field+op+value
	binCmdGetVer    = 0x29 // GETVER:   [1B][2B keyLen][4B 0] + key → 8B version LE (0 = absent)

	binStatusOK       = 0x00
	binStatusErr      = 0x01
	binStatusNotFound = 0x02
	binStatusExists   = 0x03 // SETNX: key already existed
	binStatusMismatch = 0x04 // CAS: expected value did not match current
	binStatusConflict = 0x05 // TXN: optimistic version check failed — retry

	// maxPooledPayload caps what gets returned to binPayloadPool.
	// Requests above this threshold get GC'd normally instead of inflating RSS.
	maxPooledPayload = 64 << 10 // 64 KB

	// maxBatchCount caps MPUT / MGET entry counts to prevent memory exhaustion.
	maxBatchCount = 100_000

	// maxPipelineBatch caps opportunistic PUT/GET coalescing per pipeline pass.
	// 256 entries × avg 200 B = ~50 KB per implicit batch — well within a
	// single TCP segment delivered by the kernel in one recv() call.
	maxPipelineBatch = 256
)

// binPayloadPool reuses []byte slices for reading the [key || value] payload in
// each binary-protocol frame.  engine.Put copies value bytes into WAL + segment
// before returning, so the pooled slice is safe to reclaim immediately after Put.
var binPayloadPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// mputRespPool reuses the per-batch response byte slice [0x00][4B count][N × 1B].
var mputRespPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

func main() {
	addr := flag.String("addr", ":9000", "TCP address to listen on")
	metricsAddr := flag.String("metrics-addr", ":2112", "HTTP address for Prometheus /metrics and /healthz")
	adminToken := flag.String("admin-token", os.Getenv("VELTRIX_ADMIN_TOKEN"), "Bearer token required for /admin/* endpoints (env VELTRIX_ADMIN_TOKEN).\n\tWhen empty, /admin/* only accepts loopback connections; /metrics, /healthz and /readyz are always unauthenticated.")
	dataDir := flag.String("data", "./veltrixdb-data", "Single data directory (WAL + segments). Ignored when --data-dirs is set.")
	dataDirs := flag.String("data-dirs", "", "Comma-separated NVMe mount paths, one per disk.\n\tExample: --data-dirs=/mnt/nvme0,/mnt/nvme1,...,/mnt/nvme7\n\tEach disk gets its own segment writer and compaction goroutine.\n\tWAL is placed on the first disk. Overrides --data when set.")
	rawVlogs := flag.String("raw-vlogs", "", "(Linux) Comma-separated raw NVMe block-device paths for VLog backing, paired index-by-index with --data-dirs.\n"+
		"\tExample: --raw-vlogs=/dev/nvme0n1,/dev/nvme1n1,...,/dev/nvme7n1\n"+
		"\tBypasses XFS for VLog I/O. WAL and segment files still live on --data-dirs.\n"+
		"\tRequires CAP_SYS_RAWIO or root. First open writes a 4 KB superblock at offset 0 of each device.\n"+
		"\tNumber of entries must equal the number of --data-dirs entries.")
	nodeID := flag.String("node", "node-1", "Unique node ID in the cluster")
	cacheMB := flag.Uint("cache", 256, "LIRS cache size in MB (ignored when --auto-tune is set)")
	gcThreshold := flag.Float64("gc-threshold", 0.30,
		"VLog dead-space ratio that triggers compaction (0.0–1.0).\n\tLower = more frequent but smaller GC passes. Default 0.30.")
	walWindowMs := flag.Int("wal-flush-window-ms", 15,
		"WAL group-commit flush window in milliseconds.\n"+
			"\t15ms (default): ~200 entries/batch at 100K writes/s; matches WriteBatcher window.\n"+
			"\t5ms: lower latency but fewer entries/fdatasync; use for latency-sensitive workloads.\n"+
			"\tMust match --vlog-flush-window-ms (Invariant 20).")
	vlogWindowMs := flag.Int("vlog-flush-window-ms", 15,
		"VLog fdatasync group-commit window in milliseconds.\n"+
			"\tMust equal --wal-flush-window-ms. Concurrent WAL+VLog race; P99 = max(both).")
	walMaxBatch := flag.Int("wal-max-batch", 4096,
		"Maximum WAL entries per group-commit flush before the timer fires early.\n"+
			"\tRaise if write rate exceeds window×max_batch (e.g. 100K/s × 0.005s = 500 → 4096 is headroom).")
	seeds := flag.String("seeds", "", "Comma-separated peer seed nodes: nodeID=host:port (skip self)")
	autoTune := flag.Bool("auto-tune", false,
		"Auto-detect hardware and self-tune config (80% RAM rule, NVMe scheduler, hugepages, cpu governor).\n\tRequires root or CAP_SYS_ADMIN on Linux for sysctl / rlimit changes.")
	readHeavy := flag.Bool("read-heavy", false,
		"Use the read-heavy preset: 400 GB cache, LIRRatio=0.95, longer GC interval (300 s), 60 s TTL scan.\n"+
			"\tTuned for P99 < 5 ms at 2 M+ ops/sec on n2-highmem-64. Overrides DefaultStorageConfig but not --auto-tune or per-flag overrides.")

	// TLS flags
	tlsCert := flag.String("tls-cert", "", "Path to TLS server certificate PEM file. Enables TLS when set together with --tls-key.")
	tlsKey := flag.String("tls-key", "", "Path to TLS server private key PEM file.")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS client verification. When set, clients must present a cert signed by this CA.")
	tlsAddr := flag.String("tls-addr", ":9443", "TCP address for the TLS listener (separate from --addr plain-text port). Only active when --tls-cert and --tls-key are set.")

	// Auth flags
	authConfig := flag.String("auth-config", "", "Path to RBAC auth config JSON (users/roles). When set, AUTH is required before any operation.")

	// ── Distributed / cluster flags ──────────────────────────────────────────
	mode := flag.String("mode", "standalone",
		"Deployment mode: standalone|raft|replicated.\n"+
			"\tstandalone (default): single-node, every request served from local state (backward compatible).\n"+
			"\traft: writes go through a Raft log (quorum-committed, linearizable writes; non-leaders redirect).\n"+
			"\treplicated: primary-copy replication with per-request consistency (see --consistency).")
	nodeIDFlag := flag.String("node-id", "", "Node ID for the cluster (overrides --node when set).")
	peersFlag := flag.String("peers", "", "Comma-separated cluster peers: id@host:port,... (host:port is the peer's client --addr).")
	rackID := flag.String("rack-id", "", "Failure domain of this node (rack / cloud zone, e.g. asia-south1-a).\n\tReplica placement never puts two copies of a partition in the same rack unless the topology forces it.\n\tPropagates to peers via gossip; empty = unknown rack.")
	raftAddrFlag := flag.String("raft-addr", "", "Raft RPC listen address for this node. Empty = derive host:(clientPort+2).")
	replAddrFlag := flag.String("repl-addr", "", "Replication server listen address. Empty = derive host:(clientPort+1).")
	gossipAddrFlag := flag.String("gossip-addr", "", "Gossip listener address. Empty = derive host:(clientPort+3).")
	transferAddrFlag := flag.String("transfer-addr", "", "Partition-transfer (rebalance) HTTP listen address. Empty = derive host:(clientPort+5).")
	linReadsFlag := flag.Bool("linearizable-reads", false,
		"Raft mode: serve GET through the ReadIndex fence (quorum-confirmed, never stale).\n"+
			"\tCosts one heartbeat round-trip per read; followers redirect to the leader.")
	autoRebalance := flag.Bool("auto-rebalance", true,
		"In distributed modes, automatically rebalance the partition ring and migrate\n"+
			"\tkeys to their new owners when nodes join, leave, or fail.")
	consistencyFlag := flag.String("consistency", "eventual",
		"Replicated-mode write consistency: eventual|quorum|strong.\n"+
			"\teventual: ACK after local write (async replication).\n"+
			"\tquorum:   ACK after a majority of replicas apply the write.\n"+
			"\tstrong:   ACK after ALL replicas apply the write.")
	clusterTLSCert := flag.String("cluster-tls-cert", "", "PEM cert for inter-node (Raft/replication) TLS.")
	clusterTLSKey := flag.String("cluster-tls-key", "", "PEM key for inter-node (Raft/replication) TLS.")
	clusterTLSCA := flag.String("cluster-tls-ca", "", "PEM CA bundle for inter-node TLS verification.")
	clusterMTLS := flag.Bool("cluster-mtls", false, "Require and verify peer client certificates (mutual TLS) for inter-node traffic.")

	// Encryption / audit / scrub flags
	encryptAtRest := flag.Bool("encrypt-at-rest", false,
		"Enable AES-256-GCM encryption of values stored in VLog.\n"+
			"\tKey is loaded from VELTRIXDB_ENCRYPTION_KEY (base64) or --encryption-key-path.\n"+
			"\tStartup fails if the key is missing or not 32 bytes.")
	encryptKeyPath := flag.String("encryption-key-path", "",
		"Path to a file containing the 32-byte encryption key (raw or base64).\n"+
			"\tIgnored when VELTRIXDB_ENCRYPTION_KEY env var is set.")
	auditLog := flag.String("audit-log", "",
		"Path to append-only audit log (JSONL). Empty = disabled.\n"+
			"\tCovers PUT, DEL, atomic ops; never includes value bytes.")
	scrubMBs := flag.Int("scrub-mb-per-sec", 50,
		"VLog scrubber bandwidth cap per disk. 0 = disabled.")

	// WAL archiver (point-in-time recovery) flags
	archiveDir := flag.String("archive-dir", "",
		"Root directory for continuous WAL archiving (PITR). Empty = disabled.\n"+
			"\tA background archiver copies newly durable WAL entries into <dir>/disk<N>/.")
	archiveIntervalMs := flag.Int("archive-interval-ms", 1000,
		"How often the WAL archiver polls for new durable entries (milliseconds).")
	archiveMaxAgeSec := flag.Int64("archive-max-age-sec", 0,
		"Prune archived segments older than this many seconds. 0 = keep forever.")
	archiveMaxBytes := flag.Int64("archive-max-bytes", 0,
		"Prune oldest archived segments while total size exceeds this. 0 = unlimited.")

	flag.Parse()

	// --node-id overrides the legacy --node flag when set.
	if *nodeIDFlag != "" {
		*nodeID = *nodeIDFlag
	}

	// Parse the deployment mode and (for replicated mode) consistency level up
	// front so misconfiguration fails fast, before touching disk.
	deploy, err := parseMode(*mode)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	consistency, err := parseConsistency(*consistencyFlag)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Resolve disk paths: --data-dirs wins; fall back to single --data dir.
	var diskPaths []string
	if *dataDirs != "" {
		for _, p := range strings.Split(*dataDirs, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				diskPaths = append(diskPaths, p)
			}
		}
	}

	// Resolve raw VLog devices (optional, Linux-only).
	var rawVlogDevices []string
	if *rawVlogs != "" {
		for _, p := range strings.Split(*rawVlogs, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				rawVlogDevices = append(rawVlogDevices, p)
			}
		}
		if len(rawVlogDevices) != len(diskPaths) {
			log.Fatalf("--raw-vlogs (%d) must equal --data-dirs (%d) — pair index by index",
				len(rawVlogDevices), len(diskPaths))
		}
		log.Printf("[storage] raw VLog mode  %d devices  WAL still on --data-dirs", len(rawVlogDevices))
	}

	printBanner(*addr, *nodeID, *dataDir, diskPaths)

	// Tracing: minimal OTel-compatible. Slow ops (≥ 50 ms) auto-logged with
	// trace ID; recent spans queryable at /traces. Sampler set to 1% to keep
	// overhead negligible at multi-M-ops/sec — slow spans always recorded
	// regardless of sampler.
	tracing.Configure(tracing.Configuration{
		ServiceName:    "veltrixdb",
		ServiceVersion: "1.0.0",
		Sampler:        tracing.RateSampler(0.01),
		SlowThreshold:  50 * time.Millisecond,
		Retain:         2048,
	})

	// ── Storage engine ────────────────────────────────────────────────────
	var cfg *storage.StorageConfig

	if *autoTune {
		profile, err := hardware.Detect(diskPaths)
		if err != nil {
			log.Printf("[hardware] detection failed: %v  (falling back to defaults)", err)
			cfg = storage.DefaultStorageConfig()
			cfg.DataDirPath = *dataDir
			cfg.DataDirPaths = diskPaths
			cfg.CacheMaxSizeMB = uint32(*cacheMB)
			cfg.NumShards = 1024
		} else {
			log.Printf("[hardware] detected  RAM=%dMB  CPUs=%d  disks=%d",
				profile.TotalRAMMB, profile.CPUCores, len(profile.Disks))
			for _, d := range profile.Disks {
				log.Printf("[hardware]   %s → %s", d.Path, d.Kind)
			}

			cfg = hardware.AutoConfig(profile, diskPaths)
			if len(diskPaths) == 0 {
				cfg.DataDirPath = *dataDir
			}

			log.Printf("[hardware] auto-config  cache=%dMB  memtable=%dMB  shards=%d  compaction-threads=%d  sstable-max=%dMB",
				cfg.CacheMaxSizeMB, cfg.MaxMemorySizeMB, cfg.NumShards,
				cfg.CompactionThreads, cfg.SSTableMaxSizeMB)

			// Apply OS-level tuning; log warnings for non-fatal errors.
			for _, err := range hardware.SysTune(profile) {
				log.Printf("[hardware] sys-tune warning: %v", err)
			}
		}
	} else if *readHeavy {
		cfg = storage.ReadHeavyConfig()
		cfg.DataDirPath = *dataDir
		cfg.DataDirPaths = diskPaths
		// --cache CLI flag still wins over the preset's 400 GB default; if the
		// user did not pass --cache (kept the 256 default) we trust the preset.
		if *cacheMB != 256 {
			cfg.CacheMaxSizeMB = uint32(*cacheMB)
		}
		cfg.NumShards = 1024
		log.Printf("[config] read-heavy preset enabled  cache=%dMB  LIRRatio=%.2f  defrag=%s  ttl-scan=%s",
			cfg.CacheMaxSizeMB, cfg.LIRRatio, cfg.DefragInterval, cfg.TTLCheckInterval)
	} else {
		cfg = storage.DefaultStorageConfig()
		cfg.DataDirPath = *dataDir
		cfg.DataDirPaths = diskPaths
		cfg.CacheMaxSizeMB = uint32(*cacheMB)
		cfg.NumShards = 1024
	}

	// Apply post-preset overrides for newly-introduced config knobs so they
	// take effect regardless of which branch built cfg above.
	cfg.RawVLogDevices = rawVlogDevices
	cfg.WALFlushWindowMs = *walWindowMs
	cfg.VLogFlushWindowMs = *vlogWindowMs
	cfg.WALMaxBatchEntries = *walMaxBatch
	cfg.DefragThreshold = *gcThreshold
	cfg.EncryptionEnabled = *encryptAtRest
	cfg.EncryptionKeyPath = *encryptKeyPath
	cfg.AuditLogPath = *auditLog
	cfg.ScrubMBPerSec = *scrubMBs
	if *scrubMBs <= 0 {
		cfg.ScrubEnabled = false
	}
	cfg.ArchiveDir = *archiveDir
	cfg.ArchiveIntervalMs = *archiveIntervalMs
	cfg.MaxArchiveAgeSec = *archiveMaxAgeSec
	cfg.MaxArchiveBytes = *archiveMaxBytes

	// Apply CLI overrides that work regardless of auto-tune mode.
	cfg.RawVLogDevices = rawVlogDevices
	cfg.DefragThreshold = *gcThreshold
	cfg.WALFlushWindowMs = *walWindowMs
	cfg.VLogFlushWindowMs = *vlogWindowMs
	cfg.WALMaxBatchEntries = *walMaxBatch

	// Start health server before engine init so the liveness probe never
	// times out during slow startup (WAL replay, VLog device open, etc.).
	// /healthz always returns 200; /readyz returns 503 until engineReady is
	// set. Full metrics/admin handlers are registered on the same mux after
	// engine init — http.ServeMux is safe to extend while serving.
	var engineReady atomic.Bool
	var engineForHealth atomic.Value // *storage.StorageEngine, set after init
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	healthMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !engineReady.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, "initializing")
			return
		}
		// A tripped disk breaker degrades the node: report 503 so
		// orchestrators drain traffic while reads keep being served.
		if e, ok := engineForHealth.Load().(*storage.StorageEngine); ok && e.Degraded() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "degraded: disks %v failed\n", e.FailedDisks())
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ready")
	})
	go func() {
		if err := http.ListenAndServe(*metricsAddr, healthMux); err != nil {
			log.Printf("[metrics] server error: %v", err)
		}
	}()

	engine, err := storage.NewStorageEngine(cfg)
	if err != nil {
		log.Fatalf("storage init: %v", err)
	}
	defer engine.Close()

	// WAL archiver (PITR): no-op returning (nil, nil) when --archive-dir is
	// unset. Deferred Stop() runs BEFORE the deferred engine.Close() above
	// (LIFO), so the archiver detaches from live WALs before they close.
	walArchiver, err := storage.StartWALArchiver(engine)
	if err != nil {
		log.Fatalf("wal archiver: %v", err)
	}
	if walArchiver != nil {
		defer walArchiver.Stop()
	}

	// Rebuild the in-RAM vector indexes from persisted "@vec/..." keys once
	// the background WAL replay has finished. VSET/VSEARCH work immediately;
	// vectors written before the restart become searchable when this completes.
	go func() {
		<-engine.ReplayDone
		if n, rerr := engine.RebuildVectorIndexes(); rerr != nil {
			log.Printf("[vector] rebuild failed: %v", rerr)
		} else if n > 0 {
			log.Printf("[vector] rebuilt %d persisted vectors", n)
		}
	}()

	if len(diskPaths) > 0 {
		log.Printf("[storage] engine started  disks=%d  shards-per-disk=%d  cache=%dMB",
			len(diskPaths), 1024/len(diskPaths), cfg.CacheMaxSizeMB)
		for i, p := range diskPaths {
			log.Printf("[storage]   disk[%d] → %s", i, p)
		}
	} else {
		log.Printf("[storage] engine started  disks=1  cache=%dMB  data=%s", cfg.CacheMaxSizeMB, cfg.DataDirPath)
	}

	// Cluster layer
	clusterCfg := cluster.DefaultClusterConfig()
	pm := cluster.NewPartitionMap(clusterCfg)
	if err := pm.AddNode(*nodeID, listenHost(*addr), listenPort(*addr)); err != nil {
		log.Fatalf("partition map: %v", err)
	}
	if *rackID != "" {
		_ = pm.SetNodeRack(*nodeID, *rackID) // node was just added; cannot fail
		log.Printf("[cluster] rack-aware placement enabled  rack=%s (peers learn it via gossip)", *rackID)
	}

	// Seed peer nodes for cluster bootstrap
	if *seeds != "" {
		for _, seed := range strings.Split(*seeds, ",") {
			seed = strings.TrimSpace(seed)
			if seed == "" {
				continue
			}
			eqIdx := strings.Index(seed, "=")
			if eqIdx < 0 {
				log.Printf("[cluster] invalid seed %q (expected nodeID=host:port)", seed)
				continue
			}
			peerID := seed[:eqIdx]
			if peerID == *nodeID {
				continue // skip self
			}
			hostPort := seed[eqIdx+1:]
			host, portStr, err := net.SplitHostPort(hostPort)
			if err != nil {
				log.Printf("[cluster] bad seed address %q: %v", hostPort, err)
				continue
			}
			port, _ := strconv.Atoi(portStr)
			if err := pm.AddNode(peerID, host, port); err != nil {
				log.Printf("[cluster] register peer %s: %v", peerID, err)
			} else {
				log.Printf("[cluster] registered peer %s at %s:%d", peerID, host, port)
			}
		}
	}

	// Parse --peers (distributed modes) and register each peer in the partition
	// map so the consistent-hash ring and admin topology reflect the full cluster.
	peers, perr := parsePeers(*peersFlag, *nodeID)
	if perr != nil {
		log.Fatalf("config: %v", perr)
	}
	for _, pr := range peers {
		if err := pm.AddNode(pr.id, pr.host, pr.clientPort); err != nil {
			log.Printf("[cluster] register peer %s: %v", pr.id, err)
		} else {
			log.Printf("[cluster] registered peer %s at %s", pr.id, pr.clientAddr)
		}
	}

	fdCfg := cluster.DefaultFailureDetectorConfig()
	fd := cluster.NewFailureDetector(pm, fdCfg)
	fd.SetLocalNode(*nodeID) // exempt self from heartbeat timeout checks
	fd.Start()
	defer fd.Close()

	// Gossip: standalone uses the listener-less protocol (unchanged); the
	// distributed modes bind a real gossip listener so peers exchange liveness
	// and topology digests.
	var gossip *cluster.GossipProtocol
	if deploy == modeStandalone {
		gossip = cluster.NewGossipProtocol(*nodeID, pm, fd)
		gossip.Start()
	} else {
		gAddr := *gossipAddrFlag
		if gAddr == "" {
			gAddr = deriveAddr(*addr, offGossip)
		}
		gossip = cluster.NewGossipProtocolWithTransport(*nodeID, pm, fd,
			&cluster.GossipTransportConfig{ListenAddr: gAddr})
		if bound, gerr := gossip.StartListener(); gerr != nil {
			log.Printf("[cluster] gossip listener: %v", gerr)
		} else {
			log.Printf("[cluster] gossip listening on %s", bound)
		}
		gossip.Start()
		// Advertise each peer's derived gossip address for bootstrap.
		for _, pr := range peers {
			_ = pm.SetNodeGossipAddr(pr.id, deriveAddr(pr.clientAddr, offGossip))
		}
	}
	defer gossip.Close()
	log.Printf("[cluster] node=%s  mode=%s  vnodes=64  replication_factor=%d",
		*nodeID, *mode, clusterCfg.ReplicationFactor)

	// Build the write coordinator for the selected mode.  Standalone is a
	// pass-through wrapper (byte-for-byte identical to the pre-existing path).
	pCurrentEngine = engine
	var coord *coordinator
	var replMetrics *replication.ReplicationMetrics
	if deploy == modeStandalone {
		coord = newStandaloneCoordinator(engine)
	} else {
		c, cleanup, rm, cerr := buildCoordinator(clusterParams{
			mode:        deploy,
			nodeID:      *nodeID,
			clientAddr:  *addr,
			raftAddr:    *raftAddrFlag,
			replAddr:    *replAddrFlag,
			consistency: consistency,
			replFactor:  clusterCfg.ReplicationFactor,
			dataDir:     *dataDir,
			peers:       peers,
			tlsCert:     *clusterTLSCert,
			tlsKey:      *clusterTLSKey,
			tlsCA:       *clusterTLSCA,
			mutual:      *clusterMTLS,
		})
		if cerr != nil {
			log.Fatalf("cluster setup: %v", cerr)
		}
		coord = c
		replMetrics = rm
		defer cleanup()
	}
	coord.pm = pm
	if coord.localID == "" {
		coord.localID = *nodeID
	}
	if *linReadsFlag {
		if deploy != modeRaft {
			log.Printf("[server] --linearizable-reads has no effect outside --mode=raft")
		} else {
			coord.linReads = true
			log.Printf("[raft] linearizable reads enabled (ReadIndex fence per GET)")
		}
	}

	// Partition transfer + auto-rebalance (distributed modes): stream keys to
	// their new owners when the ring changes instead of only updating routing.
	if deploy != modeStandalone {
		tAddr := *transferAddrFlag
		if tAddr == "" {
			tAddr = deriveAddr(*addr, offTransfer)
		}
		var transferTLS *cluster.TransferTLSConfig
		if *clusterTLSCert != "" && *clusterTLSKey != "" {
			transferTLS = &cluster.TransferTLSConfig{
				Enabled:           true,
				CertFile:          *clusterTLSCert,
				KeyFile:           *clusterTLSKey,
				CAFile:            *clusterTLSCA,
				RequireClientCert: *clusterMTLS,
			}
		}
		ta, terr := cluster.NewTransferAgentTLS(pm, *nodeID, engine, tAddr, transferTLS)
		if terr != nil {
			log.Fatalf("transfer agent: %v", terr)
		}
		if terr := ta.Start(); terr != nil {
			log.Fatalf("transfer agent: %v", terr)
		}
		defer ta.Stop()
		pm.SetNodeTransferAddr(*nodeID, tAddr)
		for _, pr := range peers {
			pm.SetNodeTransferAddr(pr.id, deriveAddr(pr.clientAddr, offTransfer))
		}
		if *autoRebalance {
			stopRebalancer := startAutoRebalancer(pm, ta, *nodeID)
			defer stopRebalancer()
			log.Printf("[rebalance] auto-rebalance enabled  transfer=%s  debounce=%s", tAddr, rebalanceDebounce)
		} else {
			log.Printf("[rebalance] auto-rebalance disabled (--auto-rebalance=false); transfer agent listening on %s", tAddr)
		}
	}

	// Wire full metrics/admin onto the existing health mux now that engine is ready.
	reg := prometheus.NewRegistry()
	collector := veltrixmetrics.NewVeltrixCollector(*nodeID, engine, pm, fd, replMetrics)
	reg.MustRegister(collector)
	healthMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	adminMux := http.NewServeMux()
	adminapi.Register(adminMux, engine, "/admin", storage.NewBackupEngine(engine))
	registerClusterAdmin(adminMux, "/admin", coord, pm)
	healthMux.Handle("/admin/", adminapi.Guard(*adminToken, adminMux))
	if *adminToken != "" {
		log.Printf("[admin] token auth enabled for /admin/*")
	} else {
		log.Printf("[admin] /admin/* restricted to loopback — set --admin-token (or VELTRIX_ADMIN_TOKEN) to allow remote access")
	}
	healthMux.HandleFunc("/traces", func(w http.ResponseWriter, r *http.Request) {
		tracing.HandleTracesHTTP(w, r)
	})
	engineForHealth.Store(engine)
	engineReady.Store(true)
	log.Printf("[metrics] serving on %s  /metrics  /healthz  /readyz  /admin/*  /traces", *metricsAddr)

	// Auth enforcer
	ae := security.NewAuthEnforcer()
	if *authConfig != "" {
		if err := ae.LoadConfig(*authConfig); err != nil {
			log.Fatalf("auth config: %v", err)
		}
		log.Printf("[auth] RBAC enabled  config=%s", *authConfig)
	} else {
		log.Printf("[auth] RBAC disabled — all operations permitted without credentials")
	}

	// Plain TCP listener — always active on --addr (default :9000).
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("[server] plaintext listening on %s", *addr)

	// Optional TLS listener on a separate port (--tls-addr, default :9443).
	// Both plain and TLS listeners run concurrently — plain for internal
	// cluster traffic, TLS for external clients.
	tlsCfg := &security.TLSConfig{
		CertFile: *tlsCert,
		KeyFile:  *tlsKey,
		CAFile:   *tlsCA,
	}
	var tlsLn net.Listener
	if tlsCfg.IsEnabled() {
		serverTLS, err := security.ServerTLSConfig(tlsCfg)
		if err != nil {
			log.Fatalf("TLS config: %v", err)
		}
		rawTLS, err := net.Listen("tcp", *tlsAddr)
		if err != nil {
			log.Fatalf("TLS listen %s: %v", *tlsAddr, err)
		}
		tlsLn = tls.NewListener(rawTLS, serverTLS)
		if *tlsCA != "" {
			log.Printf("[tls] mTLS enabled on %s  cert=%s  ca=%s", *tlsAddr, *tlsCert, *tlsCA)
		} else {
			log.Printf("[tls] TLS enabled on %s  cert=%s", *tlsAddr, *tlsCert)
		}
		go func() {
			for {
				conn, err := tlsLn.Accept()
				if err != nil {
					return
				}
				go handleConn(conn, engine, ae, coord)
			}
		}()
	} else {
		log.Printf("[tls] disabled — TLS listener not started")
	}

	log.Printf("[server] ready — plain: %s  tls: %s", *addr, func() string {
		if tlsCfg.IsEnabled() {
			return *tlsAddr
		}
		return "off"
	}())

	// Graceful shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("[server] shutting down…")
		ln.Close()
		if tlsLn != nil {
			tlsLn.Close()
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			break
		}
		go handleConn(conn, engine, ae, coord)
	}

	log.Println("[server] stopped")
}

// handleConn serves one client connection.
// Protocol is auto-detected from the first byte:
//
//	0x01–0x09  → binary protocol (length-prefixed, zero-copy friendly)
//	            0x06 = MPUT (vectorized multi-put batch frame)
//	            0x07 = MGET (vectorized multi-get batch frame)
//	            0x09 = AUTH (authenticate before issuing commands)
//	any letter → text line protocol (backward-compatible)
//
// Binary request format (single-op):
//
//	[1B] cmd  [2B] keyLen LE  [4B] valLen LE  [keyLen]key  [valLen]value
//
// Binary request format (batch — MPUT 0x06 / MGET 0x07):
//
//	[1B] cmd  [2B] unused=0  [4B] count LE  …entries…
//
// Binary AUTH frame (0x09):
//
//	[1B 0x09] [2B userLen LE] [4B passLen LE] [user] [pass]
//
// Binary response format:
//
//	[1B] status  [4B] payloadLen LE  [payloadLen]payload
// metricsConn wraps a net.Conn and accumulates byte-level I/O counters into
// StorageMetrics so the Prometheus collector can report network throughput
// without any per-request overhead beyond two atomic adds.
type metricsConn struct {
	net.Conn
	m *storage.StorageMetrics
}

func (mc *metricsConn) Read(b []byte) (int, error) {
	n, err := mc.Conn.Read(b)
	if n > 0 {
		mc.m.NetworkBytesIn.Add(uint64(n))
	}
	return n, err
}

func (mc *metricsConn) Write(b []byte) (int, error) {
	n, err := mc.Conn.Write(b)
	if n > 0 {
		mc.m.NetworkBytesOut.Add(uint64(n))
	}
	return n, err
}

func handleConn(conn net.Conn, engine *storage.StorageEngine, ae *security.AuthEnforcer, coord *coordinator) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[server] recovered panic in handleConn: %v\n%s", r, debug.Stack())
		}
	}()
	m := engine.GetMetrics()
	m.ActiveConnections.Add(1)
	defer m.ActiveConnections.Add(-1)

	conn.SetDeadline(time.Time{})
	setSocketRecvBuf(conn, 256<<10)
	defer conn.Close()

	// Wrap the connection so all I/O increments network byte counters.
	mc := &metricsConn{Conn: conn, m: m}

	ca := security.NewConnAuth(ae)
	br := bufio.NewReaderSize(mc, 64*1024)

	first, err := br.ReadByte()
	if err != nil {
		return
	}
	if first >= 0x01 && first <= binCmdGetVer {
		_ = br.UnreadByte()
		handleBinaryConn(mc, br, engine, ca, coord)
	} else {
		_ = br.UnreadByte()
		handleTextConn(mc, br, engine, ca, coord)
	}
}

// handleTextConn serves the original line-based text protocol.
func handleTextConn(conn net.Conn, br *bufio.Reader, engine *storage.StorageEngine, ca *security.ConnAuth, coord *coordinator) {
	scanner := bufio.NewScanner(br)
	w := bufio.NewWriterSize(conn, 32*1024)

	writeLine := func(s string) {
		fmt.Fprintln(w, s)
		w.Flush()
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {

		case "AUTH":
			if len(parts) < 3 {
				writeLine("ERR usage: AUTH <username> <password>")
				continue
			}
			if err := ca.Login(parts[1], parts[2]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		case "PUT", "SET":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 3 {
				writeLine("ERR usage: PUT <key> <value>")
				continue
			}
			key, value := parts[1], []byte(parts[2])
			if err := coord.Put(key, value, -1); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		case "GET":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: GET <key>")
				continue
			}
			val, err := coord.Get(parts[1])
			if err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine(string(val))
			}

		case "DEL", "DELETE":
			if err := ca.Check(security.PermDelete); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: DEL <key>")
				continue
			}
			if err := coord.Delete(parts[1]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// PUTEX <key> <ttl-seconds> <value> — PUT with expiration (text-protocol
		// counterpart of the binary PUT's TTL field).
		case "PUTEX":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 4)
			if len(full) < 4 {
				writeLine("ERR usage: PUTEX <key> <ttl-seconds> <value>")
				continue
			}
			ttl, err := strconv.ParseInt(full[2], 10, 32)
			if err != nil || ttl < 1 {
				writeLine("ERR bad ttl " + strconv.Quote(full[2]))
				continue
			}
			if err := coord.Put(full[1], []byte(full[3]), int32(ttl)); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// INCR <key> [delta] / DECR <key> [delta] — atomic counters.
		case "INCR", "DECR":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: " + cmd + " <key> [delta]")
				continue
			}
			delta := int64(1)
			if len(parts) >= 3 {
				d, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
				if err != nil {
					writeLine("ERR bad delta " + strconv.Quote(parts[2]))
					continue
				}
				delta = d
			}
			var v int64
			var err error
			if cmd == "INCR" {
				v, err = coord.Increment(parts[1], delta, -1)
			} else {
				v, err = coord.Decrement(parts[1], delta, -1)
			}
			if err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine(strconv.FormatInt(v, 10))
			}

		// SETNX <key> <value> — set only if the key does not exist.
		// Replies 1 (created) or 0 (already exists).
		case "SETNX":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 3 {
				writeLine("ERR usage: SETNX <key> <value>")
				continue
			}
			res, err := coord.SetIfNotExists(parts[1], []byte(parts[2]), -1)
			switch {
			case err != nil:
				writeLine("ERR " + err.Error())
			case res == storage.SetNXCreated:
				writeLine("1")
			default:
				writeLine("0")
			}

		// CAS <key> <expected> <new> — atomic compare-and-swap.
		// Replies OK, MISMATCH, or NOTFOUND. Values are space-delimited tokens;
		// use the binary protocol for values containing spaces.
		case "CAS":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 4)
			if len(full) < 4 {
				writeLine("ERR usage: CAS <key> <expected> <new>")
				continue
			}
			res, err := coord.CompareAndSwap(full[1], []byte(full[2]), []byte(full[3]), -1)
			switch {
			case err != nil:
				writeLine("ERR " + err.Error())
			case res == storage.CASSuccess:
				writeLine("OK")
			case res == storage.CASKeyNotFound:
				writeLine("NOTFOUND")
			default:
				writeLine("MISMATCH")
			}

		// MGET <key> [key ...] — one "<key> <value>" line per found key, END.
		case "MGET":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			keys := strings.Fields(line)[1:]
			if len(keys) == 0 {
				writeLine("ERR usage: MGET <key> [key ...]")
				continue
			}
			for _, k := range keys {
				if val, err := engine.Get(k); err == nil {
					fmt.Fprintf(w, "%s %s\n", k, val)
				}
			}
			writeLine("END")

		// ── List commands ──────────────────────────────────────────────────────
		// LPUSH/RPUSH <key> <value>  → new length
		case "LPUSH", "RPUSH":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 3 {
				writeLine("ERR usage: " + cmd + " <key> <value>")
				continue
			}
			n, err := coord.ListPush(parts[1], []byte(parts[2]), cmd == "LPUSH")
			if err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine(strconv.FormatInt(n, 10))
			}

		// LPOP/RPOP <key>  → value, or NIL when empty
		case "LPOP", "RPOP":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: " + cmd + " <key>")
				continue
			}
			v, found, err := coord.ListPop(parts[1], cmd == "LPOP")
			switch {
			case err != nil:
				writeLine("ERR " + err.Error())
			case !found:
				writeLine("NIL")
			default:
				writeLine(string(v))
			}

		// LLEN <key>
		case "LLEN":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: LLEN <key>")
				continue
			}
			writeLine(strconv.FormatInt(engine.LLen(parts[1]), 10))

		// LRANGE <key> <start> <stop>  (Redis semantics, stop inclusive, negatives from end)
		case "LRANGE":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 4 {
				writeLine("ERR usage: LRANGE <key> <start> <stop>")
				continue
			}
			start, err1 := strconv.ParseInt(full[2], 10, 64)
			stop, err2 := strconv.ParseInt(full[3], 10, 64)
			if err1 != nil || err2 != nil {
				writeLine("ERR bad range")
				continue
			}
			vals, err := engine.LRange(full[1], start, stop)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, v := range vals {
				fmt.Fprintf(w, "%s\n", v)
			}
			writeLine("END")

		// ── Set commands ───────────────────────────────────────────────────────
		// SADD/SREM <key> <member>  → 1 when added/removed, 0 otherwise
		case "SADD", "SREM":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 3 {
				writeLine("ERR usage: " + cmd + " <key> <member>")
				continue
			}
			var changed bool
			var err error
			if cmd == "SADD" {
				changed, err = coord.SetAdd(parts[1], parts[2])
			} else {
				changed, err = coord.SetRem(parts[1], parts[2])
			}
			switch {
			case err != nil:
				writeLine("ERR " + err.Error())
			case changed:
				writeLine("1")
			default:
				writeLine("0")
			}

		// SISMEMBER <key> <member>
		case "SISMEMBER":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 3 {
				writeLine("ERR usage: SISMEMBER <key> <member>")
				continue
			}
			if engine.SIsMember(parts[1], parts[2]) {
				writeLine("1")
			} else {
				writeLine("0")
			}

		// SMEMBERS <key>
		case "SMEMBERS":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: SMEMBERS <key>")
				continue
			}
			for _, m := range engine.SMembers(parts[1]) {
				fmt.Fprintf(w, "%s\n", m)
			}
			writeLine("END")

		// SCARD <key>
		case "SCARD":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if len(parts) < 2 {
				writeLine("ERR usage: SCARD <key>")
				continue
			}
			writeLine(strconv.Itoa(engine.SCard(parts[1])))

		case "INFO":
			if err := ca.Check(security.PermAdmin); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			m := engine.GetMetrics()
			cs := engine.GetCacheStats()
			info := fmt.Sprintf(
				"keys=%d writes=%d reads=%d deletes=%d cache_hits=%d cache_misses=%d"+
					" hit_rate=%.2f wal_flushes=%d compaction_runs=%d version=%d",
				engine.GetIndexSize(),
				m.Writes.Load(),
				m.Reads.Load(),
				m.Deletes.Load(),
				m.CacheHits.Load(),
				m.CacheMisses.Load(),
				cs.HitRate,
				m.WALFlushes.Load(),
				m.CompactionRuns.Load(),
				engine.GetVersion(),
			)
			for _, ds := range engine.GetDiskStats() {
				info += fmt.Sprintf(" disk[%d]=%s:%.1fMB", ds.DiskIdx, ds.Path, float64(ds.SegmentBytes)/1e6)
			}
			if fd := engine.FailedDisks(); len(fd) > 0 {
				info += fmt.Sprintf(" FAILED_DISKS=%v", fd)
			}
			writeLine(info)

		case "PING":
			writeLine("PONG")

		// TOPOLOGY returns the cluster topology as one JSON line so a
		// cluster-aware client can bootstrap consistent-hash routing (and, in
		// raft mode, discover the leader) over the storage port alone.
		case "TOPOLOGY", "CLUSTER":
			writeLine(coord.topologyJSONLine())

		case "QUIT", "EXIT":
			writeLine("BYE")
			return

		// ── Namespace commands ─────────────────────────────────────────────────
		// NSPUT <namespace> <key> <value>
		case "NSPUT":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			// parts[0]=NSPUT parts[1]=ns parts[2]="key value" — re-split
			full := strings.SplitN(line, " ", 4)
			if len(full) < 4 {
				writeLine("ERR usage: NSPUT <namespace> <key> <value>")
				continue
			}
			if err := coord.PutNS(full[1], full[2], []byte(full[3]), -1); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// NSGET <namespace> <key>
		case "NSGET":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 3)
			if len(full) < 3 {
				writeLine("ERR usage: NSGET <namespace> <key>")
				continue
			}
			val, err := engine.GetNS(full[1], full[2])
			if err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine(string(val))
			}

		// NSDEL <namespace> <key>
		case "NSDEL":
			if err := ca.Check(security.PermDelete); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 3)
			if len(full) < 3 {
				writeLine("ERR usage: NSDEL <namespace> <key>")
				continue
			}
			if err := coord.DeleteNS(full[1], full[2]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// NSDROP <namespace>   — deletes ALL keys in the namespace
		case "NSDROP":
			if err := ca.Check(security.PermDelete); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 2)
			if len(full) < 2 {
				writeLine("ERR usage: NSDROP <namespace>")
				continue
			}
			n, err := coord.DropNamespace(full[1])
			if err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine(fmt.Sprintf("OK deleted=%d", n))
			}

		// NSSCAN <namespace> [prefix] [limit]
		case "NSSCAN":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 2 {
				writeLine("ERR usage: NSSCAN <namespace> [prefix] [limit]")
				continue
			}
			ns := full[1]
			prefix := ""
			limit := 0
			if len(full) >= 3 {
				prefix = full[2]
			}
			if len(full) >= 4 {
				limit, _ = strconv.Atoi(full[3])
			}
			entries, err := engine.ScanNamespace(ns, prefix, limit)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, e := range entries {
				writeLine(e.Key + " " + string(e.Value))
			}
			writeLine("END")

		// NSLIST  — lists all namespaces with key counts
		case "NSLIST":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, info := range engine.ListNamespaces() {
				writeLine(fmt.Sprintf("%s %d", info.Namespace, info.KeyCount))
			}
			writeLine("END")

		// ── Hash field commands ────────────────────────────────────────────────
		// HSET <key> <field> <value> [ttl]
		case "HSET":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 5)
			if len(full) < 4 {
				writeLine("ERR usage: HSET <key> <field> <value> [ttl]")
				continue
			}
			var ttl int32 = -1
			if len(full) == 5 {
				n, _ := strconv.ParseInt(full[4], 10, 32)
				ttl = int32(n)
			}
			if err := coord.HSet(full[1], full[2], []byte(full[3]), ttl); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// HGET <key> <field>
		case "HGET":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 3)
			if len(full) < 3 {
				writeLine("ERR usage: HGET <key> <field>")
				continue
			}
			val, err := engine.HGet(full[1], full[2])
			if err != nil || val == nil {
				writeLine("NOT_FOUND")
			} else {
				writeLine(string(val))
			}

		// HDEL <key> <field>
		case "HDEL":
			if err := ca.Check(security.PermDelete); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 3)
			if len(full) < 3 {
				writeLine("ERR usage: HDEL <key> <field>")
				continue
			}
			if err := coord.HDel(full[1], full[2]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// HGETALL <key>  → count\nfield\nvalue\n...
		case "HGETALL":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 2)
			if len(full) < 2 {
				writeLine("ERR usage: HGETALL <key>")
				continue
			}
			fields, err := engine.HGetAll(full[1])
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			writeLine(strconv.Itoa(len(fields)))
			for _, hf := range fields {
				writeLine(hf.Field)
				writeLine(string(hf.Value))
			}

		// HKEYS <key>  → count\nfield1\nfield2\n...
		case "HKEYS":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 2)
			if len(full) < 2 {
				writeLine("ERR usage: HKEYS <key>")
				continue
			}
			keys := engine.HKeys(full[1])
			writeLine(strconv.Itoa(len(keys)))
			for _, f := range keys {
				writeLine(f)
			}

		// HLEN <key>  → N
		case "HLEN":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 2)
			if len(full) < 2 {
				writeLine("ERR usage: HLEN <key>")
				continue
			}
			writeLine(strconv.Itoa(engine.HLen(full[1])))

		// HEXPIRE <key> <field> <ttl>
		case "HEXPIRE":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 4 {
				writeLine("ERR usage: HEXPIRE <key> <field> <ttl>")
				continue
			}
			n, _ := strconv.ParseInt(full[3], 10, 32)
			if err := coord.HExpire(full[1], full[2], int32(n)); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// HTTL <key> <field>  → N  (-1=immortal, -2=not found)
		case "HTTL":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.SplitN(line, " ", 3)
			if len(full) < 3 {
				writeLine("ERR usage: HTTL <key> <field>")
				continue
			}
			writeLine(strconv.Itoa(int(engine.HTTL(full[1], full[2]))))

		// ── Ordered range scans ────────────────────────────────────────────
		// RANGE <start> <end> [LIMIT n] [REV]  →  "key value" lines + END
		case "RANGE":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 3 {
				writeLine("ERR usage: RANGE <start> <end> [LIMIT n] [REV]")
				continue
			}
			start, end := full[1], full[2]
			limit, reverse, ok := parseRangeTail(full[3:])
			if !ok {
				writeLine("ERR usage: RANGE <start> <end> [LIMIT n] [REV]")
				continue
			}
			kvs, err := engine.RangeScan(start, end, limit, reverse)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, kv := range kvs {
				writeLine(kv.Key + " " + string(kv.Value))
			}
			writeLine("END")

		// SCANCUR <cursor> <limit>  — cursor "-" starts from the beginning.
		// Response: "key value" lines, then "CURSOR <next>" ("-" = exhausted).
		case "SCANCUR":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) != 3 {
				writeLine("ERR usage: SCANCUR <cursor> <limit>  (cursor '-' = start)")
				continue
			}
			cursor := full[1]
			if cursor == "-" {
				cursor = ""
			}
			limit, err := strconv.Atoi(full[2])
			if err != nil {
				writeLine("ERR usage: SCANCUR <cursor> <limit>")
				continue
			}
			kvs, next, err := engine.ScanCursor(cursor, limit)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, kv := range kvs {
				writeLine(kv.Key + " " + string(kv.Value))
			}
			if next == "" {
				next = "-"
			}
			writeLine("CURSOR " + next)

		// ── One-shot optimistic transaction ────────────────────────────────
		// TXN <n> followed by n op lines:
		//   SET <key> <value>
		//   SETIF <key> <expectedVersion> <value>   (version from VER; 0 = must not exist)
		//   DEL <key>
		// Reply: OK | CONFLICT | ERR <msg>.
		// Semantics: read-committed optimistic CAS — versions are validated,
		// then the write set commits atomically per disk via MultiPut. NOT
		// serializable; on CONFLICT re-read versions and retry.
		case "TXN":
			full := strings.Fields(line)
			if len(full) != 2 {
				writeLine("ERR usage: TXN <numOps> then <numOps> op lines (SET/SETIF/DEL)")
				continue
			}
			n, err := strconv.Atoi(full[1])
			if err != nil || n <= 0 || n > maxBatchCount {
				writeLine("ERR usage: TXN <numOps> then <numOps> op lines (SET/SETIF/DEL)")
				continue
			}
			// Permission is checked up front, but the n op lines are always
			// consumed so a denial does not desynchronise the connection.
			permErr := ca.Check(security.PermWrite)
			txnOps := make([]fsmTxnOp, 0, n)
			parseErr := ""
			for i := 0; i < n; i++ {
				if !scanner.Scan() {
					writeLine("ERR TXN truncated: connection closed mid-transaction")
					return
				}
				opLine := strings.TrimSpace(scanner.Text())
				if permErr != nil {
					continue // drain only
				}
				opParts := strings.SplitN(opLine, " ", 2)
				switch strings.ToUpper(opParts[0]) {
				case "SET":
					kv := strings.SplitN(opLine, " ", 3)
					if len(kv) < 3 {
						parseErr = "usage: SET <key> <value>"
						continue
					}
					txnOps = append(txnOps, fsmTxnOp{Op: "SET", Key: kv[1], Value: []byte(kv[2]), TTL: -1})
				case "SETIF":
					kv := strings.SplitN(opLine, " ", 4)
					if len(kv) < 4 {
						parseErr = "usage: SETIF <key> <expectedVersion> <value>"
						continue
					}
					ver, verr := strconv.ParseUint(kv[2], 10, 64)
					if verr != nil {
						parseErr = "SETIF: bad version " + kv[2]
						continue
					}
					txnOps = append(txnOps, fsmTxnOp{Op: "SETIF", Key: kv[1], Value: []byte(kv[3]), TTL: -1, ExpectedVersion: ver})
				case "DEL":
					kv := strings.SplitN(opLine, " ", 2)
					if len(kv) < 2 {
						parseErr = "usage: DEL <key>"
						continue
					}
					txnOps = append(txnOps, fsmTxnOp{Op: "DEL", Key: kv[1]})
				default:
					parseErr = "unknown txn op: " + opParts[0] + " (want SET/SETIF/DEL)"
				}
			}
			if permErr != nil {
				writeLine("ERR " + permErr.Error())
				continue
			}
			if parseErr != "" {
				writeLine("ERR " + parseErr)
				continue
			}
			switch err := coord.Txn(txnOps); {
			case err == nil:
				writeLine("OK")
			case err == storage.ErrTxnConflict:
				writeLine("CONFLICT")
			default:
				writeLine("ERR " + err.Error())
			}

		// VER <key>  →  optimistic version token (0 = absent). Pass to SETIF.
		case "VER":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) != 2 {
				writeLine("ERR usage: VER <key>")
				continue
			}
			writeLine(strconv.FormatUint(engine.KeyVersion(full[1]), 10))

		// ── Secondary indexes ──────────────────────────────────────────────
		// IDXCREATE <name> <field>  — index <field> of every value (JSON
		// object or "k=v" pairs). Persists across restart; backfills existing keys.
		case "IDXCREATE":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) != 3 {
				writeLine("ERR usage: IDXCREATE <name> <field>")
				continue
			}
			if err := coord.IdxCreate(full[1], full[2]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		case "IDXDROP":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) != 2 {
				writeLine("ERR usage: IDXDROP <name>")
				continue
			}
			if err := coord.IdxDrop(full[1]); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// IDXQUERY <name> <value> [LIMIT n]  →  primary-key lines + END
		case "IDXQUERY":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) != 3 && len(full) != 5 {
				writeLine("ERR usage: IDXQUERY <name> <value> [LIMIT n]")
				continue
			}
			limit := 0
			if len(full) == 5 {
				if strings.ToUpper(full[3]) != "LIMIT" {
					writeLine("ERR usage: IDXQUERY <name> <value> [LIMIT n]")
					continue
				}
				limit, _ = strconv.Atoi(full[4])
			}
			keys := engine.LookupBySecondary(full[1], full[2])
			for i, k := range keys {
				if limit > 0 && i >= limit {
					break
				}
				writeLine(k)
			}
			writeLine("END")

		// ── Vector index (namespace "default") ─────────────────────────────
		// VSET <key> <f1> <f2> ... — dimension fixed by the first VSET.
		case "VSET":
			if err := ca.Check(security.PermWrite); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 3 {
				writeLine("ERR usage: VSET <key> <f1> <f2> ...")
				continue
			}
			vec, err := parseFloats(full[2:])
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			if err := coord.VSet(defaultVectorNS, full[1], vec); err != nil {
				writeLine("ERR " + err.Error())
			} else {
				writeLine("OK")
			}

		// VSEARCH <k> <f1> <f2> ...  →  "id score" lines + END
		case "VSEARCH":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			full := strings.Fields(line)
			if len(full) < 3 {
				writeLine("ERR usage: VSEARCH <k> <f1> <f2> ...")
				continue
			}
			k, err := strconv.Atoi(full[1])
			if err != nil || k < 0 {
				writeLine("ERR usage: VSEARCH <k> <f1> <f2> ...")
				continue
			}
			query, err := parseFloats(full[2:])
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			matches, err := engine.SearchVector(defaultVectorNS, query, k)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, m := range matches {
				writeLine(fmt.Sprintf("%s %g", m.ID, m.Score))
			}
			writeLine("END")

		// ── Minimal query language ─────────────────────────────────────────
		// QUERY <namespace> WHERE <field> <op> <value> [LIMIT n]
		// ops: = != > < >= <= contains. Records are the namespace's values,
		// parsed as JSON objects or "k=v" pairs. Uses a secondary index for
		// "=" on an indexed field, else scans the namespace.
		case "QUERY":
			if err := ca.Check(security.PermRead); err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			const usage = "ERR usage: QUERY <namespace> WHERE <field> <op> <value> [LIMIT n]  (op: = != > < >= <= contains)"
			full := strings.Fields(line)
			if (len(full) != 6 && len(full) != 8) || strings.ToUpper(full[2]) != "WHERE" {
				writeLine(usage)
				continue
			}
			ns, field, op, value := full[1], full[3], full[4], full[5]
			if !storage.IsQueryOp(op) {
				writeLine(usage)
				continue
			}
			limit := 0
			if len(full) == 8 {
				if strings.ToUpper(full[6]) != "LIMIT" {
					writeLine(usage)
					continue
				}
				var perr error
				limit, perr = strconv.Atoi(full[7])
				if perr != nil {
					writeLine(usage)
					continue
				}
			}
			entries, err := engine.QueryNS(ns, field, op, value, limit)
			if err != nil {
				writeLine("ERR " + err.Error())
				continue
			}
			for _, e := range entries {
				writeLine(e.Key + " " + string(e.Value))
			}
			writeLine("END")

		default:
			writeLine("ERR unknown command: " + cmd + " (supported: AUTH PUT GET DEL INFO PING QUIT NSPUT NSGET NSDEL NSDROP NSSCAN NSLIST HSET HGET HDEL HGETALL HKEYS HLEN HEXPIRE HTTL RANGE SCANCUR TXN VER IDXCREATE IDXDROP IDXQUERY VSET VSEARCH QUERY)")
		}
	}
}

// parseRangeTail parses the optional "[LIMIT n] [REV]" suffix of RANGE.
func parseRangeTail(toks []string) (limit int, reverse, ok bool) {
	i := 0
	if i < len(toks) && strings.ToUpper(toks[i]) == "LIMIT" {
		if i+1 >= len(toks) {
			return 0, false, false
		}
		n, err := strconv.Atoi(toks[i+1])
		if err != nil {
			return 0, false, false
		}
		limit = n
		i += 2
	}
	if i < len(toks) && strings.ToUpper(toks[i]) == "REV" {
		reverse = true
		i++
	}
	if i != len(toks) {
		return 0, false, false
	}
	return limit, reverse, true
}

// parseFloats converts VSET/VSEARCH float tokens to []float32.
func parseFloats(toks []string) ([]float32, error) {
	out := make([]float32, len(toks))
	for i, t := range toks {
		f, err := strconv.ParseFloat(t, 32)
		if err != nil {
			return nil, fmt.Errorf("bad float %q", t)
		}
		out[i] = float32(f)
	}
	return out, nil
}

// tryCoalescePuts opportunistically reads additional single-PUT frames from br
// without blocking and returns them as a ready-to-execute MultiPutRequest slice.
//
// Vectorized read strategy
// ────────────────────────
// After the OS delivers a TCP segment the kernel copies its payload into the
// socket receive buffer in one shot.  bufio.Reader drains that buffer in a
// single Read syscall; subsequent calls to br.Buffered() return the remaining
// bytes already in memory — no additional syscalls.
//
// By peeking 7 bytes (one frame header) at a time we decode all complete PUT
// frames that are already buffered, turning N sequential per-connection round
// trips into one MultiPut batch execution.  This is the "vectorized read"
// equivalent of read(2) on a full TCP segment.
//
// firstKey / firstVal are the already-decoded first PUT entry (passed in from
// the main dispatch loop so we don't decode it twice).  firstVal must be a
// fresh copy — the caller must NOT be holding a reference to a pooled buffer
// containing firstVal.
//
// Returns nil when fewer than 2 PUTs are available (caller should use Put).
func tryCoalescePuts(
	firstKey string,
	firstVal []byte,
	br *bufio.Reader,
) []storage.MultiPutRequest {
	reqs := []storage.MultiPutRequest{
		{Key: firstKey, Value: firstVal, TTL: -1},
	}

	hdr := make([]byte, 7)
	for len(reqs) < maxPipelineBatch {
		// Only read frames that are already in the bufio buffer — never block.
		if br.Buffered() < 7 {
			break
		}
		peeked, err := br.Peek(7)
		if err != nil || len(peeked) < 7 || peeked[0] != binCmdPut {
			break
		}
		nextKeyLen := int(peeked[1]) | int(peeked[2])<<8
		nextValLen := int(peeked[3]) | int(peeked[4])<<8 | int(peeked[5])<<16 | int(peeked[6])<<24
		if nextKeyLen <= 0 || nextKeyLen > 1<<20 || nextValLen < 0 || nextValLen > 64<<20 {
			break
		}
		// Ensure the full frame (header + payload) is already buffered.
		if br.Buffered() < 7+nextKeyLen+nextValLen {
			break
		}
		// Consume the header now that we know the frame is complete.
		if _, err := io.ReadFull(br, hdr); err != nil {
			break
		}
		payloadLen := nextKeyLen + nextValLen
		payPtr := binPayloadPool.Get().(*[]byte)
		if cap(*payPtr) < payloadLen {
			*payPtr = make([]byte, payloadLen)
		} else {
			*payPtr = (*payPtr)[:payloadLen]
		}
		if _, err := io.ReadFull(br, (*payPtr)[:payloadLen]); err != nil {
			binPayloadPool.Put(payPtr)
			break
		}
		key := string((*payPtr)[:nextKeyLen])
		val := make([]byte, nextValLen)
		copy(val, (*payPtr)[nextKeyLen:payloadLen])
		if payloadLen <= maxPooledPayload {
			binPayloadPool.Put(payPtr)
		}
		reqs = append(reqs, storage.MultiPutRequest{Key: key, Value: val, TTL: -1})
	}
	if len(reqs) < 2 {
		return nil
	}
	return reqs
}

// tryCoalesceGets is the read-side counterpart of tryCoalescePuts.
// Decodes buffered GET frames without blocking and returns them for MultiGet.
func tryCoalesceGets(firstKey string, br *bufio.Reader) []string {
	keys := []string{firstKey}
	for len(keys) < maxPipelineBatch {
		if br.Buffered() < 7 {
			break
		}
		peeked, err := br.Peek(7)
		if err != nil || len(peeked) < 7 || peeked[0] != binCmdGet {
			break
		}
		nextKeyLen := int(peeked[1]) | int(peeked[2])<<8
		if nextKeyLen <= 0 || nextKeyLen > 1<<20 {
			break
		}
		if br.Buffered() < 7+nextKeyLen {
			break
		}
		hdr := make([]byte, 7)
		if _, err := io.ReadFull(br, hdr); err != nil {
			break
		}
		payPtr := binPayloadPool.Get().(*[]byte)
		if cap(*payPtr) < nextKeyLen {
			*payPtr = make([]byte, nextKeyLen)
		} else {
			*payPtr = (*payPtr)[:nextKeyLen]
		}
		if _, err := io.ReadFull(br, (*payPtr)[:nextKeyLen]); err != nil {
			binPayloadPool.Put(payPtr)
			break
		}
		keys = append(keys, string((*payPtr)[:nextKeyLen]))
		if nextKeyLen <= maxPooledPayload {
			binPayloadPool.Put(payPtr)
		}
	}
	if len(keys) < 2 {
		return nil
	}
	return keys
}

// handleBinaryConn serves the binary wire protocol.
//
// Single-op frame:
//
//	Request:  [1B cmd][2B keyLen LE][4B valLen LE][key][value]
//	Response: [1B status][4B payloadLen LE][payload]
//
// Batch frame (MPUT 0x06 / MGET 0x07):
//
//	Request:  [1B cmd][2B unused=0][4B count LE][…entries…]
//	Response: [1B 0x00 OK][4B count LE][…per-entry results…]
//
// AUTH frame (0x09):
//
//	Request:  [1B 0x09][2B userLen LE][4B passLen LE][user][pass]
//	Response: [1B status][4B payloadLen LE][payload]
func handleBinaryConn(conn net.Conn, br *bufio.Reader, engine *storage.StorageEngine, ca *security.ConnAuth, coord *coordinator) {
	m := engine.GetMetrics()
	bw := bufio.NewWriterSize(conn, 64*1024)

	// writeResp writes the 5-byte header + optional payload into bw without
	// flushing.  The caller is responsible for calling bw.Flush() once at the
	// end of each pipeline pass so that all N responses are delivered in a
	// single TCP segment rather than N separate sends.
	writeResp := func(status byte, payload []byte) error {
		hdr := [5]byte{status,
			byte(len(payload)),
			byte(len(payload) >> 8),
			byte(len(payload) >> 16),
			byte(len(payload) >> 24),
		}
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		if len(payload) > 0 {
			if _, err := bw.Write(payload); err != nil {
				return err
			}
		}
		return nil
	}

	// sendResp writes a response and flushes immediately.  Used for single-op
	// commands (DEL, PING, INFO, errors) where there is no batching opportunity.
	sendResp := func(status byte, payload []byte) error {
		if err := writeResp(status, payload); err != nil {
			return err
		}
		return bw.Flush()
	}

	hdr := make([]byte, 7) // 1 cmd + 2 keyLen + 4 valLen — allocated once per conn

	// Two scratch buffers for the readv pre-fill.  Each is half the bufio
	// buffer size so together they can fill the entire 64 KB window.
	readvBuf0 := make([]byte, 32*1024)
	readvBuf1 := make([]byte, 32*1024)
	_ = readvBuf0
	_ = readvBuf1

	for {
		// Pre-fill: if the bufio buffer is nearly empty (< 1 frame header),
		// attempt a readv(2) to pull in as many pending bytes as possible in
		// a single syscall before falling through to io.ReadFull.  On Linux
		// this can pull an entire 64 KB TCP burst in one shot; on macOS /
		// non-Linux the stub returns 0 and ReadFull handles the read normally.
		if br.Buffered() < 7 {
			_ = readvInto(conn, readvBuf0, readvBuf1)
		}

		if _, err := io.ReadFull(br, hdr); err != nil {
			return
		}
		cmd := hdr[0]
		keyLen := int(hdr[1]) | int(hdr[2])<<8
		valLen := int(hdr[3]) | int(hdr[4])<<8 | int(hdr[5])<<16 | int(hdr[6])<<24

		if cmd == binCmdMPut {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			if err := handleMPut(valLen, br, bw, engine, coord); err != nil {
				return
			}
			continue
		}
		if cmd == binCmdMGet {
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			if err := handleMGet(valLen, br, bw, engine); err != nil {
				return
			}
			continue
		}

		// ── NSPUT (0x0A) — 13-byte header ────────────────────────────────────
		// Header: [1B cmd][2B nsLen LE][2B keyLen LE][4B valLen LE][4B ttl LE signed]
		// The standard 7-byte hdr read above gives us: keyLen=nsLen, valLen=valLen.
		// We need to read 2 extra bytes for the actual keyLen + 4 bytes for TTL.
		if cmd == binCmdNSPut {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			// hdr[1:3] = nsLen, hdr[3:7] = valLen (already parsed as keyLen/valLen above)
			nsLen := keyLen
			valLenNS := valLen
			// Read 2B actual keyLen + 4B TTL
			var extra [6]byte
			if _, err := io.ReadFull(br, extra[:]); err != nil {
				return
			}
			actualKeyLen := int(binary.LittleEndian.Uint16(extra[0:2]))
			ttl := int32(binary.LittleEndian.Uint32(extra[2:6]))
			totalNeed := nsLen + actualKeyLen + valLenNS
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			payloadLen := len(*payPtr)
			if nsLen+actualKeyLen > payloadLen {
				log.Printf("[server] NSPUT frame corrupt: nsLen=%d actualKeyLen=%d totalNeed=%d payloadLen=%d valLenNS=%d",
					nsLen, actualKeyLen, totalNeed, payloadLen, valLenNS)
				binPayloadPool.Put(payPtr)
				_ = sendResp(binStatusErr, []byte("malformed NSPUT frame"))
				continue
			}
			ns := string((*payPtr)[:nsLen])
			k := string((*payPtr)[nsLen : nsLen+actualKeyLen])
			valCopy := make([]byte, valLenNS)
			copy(valCopy, (*payPtr)[nsLen+actualKeyLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := coord.PutNS(ns, k, valCopy, ttl); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}
			continue
		}

		// ── HSET (0x10) — 13-byte header ─────────────────────────────────────
		// Header: [1B cmd][2B keyLen LE][4B valLen LE] + [2B fieldLen LE][4B ttl LE]
		// Body:   key[keyLen] + field[fieldLen] + value[valLen]
		if cmd == binCmdHSet {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			hKeyLen := keyLen
			hValLen := valLen
			var extra [6]byte
			if _, err := io.ReadFull(br, extra[:]); err != nil {
				return
			}
			fieldLen := int(binary.LittleEndian.Uint16(extra[0:2]))
			ttl := int32(binary.LittleEndian.Uint32(extra[2:6]))
			totalNeed := hKeyLen + fieldLen + hValLen
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			hKey := string((*payPtr)[:hKeyLen])
			hField := string((*payPtr)[hKeyLen : hKeyLen+fieldLen])
			valCopy := make([]byte, hValLen)
			copy(valCopy, (*payPtr)[hKeyLen+fieldLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := coord.HSet(hKey, hField, valCopy, ttl); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}
			continue
		}

		// ── HGET (0x11) / HDEL (0x12) / HTTL (0x17) — 9-byte header ─────────
		// Header: [1B cmd][2B keyLen LE][4B 0] + [2B fieldLen LE]
		// Body:   key[keyLen] + field[fieldLen]
		if cmd == binCmdHGet || cmd == binCmdHDel || cmd == binCmdHTTL {
			if cmd == binCmdHGet || cmd == binCmdHTTL {
				if err := ca.Check(security.PermRead); err != nil {
					_ = sendResp(binStatusErr, []byte(err.Error()))
					return
				}
			} else {
				if err := ca.Check(security.PermDelete); err != nil {
					_ = sendResp(binStatusErr, []byte(err.Error()))
					return
				}
			}
			hKeyLen := keyLen
			var fieldLenBuf [2]byte
			if _, err := io.ReadFull(br, fieldLenBuf[:]); err != nil {
				return
			}
			fieldLen := int(binary.LittleEndian.Uint16(fieldLenBuf[:]))
			totalNeed := hKeyLen + fieldLen
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			hKey := string((*payPtr)[:hKeyLen])
			hField := string((*payPtr)[hKeyLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			switch cmd {
			case binCmdHGet:
				val, err := engine.HGet(hKey, hField)
				if err != nil || val == nil {
					_ = sendResp(binStatusNotFound, nil)
				} else {
					_ = sendResp(binStatusOK, val)
				}
			case binCmdHDel:
				if err := coord.HDel(hKey, hField); err != nil {
					_ = sendResp(binStatusErr, []byte(err.Error()))
				} else {
					_ = sendResp(binStatusOK, nil)
				}
			case binCmdHTTL:
				ttlVal := engine.HTTL(hKey, hField)
				var ttlBuf [4]byte
				binary.LittleEndian.PutUint32(ttlBuf[:], uint32(ttlVal))
				if ttlVal == -2 {
					_ = sendResp(binStatusNotFound, ttlBuf[:])
				} else {
					_ = sendResp(binStatusOK, ttlBuf[:])
				}
			}
			continue
		}

		// ── HEXPIRE (0x16) — 9-byte header ───────────────────────────────────
		// Header: [1B cmd][2B keyLen LE][4B ttl LE (signed)] + [2B fieldLen LE]
		// Body:   key[keyLen] + field[fieldLen]
		if cmd == binCmdHExpire {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			hKeyLen := keyLen
			ttl := int32(valLen) // ttl encoded in the 4-byte aux slot
			var fieldLenBuf [2]byte
			if _, err := io.ReadFull(br, fieldLenBuf[:]); err != nil {
				return
			}
			fieldLen := int(binary.LittleEndian.Uint16(fieldLenBuf[:]))
			totalNeed := hKeyLen + fieldLen
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			hKey := string((*payPtr)[:hKeyLen])
			hField := string((*payPtr)[hKeyLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := coord.HExpire(hKey, hField, ttl); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}
			continue
		}

		// ── NSSCAN (0x0E) — 9-byte header ────────────────────────────────────
		// Header: [1B cmd][2B nsLen LE][2B prefixLen LE][4B limit LE]
		// keyLen=nsLen, valLen=limit (reusing standard field positions)
		if cmd == binCmdNSScan {
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			nsLen := keyLen
			limit := valLen
			// Read ns + prefix payload
			payPtr := binPayloadPool.Get().(*[]byte)
			prefixLen := 0 // will be computed from remaining bytes after ns
			// We must peek how many bytes remain for prefix; the frame doesn't encode prefixLen separately.
			// Convention: payload = ns bytes + prefix bytes; prefix fills the rest.
			// So we encoded the outer keyLen as nsLen, valLen as prefixLen-encoded-as-int.
			// Actually: keyLen=nsLen, valLen=prefixLen (outer header), then the payload buffer.
			// Here valLen was parsed as limit. We need prefixLen from somewhere.
			// Simplest fix: encode ns and prefix together with limit in aux.
			// Already read: hdr[1:3]=nsLen, hdr[3:7]=limit. But we lost prefixLen.
			// Solution: read 2 more bytes for prefixLen, then ns + prefix.
			// Extra 2 bytes:
			var prefixLenBuf [2]byte
			if _, err := io.ReadFull(br, prefixLenBuf[:]); err != nil {
				return
			}
			prefixLen = int(binary.LittleEndian.Uint16(prefixLenBuf[:]))
			totalNeed := nsLen + prefixLen
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if totalNeed > 0 {
				if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
					binPayloadPool.Put(payPtr)
					return
				}
			}
			ns := string((*payPtr)[:nsLen])
			prefix := string((*payPtr)[nsLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := handleNSScan(ns, prefix, limit, bw, engine); err != nil {
				return
			}
			continue
		}

		// ── CAS (0x18) — extra 8-byte header [4B newLen][4B ttl] ───────────
		if cmd == binCmdCAS {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			expectedLen := valLen
			var extra [8]byte
			if _, err := io.ReadFull(br, extra[:]); err != nil {
				return
			}
			newLen := int(binary.LittleEndian.Uint32(extra[0:4]))
			ttl := int32(binary.LittleEndian.Uint32(extra[4:8]))
			totalNeed := keyLen + expectedLen + newLen
			if keyLen <= 0 || keyLen > 1<<20 || expectedLen < 0 || expectedLen > 64<<20 ||
				newLen < 0 || newLen > 64<<20 {
				_ = sendResp(binStatusErr, []byte("frame too large"))
				return
			}
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			k := string((*payPtr)[:keyLen])
			expected := make([]byte, expectedLen)
			copy(expected, (*payPtr)[keyLen:keyLen+expectedLen])
			newVal := make([]byte, newLen)
			copy(newVal, (*payPtr)[keyLen+expectedLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			res, err := coord.CompareAndSwap(k, expected, newVal, ttl)
			if err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				continue
			}
			switch res {
			case storage.CASSuccess:
				_ = sendResp(binStatusOK, nil)
			case storage.CASMismatch:
				_ = sendResp(binStatusMismatch, nil)
			case storage.CASKeyNotFound:
				_ = sendResp(binStatusNotFound, nil)
			}
			continue
		}

		// ── INCR (0x19) / DECR (0x1A) — extra 12-byte [8B delta][4B ttl] ───
		if cmd == binCmdIncr || cmd == binCmdDecr {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			var extra [12]byte
			if _, err := io.ReadFull(br, extra[:]); err != nil {
				return
			}
			delta := int64(binary.LittleEndian.Uint64(extra[0:8]))
			ttl := int32(binary.LittleEndian.Uint32(extra[8:12]))
			if keyLen <= 0 || keyLen > 1<<20 {
				_ = sendResp(binStatusErr, []byte("frame too large"))
				return
			}
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < keyLen {
				*payPtr = make([]byte, keyLen)
			} else {
				*payPtr = (*payPtr)[:keyLen]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:keyLen]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			k := string((*payPtr)[:keyLen])
			if keyLen <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			var newVal int64
			var err error
			if cmd == binCmdIncr {
				newVal, err = coord.Increment(k, delta, ttl)
			} else {
				newVal, err = coord.Decrement(k, delta, ttl)
			}
			if err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				continue
			}
			var ret [8]byte
			binary.LittleEndian.PutUint64(ret[:], uint64(newVal))
			_ = sendResp(binStatusOK, ret[:])
			continue
		}

		// ── SETNX (0x1B) — extra 4-byte [4B ttl] ──────────────────────────
		if cmd == binCmdSetNX {
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				return
			}
			var extra [4]byte
			if _, err := io.ReadFull(br, extra[:]); err != nil {
				return
			}
			ttl := int32(binary.LittleEndian.Uint32(extra[:]))
			if keyLen <= 0 || keyLen > 1<<20 || valLen < 0 || valLen > 64<<20 {
				_ = sendResp(binStatusErr, []byte("frame too large"))
				return
			}
			totalNeed := keyLen + valLen
			payPtr := binPayloadPool.Get().(*[]byte)
			if cap(*payPtr) < totalNeed {
				*payPtr = make([]byte, totalNeed)
			} else {
				*payPtr = (*payPtr)[:totalNeed]
			}
			if _, err := io.ReadFull(br, (*payPtr)[:totalNeed]); err != nil {
				binPayloadPool.Put(payPtr)
				return
			}
			k := string((*payPtr)[:keyLen])
			v := make([]byte, valLen)
			copy(v, (*payPtr)[keyLen:])
			if totalNeed <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			res, err := coord.SetIfNotExists(k, v, ttl)
			if err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				continue
			}
			switch res {
			case storage.SetNXCreated:
				_ = sendResp(binStatusOK, nil)
			case storage.SetNXExists:
				_ = sendResp(binStatusExists, nil)
			}
			continue
		}

		// ── Extended ops (0x20–0x29): range scans, TXN, indexes, vectors ───
		if cmd >= binCmdRange && cmd <= binCmdGetVer {
			if err := handleExtOp(cmd, keyLen, valLen, br, bw, engine, ca, coord); err != nil {
				return
			}
			continue
		}

		// ── Single-op path ───────────────────────────────────────────────
		if keyLen < 0 || keyLen > 1<<20 || valLen < 0 || valLen > 64<<20 {
			_ = sendResp(binStatusErr, []byte("frame too large"))
			return
		}

		// Obtain a pooled buffer for the key+value payload.
		// engine.Put copies the value bytes into WAL + segment synchronously,
		// so the buffer is safe to return to the pool as soon as Put returns.
		need := keyLen + valLen
		payPtr := binPayloadPool.Get().(*[]byte)
		if cap(*payPtr) < need {
			*payPtr = make([]byte, need)
		} else {
			*payPtr = (*payPtr)[:need]
		}
		payload := *payPtr

		if _, err := io.ReadFull(br, payload); err != nil {
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			return
		}
		key := string(payload[:keyLen])
		value := payload[keyLen:]

		switch cmd {
		case binCmdAuth:
			// key=username, value=password (reuse existing frame layout)
			if err := ca.Login(key, string(value)); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, []byte("OK"))
			}

		case binCmdPut:
			if err := ca.Check(security.PermWrite); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			valCopy := make([]byte, len(value))
			copy(valCopy, value)
			if reqs := tryCoalescePuts(key, valCopy, br); reqs != nil {
				m.MultiPutBatches.Add(1)
				m.MultiPutEntries.Add(uint64(len(reqs)))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				for _, perr := range coord.MultiPut(reqs) {
					if perr != nil {
						_ = writeResp(binStatusErr, []byte(perr.Error()))
					} else {
						_ = writeResp(binStatusOK, nil)
					}
				}
				_ = bw.Flush()
				continue
			}
			if err := coord.Put(key, valCopy, -1); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}

		case binCmdGet:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			if keys := tryCoalesceGets(key, br); keys != nil {
				m.MultiGetBatches.Add(1)
				m.MultiGetEntries.Add(uint64(len(keys)))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				for _, r := range engine.MultiGet(keys) {
					if !r.Found || r.Value == nil {
						_ = writeResp(binStatusNotFound, nil)
					} else {
						_ = writeResp(binStatusOK, r.Value)
					}
				}
				_ = bw.Flush()
				continue
			}
			val, err := coord.Get(key)
			if err != nil {
				if _, moved := err.(*redirectError); moved {
					_ = sendResp(binStatusErr, []byte(err.Error()))
				} else {
					_ = sendResp(binStatusNotFound, nil)
				}
			} else {
				_ = sendResp(binStatusOK, val)
			}

		case binCmdDel:
			if err := ca.Check(security.PermDelete); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			if err := coord.Delete(key); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}

		case binCmdPing:
			_ = sendResp(binStatusOK, []byte("PONG"))

		case binCmdInfo:
			if err := ca.Check(security.PermAdmin); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			m := engine.GetMetrics()
			cs := engine.GetCacheStats()
			info := fmt.Sprintf(
				"keys=%d writes=%d reads=%d cache_hit_rate=%.2f wal_flushes=%d",
				engine.GetIndexSize(), m.Writes.Load(), m.Reads.Load(),
				cs.HitRate, m.WALFlushes.Load(),
			)
			_ = sendResp(binStatusOK, []byte(info))

		// ── Namespace single-op commands ────────────────────────────────────
		// NSGET (0x0B) and NSDEL (0x0C) share the same 9-byte header layout:
		//   [1B cmd][2B nsLen LE][2B keyLen LE][4B 0]
		// key field in the standard 7-byte hdr holds nsLen; val field holds keyLen.
		// The actual key follows ns in the payload.
		case binCmdNSGet:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			// payload = ns bytes (keyLen bytes) + key bytes (valLen bytes)
			// here: keyLen=nsLen, valLen=keyLen in the outer header
			ns := string(payload[:keyLen])
			k := string(payload[keyLen:])
			val, err := engine.GetNS(ns, k)
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err != nil {
				_ = sendResp(binStatusNotFound, nil)
			} else {
				_ = sendResp(binStatusOK, val)
			}
			continue

		case binCmdNSDel:
			if err := ca.Check(security.PermDelete); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			ns := string(payload[:keyLen])
			k := string(payload[keyLen:])
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := coord.DeleteNS(ns, k); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, nil)
			}
			continue

		// NSDROP (0x0D): [1B cmd][2B nsLen LE][2B 0][4B 0] + ns bytes
		// keyLen=nsLen, valLen=0 in the outer header
		case binCmdNSDrop:
			if err := ca.Check(security.PermDelete); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			ns := string(payload[:keyLen])
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			n, err := coord.DropNamespace(ns)
			if err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
			} else {
				_ = sendResp(binStatusOK, []byte(fmt.Sprintf("deleted=%d", n)))
			}
			continue

		// NSLIST (0x0F): no body; keyLen=0, valLen=0
		case binCmdNSList:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := handleNSList(bw, engine); err != nil {
				return
			}
			continue

		// ── Hash field commands with standard 7-byte header ────────────────
		// HGETALL (0x13), HKEYS (0x14), HLEN (0x15): [1B cmd][2B keyLen][4B 0] + key
		case binCmdHGetAll:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			hKey := key
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := handleHGetAll(hKey, bw, engine); err != nil {
				return
			}
			continue

		case binCmdHKeys:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			hKey := key
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			if err := handleHKeys(hKey, bw, engine); err != nil {
				return
			}
			continue

		case binCmdHLen:
			if err := ca.Check(security.PermRead); err != nil {
				_ = sendResp(binStatusErr, []byte(err.Error()))
				if need <= maxPooledPayload {
					binPayloadPool.Put(payPtr)
				}
				continue
			}
			count := engine.HLen(key)
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			var lenResp [5]byte
			lenResp[0] = binStatusOK
			binary.LittleEndian.PutUint32(lenResp[1:], uint32(count))
			_, _ = bw.Write(lenResp[:])
			_ = bw.Flush()
			continue

		default:
			_ = sendResp(binStatusErr, []byte("unknown command"))
			if need <= maxPooledPayload {
				binPayloadPool.Put(payPtr)
			}
			return
		}

		// Return the payload buffer to the pool for the next request on this conn.
		// Only pool if the buffer is within the size cap — very large allocations
		// are not worth keeping in the pool and would inflate RSS unnecessarily.
		if need <= maxPooledPayload {
			binPayloadPool.Put(payPtr)
		}
	}
}

// handleMPut processes a MPUT batch frame.
//
// Per-entry request format (after the 7-byte outer header):
//
//	[2B keyLen LE][4B valLen LE][4B ttl LE signed][key bytes][value bytes]
//
// Response:
//
//	[1B 0x00 OK][4B count LE][count × 1B per-entry status]
func handleMPut(count int, br *bufio.Reader, bw *bufio.Writer, engine *storage.StorageEngine, coord *coordinator) error {
	if count <= 0 || count > maxBatchCount {
		resp := [5]byte{binStatusErr}
		_, err := bw.Write(resp[:])
		if err == nil {
			err = bw.Flush()
		}
		return err
	}

	reqs := make([]storage.MultiPutRequest, count)
	var entHdr [10]byte // [2B keyLen][4B valLen][4B ttl]

	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(br, entHdr[:]); err != nil {
			return err
		}
		keyLen := int(binary.LittleEndian.Uint16(entHdr[0:2]))
		valLen := int(binary.LittleEndian.Uint32(entHdr[2:6]))
		ttl := int32(binary.LittleEndian.Uint32(entHdr[6:10]))

		// Use the pooled I/O buffer to read key+value, then copy value bytes.
		// Key is converted to string (copy); value is copied to a fresh slice
		// so the pooled buffer can be returned immediately (invariant 5).
		need := keyLen + valLen
		payPtr := binPayloadPool.Get().(*[]byte)
		if cap(*payPtr) < need {
			*payPtr = make([]byte, need)
		} else {
			*payPtr = (*payPtr)[:need]
		}
		if _, err := io.ReadFull(br, (*payPtr)[:need]); err != nil {
			binPayloadPool.Put(payPtr)
			return err
		}
		key := string((*payPtr)[:keyLen])
		val := make([]byte, valLen)
		copy(val, (*payPtr)[keyLen:need])
		if need <= maxPooledPayload {
			binPayloadPool.Put(payPtr)
		}
		reqs[i] = storage.MultiPutRequest{Key: key, Value: val, TTL: ttl}
	}

	putErrs := coord.MultiPut(reqs)

	respPtr := mputRespPool.Get().(*[]byte)
	needed := 5 + count
	if cap(*respPtr) < needed {
		*respPtr = make([]byte, needed)
	} else {
		*respPtr = (*respPtr)[:needed]
	}
	resp := *respPtr
	resp[0] = binStatusOK
	binary.LittleEndian.PutUint32(resp[1:5], uint32(count))
	for i, err := range putErrs {
		if err != nil {
			resp[5+i] = binStatusErr
		} else {
			resp[5+i] = binStatusOK
		}
	}
	_, writeErr := bw.Write(resp[:needed])
	mputRespPool.Put(respPtr)
	if writeErr != nil {
		return writeErr
	}
	return bw.Flush()
}

// handleMGet processes a MGET batch frame.
//
// Per-entry request format (after the 7-byte outer header):
//
//	[2B keyLen LE][key bytes]
//
// Response:
//
//	[1B 0x00 OK][4B count LE]
//	count × [1B status][4B valLen LE][value bytes]
func handleMGet(count int, br *bufio.Reader, bw *bufio.Writer, engine *storage.StorageEngine) error {
	if count <= 0 || count > maxBatchCount {
		resp := [5]byte{binStatusErr}
		_, err := bw.Write(resp[:])
		if err == nil {
			err = bw.Flush()
		}
		return err
	}

	keys := make([]string, count)
	var keyLenBuf [2]byte

	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(br, keyLenBuf[:]); err != nil {
			return err
		}
		keyLen := int(binary.LittleEndian.Uint16(keyLenBuf[:]))

		payPtr := binPayloadPool.Get().(*[]byte)
		if cap(*payPtr) < keyLen {
			*payPtr = make([]byte, keyLen)
		} else {
			*payPtr = (*payPtr)[:keyLen]
		}
		if _, err := io.ReadFull(br, (*payPtr)[:keyLen]); err != nil {
			binPayloadPool.Put(payPtr)
			return err
		}
		keys[i] = string((*payPtr)[:keyLen])
		if keyLen <= maxPooledPayload {
			binPayloadPool.Put(payPtr)
		}
	}

	results := engine.MultiGet(keys)

	var respHdr [5]byte
	respHdr[0] = binStatusOK
	binary.LittleEndian.PutUint32(respHdr[1:], uint32(count))
	if _, err := bw.Write(respHdr[:]); err != nil {
		return err
	}

	var entHdr [5]byte
	for _, r := range results {
		if !r.Found || r.Value == nil {
			entHdr[0] = binStatusNotFound
			binary.LittleEndian.PutUint32(entHdr[1:], 0)
			if _, err := bw.Write(entHdr[:]); err != nil {
				return err
			}
			continue
		}
		entHdr[0] = binStatusOK
		binary.LittleEndian.PutUint32(entHdr[1:], uint32(len(r.Value)))
		if _, err := bw.Write(entHdr[:]); err != nil {
			return err
		}
		if _, err := bw.Write(r.Value); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// handleNSScan returns scan results for namespace ns with key prefix.
//
// Response format:
//
//	[1B 0x00 OK][4B count LE]
//	count × [2B keyLen LE][4B valLen LE][key bytes][value bytes]
func handleNSScan(ns, prefix string, limit int, bw *bufio.Writer, engine *storage.StorageEngine) error {
	entries, err := engine.ScanNamespace(ns, prefix, limit)
	if err != nil {
		resp := [5]byte{binStatusErr}
		if _, werr := bw.Write(resp[:]); werr != nil {
			return werr
		}
		return bw.Flush()
	}

	var hdr [5]byte
	hdr[0] = binStatusOK
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(entries)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	var entHdr [6]byte // [2B keyLen][4B valLen]
	for _, e := range entries {
		binary.LittleEndian.PutUint16(entHdr[0:2], uint16(len(e.Key)))
		binary.LittleEndian.PutUint32(entHdr[2:6], uint32(len(e.Value)))
		if _, err := bw.Write(entHdr[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(e.Key); err != nil {
			return err
		}
		if _, err := bw.Write(e.Value); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// handleNSList returns the list of all namespaces with key counts.
//
// Response format:
//
//	[1B 0x00 OK][4B count LE]
//	count × [2B nsLen LE][4B keyCount LE][ns bytes]
func handleNSList(bw *bufio.Writer, engine *storage.StorageEngine) error {
	namespaces := engine.ListNamespaces()

	var hdr [5]byte
	hdr[0] = binStatusOK
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(namespaces)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	var entHdr [6]byte // [2B nsLen][4B keyCount]
	for _, info := range namespaces {
		binary.LittleEndian.PutUint16(entHdr[0:2], uint16(len(info.Namespace)))
		binary.LittleEndian.PutUint32(entHdr[2:6], uint32(info.KeyCount))
		if _, err := bw.Write(entHdr[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(info.Namespace); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// handleHGetAll returns all non-expired fields and their values for hash key.
//
// Response format:
//
//	[1B 0x00 OK][4B count LE]
//	count × [2B fieldLen LE][4B valLen LE][field bytes][value bytes]
func handleHGetAll(key string, bw *bufio.Writer, engine *storage.StorageEngine) error {
	fields, err := engine.HGetAll(key)
	if err != nil {
		resp := [5]byte{binStatusErr}
		if _, werr := bw.Write(resp[:]); werr != nil {
			return werr
		}
		return bw.Flush()
	}
	var hdr [5]byte
	hdr[0] = binStatusOK
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(fields)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	var entHdr [6]byte // [2B fieldLen][4B valLen]
	for _, hf := range fields {
		binary.LittleEndian.PutUint16(entHdr[0:2], uint16(len(hf.Field)))
		binary.LittleEndian.PutUint32(entHdr[2:6], uint32(len(hf.Value)))
		if _, err := bw.Write(entHdr[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(hf.Field); err != nil {
			return err
		}
		if _, err := bw.Write(hf.Value); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// handleHKeys returns the names of all non-expired fields in hash key.
//
// Response format:
//
//	[1B 0x00 OK][4B count LE]
//	count × [2B fieldLen LE][field bytes]
func handleHKeys(key string, bw *bufio.Writer, engine *storage.StorageEngine) error {
	fields := engine.HKeys(key)
	var hdr [5]byte
	hdr[0] = binStatusOK
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(fields)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	var fieldHdr [2]byte
	for _, f := range fields {
		binary.LittleEndian.PutUint16(fieldHdr[0:2], uint16(len(f)))
		if _, err := bw.Write(fieldHdr[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(f); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// listenHost extracts the host part from an addr string like ":9000" or "0.0.0.0:9000".
func listenHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "0.0.0.0"
	}
	return host
}

// listenPort extracts the integer port from an addr string.
func listenPort(addr string) int {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 9000
	}
	port, _ := strconv.Atoi(portStr)
	return port
}

func printBanner(addr, nodeID, dataDir string, diskPaths []string) {
	fmt.Printf(`
╔══════════════════════════════════════════════════╗
║           VeltrixDB — shard-per-core KV          ║
║  ART index · LIRS cache · io_uring scheduler     ║
╚══════════════════════════════════════════════════╝
  node    : %s
  listen  : %s
`, nodeID, addr)

	if len(diskPaths) > 0 {
		fmt.Printf("  disks   : %d NVMe  (%d shards each)\n", len(diskPaths), 256/len(diskPaths))
		for i, p := range diskPaths {
			fmt.Printf("    disk[%d] → %s\n", i, p)
		}
	} else {
		fmt.Printf("  data    : %s\n", dataDir)
	}
	fmt.Println()
}
