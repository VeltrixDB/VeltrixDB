package cluster

// partition_transfer.go — Live data migration between nodes during rebalancing.
//
// When a node is added to the cluster (or removed), the consistent hash ring
// re-assigns some key ranges to different nodes.  PartitionMap.Rebalance()
// updates the routing table but does not move any data.  TransferAgent closes
// that gap: it scans the local key space, identifies keys whose new owner is a
// different node, streams those keys to the new owner over HTTP in batches, and
// deletes them locally after confirmed receipt.
//
// Protocol
//   POST /transfer/keys        — receive a KeyBatch from another node
//   GET  /transfer/health      — liveness check
//
// Concurrency
//   Outbound migrations run per-destination in separate goroutines so all
//   target nodes receive data in parallel.  Each batch is 500 keys × ≤64 KB
//   average value ≈ 32 MB per HTTP call — large enough to amortise HTTP
//   overhead, small enough to avoid memory pressure.
//
// Integration
//   pm.AddNodeAndRebalance(nodeID, addr, port, ta, partitionCount)
//   handles: AddNode → Rebalance → ta.MigrateToNewOwners() (background).

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LocalStore is the interface TransferAgent uses to interact with the local
// storage engine.  StorageEngine satisfies this interface after the additions
// in storage/engine.go.
type LocalStore interface {
	ScanKeys() []string
	Get(key string) ([]byte, error)
	GetTTLForKey(key string) int32
	Put(key string, value []byte, ttl int32) error
	Delete(key string) error
}

// KeyValue is one key-value pair sent in a transfer batch.
type KeyValue struct {
	Key   string `json:"k"`
	Value []byte `json:"v"`
	TTL   int32  `json:"ttl"` // -1 immortal, 0 no TTL, >0 seconds remaining
}

// KeyBatch is the HTTP body sent from source to destination.
//
// Epoch is the sender's fencing epoch (see epoch.go). The receiver rejects
// batches whose epoch is older than its own with HTTP 409 Conflict — a source
// acting on a pre-partition topology must not write into the newer one.
type KeyBatch struct {
	SourceNode string     `json:"src"`
	Epoch      uint64     `json:"epoch"`
	Keys       []KeyValue `json:"keys"`
}

const (
	transferBatchSize    = 500
	transferHTTPTimeout  = 60 * time.Second
	transferListenSuffix = ":9100" // default transfer HTTP port; override with listenAddr
)

// TransferTLSConfig configures optional TLS for partition-transfer traffic.
// The zero value (or a nil pointer) means plaintext HTTP — the default.
type TransferTLSConfig struct {
	Enabled           bool
	CertFile          string // PEM certificate presented by this node (server side, and client side for mTLS)
	KeyFile           string // PEM private key for CertFile
	CAFile            string // PEM CA bundle used to verify the remote side; "" → system roots
	RequireClientCert bool   // mTLS: the server requires and verifies a client certificate
}

// TransferAgent receives inbound key migrations and drives outbound migrations
// after a rebalance.
type TransferAgent struct {
	pm          *PartitionMap
	localNodeID string
	store       LocalStore
	httpServer  *http.Server
	httpClient  *http.Client
	done        chan struct{}
	// transferAddr is the "<host>:port" this node listens on for inbound transfers.
	transferAddr string
	scheme       string       // "http" or "https"
	tlsEnabled   bool
	boundAddr    atomic.Value // string; actual listen address, set by Start (useful with ":0")
}

// NewTransferAgent creates a plaintext TransferAgent.
//   listenAddr   — ":9100" or "0.0.0.0:9100"; the HTTP server address for receiving keys
//   transferAddr — the advertised "<host>:<port>" that OTHER nodes dial to reach this node
func NewTransferAgent(pm *PartitionMap, localNodeID string, store LocalStore, listenAddr string) *TransferAgent {
	ta, err := NewTransferAgentTLS(pm, localNodeID, store, listenAddr, nil)
	if err != nil {
		// Unreachable: TLS setup is the only error source and tlsCfg is nil.
		log.Printf("[transfer] agent init: %v", err)
	}
	return ta
}

// NewTransferAgentTLS creates a TransferAgent with optional TLS (TLS 1.3
// minimum, stdlib crypto/tls). tlsCfg == nil or tlsCfg.Enabled == false yields
// plaintext HTTP, identical to NewTransferAgent. When enabled:
//   - the receive server serves HTTPS with CertFile/KeyFile;
//   - outbound batches use HTTPS, verifying the remote against CAFile (or
//     system roots) and presenting CertFile as a client certificate;
//   - RequireClientCert additionally makes the server demand and verify a
//     client certificate against CAFile (mutual TLS).
func NewTransferAgentTLS(pm *PartitionMap, localNodeID string, store LocalStore, listenAddr string, tlsCfg *TransferTLSConfig) (*TransferAgent, error) {
	ta := &TransferAgent{
		pm:           pm,
		localNodeID:  localNodeID,
		store:        store,
		done:         make(chan struct{}),
		transferAddr: listenAddr,
		scheme:       "http",
		httpClient:   &http.Client{Timeout: transferHTTPTimeout},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/transfer/keys", ta.handleReceive)
	mux.HandleFunc("/transfer/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ta.httpServer = &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  transferHTTPTimeout,
		WriteTimeout: transferHTTPTimeout,
	}

	if tlsCfg != nil && tlsCfg.Enabled {
		serverTLS, clientTLS, err := buildTransferTLS(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("transfer TLS: %w", err)
		}
		ta.tlsEnabled = true
		ta.scheme = "https"
		ta.httpServer.TLSConfig = serverTLS
		ta.httpClient = &http.Client{
			Timeout:   transferHTTPTimeout,
			Transport: &http.Transport{TLSClientConfig: clientTLS},
		}
	}
	return ta, nil
}

// buildTransferTLS turns a TransferTLSConfig into server- and client-side
// tls.Configs. Both pin the minimum version to TLS 1.3.
func buildTransferTLS(cfg *TransferTLSConfig) (server, client *tls.Config, err error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load key pair: %w", err)
	}

	var caPool *x509.CertPool
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read CA file: %w", err)
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("no certificates parsed from CA file %s", cfg.CAFile)
		}
	}

	server = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if cfg.RequireClientCert {
		if caPool == nil {
			return nil, nil, fmt.Errorf("RequireClientCert needs CAFile to verify client certificates")
		}
		server.ClientCAs = caPool
		server.ClientAuth = tls.RequireAndVerifyClientCert
	}

	client = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert}, // presented when the server asks (mTLS)
		RootCAs:      caPool,                  // nil → system roots
	}
	return server, client, nil
}

// Start binds the listen socket and launches the receive server (HTTP or
// HTTPS per configuration) in a background goroutine. Binding synchronously
// lets callers use ":0" and read the actual port via BoundAddr.
func (ta *TransferAgent) Start() error {
	ln, err := net.Listen("tcp", ta.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("transfer listen %s: %w", ta.httpServer.Addr, err)
	}
	ta.boundAddr.Store(ln.Addr().String())

	go func() {
		var serveErr error
		if ta.tlsEnabled {
			serveErr = ta.httpServer.ServeTLS(ln, "", "") // certs already in TLSConfig
		} else {
			serveErr = ta.httpServer.Serve(ln)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("[transfer] HTTP server error: %v", serveErr)
		}
	}()
	log.Printf("[transfer] agent started  node=%s  listen=%s  tls=%v",
		ta.localNodeID, ln.Addr().String(), ta.tlsEnabled)
	return nil
}

// BoundAddr returns the actual "<host>:<port>" the receive server is bound to
// (resolves ":0"). Empty until Start has been called.
func (ta *TransferAgent) BoundAddr() string {
	if v, ok := ta.boundAddr.Load().(string); ok {
		return v
	}
	return ""
}

// Stop shuts down the HTTP server.
func (ta *TransferAgent) Stop() {
	close(ta.done)
	_ = ta.httpServer.Close()
}

// handleReceive accepts a KeyBatch from another node and applies each key locally.
func (ta *TransferAgent) handleReceive(w http.ResponseWriter, r *http.Request) {
	var batch KeyBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Split-brain fencing: refuse batches from a source whose view of the
	// cluster (its epoch) is older than ours. See epoch.go.
	if err := ta.pm.ValidateEpoch(batch.Epoch); err != nil {
		http.Error(w, fmt.Sprintf("stale epoch %d (local %d) from %s",
			batch.Epoch, ta.pm.Epoch(), batch.SourceNode), http.StatusConflict)
		return
	}

	var failed int
	for _, kv := range batch.Keys {
		if err := ta.store.Put(kv.Key, kv.Value, kv.TTL); err != nil {
			log.Printf("[transfer] receive put key=%q: %v", kv.Key, err)
			failed++
		}
	}

	if failed > 0 {
		http.Error(w, fmt.Sprintf("%d/%d keys failed", failed, len(batch.Keys)),
			http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// MigrateToNewOwners scans all local keys and pushes any key whose new owner
// (per current ring state) is a different node to that node, then deletes it
// locally.  Safe to call concurrently with ongoing reads/writes; Put is atomic
// on the destination before Delete fires on the source.
//
// Returns the first non-nil error if any destination batch fails.  Keys whose
// batch failed are NOT deleted locally so the next call can retry them.
func (ta *TransferAgent) MigrateToNewOwners() error {
	keys := ta.store.ScanKeys()
	if len(keys) == 0 {
		return nil
	}

	// Group keys by destination node ID.
	byDest := make(map[string][]KeyValue)
	for _, key := range keys {
		owner, err := ta.pm.GetNodeForKey(key)
		if err != nil {
			log.Printf("[transfer] route key=%q: %v", key, err)
			continue
		}
		if owner == ta.localNodeID {
			continue // this key stays here
		}
		val, err := ta.store.Get(key)
		if err != nil {
			log.Printf("[transfer] read key=%q: %v", key, err)
			continue
		}
		ttl := ta.store.GetTTLForKey(key)
		byDest[owner] = append(byDest[owner], KeyValue{Key: key, Value: val, TTL: ttl})
	}

	if len(byDest) == 0 {
		log.Printf("[transfer] migration complete — all %d keys already on correct nodes", len(keys))
		return nil
	}

	// Fan out to all destination nodes in parallel.
	var mu sync.Mutex
	var firstErr error
	successByDest := make(map[string][]string) // nodeID → successfully-sent keys

	var wg sync.WaitGroup
	for nodeID, kvs := range byDest {
		wg.Add(1)
		go func(nodeID string, kvs []KeyValue) {
			defer wg.Done()
			sent, err := ta.sendBatches(nodeID, kvs)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("node %s: %w", nodeID, err)
				}
				log.Printf("[transfer] send to node=%s error: %v  sent=%d/%d",
					nodeID, err, sent, len(kvs))
			}
			successByDest[nodeID] = make([]string, sent)
			for i := 0; i < sent; i++ {
				successByDest[nodeID][i] = kvs[i].Key
			}
		}(nodeID, kvs)
	}
	wg.Wait()

	// Delete only the keys that were successfully delivered.
	for _, keys := range successByDest {
		for _, key := range keys {
			if err := ta.store.Delete(key); err != nil {
				log.Printf("[transfer] local delete key=%q: %v", key, err)
			}
		}
	}

	totalMoved := 0
	for _, keys := range successByDest {
		totalMoved += len(keys)
	}
	log.Printf("[transfer] migration done  moved=%d  errors=%v", totalMoved, firstErr != nil)
	return firstErr
}

const transferConnRetries = 3         // attempts on "connection refused" before giving up
const transferConnRetryDelay = 100 * time.Millisecond

// sendBatches sends kvs to nodeID in batches.  Returns (number successfully
// sent, first error).  On error the caller retains ownership of unsent keys.
//
// "Connection refused" is retried up to transferConnRetries times with a short
// back-off — this covers the window where a surviving node's transfer server
// has been started but hasn't finished binding its listen socket yet.
func (ta *TransferAgent) sendBatches(nodeID string, kvs []KeyValue) (sent int, err error) {
	addr := ta.pm.GetNodeTransferAddr(nodeID)
	if addr == "" {
		return 0, fmt.Errorf("no transfer address for node %s", nodeID)
	}
	url := ta.scheme + "://" + addr + "/transfer/keys"
	client := ta.httpClient

	for i := 0; i < len(kvs); i += transferBatchSize {
		end := i + transferBatchSize
		if end > len(kvs) {
			end = len(kvs)
		}
		batch := KeyBatch{SourceNode: ta.localNodeID, Epoch: ta.pm.Epoch(), Keys: kvs[i:end]}
		body, merr := json.Marshal(batch)
		if merr != nil {
			return sent, fmt.Errorf("marshal batch: %w", merr)
		}

		var resp *http.Response
		for attempt := 0; attempt < transferConnRetries; attempt++ {
			resp, err = client.Post(url, "application/json", bytes.NewReader(body))
			if err == nil {
				break
			}
			// Only retry on "connection refused" — all other errors are terminal.
			var netErr *net.OpError
			if errors.As(err, &netErr) && isConnRefused(netErr) && attempt < transferConnRetries-1 {
				log.Printf("[transfer] node=%s connection refused (attempt %d/%d) — retrying in %s",
					nodeID, attempt+1, transferConnRetries, transferConnRetryDelay)
				time.Sleep(transferConnRetryDelay)
				continue
			}
			return sent, fmt.Errorf("http post: %w", err)
		}
		if err != nil {
			return sent, fmt.Errorf("http post: %w", err)
		}

		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return sent, fmt.Errorf("remote HTTP %d", resp.StatusCode)
		}
		sent += end - i
	}
	return sent, nil
}

// isConnRefused reports whether a net.OpError is a "connection refused" error.
func isConnRefused(e *net.OpError) bool {
	if e.Op != "dial" {
		return false
	}
	// syscall.ECONNREFUSED on Linux/macOS; "actively refused" on Windows.
	msg := e.Err.Error()
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "actively refused")
}

// ── PartitionMap additions ────────────────────────────────────────────────────

// transferAddrs maps nodeID → "<host>:transferPort".  Populated by
// AddNodeWithTransfer.
var transferAddrs sync.Map // map[string]string

// GetNodeTransferAddr returns the transfer HTTP address for nodeID, or "".
func (pm *PartitionMap) GetNodeTransferAddr(nodeID string) string {
	if v, ok := transferAddrs.Load(nodeID); ok {
		return v.(string)
	}
	// Fall back to Node.Address + ":9100" for nodes registered without a
	// dedicated transfer address.
	pm.mu.RLock()
	n, ok := pm.Nodes[nodeID]
	pm.mu.RUnlock()
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d", n.Address, 9100)
}

// GetNodeAddr returns the "<host>:port" of a node, or "".
func (pm *PartitionMap) GetNodeAddr(nodeID string) string {
	pm.mu.RLock()
	n, ok := pm.Nodes[nodeID]
	pm.mu.RUnlock()
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s:%d", n.Address, n.Port)
}

// SetNodeTransferAddr registers the transfer HTTP address for an
// already-known node (bootstrap peers registered via AddNode).
func (pm *PartitionMap) SetNodeTransferAddr(nodeID, transferAddr string) {
	transferAddrs.Store(nodeID, transferAddr)
}

// AddNodeWithTransfer adds a node AND registers its dedicated transfer address.
func (pm *PartitionMap) AddNodeWithTransfer(nodeID, address string, port int, transferAddr string) error {
	if err := pm.AddNode(nodeID, address, port); err != nil {
		return err
	}
	transferAddrs.Store(nodeID, transferAddr)
	return nil
}

// AddNodeAndRebalance adds nodeID to the cluster, recomputes the partition map,
// and fires off an asynchronous migration using ta (if non-nil).
//
// The migration runs in the background; AddNodeAndRebalance returns as soon as
// Rebalance() succeeds.  Monitor ta.MigrateToNewOwners' log output for progress.
func (pm *PartitionMap) AddNodeAndRebalance(
	nodeID, address string,
	port int,
	transferAddr string,
	partitionCount uint32,
	ta *TransferAgent,
) error {
	if err := pm.AddNodeWithTransfer(nodeID, address, port, transferAddr); err != nil {
		return err
	}
	if err := pm.Rebalance(partitionCount); err != nil {
		return err
	}
	if ta != nil {
		go func() {
			log.Printf("[transfer] starting migration after adding node=%s", nodeID)
			if err := ta.MigrateToNewOwners(); err != nil {
				log.Printf("[transfer] migration error: %v", err)
			}
		}()
	}
	return nil
}

// RemoveNodeAndRebalance gracefully removes a healthy departing node: it marks
// the node as DRAINING, removes it from the ring, rebalances partition
// assignments, then evacuates the departing node's local keys to their new
// owners via ta.
//
// ta MUST be the departing node's own TransferAgent (ta.localNodeID == nodeID).
// Passing a coordinator's TA would scan the wrong store and silently orphan all
// of nodeID's keys.  Pass ta=nil to skip evacuation (e.g. data is already
// replicated and no explicit drain is needed).
//
// For dead or unresponsive nodes use ForceRemoveNode instead.
func (pm *PartitionMap) RemoveNodeAndRebalance(
	nodeID string,
	partitionCount uint32,
	ta *TransferAgent,
) error {
	if ta != nil && ta.localNodeID != nodeID {
		return fmt.Errorf("RemoveNodeAndRebalance: ta.localNodeID %q != nodeID %q; pass the departing node's own TransferAgent or nil", ta.localNodeID, nodeID)
	}

	// Transition to DRAINING before touching the ring so health checks and
	// metrics can observe the graceful departure rather than a hard disappearance.
	if err := pm.UpdateNodeState(nodeID, NodeStateDraining); err != nil {
		return fmt.Errorf("set node draining: %w", err)
	}

	if err := pm.RemoveNode(nodeID); err != nil {
		return err
	}
	transferAddrs.Delete(nodeID)

	// Rebalance BEFORE migration so MigrateToNewOwners sees the updated ring
	// and correctly identifies which keys have moved to surviving nodes.
	if err := pm.Rebalance(partitionCount); err != nil {
		// Ring is already modified; partitions are unassigned.  Return the error
		// so the caller can retry Rebalance or fall back to ForceRemoveNode.
		return fmt.Errorf("rebalance after removing node %s: %w", nodeID, err)
	}

	if ta != nil {
		go func() {
			log.Printf("[transfer] evacuating node=%s", nodeID)
			if err := ta.MigrateToNewOwners(); err != nil {
				log.Printf("[transfer] evacuation error node=%s: %v", nodeID, err)
			} else {
				log.Printf("[transfer] evacuation complete node=%s", nodeID)
			}
		}()
	}
	return nil
}

// ForceRemoveNode removes a dead or unresponsive node from the ring and
// rebalances partition assignments without attempting data evacuation.
// Surviving replicas remain the source of truth; the replication engine
// is responsible for re-replicating to restore the desired RF.
// Use RemoveNodeAndRebalance instead when the departing node is healthy.
func (pm *PartitionMap) ForceRemoveNode(nodeID string, partitionCount uint32) error {
	if err := pm.RemoveNode(nodeID); err != nil {
		return err
	}
	transferAddrs.Delete(nodeID)
	return pm.Rebalance(partitionCount)
}
