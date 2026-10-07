package cluster

// partition_transfer.go — Live data migration between nodes during rebalancing.
//
// When a node is added to the cluster (or removed), the consistent hash ring
// re-assigns some key ranges to different nodes.  PartitionMap.Rebalance()
// updates the routing table but does not move any data.  TransferAgent closes
// that gap: it scans the local key space, copies each key to the members of
// its RF replica set (GetReplicasForKey) that newly need it, and deletes a key
// locally only when this node is no longer one of its replicas and every
// current replica confirmed receipt (MigrateToNewOwners; CLAUDE.md invariant
// 62).  It never runs on a raft node (SetRaftManaged), and a node state change
// (failure / recovery) is not a membership change, so it moves nothing.
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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
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
	scheme       string // "http" or "https"
	tlsEnabled   bool
	boundAddr    atomic.Value // string; actual listen address, set by Start (useful with ":0")
	mux          *http.ServeMux
	// secret, when set, is the shared cluster key every request on this
	// listener must be signed with (SetClusterSecret).
	secret []byte
	// raftManaged: the store is a raft state machine; migration and inbound
	// batches are refused (SetRaftManaged).
	raftManaged atomic.Bool
	// migrateMu serialises MigrateToNewOwners; baseline (guarded by it) is
	// the membership (node ID → rack) the last successful call migrated
	// against — initially the membership when the agent was created.
	migrateMu sync.Mutex
	baseline  map[string]string
	stopOnce  sync.Once
}

// NewTransferAgent creates a plaintext TransferAgent.
//
//	listenAddr   — ":9100" or "0.0.0.0:9100"; the HTTP server address for receiving keys
//	transferAddr — the advertised "<host>:<port>" that OTHER nodes dial to reach this node
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
		baseline:     pm.memberRacks(),
	}

	mux := http.NewServeMux()
	ta.mux = mux
	mux.HandleFunc("/transfer/keys", ta.handleReceive)
	mux.HandleFunc("/transfer/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ta.httpServer = &http.Server{
		Addr:         listenAddr,
		Handler:      ta.authMiddleware(mux),
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

// Stop shuts down the HTTP server. Safe to call more than once.
func (ta *TransferAgent) Stop() {
	ta.stopOnce.Do(func() {
		close(ta.done)
		_ = ta.httpServer.Close()
	})
}

// handleReceive accepts a KeyBatch from another node and applies each key locally.
func (ta *TransferAgent) handleReceive(w http.ResponseWriter, r *http.Request) {
	if ta.RaftManaged() {
		// A key written here would bypass the raft log.
		http.Error(w, ErrMigrationRaftMode.Error(), http.StatusForbidden)
		return
	}
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

// ErrMigrationRaftMode is returned by MigrateToNewOwners, and answered (HTTP
// 403) to inbound /transfer/keys batches, on an agent marked SetRaftManaged.
// In raft mode every node applies the whole log, so each node already holds
// every key; moving keys between nodes would write and delete outside the
// log and destroy committed data.
var ErrMigrationRaftMode = errors.New("partition migration is disabled in raft mode: every node holds the full raft-replicated state")

// SetRaftManaged marks this node's store as a raft state machine. From then
// on MigrateToNewOwners refuses (ErrMigrationRaftMode) and inbound key
// batches are rejected, whoever calls them. cmd/server sets it in --mode=raft.
func (ta *TransferAgent) SetRaftManaged(on bool) { ta.raftManaged.Store(on) }

// RaftManaged reports whether SetRaftManaged(true) was called.
func (ta *TransferAgent) RaftManaged() bool { return ta.raftManaged.Load() }

// MigrateToNewOwners reconciles the local store with the replica sets of the
// current membership, using the replication factor (GetReplicasForKey):
//
//   - A key for which this node is still one of the RF replicas stays. It is
//     copied only to replicas that newly need it — members of its current
//     replica set that were not in its replica set under the membership this
//     agent last migrated against (the "baseline": the membership when the
//     agent was created, advanced after every fully successful call). With
//     an unchanged membership nothing is sent, so a node failure or recovery
//     (a state change, not a membership change) never moves anything.
//   - A key for which this node is no longer a replica is sent to EVERY
//     current replica, and deleted locally only after every one of them
//     acknowledged the batch holding it. A replica that is down keeps the
//     key here until a later call reaches it.
//   - Pinned keys (@vecns/, @idxdef/) are copied ahead of everything else to
//     every destination and never deleted (invariant 55).
//   - Derived keys are sent before records, and a record is deleted only if
//     no key routed as it was left unread (invariant 60).
//
// Returns the first error any destination produced; the baseline is then not
// advanced, so the next call re-sends. Refuses in raft mode
// (ErrMigrationRaftMode).
func (ta *TransferAgent) MigrateToNewOwners() error {
	if ta.RaftManaged() {
		log.Printf("[transfer] migration refused on node=%s: %v", ta.localNodeID, ErrMigrationRaftMode)
		return ErrMigrationRaftMode
	}
	ta.migrateMu.Lock()
	defer ta.migrateMu.Unlock()

	members := ta.pm.memberRacks()
	vnodes, rf := ta.pm.Ring.replicas, ta.pm.ReplicationFactor
	cur := newReplicaPlacement(members, vnodes, rf)
	prev := newReplicaPlacement(ta.baseline, vnodes, rf)

	keys := ta.store.ScanKeys()
	byDest := make(map[string][]KeyValue)
	var pinned []KeyValue
	leaving := make(map[string][]string) // key → replicas that must all ack before the local delete
	unread := make(map[string]bool)      // routing keys with a leaving key we could not read
	copies := 0
	for _, key := range keys {
		if isPinnedKey(key) {
			if val, err := ta.store.Get(key); err == nil {
				pinned = append(pinned, KeyValue{Key: key, Value: val, TTL: ta.store.GetTTLForKey(key)})
			}
			continue
		}
		now := cur.replicas(key)
		if len(now) == 0 {
			continue // empty membership: nowhere to send, nothing to delete
		}
		var targets []string
		stays := containsID(now, ta.localNodeID)
		if stays {
			before := prev.replicas(key)
			for _, id := range now {
				if id != ta.localNodeID && !containsID(before, id) {
					targets = append(targets, id)
				}
			}
		} else {
			targets = now
		}
		if len(targets) == 0 {
			continue
		}
		val, err := ta.store.Get(key)
		if err != nil {
			if !stays {
				unread[RoutingKey(key)] = true
			}
			log.Printf("[transfer] read key=%q: %v", key, err)
			continue
		}
		kv := KeyValue{Key: key, Value: val, TTL: ta.store.GetTTLForKey(key)}
		for _, id := range targets {
			byDest[id] = append(byDest[id], kv)
		}
		if stays {
			copies++
		} else {
			leaving[key] = targets
		}
	}

	if len(byDest) == 0 {
		ta.baseline = members
		log.Printf("[transfer] migration complete — all %d keys already on their replicas", len(keys))
		return nil
	}

	// Pinned keys go first so a namespace's settings apply before its
	// vectors arrive. Derived keys (vectors, text documents, index entries)
	// go before records: only an acknowledged prefix counts as delivered,
	// and deleting a record also deletes its vectors and documents (storage
	// deleteDerivedSearchKeys), so a record must never be deleted while one
	// of its derived keys is unacknowledged.
	for nodeID, kvs := range byDest {
		sort.SliceStable(kvs, func(i, j int) bool {
			return isDerivedKey(kvs[i].Key) && !isDerivedKey(kvs[j].Key)
		})
		byDest[nodeID] = append(append([]KeyValue(nil), pinned...), kvs...)
	}

	// Fan out to all destination nodes in parallel.
	var mu sync.Mutex
	var firstErr error
	acked := make(map[string]map[string]bool, len(byDest)) // nodeID → keys it acknowledged

	var wg sync.WaitGroup
	for nodeID, kvs := range byDest {
		wg.Add(1)
		go func(nodeID string, kvs []KeyValue) {
			defer wg.Done()
			sent, err := ta.sendBatches(nodeID, kvs)
			got := make(map[string]bool, sent)
			for i := len(pinned); i < sent; i++ {
				got[kvs[i].Key] = true
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("node %s: %w", nodeID, err)
				}
				log.Printf("[transfer] send to node=%s error: %v  sent=%d/%d",
					nodeID, err, sent, len(kvs))
			}
			acked[nodeID] = got
		}(nodeID, kvs)
	}
	wg.Wait()

	// Delete a key that left this node only once EVERY current replica has
	// acknowledged it. Derived keys first, then records.
	var deletable []string
	for key, replicas := range leaving {
		ok := true
		for _, id := range replicas {
			if !acked[id][key] {
				ok = false
				break
			}
		}
		if ok && !(isRecordKey(key) && unread[key]) {
			deletable = append(deletable, key)
		}
	}
	sort.SliceStable(deletable, func(i, j int) bool {
		return isDerivedKey(deletable[i]) && !isDerivedKey(deletable[j])
	})
	deleted := 0
	for _, key := range deletable {
		if err := ta.store.Delete(key); err != nil {
			if !isDerivedKey(key) {
				log.Printf("[transfer] local delete key=%q: %v", key, err)
			}
			continue
		}
		deleted++
	}

	if firstErr == nil {
		ta.baseline = members
	}
	log.Printf("[transfer] migration done  copied=%d  moved=%d  kept-unacked=%d  errors=%v",
		copies, deleted, len(leaving)-deleted, firstErr != nil)
	return firstErr
}

// isRecordKey reports whether key is routed as itself (not a derived key).
func isRecordKey(key string) bool { return !isDerivedKey(key) }

func containsID(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// pinnedKeyPrefixes are key families every node needs a copy of, so
// migration copies them to each destination instead of moving them:
// "@vecns/<ns>" holds a vector namespace's configuration (dimension,
// quantization), which each node applies to the vectors it owns.
// "@idxdef/<name>" carries a secondary-index definition the same way.
var pinnedKeyPrefixes = []string{"@vecns/", "@idxdef/"}

// isDerivedKey reports whether key is routed as another key (RoutingKey).
func isDerivedKey(key string) bool { return RoutingKey(key) != key }

func isPinnedKey(key string) bool {
	for _, p := range pinnedKeyPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Cluster request signing (SetClusterSecret). Each request carries
//
//	X-Veltrix-Cluster-Time: unix seconds
//	X-Veltrix-Cluster-Auth: hex HMAC-SHA256(secret, time "\n" method "\n" path "\n" body)
//
// and is rejected unless the MAC matches and the time is within
// clusterAuthMaxSkew of the receiver's clock. Without it, anyone who can
// reach the listener can write keys (/transfer/keys) and run searches
// (/internal/search). TLS with client certificates is the alternative.
const (
	clusterAuthHeader  = "X-Veltrix-Cluster-Auth"
	clusterTimeHeader  = "X-Veltrix-Cluster-Time"
	clusterAuthMaxSkew = 5 * time.Minute
	maxSignedBodyBytes = 256 << 20
)

// SetClusterSecret requires every request on this agent's listener, and
// signs every request it sends, with secret. Every node must use the same
// secret. Call before Start; an empty secret disables signing.
func (ta *TransferAgent) SetClusterSecret(secret []byte) {
	ta.secret = append([]byte(nil), secret...)
}

func signClusterRequest(secret []byte, ts, method, path string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(ts + "\n" + method + "\n" + path + "\n"))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// signRequest adds the auth headers to req (no-op without a secret).
func (ta *TransferAgent) signRequest(req *http.Request, body []byte) {
	if len(ta.secret) == 0 {
		return
	}
	ts := fmt.Sprint(time.Now().Unix())
	req.Header.Set(clusterTimeHeader, ts)
	req.Header.Set(clusterAuthHeader, signClusterRequest(ta.secret, ts, req.Method, req.URL.Path, body))
}

// authMiddleware verifies request signatures when a secret is set. The
// health probe stays open so load balancers need no key.
func (ta *TransferAgent) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(ta.secret) == 0 || r.URL.Path == "/transfer/health" {
			next.ServeHTTP(w, r)
			return
		}
		ts := r.Header.Get(clusterTimeHeader)
		var sec int64
		if _, err := fmt.Sscan(ts, &sec); err != nil {
			http.Error(w, "missing or bad "+clusterTimeHeader, http.StatusUnauthorized)
			return
		}
		if d := time.Since(time.Unix(sec, 0)); d > clusterAuthMaxSkew || d < -clusterAuthMaxSkew {
			http.Error(w, "request time outside the allowed clock skew", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxSignedBodyBytes+1))
		if err != nil || len(body) > maxSignedBodyBytes {
			http.Error(w, "unreadable or oversized body", http.StatusRequestEntityTooLarge)
			return
		}
		want := signClusterRequest(ta.secret, ts, r.Method, r.URL.Path, body)
		if !hmac.Equal([]byte(want), []byte(r.Header.Get(clusterAuthHeader))) {
			http.Error(w, "bad cluster signature", http.StatusUnauthorized)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r)
	})
}

// Handle registers an extra handler on the transfer listener. It shares the
// listener's TLS / mTLS settings, which makes it the channel for
// node-to-node requests such as distributed search. Call before Start.
func (ta *TransferAgent) Handle(pattern string, h http.Handler) {
	ta.mux.Handle(pattern, h)
}

// PostJSON sends in as JSON to path on nodeID's transfer listener and
// decodes the JSON reply into out. A non-200 reply is an error carrying the
// body text.
func (ta *TransferAgent) PostJSON(ctx context.Context, nodeID, path string, in, out interface{}) error {
	addr := ta.pm.GetNodeTransferAddr(nodeID)
	if addr == "" {
		return fmt.Errorf("no transfer address for node %s", nodeID)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ta.scheme+"://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	ta.signRequest(req, body)
	resp, err := ta.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("node %s: HTTP %d: %s", nodeID, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

const transferConnRetries = 3 // attempts on "connection refused" before giving up
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
			req, rerr := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			if rerr != nil {
				return sent, rerr
			}
			req.Header.Set("Content-Type", "application/json")
			ta.signRequest(req, body)
			resp, err = client.Do(req)
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
		// One completed key-batch migration (veltrixdb_cluster_partition_migrations_total).
		ta.pm.metrics.PartitionMigrations.Add(1)
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
