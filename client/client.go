package client

// client.go — cluster-aware VeltrixDB client.
//
// This replaces the previous mock (which used a wrong djb2 hash and a no-op
// connection) with a real client that:
//
//   1. Fetches cluster topology over the storage port (the TOPOLOGY command)
//      and builds a consistent-hash ring IDENTICAL to the server's
//      (cluster.NewConsistentHashRing with the same virtual-node count, keyed by
//      cluster.HashKey).  Routing a key therefore selects the exact owner node
//      the server would pick — no guessing.
//
//   2. Follows MOVED redirects (Redis-cluster style).  In raft mode only the
//      leader accepts writes; a write sent to a follower is answered with
//      "ERR MOVED <addr> <id>", which the client transparently retries against
//      the named node and caches for subsequent writes.
//
//   3. Reuses one persistent TCPConn per node address, dialing lazily and
//      re-dialing on error.
//
// The low-level single-node client (TCPConn in tcp.go, BinaryConn in
// pipeline.go) is unchanged and remains the right choice when you already know
// which node owns your keys.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
)

// virtualNodesPerNode must match the server's ClusterConfig.VirtualNodesPerNode
// (cluster.DefaultClusterConfig) so the client and server rings agree.
const virtualNodesPerNode = 64

// maxRedirects bounds how many MOVED hops a single request will follow.
const maxRedirects = 3

// ConsistencyLevel defines read/write consistency (advisory; the server
// enforces the actual guarantee per its --mode / --consistency configuration).
type ConsistencyLevel int

const (
	EventualConsistency ConsistencyLevel = iota
	StrongConsistency
	QuorumConsistency
)

// ClientConfig contains client configuration.
type ClientConfig struct {
	ConnectionTimeoutMs int32
	RequestTimeoutMs    int32
	ConsistencyLevel    ConsistencyLevel
	RetryCount          int
	BackoffBaseMs       int32
	RefreshIntervalMs   int32
}

// DefaultClientConfig returns sensible defaults.
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ConnectionTimeoutMs: 5000,
		RequestTimeoutMs:    1000,
		ConsistencyLevel:    QuorumConsistency,
		RetryCount:          5,
		BackoffBaseMs:       50,
		RefreshIntervalMs:   30000,
	}
}

// topologyDoc mirrors the JSON returned by the TOPOLOGY command / /admin/cluster.
type topologyDoc struct {
	Epoch uint64 `json:"epoch"`
	Raft  *struct {
		LeaderID string `json:"leader_id"`
	} `json:"raft"`
	Nodes []struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
		Port    int    `json:"port"`
		State   string `json:"state"`
	} `json:"nodes"`
}

// Client is a cluster-aware VeltrixDB client.
type Client struct {
	config *ClientConfig

	mu       sync.RWMutex
	seeds    []string
	ring     *cluster.ConsistentHashRing
	nodeAddr map[string]string // nodeID → "host:port"
	epoch    uint64

	connMu sync.Mutex
	conns  map[string]*TCPConn

	done chan struct{}
}

// NewClient creates a cluster-aware client from one or more seed node
// addresses ("host:port").  It fetches the initial topology; if that fails the
// client still works in seed-only mode (hashing across the seed addresses and
// following redirects).
func NewClient(seedAddresses []string, config *ClientConfig) (*Client, error) {
	if len(seedAddresses) == 0 {
		return nil, fmt.Errorf("at least one seed address required")
	}
	if config == nil {
		config = DefaultClientConfig()
	}
	c := &Client{
		config:   config,
		seeds:    append([]string(nil), seedAddresses...),
		nodeAddr: make(map[string]string),
		conns:    make(map[string]*TCPConn),
		done:     make(chan struct{}),
	}
	// Best-effort initial topology fetch; seed-only routing works regardless.
	_ = c.refreshTopology()
	go c.backgroundRefresh()
	return c, nil
}

func (c *Client) dialTimeout() time.Duration {
	return time.Duration(c.config.ConnectionTimeoutMs) * time.Millisecond
}

// getConn returns a cached connection to addr, dialing on demand.
func (c *Client) getConn(addr string) (*TCPConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if tc, ok := c.conns[addr]; ok {
		return tc, nil
	}
	tc, err := DialTCP(addr, c.dialTimeout())
	if err != nil {
		return nil, err
	}
	c.conns[addr] = tc
	return tc, nil
}

// dropConn closes and forgets a connection (called after a network error).
func (c *Client) dropConn(addr string) {
	c.connMu.Lock()
	if tc, ok := c.conns[addr]; ok {
		tc.Close()
		delete(c.conns, addr)
	}
	c.connMu.Unlock()
}

// addrForKey returns the storage address of the node that owns key, using the
// consistent-hash ring when topology is known, else hashing across seeds.
func (c *Client) addrForKey(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.ring != nil {
		if id, err := c.ring.GetNode(cluster.HashKey(key)); err == nil {
			if addr, ok := c.nodeAddr[id]; ok {
				return addr
			}
		}
	}
	// Seed-only fallback.
	idx := int(cluster.HashKey(key) % uint64(len(c.seeds)))
	return c.seeds[idx]
}

// refreshTopology fetches topology from any reachable node/seed and rebuilds
// the ring.
func (c *Client) refreshTopology() error {
	var lastErr error
	for _, addr := range c.knownAddrs() {
		doc, err := c.fetchTopology(addr)
		if err != nil {
			lastErr = err
			continue
		}
		if len(doc.Nodes) == 0 {
			continue
		}
		ring := cluster.NewConsistentHashRing(virtualNodesPerNode)
		nodeAddr := make(map[string]string, len(doc.Nodes))
		for _, n := range doc.Nodes {
			ring.AddNode(n.NodeID)
			nodeAddr[n.NodeID] = fmt.Sprintf("%s:%d", n.Address, n.Port)
		}
		c.mu.Lock()
		c.ring = ring
		c.nodeAddr = nodeAddr
		c.epoch = doc.Epoch
		c.mu.Unlock()
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no topology available")
	}
	return lastErr
}

// knownAddrs returns all addresses worth trying: known nodes first, then seeds.
func (c *Client) knownAddrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seen := make(map[string]bool)
	var out []string
	for _, a := range c.nodeAddr {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	for _, a := range c.seeds {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// fetchTopology sends TOPOLOGY to addr and parses the JSON reply.
func (c *Client) fetchTopology(addr string) (*topologyDoc, error) {
	tc, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	line, err := tc.Topology()
	if err != nil {
		c.dropConn(addr)
		return nil, err
	}
	var doc topologyDoc
	if err := json.Unmarshal([]byte(line), &doc); err != nil {
		return nil, fmt.Errorf("parse topology: %w", err)
	}
	return &doc, nil
}

func (c *Client) backgroundRefresh() {
	interval := time.Duration(c.config.RefreshIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			_ = c.refreshTopology()
		}
	}
}

// ── Options ──────────────────────────────────────────────────────────────────

type putOptions struct {
	TTL              int32
	ConsistencyLevel ConsistencyLevel
}

// PutOption customizes a Put.
type PutOption func(*putOptions)

// WithTTL sets a per-key TTL in seconds (-1 = immortal).
func WithTTL(ttlSeconds int32) PutOption { return func(o *putOptions) { o.TTL = ttlSeconds } }

// WithConsistency requests a consistency level (advisory).
func WithConsistency(level ConsistencyLevel) PutOption {
	return func(o *putOptions) { o.ConsistencyLevel = level }
}

type getOptions struct{ ConsistencyLevel ConsistencyLevel }

// GetOption customizes a Get.
type GetOption func(*getOptions)

// ── Public API ──────────────────────────────────────────────────────────────

// Put stores key=value, routing to the owner node and following MOVED
// redirects (to the raft leader) as needed.
func (c *Client) Put(ctx context.Context, key string, value []byte, opts ...PutOption) error {
	o := &putOptions{TTL: -1, ConsistencyLevel: c.config.ConsistencyLevel}
	for _, fn := range opts {
		fn(o)
	}
	return c.writeWithRedirect(key, func(tc *TCPConn) error {
		return tc.Put(key, value)
	})
}

// Delete removes key, with the same routing/redirect behaviour as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.writeWithRedirect(key, func(tc *TCPConn) error {
		return tc.Delete(key)
	})
}

// Get retrieves key.  Reads are served from the contacted node's local state
// (fast, possibly stale in raft/replicated modes — see the server consistency
// notes).  Returns (nil, nil) when the key is absent.
func (c *Client) Get(ctx context.Context, key string, opts ...GetOption) ([]byte, error) {
	addr := c.addrForKey(key)
	var lastErr error
	for attempt := 0; attempt < c.retries(); attempt++ {
		tc, err := c.getConn(addr)
		if err != nil {
			lastErr = err
			c.backoff(attempt)
			_ = c.refreshTopology()
			addr = c.addrForKey(key)
			continue
		}
		val, err := tc.Get(key)
		if err != nil {
			c.dropConn(addr)
			lastErr = err
			c.backoff(attempt)
			continue
		}
		return val, nil
	}
	return nil, fmt.Errorf("get %q failed: %w", key, lastErr)
}

// writeWithRedirect runs op against the key's owner, following MOVED redirects.
func (c *Client) writeWithRedirect(key string, op func(*TCPConn) error) error {
	addr := c.addrForKey(key)
	var lastErr error
	for attempt := 0; attempt < c.retries(); attempt++ {
		tc, err := c.getConn(addr)
		if err != nil {
			lastErr = err
			c.backoff(attempt)
			_ = c.refreshTopology()
			addr = c.addrForKey(key)
			continue
		}

		err = op(tc)
		if err == nil {
			return nil
		}

		// MOVED redirect: retry against the named leader, following further hops.
		if target, ok := parseMoved(err.Error()); ok {
			lastErr = err
			if target != "" {
				for hop := 0; hop < maxRedirects && target != ""; hop++ {
					rtc, derr := c.getConn(target)
					if derr != nil {
						lastErr = derr
						break
					}
					addr = target
					rerr := op(rtc)
					if rerr == nil {
						return nil
					}
					if next, ok2 := parseMoved(rerr.Error()); ok2 {
						target = next
						continue
					}
					lastErr = rerr
					break
				}
			}
			// Leader unknown or election in progress: back off and retry.
			_ = c.refreshTopology()
			c.backoff(attempt)
			addr = c.addrForKey(key)
			continue
		}

		// Network-level error: drop the conn and retry elsewhere.
		c.dropConn(addr)
		lastErr = err
		c.backoff(attempt)
		_ = c.refreshTopology()
		addr = c.addrForKey(key)
	}
	return fmt.Errorf("write %q failed: %w", key, lastErr)
}

func (c *Client) retries() int {
	if c.config.RetryCount > 0 {
		return c.config.RetryCount
	}
	return 5
}

func (c *Client) backoff(attempt int) {
	base := c.config.BackoffBaseMs
	if base <= 0 {
		base = 50
	}
	time.Sleep(time.Duration(base*int32(attempt+1)) * time.Millisecond)
}

// Topology returns the current known node addresses (id → "host:port") and the
// observed cluster epoch.  Triggers a refresh if none is cached.
func (c *Client) Topology() (map[string]string, uint64) {
	c.mu.RLock()
	empty := len(c.nodeAddr) == 0
	c.mu.RUnlock()
	if empty {
		_ = c.refreshTopology()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.nodeAddr))
	for id, a := range c.nodeAddr {
		out[id] = a
	}
	return out, c.epoch
}

// Close closes all connections and stops background refresh.
func (c *Client) Close() error {
	close(c.done)
	c.connMu.Lock()
	for _, tc := range c.conns {
		tc.Close()
	}
	c.conns = make(map[string]*TCPConn)
	c.connMu.Unlock()
	return nil
}

// parseMoved extracts the redirect target from a "MOVED <addr> <id>" error
// string (surfaced by the server on a non-leader write).  Returns (addr, true)
// when the message is a redirect; addr is "" for "MOVED -" (leader unknown).
func parseMoved(msg string) (string, bool) {
	i := strings.Index(msg, "MOVED ")
	if i < 0 {
		return "", false
	}
	fields := strings.Fields(strings.TrimSpace(msg[i+len("MOVED "):]))
	if len(fields) == 0 || fields[0] == "-" {
		return "", true
	}
	return fields[0], true
}
