package main

// membership.go — online cluster membership: add and remove nodes without
// restarting the cluster (CLAUDE.md invariant 67).
//
//   Admin API (metrics/admin listener, same auth as every /admin/* endpoint):
//     GET    /admin/cluster/members            membership, roles, version
//     POST   /admin/cluster/members            {"id","addr"[,"wait_ms"]}  add a node
//     DELETE /admin/cluster/members/{id}       [?force=true&wait_ms=N]    remove a node
//   CLI: veltrix cluster members | add-node id@host:port | remove-node id
//
//   Node-to-node (transfer listener; HMAC with the cluster secret or mTLS —
//   invariant 56; refused with 403 on an unauthenticated listener):
//     POST /cluster/members      a change forwarded to the raft leader / the
//                                replicated-mode membership coordinator
//     POST /cluster/join         a --join node asking to be added
//     POST /cluster/membership   replicated: this node's membership version
//     POST /cluster/membership/apply  replicated: install a newer version
//     POST /cluster/drain        replicated: drain this node before removal
//
// --mode=raft: membership is the raft configuration (consensus/membership.go)
// — in the log and the snapshot header, so it survives restarts and wins
// over --peers once a change was made.  A node is added as a learner, caught
// up (entries or chunked InstallSnapshot) and promoted to voter by the
// leader; removal is one config entry, a removed leader steps down after it
// commits.  Every node learns new members' client addresses from the
// configuration (Configuration.Addrs) and updates its raft transport, MOVED
// redirects, TOPOLOGY and the partition map (voters only).  Raft mode never
// moves keys: every node applies the whole log (invariant 62).
//
// --mode=replicated: membership_replicated.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/cluster"
	"github.com/VeltrixDB/veltrixdb/consensus"
	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

const (
	// membershipFile holds the replicated-mode membership (<data>/...).
	membershipFile = "cluster_membership.json"

	defaultAddWait    = 60 * time.Second
	maxAddWait        = 10 * time.Minute
	defaultDrainWait  = 2 * time.Minute
	joinRetryInterval = 2 * time.Second
	membershipPull    = 10 * time.Second
	memberRPCTimeout  = maxAddWait + 30*time.Second
)

// memberInfo is one member as the admin API reports it.
type memberInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`           // client host:port
	Role string `json:"role,omitempty"` // raft: voter | learner; replicated: member
}

// membersView is GET /admin/cluster/members.
type membersView struct {
	Mode        string       `json:"mode"`
	NodeID      string       `json:"node_id"`
	Version     uint64       `json:"version"`   // raft: config log index (0 = bootstrap --peers); replicated: membership version
	Committed   bool         `json:"committed"` // raft: the config entry is committed
	Leader      string       `json:"leader,omitempty"`
	Coordinator string       `json:"coordinator,omitempty"` // replicated: who serialises changes
	SelfStatus  string       `json:"self_status"`           // voter | learner | member | joining | removed | draining
	Members     []memberInfo `json:"members"`
}

// memberChange is one requested change (admin API body, forwarded body).
type memberChange struct {
	Op     string `json:"op"` // add | remove | join
	ID     string `json:"id"`
	Addr   string `json:"addr,omitempty"`
	WaitMs int    `json:"wait_ms,omitempty"`
	Force  bool   `json:"force,omitempty"` // replicated remove: skip the drain (dead node)
	Hops   int    `json:"hops,omitempty"`  // forwarded once at most
}

// changeResult is the answer to a change.
type changeResult struct {
	Status  string       `json:"status"` // voter | learner | removed | member | ok
	Detail  string       `json:"detail,omitempty"`
	Version uint64       `json:"version,omitempty"`
	Members []memberInfo `json:"members,omitempty"`
	// Failed: replicated mode — members that did not take the new version
	// yet (they pull it within membershipPull).
	Failed []string `json:"failed,omitempty"`
}

// httpAnswer is a status + JSON (or relayed raw) body.
type httpAnswer struct {
	status int
	body   interface{}
	raw    string // relayed body of a forwarded request (overrides body)
}

func answer(status int, body interface{}) httpAnswer { return httpAnswer{status: status, body: body} }

func answerErr(status int, format string, a ...interface{}) httpAnswer {
	return httpAnswer{status: status, body: map[string]string{"error": fmt.Sprintf(format, a...)}}
}

func (a httpAnswer) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(a.status)
	if a.raw != "" {
		_, _ = w.Write([]byte(a.raw + "\n"))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(a.body)
}

// membershipParams configures newMembershipManager.
type membershipParams struct {
	mode        deployMode
	localID     string
	selfAddr    string // advertised client address
	dataDir     string
	pm          *cluster.PartitionMap
	engine      *storage.StorageEngine
	staticPeers []peerSpec
	joinSeeds   []string // client addresses of existing members (--join)
	forceReset  bool     // --force-new-cluster
	secret      []byte
	transferTLS *cluster.TransferTLSConfig
	mtls        bool
}

// membershipManager owns online membership for one node.
type membershipManager struct {
	membershipParams
	authOK bool // transfer listener is authenticated (secret or mTLS)
	client *cluster.SignedClient

	coord    *coordinator
	attached chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// raft
	cfgCh chan consensus.Configuration // latest effective configuration

	// replicated (membership_replicated.go)
	repl       *replication.ReplicationEngine
	applyMu    sync.Mutex // one applyReplState at a time (push vs pull)
	stMu       sync.Mutex
	st         replState
	changeMu   sync.Mutex
	removed    atomic.Bool
	joining    atomic.Bool
	drainUntil atomic.Int64 // UnixNano; writes refused before it
}

func newMembershipManager(p membershipParams) (*membershipManager, error) {
	mm := &membershipManager{
		membershipParams: p,
		authOK:           len(p.secret) > 0 || p.mtls,
		attached:         make(chan struct{}),
		stopCh:           make(chan struct{}),
		cfgCh:            make(chan consensus.Configuration, 1),
	}
	c, err := cluster.NewSignedClient(p.secret, p.transferTLS, memberRPCTimeout)
	if err != nil {
		return nil, err
	}
	mm.client = c
	if p.mode == modeRaft {
		mm.wg.Add(1)
		go mm.raftApplier()
	}
	return mm, nil
}

// attach wires the coordinator (built after the raft node, whose first
// configuration may already be queued).
func (mm *membershipManager) attach(c *coordinator) {
	mm.coord = c
	c.mm = mm
	if mm.mode == modeReplicated {
		mm.repl = c.repl
	}
	close(mm.attached)
}

func (mm *membershipManager) stop() {
	mm.stopOnce.Do(func() { close(mm.stopCh) })
	mm.wg.Wait()
}

// start launches the background loops (after the listeners are up).
func (mm *membershipManager) start() {
	if len(mm.joinSeeds) > 0 {
		mm.wg.Add(1)
		go mm.joinLoop()
	}
	if mm.mode == modeReplicated {
		mm.wg.Add(1)
		go mm.pullLoop()
	}
}

// ── addresses ───────────────────────────────────────────────────────────────

// splitClientAddr parses a member's client address.
func splitClientAddr(a string) (string, int, error) {
	host, ps, err := net.SplitHostPort(a)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port <= 0 || port > 65535-offTransfer {
		return "", 0, fmt.Errorf("bad port in %q", a)
	}
	return host, port, nil
}

// validMemberAddr: a dialable host:port (not a wildcard bind address).
func validMemberAddr(a string) error {
	host, _, err := splitClientAddr(a)
	if err != nil {
		return fmt.Errorf("bad member address %q (want host:port, the node's client --addr): %v", a, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return fmt.Errorf("member address %q has no dialable host", a)
	}
	return nil
}

func validMemberID(id string) error {
	if id == "" || strings.ContainsAny(id, "@,/ \t\n") {
		return fmt.Errorf("bad member id %q", id)
	}
	return nil
}

// selfInPeers reports whether the raw --peers value names id.
func selfInPeers(spec, id string) bool {
	for _, raw := range strings.Split(spec, ",") {
		raw = strings.TrimSpace(raw)
		if at := strings.Index(raw, "@"); at > 0 && raw[:at] == id {
			return true
		}
	}
	return false
}

// staticAddr returns id's client address from --peers (or self).
func (mm *membershipManager) staticAddr(id string) string {
	if id == mm.localID {
		return mm.selfAddr
	}
	for _, p := range mm.staticPeers {
		if p.id == id {
			return p.clientAddr
		}
	}
	return ""
}

// transferAddrOf returns the transfer-listener address of a member.
func (mm *membershipManager) transferAddrOf(id, clientAddr string) string {
	if clientAddr != "" {
		return deriveAddr(clientAddr, offTransfer)
	}
	if a := mm.pm.GetNodeTransferAddr(id); a != "" {
		return a
	}
	return ""
}

// ── coordinator side ────────────────────────────────────────────────────────

// peerAddr returns a member's client address (MOVED redirects).
func (c *coordinator) peerAddr(id string) string {
	c.peerMu.RLock()
	defer c.peerMu.RUnlock()
	return c.peerClientAddr[id]
}

// setPeerAddrs merges member client addresses into the redirect map.
func (c *coordinator) setPeerAddrs(addrs map[string]string) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	if c.peerClientAddr == nil {
		c.peerClientAddr = make(map[string]string, len(addrs))
	}
	for id, a := range addrs {
		if a != "" {
			c.peerClientAddr[id] = a
		}
	}
}

// peerAddrs copies the redirect map.
func (c *coordinator) peerAddrs() map[string]string {
	c.peerMu.RLock()
	defer c.peerMu.RUnlock()
	out := make(map[string]string, len(c.peerClientAddr))
	for k, v := range c.peerClientAddr {
		out[k] = v
	}
	return out
}

// writeGate refuses replicated-mode writes on a node that is joining, being
// drained for removal, or was removed: a write accepted there could miss the
// replicas (invariant 67).  nil in every other case.
func (c *coordinator) writeGate() error {
	if c.mm == nil || c.mode != modeReplicated {
		return nil
	}
	return c.mm.writeGateErr()
}

func gateErrs(n int, err error) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return errs
}

// ── raft ────────────────────────────────────────────────────────────────────

// raftConfigHook is consensus.Options.OnConfigChange: it runs under the raft
// node's lock, so it only registers transport addresses (before a replicator
// for a new member starts) and queues the configuration for raftApplier.
func (mm *membershipManager) raftConfigHook(tr *consensus.TCPTransport) func(consensus.Configuration) {
	return func(cfg consensus.Configuration) {
		for _, id := range cfg.Members() {
			if id == mm.localID {
				continue
			}
			a := cfg.Addrs[id]
			if a == "" {
				a = mm.staticAddr(id)
			}
			if a != "" && tr != nil {
				tr.AddPeer(id, deriveAddr(a, offRaft))
			}
		}
		select {
		case mm.cfgCh <- cfg:
		default:
			select {
			case <-mm.cfgCh: // drop the older one: only the latest matters
			default:
			}
			select {
			case mm.cfgCh <- cfg:
			default:
			}
		}
	}
}

func (mm *membershipManager) raftApplier() {
	defer mm.wg.Done()
	select {
	case <-mm.attached:
	case <-mm.stopCh:
		return
	}
	for {
		select {
		case <-mm.stopCh:
			return
		case cfg := <-mm.cfgCh:
			mm.applyRaftConfig(cfg)
		}
	}
}

// memberAddrsRaft resolves every member's client address.
func (mm *membershipManager) memberAddrsRaft(cfg consensus.Configuration) map[string]string {
	out := make(map[string]string)
	for _, id := range cfg.Members() {
		a := cfg.Addrs[id]
		if a == "" {
			a = mm.staticAddr(id)
		}
		if a == "" {
			a = mm.pm.GetNodeAddr(id)
		}
		if a != "" {
			out[id] = a
		}
	}
	return out
}

// applyRaftConfig makes the redirect map and the partition map (voters)
// match a new effective raft configuration.  Raft mode moves no keys.
func (mm *membershipManager) applyRaftConfig(cfg consensus.Configuration) {
	addrs := mm.memberAddrsRaft(cfg)
	mm.coord.setPeerAddrs(addrs)
	if len(cfg.Servers) == 0 {
		return // --join node not added yet: keep the bootstrap view
	}
	want := make(map[string]bool, len(cfg.Servers))
	for _, id := range cfg.Servers {
		want[id] = true
	}
	existing := mm.pm.GetNodeStats()
	changed := false
	for _, id := range cfg.Servers {
		if _, ok := existing[id]; ok {
			continue
		}
		a := addrs[id]
		host, port, err := splitClientAddr(a)
		if err != nil {
			log.Printf("[membership] voter %s has no usable address (%q) — not in the partition map", id, a)
			continue
		}
		if err := mm.pm.AddNode(id, host, port); err != nil {
			continue
		}
		_ = mm.pm.SetNodeGossipAddr(id, deriveAddr(a, offGossip))
		mm.pm.SetNodeTransferAddr(id, deriveAddr(a, offTransfer))
		changed = true
		log.Printf("[membership] raft voter %s (%s) added to the partition map", id, a)
	}
	for id := range existing {
		if id == mm.localID || want[id] {
			continue
		}
		if err := mm.pm.RemoveNode(id); err == nil {
			mm.pm.ForgetNodeTransferAddr(id)
			changed = true
			log.Printf("[membership] %s is no longer a raft voter — removed from the partition map", id)
		}
	}
	if changed {
		_ = mm.pm.Rebalance(mm.pm.PartitionCount())
	}
}

func raftSelfStatus(n *consensus.RaftNode, cfg consensus.Configuration, id string, joining bool) string {
	switch {
	case cfg.IsVoter(id):
		return "voter"
	case cfg.IsLearner(id):
		return "learner"
	case joining && len(cfg.Servers) == 0:
		return "joining"
	default:
		return "removed"
	}
}

func (mm *membershipManager) raftView() membersView {
	n := mm.coord.raft
	cfg, idx, committed := n.Configuration()
	v := membersView{
		Mode: "raft", NodeID: mm.localID, Version: idx, Committed: committed,
		Leader:     n.GetLeaderID(),
		SelfStatus: raftSelfStatus(n, cfg, mm.localID, len(mm.joinSeeds) > 0),
	}
	addrs := mm.memberAddrsRaft(cfg)
	for _, id := range cfg.Servers {
		v.Members = append(v.Members, memberInfo{ID: id, Addr: addrs[id], Role: "voter"})
	}
	for _, id := range cfg.Learners {
		v.Members = append(v.Members, memberInfo{ID: id, Addr: addrs[id], Role: "learner"})
	}
	sort.SliceStable(v.Members, func(i, j int) bool {
		if v.Members[i].Role != v.Members[j].Role {
			return v.Members[i].Role == "voter"
		}
		return v.Members[i].ID < v.Members[j].ID
	})
	return v
}

// knownAddrs: addresses for members a bootstrap configuration has no
// Addrs for (clusters started by an older build).
func (mm *membershipManager) knownAddrs() map[string]string {
	out := mm.coord.peerAddrs()
	for _, p := range mm.staticPeers {
		if out[p.id] == "" {
			out[p.id] = p.clientAddr
		}
	}
	if out[mm.localID] == "" && mm.selfAddr != "" {
		out[mm.localID] = mm.selfAddr
	}
	return out
}

func waitFor(ch memberChange, def time.Duration) time.Duration {
	if ch.WaitMs <= 0 {
		return def
	}
	d := time.Duration(ch.WaitMs) * time.Millisecond
	if d > maxAddWait {
		d = maxAddWait
	}
	return d
}

// raftChange runs a change on the leader (or forwards it there).
func (mm *membershipManager) raftChange(ch memberChange) httpAnswer {
	n := mm.coord.raft
	if !n.IsLeader() {
		return mm.forward(ch, n.GetLeaderID(), "the raft leader")
	}
	switch ch.Op {
	case "add", "join":
		cfg, _, _ := n.Configuration()
		if ch.Op == "join" && cfg.IsMember(ch.ID) && (cfg.Addrs[ch.ID] == "" || cfg.Addrs[ch.ID] == ch.Addr) {
			role := "learner"
			if cfg.IsVoter(ch.ID) {
				role = "voter"
			}
			return answer(http.StatusOK, changeResult{Status: role, Detail: "already a member"})
		}
		def := defaultAddWait
		if ch.Op == "join" {
			def = 2 * time.Second // a joiner polls; do not hold its request
		}
		err := n.AddMember(ch.ID, ch.Addr, mm.knownAddrs(), waitFor(ch, def))
		switch {
		case err == nil:
			log.Printf("[membership] raft: %s (%s) added and promoted to voter", ch.ID, ch.Addr)
			return answer(http.StatusOK, changeResult{Status: "voter", Members: mm.raftView().Members})
		case errors.Is(err, consensus.ErrPromotionPending):
			return answer(http.StatusAccepted, changeResult{Status: "learner",
				Detail:  "added as a learner; it is promoted to voter once caught up (GET /admin/cluster/members)",
				Members: mm.raftView().Members})
		}
		return mapRaftErr(mm, ch, err)
	case "remove":
		if err := n.RemoveMember(ch.ID); err != nil {
			return mapRaftErr(mm, ch, err)
		}
		log.Printf("[membership] raft: %s removed", ch.ID)
		return answer(http.StatusOK, changeResult{Status: "removed", Members: mm.raftView().Members})
	}
	return answerErr(http.StatusBadRequest, "unknown op %q", ch.Op)
}

func mapRaftErr(mm *membershipManager, ch memberChange, err error) httpAnswer {
	switch {
	case errors.Is(err, consensus.ErrNotLeader):
		return mm.forward(ch, mm.coord.raft.GetLeaderID(), "the raft leader")
	case errors.Is(err, consensus.ErrMemberExists):
		return answerErr(http.StatusConflict, "%v", err)
	case errors.Is(err, consensus.ErrConfigChangeInProgress):
		return answerErr(http.StatusConflict, "%v — retry once it commits", err)
	case errors.Is(err, consensus.ErrMemberNotFound):
		return answerErr(http.StatusNotFound, "%v", err)
	case errors.Is(err, consensus.ErrLastVoter), errors.Is(err, consensus.ErrConfigTooLarge):
		return answerErr(http.StatusBadRequest, "%v", err)
	case errors.Is(err, consensus.ErrNoQuorum):
		return answerErr(http.StatusServiceUnavailable, "%v", err)
	}
	return answerErr(http.StatusServiceUnavailable, "configuration change not committed: %v", err)
}

// forward sends ch to the node that must run it (raft leader / replicated
// coordinator) over the transfer listener, once.
func (mm *membershipManager) forward(ch memberChange, target, what string) httpAnswer {
	if ch.Hops > 0 {
		return answerErr(http.StatusMisdirectedRequest, "this node is not %s (it is %q) — retry", what, target)
	}
	if target == "" {
		return answerErr(http.StatusServiceUnavailable, "no %s known right now — retry", what)
	}
	addr := mm.coord.peerAddr(target)
	if !mm.authOK {
		return answerErr(http.StatusServiceUnavailable,
			"this node is not %s (%s at %s): send the request to that node's admin endpoint, or configure --cluster-secret-file (or --cluster-mtls) so nodes can forward membership changes",
			what, target, addr)
	}
	taddr := mm.transferAddrOf(target, addr)
	if taddr == "" {
		return answerErr(http.StatusServiceUnavailable, "no address for %s %s", what, target)
	}
	ch.Hops++
	var res changeResult
	ctx, cancel := context.WithTimeout(context.Background(), waitFor(ch, defaultAddWait)+30*time.Second)
	defer cancel()
	err := mm.client.PostJSON(ctx, taddr, "/cluster/members", ch, &res)
	if err == nil {
		return answer(http.StatusOK, res)
	}
	var he *cluster.HTTPError
	if errors.As(err, &he) {
		return httpAnswer{status: he.Status, raw: he.Body}
	}
	return answerErr(http.StatusServiceUnavailable, "forward to %s %s (%s): %v", what, target, taddr, err)
}

// ── shared entry points ─────────────────────────────────────────────────────

func (mm *membershipManager) change(ch memberChange) httpAnswer {
	if err := validMemberID(ch.ID); err != nil {
		return answerErr(http.StatusBadRequest, "%v", err)
	}
	if ch.Op == "add" || ch.Op == "join" {
		if err := validMemberAddr(ch.Addr); err != nil {
			return answerErr(http.StatusBadRequest, "%v", err)
		}
	}
	switch mm.mode {
	case modeRaft:
		return mm.raftChange(ch)
	case modeReplicated:
		return mm.replicatedChange(ch)
	}
	return answerErr(http.StatusBadRequest, "membership changes need --mode=raft or --mode=replicated")
}

func (mm *membershipManager) view() membersView {
	if mm.mode == modeRaft {
		return mm.raftView()
	}
	return mm.replicatedView()
}

// isMember reports whether this node is in its own membership.
func (mm *membershipManager) isMember() bool {
	if mm.mode == modeRaft {
		return !mm.coord.raft.IsRemoved()
	}
	return mm.replMember()
}

// joinLoop asks the --join seeds to add this node until it is a member.
func (mm *membershipManager) joinLoop() {
	defer mm.wg.Done()
	<-mm.attached
	if !mm.authOK {
		log.Printf("[membership] --join: the transfer listener is unauthenticated (no --cluster-secret-file, no --cluster-mtls) — " +
			"nodes refuse membership traffic without it; add this node with the admin API instead (it waits to be added)")
	}
	ch := memberChange{Op: "join", ID: mm.localID, Addr: mm.selfAddr}
	logged := time.Time{}
	for i := 0; ; i++ {
		if mm.isMember() {
			log.Printf("[membership] %s is a cluster member", mm.localID)
			return
		}
		if mm.authOK {
			for _, seed := range mm.joinSeeds {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				var res changeResult
				err := mm.client.PostJSON(ctx, deriveAddr(seed, offTransfer), "/cluster/join", ch, &res)
				cancel()
				var he *cluster.HTTPError
				if err == nil || (errors.As(err, &he) && he.Status == http.StatusAccepted) {
					break // added (or catching up as a learner): wait for the configuration to arrive
				}
				if time.Since(logged) > 30*time.Second {
					log.Printf("[membership] join via %s: %v (retrying)", seed, err)
					logged = time.Now()
				}
			}
		}
		select {
		case <-mm.stopCh:
			return
		case <-time.After(joinRetryInterval):
		}
	}
}

// ── HTTP ────────────────────────────────────────────────────────────────────

// registerMembershipAdmin mounts the admin endpoints (the caller wraps the
// mux in the same adminapi.Guard as every other /admin/* endpoint).
func registerMembershipAdmin(mux *http.ServeMux, prefix string, mm *membershipManager) {
	prefix = strings.TrimRight(prefix, "/")
	base := prefix + "/cluster/members"
	mux.HandleFunc(base, func(w http.ResponseWriter, r *http.Request) {
		if mm == nil {
			answerErr(http.StatusBadRequest, "membership changes need --mode=raft or --mode=replicated").write(w)
			return
		}
		switch r.Method {
		case http.MethodGet:
			answer(http.StatusOK, mm.view()).write(w)
		case http.MethodPost:
			var ch memberChange
			body := http.MaxBytesReader(w, r.Body, 1<<16)
			if err := json.NewDecoder(body).Decode(&ch); err != nil {
				answerErr(http.StatusBadRequest, "bad body (want {\"id\":\"n4\",\"addr\":\"host:port\"}): %v", err).write(w)
				return
			}
			ch.Op, ch.Hops = "add", 0
			mm.change(ch).write(w)
		default:
			answerErr(http.StatusMethodNotAllowed, "GET or POST").write(w)
		}
	})
	mux.HandleFunc(base+"/", func(w http.ResponseWriter, r *http.Request) {
		if mm == nil {
			answerErr(http.StatusBadRequest, "membership changes need --mode=raft or --mode=replicated").write(w)
			return
		}
		if r.Method != http.MethodDelete {
			answerErr(http.StatusMethodNotAllowed, "DELETE "+base+"/{id}").write(w)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, base+"/")
		ch := memberChange{Op: "remove", ID: id}
		ch.Force, _ = strconv.ParseBool(r.URL.Query().Get("force"))
		ch.WaitMs, _ = strconv.Atoi(r.URL.Query().Get("wait_ms"))
		mm.change(ch).write(w)
	})
}

// registerMembershipTransfer mounts the node-to-node endpoints on the
// transfer listener (TransferAgent.authMiddleware signs/verifies them).
func (mm *membershipManager) registerMembershipTransfer(ta *cluster.TransferAgent) {
	guard := func(h func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !mm.authOK {
				answerErr(http.StatusForbidden, "membership traffic needs an authenticated transfer listener (--cluster-secret-file or --cluster-mtls)").write(w)
				return
			}
			if r.Method != http.MethodPost {
				answerErr(http.StatusMethodNotAllowed, "POST").write(w)
				return
			}
			select {
			case <-mm.attached:
			default:
				answerErr(http.StatusServiceUnavailable, "starting").write(w)
				return
			}
			h(w, r)
		}
	}
	decode := func(w http.ResponseWriter, r *http.Request, v interface{}) bool {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(v); err != nil {
			answerErr(http.StatusBadRequest, "bad body: %v", err).write(w)
			return false
		}
		return true
	}
	ta.Handle("/cluster/members", guard(func(w http.ResponseWriter, r *http.Request) {
		var ch memberChange
		if decode(w, r, &ch) {
			mm.change(ch).write(w)
		}
	}))
	ta.Handle("/cluster/join", guard(func(w http.ResponseWriter, r *http.Request) {
		var ch memberChange
		if decode(w, r, &ch) {
			ch.Op = "join"
			mm.change(ch).write(w)
		}
	}))
	if mm.mode != modeReplicated {
		return
	}
	ta.Handle("/cluster/membership", guard(func(w http.ResponseWriter, r *http.Request) {
		answer(http.StatusOK, mm.replState()).write(w)
	}))
	ta.Handle("/cluster/membership/apply", guard(func(w http.ResponseWriter, r *http.Request) {
		var st replState
		if !decode(w, r, &st) {
			return
		}
		applied, err := mm.applyReplState(st)
		if err != nil {
			answerErr(http.StatusInternalServerError, "%v", err).write(w)
			return
		}
		cur := mm.replState()
		answer(http.StatusOK, map[string]interface{}{"applied": applied, "version": cur.Version}).write(w)
	}))
	ta.Handle("/cluster/drain", guard(func(w http.ResponseWriter, r *http.Request) {
		var req drainRequest
		if !decode(w, r, &req) {
			return
		}
		if req.Cancel {
			mm.drainUntil.Store(0)
			log.Printf("[membership] drain cancelled")
			answer(http.StatusOK, map[string]string{"status": "serving"}).write(w)
			return
		}
		if err := mm.drainSelf(time.Duration(req.WaitMs) * time.Millisecond); err != nil {
			answerErr(http.StatusConflict, "%v", err).write(w)
			return
		}
		answer(http.StatusOK, map[string]string{"status": "drained"}).write(w)
	}))
}
