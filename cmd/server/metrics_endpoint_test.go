package main

// metrics_endpoint_test.go — starts the real server binary twice: single-node
// raft (so the raft collector is registered; one vector, text and hybrid
// search is run) and single-node replicated (so the replication metrics are
// served), and scrapes /metrics: the standard go_* / process_* series, the
// search, raft and replication metrics, and every metric named in
// monitoring/prometheus-rules.yaml must be served by one of the two.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
)

// rulesMetricAllowlist: names the rules read from other exporters, plus
// recording rules (none today).
var rulesMetricAllowlist = map[string]bool{
	"node_filesystem_avail_bytes": true, // node-exporter
	"node_filesystem_size_bytes":  true, // node-exporter
	"up":                          true, // Prometheus itself
}

func TestMetricsEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts the server binary")
	}
	bin := buildServerBinary(t)
	served, scrapeTime := scrapeServer(t, bin, "raft")
	replServed, _ := scrapeServer(t, bin, "replicated")

	t.Run("Served", func(t *testing.T) {
		want := []string{
			// standard collectors
			"go_goroutines", "go_memstats_heap_inuse_bytes", "go_gc_duration_seconds",
			"process_start_time_seconds",
			// hardware collector (dashboards)
			"veltrixdb_runtime_goroutines",
			// search
			"veltrixdb_search_requests_total", "veltrixdb_search_errors_total",
			"veltrixdb_search_latency_seconds_bucket", "veltrixdb_search_partial_total",
			"veltrixdb_search_ready", "veltrixdb_search_rebuild_loaded_keys",
			"veltrixdb_search_rebuild_total_keys", "veltrixdb_search_vectors",
			"veltrixdb_search_namespace_vectors", "veltrixdb_search_vector_namespaces",
			"veltrixdb_search_text_documents",
			// raft
			"veltrixdb_raft_term", "veltrixdb_raft_is_leader", "veltrixdb_raft_commit_index",
			"veltrixdb_raft_applied_index", "veltrixdb_raft_snapshot_index",
			"veltrixdb_raft_log_retained_entries", "veltrixdb_raft_snapshot_threshold_entries",
			"veltrixdb_raft_snapshot_captures_total", "veltrixdb_raft_snapshot_installs_total",
			"veltrixdb_raft_snapshot_chunks_sent_total", "veltrixdb_raft_snapshot_chunks_received_total",
			"veltrixdb_raft_snapshot_legacy_sent_total",
			"veltrixdb_raft_snapshot_capture_failures_total", "veltrixdb_raft_snapshot_sent_total",
			"veltrixdb_raft_snapshot_transfer_failures_total", "veltrixdb_raft_snapshot_install_failures_total",
			"veltrixdb_raft_snapshot_restore_failures_total",
			// the original collector is still there
			"veltrixdb_storage_writes_total",
		}
		for _, n := range want {
			if _, ok := served[n]; !ok {
				t.Errorf("/metrics does not serve %s", n)
			}
		}
		for _, kind := range []string{"vector", "text", "hybrid"} {
			if !servedWith(served["veltrixdb_search_requests_total"], `kind="`+kind+`"`, "1") {
				t.Errorf("veltrixdb_search_requests_total{kind=%q} != 1: %v", kind, served["veltrixdb_search_requests_total"])
			}
		}
		if !servedWith(served["veltrixdb_raft_is_leader"], "", "1") {
			t.Errorf("single-node raft server is not leader: %v", served["veltrixdb_raft_is_leader"])
		}
		t.Logf("/metrics: %d metric names, scrape took %s", len(served), scrapeTime)
		if scrapeTime > 2*time.Second {
			t.Errorf("/metrics scrape took %s", scrapeTime)
		}
	})

	t.Run("ServedReplicated", func(t *testing.T) {
		for _, n := range []string{
			"veltrixdb_replication_lag_nanoseconds", "veltrixdb_replication_replicas_failed",
			"veltrixdb_replication_catchup_gaps_total", "veltrixdb_replication_full_resyncs_total",
			"veltrixdb_replication_state_persists_total", "veltrixdb_replication_state_persist_errors_total",
			"veltrixdb_replication_replicas_resync_pending", "veltrixdb_replication_replicas_delete_gap",
		} {
			if _, ok := replServed[n]; !ok {
				t.Errorf("/metrics (--mode=replicated) does not serve %s", n)
			}
		}
		if _, ok := replServed["veltrixdb_raft_term"]; ok {
			t.Errorf("/metrics (--mode=replicated) serves raft metrics")
		}
	})

	t.Run("RulesReferenceServedMetrics", func(t *testing.T) {
		names := rulesMetricNames(t, filepath.Join("..", "..", "monitoring", "prometheus-rules.yaml"))
		if len(names) < 10 {
			t.Fatalf("parsed only %d metric names from the rules: %v", len(names), names)
		}
		t.Logf("rules reference %d metrics: %s", len(names), strings.Join(names, " "))
		for _, n := range names {
			if rulesMetricAllowlist[n] {
				continue
			}
			_, inRaft := served[n]
			_, inRepl := replServed[n]
			if !inRaft && !inRepl {
				t.Errorf("monitoring/prometheus-rules.yaml uses %s, which /metrics does not serve (raft or replicated mode)", n)
			}
		}
	})
}

// servedWith reports whether one of lines (a metric's sample lines) contains
// labelSubstr and has value want.
func servedWith(lines []string, labelSubstr, want string) bool {
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) >= 2 && strings.Contains(l, labelSubstr) && f[len(f)-1] == want {
			return true
		}
	}
	return false
}

// buildServerBinary builds this package's server binary into a temp dir.
func buildServerBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "veltrixdb-metrics-test")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build: %v", err)
	}
	return bin
}

// scrapeServer starts a single-node server of mode ("raft" or
// "replicated"); in raft mode it runs one search of each kind.  It returns
// /metrics as name → sample lines.
func scrapeServer(t *testing.T, bin, mode string) (map[string][]string, time.Duration) {
	t.Helper()

	// client port + derived raft/repl/gossip/transfer ports, then the
	// metrics port: one range, so an ephemeral port picked for /metrics
	// (sequential on macOS) cannot land on a derived port.
	base := freePortRange(t, 7)
	addr := fmt.Sprintf("127.0.0.1:%d", base)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", base+6)

	logf, err := os.Create(filepath.Join(t.TempDir(), "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin,
		"-mode", mode, "-node-id", "n1", "-peers", "n1@"+addr,
		"-addr", addr, "-metrics-addr", metricsAddr, "-data", t.TempDir(),
		"-wal-flush-window-ms", "1", "-vlog-flush-window-ms", "1")
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		logf.Close()
		if t.Failed() {
			if b, err := os.ReadFile(logf.Name()); err == nil {
				t.Logf("server log:\n%s", b)
			}
		}
	})

	var tc *client.TCPConn
	deadline := time.Now().Add(20 * time.Second)
	for {
		if tc, err = client.DialTCP(addr, time.Second); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not accept connections: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer tc.Close()

	if mode == "raft" {
		runOneSearchOfEachKind(t, tc)
	}
	t0 := time.Now()
	resp, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	took := time.Since(t0)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, %v", resp.StatusCode, err)
	}
	served := map[string][]string{}
	sc := bufio.NewScanner(strings.NewReader(string(body)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		served[name] = append(served[name], line)
	}
	return served, took
}

// runOneSearchOfEachKind runs one vector, text and hybrid search, retrying
// until the node has elected itself and the search rebuild is done.
func runOneSearchOfEachKind(t *testing.T, tc *client.TCPConn) {
	t.Helper()
	retry := func(what string, fn func() error) {
		t.Helper()
		var err error
		for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
			if err = fn(); err == nil {
				return
			}
		}
		t.Fatalf("%s: %v", what, err)
	}
	vec := []float32{1, 0, 0}
	retry("VCREATE", func() error { return tc.VCreate("docs", 3, "") })
	retry("VSET", func() error { return tc.VSetNS("docs", "a", vec) })
	retry("TSET", func() error { return tc.TSet("docs", "a", "hello metrics world") })
	retry("VSEARCH", func() error {
		_, err := tc.VSearchWithOptions(1, vec, client.VectorSearchOptions{NS: "docs"})
		return err
	})
	if _, err := tc.TSearch(1, "metrics", client.TextSearchOptions{NS: "docs"}); err != nil {
		t.Fatalf("TSEARCH: %v", err)
	}
	if _, err := tc.HSearch(1, vec, "metrics", client.HybridSearchOptions{NS: "docs"}); err != nil {
		t.Fatalf("HSEARCH: %v", err)
	}
}

// freePortRange returns a port p such that p..p+n-1 are all free.
func freePortRange(t *testing.T, n int) int {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base := l.Addr().(*net.TCPAddr).Port
		l.Close()
		if base+n > 65535 {
			continue
		}
		ok := true
		for i := 0; i < n && ok; i++ {
			c, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			c.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("no free port range")
	return 0
}

var (
	promBraces    = regexp.MustCompile(`\{[^}]*\}`)
	promRange     = regexp.MustCompile(`\[[^\]]*\]`)
	promStrings   = regexp.MustCompile(`"[^"]*"`)
	promGrouping  = regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)`)
	promIdent     = regexp.MustCompile(`[A-Za-z_:][A-Za-z0-9_:]*`)
	promoKeywords = map[string]bool{
		"and": true, "or": true, "unless": true, "bool": true, "offset": true,
		"by": true, "without": true, "on": true, "ignoring": true,
		"group_left": true, "group_right": true, "inf": true, "nan": true,
	}
)

// rulesMetricNames extracts the metric names from every `expr: |` block of a
// PrometheusRule file: labels matchers, ranges, strings and grouping clauses
// are dropped, then every identifier not followed by "(" (a function) and
// not a keyword is a metric name.
func rulesMetricNames(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var exprs []string
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ {
		trim := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trim, "expr:") {
			continue
		}
		if rest := strings.TrimSpace(strings.TrimPrefix(trim, "expr:")); rest != "|" && rest != ">" && rest != "" {
			exprs = append(exprs, rest)
			continue
		}
		indent := len(lines[i]) - len(strings.TrimLeft(lines[i], " "))
		var sb strings.Builder
		for i+1 < len(lines) {
			next := lines[i+1]
			if strings.TrimSpace(next) != "" && len(next)-len(strings.TrimLeft(next, " ")) <= indent {
				break
			}
			sb.WriteString(next)
			sb.WriteByte('\n')
			i++
		}
		exprs = append(exprs, sb.String())
	}
	seen := map[string]bool{}
	for _, e := range exprs {
		e = promStrings.ReplaceAllString(e, "")
		e = promBraces.ReplaceAllString(e, "")
		e = promRange.ReplaceAllString(e, "")
		e = promGrouping.ReplaceAllString(e, "")
		for _, loc := range promIdent.FindAllStringIndex(e, -1) {
			id := e[loc[0]:loc[1]]
			if promoKeywords[strings.ToLower(id)] {
				continue
			}
			if rest := strings.TrimLeft(e[loc[1]:], " \t\n"); strings.HasPrefix(rest, "(") {
				continue // function
			}
			if loc[0] > 0 && (e[loc[0]-1] >= '0' && e[loc[0]-1] <= '9' || e[loc[0]-1] == '.') {
				continue // exponent / number suffix such as 100e6
			}
			seen[id] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
