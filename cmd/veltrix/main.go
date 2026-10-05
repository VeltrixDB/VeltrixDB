// veltrix — primary operator CLI for VeltrixDB.
//
// Think of it like redis-cli meets kubectl top: every subsystem has its own
// subcommand, output is richly formatted with colours and tables, and --watch
// turns any command into a live dashboard that refreshes on an interval.
//
// Usage:
//
//	veltrix status            Full node health + ops summary
//	veltrix nodes             Cluster node list (role, term, lag)
//	veltrix compaction        VLog GC per disk — ratio, runs, emergency state
//	veltrix replication       Replication lag per replica, consistency level
//	veltrix cache             LIRS cache hit rate, size, evictions
//	veltrix wal               WAL bytes, entries, flush rate
//	veltrix quotas            Per-namespace quota usage
//	veltrix cdc               CDC broker stats
//	veltrix top               Live dashboard — refreshes every 2 s (or --watch N)
//	veltrix metrics [filter]  Raw Prometheus metrics, optional grep filter
//	veltrix traces            Recent OTel spans from the in-process ring buffer
//	veltrix ping              Round-trip latency check
//	veltrix put KEY VALUE     Write a key (text protocol)
//	veltrix get KEY           Read a key (text protocol)
//	veltrix del KEY           Delete a key (text protocol)
//	veltrix checkpoint        Force WAL checkpoint on all disks
//	veltrix backup DEST_DIR   Trigger a full backup to DEST_DIR
//	veltrix version           Engine version + schema version
//
// Flags (accepted before or after the command; see parseArgs):
//
//	--addr         Admin/metrics address  (default 127.0.0.1:2112)
//	--tcp          TCP data address       (default 127.0.0.1:9000)
//	--admin-token  /admin/* token, sent as Authorization: Bearer (env VELTRIX_ADMIN_TOKEN)
//	--watch N      Repeat command every N seconds (0 = run once)
//	--json         Print raw JSON instead of formatted tables
//	--prefix P     cdc-tail key-prefix filter
//	--duration N   cdc-tail: stop after N seconds
//	--no-color     Disable ANSI colours (auto-disabled when not a TTY)

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ── ANSI helpers ──────────────────────────────────────────────────────────────

var useColor = true

func init() {
	// Disable colour when stdout is not a TTY.
	if fi, err := os.Stdout.Stat(); err == nil {
		if fi.Mode()&os.ModeCharDevice == 0 {
			useColor = false
		}
	}
}

const (
	cReset  = "\033[0m"
	cBold   = "\033[1m"
	cDim    = "\033[2m"
	cGreen  = "\033[32m"
	cYellow = "\033[33m"
	cRed    = "\033[31m"
	cCyan   = "\033[36m"
	cBlue   = "\033[34m"
	cWhite  = "\033[97m"
)

func col(code, s string) string {
	if !useColor {
		return s
	}
	return code + s + cReset
}

func bold(s string) string   { return col(cBold, s) }
func green(s string) string  { return col(cGreen, s) }
func yellow(s string) string { return col(cYellow, s) }
func red(s string) string    { return col(cRed, s) }
func cyan(s string) string   { return col(cCyan, s) }
func dim(s string) string    { return col(cDim, s) }

func tick() string  { return green("✓") }
func cross() string { return red("✗") }
func warn() string  { return yellow("⚠") }

// ── Box-drawing helpers ───────────────────────────────────────────────────────

func header(title string) string {
	line := strings.Repeat("━", 52)
	return fmt.Sprintf("\n%s\n%s\n", bold(title), dim(line))
}

func sectionLine() string {
	return dim(strings.Repeat("─", 52)) + "\n"
}

// ── Table renderer ────────────────────────────────────────────────────────────

type table struct {
	headers []string
	rows    [][]string
}

func newTable(headers ...string) *table { return &table{headers: headers} }

func (t *table) add(cells ...string) { t.rows = append(t.rows, cells) }

func (t *table) render() string {
	colW := make([]int, len(t.headers))
	for i, h := range t.headers {
		colW[i] = len(h)
	}
	for _, row := range t.rows {
		for i, cell := range row {
			if i < len(colW) && len(stripANSI(cell)) > colW[i] {
				colW[i] = len(stripANSI(cell))
			}
		}
	}

	var sb strings.Builder
	// header row
	for i, h := range t.headers {
		sb.WriteString(bold(padRight(h, colW[i])))
		if i < len(t.headers)-1 {
			sb.WriteString("  ")
		}
	}
	sb.WriteByte('\n')
	for _, w := range colW {
		sb.WriteString(dim(strings.Repeat("─", w)))
		sb.WriteString("  ")
	}
	sb.WriteByte('\n')
	for _, row := range t.rows {
		for i, cell := range row {
			if i < len(colW) {
				pad := colW[i] - len(stripANSI(cell))
				sb.WriteString(cell)
				sb.WriteString(strings.Repeat(" ", pad))
				if i < len(t.headers)-1 {
					sb.WriteString("  ")
				}
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func padRight(s string, w int) string {
	pad := w - len(stripANSI(s))
	if pad < 0 {
		pad = 0
	}
	return s + strings.Repeat(" ", pad)
}

func stripANSI(s string) string {
	out := strings.Builder{}
	inEsc := false
	for _, c := range s {
		if c == '\033' {
			inEsc = true
		} else if inEsc {
			if c == 'm' {
				inEsc = false
			}
		} else {
			out.WriteRune(c)
		}
	}
	return out.String()
}

// ── Number formatting ─────────────────────────────────────────────────────────

func fmtInt(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	out := make([]byte, 0, len(s)+len(s)/3)
	pre := len(s) % 3
	if pre > 0 {
		out = append(out, s[:pre]...)
	}
	for i := pre; i < len(s); i += 3 {
		if len(out) > 0 {
			out = append(out, ',')
		}
		out = append(out, s[i:i+3]...)
	}
	return string(out)
}

func fmtBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtPct(ratio float64) string {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return "—"
	}
	return fmt.Sprintf("%.1f%%", ratio*100)
}

func fmtDur(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.1fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.1fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.1fµs", ns/1e3)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}

func gcRatioColor(ratio float64) string {
	pct := fmtPct(ratio)
	switch {
	case ratio >= 0.65:
		return red("EMERGENCY " + pct)
	case ratio >= 0.50:
		return yellow("CRITICAL " + pct)
	case ratio >= 0.30:
		return yellow(pct)
	default:
		return green(pct)
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 10 * time.Second}

// out is where every command writes its output (tests swap it for a buffer).
var out io.Writer = os.Stdout

// adminToken is the --admin-token / VELTRIX_ADMIN_TOKEN value. When set it is
// sent as "Authorization: Bearer <token>" on every HTTP request — the header
// adminapi.Guard checks for /admin/* (/metrics, /healthz, /readyz ignore it).
var adminToken string

// baseURL turns --addr into a URL prefix; a bare host:port gets http://.
func baseURL(adminAddr string) string {
	if strings.HasPrefix(adminAddr, "http://") || strings.HasPrefix(adminAddr, "https://") {
		return strings.TrimRight(adminAddr, "/")
	}
	return "http://" + adminAddr
}

func newRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	if adminToken != "" {
		req.Header.Set("Authorization", "Bearer "+adminToken)
	}
	return req, nil
}

// httpError formats a non-2xx admin response, adding a hint for the two
// adminapi.Guard rejections (401 bad/missing token, 403 loopback-only).
func httpError(code int, url string, body []byte) error {
	msg := fmt.Sprintf("HTTP %d from %s: %s", code, url, strings.TrimSpace(string(body)))
	switch code {
	case http.StatusUnauthorized:
		if adminToken == "" {
			msg += " (server requires an admin token: pass --admin-token or set VELTRIX_ADMIN_TOKEN)"
		} else {
			msg += " (the --admin-token / VELTRIX_ADMIN_TOKEN value was rejected)"
		}
	case http.StatusForbidden:
		msg += " (server has no --admin-token, so /admin/* is loopback-only: run the CLI on the node, or start the server with --admin-token and pass the same token here)"
	}
	return fmt.Errorf("%s", msg)
}

func adminGet(adminAddr, path string) ([]byte, error) {
	url := baseURL(adminAddr) + path
	req, err := newRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, httpError(resp.StatusCode, url, body)
	}
	return body, nil
}

// probe GETs a health endpoint and returns its status code and trimmed body.
// /healthz answers 200 "ok"; /readyz answers 200 "ready", or 503 with
// "initializing" / "degraded: disks [...] failed" (cmd/server/main.go).
func probe(adminAddr, path string) (int, string, error) {
	url := baseURL(adminAddr) + path
	req, err := newRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body)), nil
}

func adminPost(adminAddr, path, contentType, body string) ([]byte, error) {
	url := baseURL(adminAddr) + path
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	req, err := newRequest(http.MethodPost, url, rb)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, httpError(resp.StatusCode, url, data)
	}
	return data, nil
}

func parseJSON(data []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func jStr(m map[string]any, key string) string {
	v, _ := m[key]
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case nil:
		return "—"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func jFloat(m map[string]any, key string) float64 {
	v, _ := m[key]
	f, _ := v.(float64)
	return f
}

func jUint(m map[string]any, key string) uint64 {
	return uint64(jFloat(m, key))
}

func jMap(m map[string]any, key string) map[string]any {
	v, _ := m[key]
	r, _ := v.(map[string]any)
	return r
}

func jSlice(m map[string]any, key string) []any {
	v, _ := m[key]
	s, _ := v.([]any)
	return s
}

// ── TCP text-protocol helpers ─────────────────────────────────────────────────

func tcpCmd(tcpAddr, cmd string) (string, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr, 5*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "%s\n", cmd)
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", io.EOF
}

// ── Prometheus metrics grep ───────────────────────────────────────────────────

func fetchMetrics(adminAddr string) (map[string]float64, error) {
	data, err := adminGet(adminAddr, "/metrics")
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		// name{labels} value  OR  name value
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		name := parts[0]
		val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
		if err != nil {
			continue
		}
		// Strip labels for simplicity: take the base name before '{'.
		base := strings.SplitN(name, "{", 2)[0]
		out[base] += val
	}
	return out, nil
}

func metricVal(m map[string]float64, name string) float64 {
	return m["veltrixdb_"+name]
}

// ── Commands ──────────────────────────────────────────────────────────────────

// probeDetail renders why a health probe failed, e.g. " (503: initializing)".
func probeDetail(code int, body string, err error) string {
	if err != nil {
		return dim(" (" + err.Error() + ")")
	}
	return dim(fmt.Sprintf(" (%d: %s)", code, body))
}

// cmdStatus prints a full health + ops summary.
func cmdStatus(adminAddr, tcpAddr string, rawJSON bool) error {
	// Health checks. Decided by status code, not body text: /healthz is
	// 200 "ok", /readyz is 200 "ready" or 503 "initializing" / "degraded: …".
	hCode, hBody, hErr := probe(adminAddr, "/healthz")
	rCode, rBody, rErr := probe(adminAddr, "/readyz")

	statsData, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(statsData))
		return nil
	}
	versionData, _ := adminGet(adminAddr, "/admin/version")
	metrics, _ := fetchMetrics(adminAddr)

	stats, err := parseJSON(statsData)
	if err != nil {
		return err
	}

	var sb strings.Builder

	// ── Header ───────────────────────────────────────────────────────────────
	hStatus := tick() + " HEALTHY"
	if hErr != nil || hCode != http.StatusOK {
		hStatus = cross() + " UNHEALTHY" + probeDetail(hCode, hBody, hErr)
	}
	rStatus := tick() + " READY"
	if rErr != nil || rCode != http.StatusOK {
		rStatus = warn() + " NOT READY" + probeDetail(rCode, rBody, rErr)
	}

	sb.WriteString(header(fmt.Sprintf("VeltrixDB Node — %s", bold(tcpAddr))))
	sb.WriteString(fmt.Sprintf("  %-14s %s\n", "Health:", hStatus))
	sb.WriteString(fmt.Sprintf("  %-14s %s\n", "Readiness:", rStatus))
	if versionData != nil {
		ver, _ := parseJSON(versionData)
		sb.WriteString(fmt.Sprintf("  %-14s schema=%-4s encryption=%s\n",
			"Version:",
			jStr(ver, "current_schema_version"),
			jStr(ver, "encryption_enabled")))
	}
	sb.WriteString(fmt.Sprintf("  %-14s %s\n", "Admin addr:", adminAddr))

	// ── Index & Operations ───────────────────────────────────────────────────
	sb.WriteString(sectionLine())
	sb.WriteString(bold("Index & Operations") + "\n")
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Live keys:", fmtInt(jUint(stats, "index_keys"))))
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Writes total:", fmtInt(jUint(stats, "writes_total"))))
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Reads total:", fmtInt(jUint(stats, "reads_total"))))
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Deletes total:", fmtInt(jUint(stats, "deletes_total"))))
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Atomic ops:", fmtInt(jUint(stats, "atomic_ops_total"))))

	// ── Cache ────────────────────────────────────────────────────────────────
	sb.WriteString(sectionLine())
	sb.WriteString(bold("Cache (LIRS)") + "\n")
	cache := jMap(stats, "cache")
	if cache != nil {
		used := jUint(cache, "CurrentSizeBytes")
		cap_ := jUint(cache, "MaxSizeBytes")
		hits := jUint(cache, "Hits")
		total := hits + jUint(cache, "Misses")
		var hitRate float64
		if total > 0 {
			hitRate = float64(hits) / float64(total)
		}
		pct := ""
		if cap_ > 0 {
			pct = fmt.Sprintf(" (%.1f%%)", float64(used)/float64(cap_)*100)
		}
		hitColor := green
		if hitRate < 0.90 {
			hitColor = yellow
		}
		if hitRate < 0.70 {
			hitColor = red
		}
		sb.WriteString(fmt.Sprintf("  %-20s %s / %s%s\n", "Size:", fmtBytes(used), fmtBytes(cap_), dim(pct)))
		sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Hit rate:", hitColor(fmtPct(hitRate))))
		sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Evictions:", fmtInt(jUint(cache, "Evictions"))))
	}

	// ── WAL ──────────────────────────────────────────────────────────────────
	sb.WriteString(sectionLine())
	sb.WriteString(bold("WAL") + "\n")
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Bytes written:", fmtBytes(jUint(stats, "wal_bytes"))))
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Entries:", fmtInt(jUint(stats, "wal_entries"))))
	if metrics != nil {
		sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Flushes:", fmtInt(uint64(metricVal(metrics, "storage_wal_flushes_total")))))
	}

	// ── CDC ──────────────────────────────────────────────────────────────────
	sb.WriteString(sectionLine())
	sb.WriteString(bold("CDC") + "\n")
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Broadcast total:", fmtInt(jUint(stats, "cdc_broadcast_total"))))
	dropped := jUint(stats, "cdc_dropped_total")
	droppedStr := fmtInt(dropped)
	if dropped > 0 {
		droppedStr = yellow(droppedStr + " !")
	} else {
		droppedStr = green(droppedStr)
	}
	sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Dropped:", droppedStr))
	sb.WriteString(fmt.Sprintf("  %-20s %d\n", "Active subscribers:", int(jFloat(stats, "cdc_subscribers"))))

	// ── VLog per-disk summary ─────────────────────────────────────────────────
	vlogs := jSlice(stats, "vlogs")
	if len(vlogs) > 0 {
		sb.WriteString(sectionLine())
		sb.WriteString(bold("VLog (per disk summary)") + "\n")
		tbl := newTable("DISK", "SIZE", "GC RATIO", "LIVE BYTES", "STATUS")
		for _, v := range vlogs {
			vm, _ := v.(map[string]any)
			if vm == nil {
				continue
			}
			ratio := jFloat(vm, "GarbageRatio")
			gcState := green("normal")
			if ratio >= 0.65 {
				gcState = red("EMERGENCY")
			} else if ratio >= 0.50 {
				gcState = yellow("critical")
			}
			tbl.add(
				fmt.Sprintf("%d", int(jFloat(vm, "DiskIdx"))),
				fmtBytes(jUint(vm, "FileBytes")),
				gcRatioColor(ratio),
				fmtBytes(jUint(vm, "LiveBytes")),
				gcState,
			)
		}
		for _, line := range strings.Split(tbl.render(), "\n") {
			if line != "" {
				sb.WriteString("  " + line + "\n")
			}
		}
	}

	// ── Admission control ─────────────────────────────────────────────────────
	if metrics != nil {
		throttles := uint64(metricVal(metrics, "storage_write_admission_throttles_total"))
		if throttles > 0 {
			sb.WriteString(sectionLine())
			sb.WriteString(warn() + " " + bold("Admission control") + "\n")
			sb.WriteString(fmt.Sprintf("  Write throttles: %s\n", yellow(fmtInt(throttles))))
			sb.WriteString(fmt.Sprintf("  %s\n", dim("(read EWMA > 20ms triggered write throttling)")))
		}
	}

	fmt.Fprint(out, sb.String())
	return nil
}

// cmdCompaction shows per-disk VLog GC status.
func cmdCompaction(adminAddr string, rawJSON bool) error {
	statsData, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	metrics, err := fetchMetrics(adminAddr)
	if err != nil {
		metrics = nil
	}

	stats, err := parseJSON(statsData)
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(statsData))
		return nil
	}

	fmt.Fprint(out, header("VLog Compaction (GC) Status"))

	vlogs := jSlice(stats, "vlogs")
	if len(vlogs) == 0 {
		fmt.Fprintln(out, dim("  No VLog data available."))
		return nil
	}

	tbl := newTable("DISK", "SIZE", "LIVE", "DEAD", "GC RATIO", "STATUS")
	for _, v := range vlogs {
		vm, _ := v.(map[string]any)
		if vm == nil {
			continue
		}
		total := jUint(vm, "FileBytes")
		live := jUint(vm, "LiveBytes")
		var dead uint64
		if total >= live {
			dead = total - live
		}
		ratio := jFloat(vm, "GarbageRatio")
		gcStatus := green("normal")
		if ratio >= 0.65 {
			gcStatus = red("EMERGENCY — GC uncapped, bypass pause")
		} else if ratio >= 0.50 {
			gcStatus = yellow("CRITICAL — BW raised to 200 MB/s, interval halved")
		} else if ratio >= 0.30 {
			gcStatus = yellow("active GC")
		}
		tbl.add(
			fmt.Sprintf("%d", int(jFloat(vm, "DiskIdx"))),
			fmtBytes(total),
			fmtBytes(live),
			fmtBytes(dead),
			gcRatioColor(ratio),
			gcStatus,
		)
	}
	fmt.Fprint(out, tbl.render())

	if metrics != nil {
		fmt.Fprint(out, sectionLine())
		fmt.Fprintln(out, bold("GC Run Counters"))
		gcRuns := uint64(metricVal(metrics, "vlog_gc_runs_total"))
		emergency := uint64(metricVal(metrics, "vlog_gc_emergency_runs_total"))
		skippedPaused := uint64(metricVal(metrics, "vlog_gc_skipped_paused_total"))
		skippedRatio := uint64(metricVal(metrics, "vlog_gc_skipped_ratio_total"))
		readErrs := uint64(metricVal(metrics, "vlog_gc_read_errors_total"))
		casFails := uint64(metricVal(metrics, "vlog_gc_cas_fails_total"))
		throttles := uint64(metricVal(metrics, "storage_write_admission_throttles_total"))

		fmt.Fprintf(out, "  %-36s %s\n", "GC runs (total):", fmtInt(gcRuns))

		emStr := fmtInt(emergency)
		if emergency > 0 {
			emStr = red(emStr + " — sustained writes exceed GC throughput!")
		} else {
			emStr = green(emStr)
		}
		fmt.Fprintf(out, "  %-36s %s\n", "Emergency runs:", emStr)
		fmt.Fprintf(out, "  %-36s %s\n", "Skipped (ratio below threshold):", fmtInt(skippedRatio))

		pausedStr := fmtInt(skippedPaused)
		if skippedPaused > 0 {
			pausedStr = yellow(pausedStr + " (read EWMA > 20ms, GC paused)")
		}
		fmt.Fprintf(out, "  %-36s %s\n", "Skipped (admission pause):", pausedStr)

		if readErrs > 0 {
			fmt.Fprintf(out, "  %-36s %s\n", "Read errors (VLog corruption?):", red(fmtInt(readErrs)))
		}
		if casFails > 0 {
			fmt.Fprintf(out, "  %-36s %s\n", "CAS failures (concurrent writes):", yellow(fmtInt(casFails)))
		}
		if throttles > 0 {
			fmt.Fprintf(out, "  %-36s %s\n", "Write admission throttles:", yellow(fmtInt(throttles)))
		}

		// Admission control state
		fmt.Fprint(out, sectionLine())
		fmt.Fprintln(out, bold("Admission Control"))
		gcPaused := skippedPaused > 0 && gcRuns == 0
		if gcPaused {
			fmt.Fprintf(out, "  GC state: %s\n", red("PAUSED — read EWMA above 20ms threshold"))
		} else {
			fmt.Fprintf(out, "  GC state: %s\n", green("running"))
		}
		fmt.Fprintf(out, "  %s\n", dim("GC latency threshold: 15ms EWMA → throttle to 60 MB/s"))
		fmt.Fprintf(out, "  %s\n", dim("Admission threshold:  20ms EWMA → pause GC + throttle writes (resume < 10ms)"))
		fmt.Fprintf(out, "  %s\n", dim("Emergency:           ≥65%% garbage → bypass pause, uncap BW"))
	}

	return nil
}

// cmdCache shows detailed LIRS cache stats.
func cmdCache(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}
	stats, _ := parseJSON(data)
	cache := jMap(stats, "cache")

	fmt.Fprint(out, header("Cache (LIRS)"))
	if cache == nil {
		fmt.Fprintln(out, dim("  No cache stats available."))
		return nil
	}

	used := jUint(cache, "CurrentSizeBytes")
	cap_ := jUint(cache, "MaxSizeBytes")
	hits := jUint(cache, "Hits")
	misses := jUint(cache, "Misses")
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	hitColor := green
	if hitRate < 0.90 {
		hitColor = yellow
	}
	if hitRate < 0.70 {
		hitColor = red
	}

	fillPct := float64(0)
	if cap_ > 0 {
		fillPct = float64(used) / float64(cap_) * 100
	}

	// Inline bar chart (40 chars).
	barW := 40
	filled := int(fillPct / 100 * float64(barW))
	if filled > barW {
		filled = barW
	}
	bar := green(strings.Repeat("█", filled)) + dim(strings.Repeat("░", barW-filled))

	fmt.Fprintf(out, "  %-20s %s / %s  (%.1f%%)\n", "Size:", fmtBytes(used), fmtBytes(cap_), fillPct)
	fmt.Fprintf(out, "  %-20s [%s]\n", "Fill:", bar)
	fmt.Fprintf(out, "  %-20s %s  (%s hits / %s misses)\n",
		"Hit rate:", hitColor(fmtPct(hitRate)), fmtInt(hits), fmtInt(misses))
	fmt.Fprintf(out, "  %-20s %s\n", "Evictions:", fmtInt(jUint(cache, "Evictions")))

	return nil
}

// cmdWAL shows WAL stats.
func cmdWAL(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}
	stats, _ := parseJSON(data)
	metrics, _ := fetchMetrics(adminAddr)

	fmt.Fprint(out, header("Write-Ahead Log (WAL)"))
	fmt.Fprintf(out, "  %-24s %s\n", "Bytes written:", fmtBytes(jUint(stats, "wal_bytes")))
	fmt.Fprintf(out, "  %-24s %s\n", "Entries written:", fmtInt(jUint(stats, "wal_entries")))

	if metrics != nil {
		flushes := uint64(metricVal(metrics, "storage_wal_flushes_total"))
		batchSize := float64(0)
		if flushes > 0 {
			batchSize = float64(jUint(stats, "wal_entries")) / float64(flushes)
		}
		fmt.Fprintf(out, "  %-24s %s\n", "Total flushes:", fmtInt(flushes))
		fmt.Fprintf(out, "  %-24s %.1f entries/flush\n", "Avg batch size:", batchSize)
	}

	fmt.Fprint(out, sectionLine())
	fmt.Fprintf(out, "  %s\n", dim("Flush window: 15 ms default (group-commit). P99 ≈ window + fdatasync."))
	fmt.Fprintf(out, "  %s\n", dim("On Linux NVMe: fdatasync ~0.2–0.5ms → P99 ~15.2ms."))
	fmt.Fprintf(out, "  %s\n", dim("macOS uses plain fsync(2) (~0.02ms, drive cache only) — dev builds"))
	fmt.Fprintf(out, "  %s\n", dim("are not power-loss safe and their write timings do not predict Linux."))

	return nil
}

// cmdReplication shows replication state from GET /admin/cluster
// (cmd/server/admin_cluster.go): top-level "mode"/"consistency" and a
// "replication" list of {node_id, state, last_ack_seq, lag_bytes, lag_ns}.
// /admin/stats carries no replica information.
func cmdReplication(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/cluster")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}
	topo, err := parseJSON(data)
	if err != nil {
		return err
	}

	fmt.Fprint(out, header("Replication"))

	replicas := jSlice(topo, "replication")
	if len(replicas) == 0 {
		fmt.Fprintln(out, dim(fmt.Sprintf("  mode=%s — no replicas reported (replica lag is only reported in --mode=replicated).", jStr(topo, "mode"))))
		return nil
	}
	consistency := jStr(topo, "consistency")

	tbl := newTable("REPLICA", "STATE", "CONSISTENCY", "LAG", "LAG BYTES", "LAST ACK SEQ")
	for _, r := range replicas {
		rm, _ := r.(map[string]any)
		if rm == nil {
			continue
		}
		lag := jFloat(rm, "lag_ns")
		lagStr := fmtDur(lag)
		if lag > 1e9 {
			lagStr = red(lagStr)
		} else if lag > 100e6 {
			lagStr = yellow(lagStr)
		} else {
			lagStr = green(lagStr)
		}
		// replication.ReplicaState.String(): SYNC, SYNC_PENDING, LAG, FAILED.
		state := jStr(rm, "state")
		stateStr := state
		switch strings.ToUpper(state) {
		case "SYNC":
			stateStr = green(state)
		case "LAG", "SYNC_PENDING":
			stateStr = yellow(state)
		case "FAILED":
			stateStr = red(state)
		}
		tbl.add(
			jStr(rm, "node_id"),
			stateStr,
			consistency,
			lagStr,
			fmtBytes(jUint(rm, "lag_bytes")),
			fmtInt(jUint(rm, "last_ack_seq")),
		)
	}
	fmt.Fprint(out, tbl.render())

	return nil
}

// cmdNodes shows cluster node topology.
func cmdNodes(adminAddr string, rawJSON bool) error {
	// Prefer the wired /admin/cluster topology endpoint (role, raft term/leader,
	// peers, epoch, replica lag); fall back to legacy /admin/stats.
	data, err := adminGet(adminAddr, "/admin/cluster")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}
	topo, _ := parseJSON(data)

	fmt.Fprint(out, header("Cluster Nodes"))

	mode := jStr(topo, "mode")
	raft, _ := topo["raft"].(map[string]any)
	leaderID := ""
	term := 0.0
	if raft != nil {
		leaderID = jStr(raft, "leader_id")
		term = jFloat(raft, "term")
	}
	fmt.Fprintf(out, "  mode=%s  epoch=%g", mode, jFloat(topo, "epoch"))
	if c := jStr(topo, "consistency"); c != "" {
		fmt.Fprintf(out, "  consistency=%s", c)
	}
	fmt.Fprintln(out)

	// Index replica lag by node id (replicated mode).
	lagByNode := map[string]float64{}
	for _, r := range jSlice(topo, "replication") {
		rm, _ := r.(map[string]any)
		if rm != nil {
			lagByNode[jStr(rm, "node_id")] = jFloat(rm, "lag_ns")
		}
	}

	nodes := jSlice(topo, "nodes")
	if len(nodes) == 0 {
		fmt.Fprintln(out, dim("  No nodes reported."))
		return nil
	}

	tbl := newTable("NODE ID", "ROLE", "TERM", "HEALTH", "ADDR", "LAG")
	for _, n := range nodes {
		nm, _ := n.(map[string]any)
		if nm == nil {
			continue
		}
		id := jStr(nm, "node_id")
		role := "—"
		if raft != nil {
			if id == leaderID {
				role = bold(green("LEADER"))
			} else {
				role = cyan("FOLLOWER")
			}
		}
		state := strings.ToUpper(jStr(nm, "state"))
		healthStr := green(tick() + " " + state)
		if state != "ACTIVE" && state != "" {
			healthStr = red(cross() + " " + state)
		}
		addr := fmt.Sprintf("%s:%g", jStr(nm, "address"), jFloat(nm, "port"))
		lagStr := "—"
		if lag := lagByNode[id]; lag > 0 {
			lagStr = fmtDur(lag)
		}
		termStr := "—"
		if raft != nil {
			termStr = fmt.Sprintf("%g", term)
		}
		tbl.add(id, role, termStr, healthStr, addr, lagStr)
	}
	fmt.Fprint(out, tbl.render())
	return nil
}

// cmdQuotas shows per-namespace quota usage. GET /admin/quotas returns
// []storage.QuotaSnapshot, which has no json tags, so the keys are the Go
// field names: Namespace, WritesPerSec, BurstWrites, MaxKeys, KeyCount,
// TokensLeft.
func cmdQuotas(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/quotas")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}

	var quotas []map[string]any
	if err := json.Unmarshal(data, &quotas); err != nil {
		return fmt.Errorf("decode /admin/quotas: %w", err)
	}
	sort.Slice(quotas, func(i, j int) bool { return jStr(quotas[i], "Namespace") < jStr(quotas[j], "Namespace") })

	fmt.Fprint(out, header("Per-Namespace Quotas"))
	if len(quotas) == 0 {
		fmt.Fprintln(out, dim("  No quotas configured."))
		return nil
	}

	tbl := newTable("NAMESPACE", "WRITES/S LIMIT", "BURST", "MAX KEYS", "CURRENT KEYS", "TOKENS LEFT", "RATE STATUS")
	for _, q := range quotas {
		ns := jStr(q, "Namespace")
		if ns == "" {
			ns = dim("(default)")
		}
		limit := jFloat(q, "WritesPerSec")
		burst := jFloat(q, "BurstWrites")
		maxKeys := jFloat(q, "MaxKeys")
		curKeys := jFloat(q, "KeyCount")
		tokens := jFloat(q, "TokensLeft")

		limitStr := fmt.Sprintf("%g", limit)
		burstStr := fmt.Sprintf("%g", burst)
		tokStr := fmt.Sprintf("%.0f", tokens)
		if limit == 0 {
			limitStr = dim("unlimited")
			burstStr = dim("—")
			tokStr = dim("—")
		}
		maxStr := fmtInt(uint64(maxKeys))
		if maxKeys == 0 {
			maxStr = dim("unlimited")
		}
		curStr := fmtInt(uint64(curKeys))
		if maxKeys > 0 && curKeys >= maxKeys {
			curStr = red(curStr + " (full)")
		} else if maxKeys > 0 && curKeys/maxKeys > 0.9 {
			curStr = yellow(curStr + " (90%+)")
		}

		// TokensLeft is the bucket level at the last write; < 1 means the
		// next write would be rejected with ErrRateLimited.
		rateStatus := green("ok")
		if limit > 0 && tokens < 1 {
			rateStatus = red("throttled")
		}

		tbl.add(ns, limitStr, burstStr, maxStr, curStr, tokStr, rateStatus)
	}
	fmt.Fprint(out, tbl.render())
	return nil
}

// cmdCDC shows CDC broker status.
func cmdCDC(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	stats, _ := parseJSON(data)

	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}

	fmt.Fprint(out, header("CDC (Change Data Capture)"))
	total := jUint(stats, "cdc_broadcast_total")
	dropped := jUint(stats, "cdc_dropped_total")
	subs := int(jFloat(stats, "cdc_subscribers"))

	dropStr := fmtInt(dropped)
	if dropped > 0 {
		dropStr = red(dropStr + " (slow consumer — events dropped)")
	} else {
		dropStr = green(dropStr)
	}

	fmt.Fprintf(out, "  %-24s %s\n", "Events broadcast:", fmtInt(total))
	fmt.Fprintf(out, "  %-24s %s\n", "Events dropped:", dropStr)
	fmt.Fprintf(out, "  %-24s %d\n", "Active subscribers:", subs)
	fmt.Fprint(out, sectionLine())
	fmt.Fprintf(out, "  %s\n", dim("A subscriber is auto-evicted after 3 consecutive dropped events."))
	fmt.Fprintf(out, "  %s\n", dim("For cross-node CDC: use repl-ship (long-polls /admin/cdc)."))
	fmt.Fprintf(out, "  %s\n", dim("Stream live: veltrix cdc-tail [--prefix=<key-prefix>]"))

	return nil
}

// cdcEvent mirrors storage.CDCEvent as /admin/cdc encodes it: no json tags,
// so the keys are "Op", "Key", "Value" (base64 []byte; null for DEL) and
// "Timestamp" (µs since epoch). encoding/json matches keys case-insensitively,
// so lowercase "op"/"key"/"value"/"timestamp" decode too.
type cdcEvent struct {
	Op        string
	Key       string
	Value     []byte
	Timestamp int64
}

// maxValuePreview caps how many value bytes cdc-tail prints per event.
const maxValuePreview = 64

// formatCDCLine renders one /admin/cdc JSON line for cdc-tail. ok is false
// when the line is not a CDC event (caller prints it verbatim).
func formatCDCLine(line []byte) (string, bool) {
	var ev cdcEvent
	if err := json.Unmarshal(line, &ev); err != nil || ev.Op == "" {
		return "", false
	}
	opStr := padRight(ev.Op, 4)
	switch strings.ToUpper(ev.Op) {
	case "PUT", "SET":
		opStr = green(opStr)
	case "DEL", "DELETE":
		opStr = red(opStr)
	}
	t := time.UnixMicro(ev.Timestamp).Format("15:04:05.000000")
	s := fmt.Sprintf("%s  %s  %s", dim(t), opStr, bold(ev.Key))
	if ev.Value != nil {
		v := ev.Value
		suffix := ""
		if len(v) > maxValuePreview {
			v = v[:maxValuePreview]
			suffix = "…"
		}
		s += "  " + dim(fmt.Sprintf("(%d B)", len(ev.Value))) + " " + strconv.Quote(string(v)) + suffix
	}
	return s, true
}

// cmdCDCTail streams CDC events (GET /admin/cdc, JSON Lines) to out until the
// server closes the stream or durationSec elapses (0 = forever).
func cmdCDCTail(adminAddr, prefix string, durationSec int, rawJSON bool) error {
	q := url.Values{}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if durationSec > 0 {
		q.Set("duration_seconds", strconv.Itoa(durationSec))
	}
	u := baseURL(adminAddr) + "/admin/cdc"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := newRequest(http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	// No client timeout: the stream is long-lived by design.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return httpError(resp.StatusCode, u, body)
	}
	if !rawJSON {
		fmt.Fprintf(errOut, "%s Streaming CDC events from %s (Ctrl+C to stop)\n\n",
			dim("[cdc]"), bold(adminAddr))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 64<<20) // values can be large
	for scanner.Scan() {
		line := scanner.Bytes()
		if rawJSON {
			fmt.Fprintln(out, string(line))
			continue
		}
		if s, ok := formatCDCLine(line); ok {
			fmt.Fprintln(out, s)
		} else {
			fmt.Fprintln(out, string(line))
		}
	}
	return scanner.Err()
}

// cmdMetrics prints raw Prometheus metrics with optional grep.
func cmdMetrics(adminAddr, filter string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/metrics")
	if err != nil {
		return err
	}
	if rawJSON || filter == "" {
		fmt.Fprintln(out, string(data))
		return nil
	}
	filter = strings.ToLower(filter)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(strings.ToLower(line), filter) {
			fmt.Fprintln(out, line)
		}
	}
	return nil
}

// cmdTraces prints recent OTel spans from the in-process ring buffer.
func cmdTraces(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/traces")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Fprintln(out, string(data))
		return nil
	}

	fmt.Fprint(out, header("Recent OTel Traces (slow + error spans)"))
	scanner := bufio.NewScanner(bytes.NewReader(data))
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil {
			durNs := jFloat(m, "duration_ns")
			name := jStr(m, "name")
			errStr := jStr(m, "error")

			durColor := green
			if durNs > 50e6 {
				durColor = red
			} else if durNs > 10e6 {
				durColor = yellow
			}

			marker := " "
			if errStr != "" && errStr != "—" && errStr != "false" && errStr != "<nil>" {
				marker = red("E")
			}

			t := time.Unix(0, int64(jFloat(m, "start_unix_ns"))).Format("15:04:05.000")
			fmt.Fprintf(out, "  %s %s  %-30s  %s\n",
				marker,
				dim(t),
				bold(name),
				durColor(fmtDur(durNs)))
			if errStr != "" && errStr != "—" && errStr != "false" {
				fmt.Fprintf(out, "       %s\n", red(errStr))
			}
			count++
		} else {
			fmt.Fprintln(out, line)
			count++
		}
	}
	if count == 0 {
		fmt.Fprintln(out, dim("  No traces in ring buffer. Traces appear for ops ≥50ms or with errors."))
	}
	return scanner.Err()
}

// cmdTop runs a live dashboard that refreshes every interval seconds.
func cmdTop(adminAddr, tcpAddr string, interval int) error {
	clearScreen := func() {
		fmt.Fprint(out, "\033[H\033[2J") // move cursor home + clear
	}
	type snapshot struct {
		writes, reads, deletes uint64
		cacheHits, cacheMisses uint64
		gcEmergency            uint64
		gcRatio                float64
		keys                   uint64
		ts                     time.Time
	}

	var prev *snapshot

	for {
		statsData, err := adminGet(adminAddr, "/admin/stats")
		if err != nil {
			clearScreen()
			fmt.Fprintln(out, red("Connection error: ")+err.Error())
			time.Sleep(time.Duration(interval) * time.Second)
			continue
		}
		stats, _ := parseJSON(statsData)
		metrics, _ := fetchMetrics(adminAddr)

		cur := &snapshot{
			writes:  jUint(stats, "writes_total"),
			reads:   jUint(stats, "reads_total"),
			deletes: jUint(stats, "deletes_total"),
			keys:    jUint(stats, "index_keys"),
			ts:      time.Now(),
		}
		cache := jMap(stats, "cache")
		if cache != nil {
			cur.cacheHits = jUint(cache, "Hits")
			cur.cacheMisses = jUint(cache, "Misses")
		}
		vlogs := jSlice(stats, "vlogs")
		if len(vlogs) > 0 {
			for _, v := range vlogs {
				vm, _ := v.(map[string]any)
				if vm == nil {
					continue
				}
				r := jFloat(vm, "GarbageRatio")
				if r > cur.gcRatio {
					cur.gcRatio = r
				}
			}
		}
		if metrics != nil {
			cur.gcEmergency = uint64(metricVal(metrics, "vlog_gc_emergency_runs_total"))
		}

		clearScreen()
		now := time.Now().Format("15:04:05")
		fmt.Fprintf(out, "%s  VeltrixDB Live — %s     %s\n",
			bold("⚡"),
			bold(tcpAddr),
			dim("refresh: "+strconv.Itoa(interval)+"s   Ctrl+C to exit   "+now))
		fmt.Fprintln(out, dim(strings.Repeat("─", 70)))

		if prev != nil {
			elapsed := cur.ts.Sub(prev.ts).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			writePS := float64(cur.writes-prev.writes) / elapsed
			readPS := float64(cur.reads-prev.reads) / elapsed
			delPS := float64(cur.deletes-prev.deletes) / elapsed

			hits := cur.cacheHits - prev.cacheHits
			misses := cur.cacheMisses - prev.cacheMisses
			var hitRate float64
			if hits+misses > 0 {
				hitRate = float64(hits) / float64(hits+misses)
			}

			hitColor := green
			if hitRate < 0.90 {
				hitColor = yellow
			}
			if hitRate < 0.70 {
				hitColor = red
			}

			fmt.Fprintf(out, "\n  %s  %-14s  %s  %-14s  %s  %-14s\n",
				bold("Writes/s:"), cyan(fmt.Sprintf("%.0f", writePS)),
				bold("Reads/s:"), cyan(fmt.Sprintf("%.0f", readPS)),
				bold("Deletes/s:"), cyan(fmt.Sprintf("%.0f", delPS)))
			fmt.Fprintf(out, "  %s  %-14s  %s  %-14s\n",
				bold("Cache hit:"), hitColor(fmtPct(hitRate)),
				bold("GC ratio:"), gcRatioColor(cur.gcRatio))
		} else {
			fmt.Fprintf(out, "\n  %s\n", dim("Collecting baseline... (next refresh in "+strconv.Itoa(interval)+"s)"))
		}

		fmt.Fprintf(out, "\n  %s  %s\n", bold("Live keys:"), cyan(fmtInt(cur.keys)))

		if cur.gcEmergency > 0 {
			fmt.Fprintf(out, "\n  %s %s\n", red("⚠  EMERGENCY GC:"),
				red("garbage ratio ≥65%%. Write rate exceeds GC throughput."))
		}

		throttles := uint64(0)
		if metrics != nil {
			throttles = uint64(metricVal(metrics, "storage_write_admission_throttles_total"))
		}
		if throttles > 0 {
			fmt.Fprintf(out, "  %s %s throttle events\n", warn(), yellow(fmtInt(throttles)+" write admission"))
		}

		// Per-disk GC bar.
		if len(vlogs) > 0 {
			fmt.Fprintln(out)
			fmt.Fprintln(out, dim(strings.Repeat("─", 70)))
			fmt.Fprintf(out, "  %-6s %-12s %s\n", bold("DISK"), bold("GC RATIO"), bold("GARBAGE BAR"))
			barW := 40
			for _, v := range vlogs {
				vm, _ := v.(map[string]any)
				if vm == nil {
					continue
				}
				ratio := jFloat(vm, "GarbageRatio")
				filled := int(ratio * float64(barW))
				if filled > barW {
					filled = barW
				}
				barColor := green
				if ratio >= 0.65 {
					barColor = red
				} else if ratio >= 0.50 {
					barColor = yellow
				}
				bar := barColor(strings.Repeat("█", filled)) + dim(strings.Repeat("░", barW-filled))
				fmt.Fprintf(out, "  %-6d %-12s [%s]\n", int(jFloat(vm, "DiskIdx")), gcRatioColor(ratio), bar)
			}
		}

		prev = cur
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

// cmdPing checks connectivity and reports round-trip latency.
func cmdPing(tcpAddr, adminAddr string) error {
	fmt.Fprintf(out, "Pinging VeltrixDB at %s ...\n\n", bold(tcpAddr))

	// TCP ping.
	const n = 5
	var totalTCP time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		resp, err := tcpCmd(tcpAddr, "PING")
		rtt := time.Since(start)
		if err != nil {
			fmt.Fprintf(out, "  #%d  TCP  %s\n", i+1, red(err.Error()))
		} else if resp == "PONG" {
			fmt.Fprintf(out, "  #%d  TCP  %s  rtt=%s\n", i+1, green("PONG"), cyan(rtt.Round(time.Microsecond).String()))
			totalTCP += rtt
		} else {
			fmt.Fprintf(out, "  #%d  TCP  unexpected response: %q\n", i+1, resp)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// HTTP health ping.
	fmt.Fprintln(out)
	start := time.Now()
	data, err := adminGet(adminAddr, "/healthz")
	httpRTT := time.Since(start)
	if err != nil {
		fmt.Fprintf(out, "  HTTP /healthz  %s\n", red(err.Error()))
	} else {
		status := strings.TrimSpace(string(data))
		fmt.Fprintf(out, "  HTTP /healthz  %s  %s  rtt=%s\n",
			green(status), dim("(admin port)"), cyan(httpRTT.Round(time.Microsecond).String()))
	}

	fmt.Fprintf(out, "\n  Avg TCP RTT: %s\n", cyan((totalTCP / n).Round(time.Microsecond).String()))
	return nil
}

// cmdPut/Get/Del — direct key ops via text protocol.
func cmdPut(tcpAddr, key, value string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("PUT %s %s", key, value))
	if err != nil {
		return err
	}
	if resp == "OK" {
		fmt.Fprintln(out, green("OK"))
	} else {
		fmt.Fprintln(out, red(resp))
	}
	return nil
}

func cmdGet(tcpAddr, key string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("GET %s", key))
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERR") {
		// The text protocol answers "ERR <reason>" (e.g. "ERR key not found").
		fmt.Fprintln(out, red(resp))
	} else {
		fmt.Fprintln(out, resp)
	}
	return nil
}

func cmdDel(tcpAddr, key string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("DEL %s", key))
	if err != nil {
		return err
	}
	if resp == "OK" {
		fmt.Fprintln(out, green("OK"))
	} else {
		fmt.Fprintln(out, red(resp))
	}
	return nil
}

// cmdCheckpoint forces a WAL checkpoint.
func cmdCheckpoint(adminAddr string) error {
	data, err := adminPost(adminAddr, "/admin/checkpoint", "application/json", "")
	if err != nil {
		return err
	}
	m, _ := parseJSON(data)
	fmt.Fprintf(out, "%s Checkpoint complete in %s ms\n",
		green(tick()), bold(jStr(m, "duration_ms")))
	return nil
}

// cmdBackup triggers a full backup.
func cmdBackup(adminAddr, destDir string) error {
	body := fmt.Sprintf(`{"type":"full","dest_dir":%q}`, destDir)
	fmt.Fprintf(out, "Triggering full backup → %s ...\n", bold(destDir))
	data, err := adminPost(adminAddr, "/admin/backup", "application/json", body)
	if err != nil {
		return err
	}
	m, _ := parseJSON(data)
	fmt.Fprintf(out, "%s Backup complete\n", green(tick()))
	fmt.Fprintf(out, "  ID:       %s\n", bold(jStr(m, "backup_id")))
	fmt.Fprintf(out, "  Duration: %s ms\n", jStr(m, "duration_ms"))
	fmt.Fprintf(out, "  Disks:    %s\n", jStr(m, "num_disks"))
	return nil
}

// cmdVersion prints engine + schema version.
func cmdVersion(adminAddr string) error {
	data, err := adminGet(adminAddr, "/admin/version")
	if err != nil {
		return err
	}
	m, _ := parseJSON(data)
	fmt.Fprintf(out, "Schema version: %s\n", bold(jStr(m, "current_schema_version")))
	fmt.Fprintf(out, "Encryption:     %s\n", jStr(m, "encryption_enabled"))
	return nil
}

// cmdScrubber shows data-integrity scrubber status.
func cmdScrubber(adminAddr string) error {
	metrics, err := fetchMetrics(adminAddr)
	if err != nil {
		return err
	}

	fmt.Fprint(out, header("Data Integrity Scrubber"))
	records := uint64(metricVal(metrics, "scrub_records_total"))
	corruption := uint64(metricVal(metrics, "scrub_corruption_total"))

	fmt.Fprintf(out, "  %-24s %s\n", "Records scanned:", fmtInt(records))
	corrStr := fmtInt(corruption)
	if corruption > 0 {
		corrStr = red(corrStr + " ⚠  CRC32C mismatches detected!")
	} else {
		corrStr = green(corrStr + " (clean)")
	}
	fmt.Fprintf(out, "  %-24s %s\n", "Corruptions found:", corrStr)
	fmt.Fprint(out, sectionLine())
	fmt.Fprintf(out, "  %s\n", dim("Scrubber walks VLog records at 50 MB/s (configurable)."))
	fmt.Fprintf(out, "  %s\n", dim("Corruption increments veltrixdb_scrub_corruption_total and logs disk+offset."))
	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

const usageText = `veltrix — VeltrixDB operator CLI

Usage:
  veltrix [flags] COMMAND [args] [flags]

Flags may appear before or after the command (veltrix --watch 2 top and
veltrix top --watch 2 are equivalent). Use -- to end flag parsing when an
argument starts with '-' (veltrix put k -- -1).

Commands:
  status              Full node health + ops summary
  nodes               Cluster node topology (role, term, health)
  compaction          VLog GC per disk — ratio, runs, emergency state
  replication         Replication lag per replica (GET /admin/cluster)
  cache               LIRS cache hit rate, size, evictions
  wal                 WAL bytes, entries, flush stats
  quotas              Per-namespace quota usage
  cdc                 CDC broker stats
  cdc-tail            Stream live CDC events (Ctrl+C to stop)
  scrubber            Data integrity scrubber status
  metrics [filter]    Raw Prometheus metrics (optional grep filter)
  traces              Recent OTel spans (slow + error ops)
  top                 Live dashboard (refreshes every --watch seconds, default 2)
  ping                Round-trip latency check (TCP + HTTP)
  put KEY VALUE       Write a key
  get KEY             Read a key
  del KEY             Delete a key
  checkpoint          Force WAL checkpoint on all disks
  backup DEST_DIR     Trigger full backup to DEST_DIR (a path on the server)
  version             Engine + schema version

Flags:
  --addr         Admin/metrics HTTP address  (default 127.0.0.1:2112)
  --tcp          TCP data address            (default 127.0.0.1:9000)
  --admin-token  Token for /admin/* (sent as Authorization: Bearer); required
                 when the server runs with --admin-token (env VELTRIX_ADMIN_TOKEN)
  --watch N      Refresh interval (seconds) for top and read-only commands (0 = once)
  --json         Print raw JSON instead of formatted tables
  --prefix P     Key prefix filter for cdc-tail
  --duration N   cdc-tail: stop after N seconds (0 = until interrupted)
  --no-color     Disable ANSI colors

Examples:
  veltrix status
  veltrix top --watch 2
  veltrix compaction --watch 5
  veltrix cdc-tail --prefix orders/
  veltrix metrics vlog_gc
  veltrix put mykey "hello world"
  veltrix get mykey
  veltrix backup /mnt/backup/2026-05-22
  veltrix checkpoint
  veltrix --addr 10.0.0.5:2112 --admin-token "$TOKEN" status
`

// errOut receives diagnostics (tests swap it for a buffer).
var errOut io.Writer = os.Stderr

// config holds every flag value.
type config struct {
	adminAddr   string
	tcpAddr     string
	token       string
	prefix      string
	watchSec    int
	durationSec int
	rawJSON     bool
	noColor     bool
}

// parseArgs parses flags that may sit before, between or after positional
// arguments. Go's flag package stops at the first non-flag argument, so the
// remaining args are re-parsed after each positional one is peeled off; a
// literal "--" ends flag parsing for everything after it.
func parseArgs(args []string, stderr io.Writer) (config, []string, error) {
	c := config{
		adminAddr: "127.0.0.1:2112",
		tcpAddr:   "127.0.0.1:9000",
		token:     os.Getenv("VELTRIX_ADMIN_TOKEN"),
	}
	fs := flag.NewFlagSet("veltrix", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usageText) }
	fs.StringVar(&c.adminAddr, "addr", c.adminAddr, "Admin/metrics HTTP address")
	fs.StringVar(&c.tcpAddr, "tcp", c.tcpAddr, "TCP data address")
	fs.StringVar(&c.token, "admin-token", c.token, "Admin API token (env VELTRIX_ADMIN_TOKEN)")
	fs.IntVar(&c.watchSec, "watch", 0, "Refresh interval in seconds (0 = run once)")
	fs.IntVar(&c.durationSec, "duration", 0, "cdc-tail: stop after N seconds (0 = until interrupted)")
	fs.BoolVar(&c.rawJSON, "json", false, "Print raw JSON")
	fs.BoolVar(&c.noColor, "no-color", false, "Disable ANSI colours")
	fs.StringVar(&c.prefix, "prefix", "", "Key prefix for cdc-tail")

	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return c, nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			pos = append(pos, rest...)
			break
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
	return c, pos, nil
}

func main() { os.Exit(run(os.Args[1:])) }

// run executes one CLI invocation and returns the process exit code.
func run(argv []string) int {
	c, args, err := parseArgs(argv, errOut)
	if err == flag.ErrHelp {
		return 0
	}
	if err != nil {
		return 2
	}
	if c.noColor {
		useColor = false
	}
	adminToken = c.token

	if len(args) == 0 {
		fmt.Fprint(errOut, usageText)
		return 2
	}

	cmd := args[0]
	rest := args[1:]

	fail := func(err error) int {
		if err != nil {
			fmt.Fprintln(errOut, red("error: ")+err.Error())
			return 1
		}
		return 0
	}
	usage := func(s string) int {
		fmt.Fprintln(errOut, "usage: "+s)
		return 2
	}

	// Read-only commands honour --watch and run in a loop.
	runOnce := func(fn func() error) int {
		if c.watchSec <= 0 {
			return fail(fn())
		}
		for {
			if err := fn(); err != nil {
				fmt.Fprintln(errOut, red("error: ")+err.Error())
			}
			time.Sleep(time.Duration(c.watchSec) * time.Second)
			fmt.Fprint(out, "\033[H\033[2J") // clear
		}
	}

	switch cmd {
	case "status":
		return runOnce(func() error { return cmdStatus(c.adminAddr, c.tcpAddr, c.rawJSON) })
	case "nodes":
		return runOnce(func() error { return cmdNodes(c.adminAddr, c.rawJSON) })
	case "compaction", "gc":
		return runOnce(func() error { return cmdCompaction(c.adminAddr, c.rawJSON) })
	case "replication", "repl":
		return runOnce(func() error { return cmdReplication(c.adminAddr, c.rawJSON) })
	case "cache":
		return runOnce(func() error { return cmdCache(c.adminAddr, c.rawJSON) })
	case "wal":
		return runOnce(func() error { return cmdWAL(c.adminAddr, c.rawJSON) })
	case "quotas", "quota":
		return runOnce(func() error { return cmdQuotas(c.adminAddr, c.rawJSON) })
	case "cdc":
		return runOnce(func() error { return cmdCDC(c.adminAddr, c.rawJSON) })
	case "cdc-tail":
		return fail(cmdCDCTail(c.adminAddr, c.prefix, c.durationSec, c.rawJSON))
	case "scrubber":
		return runOnce(func() error { return cmdScrubber(c.adminAddr) })
	case "metrics":
		filter := ""
		if len(rest) > 0 {
			filter = rest[0]
		}
		return runOnce(func() error { return cmdMetrics(c.adminAddr, filter, c.rawJSON) })
	case "traces":
		return runOnce(func() error { return cmdTraces(c.adminAddr, c.rawJSON) })
	case "top":
		interval := c.watchSec
		if interval <= 0 {
			interval = 2
		}
		return fail(cmdTop(c.adminAddr, c.tcpAddr, interval)) // runs until Ctrl+C
	case "ping":
		return fail(cmdPing(c.tcpAddr, c.adminAddr))
	case "put":
		if len(rest) < 2 {
			return usage("veltrix put KEY VALUE")
		}
		return fail(cmdPut(c.tcpAddr, rest[0], strings.Join(rest[1:], " ")))
	case "get":
		if len(rest) < 1 {
			return usage("veltrix get KEY")
		}
		return fail(cmdGet(c.tcpAddr, rest[0]))
	case "del":
		if len(rest) < 1 {
			return usage("veltrix del KEY")
		}
		return fail(cmdDel(c.tcpAddr, rest[0]))
	case "checkpoint":
		return fail(cmdCheckpoint(c.adminAddr))
	case "backup":
		if len(rest) < 1 {
			return usage("veltrix backup DEST_DIR")
		}
		return fail(cmdBackup(c.adminAddr, rest[0]))
	case "version":
		return fail(cmdVersion(c.adminAddr))
	case "help":
		fmt.Fprint(out, usageText)
		return 0
	default:
		// Try to be helpful: if they typed a Prometheus metric name directly,
		// show matching metrics.
		if strings.Contains(cmd, "_") {
			return fail(cmdMetrics(c.adminAddr, cmd, c.rawJSON))
		}
		fmt.Fprintf(errOut, "unknown command %q — run 'veltrix help' for usage\n", cmd)
		return 2
	}
}

// ── Sorting helper used by list commands ──────────────────────────────────────

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
