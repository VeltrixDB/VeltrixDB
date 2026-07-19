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
//	veltrix top               Live dashboard — refreshes every 2 s (or -w N)
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
// Global flags:
//
//	--addr    Admin/metrics address  (default 127.0.0.1:2112)
//	--tcp     TCP data address       (default 127.0.0.1:9000)
//	--watch N Repeat command every N seconds (0 = run once)
//	--json    Print raw JSON instead of formatted tables
//	--no-color  Disable ANSI colours (auto-disabled when not a TTY)

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

func adminGet(adminAddr, path string) ([]byte, error) {
	url := "http://" + adminAddr + path
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, url, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func adminPost(adminAddr, path, contentType, body string) ([]byte, error) {
	url := "http://" + adminAddr + path
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	resp, err := httpClient.Post(url, contentType, rb)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(out)))
	}
	return out, nil
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

// cmdStatus prints a full health + ops summary.
func cmdStatus(adminAddr, tcpAddr string, rawJSON bool) error {
	// Health checks (parallel).
	type hcheck struct{ name, url string }
	checks := []hcheck{{"healthz", "/healthz"}, {"readyz", "/readyz"}}
	health := make(map[string]bool, 2)
	for _, c := range checks {
		data, err := adminGet(adminAddr, c.url)
		health[c.name] = err == nil && strings.Contains(string(data), "ok")
	}

	statsData, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Println(string(statsData))
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
	if !health["healthz"] {
		hStatus = cross() + " UNHEALTHY"
	}
	rStatus := tick() + " READY"
	if !health["readyz"] {
		rStatus = warn() + " NOT READY"
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
		used := jUint(cache, "size_bytes")
		cap_ := jUint(cache, "max_bytes")
		hits := jUint(cache, "hits")
		total := hits + jUint(cache, "misses")
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
		sb.WriteString(fmt.Sprintf("  %-20s %s\n", "Evictions:", fmtInt(jUint(cache, "evictions"))))
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
		for i, v := range vlogs {
			vm, _ := v.(map[string]any)
			if vm == nil {
				continue
			}
			ratio := jFloat(vm, "gc_ratio")
			gcState := green("normal")
			if ratio >= 0.65 {
				gcState = red("EMERGENCY")
			} else if ratio >= 0.50 {
				gcState = yellow("critical")
			}
			tbl.add(
				fmt.Sprintf("%d", i),
				fmtBytes(jUint(vm, "size_bytes")),
				gcRatioColor(ratio),
				fmtBytes(jUint(vm, "live_bytes")),
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
			sb.WriteString(fmt.Sprintf("  %s\n", dim("(read EWMA > 4ms triggered write throttling)")))
		}
	}

	fmt.Print(sb.String())
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
		fmt.Println(string(statsData))
		return nil
	}

	fmt.Print(header("VLog Compaction (GC) Status"))

	vlogs := jSlice(stats, "vlogs")
	if len(vlogs) == 0 {
		fmt.Println(dim("  No VLog data available."))
		return nil
	}

	tbl := newTable("DISK", "SIZE", "LIVE", "DEAD", "GC RATIO", "STATUS")
	for i, v := range vlogs {
		vm, _ := v.(map[string]any)
		if vm == nil {
			continue
		}
		total := jUint(vm, "size_bytes")
		live := jUint(vm, "live_bytes")
		var dead uint64
		if total >= live {
			dead = total - live
		}
		ratio := jFloat(vm, "gc_ratio")
		gcStatus := green("normal")
		if ratio >= 0.65 {
			gcStatus = red("EMERGENCY — GC uncapped, bypass pause")
		} else if ratio >= 0.50 {
			gcStatus = yellow("CRITICAL — BW raised to 200 MB/s, interval halved")
		} else if ratio >= 0.30 {
			gcStatus = yellow("active GC")
		}
		tbl.add(
			fmt.Sprintf("%d", i),
			fmtBytes(total),
			fmtBytes(live),
			fmtBytes(dead),
			gcRatioColor(ratio),
			gcStatus,
		)
	}
	fmt.Print(tbl.render())

	if metrics != nil {
		fmt.Print(sectionLine())
		fmt.Println(bold("GC Run Counters"))
		gcRuns := uint64(metricVal(metrics, "vlog_gc_runs_total"))
		emergency := uint64(metricVal(metrics, "vlog_gc_emergency_runs_total"))
		skippedPaused := uint64(metricVal(metrics, "vlog_gc_skipped_paused_total"))
		skippedRatio := uint64(metricVal(metrics, "vlog_gc_skipped_ratio_total"))
		readErrs := uint64(metricVal(metrics, "vlog_gc_read_errors_total"))
		casFails := uint64(metricVal(metrics, "vlog_gc_cas_fails_total"))
		throttles := uint64(metricVal(metrics, "storage_write_admission_throttles_total"))

		fmt.Printf("  %-36s %s\n", "GC runs (total):", fmtInt(gcRuns))

		emStr := fmtInt(emergency)
		if emergency > 0 {
			emStr = red(emStr + " — sustained writes exceed GC throughput!")
		} else {
			emStr = green(emStr)
		}
		fmt.Printf("  %-36s %s\n", "Emergency runs:", emStr)
		fmt.Printf("  %-36s %s\n", "Skipped (ratio below threshold):", fmtInt(skippedRatio))

		pausedStr := fmtInt(skippedPaused)
		if skippedPaused > 0 {
			pausedStr = yellow(pausedStr + " (read EWMA > 4ms, GC paused)")
		}
		fmt.Printf("  %-36s %s\n", "Skipped (admission pause):", pausedStr)

		if readErrs > 0 {
			fmt.Printf("  %-36s %s\n", "Read errors (VLog corruption?):", red(fmtInt(readErrs)))
		}
		if casFails > 0 {
			fmt.Printf("  %-36s %s\n", "CAS failures (concurrent writes):", yellow(fmtInt(casFails)))
		}
		if throttles > 0 {
			fmt.Printf("  %-36s %s\n", "Write admission throttles:", yellow(fmtInt(throttles)))
		}

		// Admission control state
		fmt.Print(sectionLine())
		fmt.Println(bold("Admission Control"))
		gcPaused := skippedPaused > 0 && gcRuns == 0
		if gcPaused {
			fmt.Printf("  GC state: %s\n", red("PAUSED — read EWMA above 4ms threshold"))
		} else {
			fmt.Printf("  GC state: %s\n", green("running"))
		}
		fmt.Printf("  %s\n", dim("GC latency threshold: 3ms EWMA → throttle to 60 MB/s"))
		fmt.Printf("  %s\n", dim("Admission threshold:  4ms EWMA → pause GC + throttle writes 2ms"))
		fmt.Printf("  %s\n", dim("Emergency:           ≥65%% garbage → bypass pause, uncap BW"))
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
		fmt.Println(string(data))
		return nil
	}
	stats, _ := parseJSON(data)
	cache := jMap(stats, "cache")

	fmt.Print(header("Cache (LIRS)"))
	if cache == nil {
		fmt.Println(dim("  No cache stats available."))
		return nil
	}

	used := jUint(cache, "size_bytes")
	cap_ := jUint(cache, "max_bytes")
	hits := jUint(cache, "hits")
	misses := jUint(cache, "misses")
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

	fmt.Printf("  %-20s %s / %s  (%.1f%%)\n", "Size:", fmtBytes(used), fmtBytes(cap_), fillPct)
	fmt.Printf("  %-20s [%s]\n", "Fill:", bar)
	fmt.Printf("  %-20s %s  (%s hits / %s misses)\n",
		"Hit rate:", hitColor(fmtPct(hitRate)), fmtInt(hits), fmtInt(misses))
	fmt.Printf("  %-20s %s\n", "Evictions:", fmtInt(jUint(cache, "evictions")))

	if hotKeys := jSlice(cache, "hot_keys"); len(hotKeys) > 0 {
		fmt.Print(sectionLine())
		fmt.Println(bold("Hot Keys (most frequent)"))
		for i, k := range hotKeys {
			if i >= 10 {
				break
			}
			fmt.Printf("  %2d. %s\n", i+1, k)
		}
	}

	return nil
}

// cmdWAL shows WAL stats.
func cmdWAL(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Println(string(data))
		return nil
	}
	stats, _ := parseJSON(data)
	metrics, _ := fetchMetrics(adminAddr)

	fmt.Print(header("Write-Ahead Log (WAL)"))
	fmt.Printf("  %-24s %s\n", "Bytes written:", fmtBytes(jUint(stats, "wal_bytes")))
	fmt.Printf("  %-24s %s\n", "Entries written:", fmtInt(jUint(stats, "wal_entries")))

	if metrics != nil {
		flushes := uint64(metricVal(metrics, "storage_wal_flushes_total"))
		batchSize := float64(0)
		if flushes > 0 {
			batchSize = float64(jUint(stats, "wal_entries")) / float64(flushes)
		}
		fmt.Printf("  %-24s %s\n", "Total flushes:", fmtInt(flushes))
		fmt.Printf("  %-24s %.1f entries/flush\n", "Avg batch size:", batchSize)
	}

	fmt.Print(sectionLine())
	fmt.Printf("  %s\n", dim("Flush window: 10 ms (group-commit). P99 ≈ window + fdatasync."))
	fmt.Printf("  %s\n", dim("On Linux NVMe: fdatasync ~0.2–0.5ms → P99 ~10.2ms."))
	fmt.Printf("  %s\n", dim("On macOS (F_FULLFSYNC ~8ms) → P99 ~18ms."))

	return nil
}

// cmdReplication shows replication state.
func cmdReplication(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/stats")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Println(string(data))
		return nil
	}
	stats, _ := parseJSON(data)

	fmt.Print(header("Replication"))

	replicas := jSlice(stats, "replicas")
	if len(replicas) == 0 {
		fmt.Println(dim("  Single-node mode — no replicas configured."))
		return nil
	}

	tbl := newTable("REPLICA", "STATE", "CONSISTENCY", "LAG", "ACK WATERMARK")
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
		state := jStr(rm, "state")
		stateStr := state
		switch state {
		case "Sync":
			stateStr = green(state)
		case "Lag":
			stateStr = yellow(state)
		case "Failed":
			stateStr = red(state)
		}
		tbl.add(
			jStr(rm, "id"),
			stateStr,
			jStr(rm, "consistency"),
			lagStr,
			fmtInt(jUint(rm, "ack_watermark")),
		)
	}
	fmt.Print(tbl.render())

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
		fmt.Println(string(data))
		return nil
	}
	topo, _ := parseJSON(data)

	fmt.Print(header("Cluster Nodes"))

	mode := jStr(topo, "mode")
	raft, _ := topo["raft"].(map[string]any)
	leaderID := ""
	term := 0.0
	if raft != nil {
		leaderID = jStr(raft, "leader_id")
		term = jFloat(raft, "term")
	}
	fmt.Printf("  mode=%s  epoch=%g", mode, jFloat(topo, "epoch"))
	if c := jStr(topo, "consistency"); c != "" {
		fmt.Printf("  consistency=%s", c)
	}
	fmt.Println()

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
		fmt.Println(dim("  No nodes reported."))
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
	fmt.Print(tbl.render())
	return nil
}

// cmdQuotas shows per-namespace quota usage.
func cmdQuotas(adminAddr string, rawJSON bool) error {
	data, err := adminGet(adminAddr, "/admin/quotas")
	if err != nil {
		return err
	}
	if rawJSON {
		fmt.Println(string(data))
		return nil
	}

	var quotas []map[string]any
	if err := json.Unmarshal(data, &quotas); err != nil {
		// Try as the stats format.
		fmt.Println(string(data))
		return nil
	}

	fmt.Print(header("Per-Namespace Quotas"))
	if len(quotas) == 0 {
		fmt.Println(dim("  No quotas configured."))
		return nil
	}

	tbl := newTable("NAMESPACE", "WRITES/S LIMIT", "BURST", "MAX KEYS", "CURRENT KEYS", "RATE STATUS")
	for _, q := range quotas {
		ns := jStr(q, "namespace")
		limit := jFloat(q, "writes_per_sec")
		burst := jFloat(q, "burst_writes")
		maxKeys := jFloat(q, "max_keys")
		curKeys := jFloat(q, "current_keys")

		limitStr := fmt.Sprintf("%g", limit)
		if limit == 0 {
			limitStr = dim("unlimited")
		}
		maxStr := fmt.Sprintf("%s", fmtInt(uint64(maxKeys)))
		if maxKeys == 0 {
			maxStr = dim("unlimited")
		}
		curStr := fmtInt(uint64(curKeys))
		if maxKeys > 0 && curKeys/maxKeys > 0.9 {
			curStr = yellow(curStr + " (90%+)")
		}

		rateStatus := jStr(q, "rate_status")
		if rateStatus == "" {
			rateStatus = green("ok")
		} else if rateStatus == "throttled" {
			rateStatus = red(rateStatus)
		}

		tbl.add(ns, limitStr, fmt.Sprintf("%g", burst), maxStr, curStr, rateStatus)
	}
	fmt.Print(tbl.render())
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
		fmt.Println(string(data))
		return nil
	}

	fmt.Print(header("CDC (Change Data Capture)"))
	total := jUint(stats, "cdc_broadcast_total")
	dropped := jUint(stats, "cdc_dropped_total")
	subs := int(jFloat(stats, "cdc_subscribers"))

	dropStr := fmtInt(dropped)
	if dropped > 0 {
		dropStr = red(dropStr + " (slow consumer — events dropped)")
	} else {
		dropStr = green(dropStr)
	}

	fmt.Printf("  %-24s %s\n", "Events broadcast:", fmtInt(total))
	fmt.Printf("  %-24s %s\n", "Events dropped:", dropStr)
	fmt.Printf("  %-24s %d\n", "Active subscribers:", subs)
	fmt.Print(sectionLine())
	fmt.Printf("  %s\n", dim("Slow consumers are auto-evicted after 3 consecutive drops."))
	fmt.Printf("  %s\n", dim("For cross-node CDC: use repl-ship (long-polls /admin/cdc)."))
	fmt.Printf("  %s\n", dim("Stream live: veltrix cdc-tail [--prefix=<key-prefix>]"))

	return nil
}

// cmdCDCTail streams CDC events to stdout.
func cmdCDCTail(adminAddr, prefix string) error {
	url := "http://" + adminAddr + "/admin/cdc"
	if prefix != "" {
		url += "?prefix=" + prefix
	}
	fmt.Printf("%s Streaming CDC events from %s (Ctrl+C to stop)\n\n",
		dim("[cdc]"), bold(adminAddr))
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		// Pretty-print the JSON line.
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil {
			op := jStr(m, "op")
			opStr := op
			switch strings.ToUpper(op) {
			case "PUT", "SET":
				opStr = green(op)
			case "DEL", "DELETE":
				opStr = red(op)
			}
			ts := int64(jFloat(m, "timestamp_us"))
			t := time.UnixMicro(ts).Format("15:04:05.000")
			fmt.Printf("%s  %s  %s  %s\n",
				dim(t),
				padRight(opStr, 8),
				bold(jStr(m, "key")),
				dim(fmt.Sprintf("ver=%s", jStr(m, "version"))))
		} else {
			fmt.Println(line)
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
		fmt.Println(string(data))
		return nil
	}
	filter = strings.ToLower(filter)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(strings.ToLower(line), filter) {
			fmt.Println(line)
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
		fmt.Println(string(data))
		return nil
	}

	fmt.Print(header("Recent OTel Traces (slow + error spans)"))
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

			ts := int64(jFloat(m, "start_us"))
			t := time.UnixMicro(ts).Format("15:04:05.000")
			fmt.Printf("  %s %s  %-30s  %s\n",
				marker,
				dim(t),
				bold(name),
				durColor(fmtDur(durNs)))
			if errStr != "" && errStr != "—" && errStr != "false" {
				fmt.Printf("       %s\n", red(errStr))
			}
			count++
		} else {
			fmt.Println(line)
			count++
		}
	}
	if count == 0 {
		fmt.Println(dim("  No traces in ring buffer. Traces appear for ops ≥50ms or with errors."))
	}
	return scanner.Err()
}

// cmdTop runs a live dashboard that refreshes every interval seconds.
func cmdTop(adminAddr, tcpAddr string, interval int) error {
	clearScreen := func() {
		fmt.Print("\033[H\033[2J") // move cursor home + clear
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
			fmt.Println(red("Connection error: ") + err.Error())
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
			cur.cacheHits = jUint(cache, "hits")
			cur.cacheMisses = jUint(cache, "misses")
		}
		vlogs := jSlice(stats, "vlogs")
		if len(vlogs) > 0 {
			for _, v := range vlogs {
				vm, _ := v.(map[string]any)
				if vm == nil {
					continue
				}
				r := jFloat(vm, "gc_ratio")
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
		fmt.Printf("%s  VeltrixDB Live — %s     %s\n",
			bold("⚡"),
			bold(tcpAddr),
			dim("refresh: "+strconv.Itoa(interval)+"s   Ctrl+C to exit   "+now))
		fmt.Println(dim(strings.Repeat("─", 70)))

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

			fmt.Printf("\n  %s  %-14s  %s  %-14s  %s  %-14s\n",
				bold("Writes/s:"), cyan(fmt.Sprintf("%.0f", writePS)),
				bold("Reads/s:"), cyan(fmt.Sprintf("%.0f", readPS)),
				bold("Deletes/s:"), cyan(fmt.Sprintf("%.0f", delPS)))
			fmt.Printf("  %s  %-14s  %s  %-14s\n",
				bold("Cache hit:"), hitColor(fmtPct(hitRate)),
				bold("GC ratio:"), gcRatioColor(cur.gcRatio))
		} else {
			fmt.Printf("\n  %s\n", dim("Collecting baseline... (next refresh in "+strconv.Itoa(interval)+"s)"))
		}

		fmt.Printf("\n  %s  %s\n", bold("Live keys:"), cyan(fmtInt(cur.keys)))

		if cur.gcEmergency > 0 {
			fmt.Printf("\n  %s %s\n", red("⚠  EMERGENCY GC:"),
				red("garbage ratio ≥65%%. Write rate exceeds GC throughput."))
		}

		throttles := uint64(0)
		if metrics != nil {
			throttles = uint64(metricVal(metrics, "storage_write_admission_throttles_total"))
		}
		if throttles > 0 {
			fmt.Printf("  %s %s throttle events\n", warn(), yellow(fmtInt(throttles)+" write admission"))
		}

		// Per-disk GC bar.
		if len(vlogs) > 0 {
			fmt.Println()
			fmt.Println(dim(strings.Repeat("─", 70)))
			fmt.Printf("  %-6s %-12s %s\n", bold("DISK"), bold("GC RATIO"), bold("GARBAGE BAR"))
			barW := 40
			for i, v := range vlogs {
				vm, _ := v.(map[string]any)
				if vm == nil {
					continue
				}
				ratio := jFloat(vm, "gc_ratio")
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
				fmt.Printf("  %-6d %-12s [%s]\n", i, gcRatioColor(ratio), bar)
			}
		}

		prev = cur
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

// cmdPing checks connectivity and reports round-trip latency.
func cmdPing(tcpAddr, adminAddr string) error {
	fmt.Printf("Pinging VeltrixDB at %s ...\n\n", bold(tcpAddr))

	// TCP ping.
	const n = 5
	var totalTCP time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		resp, err := tcpCmd(tcpAddr, "PING")
		rtt := time.Since(start)
		if err != nil {
			fmt.Printf("  #%d  TCP  %s\n", i+1, red(err.Error()))
		} else if resp == "PONG" {
			fmt.Printf("  #%d  TCP  %s  rtt=%s\n", i+1, green("PONG"), cyan(rtt.Round(time.Microsecond).String()))
			totalTCP += rtt
		} else {
			fmt.Printf("  #%d  TCP  unexpected response: %q\n", i+1, resp)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// HTTP health ping.
	fmt.Println()
	start := time.Now()
	data, err := adminGet(adminAddr, "/healthz")
	httpRTT := time.Since(start)
	if err != nil {
		fmt.Printf("  HTTP /healthz  %s\n", red(err.Error()))
	} else {
		status := strings.TrimSpace(string(data))
		fmt.Printf("  HTTP /healthz  %s  %s  rtt=%s\n",
			green(status), dim("(admin port)"), cyan(httpRTT.Round(time.Microsecond).String()))
	}

	fmt.Printf("\n  Avg TCP RTT: %s\n", cyan((totalTCP / n).Round(time.Microsecond).String()))
	return nil
}

// cmdPut/Get/Del — direct key ops via text protocol.
func cmdPut(tcpAddr, key, value string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("PUT %s %s", key, value))
	if err != nil {
		return err
	}
	if resp == "OK" {
		fmt.Println(green("OK"))
	} else {
		fmt.Println(red(resp))
	}
	return nil
}

func cmdGet(tcpAddr, key string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("GET %s", key))
	if err != nil {
		return err
	}
	if resp == "ERR" || resp == "" {
		fmt.Println(red("(not found)"))
	} else {
		fmt.Println(resp)
	}
	return nil
}

func cmdDel(tcpAddr, key string) error {
	resp, err := tcpCmd(tcpAddr, fmt.Sprintf("DEL %s", key))
	if err != nil {
		return err
	}
	if resp == "OK" {
		fmt.Println(green("OK"))
	} else {
		fmt.Println(red(resp))
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
	fmt.Printf("%s Checkpoint complete in %s ms\n",
		green(tick()), bold(jStr(m, "duration_ms")))
	return nil
}

// cmdBackup triggers a full backup.
func cmdBackup(adminAddr, destDir string) error {
	body := fmt.Sprintf(`{"type":"full","dest_dir":%q}`, destDir)
	fmt.Printf("Triggering full backup → %s ...\n", bold(destDir))
	data, err := adminPost(adminAddr, "/admin/backup", "application/json", body)
	if err != nil {
		return err
	}
	m, _ := parseJSON(data)
	fmt.Printf("%s Backup complete\n", green(tick()))
	fmt.Printf("  ID:       %s\n", bold(jStr(m, "backup_id")))
	fmt.Printf("  Duration: %s ms\n", jStr(m, "duration_ms"))
	fmt.Printf("  Disks:    %s\n", jStr(m, "num_disks"))
	return nil
}

// cmdVersion prints engine + schema version.
func cmdVersion(adminAddr string) error {
	data, err := adminGet(adminAddr, "/admin/version")
	if err != nil {
		return err
	}
	m, _ := parseJSON(data)
	fmt.Printf("Schema version: %s\n", bold(jStr(m, "current_schema_version")))
	fmt.Printf("Encryption:     %s\n", jStr(m, "encryption_enabled"))
	return nil
}

// cmdScrubber shows data-integrity scrubber status.
func cmdScrubber(adminAddr string) error {
	metrics, err := fetchMetrics(adminAddr)
	if err != nil {
		return err
	}

	fmt.Print(header("Data Integrity Scrubber"))
	records := uint64(metricVal(metrics, "scrub_records_total"))
	corruption := uint64(metricVal(metrics, "scrub_corruption_total"))

	fmt.Printf("  %-24s %s\n", "Records scanned:", fmtInt(records))
	corrStr := fmtInt(corruption)
	if corruption > 0 {
		corrStr = red(corrStr + " ⚠  CRC32C mismatches detected!")
	} else {
		corrStr = green(corrStr + " (clean)")
	}
	fmt.Printf("  %-24s %s\n", "Corruptions found:", corrStr)
	fmt.Print(sectionLine())
	fmt.Printf("  %s\n", dim("Scrubber walks VLog records at 50 MB/s (configurable)."))
	fmt.Printf("  %s\n", dim("Corruption increments veltrixdb_scrub_corruption_total and logs disk+offset."))
	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

const usageText = `veltrix — VeltrixDB operator CLI

Usage:
  veltrix [flags] COMMAND [args]

Commands:
  status              Full node health + ops summary
  nodes               Cluster node topology (role, term, health)
  compaction          VLog GC per disk — ratio, runs, emergency state
  replication         Replication lag + vector clock status
  cache               LIRS cache hit rate, size, evictions
  wal                 WAL bytes, entries, flush stats
  quotas              Per-namespace quota usage
  cdc                 CDC broker stats
  cdc-tail            Stream live CDC events (Ctrl+C to stop)
  scrubber            Data integrity scrubber status
  metrics [filter]    Raw Prometheus metrics (optional grep filter)
  traces              Recent OTel spans (slow + error ops)
  top                 Live dashboard (refreshes every --watch seconds)
  ping                Round-trip latency check (TCP + HTTP)
  put KEY VALUE       Write a key
  get KEY             Read a key
  del KEY             Delete a key
  checkpoint          Force WAL checkpoint on all disks
  backup DEST_DIR     Trigger full backup to DEST_DIR
  version             Engine + schema version

Global flags:
  --addr   Admin/metrics HTTP address  (default 127.0.0.1:2112)
  --tcp    TCP data address            (default 127.0.0.1:9000)
  --watch  Refresh interval (seconds) for top/status/compaction (0 = once)
  --json   Print raw JSON instead of formatted tables
  --no-color  Disable ANSI colors

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
  veltrix --addr 10.0.0.5:2112 status
`

func main() {
	var (
		adminAddr = flag.String("addr", "127.0.0.1:2112", "Admin/metrics HTTP address")
		tcpAddr   = flag.String("tcp", "127.0.0.1:9000", "TCP data address")
		watchSec  = flag.Int("watch", 0, "Refresh interval in seconds (0 = run once)")
		rawJSON   = flag.Bool("json", false, "Print raw JSON")
		noColor   = flag.Bool("no-color", false, "Disable ANSI colours")
		prefix    = flag.String("prefix", "", "Key prefix for cdc-tail")
	)
	flag.Usage = func() { fmt.Fprint(os.Stderr, usageText) }
	flag.Parse()

	if *noColor {
		useColor = false
	}

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	cmd := args[0]
	rest := args[1:]

	// Commands that support --watch run in a loop.
	runOnce := func(fn func() error) {
		if *watchSec > 0 {
			for {
				if err := fn(); err != nil {
					fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
				}
				time.Sleep(time.Duration(*watchSec) * time.Second)
				fmt.Print("\033[H\033[2J") // clear
			}
		} else {
			if err := fn(); err != nil {
				fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
				os.Exit(1)
			}
		}
	}

	switch cmd {
	case "status":
		runOnce(func() error { return cmdStatus(*adminAddr, *tcpAddr, *rawJSON) })

	case "nodes":
		runOnce(func() error { return cmdNodes(*adminAddr, *rawJSON) })

	case "compaction", "gc":
		runOnce(func() error { return cmdCompaction(*adminAddr, *rawJSON) })

	case "replication", "repl":
		runOnce(func() error { return cmdReplication(*adminAddr, *rawJSON) })

	case "cache":
		runOnce(func() error { return cmdCache(*adminAddr, *rawJSON) })

	case "wal":
		runOnce(func() error { return cmdWAL(*adminAddr, *rawJSON) })

	case "quotas", "quota":
		runOnce(func() error { return cmdQuotas(*adminAddr, *rawJSON) })

	case "cdc":
		runOnce(func() error { return cmdCDC(*adminAddr, *rawJSON) })

	case "cdc-tail":
		if err := cmdCDCTail(*adminAddr, *prefix); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "scrubber":
		runOnce(func() error { return cmdScrubber(*adminAddr) })

	case "metrics":
		filter := ""
		if len(rest) > 0 {
			filter = rest[0]
		}
		runOnce(func() error { return cmdMetrics(*adminAddr, filter, *rawJSON) })

	case "traces":
		runOnce(func() error { return cmdTraces(*adminAddr, *rawJSON) })

	case "top":
		interval := *watchSec
		if interval == 0 {
			interval = 2
		}
		cmdTop(*adminAddr, *tcpAddr, interval) // runs until Ctrl+C

	case "ping":
		if err := cmdPing(*tcpAddr, *adminAddr); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "put":
		if len(rest) < 2 {
			fmt.Fprintln(os.Stderr, "usage: veltrix put KEY VALUE")
			os.Exit(2)
		}
		if err := cmdPut(*tcpAddr, rest[0], strings.Join(rest[1:], " ")); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "get":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "usage: veltrix get KEY")
			os.Exit(2)
		}
		if err := cmdGet(*tcpAddr, rest[0]); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "del":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "usage: veltrix del KEY")
			os.Exit(2)
		}
		if err := cmdDel(*tcpAddr, rest[0]); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "checkpoint":
		if err := cmdCheckpoint(*adminAddr); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "backup":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "usage: veltrix backup DEST_DIR")
			os.Exit(2)
		}
		if err := cmdBackup(*adminAddr, rest[0]); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "version":
		if err := cmdVersion(*adminAddr); err != nil {
			fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
			os.Exit(1)
		}

	case "help", "--help", "-h":
		fmt.Fprint(os.Stdout, usageText)

	default:
		// Try to be helpful: if they typed a Prometheus metric name directly,
		// show matching metrics.
		if strings.Contains(cmd, "_") {
			if err := cmdMetrics(*adminAddr, cmd, *rawJSON); err != nil {
				fmt.Fprintln(os.Stderr, red("error: ")+err.Error())
				os.Exit(1)
			}
			return
		}
		fmt.Fprintf(os.Stderr, "unknown command %q — run 'veltrix help' for usage\n", cmd)
		os.Exit(2)
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
