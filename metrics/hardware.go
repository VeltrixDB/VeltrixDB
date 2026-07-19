package metrics

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// HardwareCollector exposes Go runtime stats and OS-level hardware metrics
// (process RSS, goroutines, GC, and per-NVMe-disk I/O from /proc/diskstats).
//
// Registration:
//
//	hw := metrics.NewHardwareCollector(nodeID, diskDevices)
//	prometheus.MustRegister(hw)
//
// diskDevices is a slice of Linux block device names to track, e.g.
// []string{"nvme0n1","nvme1n1",...,"nvme7n1"}.  On macOS the /proc reads
// are skipped and only Go runtime stats are collected.
type HardwareCollector struct {
	nodeID      string
	diskDevices []string // Linux block device names (no /dev/ prefix)

	// Go runtime
	goroutines    *prometheus.Desc
	heapInuse     *prometheus.Desc
	heapSys       *prometheus.Desc
	gcPauseTotalS *prometheus.Desc
	gcCycles      *prometheus.Desc
	stackInuse    *prometheus.Desc

	// Process
	processRSSBytes *prometheus.Desc
	processOpenFDs  *prometheus.Desc

	// System memory (/proc/meminfo, Linux only)
	memTotalBytes     *prometheus.Desc
	memAvailableBytes *prometheus.Desc
	memBuffersBytes   *prometheus.Desc
	memCachedBytes    *prometheus.Desc

	// CPU (/proc/stat, Linux only) — iowait fraction per CPU
	cpuIOwaitFraction *prometheus.Desc // gauge per cpu label

	// Per-disk NVMe I/O (/proc/diskstats, Linux only)
	diskReadsCompleted  *prometheus.Desc
	diskWritesCompleted *prometheus.Desc
	diskReadBytes       *prometheus.Desc
	diskWrittenBytes    *prometheus.Desc
	diskReadTimeMs      *prometheus.Desc
	diskWriteTimeMs     *prometheus.Desc
	diskIOQueueDepth    *prometheus.Desc // IOs currently in flight
}

// NewHardwareCollector creates a HardwareCollector for the given node.
// diskDevices should be the bare device names (nvme0n1, not /dev/nvme0n1).
func NewHardwareCollector(nodeID string, diskDevices []string) *HardwareCollector {
	const ns = "veltrixdb"
	label := []string{"node"}

	desc := func(subsystem, name, help string, extraLabels ...string) *prometheus.Desc {
		lbls := append(label, extraLabels...)
		return prometheus.NewDesc(
			prometheus.BuildFQName(ns, subsystem, name),
			help, lbls, nil,
		)
	}

	return &HardwareCollector{
		nodeID:      nodeID,
		diskDevices: diskDevices,

		goroutines:    desc("runtime", "goroutines", "Number of goroutines currently running."),
		heapInuse:     desc("runtime", "heap_inuse_bytes", "Go heap bytes in use by live objects."),
		heapSys:       desc("runtime", "heap_sys_bytes", "Go heap bytes obtained from the OS."),
		gcPauseTotalS: desc("runtime", "gc_pause_total_seconds", "Cumulative GC stop-the-world pause duration in seconds."),
		gcCycles:      desc("runtime", "gc_cycles_total", "Cumulative number of completed GC cycles."),
		stackInuse:    desc("runtime", "stack_inuse_bytes", "Go stack bytes in use by goroutine stacks."),

		processRSSBytes: desc("process", "resident_memory_bytes", "Process resident set size in bytes (VmRSS from /proc/self/status)."),
		processOpenFDs:  desc("process", "open_fds", "Number of open file descriptors (from /proc/self/fd, Linux only)."),

		memTotalBytes:     desc("system", "memory_total_bytes", "Total physical memory installed (/proc/meminfo MemTotal)."),
		memAvailableBytes: desc("system", "memory_available_bytes", "Available memory estimate (/proc/meminfo MemAvailable)."),
		memBuffersBytes:   desc("system", "memory_buffers_bytes", "Memory used by kernel buffers (/proc/meminfo Buffers)."),
		memCachedBytes:    desc("system", "memory_cached_bytes", "Memory used by page cache (/proc/meminfo Cached)."),

		cpuIOwaitFraction: desc("system", "cpu_iowait_fraction", "Fraction of CPU time spent waiting for I/O (from /proc/stat).", "cpu"),

		diskReadsCompleted:  desc("disk", "reads_completed_total", "Total read I/Os completed successfully.", "device"),
		diskWritesCompleted: desc("disk", "writes_completed_total", "Total write I/Os completed successfully.", "device"),
		diskReadBytes:       desc("disk", "read_bytes_total", "Total bytes read from the device (sectors × 512).", "device"),
		diskWrittenBytes:    desc("disk", "written_bytes_total", "Total bytes written to the device (sectors × 512).", "device"),
		diskReadTimeMs:      desc("disk", "read_time_milliseconds_total", "Total wall-clock time spent reading (ms).", "device"),
		diskWriteTimeMs:     desc("disk", "write_time_milliseconds_total", "Total wall-clock time spent writing (ms).", "device"),
		diskIOQueueDepth:    desc("disk", "io_in_flight", "Number of I/Os currently in progress (queue depth).", "device"),
	}
}

func (h *HardwareCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range h.allDescs() {
		ch <- d
	}
}

func (h *HardwareCollector) allDescs() []*prometheus.Desc {
	return []*prometheus.Desc{
		h.goroutines, h.heapInuse, h.heapSys, h.gcPauseTotalS, h.gcCycles, h.stackInuse,
		h.processRSSBytes, h.processOpenFDs,
		h.memTotalBytes, h.memAvailableBytes, h.memBuffersBytes, h.memCachedBytes,
		h.cpuIOwaitFraction,
		h.diskReadsCompleted, h.diskWritesCompleted,
		h.diskReadBytes, h.diskWrittenBytes,
		h.diskReadTimeMs, h.diskWriteTimeMs,
		h.diskIOQueueDepth,
	}
}

func (h *HardwareCollector) Collect(ch chan<- prometheus.Metric) {
	node := h.nodeID

	g := func(desc *prometheus.Desc, v float64, extraLabels ...string) {
		lbls := append([]string{node}, extraLabels...)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, lbls...)
	}
	cnt := func(desc *prometheus.Desc, v float64, extraLabels ...string) {
		lbls := append([]string{node}, extraLabels...)
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, v, lbls...)
	}

	// ── Go runtime ───────────────────────────────────────────────────────────
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	g(h.goroutines, float64(runtime.NumGoroutine()))
	g(h.heapInuse, float64(ms.HeapInuse))
	g(h.heapSys, float64(ms.HeapSys))
	g(h.stackInuse, float64(ms.StackInuse))
	cnt(h.gcPauseTotalS, float64(ms.PauseTotalNs)/1e9)
	cnt(h.gcCycles, float64(ms.NumGC))

	// ── Process RSS ───────────────────────────────────────────────────────────
	if rss, ok := readProcRSS(); ok {
		g(h.processRSSBytes, float64(rss))
	}
	if fds, ok := countOpenFDs(); ok {
		g(h.processOpenFDs, float64(fds))
	}

	// ── System memory (/proc/meminfo) ─────────────────────────────────────────
	if mem, ok := readMemInfo(); ok {
		g(h.memTotalBytes, float64(mem["MemTotal"]*1024))
		g(h.memAvailableBytes, float64(mem["MemAvailable"]*1024))
		g(h.memBuffersBytes, float64(mem["Buffers"]*1024))
		g(h.memCachedBytes, float64(mem["Cached"]*1024))
	}

	// ── CPU iowait (/proc/stat) ───────────────────────────────────────────────
	for cpu, frac := range readCPUIOwait() {
		g(h.cpuIOwaitFraction, frac, cpu)
	}

	// ── Per-disk NVMe stats (/proc/diskstats) ─────────────────────────────────
	diskStats := readDiskStats()
	for _, dev := range h.diskDevices {
		if s, ok := diskStats[dev]; ok {
			cnt(h.diskReadsCompleted, float64(s[0]), dev)
			cnt(h.diskWrittenBytes, float64(s[2]*512), dev) // sectors → bytes (sector=512 B)
			cnt(h.diskReadBytes, float64(s[1]*512), dev)
			cnt(h.diskWritesCompleted, float64(s[3]), dev)
			cnt(h.diskReadTimeMs, float64(s[4]), dev)
			cnt(h.diskWriteTimeMs, float64(s[5]), dev)
			g(h.diskIOQueueDepth, float64(s[6]), dev)
		}
	}
}

// ── /proc helpers ─────────────────────────────────────────────────────────────

func readProcRSS() (int64, bool) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb * 1024, true
				}
			}
		}
	}
	return 0, false
}

func countOpenFDs() (int, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func readMemInfo() (map[string]int64, bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, false
	}
	out := make(map[string]int64)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		val, err := strconv.ParseInt(fields[1], 10, 64)
		if err == nil {
			out[key] = val
		}
	}
	return out, true
}

// readCPUIOwait reads /proc/stat and returns iowait fraction per CPU.
// Returns nil on non-Linux. Key format: "cpu0", "cpu1", ... or "cpu" for aggregate.
func readCPUIOwait() map[string]float64 {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil
	}
	out := make(map[string]float64)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		// fields: cpu name, user, nice, system, idle, iowait, irq, softirq, ...
		var vals [7]int64
		ok := true
		for i := 0; i < 7; i++ {
			v, err := strconv.ParseInt(fields[i+1], 10, 64)
			if err != nil {
				ok = false
				break
			}
			vals[i] = v
		}
		if !ok {
			continue
		}
		total := vals[0] + vals[1] + vals[2] + vals[3] + vals[4] + vals[5] + vals[6]
		iowait := vals[4]
		if total > 0 {
			out[fields[0]] = float64(iowait) / float64(total)
		}
	}
	return out
}

// diskStatsFields holds the subset of /proc/diskstats fields we care about.
// Index: 0=reads_completed, 1=sectors_read, 2=sectors_written, 3=writes_completed,
//
//	4=read_time_ms, 5=write_time_ms, 6=ios_in_progress
func readDiskStats() map[string][7]int64 {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return nil
	}
	out := make(map[string][7]int64)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// /proc/diskstats has ≥14 fields: major minor devname + 11 stat fields
		if len(fields) < 14 {
			continue
		}
		dev := fields[2]
		var s [7]int64
		// fields[3]=reads_completed, [5]=sectors_read, [7]=writes_completed, [9]=sectors_written
		// [6]=read_time_ms, [10]=write_time_ms, [11]=ios_in_progress
		idxMap := [7]int{3, 5, 9, 7, 6, 10, 11}
		ok := true
		for i, idx := range idxMap {
			v, err := strconv.ParseInt(fields[idx], 10, 64)
			if err != nil {
				ok = false
				break
			}
			s[i] = v
		}
		if ok {
			out[dev] = s
		}
	}
	return out
}

// DevicesFromDataDirs infers the bare NVMe device names from the data directory
// paths by resolving them to mount points via /proc/mounts (Linux only).
// Returns nil on macOS or if /proc/mounts is unavailable.
// Falls back to a best-effort list of nvme0n1…nvme7n1 if resolution fails.
func DevicesFromDataDirs(dataDirs []string) []string {
	mounts, err := readMountDevices()
	if err != nil || len(mounts) == 0 {
		// Best-effort fallback: standard GKE NVMe device names
		devs := make([]string, 0, 8)
		for i := 0; i < 8; i++ {
			devs = append(devs, fmt.Sprintf("nvme%dn1", i))
		}
		return devs
	}
	seen := make(map[string]bool)
	var devs []string
	for _, dir := range dataDirs {
		if dev, ok := mounts[dir]; ok && !seen[dev] {
			seen[dev] = true
			devs = append(devs, dev)
		}
	}
	if len(devs) == 0 {
		for i := 0; i < len(dataDirs); i++ {
			name := fmt.Sprintf("nvme%dn1", i)
			if !seen[name] {
				seen[name] = true
				devs = append(devs, name)
			}
		}
	}
	return devs
}

func readMountDevices() (map[string]string, error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return nil, err
	}
	out := make(map[string]string) // mountpoint → device basename
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		devPath := fields[0]
		mountPoint := fields[1]
		// Extract basename of device (e.g. /dev/nvme0n1 → nvme0n1)
		parts := strings.Split(devPath, "/")
		dev := parts[len(parts)-1]
		out[mountPoint] = dev
	}
	return out, nil
}
