//go:build linux

package hardware

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// SysTune applies production OS settings for VeltrixDB.
// Requires root or CAP_SYS_ADMIN on Linux.
// Non-fatal errors are collected and returned so callers can log warnings
// without aborting startup.
func SysTune(p *Profile) []error {
	var errs []error
	push := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	// ── Network ──────────────────────────────────────────────────────────
	push(sysctl("net.core.somaxconn", "65535"))
	push(sysctl("net.ipv4.tcp_tw_reuse", "1"))
	push(sysctl("net.ipv4.ip_local_port_range", "1024 65535"))

	// ── Memory ───────────────────────────────────────────────────────────
	push(sysctl("vm.swappiness", "1"))
	push(sysctl("vm.overcommit_memory", "1"))
	// Reserve 2 MB huge pages when RAM > 32 GB (5% of RAM, min 512 pages = 1 GB).
	if p.TotalRAMMB > 32*1024 {
		push(reserveHugePages(p.TotalRAMMB))
	}

	// ── Disk: set I/O scheduler to none for every NVMe device ────────────
	for _, d := range p.Disks {
		if d.Kind == DiskKindNVMe {
			dev := blockDevForPath(d.Path)
			if dev != "" {
				push(nvmeScheduler(dev))
			}
		}
	}

	// ── CPU ──────────────────────────────────────────────────────────────
	push(cpuGovernor("performance"))
	push(stopIRQBalance())

	// ── File descriptors ─────────────────────────────────────────────────
	push(setRLimitNoFile(250_000))

	return errs
}

func sysctl(key, val string) error {
	out, err := exec.Command("sysctl", "-w", key+"="+val).CombinedOutput()
	if err != nil {
		return fmt.Errorf("sysctl %s=%s: %w (%s)", key, val, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func reserveHugePages(totalRAMMB uint64) error {
	count := totalRAMMB * 5 / 100 / 2 // 5% of RAM in 2 MB pages
	if count < 512 {
		count = 512
	}
	return os.WriteFile("/proc/sys/vm/nr_hugepages", []byte(fmt.Sprintf("%d", count)), 0644)
}

func nvmeScheduler(dev string) error {
	path := filepath.Join("/sys/block", dev, "queue/scheduler")
	if err := os.WriteFile(path, []byte("none"), 0644); err != nil {
		return fmt.Errorf("scheduler %s→none: %w", dev, err)
	}
	return nil
}

func cpuGovernor(gov string) error {
	matches, _ := filepath.Glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")
	for _, p := range matches {
		if err := os.WriteFile(p, []byte(gov), 0644); err != nil {
			return fmt.Errorf("cpu governor: %w", err)
		}
	}
	return nil
}

func stopIRQBalance() error {
	if _, err := exec.Command("systemctl", "stop", "irqbalance").CombinedOutput(); err != nil {
		// Fallback for non-systemd hosts.
		if _, err2 := exec.Command("service", "irqbalance", "stop").CombinedOutput(); err2 != nil {
			return fmt.Errorf("stop irqbalance: %w", err)
		}
	}
	return nil
}

func setRLimitNoFile(n uint64) error {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return fmt.Errorf("getrlimit: %w", err)
	}
	rl.Cur = n
	if rl.Max < n {
		rl.Max = n
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return fmt.Errorf("setrlimit NOFILE=%d: %w", n, err)
	}
	return nil
}
