//go:build linux

package hardware

import (
	"os"
	"path/filepath"
	"strings"
)

// diskKind reads /sys/block/<dev>/queue/rotational to classify the device.
// Returns DiskKindNVMe when the file is absent or rotational == 0.
func diskKind(path string) DiskKind {
	dev := blockDevForPath(path)
	if dev == "" {
		return DiskKindNVMe
	}
	data, err := os.ReadFile(filepath.Join("/sys/block", dev, "queue/rotational"))
	if err != nil {
		return DiskKindNVMe
	}
	if strings.TrimSpace(string(data)) == "1" {
		return DiskKindHDD
	}
	return DiskKindNVMe
}

// blockDevForPath resolves the block device name (e.g. "nvme0n1") for path
// by reading /proc/mounts and selecting the longest matching mount point.
func blockDevForPath(path string) string {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return ""
	}
	bestLen, bestDev := 0, ""
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		dev, mp := fields[0], fields[1]
		if strings.HasPrefix(path, mp) && len(mp) > bestLen {
			bestLen = len(mp)
			bestDev = filepath.Base(dev)
		}
	}
	return bestDev
}
