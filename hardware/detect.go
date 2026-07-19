package hardware

import (
	"fmt"
	"runtime"
)

// DiskKind classifies a block device.
type DiskKind int

const (
	DiskKindNVMe DiskKind = iota // rotational == 0 (NVMe / SSD)
	DiskKindHDD                  // rotational == 1
)

func (d DiskKind) String() string {
	if d == DiskKindNVMe {
		return "NVMe/SSD"
	}
	return "HDD"
}

// DiskInfo holds per-path metadata resolved at startup.
type DiskInfo struct {
	Path string
	Kind DiskKind
}

// Profile captures discovered hardware resources used by AutoConfig.
type Profile struct {
	TotalRAMMB uint64
	CPUCores   int
	Disks      []DiskInfo
}

// Detect probes RAM, CPU count, and disk type for each path.
// On non-Linux platforms disk type always returns DiskKindNVMe.
func Detect(diskPaths []string) (*Profile, error) {
	ram, err := totalRAMMB()
	if err != nil {
		return nil, fmt.Errorf("hardware detect RAM: %w", err)
	}
	if ram == 0 {
		ram = 4096
	}

	disks := make([]DiskInfo, 0, len(diskPaths))
	for _, p := range diskPaths {
		disks = append(disks, DiskInfo{Path: p, Kind: diskKind(p)})
	}

	return &Profile{
		TotalRAMMB: ram,
		CPUCores:   runtime.NumCPU(),
		Disks:      disks,
	}, nil
}
