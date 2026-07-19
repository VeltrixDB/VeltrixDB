//go:build !linux

package hardware

import (
	"os/exec"
	"strconv"
	"strings"
)

// totalRAMMB reads hw.memsize via sysctl on macOS.
// Falls back to 4 GB if unavailable.
func totalRAMMB() (uint64, error) {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 4096, nil
	}
	b, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 4096, nil
	}
	return b / (1024 * 1024), nil
}
