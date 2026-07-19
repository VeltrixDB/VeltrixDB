//go:build linux

package hardware

import "syscall"

func totalRAMMB() (uint64, error) {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0, err
	}
	return info.Totalram * uint64(info.Unit) / (1024 * 1024), nil
}
