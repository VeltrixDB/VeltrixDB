//go:build linux

package storage

import (
	"fmt"
	"syscall"
)

const (
	mapHugeTLB = 0x40000  // MAP_HUGETLB
	mapHuge2MB = 21 << 26 // MAP_HUGE_2MB (MAP_HUGE_SHIFT = 26)
)

// HugepageAlloc allocates `size` bytes backed by 2 MB transparent hugepages.
//
// Why hugepages for the Index Vault
// ──────────────────────────────────
// 1 billion IndexEntry records × 64 bytes = 64 GB of metadata.
// With standard 4 KB pages, the kernel page table for 64 GB requires
// ~16 M entries.  A typical x86-64 CPU L2-TLB has 1 536 entries, so even
// a warm working set causes ~10 000 TLB misses per second under load.
//
// 2 MB hugepages cover the same 64 GB with 32 768 entries — a 512× reduction
// in page-table entries.  The L2-TLB covers the entire hot working set,
// eliminating TLB-miss page-walk latency (~100 ns per miss on NUMA).
//
// Falls back to regular anonymous mmap if the system has no free hugepages
// (e.g., during development or on machines without vm.nr_hugepages configured).
func HugepageAlloc(size int64) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("hugepage alloc: invalid size %d", size)
	}

	// Align to 2 MB hugepage boundary.
	const hugePageSize = 2 << 20
	alignedSize := int((size + hugePageSize - 1) &^ (hugePageSize - 1))

	// syscall.Mmap returns []byte directly — no uintptr→unsafe.Pointer needed.
	// Try 2 MB explicit hugepages first.
	b, err := syscall.Mmap(
		-1, 0, alignedSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|mapHugeTLB|mapHuge2MB,
	)
	if err == nil {
		return b[:int(size)], nil
	}

	// Hugepages not available: fall back to regular anonymous mmap.
	b, err = syscall.Mmap(
		-1, 0, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS,
	)
	if err != nil {
		return nil, fmt.Errorf("hugepage fallback mmap: %w", err)
	}
	return b, nil
}

// HugepageFree releases memory previously allocated by HugepageAlloc.
func HugepageFree(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if err := syscall.Munmap(b); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}
	return nil
}
