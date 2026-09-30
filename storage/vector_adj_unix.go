//go:build unix

package storage

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// adjRegion is a MAP_SHARED mapping of an unlinked scratch file.
type adjRegion struct {
	f    *os.File
	data []byte
}

func newAdjRegion(dir string) (adjRegion, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return adjRegion{}, err
	}
	f, err := os.CreateTemp(dir, "hnsw-adj0-*")
	if err != nil {
		return adjRegion{}, err
	}
	_ = os.Remove(f.Name()) // scratch: freed when closed, never left behind
	return adjRegion{f: f}, nil
}

func (r *adjRegion) resize(size int) error {
	if err := r.f.Truncate(int64(size)); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	data, err := syscall.Mmap(int(r.f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap %d bytes: %w", size, err)
	}
	if r.data != nil {
		copy(data, r.data) // same file, already identical; keeps semantics obvious
		_ = syscall.Munmap(r.data)
	}
	r.data = data
	return nil
}

func (r *adjRegion) ints() []int32 {
	if len(r.data) == 0 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&r.data[0])), len(r.data)/4)
}

func (r *adjRegion) close() {
	if r.data != nil {
		_ = syscall.Munmap(r.data)
		r.data = nil
	}
	if r.f != nil {
		_ = r.f.Close()
		r.f = nil
	}
}
