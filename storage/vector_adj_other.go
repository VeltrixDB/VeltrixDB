//go:build !unix

package storage

import "unsafe"

// adjRegion falls back to heap memory where mmap is unavailable: the disk
// graph setting is accepted and behaves like an in-memory graph.
type adjRegion struct{ data []byte }

func newAdjRegion(string) (adjRegion, error) { return adjRegion{}, nil }

func (r *adjRegion) resize(size int) error {
	d := make([]byte, size)
	copy(d, r.data)
	r.data = d
	return nil
}

func (r *adjRegion) ints() []int32 {
	if len(r.data) == 0 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&r.data[0])), len(r.data)/4)
}

func (r *adjRegion) close() { r.data = nil }
