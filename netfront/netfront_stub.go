//go:build !cgo || !go1.21

package netfront

import (
	"errors"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// Available reports whether this build contains the front-end (it needs cgo).
const Available = false

// Backend mirrors the cgo build's interface so callers compile either way.
type Backend interface {
	Get(key string) ([]byte, error)
	MultiGet(keys []string) []storage.MultiGetResult
	Put(key string, value []byte, ttl int32) error
	MultiPut(reqs []storage.MultiPutRequest) []error
	Delete(key string) error
}

// Config mirrors the cgo build's Config.
type Config struct {
	Addr      string
	Threads   int
	IOBackend string
	RingDepth int
}

// Server is never constructed without cgo.
type Server struct{}

// Start always fails without cgo.
func Start(Config, Backend) (*Server, error) {
	return nil, errors.New("netfront: this binary was built with CGO_ENABLED=0; the C++ front-end is not included")
}

func (*Server) Port() int                       { return 0 }
func (*Server) IOBackend() string               { return "" }
func (*Server) Stats() (uint64, uint64, uint64) { return 0, 0, 0 }
func (*Server) Close()                          {}
