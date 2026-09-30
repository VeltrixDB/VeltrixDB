package storage

// search_hooks.go — keeps the in-RAM search indexes in sync with the
// reserved keys they are derived from.
//
// Vector, text, vector-namespace-settings and replicated index-definition
// writes are ordinary KV writes of reserved keys ("@vec/<ns>/<id>",
// "@txt/<ns>/<id>", "@vecns/<ns>", "@idxdef/<name>"). Put,
// Delete and MultiPut call these hooks after the write commits, so the RAM
// indexes follow the KV state whichever path a write takes: the API
// (PutVector / PutText), raft apply, replication apply, raft snapshot
// restore, partition transfer on rebalance, or backup restore. Before the
// hooks, each of those paths had to remember to refresh the index itself,
// and partition transfer did not — rebalanced vectors vanished from the
// destination's search and lingered on the source's.
//
// Hook failures are logged, never returned: the KV write has already
// committed and must not be reported as failed.

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// isSearchKey is the hot-path pre-check: one byte compare for ordinary keys.
func isSearchKey(key string) bool {
	return len(key) > 5 && key[0] == '@' &&
		(IsVectorKey(key) || IsTextKey(key) ||
			strings.HasPrefix(key, vectorNSConfigPrefix) || strings.HasPrefix(key, indexDefPrefix))
}

// onSearchKeyPut refreshes the RAM index for a committed Put of key.
func (se *StorageEngine) onSearchKeyPut(key string, value []byte) {
	var err error
	switch {
	case IsVectorKey(key):
		err = se.LoadVectorBlob(key, value)
	case IsTextKey(key):
		err = se.loadTextDoc(key, value)
	case strings.HasPrefix(key, vectorNSConfigPrefix):
		err = se.applyVectorNSConfig(key[len(vectorNSConfigPrefix):], value)
	case strings.HasPrefix(key, indexDefPrefix):
		err = se.applyIndexDef(key, value)
	}
	if err != nil {
		log.Printf("[search] index refresh for %q: %v", key, err)
	}
}

// onSearchKeyDelete drops key's entry from its RAM index.
func (se *StorageEngine) onSearchKeyDelete(key string) {
	var err error
	switch {
	case IsVectorKey(key):
		err = se.UnloadVectorKey(key)
	case IsTextKey(key):
		err = se.unloadTextDoc(key)
	case strings.HasPrefix(key, indexDefPrefix):
		err = se.dropIndexDef(key)
	}
	// A deleted "@vecns/" key leaves the namespace registered: its vectors
	// are still persisted and searchable.
	if err != nil {
		log.Printf("[search] index removal for %q: %v", key, err)
	}
}

// searchRebuild tracks the startup rebuild so searches can refuse to answer
// from a half-loaded index (SearchIndexStatus). Zero value = not rebuilding.
type searchRebuild struct {
	pending atomic.Bool  // BeginSearchRebuild called, rebuild not finished
	total   atomic.Int64 // keys to load (known once the rebuild has scanned)
	loaded  atomic.Int64
}

// BeginSearchRebuild marks the search indexes as incomplete until the next
// RebuildSearchIndexes finishes. The server calls it before starting the
// rebuild in the background, so a search that arrives in between is refused
// instead of answered from an empty index.
func (se *StorageEngine) BeginSearchRebuild() { se.searchRebuild.pending.Store(true) }

// SearchIndexStatus reports whether the search indexes hold every persisted
// vector and document, and the rebuild's progress while they do not.
func (se *StorageEngine) SearchIndexStatus() (ready bool, loaded, total int64) {
	return !se.searchRebuild.pending.Load(), se.searchRebuild.loaded.Load(), se.searchRebuild.total.Load()
}

// ErrSearchRebuilding is returned (wrapped) for searches refused while the
// startup rebuild is running.
var ErrSearchRebuilding = errors.New("search indexes are still rebuilding after restart")

// CheckSearchReady returns a wrapped ErrSearchRebuilding with the progress
// while the rebuild is running, else nil.
func (se *StorageEngine) CheckSearchReady() error {
	if ready, loaded, total := se.SearchIndexStatus(); !ready {
		return fmt.Errorf("%w (loaded %d of %d; retry, or start the server with --search-allow-partial)", ErrSearchRebuilding, loaded, total)
	}
	return nil
}

// RebuildSearchIndexes reloads every search index from the persisted
// reserved keys: vector namespace settings first (so quantized namespaces are
// created as such), then vectors and text documents on GOMAXPROCS workers.
// Call once at startup AFTER WAL replay has finished (<-se.ReplayDone).
// Returns the number of vectors and text documents loaded. Corrupt entries
// are skipped with a log line rather than failing the whole rebuild.
func (se *StorageEngine) RebuildSearchIndexes() (vectors, docs int, err error) {
	defer se.searchRebuild.pending.Store(false)
	se.searchRebuild.loaded.Store(0)
	for _, k := range se.scanKeysWithPrefix(vectorNSConfigPrefix) {
		val, gerr := se.Get(k)
		if gerr != nil {
			continue
		}
		if aerr := se.applyVectorNSConfig(k[len(vectorNSConfigPrefix):], val); aerr != nil {
			log.Printf("[search] rebuild: skip %q: %v", k, aerr)
		}
	}

	var nVec, nDoc atomic.Int64
	work := make(chan string, 256)
	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				se.searchRebuild.loaded.Add(1)
				val, gerr := se.Get(k)
				if gerr != nil {
					continue
				}
				if IsVectorKey(k) {
					if lerr := se.LoadVectorBlob(k, val); lerr != nil {
						log.Printf("[search] rebuild: skip %q: %v", k, lerr)
						continue
					}
					nVec.Add(1)
				} else {
					if lerr := se.loadTextDoc(k, val); lerr != nil {
						log.Printf("[search] rebuild: skip %q: %v", k, lerr)
						continue
					}
					nDoc.Add(1)
				}
			}
		}()
	}
	var keys []string
	for _, prefix := range []string{vectorKeyPrefix, textKeyPrefix} {
		keys = append(keys, se.scanKeysWithPrefix(prefix)...)
	}
	se.searchRebuild.total.Store(int64(len(keys)))
	for _, k := range keys {
		work <- k
	}
	close(work)
	wg.Wait()
	return int(nVec.Load()), int(nDoc.Load()), nil
}
