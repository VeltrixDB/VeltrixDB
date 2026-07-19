package storage

import (
	"log"
	"sync"
	"time"
)

// CheckpointManager runs periodic WAL checkpoints so crash recovery replays
// only the delta since the last checkpoint, not the full write history.
//
// On every tick it calls the same routine that Close() uses: write a compacted
// WAL with one record per live key, then truncate the live WAL file.  On the
// next startup the replayed WAL is O(numLiveKeys) regardless of how many writes
// happened since the previous checkpoint.
//
// Default interval: 5 minutes.  At 100K writes/s that is up to 30M WAL entries
// between checkpoints; a checkpoint reduces replay to ≤ numLiveKeys entries.
//
// The checkpoint is safe to call while the engine is accepting writes.  It
// takes a per-disk write lock only for the duration of the fdatasync+rename of
// the compacted file — the same lock already held during every WAL flush.
type CheckpointManager struct {
	se       *StorageEngine
	interval time.Duration
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewCheckpointManager creates a manager but does not start it.
func NewCheckpointManager(se *StorageEngine, interval time.Duration) *CheckpointManager {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &CheckpointManager{
		se:       se,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start launches the background ticker goroutine.
func (cm *CheckpointManager) Start() {
	cm.wg.Add(1)
	go cm.loop()
	log.Printf("[checkpoint] manager started  interval=%s", cm.interval)
}

// Stop shuts down the ticker gracefully.
func (cm *CheckpointManager) Stop() {
	close(cm.done)
	cm.wg.Wait()
}

// ForceCheckpoint runs one checkpoint immediately (outside the ticker cadence).
func (cm *CheckpointManager) ForceCheckpoint() error {
	return cm.se.Checkpoint()
}

func (cm *CheckpointManager) loop() {
	defer cm.wg.Done()
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()
	for {
		select {
		case <-cm.done:
			return
		case <-ticker.C:
			start := time.Now()
			if err := cm.se.Checkpoint(); err != nil {
				log.Printf("[checkpoint] periodic checkpoint error: %v", err)
			} else {
				log.Printf("[checkpoint] periodic checkpoint done  elapsed=%s", time.Since(start).Round(time.Millisecond))
			}
		}
	}
}
