package storage

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Cache is the pluggable data-block cache interface.  Eviction applies only to
// this layer; the Index Vault (shardedIndex) is never evicted.
type Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Evict(key string)
	Size() uint64
	Stats() CacheStats
}

// ── LIRS (Low Inter-reference Recency Set) ───────────────────────────────────
//
// LIRS is scan-resistant unlike LRU: a single sequential scan cannot displace
// frequently referenced blocks because frequency is captured implicitly through
// the inter-reference recency of each block.
//
// Two block categories:
//   LIR — Low Inter-reference Recency. Always resident. High temporal locality.
//   HIR — High Inter-reference Recency. Evictable; tracked in Q queue.
//
// Two data structures:
//   S — recency stack.  Front = most recently accessed.  Contains LIR blocks
//       plus any HIR block referenced since it was last in S.  Invariant: the
//       element at S.Back() is always a LIR block (maintained by pruneS).
//   Q — resident HIR queue.  Front = most recently accessed HIR.
//       Back = LRU victim for eviction.
//
// On every access the inter-reference recency is implicitly updated by the
// block's position in S.  HIR blocks that appear in S are eligible for
// promotion to LIR on their next access.

type lirsNode struct {
	key      string
	value    []byte
	size     uint64
	isLIR    bool
	resident bool // false = ghost: key tracked in S but value evicted
	// priority encodes value-awareness: 2 = small (≤ smallValueThreshold),
	// 1 = large.  Used by the eviction scan to prefer large cold victims.
	priority uint8
	sElem    *list.Element
	qElem    *list.Element
}

// smallValueThreshold — values at or below this size are treated as "small"
// and receive priority in the LIR set.  At 256 bytes a ValuePointer + small
// value fits in a single 64-byte cache line with no second fetch.
const smallValueThreshold = 256

// victimScanWindow controls how many HIR entries we scan when looking for the
// largest-size eviction victim.  A larger window finds more byte savings but
// adds O(window) work per eviction.  16 is a good balance: scanning 16 entries
// at ~64 B/entry costs one cache line fetch and typically finds a victim
// 2-8× larger than the pure LRU tail.
const victimScanWindow = 16

// LIRSCache implements the Cache interface with LIRS eviction.
//
// Value-aware extension:
//   - Each lirsNode carries a `priority` score: small values (≤ 256 B) get
//     score 2 (high), larger values score 1 (normal).
//   - evictHIRIfNeeded scans up to victimScanWindow entries from the cold tail
//     of Q and evicts the one with the lowest priority × inverse_size score,
//     preferring to free large cold values before small hot ones.
type LIRSCache struct {
	mu       sync.Mutex
	maxBytes uint64
	lirLimit uint64 // target ceiling for the LIR set in bytes

	lirBytes uint64
	hirBytes uint64

	S     *list.List           // recency stack
	Q     *list.List           // resident-HIR queue
	index map[string]*lirsNode // key → node (all states: LIR, HIR, ghost)

	hitCount  atomic.Uint64
	missCount atomic.Uint64
	evictions atomic.Uint64
}

// NewLIRSCache creates a LIRS cache capped at maxSizeMB megabytes.
// lirRatio controls what fraction of the cache the LIR set may occupy (0.95
// is the standard recommendation; the remaining 5% is the resident HIR quota).
func NewLIRSCache(maxSizeMB uint32, lirRatio float64) Cache {
	if lirRatio <= 0 || lirRatio >= 1 {
		lirRatio = 0.95
	}
	maxBytes := uint64(maxSizeMB) * 1024 * 1024
	return &LIRSCache{
		maxBytes: maxBytes,
		lirLimit: uint64(float64(maxBytes) * lirRatio),
		S:        list.New(),
		Q:        list.New(),
		index:    make(map[string]*lirsNode),
	}
}

// Get retrieves a cached value and updates the LIRS state machine.
func (c *LIRSCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.index[key]
	if !ok || !node.resident {
		c.missCount.Add(1)
		return nil, false
	}

	c.access(node)
	c.hitCount.Add(1)
	return node.value, true
}

// Put inserts or updates a value.  New entries always enter as HIR resident.
func (c *LIRSCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := uint64(len(value))

	if node, ok := c.index[key]; ok {
		oldSize := node.size
		node.value = value
		node.size = size

		if node.resident {
			delta := int64(size) - int64(oldSize)
			if node.isLIR {
				c.lirBytes = addDelta(c.lirBytes, delta)
			} else {
				c.hirBytes = addDelta(c.hirBytes, delta)
			}
			c.access(node)
		} else {
			node.value = value
			node.size = size
			node.resident = true
			if size <= smallValueThreshold {
				node.priority = 2
			} else {
				node.priority = 1
			}
			c.hirBytes += size

			if node.sElem == nil {
				node.sElem = c.S.PushFront(node)
			} else {
				c.S.MoveToFront(node.sElem)
			}
			if node.qElem == nil {
				node.qElem = c.Q.PushFront(node)
			}
		}
	} else {
		p := uint8(1)
		if size <= smallValueThreshold {
			p = 2
		}
		node := &lirsNode{
			key:      key,
			value:    value,
			size:     size,
			isLIR:    false,
			resident: true,
			priority: p,
		}
		node.sElem = c.S.PushFront(node)
		node.qElem = c.Q.PushFront(node)
		c.index[key] = node
		c.hirBytes += size
	}

	c.pruneS()
	c.evictHIRIfNeeded()
}

// Evict forcibly removes a key from the cache.
func (c *LIRSCache) Evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.index[key]
	if !ok {
		return
	}
	c.removeNode(node)
}

func (c *LIRSCache) Size() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lirBytes + c.hirBytes
}

func (c *LIRSCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	hits := c.hitCount.Load()
	misses := c.missCount.Load()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return CacheStats{
		Hits:             hits,
		Misses:           misses,
		Evictions:        c.evictions.Load(),
		CurrentSizeBytes: c.lirBytes + c.hirBytes,
		MaxSizeBytes:     c.maxBytes,
		HitRate:          hitRate,
	}
}

// ── LIRS internal state machine ───────────────────────────────────────────────

// access updates S/Q positions for an already-resident node.
// Caller must hold c.mu.
func (c *LIRSCache) access(node *lirsNode) {
	if node.isLIR {
		c.S.MoveToFront(node.sElem)
		c.pruneS()
		return
	}

	// HIR resident hit.
	if node.sElem != nil {
		c.S.MoveToFront(node.sElem)
		c.Q.Remove(node.qElem)
		node.qElem = nil
		c.hirBytes -= node.size
		node.isLIR = true
		c.lirBytes += node.size

		if c.lirBytes > c.lirLimit && c.S.Len() > 1 {
			c.demoteBottomLIR()
		}
		c.pruneS()
	} else {
		node.sElem = c.S.PushFront(node)
		if node.qElem != nil {
			c.Q.MoveToFront(node.qElem)
		}
		c.pruneS()
	}
}

// demoteBottomLIR converts the LIR block at the bottom of S into a HIR block
// and places it at the front of Q.  Called when the LIR set exceeds lirLimit.
// Caller must hold c.mu.
func (c *LIRSCache) demoteBottomLIR() {
	// Walk backwards from S.Back() to find the first LIR node
	// (pruneS has removed any non-LIR nodes at the very back, but the current
	// node we just moved to front may have exposed a non-LIR node transiently).
	for elem := c.S.Back(); elem != nil; elem = elem.Prev() {
		candidate := elem.Value.(*lirsNode)
		if !candidate.isLIR {
			continue
		}
		// Demote this LIR node to HIR.
		candidate.isLIR = false
		c.lirBytes -= candidate.size
		c.hirBytes += candidate.size
		if candidate.resident {
			candidate.qElem = c.Q.PushFront(candidate)
		}
		break
	}
}

// pruneS removes HIR blocks (both ghost and resident) from the bottom of S
// until the bottom element is a LIR block.  This maintains the invariant that
// S.Back() is always LIR, enabling O(1) demotion during HIR→LIR promotion.
// Caller must hold c.mu.
func (c *LIRSCache) pruneS() {
	for c.S.Len() > 0 {
		back := c.S.Back()
		node := back.Value.(*lirsNode)
		if node.isLIR {
			break
		}
		// Non-LIR at bottom of S: remove from S.
		c.S.Remove(back)
		node.sElem = nil
		// If it was a ghost (not in Q either), drop from index entirely.
		if !node.resident && node.qElem == nil {
			delete(c.index, node.key)
		}
	}
}

// evictHIRIfNeeded evicts resident HIR blocks until total cache bytes are
// within maxBytes.  Evicted blocks become ghosts if they are still referenced
// in S; otherwise they are fully removed from the index.
//
// Value-aware victim selection (Greedy-Dual Size, bounded scan):
// Instead of always evicting Q.Back() (pure LRU), we scan up to
// victimScanWindow entries from the cold tail of Q and pick the one with the
// best "eviction value" score: large low-priority values are evicted first,
// small high-priority values are retained as long as possible.
//
//	score = size / priority   (lower score = better to keep)
//
// By evicting the highest-score (large, low-priority) entry we maximise bytes
// freed per eviction while keeping small hot values in the cache.
//
// Caller must hold c.mu.
func (c *LIRSCache) evictHIRIfNeeded() {
	for c.lirBytes+c.hirBytes > c.maxBytes && c.Q.Len() > 0 {
		victim := c.findEvictionVictim()
		if victim == nil {
			break
		}
		node := victim.Value.(*lirsNode)

		c.Q.Remove(victim)
		node.qElem = nil
		c.hirBytes -= node.size
		node.value = nil
		node.resident = false
		c.evictions.Add(1)

		if node.sElem == nil {
			delete(c.index, node.key)
		} else {
		}
	}
}

// findEvictionVictim scans up to victimScanWindow entries from the cold tail
// of Q and returns the element with the highest size/priority ratio.
// Falls back to Q.Back() (pure LRU) if Q is empty.
// Caller must hold c.mu.
func (c *LIRSCache) findEvictionVictim() *list.Element {
	best := c.Q.Back()
	if best == nil {
		return nil
	}
	bestNode := best.Value.(*lirsNode)
	// score = size / priority; we compare size*otherPriority vs otherSize*priority
	// to avoid floating point — equivalent to comparing size/priority ratios.
	bestSz := bestNode.size
	bestPri := uint64(bestNode.priority)

	elem := best.Prev()
	for i := 1; i < victimScanWindow && elem != nil; i++ {
		n := elem.Value.(*lirsNode)
		// Is n a better victim than best? n is better if n.size/n.priority > best.size/best.priority
		if n.size*bestPri > bestSz*uint64(n.priority) {
			best = elem
			bestSz = n.size
			bestPri = uint64(n.priority)
		}
		elem = elem.Prev()
	}
	return best
}

// removeNode fully removes node from every data structure.
// Caller must hold c.mu.
func (c *LIRSCache) removeNode(node *lirsNode) {
	if node.sElem != nil {
		c.S.Remove(node.sElem)
		node.sElem = nil
	}
	if node.qElem != nil {
		c.Q.Remove(node.qElem)
		node.qElem = nil
	}
	if node.resident {
		if node.isLIR {
			c.lirBytes -= node.size
		} else {
			c.hirBytes -= node.size
		}
	}
	delete(c.index, node.key)
}

// addDelta applies a signed delta to an unsigned counter, clamping at zero.
func addDelta(base uint64, delta int64) uint64 {
	if delta < 0 && uint64(-delta) > base {
		return 0
	}
	return uint64(int64(base) + delta)
}
