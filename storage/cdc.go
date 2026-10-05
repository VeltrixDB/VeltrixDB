package storage

// cdc.go — Change Data Capture: subscribers see every successful mutation.
//
// Use cases:
//   - Cache invalidation pipelines (Redis, CDN edge caches)
//   - Real-time analytics (push to Kafka / Kinesis / Pub-Sub)
//   - Materialized view rebuilds
//   - Cross-region replication (custom transport above the CDC stream)
//
// Design choices:
//   - Subscribers receive events through a per-subscription buffered channel.
//     A slow subscriber is auto-disconnected after 3 consecutive drops on a
//     full channel; the producer side never blocks.
//   - Every engine write path emits: Put, Delete, MultiPut (MPUT, coalesced
//     pipelined PUTs, TXN commit, Raft ApplyBatch, WriteBatcher) and the
//     atomic ops (CAS / INCR / DECR / SETNX). One event per key, always Op
//     "PUT" or "DEL", emitted after the key's index entry is installed.  This is the same trade-off Kafka makes:
//     consumers must keep up or be evicted.
//   - The broker is in-process only.  For cross-process / cross-region CDC,
//     a tail consumer reads from the in-process channel and writes to its
//     own transport (gRPC, Kafka, …).
//   - Events DO carry the value bytes for PUT — that is the whole point of
//     CDC.  Subscribers that don't want values can ignore the field.
//
// Wire-protocol surface: a SUBSCRIBE binary command (0x1C) opens a long-lived
// connection that streams CDC events back to the client until the connection
// is closed.  See cmd/server/cdc_handler.go for the framing.

import (
	"sync"
	"sync/atomic"
)

// CDCEvent is one mutation. Op is "PUT" or "DEL"; Value is empty for DEL.
type CDCEvent struct {
	Op        string
	Key       string
	Value     []byte
	Timestamp int64 // microseconds since Unix epoch
}

// cdcEvictAfterDrops is how many CONSECUTIVE drops evict a subscriber. A
// successful send resets the count, so a consumer that briefly falls behind
// but keeps draining is never evicted for drops spread over its lifetime.
const cdcEvictAfterDrops = 3

// cdcSubscription is one open subscriber's send channel.
type cdcSubscription struct {
	id     uint64
	ch     chan CDCEvent
	prefix string // empty = match all
	// consecutiveDrops counts drops since the last successful send. Atomic:
	// Broadcast runs concurrently from every writing goroutine.
	consecutiveDrops atomic.Uint32
}

// CDCBroker fan-outs events to all live subscribers. Methods are safe for
// concurrent use.
type CDCBroker struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[uint64]*cdcSubscription
	// nsubs mirrors len(subs) so Broadcast with no subscribers — the normal
	// case — is one atomic load, without touching mu.
	nsubs atomic.Int32

	// Total events broadcast and dropped — exposed via metrics layer.
	totalBroadcast atomic.Uint64
	totalDropped   atomic.Uint64
}

// NewCDCBroker creates an empty broker. Caller installs it on the engine.
func NewCDCBroker() *CDCBroker {
	return &CDCBroker{subs: map[uint64]*cdcSubscription{}}
}

// Subscribe returns a receive-only channel and a cancel function. bufferSize
// caps how many events can queue before drops start; cdcEvictAfterDrops
// consecutive drops auto-disconnect the subscriber.
// keyPrefix filters events: only keys starting with the prefix are sent. Empty
// prefix means subscribe to all.
func (b *CDCBroker) Subscribe(bufferSize int, keyPrefix string) (<-chan CDCEvent, func()) {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	sub := &cdcSubscription{id: id, ch: make(chan CDCEvent, bufferSize), prefix: keyPrefix}
	b.subs[id] = sub
	b.nsubs.Store(int32(len(b.subs)))
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		b.removeLocked(id)
		b.mu.Unlock()
	}
	return sub.ch, cancel
}

// removeLocked closes and forgets subscription id. Caller holds b.mu (write).
func (b *CDCBroker) removeLocked(id uint64) {
	if s, ok := b.subs[id]; ok {
		close(s.ch)
		delete(b.subs, id)
		b.nsubs.Store(int32(len(b.subs)))
	}
}

// hasSubscribers reports whether any subscriber is registered. A hint for
// batch producers that would otherwise build one event per key for nobody.
func (b *CDCBroker) hasSubscribers() bool { return b.nsubs.Load() > 0 }

// countBroadcasts records n events that were not sent because there were no
// subscribers, keeping totalBroadcast equal to the number of mutations.
func (b *CDCBroker) countBroadcasts(n int) { b.totalBroadcast.Add(uint64(n)) }

// Broadcast delivers ev to every interested subscriber. Non-blocking on slow
// subscribers — if a channel is full, the event is dropped for that subscriber.
// cdcEvictAfterDrops (3) CONSECUTIVE drops auto-disconnect the subscription;
// any successful send resets the count.
//
// Sends happen under the read lock. They are non-blocking selects, so the
// lock is held for microseconds, and holding it is what makes them safe:
// close(s.ch) only ever runs under the write lock, so no Broadcast can send
// on a channel that an eviction or cancel is closing (sending on a snapshot
// after RUnlock could panic with "send on closed channel").
func (b *CDCBroker) Broadcast(ev CDCEvent) {
	b.totalBroadcast.Add(1)
	if b.nsubs.Load() == 0 {
		return
	}
	evict := false
	b.mu.RLock()
	for _, s := range b.subs {
		if s.prefix != "" && !hasPrefix(ev.Key, s.prefix) {
			continue
		}
		select {
		case s.ch <- ev:
			// Load first: a store on every send would bounce the line
			// between writers for no reason when nothing was dropped.
			if s.consecutiveDrops.Load() != 0 {
				s.consecutiveDrops.Store(0)
			}
		default:
			b.totalDropped.Add(1)
			if s.consecutiveDrops.Add(1) >= cdcEvictAfterDrops {
				evict = true
			}
		}
	}
	b.mu.RUnlock()

	if evict {
		b.mu.Lock()
		for id, s := range b.subs {
			// Re-checked under the write lock: a send that succeeded in
			// between reset the count, and that subscriber stays.
			if s.consecutiveDrops.Load() >= cdcEvictAfterDrops {
				b.removeLocked(id)
			}
		}
		b.mu.Unlock()
	}
}

// Stats returns broker-wide counters.
func (b *CDCBroker) Stats() (total, dropped uint64, subscribers int) {
	b.mu.RLock()
	subscribers = len(b.subs)
	b.mu.RUnlock()
	return b.totalBroadcast.Load(), b.totalDropped.Load(), subscribers
}

// Subscribe is the engine-level convenience wrapper. Returns a channel and
// cancel func; pass keyPrefix="" to receive every mutation.
func (se *StorageEngine) Subscribe(bufferSize int, keyPrefix string) (<-chan CDCEvent, func()) {
	return se.cdc.Subscribe(bufferSize, keyPrefix)
}

// CDCStats returns total broadcasts, total drops, and current subscriber count.
func (se *StorageEngine) CDCStats() (uint64, uint64, int) {
	return se.cdc.Stats()
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
