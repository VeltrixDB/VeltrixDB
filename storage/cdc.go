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
//     A slow subscriber is auto-disconnected if its channel fills up; the
//     producer side never blocks.  This is the same trade-off Kafka makes:
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

// cdcSubscription is one open subscriber's send channel.
type cdcSubscription struct {
	id      uint64
	ch      chan CDCEvent
	prefix  string // empty = match all
	dropped uint64 // events dropped for this subscriber (channel full)
}

// CDCBroker fan-outs events to all live subscribers. Methods are safe for
// concurrent use.
type CDCBroker struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[uint64]*cdcSubscription

	// Total events broadcast and dropped — exposed via metrics layer.
	totalBroadcast atomic.Uint64
	totalDropped   atomic.Uint64
}

// NewCDCBroker creates an empty broker. Caller installs it on the engine.
func NewCDCBroker() *CDCBroker {
	return &CDCBroker{subs: map[uint64]*cdcSubscription{}}
}

// Subscribe returns a receive-only channel and a cancel function. bufferSize
// caps how many events can queue before the subscriber is auto-disconnected.
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
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		if s, ok := b.subs[id]; ok {
			close(s.ch)
			delete(b.subs, id)
		}
		b.mu.Unlock()
	}
	return sub.ch, cancel
}

// Broadcast delivers ev to every interested subscriber. Non-blocking on slow
// subscribers — if a channel is full, the event is dropped for that subscriber.
// Three or more consecutive drops auto-disconnect the subscription.
func (b *CDCBroker) Broadcast(ev CDCEvent) {
	b.totalBroadcast.Add(1)
	b.mu.RLock()
	if len(b.subs) == 0 {
		b.mu.RUnlock()
		return
	}
	// Snapshot subs to a slice so we don't hold the lock while sending.
	snapshot := make([]*cdcSubscription, 0, len(b.subs))
	for _, s := range b.subs {
		if s.prefix != "" && !hasPrefix(ev.Key, s.prefix) {
			continue
		}
		snapshot = append(snapshot, s)
	}
	b.mu.RUnlock()

	var toEvict []uint64
	for _, s := range snapshot {
		select {
		case s.ch <- ev:
		default:
			b.totalDropped.Add(1)
			s.dropped++
			if s.dropped >= 3 {
				toEvict = append(toEvict, s.id)
			}
		}
	}
	if len(toEvict) > 0 {
		b.mu.Lock()
		for _, id := range toEvict {
			if s, ok := b.subs[id]; ok {
				close(s.ch)
				delete(b.subs, id)
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
