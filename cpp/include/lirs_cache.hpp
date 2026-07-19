#pragma once
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace veltrix {

// ── LIRS Data Cache ───────────────────────────────────────────────────────────
//
// LIRS (Low Inter-reference Recency Set) is a scan-resistant replacement for
// LRU.  A single full-table sequential scan cannot evict frequently-referenced
// ("hot") blocks, because the algorithm measures inter-reference recency (IRR)
// — the time between the last two accesses — rather than simple recency.
//
// ── Block categories ──
//   LIR   Low Inter-reference Recency.  Always resident.  The "hot set".
//         Size bounded by lir_limit_ (≈ 99% of total cache).
//   HIR-R High IRR, Resident.  In cache but evictable.  Lives in Q queue.
//   HIR-G High IRR, Ghost.  Value evicted; key retained in S for promotion
//         tracking.  Costs only a list entry + hash map slot.
//
// ── Data structures ──
//   S — recency stack, doubly-linked list.
//         front = most recently accessed.
//         Invariant (maintained by prune_s): S.back() is always an LIR block.
//   Q — resident-HIR queue, doubly-linked list.
//         front = most recently accessed HIR.
//         back  = LRU eviction victim.
//
// ── Access algorithm ──
//   hit on LIR block   → move to front of S; prune_s.
//   hit on HIR-R in S  → promote to LIR; demote bottom LIR to HIR-R; prune_s.
//   hit on HIR-R not S → push onto front of S; refresh Q position; prune_s.
//   miss (HIR-G in S)  → ghost hit; value must be fetched from disk first,
//                         then put() re-inserts it and triggers the above.
//   miss (not in S/Q)  → pure miss; caller fetches from disk; put() inserts.
//
// Thread safety: NONE.  One LIRSCache per shard; only the owning shard thread
//   calls get/put/evict.

class LIRSCache {
public:
    // ── Construction ──────────────────────────────────────────────────────────

    // capacity_bytes: total byte budget.
    // lir_ratio:      fraction of capacity reserved for LIR blocks (0.95–0.99).
    explicit LIRSCache(std::size_t capacity_bytes, double lir_ratio = 0.99);

    LIRSCache(const LIRSCache&)            = delete;
    LIRSCache& operator=(const LIRSCache&) = delete;

    // ── Public API ────────────────────────────────────────────────────────────

    // Return a read-only view of the cached bytes, or nullopt on miss / ghost.
    // Updates the LIRS state machine on every call (promotes, moves in S/Q).
    [[nodiscard]] std::optional<std::span<const uint8_t>> get(std::string_view key);

    // Insert or update a value.  New entries always start as HIR-Resident.
    // After insertion the cache may evict one or more HIR-R blocks to honour
    // the capacity limit.
    void put(std::string_view key, std::vector<uint8_t> value);

    // Forcibly remove a key (called when a tombstone or TTL expiry is written).
    void evict(std::string_view key);

    // Is key resident (LIR or HIR-R) in the cache right now?
    [[nodiscard]] bool contains(std::string_view key) const;

    // ── Capacity ──────────────────────────────────────────────────────────────
    [[nodiscard]] std::size_t size_bytes()  const noexcept { return lir_bytes_ + hir_bytes_; }
    [[nodiscard]] std::size_t lir_bytes()   const noexcept { return lir_bytes_; }
    [[nodiscard]] std::size_t hir_bytes()   const noexcept { return hir_bytes_; }
    [[nodiscard]] std::size_t capacity()    const noexcept { return capacity_; }

    // ── Telemetry ─────────────────────────────────────────────────────────────
    [[nodiscard]] uint64_t hits()      const noexcept { return hits_; }
    [[nodiscard]] uint64_t misses()    const noexcept { return misses_; }
    [[nodiscard]] uint64_t evictions() const noexcept { return evictions_; }
    [[nodiscard]] double   hit_rate()  const noexcept {
        const uint64_t total = hits_ + misses_;
        return total ? static_cast<double>(hits_) / static_cast<double>(total) : 0.0;
    }

private:
    // ── Internal types ────────────────────────────────────────────────────────

    enum class State : uint8_t { LIR, HIR_Resident, HIR_Ghost };

    struct Node {
        std::string          key;
        std::vector<uint8_t> value;   // empty for Ghost nodes
        std::size_t          sz{0};   // byte size of value (0 for ghosts)
        State                state{State::HIR_Resident};

        // Positions in S and Q.  std::nullopt when not present in that list.
        // Using raw iterators (not pointers) because list<Node*> iterators are
        // stable under all list mutations other than erase of the same element.
        using SIter = std::list<Node*>::iterator;
        using QIter = std::list<Node*>::iterator;

        std::optional<SIter> s_pos;   // position in S_; nullopt ↔ not in S
        std::optional<QIter> q_pos;   // position in Q_; nullopt ↔ not in Q
    };

    // ── LIRS state machine ────────────────────────────────────────────────────

    // Dispatch access to the correct case.
    void access(Node& n);

    // Case 1: LIR block accessed — move to front of S.
    void access_lir(Node& n);

    // Case 2: HIR-Resident in S — promote to LIR.
    void access_hir_in_s(Node& n);

    // Case 3: HIR-Resident NOT in S — push onto S front, refresh Q.
    void access_hir_not_in_s(Node& n);

    // Demote the LIR block at the bottom of S to HIR-Resident (called when the
    // LIR set would grow beyond lir_limit_).
    void demote_bottom_lir();

    // Remove HIR (ghost or resident) blocks from the back of S until S.back()
    // is an LIR block.  Ghosts with no Q entry are fully removed from the index.
    void prune_s();

    // Evict LRU HIR-Resident blocks from Q until size_bytes() ≤ capacity_.
    void evict_hir_if_needed();

    // Push node onto the front of S and record the iterator.
    void push_s_front(Node& n);

    // Push node onto the front of Q and record the iterator.
    void push_q_front(Node& n);

    // Remove node from S (noop if not in S).
    void remove_from_s(Node& n);

    // Remove node from Q (noop if not in Q).
    void remove_from_q(Node& n);

    // Fully remove from S, Q, and the index (frees memory).
    void destroy(Node& n);

    // ── Data members ──────────────────────────────────────────────────────────

    std::size_t capacity_;   // total byte budget
    std::size_t lir_limit_;  // max bytes for LIR set (≈ 99% of capacity_)

    std::size_t lir_bytes_{0};
    std::size_t hir_bytes_{0};

    // S: recency stack — stores raw pointers into index_ nodes (stable).
    std::list<Node*> S_;

    // Q: resident-HIR queue — back is eviction victim.
    std::list<Node*> Q_;

    // Index: owns all node objects.  unique_ptr provides pointer stability
    // across unordered_map rehash (only the map bucket changes, not the Node).
    std::unordered_map<std::string, std::unique_ptr<Node>> index_;

    uint64_t hits_{0};
    uint64_t misses_{0};
    uint64_t evictions_{0};
};

} // namespace veltrix
