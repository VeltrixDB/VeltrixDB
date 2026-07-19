#include "lirs_cache.hpp"
#include <cassert>
#include <stdexcept>

namespace veltrix {

// ── Construction ──────────────────────────────────────────────────────────────

LIRSCache::LIRSCache(std::size_t capacity_bytes, double lir_ratio)
    : capacity_(capacity_bytes)
    , lir_limit_(static_cast<std::size_t>(
          static_cast<double>(capacity_bytes) * lir_ratio))
{
    if (lir_ratio <= 0.0 || lir_ratio >= 1.0) {
        throw std::invalid_argument("lir_ratio must be in (0, 1)");
    }
    if (capacity_bytes == 0) {
        throw std::invalid_argument("capacity_bytes must be > 0");
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

std::optional<std::span<const uint8_t>> LIRSCache::get(std::string_view key) {
    auto it = index_.find(std::string{key});
    if (it == index_.end()) {
        ++misses_;
        return std::nullopt;
    }

    Node& n = *it->second;

    if (n.state == State::HIR_Ghost) {
        // Ghost hit: the caller must fetch from disk and then call put().
        ++misses_;
        return std::nullopt;
    }

    // Resident hit (LIR or HIR-R).
    access(n);
    ++hits_;
    return std::span<const uint8_t>{n.value};
}

void LIRSCache::put(std::string_view key, std::vector<uint8_t> value) {
    const std::size_t new_size = value.size();
    const std::string skey{key};

    auto it = index_.find(skey);

    if (it != index_.end()) {
        Node& n = *it->second;

        if (n.state == State::HIR_Ghost) {
            // Ghost revival: re-insert the value and treat as HIR-R.
            n.value = std::move(value);
            n.sz    = new_size;
            n.state = State::HIR_Resident;
            hir_bytes_ += new_size;

            // Move to front of S (it's already there as a ghost).
            if (n.s_pos) {
                S_.erase(*n.s_pos);
            }
            n.s_pos = S_.insert(S_.begin(), &n);

            // Add to Q (was not in Q when it was a ghost).
            push_q_front(n);
        } else {
            // Live update: replace value, adjust byte counts.
            const std::size_t old_size = n.sz;
            n.value = std::move(value);
            n.sz    = new_size;

            if (n.state == State::LIR) {
                lir_bytes_ -= old_size;
                lir_bytes_ += new_size;
            } else {
                hir_bytes_ -= old_size;
                hir_bytes_ += new_size;
            }

            // Treat update as an access to refresh LIR/HIR state.
            access(n);
            evict_hir_if_needed();
            return;
        }
    } else {
        // Brand-new entry: always starts as HIR-Resident.
        auto node = std::make_unique<Node>();
        node->key   = skey;
        node->value = std::move(value);
        node->sz    = new_size;
        node->state = State::HIR_Resident;

        Node* ptr = node.get();
        index_[skey] = std::move(node);

        ptr->s_pos = S_.insert(S_.begin(), ptr);
        push_q_front(*ptr);
        hir_bytes_ += new_size;
    }

    prune_s();
    evict_hir_if_needed();
}

void LIRSCache::evict(std::string_view key) {
    auto it = index_.find(std::string{key});
    if (it == index_.end()) return;
    destroy(*it->second);
}

bool LIRSCache::contains(std::string_view key) const {
    auto it = index_.find(std::string{key});
    if (it == index_.end()) return false;
    return it->second->state != State::HIR_Ghost;
}

// ── LIRS state machine ────────────────────────────────────────────────────────

void LIRSCache::access(Node& n) {
    switch (n.state) {
    case State::LIR:
        access_lir(n);
        break;
    case State::HIR_Resident:
        if (n.s_pos) {
            access_hir_in_s(n);
        } else {
            access_hir_not_in_s(n);
        }
        break;
    case State::HIR_Ghost:
        // Ghosts are not resident; callers should not pass ghosts here.
        break;
    }
}

// Case 1: LIR block accessed.
// Move to top of S; prune ghosts from S bottom.
void LIRSCache::access_lir(Node& n) {
    assert(n.s_pos && "LIR block must always be in S");
    S_.erase(*n.s_pos);
    n.s_pos = S_.insert(S_.begin(), &n);
    prune_s();
}

// Case 2: HIR-Resident block that is currently in S.
// Promote it to LIR; demote the bottom LIR to HIR-R; prune S.
void LIRSCache::access_hir_in_s(Node& n) {
    assert(n.state == State::HIR_Resident && n.s_pos);

    // Move to front of S.
    S_.erase(*n.s_pos);
    n.s_pos = S_.insert(S_.begin(), &n);

    // Remove from Q (it is being promoted to LIR).
    remove_from_q(n);
    hir_bytes_ -= n.sz;

    // Promote.
    n.state = State::LIR;
    lir_bytes_ += n.sz;

    // The LIR set grew; compensate by demoting the bottom LIR.
    if (lir_bytes_ > lir_limit_) {
        demote_bottom_lir();
    }

    prune_s();
    evict_hir_if_needed();
}

// Case 3: HIR-Resident block that is NOT in S.
// Push onto front of S; refresh Q position to reflect recent access.
void LIRSCache::access_hir_not_in_s(Node& n) {
    assert(n.state == State::HIR_Resident && !n.s_pos);

    push_s_front(n);

    // Move to front of Q (recently accessed; not LRU anymore).
    if (n.q_pos) {
        Q_.erase(*n.q_pos);
    }
    push_q_front(n);

    prune_s();
}

// Demote the LIR block at the back of S to HIR-Resident.
// After prune_s() the demoted block — now HIR — will fall off the bottom of S.
void LIRSCache::demote_bottom_lir() {
    // Walk backwards to find the first LIR block from the back.
    // After prune_s has run, the very back is guaranteed LIR.
    // We still scan in case this is called before a prune.
    for (auto it = S_.rbegin(); it != S_.rend(); ++it) {
        Node* candidate = *it;
        if (candidate->state != State::LIR) continue;

        candidate->state = State::HIR_Resident;
        lir_bytes_ -= candidate->sz;
        hir_bytes_ += candidate->sz;
        push_q_front(*candidate);
        return;
    }
}

// Remove HIR entries from the bottom of S until S.back() is an LIR block.
// Ghost entries with no Q slot are fully destroyed.
void LIRSCache::prune_s() {
    while (!S_.empty()) {
        Node* back = S_.back();
        if (back->state == State::LIR) break;  // invariant satisfied

        // HIR (resident or ghost) at the bottom of S.
        S_.pop_back();
        back->s_pos.reset();

        if (back->state == State::HIR_Ghost && !back->q_pos) {
            // Ghost no longer reachable from either S or Q: delete entirely.
            index_.erase(back->key);
            // 'back' is now a dangling pointer — do not touch it.
        }
        // HIR-Resident blocks remain in Q and stay in the index.
    }
}

// Evict LRU HIR-Resident blocks from Q until total bytes ≤ capacity_.
void LIRSCache::evict_hir_if_needed() {
    while (lir_bytes_ + hir_bytes_ > capacity_ && !Q_.empty()) {
        Node* victim = Q_.back();
        Q_.pop_back();
        victim->q_pos.reset();

        hir_bytes_ -= victim->sz;
        victim->sz   = 0;
        victim->value.clear();
        victim->value.shrink_to_fit();

        if (victim->s_pos) {
            // Still referenced in S: demote to Ghost.
            victim->state = State::HIR_Ghost;
        } else {
            // Not in S either: fully remove.
            index_.erase(victim->key);
        }
        ++evictions_;
    }
}

// ── List helpers ──────────────────────────────────────────────────────────────

void LIRSCache::push_s_front(Node& n) {
    n.s_pos = S_.insert(S_.begin(), &n);
}

void LIRSCache::push_q_front(Node& n) {
    n.q_pos = Q_.insert(Q_.begin(), &n);
}

void LIRSCache::remove_from_s(Node& n) {
    if (n.s_pos) {
        S_.erase(*n.s_pos);
        n.s_pos.reset();
    }
}

void LIRSCache::remove_from_q(Node& n) {
    if (n.q_pos) {
        Q_.erase(*n.q_pos);
        n.q_pos.reset();
    }
}

// Fully remove a node from all structures and the index.
void LIRSCache::destroy(Node& n) {
    if (n.state == State::LIR)          lir_bytes_ -= n.sz;
    else if (n.state == State::HIR_Resident) hir_bytes_ -= n.sz;

    remove_from_s(n);
    remove_from_q(n);

    const std::string key = n.key;  // copy before the node is destroyed
    index_.erase(key);
    ++evictions_;
}

} // namespace veltrix
