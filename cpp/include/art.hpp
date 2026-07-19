#pragma once

#include "allocator.hpp"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string_view>

// SIMD headers (guarded; CMakeLists enables -msse4.2)
#if defined(__SSE2__)
#  include <emmintrin.h>   // SSE2: _mm_cmpeq_epi8, _mm_movemask_epi8
#  include <immintrin.h>   // AVX (future)
#endif

namespace veltrix {

// ── 16-byte Packed Leaf Metadata ──────────────────────────────────────────────
//
// Stored at every ART leaf.  Fits in one 128-bit SIMD register.
//
// raw1 bit layout (LSB → MSB):
//   [ 0..15]  segment_id :  16  which .dat segment file holds the value
//   [16..55]  offset     :  40  byte offset within segment (1 TB addressable)
//   [56..56]  tombstone  :   1  key is deleted (grace-period sentinel)
//   [57..63]  version    :   7  monotonic write generation (CAS disambiguation)
//
// expiry_ts  : Unix timestamp (seconds); 0 = immortal.
// checksum   : CRC-8/MAXIM over (raw1 || expiry_ts), detects metadata corruption.

struct alignas(16) LeafMetadata {
    uint64_t raw1{0};       // segment_id | offset | tombstone | version
    uint32_t expiry_ts{0};  // TTL expiry in Unix epoch seconds; 0 = immortal
    uint8_t  checksum{0};   // CRC-8 of (raw1 || expiry_ts)
    uint8_t  _pad[3]{};     // → sizeof == 16

    static constexpr uint64_t kSegMask = 0x000000000000FFFFull; // [0..15]
    static constexpr uint64_t kOffMask = 0x00FFFFFFFFFFFF0000ull; // [16..55] — 40 bits
    static constexpr uint64_t kTSMask  = 0x0100000000000000ull; // [56]
    static constexpr uint64_t kVerMask = 0xFE00000000000000ull; // [57..63]

    static constexpr unsigned kSegShift = 0;
    static constexpr unsigned kOffShift = 16;
    static constexpr unsigned kTSShift  = 56;
    static constexpr unsigned kVerShift = 57;

    [[nodiscard]] constexpr uint16_t segment_id() const noexcept {
        return static_cast<uint16_t>((raw1 & kSegMask) >> kSegShift);
    }
    [[nodiscard]] constexpr uint64_t offset() const noexcept {
        return (raw1 & kOffMask) >> kOffShift;
    }
    [[nodiscard]] constexpr bool tombstone() const noexcept {
        return (raw1 & kTSMask) != 0;
    }
    [[nodiscard]] constexpr uint8_t version() const noexcept {
        return static_cast<uint8_t>((raw1 & kVerMask) >> kVerShift);
    }
    [[nodiscard]] constexpr bool is_expired(uint32_t now_sec) const noexcept {
        return expiry_ts != 0 && now_sec >= expiry_ts;
    }
    [[nodiscard]] constexpr bool is_valid(uint32_t now_sec) const noexcept {
        return !tombstone() && !is_expired(now_sec);
    }

    // ── Factories ──────────────────────────────────────────────────────────────

    [[nodiscard]] static LeafMetadata make(uint16_t seg,
                                           uint64_t off,
                                           uint32_t expiry = 0,
                                           bool     ts     = false,
                                           uint8_t  ver    = 0) noexcept {
        LeafMetadata m{};
        m.raw1 = (static_cast<uint64_t>(seg) << kSegShift)
               | ((off & 0x00FF'FFFF'FFFFull) << kOffShift)
               | (static_cast<uint64_t>(ts ? 1 : 0) << kTSShift)
               | (static_cast<uint64_t>(ver & 0x7Fu) << kVerShift);
        m.expiry_ts = expiry;
        m.checksum  = crc8(m.raw1, m.expiry_ts);
        return m;
    }

    [[nodiscard]] LeafMetadata as_tombstone() const noexcept {
        LeafMetadata m = *this;
        m.raw1    |= kTSMask;
        m.checksum = crc8(m.raw1, m.expiry_ts);
        return m;
    }
    [[nodiscard]] LeafMetadata with_new_location(uint16_t seg,
                                                  uint64_t off) const noexcept {
        LeafMetadata m = *this;
        m.raw1 = (m.raw1 & ~(kSegMask | kOffMask))
               | (static_cast<uint64_t>(seg) << kSegShift)
               | ((off & 0x00FF'FFFF'FFFFull) << kOffShift);
        m.checksum = crc8(m.raw1, m.expiry_ts);
        return m;
    }

    [[nodiscard]] bool checksum_ok() const noexcept {
        return checksum == crc8(raw1, expiry_ts);
    }

private:
    static uint8_t crc8(uint64_t r1, uint32_t exp) noexcept {
        // CRC-8/MAXIM (polynomial 0x31) over 12 bytes of metadata
        uint8_t crc = 0;
        auto feed = [&](uint8_t b) {
            crc ^= b;
            for (int i = 0; i < 8; ++i)
                crc = (crc & 0x80u) ? static_cast<uint8_t>((crc << 1) ^ 0x31u)
                                    : static_cast<uint8_t>(crc << 1);
        };
        for (int i = 0; i < 8; ++i) feed(static_cast<uint8_t>(r1 >> (i * 8)));
        for (int i = 0; i < 4; ++i) feed(static_cast<uint8_t>(exp >> (i * 8)));
        return crc;
    }
};
static_assert(sizeof(LeafMetadata) == 16, "LeafMetadata must be 16 bytes");
static_assert(alignof(LeafMetadata) == 16, "LeafMetadata must be 16-byte aligned");


// ─────────────────────────────────────────────────────────────────────────────
// ART Internal Node Definitions
// ─────────────────────────────────────────────────────────────────────────────
//
// All internal nodes share a NodeHeader at byte offset 0.
// All are aligned to 64 bytes (one CPU cache line) via alignas(64).
// The child pointer type (ArtNodePtr) uses bit 0 as a leaf tag so that leaf
// vs. internal distinction costs zero extra memory.

// Forward declarations needed for ArtNodePtr
struct ArtNode4;
struct ArtNode16;
struct ArtNode48;
struct ArtNode256;
struct ArtLeaf;

enum class NodeType : uint8_t {
    NODE4   = 4,
    NODE16  = 16,
    NODE48  = 48,
    NODE256 = 0,   // 0 to make zero-init safe
};

// ── Common node header ────────────────────────────────────────────────────────
// Every internal node starts with this 12-byte header.
struct NodeHeader {
    static constexpr uint8_t kMaxPrefixLen = 8;

    NodeType type{NodeType::NODE4};
    uint8_t  num_children{0};
    uint8_t  prefix_len{0};      // byte length of compressed prefix (0..kMaxPrefixLen)
    uint8_t  prefix_overflow{0}; // 1 if true prefix > kMaxPrefixLen (verify at leaf)
    uint8_t  prefix[kMaxPrefixLen]{};  // inline compressed prefix bytes
    // Total: 4 + 8 = 12 bytes
};
static_assert(sizeof(NodeHeader) == 12);

// ── Tagged child pointer ───────────────────────────────────────────────────────
//
// Since all allocations come from a 64-byte-aligned slab, bit 0 of every valid
// pointer is 0.  We use bit 0 to distinguish:
//   bit 0 == 0 → internal node (read NodeHeader::type for exact kind)
//   bit 0 == 1 → ArtLeaf*  (subtract 1 to get the real address)
class ArtNodePtr {
public:
    ArtNodePtr() = default;

    [[nodiscard]] static ArtNodePtr null() noexcept { return {}; }

    [[nodiscard]] static ArtNodePtr from_leaf(ArtLeaf* p) noexcept {
        return ArtNodePtr{reinterpret_cast<uintptr_t>(p) | kLeafBit};
    }
    [[nodiscard]] static ArtNodePtr from_node(void* p) noexcept {
        return ArtNodePtr{reinterpret_cast<uintptr_t>(p)};
    }

    [[nodiscard]] bool is_null()  const noexcept { return ptr_ == 0; }
    [[nodiscard]] bool is_leaf()  const noexcept { return (ptr_ & kLeafBit) != 0; }
    [[nodiscard]] bool is_node()  const noexcept { return !is_null() && !is_leaf(); }

    [[nodiscard]] ArtLeaf*   as_leaf()  const noexcept {
        return reinterpret_cast<ArtLeaf*>(ptr_ & ~kLeafBit);
    }
    [[nodiscard]] NodeHeader* header()  const noexcept {
        return reinterpret_cast<NodeHeader*>(ptr_);
    }
    [[nodiscard]] ArtNode4*   as4()     const noexcept {
        return reinterpret_cast<ArtNode4*>(ptr_);
    }
    [[nodiscard]] ArtNode16*  as16()    const noexcept {
        return reinterpret_cast<ArtNode16*>(ptr_);
    }
    [[nodiscard]] ArtNode48*  as48()    const noexcept {
        return reinterpret_cast<ArtNode48*>(ptr_);
    }
    [[nodiscard]] ArtNode256* as256()   const noexcept {
        return reinterpret_cast<ArtNode256*>(ptr_);
    }

    [[nodiscard]] NodeType node_type() const noexcept {
        return header()->type;
    }

    bool operator==(const ArtNodePtr& o) const noexcept { return ptr_ == o.ptr_; }
    bool operator!=(const ArtNodePtr& o) const noexcept { return ptr_ != o.ptr_; }

private:
    static constexpr uintptr_t kLeafBit = 1;
    explicit ArtNodePtr(uintptr_t p) noexcept : ptr_(p) {}
    uintptr_t ptr_{0};
};


// ── ArtNode4  (0–4 children) ──────────────────────────────────────────────────
//
// Exactly 64 bytes = one cache line.
// keys[] is kept sorted so linear search terminates on first key >= target.
//
// Layout (bytes):
//   [0 ..11]  NodeHeader        12
//   [12..15]  keys[4]            4
//   [16..31]  _pad[16]          16  → children at 32-byte boundary
//   [32..63]  children[4]       32  (4 × 8-byte pointers)

struct alignas(64) ArtNode4 {
    NodeHeader hdr;           // 12 bytes [0]
    uint8_t    keys[4]{};     //  4 bytes [12]
    uint8_t    _pad[16]{};    // 16 bytes [16]
    ArtNodePtr children[4];   // 32 bytes [32]
    // Total: 64 bytes ✓

    explicit ArtNode4() { hdr.type = NodeType::NODE4; }

    [[nodiscard]] ArtNodePtr* find_child(uint8_t byte) noexcept {
        for (uint8_t i = 0; i < hdr.num_children; ++i)
            if (keys[i] == byte) return &children[i];
        return nullptr;
    }

    // Inserts in sorted key order.  Caller must verify !is_full() first.
    void add_child(uint8_t byte, ArtNodePtr child) noexcept {
        uint8_t pos = 0;
        while (pos < hdr.num_children && keys[pos] < byte) ++pos;
        std::memmove(keys + pos + 1, keys + pos, hdr.num_children - pos);
        std::memmove(&children[pos + 1], &children[pos],
                     (hdr.num_children - pos) * sizeof(ArtNodePtr));
        keys[pos]     = byte;
        children[pos] = child;
        ++hdr.num_children;
    }

    [[nodiscard]] bool is_full() const noexcept { return hdr.num_children == 4; }
};
static_assert(sizeof(ArtNode4) == 64);
static_assert(alignof(ArtNode4) == 64);


// ── ArtNode16  (5–16 children) ────────────────────────────────────────────────
//
// keys[] starts at offset 16 (16-byte aligned) so _mm_load_si128 is valid.
// SSE2 _mm_cmpeq_epi8 compares all 16 keys in a single instruction.
//
// Layout (bytes):
//   [0 ..11]  NodeHeader        12
//   [12..15]  _pad0[4]           4  → keys at 16-byte boundary
//   [16..31]  keys[16]          16  ← SSE2 target
//   [32..47]  _pad1[16]         16  → children at 48-byte boundary
//   [48..175] children[16]     128  (16 × 8)
// sizeof == 176, padded to 192 by alignas(64) rule (sizeof % alignof == 0)

struct alignas(64) ArtNode16 {
    NodeHeader hdr;            // 12 bytes [0]
    uint8_t    _pad0[4]{};     //  4 bytes [12]
    uint8_t    keys[16]{};     // 16 bytes [16] ← 16-byte aligned ✓
    uint8_t    _pad1[16]{};    // 16 bytes [32]
    ArtNodePtr children[16];   // 128 bytes [48]

    explicit ArtNode16() { hdr.type = NodeType::NODE16; }

    // SSE2: compare all 16 keys in one instruction.
    [[nodiscard]] ArtNodePtr* find_child(uint8_t byte) noexcept {
#if defined(__SSE2__)
        const __m128i target = _mm_set1_epi8(static_cast<char>(byte));
        // keys[] is at offset 16 within a 64-byte-aligned struct → 16-byte aligned.
        const __m128i key_v  = _mm_load_si128(reinterpret_cast<const __m128i*>(keys));
        const __m128i cmp    = _mm_cmpeq_epi8(target, key_v);
        uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(cmp));
        mask &= (1u << hdr.num_children) - 1u; // blank unused slots
        if (mask == 0) return nullptr;
        return &children[__builtin_ctz(mask)];
#else
        for (uint8_t i = 0; i < hdr.num_children; ++i)
            if (keys[i] == byte) return &children[i];
        return nullptr;
#endif
    }

    void add_child(uint8_t byte, ArtNodePtr child) noexcept {
        uint8_t pos = 0;
        while (pos < hdr.num_children && keys[pos] < byte) ++pos;
        std::memmove(keys + pos + 1, keys + pos, hdr.num_children - pos);
        std::memmove(&children[pos + 1], &children[pos],
                     (hdr.num_children - pos) * sizeof(ArtNodePtr));
        keys[pos]     = byte;
        children[pos] = child;
        ++hdr.num_children;
    }

    [[nodiscard]] bool is_full() const noexcept { return hdr.num_children == 16; }
};


// ── ArtNode48  (17–48 children) ───────────────────────────────────────────────
//
// child_index[256]: maps each input byte → slot index (1..48) or 0 (empty).
// child_index starts at 64-byte boundary for cache-friendly stride-access by Janitor.
//
// SSE2 accelerates bulk iteration (count valid entries across 256-byte index).
//
// Layout:
//   [0  ..11 ]  NodeHeader           12
//   [12 ..63 ]  _pad0[52]            52  → child_index at 64
//   [64 ..319]  child_index[256]    256
//   [320..703]  children[48]        384  (48 × 8)
// sizeof == 704 = 11 × 64 ✓

struct alignas(64) ArtNode48 {
    NodeHeader hdr;                // 12 bytes [0]
    uint8_t    _pad0[52]{};        // 52 bytes [12]
    uint8_t    child_index[256]{}; // 256 bytes [64]  ← 64-byte aligned ✓
    ArtNodePtr children[48];       // 384 bytes [320]

    explicit ArtNode48() { hdr.type = NodeType::NODE48; }

    [[nodiscard]] ArtNodePtr* find_child(uint8_t byte) noexcept {
        const uint8_t slot = child_index[byte];
        return (slot == 0) ? nullptr : &children[slot - 1];
    }

    void add_child(uint8_t byte, ArtNodePtr child) noexcept {
        const uint8_t slot = static_cast<uint8_t>(hdr.num_children + 1);
        children[slot - 1] = child;
        child_index[byte]  = slot;
        ++hdr.num_children;
    }

    [[nodiscard]] bool is_full() const noexcept { return hdr.num_children == 48; }

    // SSE2-accelerated count of occupied slots (used by Janitor scan-cost estimate).
    [[nodiscard]] int occupied_count_simd() const noexcept {
#if defined(__SSE2__)
        int count = 0;
        const __m128i zero = _mm_setzero_si128();
        for (int i = 0; i < 256; i += 16) {
            __m128i v    = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(child_index + i));
            __m128i cmp  = _mm_cmpeq_epi8(v, zero);
            uint32_t empties = static_cast<uint32_t>(_mm_movemask_epi8(cmp));
            count += 16 - __builtin_popcount(empties);
        }
        return count;
#else
        return hdr.num_children;
#endif
    }
};
static_assert(sizeof(ArtNode48) == 704);
static_assert(alignof(ArtNode48) == 64);


// ── ArtNode256  (49–256 children) ────────────────────────────────────────────
//
// Direct-mapped array: children[byte] — O(1) lookup, no search needed.
// children[] starts at 64-byte boundary.
//
// Layout:
//   [0  ..11  ]  NodeHeader        12
//   [12 ..63  ]  _pad0[52]         52
//   [64 ..2111]  children[256]   2048  (256 × 8)
// sizeof == 2112 = 33 × 64 ✓

struct alignas(64) ArtNode256 {
    NodeHeader hdr;            // 12 bytes [0]
    uint8_t    _pad0[52]{};    // 52 bytes [12]
    ArtNodePtr children[256];  // 2048 bytes [64] ← 64-byte aligned ✓

    explicit ArtNode256() { hdr.type = NodeType::NODE256; }

    [[nodiscard]] ArtNodePtr* find_child(uint8_t byte) noexcept {
        return children[byte].is_null() ? nullptr : &children[byte];
    }

    void add_child(uint8_t byte, ArtNodePtr child) noexcept {
        children[byte] = child;
        ++hdr.num_children;
    }

    [[nodiscard]] bool is_full() const noexcept { return false; } // never full
};
static_assert(sizeof(ArtNode256) == 2112);
static_assert(alignof(ArtNode256) == 64);


// ── ArtLeaf ───────────────────────────────────────────────────────────────────
//
// Variable-size allocation: sizeof(ArtLeaf) + key_len bytes.
// Key bytes are placed immediately after the struct in the pool allocation.
// The 32-byte fixed header + key data is kept 64-byte aligned (pool guarantee).

struct ArtLeaf {
    LeafMetadata meta;        // 16 bytes [0]
    uint32_t     key_len{0};  //  4 bytes [16]
    uint8_t      _pad[12]{};  // 12 bytes [20] → header = 32 bytes
    // uint8_t key[key_len] follows at offset 32 in the allocation

    [[nodiscard]] std::string_view key_view() const noexcept {
        return {reinterpret_cast<const char*>(this + 1), key_len};
    }
    [[nodiscard]] bool key_matches(std::string_view k) const noexcept {
        return key_view() == k;
    }
};
static_assert(sizeof(ArtLeaf) == 32);


// ── Slab Allocator for ART Nodes ──────────────────────────────────────────────
//
// Uses SegmentedPool with 64-byte alignment (cache-line, not page-sized).
// All internal nodes and leaves occupy contiguous slabs, maximising L3
// cache hit probability during tree traversal.
//
// Separate from the I/O SegmentedPool (which uses 4096-byte alignment) so
// that ART node allocations don't waste large pages.

class ArtSlabAllocator {
public:
    static constexpr std::size_t kSegmentBytes = 256u * 1024; // 256 KB slabs
    static constexpr std::size_t kAlignment    = 64;          // cache-line aligned

    ArtSlabAllocator() : pool_(kSegmentBytes, kAlignment) {}

    template <typename T, typename... Args>
    [[nodiscard]] T* make(Args&&... args) {
        static_assert(alignof(T) <= kAlignment,
                      "Node alignment exceeds pool alignment");
        void* mem = pool_.allocate(sizeof(T));
        return ::new (mem) T(std::forward<Args>(args)...);
    }

    // Allocate a leaf + key bytes in one contiguous chunk.
    [[nodiscard]] ArtLeaf* make_leaf(std::string_view key, LeafMetadata meta) {
        const std::size_t sz = sizeof(ArtLeaf) + key.size();
        void* mem = pool_.allocate(sz);
        auto* leaf = ::new (mem) ArtLeaf{};
        leaf->meta    = meta;
        leaf->key_len = static_cast<uint32_t>(key.size());
        std::memcpy(reinterpret_cast<char*>(leaf + 1), key.data(), key.size());
        return leaf;
    }

    [[nodiscard]] std::size_t bytes_used() const noexcept {
        return pool_.bytes_used();
    }

private:
    SegmentedPool pool_;
};


// ── ArtIndex ──────────────────────────────────────────────────────────────────
//
// The top-level Adaptive Radix Tree.  One instance per shard (single-writer).
//
// Key properties:
//   • Prefix compression: each internal node stores up to 8 bytes of a shared
//     prefix, collapsing long chains of single-child nodes into O(1) space.
//   • In-order iteration: DFS visits keys in lexicographic order, enabling
//     the Janitor to re-write segment data in sorted key order (physical order
//     optimization).
//   • SIMD search: Node16 uses SSE2 for parallel key comparison.
//   • Cache-local: all nodes live in contiguous 64-byte-aligned slabs.
//
// Thread safety: NONE — single writer (the owning shard thread).
// The Defragmenter reads leaf metadata through const references returned by
// iterate(); it never mutates the tree structure.

class ArtIndex {
public:
    ArtIndex() = default;

    // ── Primary operations ─────────────────────────────────────────────────────

    // Insert or update key → meta.  Returns true if this was a new key.
    bool insert(std::string_view key, LeafMetadata meta);

    // Look up key.  Returns pointer to leaf metadata (valid until next insert/remove),
    // or nullptr if not found / tombstone / expired.
    [[nodiscard]] const LeafMetadata* lookup(std::string_view key) const noexcept;

    // Mutable lookup — used by Defragmenter to CAS-update locations.
    [[nodiscard]] LeafMetadata* lookup_mutable(std::string_view key) noexcept;

    // Set tombstone flag on key.  Returns false if key not found.
    bool mark_tombstone(std::string_view key) noexcept;

    // ── Iteration (in lexicographic key order) ─────────────────────────────────
    //
    // Calls callback(key, meta) for every leaf in the tree.
    // Tombstones are included (callback may filter by meta.tombstone()).
    // The callback receives a mutable LeafMetadata& to support in-place updates
    // (e.g., the Janitor updating the offset after re-writing a record).
    using IterCallback = std::function<void(std::string_view, LeafMetadata&)>;
    void iterate(const IterCallback& cb);

    // ── Stats ──────────────────────────────────────────────────────────────────
    [[nodiscard]] std::size_t size()        const noexcept { return size_; }
    [[nodiscard]] std::size_t node_bytes()  const noexcept { return alloc_.bytes_used(); }

private:
    // ── Recursive helpers ──────────────────────────────────────────────────────

    // Returns a pointer to the child slot in *node that matches byte, or nullptr.
    [[nodiscard]] static ArtNodePtr* find_child(ArtNodePtr node, uint8_t byte) noexcept;

    // Add a child to node.  Grows the node type if necessary; updates *node_ptr.
    void add_child(ArtNodePtr* node_ptr, uint8_t byte, ArtNodePtr child);

    // Count of bytes in key[depth..] that match node's compressed prefix.
    // If there is an overflow (prefix_len > kMaxPrefixLen) and mismatch is in
    // the unchecked suffix, fallback_leaf is used to read the real byte.
    static uint8_t check_prefix(const NodeHeader& hdr,
                                 std::string_view key,
                                 unsigned depth,
                                 const ArtLeaf* fallback_leaf) noexcept;

    // Recursively insert; returns true if a new leaf was created.
    bool insert_recursive(ArtNodePtr* node_ptr,
                          std::string_view key,
                          LeafMetadata meta,
                          unsigned depth);

    // Recursively look up.
    [[nodiscard]] LeafMetadata* lookup_recursive(ArtNodePtr node,
                                                  std::string_view key,
                                                  unsigned depth) const noexcept;

    // Recursive in-order DFS.
    void iterate_recursive(ArtNodePtr node, const IterCallback& cb) const;

    // Find the leftmost leaf beneath node (used during prefix-split).
    [[nodiscard]] static const ArtLeaf* minimum_leaf(ArtNodePtr node) noexcept;

    // Grow node4 → node16, node16 → node48, node48 → node256.
    ArtNodePtr grow(ArtNodePtr old_node);

    // Copy prefix from source node header (used when splitting).
    static void copy_header(NodeHeader& dst, const NodeHeader& src) noexcept;

    // ── Data members ──────────────────────────────────────────────────────────
    ArtNodePtr       root_;
    ArtSlabAllocator alloc_;
    std::size_t      size_{0};   // live leaf count (including tombstones)
};

} // namespace veltrix
