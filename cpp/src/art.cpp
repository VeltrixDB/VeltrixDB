#include "art.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>

namespace veltrix {

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// Walk to the leftmost (minimum-key) leaf in a subtree.
const ArtLeaf* ArtIndex::minimum_leaf(ArtNodePtr node) noexcept {
    while (!node.is_null() && !node.is_leaf()) {
        const NodeHeader* hdr = node.header();
        switch (hdr->type) {
        case NodeType::NODE4: {
            auto* n = node.as4();
            node = (n->hdr.num_children > 0) ? n->children[0] : ArtNodePtr::null();
            break;
        }
        case NodeType::NODE16: {
            auto* n = node.as16();
            node = (n->hdr.num_children > 0) ? n->children[0] : ArtNodePtr::null();
            break;
        }
        case NodeType::NODE48: {
            auto* n = node.as48();
            for (int b = 0; b < 256; ++b) {
                if (n->child_index[b] != 0) {
                    node = n->children[n->child_index[b] - 1];
                    break;
                }
            }
            break;
        }
        case NodeType::NODE256: {
            auto* n = node.as256();
            for (int b = 0; b < 256; ++b) {
                if (!n->children[b].is_null()) {
                    node = n->children[b];
                    break;
                }
            }
            break;
        }
        }
    }
    return node.is_leaf() ? node.as_leaf() : nullptr;
}

// Returns the count of bytes in key[depth..] that match hdr.prefix[0..].
// If the prefix was truncated (prefix_overflow), we trust the stored bytes
// and fall back to the provided leaf's key for bytes past kMaxPrefixLen.
uint8_t ArtIndex::check_prefix(const NodeHeader& hdr,
                                std::string_view key,
                                unsigned depth,
                                const ArtLeaf* fallback_leaf) noexcept {
    const uint8_t stored = static_cast<uint8_t>(
        std::min<unsigned>(hdr.prefix_len, NodeHeader::kMaxPrefixLen));

    uint8_t matched = 0;
    while (matched < stored) {
        const unsigned key_pos = depth + matched;
        if (key_pos >= key.size()) break;
        if (static_cast<uint8_t>(key[key_pos]) != hdr.prefix[matched]) break;
        ++matched;
    }

    // If we've matched all stored bytes but there's more claimed prefix
    // (prefix_overflow), compare against the fallback leaf's key.
    if (matched == stored && hdr.prefix_overflow && fallback_leaf) {
        const std::string_view leaf_key = fallback_leaf->key_view();
        for (uint8_t extra = stored; extra < hdr.prefix_len; ++extra) {
            const unsigned key_pos = depth + extra;
            if (key_pos >= key.size() || key_pos >= leaf_key.size()) break;
            if (key[key_pos] != leaf_key[key_pos]) break;
            ++matched;
        }
    }

    return matched;
}

static void copy_header(NodeHeader& dst, const NodeHeader& src) noexcept {
    dst.num_children    = src.num_children;
    dst.prefix_len      = src.prefix_len;
    dst.prefix_overflow = src.prefix_overflow;
    std::memcpy(dst.prefix, src.prefix, NodeHeader::kMaxPrefixLen);
}

// ─────────────────────────────────────────────────────────────────────────────
// find_child  (dispatch to the right node type)
// ─────────────────────────────────────────────────────────────────────────────

ArtNodePtr* ArtIndex::find_child(ArtNodePtr node, uint8_t byte) noexcept {
    if (node.is_null() || node.is_leaf()) return nullptr;
    switch (node.node_type()) {
    case NodeType::NODE4:   return node.as4()->find_child(byte);
    case NodeType::NODE16:  return node.as16()->find_child(byte);
    case NodeType::NODE48:  return node.as48()->find_child(byte);
    case NodeType::NODE256: return node.as256()->find_child(byte);
    }
    return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// grow  —  promote an internal node to the next larger type
// ─────────────────────────────────────────────────────────────────────────────

ArtNodePtr ArtIndex::grow(ArtNodePtr old_node) {
    const NodeType type = old_node.node_type();

    if (type == NodeType::NODE4) {
        auto* old = old_node.as4();
        auto* n   = alloc_.make<ArtNode16>();
        copy_header(n->hdr, old->hdr);
        n->hdr.num_children = 0;
        for (uint8_t i = 0; i < old->hdr.num_children; ++i)
            n->add_child(old->keys[i], old->children[i]);
        return ArtNodePtr::from_node(n);
    }

    if (type == NodeType::NODE16) {
        auto* old = old_node.as16();
        auto* n   = alloc_.make<ArtNode48>();
        copy_header(n->hdr, old->hdr);
        n->hdr.num_children = 0;
        for (uint8_t i = 0; i < old->hdr.num_children; ++i)
            n->add_child(old->keys[i], old->children[i]);
        return ArtNodePtr::from_node(n);
    }

    if (type == NodeType::NODE48) {
        auto* old = old_node.as48();
        auto* n   = alloc_.make<ArtNode256>();
        copy_header(n->hdr, old->hdr);
        n->hdr.num_children = 0;
        for (int b = 0; b < 256; ++b) {
            if (old->child_index[b] != 0)
                n->add_child(static_cast<uint8_t>(b),
                             old->children[old->child_index[b] - 1]);
        }
        return ArtNodePtr::from_node(n);
    }

    // NODE256 is never full
    assert(false && "grow called on NODE256");
    return old_node;
}

// ─────────────────────────────────────────────────────────────────────────────
// add_child  —  insert a child pointer, growing the node if needed
// ─────────────────────────────────────────────────────────────────────────────

void ArtIndex::add_child(ArtNodePtr* node_ptr, uint8_t byte, ArtNodePtr child) {
    ArtNodePtr node = *node_ptr;

    bool needs_grow = false;
    switch (node.node_type()) {
    case NodeType::NODE4:   needs_grow = node.as4()->is_full();   break;
    case NodeType::NODE16:  needs_grow = node.as16()->is_full();  break;
    case NodeType::NODE48:  needs_grow = node.as48()->is_full();  break;
    case NodeType::NODE256: needs_grow = false;                   break;
    }

    if (needs_grow) {
        node = grow(node);
        *node_ptr = node;
    }

    switch (node.node_type()) {
    case NodeType::NODE4:   node.as4()->add_child(byte, child);   break;
    case NodeType::NODE16:  node.as16()->add_child(byte, child);  break;
    case NodeType::NODE48:  node.as48()->add_child(byte, child);  break;
    case NodeType::NODE256: node.as256()->add_child(byte, child); break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// insert_recursive
//
// depth  — byte index into `key` we're currently consuming.
//
// Returns true if a new leaf was inserted (size_ incremented by caller).
// ─────────────────────────────────────────────────────────────────────────────

bool ArtIndex::insert_recursive(ArtNodePtr* node_ptr,
                                 std::string_view key,
                                 LeafMetadata meta,
                                 unsigned depth) {
    ArtNodePtr node = *node_ptr;

    // ── Case 1: empty slot → create new leaf ──────────────────────────────────
    if (node.is_null()) {
        *node_ptr = ArtNodePtr::from_leaf(alloc_.make_leaf(key, meta));
        return true; // new key
    }

    // ── Case 2: existing leaf ─────────────────────────────────────────────────
    if (node.is_leaf()) {
        ArtLeaf* existing = node.as_leaf();
        if (existing->key_matches(key)) {
            // Key already exists: update metadata in-place.
            existing->meta = meta;
            return false; // not a new key
        }
        // Two different keys share this slot → create an internal node4
        // with a compressed prefix for their common prefix.
        const std::string_view existing_key = existing->key_view();

        auto* new_node = alloc_.make<ArtNode4>();

        // Find the length of the common prefix between the two keys from `depth`.
        unsigned prefix_len = 0;
        while (depth + prefix_len < key.size() &&
               depth + prefix_len < existing_key.size() &&
               key[depth + prefix_len] == existing_key[depth + prefix_len]) {
            ++prefix_len;
        }

        // Store the compressed prefix in the new node.
        new_node->hdr.prefix_len = static_cast<uint8_t>(
            std::min<unsigned>(prefix_len, NodeHeader::kMaxPrefixLen));
        new_node->hdr.prefix_overflow = (prefix_len > NodeHeader::kMaxPrefixLen) ? 1 : 0;
        std::memcpy(new_node->hdr.prefix, key.data() + depth,
                    new_node->hdr.prefix_len);

        // Attach the existing leaf and the new leaf as children.
        const unsigned split_depth = depth + prefix_len;
        const uint8_t existing_byte =
            (split_depth < existing_key.size())
                ? static_cast<uint8_t>(existing_key[split_depth])
                : 0; // key exhausted → terminal byte
        const uint8_t new_byte =
            (split_depth < key.size())
                ? static_cast<uint8_t>(key[split_depth])
                : 0;

        new_node->add_child(existing_byte, node); // existing leaf
        new_node->add_child(new_byte,
                            ArtNodePtr::from_leaf(alloc_.make_leaf(key, meta)));

        *node_ptr = ArtNodePtr::from_node(new_node);
        return true;
    }

    // ── Case 3: internal node ─────────────────────────────────────────────────
    NodeHeader& hdr = *node.header();

    // Check compressed prefix.
    const ArtLeaf* min_leaf = minimum_leaf(node);
    const uint8_t prefix_match = check_prefix(hdr, key, depth, min_leaf);

    if (prefix_match < hdr.prefix_len) {
        // Prefix mismatch: split this node's prefix at `prefix_match`.
        auto* new_node = alloc_.make<ArtNode4>();
        new_node->hdr.prefix_len = static_cast<uint8_t>(
            std::min<unsigned>(prefix_match, NodeHeader::kMaxPrefixLen));
        new_node->hdr.prefix_overflow =
            (prefix_match > NodeHeader::kMaxPrefixLen) ? 1 : 0;
        std::memcpy(new_node->hdr.prefix, hdr.prefix, new_node->hdr.prefix_len);

        // The old node keeps the suffix of its prefix after the split point.
        const uint8_t old_byte =
            (prefix_match < NodeHeader::kMaxPrefixLen)
                ? hdr.prefix[prefix_match]
                : (min_leaf
                       ? static_cast<uint8_t>(
                             min_leaf->key_view()[depth + prefix_match])
                       : 0u);

        // Adjust old node's prefix: strip the first (prefix_match + 1) bytes.
        hdr.prefix_overflow = 0;
        const uint8_t remaining =
            static_cast<uint8_t>(hdr.prefix_len - prefix_match - 1);
        hdr.prefix_len = remaining;
        if (remaining > 0 && prefix_match + 1 < NodeHeader::kMaxPrefixLen) {
            std::memmove(hdr.prefix,
                         hdr.prefix + prefix_match + 1,
                         std::min<uint8_t>(remaining, NodeHeader::kMaxPrefixLen));
        }

        new_node->add_child(old_byte, node);

        const uint8_t new_byte =
            (depth + prefix_match < key.size())
                ? static_cast<uint8_t>(key[depth + prefix_match])
                : 0;
        new_node->add_child(new_byte,
                            ArtNodePtr::from_leaf(alloc_.make_leaf(key, meta)));
        *node_ptr = ArtNodePtr::from_node(new_node);
        return true;
    }

    // Prefix fully matched: consume it and descend.
    depth += hdr.prefix_len;

    if (depth >= key.size()) {
        // Key is exhausted at this node.  If there's a null-terminator child,
        // use that; otherwise add one.
        ArtNodePtr* slot = find_child(node, 0);
        if (slot) {
            // Update the leaf under the null terminator.
            return insert_recursive(slot, key, meta, depth);
        }
        add_child(node_ptr, 0,
                  ArtNodePtr::from_leaf(alloc_.make_leaf(key, meta)));
        return true;
    }

    const uint8_t next_byte = static_cast<uint8_t>(key[depth]);
    ArtNodePtr*   slot      = find_child(node, next_byte);

    if (slot) {
        return insert_recursive(slot, key, meta, depth + 1);
    }

    // No child for this byte: add a new leaf.
    add_child(node_ptr, next_byte,
              ArtNodePtr::from_leaf(alloc_.make_leaf(key, meta)));
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// lookup_recursive
// ─────────────────────────────────────────────────────────────────────────────

LeafMetadata* ArtIndex::lookup_recursive(ArtNodePtr node,
                                          std::string_view key,
                                          unsigned depth) const noexcept {
    while (!node.is_null()) {
        if (node.is_leaf()) {
            ArtLeaf* leaf = node.as_leaf();
            return leaf->key_matches(key) ? &leaf->meta : nullptr;
        }

        const NodeHeader& hdr = *node.header();

        // Check compressed prefix.
        const ArtLeaf* min_leaf = minimum_leaf(node);
        const uint8_t prefix_match = check_prefix(hdr, key, depth, min_leaf);
        if (prefix_match < hdr.prefix_len) return nullptr; // prefix mismatch

        depth += hdr.prefix_len;
        if (depth >= key.size()) {
            // Try null-terminator child.
            ArtNodePtr* slot = find_child(node, 0);
            if (!slot) return nullptr;
            node = *slot;
            continue;
        }

        const uint8_t next_byte = static_cast<uint8_t>(key[depth]);
        ArtNodePtr*   slot      = find_child(node, next_byte);
        if (!slot) return nullptr;
        node = *slot;
        depth++;
    }
    return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// iterate_recursive  (DFS, in lexicographic order)
// ─────────────────────────────────────────────────────────────────────────────

void ArtIndex::iterate_recursive(ArtNodePtr node, const IterCallback& cb) const {
    if (node.is_null()) return;

    if (node.is_leaf()) {
        ArtLeaf* leaf = node.as_leaf();
        cb(leaf->key_view(), leaf->meta);
        return;
    }

    const NodeHeader& hdr = *node.header();

    switch (hdr.type) {
    case NodeType::NODE4: {
        auto* n = node.as4();
        for (uint8_t i = 0; i < n->hdr.num_children; ++i)
            iterate_recursive(n->children[i], cb);
        break;
    }
    case NodeType::NODE16: {
        auto* n = node.as16();
        // keys[] is sorted, so iteration is already in lexicographic order.
        for (uint8_t i = 0; i < n->hdr.num_children; ++i)
            iterate_recursive(n->children[i], cb);
        break;
    }
    case NodeType::NODE48: {
        auto* n = node.as48();
        // Iterate bytes 0..255 in order to produce lexicographic traversal.
        for (int b = 0; b < 256; ++b) {
            if (n->child_index[b] != 0)
                iterate_recursive(n->children[n->child_index[b] - 1], cb);
        }
        break;
    }
    case NodeType::NODE256: {
        auto* n = node.as256();
        for (int b = 0; b < 256; ++b) {
            if (!n->children[b].is_null())
                iterate_recursive(n->children[b], cb);
        }
        break;
    }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

bool ArtIndex::insert(std::string_view key, LeafMetadata meta) {
    const bool is_new = insert_recursive(&root_, key, meta, 0);
    if (is_new) ++size_;
    return is_new;
}

const LeafMetadata* ArtIndex::lookup(std::string_view key) const noexcept {
    return lookup_recursive(root_, key, 0);
}

LeafMetadata* ArtIndex::lookup_mutable(std::string_view key) noexcept {
    return lookup_recursive(root_, key, 0);
}

bool ArtIndex::mark_tombstone(std::string_view key) noexcept {
    LeafMetadata* m = lookup_mutable(key);
    if (!m) return false;
    *m = m->as_tombstone();
    return true;
}

void ArtIndex::iterate(const IterCallback& cb) {
    iterate_recursive(root_, cb);
}

// ─────────────────────────────────────────────────────────────────────────────
// Janitor: Physical-Order Compaction using ART in-order traversal
//
// The ART's in-order traversal visits keys in lexicographic (sorted) order.
// By re-writing live records to a new segment in this order, the physical
// layout of data on disk mirrors the logical (key-sorted) index order.
//
// Result:
//   • Future range scans become sequential reads on NVMe → maximises
//     SSD internal parallelism and reduces controller queue depth.
//   • The SSD's read-ahead prefetcher fires on every sequential read.
//   • After compaction, a scan for key range [A..B] reads only the pages
//     that contain those keys — no wasted I/O.
//
// Usage:
//   1. Open a fresh segment file (new_fd) for writing.
//   2. Call art_janitor_compact().
//   3. Each live record is written to new_fd in sorted key order.
//   4. The leaf's meta.offset is updated to its new position in new_fd.
//   5. Caller fsyncs new_fd, then swaps it for the old segment files.
//
// Parameters:
//   idx        — the ART index for this shard
//   now_sec    — current Unix time (for TTL checks)
//   read_record— callback: (old_seg, old_offset) → raw record bytes (or empty)
//   write_record— callback: (key, raw_bytes) → new_offset in new segment
// ─────────────────────────────────────────────────────────────────────────────

void art_janitor_compact(
    ArtIndex& idx,
    uint32_t  now_sec,
    const std::function<std::vector<uint8_t>(uint16_t seg, uint64_t off)>&
        read_record,
    const std::function<uint64_t(std::string_view key,
                                  const std::vector<uint8_t>& bytes)>&
        write_record)
{
    // Sequential scan of the ART tree in lexicographic key order.
    // Valid records are re-written to the new segment in this exact order.
    // The leaf metadata is updated with the new offset after each write.
    idx.iterate([&](std::string_view key, LeafMetadata& meta) {
        if (!meta.is_valid(now_sec)) return; // skip tombstones and expired keys

        std::vector<uint8_t> bytes =
            read_record(meta.segment_id(), meta.offset());
        if (bytes.empty()) return; // record vanished or I/O error — skip

        const uint64_t new_offset = write_record(key, bytes);

        // Update the leaf's location in-place — next lookup finds the new segment.
        // The segment_id of the destination segment is supplied by the caller
        // via the write_record return value convention:
        //   high 16 bits = new segment_id, low 48 bits = new offset
        const uint16_t new_seg = static_cast<uint16_t>(new_offset >> 48);
        const uint64_t new_off = new_offset & 0x0000'FFFF'FFFF'FFFFull;
        meta = meta.with_new_location(new_seg, new_off);
    });
}

} // namespace veltrix
