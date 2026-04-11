#pragma once

#include "compressfs/metadata.hpp"
#include "compressfs/slab_allocator.hpp"
#include "compressfs/stats.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>

namespace compressfs {
// LRU block cache with slab-allocated storage, dirty tracking, and reader-writer
// lock for thread safety.
//
// Why reader-writer lock: filesystem workloads are read-heavy. Multiple
// concurrent reads can access the cache simultaneously; only writes need
// exclusive access. A plain mutex would serialize all reads unnecessarily.
class BlockCache {
public:
    // Called when a dirty block must be flushed to disk (on eviction or explicit flush).
    // The callback should compress and write the block to the backing store.
    using FlushCallback = std::function<Error(const std::string& path,
                                              uint32_t block_idx,
                                              std::span<const uint8_t> data)>;

    // block_size: size of each decompressed block
    // max_memory: memory budget in bytes (determines slab pool size)
    // flush_fn: callback for flushing dirty blocks
    BlockCache(size_t block_size, size_t max_memory, FlushCallback flush_fn,
               FsStats* stats = nullptr);
    ~BlockCache();

    BlockCache(const BlockCache&) = delete;
    BlockCache& operator=(const BlockCache&) = delete;

    // Lookup a cached block. Returns pointer to decompressed data (block_size bytes),
    // or nullptr on cache miss. Moves entry to LRU head on hit.
    [[nodiscard]] const uint8_t* get(const std::string& path, uint32_t block_idx);

    // Insert or update a block in the cache. Evicts LRU entries if over budget.
    // Dirty entries are flushed via FlushCallback before eviction.
    Error put(const std::string& path, uint32_t block_idx,
              std::span<const uint8_t> data, bool dirty = false);

    // Mark an existing cached block as dirty.
    void mark_dirty(const std::string& path, uint32_t block_idx);

    // Flush all dirty blocks for a specific file.
    Error flush_file(const std::string& path);

    // Flush all dirty blocks across all files.
    Error flush_all();

    // Remove all cached entries for a file (e.g. on delete or truncate).
    void invalidate_file(const std::string& path);

    [[nodiscard]] size_t entry_count() const;
    [[nodiscard]] size_t memory_used() const;
    [[nodiscard]] size_t block_size() const { return block_size_; }

private:
    struct CacheKey {
        std::string path;
        uint32_t block_idx;

        bool operator==(const CacheKey& o) const {
            return block_idx == o.block_idx && path == o.path;
        }
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const {
            size_t h1 = std::hash<std::string>{}(k.path);
            size_t h2 = std::hash<uint32_t>{}(k.block_idx);
            // Combine hashes - boost::hash_combine pattern
            return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    struct CacheEntry {
        CacheKey key;
        uint8_t* data = nullptr;      // slab-allocated
        bool dirty = false;
        CacheEntry* lru_prev = nullptr;
        CacheEntry* lru_next = nullptr;
    };

    // LRU list operations (caller must hold exclusive lock)
    void lru_remove(CacheEntry* entry);
    void lru_push_front(CacheEntry* entry);
    CacheEntry* lru_pop_back();

    // Evict entries until we have room for one more block (caller holds exclusive lock)
    Error evict_if_needed();

    // Flush and remove a single entry (caller holds exclusive lock)
    Error flush_entry(CacheEntry* entry);

    size_t block_size_;
    size_t max_entries_;
    FlushCallback flush_fn_;

    SlabAllocator slab_;
    std::unordered_map<CacheKey, CacheEntry*, CacheKeyHash> map_;

    // LRU list with sentinel head/tail for O(1) insert/remove
    CacheEntry lru_head_sentinel_;
    CacheEntry lru_tail_sentinel_;

    FsStats* stats_;
    mutable std::shared_mutex mutex_;
};

} // namespace compressfs
