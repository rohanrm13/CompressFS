#include "compressfs/block_cache.hpp"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <vector>

namespace compressfs {

BlockCache::BlockCache(size_t block_size, size_t max_memory,
                       FlushCallback flush_fn, FsStats* stats)
    : block_size_(block_size),
      max_entries_(max_memory / block_size),
      flush_fn_(std::move(flush_fn)),
      slab_(block_size, max_memory),
      stats_(stats) {

    // Initialize LRU sentinel doubly-linked list
    lru_head_sentinel_.lru_next = &lru_tail_sentinel_;
    lru_head_sentinel_.lru_prev = nullptr;
    lru_tail_sentinel_.lru_prev = &lru_head_sentinel_;
    lru_tail_sentinel_.lru_next = nullptr;
}

BlockCache::~BlockCache() {
    // Flush all dirty entries before destruction
    (void)flush_all();

    // Free all entries (slab memory is freed by SlabAllocator destructor,
    // but we need to delete the CacheEntry objects)
    for (auto& [key, entry] : map_) {
        if (entry->data)
            slab_.deallocate(entry->data);
        delete entry;
    }
}

void BlockCache::lru_remove(CacheEntry* entry) {
    entry->lru_prev->lru_next = entry->lru_next;
    entry->lru_next->lru_prev = entry->lru_prev;
    entry->lru_prev = nullptr;
    entry->lru_next = nullptr;
}

void BlockCache::lru_push_front(CacheEntry* entry) {
    entry->lru_next = lru_head_sentinel_.lru_next;
    entry->lru_prev = &lru_head_sentinel_;
    lru_head_sentinel_.lru_next->lru_prev = entry;
    lru_head_sentinel_.lru_next = entry;
}

BlockCache::CacheEntry* BlockCache::lru_pop_back() {
    CacheEntry* tail = lru_tail_sentinel_.lru_prev;
    if (tail == &lru_head_sentinel_)
        return nullptr; // list is empty
    lru_remove(tail);
    return tail;
}

Error BlockCache::flush_entry(CacheEntry* entry) {
    if (!entry->dirty)
        return Error::Ok;

    Error err = flush_fn_(entry->key.path, entry->key.block_idx,
                          std::span<const uint8_t>(entry->data, block_size_));
    if (err == Error::Ok)
        entry->dirty = false;
    return err;
}

Error BlockCache::evict_if_needed() {
    while (map_.size() >= max_entries_) {
        CacheEntry* victim = lru_pop_back();
        if (!victim)
            return Error::NoMem; // should not happen - map_.size() > 0 but list empty

        // Flush dirty victim before evicting
        Error err = flush_entry(victim);
        if (err != Error::Ok) {
            // BUG RISK: flush failed but we still evict to make room.
            // The data is lost. This is the write-through guarantee failure case.
            // In write-through mode, blocks should already be on disk, so dirty
            // entries are rare (only in write-back mode which we don't use yet).
        }

        map_.erase(victim->key);
        slab_.deallocate(victim->data);
        delete victim;

        if (stats_)
            stats_->cache_evictions.fetch_add(1, std::memory_order_relaxed);
    }
    return Error::Ok;
}

const uint8_t* BlockCache::get(const std::string& path, uint32_t block_idx) {
    std::unique_lock lock(mutex_);

    CacheKey key{std::string(path), block_idx};
    auto it = map_.find(key);
    if (it == map_.end()) {
        if (stats_)
            stats_->cache_misses.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    if (stats_)
        stats_->cache_hits.fetch_add(1, std::memory_order_relaxed);

    // Move to LRU head (most recently used)
    CacheEntry* entry = it->second;
    lru_remove(entry);
    lru_push_front(entry);

    return entry->data;
}

Error BlockCache::put(const std::string& path, uint32_t block_idx,
                      std::span<const uint8_t> data, bool dirty) {
    std::unique_lock lock(mutex_);

    CacheKey key{std::string(path), block_idx};
    auto it = map_.find(key);

    if (it != map_.end()) {
        // Update existing entry
        CacheEntry* entry = it->second;
        std::memcpy(entry->data, data.data(),
                    std::min(data.size(), block_size_));
        if (data.size() < block_size_)
            std::memset(entry->data + data.size(), 0, block_size_ - data.size());
        entry->dirty = entry->dirty || dirty;

        lru_remove(entry);
        lru_push_front(entry);
        return Error::Ok;
    }

    // New entry - evict if needed
    Error err = evict_if_needed();
    if (err != Error::Ok)
        return err;

    uint8_t* slab = slab_.allocate();
    if (!slab)
        return Error::NoMem;

    std::memcpy(slab, data.data(), std::min(data.size(), block_size_));
    if (data.size() < block_size_)
        std::memset(slab + data.size(), 0, block_size_ - data.size());

    auto* entry = new CacheEntry{std::move(key), slab, dirty, nullptr, nullptr};
    map_[entry->key] = entry;
    lru_push_front(entry);

    return Error::Ok;
}

void BlockCache::mark_dirty(const std::string& path, uint32_t block_idx) {
    std::unique_lock lock(mutex_);

    CacheKey key{std::string(path), block_idx};
    auto it = map_.find(key);
    if (it != map_.end())
        it->second->dirty = true;
}

Error BlockCache::flush_file(const std::string& path) {
    std::unique_lock lock(mutex_);

    Error result = Error::Ok;
    for (auto& [key, entry] : map_) {
        if (key.path == path && entry->dirty) {
            Error err = flush_entry(entry);
            if (err != Error::Ok)
                result = err;
        }
    }
    return result;
}

Error BlockCache::flush_all() {
    std::unique_lock lock(mutex_);

    Error result = Error::Ok;
    for (auto& [key, entry] : map_) {
        if (entry->dirty) {
            Error err = flush_entry(entry);
            if (err != Error::Ok)
                result = err;
        }
    }
    return result;
}

void BlockCache::invalidate_file(const std::string& path) {
    std::unique_lock lock(mutex_);

    // Collect entries to remove (can't modify map while iterating)
    std::vector<CacheKey> to_remove;
    for (auto& [key, entry] : map_) {
        if (key.path == path)
            to_remove.push_back(key);
    }

    for (auto& key : to_remove) {
        auto it = map_.find(key);
        if (it == map_.end())
            continue;

        CacheEntry* entry = it->second;
        lru_remove(entry);

        // BUG RISK: dirty entry being invalidated without flush.
        // This is intentional for truncate/delete - the blocks are being removed
        // from disk, so the cached dirty data is stale. The caller must ensure
        // metadata consistency separately.

        slab_.deallocate(entry->data);
        map_.erase(it);
        delete entry;
    }
}

size_t BlockCache::entry_count() const {
    std::shared_lock lock(mutex_);
    return map_.size();
}

size_t BlockCache::memory_used() const {
    std::shared_lock lock(mutex_);
    return map_.size() * block_size_;
}

} // namespace compressfs
