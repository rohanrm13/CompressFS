#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string>

namespace compressfs {

// Filesystem-wide statistics. Incremented from BlockManager and BlockCache
// on every relevant operation.
struct FsStats {
    std::atomic<uint64_t> blocks_read{0};
    std::atomic<uint64_t> blocks_written{0};
    std::atomic<uint64_t> bytes_read_logical{0};
    std::atomic<uint64_t> bytes_written_logical{0};
    std::atomic<uint64_t> compressions{0};
    std::atomic<uint64_t> decompressions{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> cache_evictions{0};
};

// Format stats as human-readable text for the virtual stats file.
inline std::string format_stats(const FsStats& s) {
    auto load = [](const std::atomic<uint64_t>& a) {
        return a.load(std::memory_order_relaxed);
    };

    uint64_t hits = load(s.cache_hits);
    uint64_t misses = load(s.cache_misses);
    uint64_t total_lookups = hits + misses;
    double hit_rate = (total_lookups > 0)
                          ? (100.0 * static_cast<double>(hits) /
                             static_cast<double>(total_lookups))
                          : 0.0;

    char buf[1024];
    int n = std::snprintf(buf, sizeof(buf),
        "=== compressfs statistics ===\n"
        "blocks_read:           %lu\n"
        "blocks_written:        %lu\n"
        "bytes_read_logical:    %lu\n"
        "bytes_written_logical: %lu\n"
        "compressions:          %lu\n"
        "decompressions:        %lu\n"
        "cache_hits:            %lu\n"
        "cache_misses:          %lu\n"
        "cache_hit_rate:        %.1f%%\n"
        "cache_evictions:       %lu\n",
        static_cast<unsigned long>(load(s.blocks_read)),
        static_cast<unsigned long>(load(s.blocks_written)),
        static_cast<unsigned long>(load(s.bytes_read_logical)),
        static_cast<unsigned long>(load(s.bytes_written_logical)),
        static_cast<unsigned long>(load(s.compressions)),
        static_cast<unsigned long>(load(s.decompressions)),
        static_cast<unsigned long>(hits),
        static_cast<unsigned long>(misses),
        hit_rate,
        static_cast<unsigned long>(load(s.cache_evictions)));

    return std::string(buf, static_cast<size_t>(n));
}

} // namespace compressfs
