#include "compressfs/stats.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace compressfs;

static int g_pass = 0;
static int g_fail = 0;

#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__,   \
                         #expr);                                              \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define ASSERT_EQ(a, b)                                                      \
    do {                                                                      \
        if ((a) != (b)) {                                                     \
            std::fprintf(stderr, "  FAIL %s:%d: %s != %s\n", __FILE__,       \
                         __LINE__, #a, #b);                                   \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define RUN(name)                                                             \
    do {                                                                      \
        std::fprintf(stderr, "  %-55s", #name);                              \
        name();                                                               \
        if (g_fail == prev_fail) {                                            \
            std::fprintf(stderr, "PASS\n");                                   \
            ++g_pass;                                                         \
        }                                                                     \
        prev_fail = g_fail;                                                   \
    } while (0)

static void test_initial_values() {
    FsStats stats;
    ASSERT_EQ(stats.blocks_read.load(), 0u);
    ASSERT_EQ(stats.blocks_written.load(), 0u);
    ASSERT_EQ(stats.bytes_read_logical.load(), 0u);
    ASSERT_EQ(stats.bytes_written_logical.load(), 0u);
    ASSERT_EQ(stats.compressions.load(), 0u);
    ASSERT_EQ(stats.decompressions.load(), 0u);
    ASSERT_EQ(stats.cache_hits.load(), 0u);
    ASSERT_EQ(stats.cache_misses.load(), 0u);
    ASSERT_EQ(stats.cache_evictions.load(), 0u);
}

static void test_increment_and_read() {
    FsStats stats;
    stats.blocks_read.fetch_add(10, std::memory_order_relaxed);
    stats.blocks_written.fetch_add(5, std::memory_order_relaxed);
    stats.cache_hits.fetch_add(100, std::memory_order_relaxed);
    stats.cache_misses.fetch_add(20, std::memory_order_relaxed);

    ASSERT_EQ(stats.blocks_read.load(), 10u);
    ASSERT_EQ(stats.blocks_written.load(), 5u);
    ASSERT_EQ(stats.cache_hits.load(), 100u);
    ASSERT_EQ(stats.cache_misses.load(), 20u);
}

static void test_format_stats_output() {
    FsStats stats;
    stats.blocks_read.store(100);
    stats.blocks_written.store(50);
    stats.bytes_read_logical.store(1000000);
    stats.bytes_written_logical.store(500000);
    stats.compressions.store(50);
    stats.decompressions.store(100);
    stats.cache_hits.store(80);
    stats.cache_misses.store(20);
    stats.cache_evictions.store(5);

    std::string text = format_stats(stats);

    // Verify header
    ASSERT_TRUE(text.find("=== compressfs statistics ===") != std::string::npos);

    // Verify key counters are present
    ASSERT_TRUE(text.find("blocks_read:") != std::string::npos);
    ASSERT_TRUE(text.find("100") != std::string::npos);
    ASSERT_TRUE(text.find("cache_hit_rate:") != std::string::npos);
    ASSERT_TRUE(text.find("80.0%") != std::string::npos); // 80 / (80+20) = 80%
    ASSERT_TRUE(text.find("cache_evictions:") != std::string::npos);
}

static void test_format_stats_zero_lookups() {
    FsStats stats; // all zero
    std::string text = format_stats(stats);

    // Hit rate should be 0% when no lookups happened
    ASSERT_TRUE(text.find("cache_hit_rate:") != std::string::npos);
    ASSERT_TRUE(text.find("0.0%") != std::string::npos);
}

static void test_concurrent_increments() {
    FsStats stats;
    constexpr int threads = 4;
    constexpr int iters = 10000;

    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t) {
        pool.emplace_back([&stats]() {
            for (int i = 0; i < iters; ++i) {
                stats.blocks_read.fetch_add(1, std::memory_order_relaxed);
                stats.cache_hits.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : pool)
        t.join();

    ASSERT_EQ(stats.blocks_read.load(), static_cast<uint64_t>(threads * iters));
    ASSERT_EQ(stats.cache_hits.load(), static_cast<uint64_t>(threads * iters));
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_stats:\n");

    RUN(test_initial_values);
    RUN(test_increment_and_read);
    RUN(test_format_stats_output);
    RUN(test_format_stats_zero_lookups);
    RUN(test_concurrent_increments);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
