#include "compressfs/block_cache.hpp"

#include <cstdio>
#include <cstring>
#include <string>
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

static std::vector<uint8_t> make_block(size_t size, uint8_t fill) {
    return std::vector<uint8_t>(size, fill);
}

// No-op flush callback for tests that don't need flushing
static Error noop_flush(const std::string&, uint32_t,
                        std::span<const uint8_t>) {
    return Error::Ok;
}

// Tests

static void test_put_get_roundtrip() {
    BlockCache cache(256, 256 * 4, noop_flush);
    auto data = make_block(256, 0xAA);

    Error err = cache.put("file1", 0, data);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(cache.entry_count(), 1u);

    const uint8_t* result = cache.get("file1", 0);
    ASSERT_TRUE(result != nullptr);
    ASSERT_EQ(result[0], static_cast<uint8_t>(0xAA));
    ASSERT_EQ(result[255], static_cast<uint8_t>(0xAA));
}

static void test_cache_miss() {
    BlockCache cache(256, 256 * 4, noop_flush);

    const uint8_t* result = cache.get("nonexistent", 0);
    ASSERT_TRUE(result == nullptr);

    // Put a block, then miss on different index
    auto data = make_block(256, 0xBB);
    (void)cache.put("file1", 0, data);

    result = cache.get("file1", 1);
    ASSERT_TRUE(result == nullptr);

    result = cache.get("file2", 0);
    ASSERT_TRUE(result == nullptr);
}

static void test_lru_eviction() {
    // Cache holds 4 blocks
    BlockCache cache(256, 256 * 4, noop_flush);

    // Fill cache
    for (uint32_t i = 0; i < 4; ++i) {
        auto data = make_block(256, static_cast<uint8_t>(i));
        (void)cache.put("file", i, data);
    }
    ASSERT_EQ(cache.entry_count(), 4u);

    // Add one more - block 0 (LRU) should be evicted
    auto data = make_block(256, 0xFF);
    (void)cache.put("file", 4, data);
    ASSERT_EQ(cache.entry_count(), 4u);

    // Block 0 should be gone
    ASSERT_TRUE(cache.get("file", 0) == nullptr);

    // Block 4 should be present
    const uint8_t* result = cache.get("file", 4);
    ASSERT_TRUE(result != nullptr);
    ASSERT_EQ(result[0], static_cast<uint8_t>(0xFF));
}

static void test_lru_access_refreshes() {
    // Cache holds 3 blocks
    BlockCache cache(256, 256 * 3, noop_flush);

    (void)cache.put("f", 0, make_block(256, 0x00));
    (void)cache.put("f", 1, make_block(256, 0x11));
    (void)cache.put("f", 2, make_block(256, 0x22));

    // Access block 0 - moves it to head
    (void)cache.get("f", 0);

    // Insert block 3 - should evict block 1 (now LRU), not block 0
    (void)cache.put("f", 3, make_block(256, 0x33));

    ASSERT_TRUE(cache.get("f", 0) != nullptr); // refreshed, still present
    ASSERT_TRUE(cache.get("f", 1) == nullptr); // evicted
    ASSERT_TRUE(cache.get("f", 2) != nullptr); // still present
    ASSERT_TRUE(cache.get("f", 3) != nullptr); // just inserted
}

static void test_dirty_flush_on_eviction() {
    // Track flushed blocks
    struct FlushRecord {
        std::string path;
        uint32_t idx;
        uint8_t first_byte;
    };
    std::vector<FlushRecord> flushed;

    auto flush_fn = [&](const std::string& path, uint32_t idx,
                        std::span<const uint8_t> data) -> Error {
        flushed.push_back({path, idx, data[0]});
        return Error::Ok;
    };

    BlockCache cache(256, 256 * 2, flush_fn);

    // Insert 2 dirty blocks
    (void)cache.put("f", 0, make_block(256, 0xAA), true);
    (void)cache.put("f", 1, make_block(256, 0xBB), true);

    // Insert a 3rd - should evict block 0 (dirty) -> flush callback called
    (void)cache.put("f", 2, make_block(256, 0xCC));

    ASSERT_EQ(flushed.size(), 1u);
    ASSERT_EQ(flushed[0].path, "f");
    ASSERT_EQ(flushed[0].idx, 0u);
    ASSERT_EQ(flushed[0].first_byte, static_cast<uint8_t>(0xAA));
}

static void test_flush_file() {
    std::vector<std::pair<std::string, uint32_t>> flushed;

    auto flush_fn = [&](const std::string& path, uint32_t idx,
                        std::span<const uint8_t>) -> Error {
        flushed.push_back({path, idx});
        return Error::Ok;
    };

    BlockCache cache(256, 256 * 8, flush_fn);

    (void)cache.put("file_a", 0, make_block(256, 0x11), true);
    (void)cache.put("file_a", 1, make_block(256, 0x22), true);
    (void)cache.put("file_b", 0, make_block(256, 0x33), true);

    // Flush only file_a
    Error err = cache.flush_file("file_a");
    ASSERT_EQ(err, Error::Ok);

    // Should have flushed 2 entries (both from file_a)
    ASSERT_EQ(flushed.size(), 2u);
    for (auto& [path, idx] : flushed)
        ASSERT_EQ(path, "file_a");

    // file_b should still be dirty
    flushed.clear();
    err = cache.flush_all();
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(flushed.size(), 1u);
    ASSERT_EQ(flushed[0].first, "file_b");
}

static void test_flush_all() {
    int flush_count = 0;
    auto flush_fn = [&](const std::string&, uint32_t,
                        std::span<const uint8_t>) -> Error {
        ++flush_count;
        return Error::Ok;
    };

    BlockCache cache(256, 256 * 4, flush_fn);

    (void)cache.put("a", 0, make_block(256, 0), true);
    (void)cache.put("b", 0, make_block(256, 0), true);
    (void)cache.put("c", 0, make_block(256, 0), false); // not dirty

    Error err = cache.flush_all();
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(flush_count, 2); // only 2 dirty entries
}

static void test_invalidate_file() {
    BlockCache cache(256, 256 * 8, noop_flush);

    (void)cache.put("file_a", 0, make_block(256, 0x11));
    (void)cache.put("file_a", 1, make_block(256, 0x22));
    (void)cache.put("file_b", 0, make_block(256, 0x33));

    ASSERT_EQ(cache.entry_count(), 3u);

    cache.invalidate_file("file_a");

    ASSERT_EQ(cache.entry_count(), 1u);
    ASSERT_TRUE(cache.get("file_a", 0) == nullptr);
    ASSERT_TRUE(cache.get("file_a", 1) == nullptr);
    ASSERT_TRUE(cache.get("file_b", 0) != nullptr);
}

static void test_update_existing_entry() {
    BlockCache cache(256, 256 * 4, noop_flush);

    (void)cache.put("f", 0, make_block(256, 0xAA));
    (void)cache.put("f", 0, make_block(256, 0xBB)); // update

    ASSERT_EQ(cache.entry_count(), 1u); // still 1 entry, not 2

    const uint8_t* result = cache.get("f", 0);
    ASSERT_TRUE(result != nullptr);
    ASSERT_EQ(result[0], static_cast<uint8_t>(0xBB)); // updated value
}

static void test_memory_tracking() {
    BlockCache cache(1024, 1024 * 4, noop_flush);

    ASSERT_EQ(cache.memory_used(), 0u);

    (void)cache.put("f", 0, make_block(1024, 0));
    ASSERT_EQ(cache.memory_used(), 1024u);

    (void)cache.put("f", 1, make_block(1024, 0));
    ASSERT_EQ(cache.memory_used(), 2048u);

    cache.invalidate_file("f");
    ASSERT_EQ(cache.memory_used(), 0u);
}

static void test_mark_dirty() {
    int flush_count = 0;
    auto flush_fn = [&](const std::string&, uint32_t,
                        std::span<const uint8_t>) -> Error {
        ++flush_count;
        return Error::Ok;
    };

    BlockCache cache(256, 256 * 4, flush_fn);

    // Insert clean entry
    (void)cache.put("f", 0, make_block(256, 0xAA), false);

    // Flush should do nothing (not dirty)
    (void)cache.flush_all();
    ASSERT_EQ(flush_count, 0);

    // Mark dirty
    cache.mark_dirty("f", 0);

    // Now flush should call callback
    (void)cache.flush_all();
    ASSERT_EQ(flush_count, 1);
}

static void test_multiple_files_interleaved() {
    BlockCache cache(256, 256 * 6, noop_flush);

    (void)cache.put("alpha", 0, make_block(256, 0x01));
    (void)cache.put("beta", 0, make_block(256, 0x02));
    (void)cache.put("alpha", 1, make_block(256, 0x03));
    (void)cache.put("beta", 1, make_block(256, 0x04));
    (void)cache.put("gamma", 0, make_block(256, 0x05));

    ASSERT_EQ(cache.entry_count(), 5u);

    const uint8_t* r1 = cache.get("alpha", 0);
    ASSERT_TRUE(r1 != nullptr);
    ASSERT_EQ(r1[0], static_cast<uint8_t>(0x01));

    const uint8_t* r2 = cache.get("beta", 1);
    ASSERT_TRUE(r2 != nullptr);
    ASSERT_EQ(r2[0], static_cast<uint8_t>(0x04));

    cache.invalidate_file("beta");
    ASSERT_EQ(cache.entry_count(), 3u);
    ASSERT_TRUE(cache.get("beta", 0) == nullptr);
    ASSERT_TRUE(cache.get("beta", 1) == nullptr);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_block_cache:\n");

    RUN(test_put_get_roundtrip);
    RUN(test_cache_miss);
    RUN(test_lru_eviction);
    RUN(test_lru_access_refreshes);
    RUN(test_dirty_flush_on_eviction);
    RUN(test_flush_file);
    RUN(test_flush_all);
    RUN(test_invalidate_file);
    RUN(test_update_existing_entry);
    RUN(test_memory_tracking);
    RUN(test_mark_dirty);
    RUN(test_multiple_files_interleaved);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
