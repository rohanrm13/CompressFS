#include "compressfs/slab_allocator.hpp"

#include <cstdio>
#include <cstring>
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

static void test_alloc_dealloc_single() {
    SlabAllocator alloc(1024, 1024 * 10);
    ASSERT_EQ(alloc.capacity(), 10u);
    ASSERT_EQ(alloc.available(), 10u);

    uint8_t* p = alloc.allocate();
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(alloc.available(), 9u);
    ASSERT_TRUE(alloc.owns(p));

    // Write to the slab - should not crash
    std::memset(p, 0xAA, 1024);

    alloc.deallocate(p);
    ASSERT_EQ(alloc.available(), 10u);
}

static void test_exhaust_pool() {
    SlabAllocator alloc(256, 256 * 4);
    ASSERT_EQ(alloc.capacity(), 4u);

    std::vector<uint8_t*> ptrs;
    for (size_t i = 0; i < 4; ++i) {
        uint8_t* p = alloc.allocate();
        ASSERT_TRUE(p != nullptr);
        ptrs.push_back(p);
    }
    ASSERT_EQ(alloc.available(), 0u);

    // Pool exhausted
    uint8_t* p = alloc.allocate();
    ASSERT_TRUE(p == nullptr);

    // Dealloc one and re-alloc
    alloc.deallocate(ptrs.back());
    ptrs.pop_back();
    ASSERT_EQ(alloc.available(), 1u);

    p = alloc.allocate();
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(alloc.available(), 0u);

    // Cleanup
    for (auto* ptr : ptrs) alloc.deallocate(ptr);
    alloc.deallocate(p);
}

static void test_alloc_all_dealloc_all_realloc() {
    constexpr size_t count = 100;
    SlabAllocator alloc(512, 512 * count);
    ASSERT_EQ(alloc.capacity(), count);

    std::vector<uint8_t*> ptrs;
    for (size_t i = 0; i < count; ++i) {
        uint8_t* p = alloc.allocate();
        ASSERT_TRUE(p != nullptr);
        // Write unique pattern to verify no overlap
        std::memset(p, static_cast<int>(i & 0xFF), 512);
        ptrs.push_back(p);
    }
    ASSERT_EQ(alloc.available(), 0u);

    for (auto* p : ptrs)
        alloc.deallocate(p);
    ASSERT_EQ(alloc.available(), count);
    ptrs.clear();

    // Re-allocate everything
    for (size_t i = 0; i < count; ++i) {
        uint8_t* p = alloc.allocate();
        ASSERT_TRUE(p != nullptr);
        ptrs.push_back(p);
    }
    ASSERT_EQ(alloc.available(), 0u);

    for (auto* p : ptrs)
        alloc.deallocate(p);
}

static void test_owns_validation() {
    SlabAllocator alloc(1024, 1024 * 4);

    uint8_t* p = alloc.allocate();
    ASSERT_TRUE(alloc.owns(p));

    // Pointer outside pool
    uint8_t stack_buf[1024];
    ASSERT_TRUE(!alloc.owns(stack_buf));

    // Null pointer
    ASSERT_TRUE(!alloc.owns(nullptr));

    // Misaligned pointer within pool (offset by 1 byte)
    ASSERT_TRUE(!alloc.owns(p + 1));

    alloc.deallocate(p);
}

static void test_capacity_tracking() {
    SlabAllocator alloc(4096, 4096 * 8);
    ASSERT_EQ(alloc.block_size(), 4096u);
    ASSERT_EQ(alloc.capacity(), 8u);
    ASSERT_EQ(alloc.available(), 8u);

    uint8_t* p1 = alloc.allocate();
    uint8_t* p2 = alloc.allocate();
    ASSERT_EQ(alloc.available(), 6u);

    alloc.deallocate(p1);
    ASSERT_EQ(alloc.available(), 7u);

    alloc.deallocate(p2);
    ASSERT_EQ(alloc.available(), 8u);
}

static void test_pool_size_rounded_down() {
    // Pool size not a multiple of block_size - should round down
    SlabAllocator alloc(1024, 1024 * 3 + 500);
    ASSERT_EQ(alloc.capacity(), 3u); // 3500 / 1024 = 3
    ASSERT_EQ(alloc.available(), 3u);
}

static void test_write_entire_slab() {
    SlabAllocator alloc(65536, 65536 * 2);
    uint8_t* p = alloc.allocate();
    ASSERT_TRUE(p != nullptr);

    // Write entire 64 KiB slab - validates mmap region is large enough
    for (size_t i = 0; i < 65536; ++i)
        p[i] = static_cast<uint8_t>(i & 0xFF);

    // Verify
    for (size_t i = 0; i < 65536; ++i)
        ASSERT_EQ(p[i], static_cast<uint8_t>(i & 0xFF));

    alloc.deallocate(p);
}

static void test_no_overlap() {
    SlabAllocator alloc(256, 256 * 4);
    uint8_t* p1 = alloc.allocate();
    uint8_t* p2 = alloc.allocate();
    uint8_t* p3 = alloc.allocate();

    // Write different patterns to each
    std::memset(p1, 0x11, 256);
    std::memset(p2, 0x22, 256);
    std::memset(p3, 0x33, 256);

    // Verify no overlap
    for (size_t i = 0; i < 256; ++i) {
        ASSERT_EQ(p1[i], static_cast<uint8_t>(0x11));
        ASSERT_EQ(p2[i], static_cast<uint8_t>(0x22));
        ASSERT_EQ(p3[i], static_cast<uint8_t>(0x33));
    }

    alloc.deallocate(p1);
    alloc.deallocate(p2);
    alloc.deallocate(p3);
}

static void test_move_semantics() {
    SlabAllocator alloc(1024, 1024 * 4);
    uint8_t* p = alloc.allocate();
    ASSERT_TRUE(p != nullptr);
    std::memset(p, 0xAA, 1024);

    // Move construct
    SlabAllocator alloc2(std::move(alloc));
    ASSERT_EQ(alloc2.capacity(), 4u);
    ASSERT_EQ(alloc2.available(), 3u);
    ASSERT_TRUE(alloc2.owns(p));

    // Original should be empty
    ASSERT_EQ(alloc.capacity(), 0u);

    alloc2.deallocate(p);
    ASSERT_EQ(alloc2.available(), 4u);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_slab_allocator:\n");

    RUN(test_alloc_dealloc_single);
    RUN(test_exhaust_pool);
    RUN(test_alloc_all_dealloc_all_realloc);
    RUN(test_owns_validation);
    RUN(test_capacity_tracking);
    RUN(test_pool_size_rounded_down);
    RUN(test_write_entire_slab);
    RUN(test_no_overlap);
    RUN(test_move_semantics);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
