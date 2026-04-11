#include "compressfs/codec.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
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

// --- Test data generators ---

static std::vector<uint8_t> make_compressible(size_t size) {
    // All zeros - maximally compressible
    return std::vector<uint8_t>(size, 0);
}

static std::vector<uint8_t> make_incompressible(size_t size) {
    // Pseudo-random bytes - resists compression
    std::vector<uint8_t> data(size);
    uint32_t state = 0xDEADBEEF;
    for (size_t i = 0; i < size; ++i) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        data[i] = static_cast<uint8_t>(state & 0xFF);
    }
    return data;
}

static std::vector<uint8_t> make_pattern(size_t size) {
    // Repeating byte pattern - moderately compressible
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(i & 0xFF);
    return data;
}

// --- Generic codec roundtrip test ---

static void roundtrip(const CodecBase* codec, std::span<const uint8_t> input,
                      int level = 0) {
    size_t max_out = codec->max_compressed_size(input.size());
    ASSERT_TRUE(max_out > 0);

    std::vector<uint8_t> compressed(max_out);
    size_t comp_size = codec->compress(input, compressed, level);
    ASSERT_TRUE(comp_size > 0);
    ASSERT_TRUE(comp_size <= max_out);

    std::vector<uint8_t> decompressed(input.size());
    size_t decomp_size = codec->decompress(
        std::span<const uint8_t>(compressed.data(), comp_size),
        decompressed, input.size());
    ASSERT_EQ(decomp_size, input.size());
    ASSERT_TRUE(std::memcmp(decompressed.data(), input.data(), input.size()) == 0);
}

// === Noop codec tests ===

static void test_noop_roundtrip_small() {
    auto* codec = get_codec(Codec::None);
    ASSERT_TRUE(codec != nullptr);
    auto data = make_pattern(100);
    roundtrip(codec, data);
}

static void test_noop_roundtrip_block() {
    auto* codec = get_codec(Codec::None);
    auto data = make_pattern(65536);
    roundtrip(codec, data);
}

static void test_noop_compress_is_identity() {
    auto* codec = get_codec(Codec::None);
    auto data = make_pattern(256);
    std::vector<uint8_t> out(256);
    size_t n = codec->compress(data, out, 0);
    ASSERT_EQ(n, 256u);
    ASSERT_TRUE(std::memcmp(out.data(), data.data(), 256) == 0);
}

static void test_noop_max_compressed_size() {
    auto* codec = get_codec(Codec::None);
    ASSERT_EQ(codec->max_compressed_size(0), 0u);
    ASSERT_EQ(codec->max_compressed_size(100), 100u);
    ASSERT_EQ(codec->max_compressed_size(65536), 65536u);
}

static void test_noop_identity() {
    auto* codec = get_codec(Codec::None);
    ASSERT_EQ(codec->codec_id(), Codec::None);
    ASSERT_TRUE(std::strcmp(codec->name(), "none") == 0);
}

// === LZ4 codec tests ===

static void test_lz4_roundtrip_small() {
    auto* codec = get_codec(Codec::LZ4);
    ASSERT_TRUE(codec != nullptr);
    auto data = make_pattern(100);
    roundtrip(codec, data);
}

static void test_lz4_roundtrip_block() {
    auto* codec = get_codec(Codec::LZ4);
    auto data = make_pattern(65536);
    roundtrip(codec, data);
}

static void test_lz4_roundtrip_large() {
    auto* codec = get_codec(Codec::LZ4);
    auto data = make_pattern(256 * 1024);
    roundtrip(codec, data);
}

static void test_lz4_roundtrip_1byte() {
    auto* codec = get_codec(Codec::LZ4);
    std::vector<uint8_t> data = {0x42};
    roundtrip(codec, data);
}

static void test_lz4_compressible_shrinks() {
    auto* codec = get_codec(Codec::LZ4);
    auto data = make_compressible(65536);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> out(max_out);
    size_t n = codec->compress(data, out, 0);
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(n < data.size()); // zeros must compress
}

static void test_lz4_incompressible_survives() {
    auto* codec = get_codec(Codec::LZ4);
    auto data = make_incompressible(65536);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> out(max_out);
    size_t n = codec->compress(data, out, 0);
    ASSERT_TRUE(n > 0); // must succeed even if output > input
    ASSERT_TRUE(n <= max_out);

    // Roundtrip
    std::vector<uint8_t> dec(data.size());
    size_t dn = codec->decompress(
        std::span<const uint8_t>(out.data(), n), dec, data.size());
    ASSERT_EQ(dn, data.size());
    ASSERT_TRUE(dec == data);
}

static void test_lz4_max_compressed_size() {
    auto* codec = get_codec(Codec::LZ4);
    size_t bound = codec->max_compressed_size(65536);
    ASSERT_TRUE(bound > 65536); // LZ4 bound is always > input for small inputs
}

static void test_lz4_decompress_corrupt() {
    auto* codec = get_codec(Codec::LZ4);
    // Garbage data - should not crash, should return 0
    auto garbage = make_incompressible(100);
    std::vector<uint8_t> out(65536);
    size_t n = codec->decompress(garbage, out, 65536);
    ASSERT_EQ(n, 0u);
}

static void test_lz4_decompress_truncated() {
    auto* codec = get_codec(Codec::LZ4);
    auto data = make_pattern(1024);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> compressed(max_out);
    size_t comp_size = codec->compress(data, compressed, 0);
    ASSERT_TRUE(comp_size > 4);

    // Give only half the compressed data
    std::vector<uint8_t> out(1024);
    size_t n = codec->decompress(
        std::span<const uint8_t>(compressed.data(), comp_size / 2),
        out, 1024);
    ASSERT_EQ(n, 0u);
}

static void test_lz4_identity() {
    auto* codec = get_codec(Codec::LZ4);
    ASSERT_EQ(codec->codec_id(), Codec::LZ4);
    ASSERT_TRUE(std::strcmp(codec->name(), "lz4") == 0);
}

// === Zstd codec tests ===

static void test_zstd_roundtrip_small() {
    auto* codec = get_codec(Codec::Zstd);
    ASSERT_TRUE(codec != nullptr);
    auto data = make_pattern(100);
    roundtrip(codec, data);
}

static void test_zstd_roundtrip_block() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(65536);
    roundtrip(codec, data);
}

static void test_zstd_roundtrip_large() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(256 * 1024);
    roundtrip(codec, data);
}

static void test_zstd_roundtrip_1byte() {
    auto* codec = get_codec(Codec::Zstd);
    std::vector<uint8_t> data = {0x42};
    roundtrip(codec, data);
}

static void test_zstd_compressible_shrinks() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_compressible(65536);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> out(max_out);
    size_t n = codec->compress(data, out, 3);
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(n < data.size());
}

static void test_zstd_incompressible_survives() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_incompressible(65536);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> out(max_out);
    size_t n = codec->compress(data, out, 3);
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(n <= max_out);

    std::vector<uint8_t> dec(data.size());
    size_t dn = codec->decompress(
        std::span<const uint8_t>(out.data(), n), dec, data.size());
    ASSERT_EQ(dn, data.size());
    ASSERT_TRUE(dec == data);
}

static void test_zstd_level_1_roundtrip() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(65536);
    roundtrip(codec, data, 1);
}

static void test_zstd_level_19_roundtrip() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(65536);
    roundtrip(codec, data, 19);
}

static void test_zstd_higher_level_better_ratio() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(65536);
    size_t max_out = codec->max_compressed_size(data.size());

    std::vector<uint8_t> out1(max_out), out19(max_out);
    size_t n1 = codec->compress(data, out1, 1);
    size_t n19 = codec->compress(data, out19, 19);
    ASSERT_TRUE(n1 > 0);
    ASSERT_TRUE(n19 > 0);
    // Higher level should compress at least as well (usually better)
    ASSERT_TRUE(n19 <= n1);
}

static void test_zstd_negative_level_roundtrip() {
    // Zstd supports negative levels for fast compression
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(65536);
    roundtrip(codec, data, -5);
}

static void test_zstd_decompress_corrupt() {
    auto* codec = get_codec(Codec::Zstd);
    auto garbage = make_incompressible(100);
    std::vector<uint8_t> out(65536);
    size_t n = codec->decompress(garbage, out, 65536);
    ASSERT_EQ(n, 0u);
}

static void test_zstd_decompress_truncated() {
    auto* codec = get_codec(Codec::Zstd);
    auto data = make_pattern(1024);
    size_t max_out = codec->max_compressed_size(data.size());
    std::vector<uint8_t> compressed(max_out);
    size_t comp_size = codec->compress(data, compressed, 3);
    ASSERT_TRUE(comp_size > 4);

    std::vector<uint8_t> out(1024);
    size_t n = codec->decompress(
        std::span<const uint8_t>(compressed.data(), comp_size / 2),
        out, 1024);
    ASSERT_EQ(n, 0u);
}

static void test_zstd_max_compressed_size() {
    auto* codec = get_codec(Codec::Zstd);
    size_t bound = codec->max_compressed_size(65536);
    ASSERT_TRUE(bound > 0);
    ASSERT_TRUE(bound >= 65536); // bound is always >= input
}

static void test_zstd_identity() {
    auto* codec = get_codec(Codec::Zstd);
    ASSERT_EQ(codec->codec_id(), Codec::Zstd);
    ASSERT_TRUE(std::strcmp(codec->name(), "zstd") == 0);
}

// === Factory tests ===

static void test_factory_all_codecs() {
    ASSERT_TRUE(get_codec(Codec::None) != nullptr);
    ASSERT_TRUE(get_codec(Codec::LZ4) != nullptr);
    ASSERT_TRUE(get_codec(Codec::Zstd) != nullptr);
}

static void test_factory_singleton() {
    // Same pointer on repeated calls - proves singleton behavior
    auto* a = get_codec(Codec::LZ4);
    auto* b = get_codec(Codec::LZ4);
    ASSERT_TRUE(a == b);

    auto* c = get_codec(Codec::Zstd);
    auto* d = get_codec(Codec::Zstd);
    ASSERT_TRUE(c == d);
}

static void test_factory_unknown_returns_null() {
    auto* codec = get_codec(static_cast<Codec>(255));
    ASSERT_TRUE(codec == nullptr);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_codec:\n");

    // Noop
    RUN(test_noop_roundtrip_small);
    RUN(test_noop_roundtrip_block);
    RUN(test_noop_compress_is_identity);
    RUN(test_noop_max_compressed_size);
    RUN(test_noop_identity);

    // LZ4
    RUN(test_lz4_roundtrip_small);
    RUN(test_lz4_roundtrip_block);
    RUN(test_lz4_roundtrip_large);
    RUN(test_lz4_roundtrip_1byte);
    RUN(test_lz4_compressible_shrinks);
    RUN(test_lz4_incompressible_survives);
    RUN(test_lz4_max_compressed_size);
    RUN(test_lz4_decompress_corrupt);
    RUN(test_lz4_decompress_truncated);
    RUN(test_lz4_identity);

    // Zstd
    RUN(test_zstd_roundtrip_small);
    RUN(test_zstd_roundtrip_block);
    RUN(test_zstd_roundtrip_large);
    RUN(test_zstd_roundtrip_1byte);
    RUN(test_zstd_compressible_shrinks);
    RUN(test_zstd_incompressible_survives);
    RUN(test_zstd_level_1_roundtrip);
    RUN(test_zstd_level_19_roundtrip);
    RUN(test_zstd_higher_level_better_ratio);
    RUN(test_zstd_negative_level_roundtrip);
    RUN(test_zstd_decompress_corrupt);
    RUN(test_zstd_decompress_truncated);
    RUN(test_zstd_max_compressed_size);
    RUN(test_zstd_identity);

    // Factory
    RUN(test_factory_all_codecs);
    RUN(test_factory_singleton);
    RUN(test_factory_unknown_returns_null);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
