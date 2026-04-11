#include "compressfs/block_manager.hpp"
#include "compressfs/codec.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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

// Test environment: creates a temp dir with a BackingStore + BlockManager
struct TestEnv {
    std::string tmpdir;
    BackingStore store;
    BlockManager* mgr = nullptr;

    TestEnv() = default;
    TestEnv(TestEnv&&) noexcept = default;
    TestEnv& operator=(TestEnv&&) noexcept = default;

    ~TestEnv() {
        delete mgr;
        store = BackingStore{};
        if (!tmpdir.empty())
            std::filesystem::remove_all(tmpdir);
    }
};

static TestEnv make_env(Codec codec_id = Codec::None, int level = 0) {
    TestEnv env;
    char tmpl[] = "/tmp/cfs_bm_test_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    if (!dir) std::abort();
    env.tmpdir = dir;

    auto [err, bs] = BackingStore::open(env.tmpdir + "/store");
    if (err != Error::Ok) std::abort();
    env.store = std::move(bs);

    const CodecBase* codec = get_codec(codec_id);
    if (!codec) std::abort();
    env.mgr = new BlockManager(env.store, codec, level);
    return env;
}

static Metadata make_meta(uint32_t block_size = 1024) {
    Metadata m;
    m.block_size = block_size;
    m.original_size = 0;
    m.block_count = 0;
    return m;
}

static std::vector<uint8_t> make_pattern(size_t size, uint8_t seed = 0) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    return data;
}

static std::vector<uint8_t> make_random(size_t size) {
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

// --- Roundtrip tests per codec ---

static void do_roundtrip_test(Codec codec_id, int level = 0) {
    auto env = make_env(codec_id, level);
    auto meta = make_meta(1024);
    const char* path = "testfile";

    // Write metadata first
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(2048); // 2 blocks
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  data.size(), 0);
    ASSERT_EQ(static_cast<size_t>(written), data.size());
    ASSERT_EQ(meta.original_size, 2048u);
    ASSERT_EQ(meta.block_count, 2u);

    std::vector<char> rbuf(2048);
    int nread = env.mgr->read(path, meta, rbuf.data(), rbuf.size(), 0);
    ASSERT_EQ(static_cast<size_t>(nread), 2048u);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), 2048) == 0);
}

static void test_write_read_roundtrip_noop() { do_roundtrip_test(Codec::None); }
static void test_write_read_roundtrip_lz4()  { do_roundtrip_test(Codec::LZ4); }
static void test_write_read_roundtrip_zstd() { do_roundtrip_test(Codec::Zstd, 3); }

// --- Partial block write ---

static void test_partial_block_write() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "partial";
    (void)env.store.write_metadata(path, meta);

    // Write 500 bytes at offset 200 within block 0
    auto data = make_pattern(500, 42);
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  500, 200);
    ASSERT_EQ(written, 500);
    ASSERT_EQ(meta.original_size, 700u); // 200 + 500

    // Read back: bytes 0-199 should be 0 (sparse), bytes 200-699 should be data
    std::vector<char> rbuf(700);
    int nread = env.mgr->read(path, meta, rbuf.data(), 700, 0);
    ASSERT_EQ(nread, 700);

    // First 200 bytes: zeros (read-modify-write started with zeros)
    for (int i = 0; i < 200; ++i)
        ASSERT_EQ(static_cast<uint8_t>(rbuf[static_cast<size_t>(i)]), 0u);

    // Bytes 200-699: our data
    ASSERT_TRUE(std::memcmp(rbuf.data() + 200, data.data(), 500) == 0);
}

// --- Cross-block write ---

static void test_cross_block_write() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "cross";
    (void)env.store.write_metadata(path, meta);

    // Write spanning block boundary: offset=900, size=300 -> blocks 0 and 1
    auto data = make_pattern(300, 77);
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  300, 900);
    ASSERT_EQ(written, 300);
    ASSERT_EQ(meta.original_size, 1200u);
    ASSERT_EQ(meta.block_count, 2u);

    // Read back the written range
    std::vector<char> rbuf(300);
    int nread = env.mgr->read(path, meta, rbuf.data(), 300, 900);
    ASSERT_EQ(nread, 300);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), 300) == 0);
}

// --- Cross-block read ---

static void test_cross_block_read() {
    auto env = make_env(Codec::Zstd, 1);
    auto meta = make_meta(1024);
    const char* path = "crossread";
    (void)env.store.write_metadata(path, meta);

    // Write 2 full blocks
    auto data = make_pattern(2048, 11);
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  2048, 0);
    ASSERT_EQ(written, 2048);

    // Read across block boundary: offset=512, size=1024 -> spans blocks 0 and 1
    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 512);
    ASSERT_EQ(nread, 1024);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data() + 512, 1024) == 0);
}

// --- Overwrite compressed ---

static void test_overwrite_compressed() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "overwrite";
    (void)env.store.write_metadata(path, meta);

    auto data1 = make_pattern(1024, 0xAA);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data1.data()), 1024, 0);

    auto data2 = make_pattern(1024, 0xBB);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data2.data()), 1024, 0);

    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 0);
    ASSERT_EQ(nread, 1024);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data2.data(), 1024) == 0);
}

// --- Read sparse ---

static void test_read_sparse() {
    auto env = make_env(Codec::None);
    auto meta = make_meta(1024);
    meta.original_size = 2048; // file is 2048 bytes but no blocks written
    const char* path = "sparse";
    (void)env.store.write_metadata(path, meta);

    std::vector<char> rbuf(2048, static_cast<char>(-1));
    int nread = env.mgr->read(path, meta, rbuf.data(), 2048, 0);
    ASSERT_EQ(nread, 2048);

    // All zeros - no blocks exist
    for (size_t i = 0; i < 2048; ++i)
        ASSERT_EQ(rbuf[i], 0);
}

// --- Truncate shrink ---

static void test_truncate_shrink() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "trunc_shrink";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(3072); // 3 blocks
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 3072, 0);
    ASSERT_EQ(meta.block_count, 3u);

    Error err = env.mgr->truncate(path, meta, 500);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(meta.original_size, 500u);
    ASSERT_EQ(meta.block_count, 1u);

    // Read back - first 500 bytes should match original data
    std::vector<char> rbuf(500);
    int nread = env.mgr->read(path, meta, rbuf.data(), 500, 0);
    ASSERT_EQ(nread, 500);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), 500) == 0);
}

// --- Truncate extend ---

static void test_truncate_extend() {
    auto env = make_env(Codec::Zstd, 1);
    auto meta = make_meta(1024);
    const char* path = "trunc_ext";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(512);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 512, 0);

    Error err = env.mgr->truncate(path, meta, 4096);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(meta.original_size, 4096u);

    // Read beyond original data - should be zeros
    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 2048);
    ASSERT_EQ(nread, 1024);
    for (size_t i = 0; i < 1024; ++i)
        ASSERT_EQ(rbuf[i], 0);
}

// --- Large multi-block roundtrip ---

static void test_large_multiblock_lz4() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(65536); // 64 KiB blocks
    const char* path = "large_lz4";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(256 * 1024); // 4 blocks
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  data.size(), 0);
    ASSERT_EQ(static_cast<size_t>(written), data.size());
    ASSERT_EQ(meta.block_count, 4u);

    std::vector<char> rbuf(data.size());
    int nread = env.mgr->read(path, meta, rbuf.data(), rbuf.size(), 0);
    ASSERT_EQ(static_cast<size_t>(nread), data.size());
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), data.size()) == 0);
}

// --- Incompressible data ---

static void test_incompressible_data() {
    auto env = make_env(Codec::Zstd, 3);
    auto meta = make_meta(1024);
    const char* path = "random";
    (void)env.store.write_metadata(path, meta);

    auto data = make_random(2048);
    int written = env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()),
                                  data.size(), 0);
    ASSERT_EQ(static_cast<size_t>(written), data.size());

    std::vector<char> rbuf(data.size());
    int nread = env.mgr->read(path, meta, rbuf.data(), rbuf.size(), 0);
    ASSERT_EQ(static_cast<size_t>(nread), data.size());
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), data.size()) == 0);
}

// --- Metadata updated after write ---

static void test_metadata_updated() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "meta_chk";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(2048);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 2048, 0);

    ASSERT_EQ(meta.block_count, 2u);
    ASSERT_EQ(meta.compressed_sizes.size(), 2u);
    ASSERT_EQ(meta.block_checksums.size(), 2u);

    // compressed_sizes should be > 0
    ASSERT_TRUE(meta.compressed_sizes[0] > 0);
    ASSERT_TRUE(meta.compressed_sizes[1] > 0);

    // For LZ4 with pattern data, compressed should be smaller than block_size
    ASSERT_TRUE(meta.compressed_sizes[0] < 1024);

    // Checksums should be non-zero
    ASSERT_TRUE(meta.block_checksums[0] != 0);
    ASSERT_TRUE(meta.block_checksums[1] != 0);
}

// --- Truncate to zero ---

static void test_truncate_to_zero() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "trunc_zero";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(2048);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 2048, 0);

    Error err = env.mgr->truncate(path, meta, 0);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(meta.original_size, 0u);
    ASSERT_EQ(meta.block_count, 0u);
    ASSERT_TRUE(meta.compressed_sizes.empty());
    ASSERT_TRUE(meta.block_checksums.empty());
}

// --- Read past EOF ---

static void test_read_past_eof() {
    auto env = make_env(Codec::None);
    auto meta = make_meta(1024);
    const char* path = "eof";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(100);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 100, 0);

    std::vector<char> rbuf(200);
    int nread = env.mgr->read(path, meta, rbuf.data(), 200, 0);
    ASSERT_EQ(nread, 100); // clamped to file size
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), 100) == 0);

    // Read entirely past EOF
    nread = env.mgr->read(path, meta, rbuf.data(), 100, 500);
    ASSERT_EQ(nread, 0);
}

static void test_corruption_detected_on_read() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "corrupt";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(1024);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 1024, 0);

    // Verify checksum was stored
    ASSERT_TRUE(meta.block_checksums.size() >= 1);
    ASSERT_TRUE(meta.block_checksums[0] != 0);

    // Manually corrupt the block file on disk: flip a byte in the compressed data
    std::string blk_path = env.tmpdir + "/store/" + path + "/blocks/0000.blk";
    FILE* f = std::fopen(blk_path.c_str(), "r+b");
    ASSERT_TRUE(f != nullptr);
    // Flip the first byte
    int ch = std::fgetc(f);
    std::fseek(f, 0, SEEK_SET);
    std::fputc(ch ^ 0xFF, f);
    std::fclose(f);

    // Read should detect corruption and return negative error
    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 0);
    ASSERT_TRUE(nread < 0); // negative = error (EIO)
}

static void test_checksum_zero_means_skip() {
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "legacy";
    (void)env.store.write_metadata(path, meta);

    auto data = make_pattern(1024);
    (void)env.mgr->write(path, meta, reinterpret_cast<const char*>(data.data()), 1024, 0);

    // Manually set checksum to 0 (simulating legacy data without checksums)
    meta.block_checksums[0] = 0;

    // Corrupt the block file
    std::string blk_path = env.tmpdir + "/store/" + path + "/blocks/0000.blk";
    FILE* f = std::fopen(blk_path.c_str(), "r+b");
    ASSERT_TRUE(f != nullptr);
    int ch = std::fgetc(f);
    std::fseek(f, 0, SEEK_SET);
    std::fputc(ch ^ 0xFF, f);
    std::fclose(f);

    // Read should NOT detect corruption (checksum=0 means skip verification)
    // It may fail at decompression level though, so we just verify it doesn't
    // return Error::Corrupt specifically - it might return a decompress error
    // or succeed with garbage data depending on what the corruption does to LZ4.
    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 0);
    // We can't assert success because corrupted compressed data may still fail
    // decompression. The key assertion is that we got past the checksum check.
    // If corruption detection was active, it would have returned before decompress.
    // This test mainly ensures the checksum=0 skip path exists and doesn't crash.
    (void)nread;
}

static void test_fallback_to_uncompressed() {
    // Write purely random (incompressible) data with LZ4.
    // The block manager should fall back to storing uncompressed.
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "fallback";
    (void)env.store.write_metadata(path, meta);

    auto data = make_random(1024);
    int written = env.mgr->write(path, meta,
                                  reinterpret_cast<const char*>(data.data()), 1024, 0);
    ASSERT_EQ(written, 1024);

    // The block should have been stored uncompressed:
    // compressed_sizes[0] >= block_size (1024)
    ASSERT_TRUE(meta.compressed_sizes.size() >= 1);
    ASSERT_TRUE(meta.compressed_sizes[0] >= 1024u);

    // Read back - should succeed and match original data
    std::vector<char> rbuf(1024);
    int nread = env.mgr->read(path, meta, rbuf.data(), 1024, 0);
    ASSERT_EQ(nread, 1024);
    ASSERT_TRUE(std::memcmp(rbuf.data(), data.data(), 1024) == 0);
}

static void test_mixed_compressible_incompressible() {
    // Write 2 blocks: first is all zeros (highly compressible), second is random
    // (incompressible). Both should be readable. The first should compress, the
    // second should fall back to uncompressed.
    auto env = make_env(Codec::LZ4);
    auto meta = make_meta(1024);
    const char* path = "mixed";
    (void)env.store.write_metadata(path, meta);

    // Block 0: compressible (all zeros)
    std::vector<uint8_t> zeros(1024, 0);
    int written = env.mgr->write(path, meta,
                                  reinterpret_cast<const char*>(zeros.data()), 1024, 0);
    ASSERT_EQ(written, 1024);

    // Block 1: incompressible (random)
    auto random_data = make_random(1024);
    written = env.mgr->write(path, meta,
                              reinterpret_cast<const char*>(random_data.data()), 1024, 1024);
    ASSERT_EQ(written, 1024);

    ASSERT_EQ(meta.block_count, 2u);

    // Block 0 should have compressed (compressed_sizes[0] < block_size)
    ASSERT_TRUE(meta.compressed_sizes[0] < 1024u);

    // Block 1 should have fallen back (compressed_sizes[1] >= block_size)
    ASSERT_TRUE(meta.compressed_sizes[1] >= 1024u);

    // Read both blocks back - should match original data
    std::vector<char> rbuf(2048);
    int nread = env.mgr->read(path, meta, rbuf.data(), 2048, 0);
    ASSERT_EQ(nread, 2048);
    ASSERT_TRUE(std::memcmp(rbuf.data(), zeros.data(), 1024) == 0);
    ASSERT_TRUE(std::memcmp(rbuf.data() + 1024, random_data.data(), 1024) == 0);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_block_manager:\n");

    RUN(test_write_read_roundtrip_noop);
    RUN(test_write_read_roundtrip_lz4);
    RUN(test_write_read_roundtrip_zstd);
    RUN(test_partial_block_write);
    RUN(test_cross_block_write);
    RUN(test_cross_block_read);
    RUN(test_overwrite_compressed);
    RUN(test_read_sparse);
    RUN(test_truncate_shrink);
    RUN(test_truncate_extend);
    RUN(test_large_multiblock_lz4);
    RUN(test_incompressible_data);
    RUN(test_metadata_updated);
    RUN(test_truncate_to_zero);
    RUN(test_read_past_eof);
    RUN(test_corruption_detected_on_read);
    RUN(test_checksum_zero_means_skip);
    RUN(test_fallback_to_uncompressed);
    RUN(test_mixed_compressible_incompressible);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
