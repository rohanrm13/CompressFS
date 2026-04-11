#include "compressfs/backing_store.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <numeric>

using namespace compressfs;

static int g_pass = 0;
static int g_fail = 0;

#define ASSERT_EQ(a, b)                                                      \
    do {                                                                      \
        if ((a) != (b)) {                                                     \
            std::fprintf(stderr, "  FAIL %s:%d: %s != %s\n", __FILE__,       \
                         __LINE__, #a, #b);                                   \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__,   \
                         #expr);                                              \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define RUN(name)                                                             \
    do {                                                                      \
        std::fprintf(stderr, "  %-50s", #name);                              \
        name();                                                               \
        if (g_fail == prev_fail) {                                            \
            std::fprintf(stderr, "PASS\n");                                   \
            ++g_pass;                                                         \
        }                                                                     \
        prev_fail = g_fail;                                                   \
    } while (0)

// Helper: create a temp directory and open a backing store in it.
// Returns the path so the caller can clean up.
struct TestEnv {
    std::string tmpdir;
    BackingStore bs;

    TestEnv() = default;
    TestEnv(TestEnv&&) noexcept = default;
    TestEnv& operator=(TestEnv&&) noexcept = default;

    ~TestEnv() {
        bs = BackingStore{};
        if (!tmpdir.empty())
            std::filesystem::remove_all(tmpdir);
    }
};

static TestEnv make_env() {
    TestEnv env;
    char tmpl[] = "/tmp/compressfs_test_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    if (!dir) {
        std::fprintf(stderr, "mkdtemp failed\n");
        std::abort();
    }
    env.tmpdir = dir;

    std::string base = env.tmpdir + "/store";
    auto [err, bs] = BackingStore::open(base);
    if (err != Error::Ok) {
        std::fprintf(stderr, "BackingStore::open failed\n");
        std::abort();
    }
    env.bs = std::move(bs);
    return env;
}

static std::vector<uint8_t> make_pattern(size_t size, uint8_t seed = 0) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    return data;
}

// Tests

static void test_open_creates_directory() {
    char tmpl[] = "/tmp/compressfs_test_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    ASSERT_TRUE(dir != nullptr);
    std::string base = std::string(dir) + "/newstore";

    auto [err, bs] = BackingStore::open(base);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(bs.is_open());
    ASSERT_TRUE(std::filesystem::is_directory(base));

    std::filesystem::remove_all(dir);
}

static void test_open_existing_directory() {
    auto env = make_env();
    // Re-open the same directory - should succeed
    auto [err, bs2] = BackingStore::open(env.tmpdir + "/store");
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(bs2.is_open());
}

static void test_open_empty_path() {
    auto [err, bs] = BackingStore::open("");
    ASSERT_EQ(err, Error::InvalidArg);
}

static void test_write_read_block() {
    auto env = make_env();
    auto data = make_pattern(4096);

    Error err = env.bs.write_block("testfile", 0, data);
    ASSERT_EQ(err, Error::Ok);

    std::vector<uint8_t> out;
    err = env.bs.read_block("testfile", 0, out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(out.size(), data.size());
    ASSERT_TRUE(out == data);
}

static void test_write_overwrite_block() {
    auto env = make_env();
    auto data1 = make_pattern(4096, 0);
    auto data2 = make_pattern(4096, 42);

    (void)env.bs.write_block("testfile", 0, data1);

    Error err = env.bs.write_block("testfile", 0, data2);
    ASSERT_EQ(err, Error::Ok);

    std::vector<uint8_t> out;
    err = env.bs.read_block("testfile", 0, out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(out == data2);
}

static void test_read_nonexistent_block() {
    auto env = make_env();
    std::vector<uint8_t> out;
    Error err = env.bs.read_block("testfile", 99, out);
    ASSERT_EQ(err, Error::NotFound);
}

static void test_large_block_64k() {
    auto env = make_env();
    auto data = make_pattern(65536);

    Error err = env.bs.write_block("largefile", 0, data);
    ASSERT_EQ(err, Error::Ok);

    std::vector<uint8_t> out;
    err = env.bs.read_block("largefile", 0, out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(out.size(), 65536u);
    ASSERT_TRUE(out == data);
}

static void test_many_blocks() {
    auto env = make_env();
    constexpr uint32_t count = 100;

    for (uint32_t i = 0; i < count; ++i) {
        auto data = make_pattern(1024, static_cast<uint8_t>(i));
        Error err = env.bs.write_block("manyblocks", i, data);
        ASSERT_EQ(err, Error::Ok);
    }

    for (uint32_t i = 0; i < count; ++i) {
        auto expected = make_pattern(1024, static_cast<uint8_t>(i));
        std::vector<uint8_t> out;
        Error err = env.bs.read_block("manyblocks", i, out);
        ASSERT_EQ(err, Error::Ok);
        ASSERT_TRUE(out == expected);
    }
}

static void test_block_index_formatting() {
    auto env = make_env();
    auto data = make_pattern(64);

    // Block 9999 should produce "9999.blk"
    Error err = env.bs.write_block("fmttest", 9999, data);
    ASSERT_EQ(err, Error::Ok);

    std::vector<uint8_t> out;
    err = env.bs.read_block("fmttest", 9999, out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(out == data);
}

static void test_write_read_metadata() {
    auto env = make_env();

    Metadata meta;
    meta.block_count = 2;
    meta.block_size = 65536;
    meta.original_size = 65536 + 1000;
    meta.codec = Codec::None;
    meta.cksum_type = ChecksumType::CRC32C;
    meta.compressed_sizes = {65536, 1000};

    Error err = env.bs.write_metadata("metafile", meta);
    ASSERT_EQ(err, Error::Ok);

    Metadata out;
    err = env.bs.read_metadata("metafile", out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(out.magic, kMetaMagic);
    ASSERT_EQ(out.block_count, 2u);
    ASSERT_EQ(out.block_size, 65536u);
    ASSERT_EQ(out.original_size, 65536u + 1000);
    ASSERT_EQ(out.compressed_sizes.size(), 2u);
    ASSERT_EQ(out.compressed_sizes[0], 65536u);
    ASSERT_EQ(out.compressed_sizes[1], 1000u);
}

static void test_overwrite_metadata() {
    auto env = make_env();

    Metadata m1;
    m1.block_count = 1;
    m1.compressed_sizes = {4096};
    (void)env.bs.write_metadata("metafile", m1);

    Metadata m2;
    m2.block_count = 3;
    m2.compressed_sizes = {100, 200, 300};
    m2.original_size = 600;
    Error err = env.bs.write_metadata("metafile", m2);
    ASSERT_EQ(err, Error::Ok);

    Metadata out;
    err = env.bs.read_metadata("metafile", out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(out.block_count, 3u);
    ASSERT_EQ(out.original_size, 600u);
}

static void test_read_nonexistent_metadata() {
    auto env = make_env();
    Metadata out;
    Error err = env.bs.read_metadata("nosuchfile", out);
    ASSERT_EQ(err, Error::NotFound);
}

static void test_delete_file() {
    auto env = make_env();

    auto data = make_pattern(1024);
    (void)env.bs.write_block("delme", 0, data);
    (void)env.bs.write_block("delme", 1, data);

    Metadata meta;
    meta.block_count = 2;
    meta.compressed_sizes = {1024, 1024};
    (void)env.bs.write_metadata("delme", meta);

    Error err = env.bs.delete_file("delme");
    ASSERT_EQ(err, Error::Ok);

    // Verify everything is gone
    std::vector<uint8_t> out;
    err = env.bs.read_block("delme", 0, out);
    ASSERT_EQ(err, Error::NotFound);

    Metadata mout;
    err = env.bs.read_metadata("delme", mout);
    ASSERT_EQ(err, Error::NotFound);
}

static void test_delete_nonexistent() {
    auto env = make_env();
    Error err = env.bs.delete_file("nope");
    ASSERT_EQ(err, Error::NotFound);
}

static void test_list_files_empty() {
    auto env = make_env();
    std::vector<std::string> files;
    Error err = env.bs.list_files(files);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(files.empty());
}

static void test_list_files_multiple() {
    auto env = make_env();
    auto data = make_pattern(64);

    (void)env.bs.write_block("alpha", 0, data);
    (void)env.bs.write_block("beta", 0, data);
    (void)env.bs.write_block("gamma", 0, data);

    std::vector<std::string> files;
    Error err = env.bs.list_files(files);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(files.size(), 3u);

    std::sort(files.begin(), files.end());
    ASSERT_EQ(files[0], "alpha");
    ASSERT_EQ(files[1], "beta");
    ASSERT_EQ(files[2], "gamma");
}

static void test_list_after_delete() {
    auto env = make_env();
    auto data = make_pattern(64);

    (void)env.bs.write_block("a", 0, data);
    (void)env.bs.write_block("b", 0, data);
    (void)env.bs.delete_file("a");

    std::vector<std::string> files;
    Error err = env.bs.list_files(files);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(files.size(), 1u);
    ASSERT_EQ(files[0], "b");
}

static void test_empty_path_rejected() {
    auto env = make_env();
    auto data = make_pattern(64);

    ASSERT_EQ(env.bs.write_block("", 0, data), Error::InvalidArg);

    std::vector<uint8_t> out;
    ASSERT_EQ(env.bs.read_block("", 0, out), Error::InvalidArg);

    Metadata meta;
    ASSERT_EQ(env.bs.write_metadata("", meta), Error::InvalidArg);
    ASSERT_EQ(env.bs.read_metadata("", meta), Error::InvalidArg);
    ASSERT_EQ(env.bs.delete_file(""), Error::InvalidArg);
}

static void test_block_index_overflow() {
    auto env = make_env();
    auto data = make_pattern(64);
    Error err = env.bs.write_block("testfile", kMaxBlocks, data);
    ASSERT_EQ(err, Error::Overflow);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_backing_store:\n");

    RUN(test_open_creates_directory);
    RUN(test_open_existing_directory);
    RUN(test_open_empty_path);
    RUN(test_write_read_block);
    RUN(test_write_overwrite_block);
    RUN(test_read_nonexistent_block);
    RUN(test_large_block_64k);
    RUN(test_many_blocks);
    RUN(test_block_index_formatting);
    RUN(test_write_read_metadata);
    RUN(test_overwrite_metadata);
    RUN(test_read_nonexistent_metadata);
    RUN(test_delete_file);
    RUN(test_delete_nonexistent);
    RUN(test_list_files_empty);
    RUN(test_list_files_multiple);
    RUN(test_list_after_delete);
    RUN(test_empty_path_rejected);
    RUN(test_block_index_overflow);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
