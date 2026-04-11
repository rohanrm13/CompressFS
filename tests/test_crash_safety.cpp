#include "compressfs/backing_store.hpp"

#include <cstdio>
#include <cstdlib>
#include <filesystem>

using namespace compressfs;
namespace fs = std::filesystem;

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

struct TestEnv {
    std::string tmpdir;
    std::string store_path;
    BackingStore bs;

    TestEnv() = default;
    TestEnv(TestEnv&&) noexcept = default;
    TestEnv& operator=(TestEnv&&) noexcept = default;

    ~TestEnv() {
        bs = BackingStore{};
        if (!tmpdir.empty())
            fs::remove_all(tmpdir);
    }
};

static TestEnv make_env() {
    TestEnv env;
    char tmpl[] = "/tmp/compressfs_crash_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    if (!dir) std::abort();
    env.tmpdir = dir;
    env.store_path = env.tmpdir + "/store";

    auto [err, bs] = BackingStore::open(env.store_path);
    if (err != Error::Ok) std::abort();
    env.bs = std::move(bs);
    return env;
}

static std::vector<uint8_t> make_pattern(size_t size, uint8_t seed = 0) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    return data;
}

// Verify that no .tmp files remain after a successful block write.
// The atomic rename pattern should leave only the final .blk file.
static void test_no_tmp_after_block_write() {
    auto env = make_env();
    auto data = make_pattern(4096);

    Error err = env.bs.write_block("crashtest", 0, data);
    ASSERT_EQ(err, Error::Ok);

    // Walk the blocks directory and check for .tmp files
    bool found_tmp = false;
    std::string blocks_dir = env.store_path + "/crashtest/blocks";
    for (const auto& entry : fs::directory_iterator(blocks_dir)) {
        if (entry.path().extension() == ".tmp") {
            found_tmp = true;
            break;
        }
    }
    ASSERT_TRUE(!found_tmp);
}

// Verify that no .tmp files remain after a successful metadata write.
static void test_no_tmp_after_metadata_write() {
    auto env = make_env();

    Metadata meta;
    meta.block_count = 0;
    meta.compressed_sizes = {};

    Error err = env.bs.write_metadata("crashtest", meta);
    ASSERT_EQ(err, Error::Ok);

    // Check for meta.bin.tmp in the file directory
    std::string file_dir = env.store_path + "/crashtest";
    bool found_tmp = false;
    for (const auto& entry : fs::directory_iterator(file_dir)) {
        std::string name = entry.path().filename().string();
        if (name.find(".tmp") != std::string::npos) {
            found_tmp = true;
            break;
        }
    }
    ASSERT_TRUE(!found_tmp);
}

// Verify that overwriting a block atomically replaces it.
// Read should never return partial/mixed data from old and new writes.
static void test_overwrite_is_atomic() {
    auto env = make_env();

    auto data_v1 = std::vector<uint8_t>(4096, 0xAA);
    auto data_v2 = std::vector<uint8_t>(4096, 0xBB);

    (void)env.bs.write_block("atomictest", 0, data_v1);
    (void)env.bs.write_block("atomictest", 0, data_v2);

    std::vector<uint8_t> out;
    Error err = env.bs.read_block("atomictest", 0, out);
    ASSERT_EQ(err, Error::Ok);

    // Must be entirely v2 - no bytes from v1
    bool all_bb = std::all_of(out.begin(), out.end(),
                              [](uint8_t b) { return b == 0xBB; });
    ASSERT_TRUE(all_bb);
}

// Verify that after writing multiple blocks then deleting,
// no stale data remains on disk.
static void test_delete_leaves_no_artifacts() {
    auto env = make_env();
    auto data = make_pattern(1024);

    (void)env.bs.write_block("deltest", 0, data);
    (void)env.bs.write_block("deltest", 1, data);

    Metadata meta;
    meta.block_count = 2;
    meta.compressed_sizes = {1024, 1024};
    (void)env.bs.write_metadata("deltest", meta);

    (void)env.bs.delete_file("deltest");

    // The file directory should not exist at all
    ASSERT_TRUE(!fs::exists(env.store_path + "/deltest"));
}

// Verify multiple sequential writes to the same block always produce
// the latest data. Tests that rename correctly replaces the target.
static void test_sequential_overwrites() {
    auto env = make_env();

    for (int i = 0; i < 50; ++i) {
        auto data = std::vector<uint8_t>(512, static_cast<uint8_t>(i));
        Error err = env.bs.write_block("seqtest", 0, data);
        ASSERT_EQ(err, Error::Ok);
    }

    std::vector<uint8_t> out;
    Error err = env.bs.read_block("seqtest", 0, out);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(out.size(), 512u);

    // Must be entirely the last value written (49)
    bool all_49 = std::all_of(out.begin(), out.end(),
                              [](uint8_t b) { return b == 49; });
    ASSERT_TRUE(all_49);
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_crash_safety:\n");

    RUN(test_no_tmp_after_block_write);
    RUN(test_no_tmp_after_metadata_write);
    RUN(test_overwrite_is_atomic);
    RUN(test_delete_leaves_no_artifacts);
    RUN(test_sequential_overwrites);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
