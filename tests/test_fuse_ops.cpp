// Integration test for FUSE operations layer.
//
// This test forks a child that mounts the compressfs filesystem, then
// the parent runs POSIX file operations against the mountpoint and
// verifies results.
//
// Requires /dev/fuse to be accessible. Skips gracefully if not available
// (e.g., in containers or WSL2 without FUSE support).

#include "compressfs/fuse_ops.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

static int g_pass = 0;
static int g_fail = 0;
static pid_t g_child = -1;
static std::string g_mountpoint;

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

static bool fuse_available() {
    return ::access("/dev/fuse", R_OK | W_OK) == 0;
}

// Wait until mountpoint is a FUSE mount (or timeout)
static bool wait_for_mount(const std::string& path, int timeout_secs) {
    for (int i = 0; i < timeout_secs * 10; ++i) {
        struct stat st;
        if (::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
            // Check if it's a mountpoint by comparing device IDs
            struct stat parent_st;
            std::string parent = path + "/..";
            if (::stat(parent.c_str(), &parent_st) == 0 &&
                st.st_dev != parent_st.st_dev) {
                return true;
            }
        }
        ::usleep(100000); // 100ms
    }
    return false;
}

static void unmount() {
    if (!g_mountpoint.empty()) {
        std::string cmd = "fusermount3 -u " + g_mountpoint + " 2>/dev/null";
        (void)::system(cmd.c_str());
    }
    if (g_child > 0) {
        ::kill(g_child, SIGTERM);
        ::waitpid(g_child, nullptr, 0);
        g_child = -1;
    }
}

// --- Tests ---

static void test_create_and_read_file() {
    std::string fpath = g_mountpoint + "/hello.txt";
    const char* data = "hello world\n";
    size_t dlen = std::strlen(data);

    int fd = ::open(fpath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    ASSERT_TRUE(fd >= 0);
    ssize_t n = ::write(fd, data, dlen);
    ASSERT_EQ(static_cast<size_t>(n), dlen);
    ::close(fd);

    // Read back
    fd = ::open(fpath.c_str(), O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    char buf[256] = {};
    n = ::read(fd, buf, sizeof(buf));
    ASSERT_EQ(static_cast<size_t>(n), dlen);
    ASSERT_TRUE(std::memcmp(buf, data, dlen) == 0);
    ::close(fd);
}

static void test_stat_file() {
    std::string fpath = g_mountpoint + "/hello.txt";
    struct stat st;
    ASSERT_TRUE(::stat(fpath.c_str(), &st) == 0);
    ASSERT_TRUE(S_ISREG(st.st_mode));
    ASSERT_EQ(static_cast<size_t>(st.st_size), std::strlen("hello world\n"));
}

static void test_readdir() {
    // Create a second file
    std::string fpath = g_mountpoint + "/second.txt";
    int fd = ::open(fpath.c_str(), O_CREAT | O_WRONLY, 0644);
    ASSERT_TRUE(fd >= 0);
    ::close(fd);

    // List directory
    DIR* dir = ::opendir(g_mountpoint.c_str());
    ASSERT_TRUE(dir != nullptr);

    int count = 0;
    bool found_hello = false, found_second = false;
    struct dirent* ent;
    while ((ent = ::readdir(dir)) != nullptr) {
        if (std::strcmp(ent->d_name, "hello.txt") == 0) found_hello = true;
        if (std::strcmp(ent->d_name, "second.txt") == 0) found_second = true;
        ++count;
    }
    ::closedir(dir);

    ASSERT_TRUE(found_hello);
    ASSERT_TRUE(found_second);
    ASSERT_TRUE(count >= 4); // ., .., hello.txt, second.txt
}

static void test_truncate() {
    std::string fpath = g_mountpoint + "/hello.txt";

    // Truncate to smaller size
    ASSERT_TRUE(::truncate(fpath.c_str(), 5) == 0);
    struct stat st;
    ::stat(fpath.c_str(), &st);
    ASSERT_EQ(st.st_size, 5);

    // Read back - should be first 5 bytes
    int fd = ::open(fpath.c_str(), O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    char buf[16] = {};
    ssize_t n = ::read(fd, buf, sizeof(buf));
    ASSERT_EQ(n, static_cast<ssize_t>(5));
    ASSERT_TRUE(std::memcmp(buf, "hello", 5) == 0);
    ::close(fd);

    // Extend via truncate (sparse)
    ASSERT_TRUE(::truncate(fpath.c_str(), 100) == 0);
    ::stat(fpath.c_str(), &st);
    ASSERT_EQ(st.st_size, 100);
}

static void test_unlink() {
    std::string fpath = g_mountpoint + "/second.txt";
    ASSERT_TRUE(::unlink(fpath.c_str()) == 0);

    struct stat st;
    ASSERT_TRUE(::stat(fpath.c_str(), &st) < 0);
    ASSERT_EQ(errno, ENOENT);
}

static void test_rename() {
    std::string from = g_mountpoint + "/hello.txt";
    std::string to   = g_mountpoint + "/renamed.txt";
    ASSERT_TRUE(::rename(from.c_str(), to.c_str()) == 0);

    struct stat st;
    ASSERT_TRUE(::stat(from.c_str(), &st) < 0);
    ASSERT_TRUE(::stat(to.c_str(), &st) == 0);

    // Rename back for subsequent tests
    ::rename(to.c_str(), from.c_str());
}

static void test_chmod() {
    std::string fpath = g_mountpoint + "/hello.txt";
    ASSERT_TRUE(::chmod(fpath.c_str(), 0755) == 0);

    struct stat st;
    ::stat(fpath.c_str(), &st);
    ASSERT_EQ(st.st_mode & 0777, static_cast<mode_t>(0755));
}

static void test_large_write_read() {
    std::string fpath = g_mountpoint + "/large.bin";

    // Write 256 KiB - spans multiple 64 KiB blocks
    constexpr size_t total = 256 * 1024;
    std::vector<uint8_t> data(total);
    for (size_t i = 0; i < total; ++i)
        data[i] = static_cast<uint8_t>(i & 0xFF);

    int fd = ::open(fpath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    ASSERT_TRUE(fd >= 0);
    ssize_t n = ::write(fd, data.data(), total);
    ASSERT_EQ(static_cast<size_t>(n), total);
    ::close(fd);

    // Read back
    fd = ::open(fpath.c_str(), O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    std::vector<uint8_t> rbuf(total);
    size_t read_total = 0;
    while (read_total < total) {
        n = ::read(fd, rbuf.data() + read_total, total - read_total);
        ASSERT_TRUE(n > 0);
        read_total += static_cast<size_t>(n);
    }
    ::close(fd);

    ASSERT_TRUE(rbuf == data);

    ::unlink(fpath.c_str());
}

static void test_partial_block_write() {
    std::string fpath = g_mountpoint + "/partial.bin";

    // Write 100 bytes at offset 0
    int fd = ::open(fpath.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    ASSERT_TRUE(fd >= 0);
    std::vector<uint8_t> d1(100, 0xAA);
    ::write(fd, d1.data(), d1.size());

    // Write 50 bytes at offset 50 (overlapping partial block write)
    ::lseek(fd, 50, SEEK_SET);
    std::vector<uint8_t> d2(50, 0xBB);
    ::write(fd, d2.data(), d2.size());
    ::close(fd);

    // Read back: bytes 0-49 should be 0xAA, bytes 50-99 should be 0xBB
    fd = ::open(fpath.c_str(), O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    std::vector<uint8_t> rbuf(100);
    ssize_t n = ::read(fd, rbuf.data(), 100);
    ASSERT_EQ(n, static_cast<ssize_t>(100));
    ::close(fd);

    for (int i = 0; i < 50; ++i)
        ASSERT_EQ(rbuf[static_cast<size_t>(i)], static_cast<uint8_t>(0xAA));
    for (int i = 50; i < 100; ++i)
        ASSERT_EQ(rbuf[static_cast<size_t>(i)], static_cast<uint8_t>(0xBB));

    ::unlink(fpath.c_str());
}

int main() {
    if (!fuse_available()) {
        std::fprintf(stderr, "SKIP: /dev/fuse not accessible\n");
        return 0;
    }

    // Create temp directories
    char backing_tmpl[] = "/tmp/cfs_backing_XXXXXX";
    char mount_tmpl[] = "/tmp/cfs_mount_XXXXXX";
    char* backing_dir = ::mkdtemp(backing_tmpl);
    char* mount_dir = ::mkdtemp(mount_tmpl);
    if (!backing_dir || !mount_dir) {
        std::fprintf(stderr, "FAIL: mkdtemp\n");
        return 1;
    }
    g_mountpoint = mount_dir;

    // Fork: child mounts, parent tests
    g_child = ::fork();
    if (g_child == 0) {
        // Child: mount filesystem in foreground
        compressfs::FsConfig cfg;
        cfg.backing_dir = backing_dir;

        // Build argv for FUSE: progname mountpoint -f
        char* fuse_argv[] = {
            const_cast<char*>("test_fuse_ops"),
            mount_dir,
            const_cast<char*>("-f"),
            nullptr
        };
        compressfs::compressfs_fuse_main(3, fuse_argv, cfg);
        ::_exit(0);
    }

    // Parent: wait for mount, then run tests
    if (!wait_for_mount(g_mountpoint, 5)) {
        std::fprintf(stderr, "FAIL: filesystem did not mount within 5 seconds\n");
        unmount();
        fs::remove_all(backing_dir);
        fs::remove_all(mount_dir);
        return 1;
    }

    int prev_fail = 0;
    std::fprintf(stderr, "test_fuse_ops:\n");

    RUN(test_create_and_read_file);
    RUN(test_stat_file);
    RUN(test_readdir);
    RUN(test_truncate);
    RUN(test_rename);
    RUN(test_unlink);
    RUN(test_chmod);
    RUN(test_large_write_read);
    RUN(test_partial_block_write);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);

    // Cleanup
    unmount();
    fs::remove_all(backing_dir);
    fs::remove_all(mount_dir);

    return g_fail > 0 ? 1 : 0;
}
