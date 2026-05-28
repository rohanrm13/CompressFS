#include "compressfs/io_uring_engine.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <limits.h> // PATH_MAX
#include <unistd.h>

using namespace compressfs;

static int g_pass = 0;
static int g_fail = 0;
static int g_skip = 0;

#define ASSERT_EQ(a, b)                                                       \
    do {                                                                      \
        if ((a) != (b)) {                                                     \
            std::fprintf(stderr, "  FAIL %s:%d: %s != %s\n", __FILE__,        \
                         __LINE__, #a, #b);                                   \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__,    \
                         #expr);                                              \
            ++g_fail;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define SKIP_IF_NO_URING()                                                    \
    do {                                                                      \
        if (!uring_ok()) {                                                    \
            ++g_skip;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define SKIP_IF_NO_BATCH()                                                    \
    do {                                                                      \
        if (!uring_ok() || !batch_ok()) {                                     \
            ++g_skip;                                                         \
            return;                                                           \
        }                                                                     \
    } while (0)

#define RUN(name)                                                             \
    do {                                                                      \
        int pf = g_fail, ps = g_skip;                                         \
        std::fprintf(stderr, "  %-50s", #name);                              \
        name();                                                               \
        if (g_skip != ps)                                                     \
            std::fprintf(stderr, "SKIP\n");                                   \
        else if (g_fail == pf) {                                              \
            std::fprintf(stderr, "PASS\n");                                   \
            ++g_pass;                                                         \
        }                                                                     \
    } while (0)

// Probe io_uring availability exactly once and cache it. Uses the smallest
// legal depth (8) so the probe itself can't fail on the pow2/>=8 check and
// only reflects genuine kernel/runtime support.
static bool uring_ok() {
    static int cached = -1; // -1 unknown, 0 no, 1 yes
    if (cached < 0) {
        auto [err, eng] = IoUringEngine::create(8);
        cached = (err == Error::Ok) ? 1 : 0;
    }
    return cached == 1;
}

static IoUringEngine make_engine(uint32_t depth = 128) {
    auto [err, eng] = IoUringEngine::create(depth);
    if (err != Error::Ok) {
        // Only reachable if availability changed between uring_ok() and here,
        // or depth is invalid. Tests always gate on SKIP_IF_NO_URING first.
        std::fprintf(stderr, "make_engine: create failed\n");
        std::abort();
    }
    return std::move(eng);
}

static std::vector<uint8_t> make_pattern(size_t size, uint8_t seed = 0) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    return data;
}

// RAII temp directory plus an O_DIRECTORY fd suitable as openat base.
struct TmpDir {
    std::string path;
    int         dfd = -1;

    TmpDir() {
        char tmpl[] = "/tmp/compressfs_uring_XXXXXX";
        char* d = ::mkdtemp(tmpl);
        if (!d) {
            std::fprintf(stderr, "mkdtemp failed\n");
            std::abort();
        }
        path = d;
        // O_DIRECTORY: the engine passes this fd as openat's dirfd. A non-dir
        // fd would surface as ENOTDIR per request, masking real bugs.
        dfd = ::open(path.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC);
        if (dfd < 0) {
            std::fprintf(stderr, "open dir failed\n");
            std::abort();
        }
    }
    ~TmpDir() {
        if (dfd >= 0) ::close(dfd);
        if (!path.empty()) std::filesystem::remove_all(path);
    }
    TmpDir(const TmpDir&)            = delete;
    TmpDir& operator=(const TmpDir&) = delete;

    std::string file(const std::string& name) const { return path + "/" + name; }
};

// Write data to base_dir/name, handling partial writes. abort() on failure —
// a test harness can't meaningfully continue if the fixture can't be built.
static void write_file(const std::string& full_path,
                       const std::vector<uint8_t>& data) {
    int fd = ::open(full_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        std::fprintf(stderr, "write_file open failed: %s\n", full_path.c_str());
        std::abort();
    }
    size_t off = 0;
    while (off < data.size()) {
        ssize_t n = ::write(fd, data.data() + off, data.size() - off);
        if (n <= 0) {
            std::fprintf(stderr, "write_file write failed\n");
            std::abort();
        }
        off += static_cast<size_t>(n);
    }
    ::close(fd);
}

// Probe (once) whether a full openat_direct->read->close chain actually
// completes on this kernel. Writes a real file, submits one request, and
// checks the read produced bytes. Cached. See SKIP_IF_NO_BATCH for why this is
// separate from uring_ok().
static bool batch_ok() {
    static int cached = -1;
    if (cached < 0) {
        cached = 0;
        if (uring_ok()) {
            TmpDir dir;
            auto data = make_pattern(64, 1);
            write_file(dir.file("probe.blk"), data);

            auto [err, eng] = IoUringEngine::create(8);
            if (err == Error::Ok) {
                std::vector<uint8_t> out(data.size());
                IoUringEngine::ReadRequest req;
                req.base_dir_fd = dir.dfd;
                req.rel_path    = "probe.blk";
                req.out         = out;
                std::span<IoUringEngine::ReadRequest> batch(&req, 1);
                if (eng.submit_and_wait(batch) == Error::Ok && req.bytes_read == 64)
                    cached = 1;
            }
        }
    }
    return cached == 1;
}

// --- Tests: create() validation (no working ring required) ---

// create() must reject depths that aren't a power of two >= 8 BEFORE touching
// the kernel, so these run even where io_uring is unavailable. The contract
// matters because max_batch_size() == depth/3 is computed off the requested
// depth; silently accepting a non-pow2 (which io_uring_queue_init rounds up)
// would make the batch ceiling lie.
static void test_create_rejects_bad_depth() {
    for (uint32_t bad : {0u, 1u, 3u, 6u, 7u, 100u, 1000u}) {
        auto [err, eng] = IoUringEngine::create(bad);
        ASSERT_EQ(err, Error::InvalidArg);
        ASSERT_TRUE(!eng.valid());
    }
}

// --- Tests: create() success + accessors ---

static void test_create_valid() {
    SKIP_IF_NO_URING();
    auto [err, eng] = IoUringEngine::create(128);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_TRUE(eng.valid());
    ASSERT_EQ(eng.queue_depth(), 128u);
    ASSERT_EQ(eng.max_batch_size(), 128u / 3); // 42
}

// --- Tests: read_at (single async pread on an already-open fd) ---

static void test_read_at_full() {
    SKIP_IF_NO_URING();
    TmpDir dir;
    auto data = make_pattern(4096, 7);
    write_file(dir.file("a"), data);

    int fd = ::open(dir.file("a").c_str(), O_RDONLY | O_CLOEXEC);
    ASSERT_TRUE(fd >= 0);

    auto eng = make_engine();
    std::vector<uint8_t> out(4096);
    ssize_t got = -1;
    Error err = eng.read_at(fd, out, 0, got);
    ::close(fd);

    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(got, static_cast<ssize_t>(4096));
    ASSERT_TRUE(out == data);
}

// Reading near EOF must return a short count, not an error. The engine
// documents that read_at permits short reads and surfaces the syscall return
// verbatim in out_bytes_read.
static void test_read_at_offset_short() {
    SKIP_IF_NO_URING();
    TmpDir dir;
    auto data = make_pattern(1000, 3);
    write_file(dir.file("a"), data);

    int fd = ::open(dir.file("a").c_str(), O_RDONLY | O_CLOEXEC);
    ASSERT_TRUE(fd >= 0);

    auto eng = make_engine();
    std::vector<uint8_t> out(4096);
    ssize_t got = -1;
    Error err = eng.read_at(fd, out, 900, got); // only 100 bytes remain
    ::close(fd);

    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(got, static_cast<ssize_t>(100));
    ASSERT_TRUE(std::memcmp(out.data(), data.data() + 900, 100) == 0);
}

static void test_read_at_bad_args() {
    SKIP_IF_NO_URING();
    auto eng = make_engine();
    std::vector<uint8_t> out(64);
    ssize_t got = 0;
    ASSERT_EQ(eng.read_at(-1, out, 0, got), Error::InvalidArg);
    ASSERT_EQ(eng.read_at(0, std::span<uint8_t>{}, 0, got), Error::InvalidArg);
}

// --- Tests: submit_and_wait (batched openat->read->close chains) ---

static void test_submit_basic_batch() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;

    constexpr size_t kFiles = 5;
    std::vector<std::string>          names(kFiles);
    std::vector<std::vector<uint8_t>> payloads(kFiles);
    for (size_t i = 0; i < kFiles; ++i) {
        names[i]    = std::to_string(i) + ".blk";
        payloads[i] = make_pattern(1024 + i * 256, static_cast<uint8_t>(i * 11));
        write_file(dir.file(names[i]), payloads[i]);
    }

    auto eng = make_engine();

    std::vector<std::vector<uint8_t>> outs(kFiles);
    std::vector<IoUringEngine::ReadRequest> reqs(kFiles);
    for (size_t i = 0; i < kFiles; ++i) {
        outs[i].resize(payloads[i].size());
        reqs[i].base_dir_fd = dir.dfd;
        reqs[i].rel_path    = names[i];
        reqs[i].out         = outs[i];
        reqs[i].offset      = 0;
    }

    Error err = eng.submit_and_wait(reqs);
    ASSERT_EQ(err, Error::Ok);

    for (size_t i = 0; i < kFiles; ++i) {
        ASSERT_EQ(reqs[i].bytes_read, static_cast<int32_t>(payloads[i].size()));
        ASSERT_EQ(reqs[i].errno_val, 0);
        ASSERT_TRUE(outs[i] == payloads[i]);
    }
}

// Submitting two batches on the same engine exercises fixed-file-slot reuse:
// each batch's close_direct must free the slot so the next batch's
// openat_direct can reclaim it. A leak here would make the second batch fail.
static void test_submit_reuse_ring() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;
    auto data = make_pattern(2048, 9);
    write_file(dir.file("x.blk"), data);

    auto eng = make_engine();

    for (int round = 0; round < 3; ++round) {
        std::vector<uint8_t> out(data.size());
        IoUringEngine::ReadRequest req;
        req.base_dir_fd = dir.dfd;
        req.rel_path    = "x.blk";
        req.out         = out;

        std::span<IoUringEngine::ReadRequest> batch(&req, 1);
        Error err = eng.submit_and_wait(batch);
        ASSERT_EQ(err, Error::Ok);
        ASSERT_EQ(req.bytes_read, static_cast<int32_t>(data.size()));
        ASSERT_TRUE(out == data);
    }
}

static void test_submit_empty_batch() {
    SKIP_IF_NO_URING();
    auto eng = make_engine();
    std::span<IoUringEngine::ReadRequest> empty;
    ASSERT_EQ(eng.submit_and_wait(empty), Error::InvalidArg);
}

// A batch larger than max_batch_size() must be rejected up front (the caller
// is expected to split). With depth 8, max_batch_size == 2, so 3 overflows.
static void test_submit_overflow() {
    SKIP_IF_NO_URING();
    auto eng = make_engine(8);
    ASSERT_EQ(eng.max_batch_size(), 2u);

    std::vector<uint8_t> buf(16);
    std::vector<IoUringEngine::ReadRequest> reqs(3);
    for (auto& r : reqs) {
        r.base_dir_fd = 0; // not inspected — overflow is checked first
        r.rel_path    = "z";
        r.out         = buf;
    }
    ASSERT_EQ(eng.submit_and_wait(reqs), Error::Overflow);
}

static void test_submit_invalid_dir_fd() {
    SKIP_IF_NO_URING();
    auto eng = make_engine();
    std::vector<uint8_t> buf(16);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = -1;
    req.rel_path    = "z";
    req.out         = buf;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);
    ASSERT_EQ(eng.submit_and_wait(batch), Error::InvalidArg);
}

static void test_submit_empty_path() {
    SKIP_IF_NO_URING();
    auto eng = make_engine();
    std::vector<uint8_t> buf(16);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = 0;
    req.rel_path    = "";
    req.out         = buf;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);
    ASSERT_EQ(eng.submit_and_wait(batch), Error::InvalidArg);
}

// A path that doesn't fit the engine's PATH_MAX copy buffer must be rejected,
// not silently truncated (truncation would open the wrong file).
static void test_submit_path_too_long() {
    SKIP_IF_NO_URING();
    auto eng = make_engine();
    std::string longpath(PATH_MAX, 'a'); // length == PATH_MAX, copy needs +1 for NUL
    std::vector<uint8_t> buf(16);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = 0;
    req.rel_path    = longpath;
    req.out         = buf;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);
    ASSERT_EQ(eng.submit_and_wait(batch), Error::InvalidArg);
}

// When openat fails, IOSQE_IO_LINK cancels the linked read/close. The engine
// must (a) keep the function-level result Ok (ring is healthy), (b) report the
// openat errno on the request, and (c) leave bytes_read == -1. ENOENT here
// also implicitly confirms the cancel didn't clobber the recorded errno with a
// later -ECANCELED CQE.
static void test_submit_nonexistent_file() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;
    auto eng = make_engine();

    std::vector<uint8_t> out(64);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = dir.dfd;
    req.rel_path    = "nope.blk";
    req.out         = out;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);

    Error err = eng.submit_and_wait(batch);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(req.bytes_read, -1);
    ASSERT_EQ(req.errno_val, ENOENT);
}

// Per-request outcomes must be independent within one batch: a missing file
// must not poison the sibling request's successful read.
static void test_submit_mixed_success_and_failure() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;
    auto data = make_pattern(512, 5);
    write_file(dir.file("present.blk"), data);

    auto eng = make_engine();

    std::vector<uint8_t> out_ok(data.size());
    std::vector<uint8_t> out_bad(64);
    std::vector<IoUringEngine::ReadRequest> reqs(2);
    reqs[0].base_dir_fd = dir.dfd;
    reqs[0].rel_path    = "present.blk";
    reqs[0].out         = out_ok;
    reqs[1].base_dir_fd = dir.dfd;
    reqs[1].rel_path    = "absent.blk";
    reqs[1].out         = out_bad;

    Error err = eng.submit_and_wait(reqs);
    ASSERT_EQ(err, Error::Ok);

    ASSERT_EQ(reqs[0].bytes_read, static_cast<int32_t>(data.size()));
    ASSERT_EQ(reqs[0].errno_val, 0);
    ASSERT_TRUE(out_ok == data);

    ASSERT_EQ(reqs[1].bytes_read, -1);
    ASSERT_EQ(reqs[1].errno_val, ENOENT);
}

// out larger than the file => short read reported as the true file size.
static void test_submit_short_read() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;
    auto data = make_pattern(100, 1);
    write_file(dir.file("small.blk"), data);

    auto eng = make_engine();
    std::vector<uint8_t> out(4096);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = dir.dfd;
    req.rel_path    = "small.blk";
    req.out         = out;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);

    Error err = eng.submit_and_wait(batch);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(req.bytes_read, static_cast<int32_t>(100));
    ASSERT_TRUE(std::memcmp(out.data(), data.data(), 100) == 0);
}

// Reading from a non-zero offset within a batch chain.
static void test_submit_with_offset() {
    SKIP_IF_NO_BATCH();
    TmpDir dir;
    auto data = make_pattern(4096, 2);
    write_file(dir.file("off.blk"), data);

    auto eng = make_engine();
    std::vector<uint8_t> out(1024);
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = dir.dfd;
    req.rel_path    = "off.blk";
    req.out         = out;
    req.offset      = 2048;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);

    Error err = eng.submit_and_wait(batch);
    ASSERT_EQ(err, Error::Ok);
    ASSERT_EQ(req.bytes_read, static_cast<int32_t>(1024));
    ASSERT_TRUE(std::memcmp(out.data(), data.data() + 2048, 1024) == 0);
}

// --- Tests: move semantics (ring ownership transfer) ---

static void test_move_construct() {
    SKIP_IF_NO_URING();
    auto src = make_engine(64);
    ASSERT_TRUE(src.valid());

    IoUringEngine dst(std::move(src));
    ASSERT_TRUE(dst.valid());
    ASSERT_EQ(dst.queue_depth(), 64u);
    ASSERT_TRUE(!src.valid()); // moved-from ring must be null, not double-freed
}

static void test_move_assign() {
    SKIP_IF_NO_URING();
    auto a = make_engine(64);
    auto b = make_engine(32);
    ASSERT_EQ(b.queue_depth(), 32u);

    b = std::move(a); // b's original ring must be torn down, then a's adopted
    ASSERT_TRUE(b.valid());
    ASSERT_EQ(b.queue_depth(), 64u);
    ASSERT_TRUE(!a.valid());
}

// --- Tests: thread_local_ring ---

// Repeated calls from one thread must return the same cached engine (lazy
// init once), and the engine must be usable for a real read.
static void test_thread_local_ring_idempotent() {
    SKIP_IF_NO_BATCH();
    IoUringEngine* r1 = thread_local_ring();
    IoUringEngine* r2 = thread_local_ring();
    ASSERT_TRUE(r1 != nullptr);
    ASSERT_TRUE(r1 == r2);

    TmpDir dir;
    auto data = make_pattern(256, 4);
    write_file(dir.file("tl.blk"), data);

    std::vector<uint8_t> out(data.size());
    IoUringEngine::ReadRequest req;
    req.base_dir_fd = dir.dfd;
    req.rel_path    = "tl.blk";
    req.out         = out;
    std::span<IoUringEngine::ReadRequest> batch(&req, 1);
    ASSERT_EQ(r1->submit_and_wait(batch), Error::Ok);
    ASSERT_TRUE(out == data);
}

// Each thread gets its own ring. We can't compare pointers across threads
// safely (different thread_local storage), but we can assert each worker sees
// a non-null ring and completes an independent read without cross-thread
// locking — the whole point of per-thread rings.
static void test_thread_local_ring_per_thread() {
    SKIP_IF_NO_BATCH();
    constexpr int kThreads = 4;
    std::atomic<int> ok{0};

    auto worker = [&](int id) {
        IoUringEngine* r = thread_local_ring();
        if (!r) return;

        TmpDir dir;
        auto data = make_pattern(512, static_cast<uint8_t>(id * 7 + 1));
        write_file(dir.file("w.blk"), data);

        std::vector<uint8_t> out(data.size());
        IoUringEngine::ReadRequest req;
        req.base_dir_fd = dir.dfd;
        req.rel_path    = "w.blk";
        req.out         = out;
        std::span<IoUringEngine::ReadRequest> batch(&req, 1);
        if (r->submit_and_wait(batch) == Error::Ok && out == data)
            ok.fetch_add(1, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i)
        threads.emplace_back(worker, i);
    for (auto& t : threads)
        t.join();

    ASSERT_EQ(ok.load(), kThreads);
}

int main() {
    std::fprintf(stderr, "test_io_uring_engine:\n");

    RUN(test_create_rejects_bad_depth);
    RUN(test_create_valid);

    RUN(test_read_at_full);
    RUN(test_read_at_offset_short);
    RUN(test_read_at_bad_args);

    RUN(test_submit_basic_batch);
    RUN(test_submit_reuse_ring);
    RUN(test_submit_empty_batch);
    RUN(test_submit_overflow);
    RUN(test_submit_invalid_dir_fd);
    RUN(test_submit_empty_path);
    RUN(test_submit_path_too_long);
    RUN(test_submit_nonexistent_file);
    RUN(test_submit_mixed_success_and_failure);
    RUN(test_submit_short_read);
    RUN(test_submit_with_offset);

    RUN(test_move_construct);
    RUN(test_move_assign);

    RUN(test_thread_local_ring_idempotent);
    RUN(test_thread_local_ring_per_thread);

    std::fprintf(stderr, "\n  %d passed, %d failed, %d skipped\n",
                 g_pass, g_fail, g_skip);
    return g_fail > 0 ? 1 : 0;
}
