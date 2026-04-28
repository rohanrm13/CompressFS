#pragma once

#include "compressfs/metadata.hpp" // Error

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

// Forward-decl liburing to keep <liburing.h> out of every TU that includes
// this header. The consumer of BackingStore / BlockManager shouldn't pay the
// liburing include cost just to see an io_uring-backed read path exists.
struct io_uring;

namespace compressfs {

class IoUringEngine {
public:
    // Per-block read request: open base_dir_fd/rel_path, read up to out.size()
    // bytes into out, close. Results populated on completion.
    struct ReadRequest {
        int              base_dir_fd; // directory fd for openat relative base
        std::string_view rel_path;    // NUL-terminated backing is NOT required
                                      // (we copy into a local buffer before prep)
        std::span<uint8_t> out;       // destination buffer
        uint64_t         offset = 0;  // byte offset within the file

        int32_t bytes_read = -1;
        int32_t errno_val  = 0;
    };

    // Factory. queue_depth is the number of SQE slots; must be a power of two,
    // at least 8. Each ReadRequest consumes 3 SQEs (openat+read+close), so the
    // practical batch ceiling is queue_depth/3.
    [[nodiscard]] static std::pair<Error, IoUringEngine> create(uint32_t queue_depth = 128);

    IoUringEngine() = default;
    ~IoUringEngine();

    IoUringEngine(const IoUringEngine&)            = delete;
    IoUringEngine& operator=(const IoUringEngine&) = delete;
    IoUringEngine(IoUringEngine&& other) noexcept;
    IoUringEngine& operator=(IoUringEngine&& other) noexcept;

    // Submit all requests as linked chains and block until every completion
    // is drained. Per-request outcomes land in ReadRequest::bytes_read /
    // errno_val; the function-level Error is ring-level only (submission
    // failure, kernel surfaced fatal error, batch too large).
    [[nodiscard]] Error submit_and_wait(std::span<ReadRequest> reqs);

    // Low-level primitive: async pread against an already-open fd. Blocks
    // until the single completion arrives. Useful for callers that already
    // hold an fd (hot-path FUSE read where the file is open in an fh table).
    [[nodiscard]] Error read_at(int fd, std::span<uint8_t> buf,
                                uint64_t offset, ssize_t& out_bytes_read);

    [[nodiscard]] bool     valid() const { return ring_ != nullptr; }
    [[nodiscard]] uint32_t queue_depth() const { return queue_depth_; }

    // Largest reqs.size() submit_and_wait will accept. Equals queue_depth/3.
    [[nodiscard]] uint32_t max_batch_size() const { return queue_depth_ / 3; }

private:
    io_uring* ring_ = nullptr;
    uint32_t  queue_depth_ = 0;
};

// Thread-local ring accessor. Lazily initializes a per-thread engine the
// first time it's called from a given thread and reuses it on subsequent
// calls. Returns nullptr if creation fails on this thread (kernel feature
// missing, rlimit exhausted). Ownership is thread_local; callers must not
// delete the returned pointer or retain it beyond the thread's lifetime.
[[nodiscard]] IoUringEngine* thread_local_ring();

} // namespace compressfs