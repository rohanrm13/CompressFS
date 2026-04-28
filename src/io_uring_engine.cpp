#include "compressfs/io_uring_engine.hpp"

#include <liburing.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits.h> // PATH_MAX
#include <new>      // std::nothrow

#ifndef IORING_SETUP_SINGLE_ISSUER
#define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_COOP_TASKRUN
#define IORING_SETUP_COOP_TASKRUN  (1U << 9)
#endif

namespace compat {

// Register a sparse fixed-file table. Equivalent to liburing's
// io_uring_register_files_sparse(ring, nr) — pass an array of -1s.
inline int register_files_sparse(struct io_uring* ring, unsigned nr) {
    constexpr unsigned kCap = 1024;
    int stack_buf[kCap];
    if (nr > kCap) return -EINVAL;
    for (unsigned i = 0; i < nr; ++i) stack_buf[i] = -1;
    return io_uring_register_files(ring, stack_buf, nr);
}

inline void sqe_set_data64(struct io_uring_sqe* sqe, uint64_t data) {
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(data)));
}

inline void prep_close_direct(struct io_uring_sqe* sqe, unsigned slot) {
    io_uring_prep_close(sqe, 0);
    sqe->file_index = slot + 1;
}

inline void prep_openat_direct(struct io_uring_sqe* sqe, int dfd,
                               const char* path, int flags, mode_t mode,
                               unsigned slot) {
    io_uring_prep_openat(sqe, dfd, path, flags, mode);
    sqe->file_index = slot + 1; // same 1-biased convention
}

} // namespace compat

namespace compressfs {

namespace {

// SQE user_data encoding. Each ReadRequest consumes 3 SQEs; we tag each CQE
// so we can route the completion to the originating request.
enum Op : uint64_t { OP_OPENAT = 0, OP_READ = 1, OP_CLOSE = 2 };

constexpr uint64_t encode_tag(uint32_t req_idx, Op op) {
    return (static_cast<uint64_t>(req_idx) << 2) | static_cast<uint64_t>(op);
}
constexpr uint32_t decode_req(uint64_t tag) { return static_cast<uint32_t>(tag >> 2); }
constexpr Op       decode_op(uint64_t tag)  { return static_cast<Op>(tag & 0x3ULL); }

bool is_pow2_ge_8(uint32_t n) { return n >= 8 && (n & (n - 1)) == 0; }

} // namespace

// IoUringEngine
std::pair<Error, IoUringEngine> IoUringEngine::create(uint32_t queue_depth) {
    IoUringEngine eng;

    if (!is_pow2_ge_8(queue_depth))
        return {Error::InvalidArg, std::move(eng)};

    auto* ring = new (std::nothrow) io_uring{};
    if (!ring)
        return {Error::NoMem, std::move(eng)};

    io_uring_params params{};
    params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;

    int rc = io_uring_queue_init_params(queue_depth, ring, &params);
    if (rc == -EINVAL) {
        // BUG RISK: kernel too old for SINGLE_ISSUER/COOP_TASKRUN. Retry
        // without them so we still work on 5.15. Silently degrading is the
        // right call here — the feature flags are pure performance hints.
        params.flags = 0;
        rc = io_uring_queue_init_params(queue_depth, ring, &params);
    }
    if (rc < 0) {
        delete ring;
        return {Error::IO, std::move(eng)};
    }

    // Pre-register a sparse fixed-file table. Slots 0..max_batch-1 are used
    // by openat_direct / read with IOSQE_FIXED_FILE / close_direct in each
    // batch. "Sparse" means all slots are initially empty (-1).
    uint32_t nr_files = queue_depth / 3;
    if (nr_files == 0) nr_files = 1;
    rc = compat::register_files_sparse(ring, nr_files);
    if (rc < 0) {
        io_uring_queue_exit(ring);
        delete ring;
        // EINVAL here on pre-5.19 kernels that lack register_files_sparse.
        // Caller can fall back to the non-batched sync path.
        return {Error::IO, std::move(eng)};
    }

    eng.ring_ = ring;
    eng.queue_depth_ = queue_depth;
    return {Error::Ok, std::move(eng)};
}

IoUringEngine::~IoUringEngine() {
    if (ring_) {
        io_uring_queue_exit(ring_);
        delete ring_;
        ring_ = nullptr;
    }
}

IoUringEngine::IoUringEngine(IoUringEngine&& other) noexcept
    : ring_(other.ring_), queue_depth_(other.queue_depth_) {
    other.ring_ = nullptr;
    other.queue_depth_ = 0;
}

IoUringEngine& IoUringEngine::operator=(IoUringEngine&& other) noexcept {
    if (this != &other) {
        if (ring_) {
            io_uring_queue_exit(ring_);
            delete ring_;
        }
        ring_ = other.ring_;
        queue_depth_ = other.queue_depth_;
        other.ring_ = nullptr;
        other.queue_depth_ = 0;
    }
    return *this;
}

Error IoUringEngine::submit_and_wait(std::span<ReadRequest> reqs) {
    if (!ring_)
        return Error::InvalidArg;
    if (reqs.empty())
        return Error::InvalidArg;
    if (reqs.size() > max_batch_size())
        return Error::Overflow;

    // Per-request path copy. string_view may point into a std::string whose
    // buffer is not NUL-terminated; openat needs a C string. We stash a copy
    // per request on the stack-allocated array below.
    //
    // BUG RISK: if the caller's string_view exceeds PATH_MAX-1, we truncate
    // and return InvalidArg. Silent truncation would open the wrong file.
    struct PathSlot { char buf[PATH_MAX]; };
    auto* paths = new (std::nothrow) PathSlot[reqs.size()];
    if (!paths)
        return Error::NoMem;

    for (size_t i = 0; i < reqs.size(); ++i) {
        const auto& req = reqs[i];
        if (req.base_dir_fd < 0 || req.rel_path.empty() || req.out.empty()) {
            delete[] paths;
            return Error::InvalidArg;
        }
        if (req.rel_path.size() >= sizeof(paths[i].buf)) {
            delete[] paths;
            return Error::InvalidArg;
        }
        std::memcpy(paths[i].buf, req.rel_path.data(), req.rel_path.size());
        paths[i].buf[req.rel_path.size()] = '\0';

        // Reset per-batch results so a re-used ReadRequest array doesn't
        // surface stale values for requests that get canceled mid-chain.
        reqs[i].bytes_read = -1;
        reqs[i].errno_val  = 0;
    }

    // Prep three linked SQEs per request.
    for (uint32_t i = 0; i < reqs.size(); ++i) {
        auto& req = reqs[i];

        io_uring_sqe* sqe_open = io_uring_get_sqe(ring_);
        // BUG RISK: get_sqe returns null if the SQ is full. We bounded reqs
        // to max_batch_size() so this shouldn't happen; if it does, the ring
        // is in a bad state and we bail.
        if (!sqe_open) {
            delete[] paths;
            return Error::IO;
        }
        compat::prep_openat_direct(sqe_open,
                                   req.base_dir_fd,
                                   paths[i].buf,
                                   O_RDONLY | O_CLOEXEC,
                                   0,
                                   i /* fixed-file slot */);
        sqe_open->flags |= IOSQE_IO_LINK;
        compat::sqe_set_data64(sqe_open, encode_tag(i, OP_OPENAT));

        io_uring_sqe* sqe_read = io_uring_get_sqe(ring_);
        if (!sqe_read) {
            delete[] paths;
            return Error::IO;
        }
        io_uring_prep_read(sqe_read,
                           static_cast<int>(i), // slot index as fd
                           req.out.data(),
                           static_cast<unsigned>(req.out.size()),
                           req.offset);
        sqe_read->flags |= IOSQE_FIXED_FILE | IOSQE_IO_HARDLINK;
        compat::sqe_set_data64(sqe_read, encode_tag(i, OP_READ));

        io_uring_sqe* sqe_close = io_uring_get_sqe(ring_);
        if (!sqe_close) {
            delete[] paths;
            return Error::IO;
        }
        compat::prep_close_direct(sqe_close, i);
        compat::sqe_set_data64(sqe_close, encode_tag(i, OP_CLOSE));
        // No link flag on close — it's the chain tail.
    }

    const unsigned expected_cqes = static_cast<unsigned>(reqs.size() * 3);

    // Submit and wait for the first CQE in one syscall. Drain the rest
    // non-blocking afterward.
    int submitted = io_uring_submit_and_wait(ring_, 1);
    if (submitted < 0) {
        delete[] paths;
        return Error::IO;
    }

    unsigned seen = 0;
    while (seen < expected_cqes) {
        io_uring_cqe* cqe = nullptr;
        int rc = io_uring_wait_cqe(ring_, &cqe);
        if (rc == -EINTR) continue; // retry on signal; io_uring_submit handles restart internally too
        if (rc < 0 || cqe == nullptr) {
            delete[] paths;
            return Error::IO;
        }

        uint64_t tag   = cqe->user_data;
        int32_t  res   = cqe->res;
        uint32_t ridx  = decode_req(tag);
        Op       op    = decode_op(tag);

        if (ridx >= reqs.size()) {
            // Defensive: garbage user_data should never happen, but if it
            // does, don't index OOB. Skip.
            io_uring_cqe_seen(ring_, cqe);
            ++seen;
            continue;
        }
        auto& req = reqs[ridx];

        switch (op) {
        case OP_OPENAT:
            if (res < 0) {
                // Chain will cancel read and close with -ECANCELED. Record
                // the openat errno here; don't let a later -ECANCELED CQE
                // overwrite it.
                req.errno_val = -res;
                req.bytes_read = -1;
            }
            break;
        case OP_READ:
            if (res >= 0) {
                req.bytes_read = res; // may be < out.size() — short read
                // Only clear errno_val if openat didn't already fail.
                if (req.errno_val == 0) req.errno_val = 0;
            } else if (res == -ECANCELED) {
                // Openat must have failed upstream. errno_val already set.
                // Leave bytes_read = -1.
            } else {
                // Read error on a successfully-opened file.
                if (req.errno_val == 0) req.errno_val = -res;
                req.bytes_read = -1;
            }
            break;
        case OP_CLOSE:
            // Close failures are observable but not actionable here — the
            // fd slot has been released by the kernel either way.
            // We deliberately do NOT overwrite req.errno_val with close
            // errors; read/open errors are more useful to propagate.
            (void)res;
            break;
        }

        io_uring_cqe_seen(ring_, cqe);
        ++seen;
    }

    delete[] paths;
    return Error::Ok;
}

Error IoUringEngine::read_at(int fd, std::span<uint8_t> buf,
                              uint64_t offset, ssize_t& out_bytes_read) {
    out_bytes_read = -EIO;
    if (!ring_ || fd < 0 || buf.empty())
        return Error::InvalidArg;

    io_uring_sqe* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return Error::IO;

    io_uring_prep_read(sqe, fd, buf.data(),
                       static_cast<unsigned>(buf.size()), offset);
    compat::sqe_set_data64(sqe, 0);

    int submitted = io_uring_submit_and_wait(ring_, 1);
    if (submitted < 0) return Error::IO;

    io_uring_cqe* cqe = nullptr;
    for (;;) {
        int rc = io_uring_wait_cqe(ring_, &cqe);
        if (rc == -EINTR) continue;
        if (rc < 0 || !cqe) return Error::IO;
        break;
    }

    out_bytes_read = cqe->res;
    io_uring_cqe_seen(ring_, cqe);
    // Per-op errors surface via out_bytes_read (negative errno). The
    // function-level Error stays Ok as long as the ring itself didn't blow up.
    return Error::Ok;
}

// Thread-local ring accessor
namespace {

struct LocalRingHolder {
    IoUringEngine engine;
    bool          tried  = false;
    bool          usable = false;
};

thread_local LocalRingHolder t_holder;

} // namespace

IoUringEngine* thread_local_ring() {
    if (t_holder.tried)
        return t_holder.usable ? &t_holder.engine : nullptr;

    t_holder.tried = true;
    auto [err, eng] = IoUringEngine::create();
    if (err != Error::Ok)
        return nullptr;

    t_holder.engine = std::move(eng);
    t_holder.usable = true;
    return &t_holder.engine;
}

} // namespace compressfs