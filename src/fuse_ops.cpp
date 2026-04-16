#define FUSE_USE_VERSION 31

#include "compressfs/fuse_ops.hpp"
#include "compressfs/backing_store.hpp"
#include "compressfs/block_cache.hpp"
#include "compressfs/block_manager.hpp"
#include "compressfs/stats.hpp"

#include <fuse3/fuse.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>

namespace compressfs {
namespace {

// Internal state

// Per-filesystem state. Passed to every FUSE callback via private_data.
struct FsState {
    BackingStore store;
    BlockCache* cache = nullptr;
    BlockManager* block_mgr = nullptr;
    FsStats stats;
    uint32_t block_size;
    uid_t mount_uid;
    gid_t mount_gid;
};

static constexpr const char* kStatsFileName = ".compressfs_stats";
static constexpr uint64_t kStatsHandleSentinel = UINT64_MAX;

static bool is_stats_path(const char* path) {
    return std::strcmp(path, "/.compressfs_stats") == 0;
}

// Per-open-file state. Allocated on open()/create(), freed on release().
// Stored in fi->fh as a reinterpret_cast<uint64_t>.
struct FileHandle {
    Metadata meta;
    std::string backing_path;
    bool meta_dirty = false;
};

// Helpers
static FsState* get_state() {
    return static_cast<FsState*>(fuse_get_context()->private_data);
}

static FileHandle* get_handle(struct fuse_file_info* fi) {
    return reinterpret_cast<FileHandle*>(fi->fh);
}

static int err_to_errno(Error e) {
    switch (e) {
        case Error::Ok: return 0;
        case Error::IO: return -EIO;
        case Error::NoMem: return -ENOMEM;
        case Error::NoSpace: return -ENOSPC;
        case Error::NotFound: return -ENOENT;
        case Error::Corrupt: return -EIO;
        case Error::Overflow: return -ENAMETOOLONG;
        case Error::InvalidArg: return -EINVAL;
    }
    return -EIO;
}

// "/foo.txt" -> "foo.txt", "/" -> ""
static std::string_view strip_slash(const char* path) {
    if (path[0] == '/')
        ++path;
    return path;
}

static struct timespec now_ts() {
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return ts;
}

static void set_time_now(Metadata& meta) {
    auto ts = now_ts();
    meta.mtime_sec = ts.tv_sec;
    meta.mtime_nsec = static_cast<uint32_t>(ts.tv_nsec);
    meta.ctime_sec = ts.tv_sec;
    meta.ctime_nsec = static_cast<uint32_t>(ts.tv_nsec);
}

static void set_ctime_now(Metadata& meta) {
    auto ts = now_ts();
    meta.ctime_sec = ts.tv_sec;
    meta.ctime_nsec = static_cast<uint32_t>(ts.tv_nsec);
}

static void meta_to_stat(const Metadata& meta, struct stat& st,
                          uint32_t block_size) {
    std::memset(&st, 0, sizeof(st));
    st.st_mode = meta.mode;
    st.st_nlink = 1;
    st.st_uid = meta.uid;
    st.st_gid = meta.gid;
    st.st_size = static_cast<off_t>(meta.original_size);
    // Why 512: st_blocks is always in 512-byte units per POSIX, regardless of
    // the actual block size. This is what du(1) uses.
    st.st_blocks = static_cast<blkcnt_t>((meta.original_size + 511) / 512);
    st.st_blksize = static_cast<blksize_t>(block_size);
    st.st_atim.tv_sec = meta.atime_sec;
    st.st_atim.tv_nsec = meta.atime_nsec;
    st.st_mtim.tv_sec = meta.mtime_sec;
    st.st_mtim.tv_nsec = meta.mtime_nsec;
    st.st_ctim.tv_sec = meta.ctime_sec;
    st.st_ctim.tv_nsec = meta.ctime_nsec;
}

// Load metadata: prefer FileHandle if available, otherwise read from store.
// Returns metadata by reference and Error.
static Error load_meta(FsState* state, const char* path,
                       struct fuse_file_info* fi, Metadata& out) {
    if (fi && fi->fh) {
        out = get_handle(fi)->meta;
        return Error::Ok;
    }
    auto sv = strip_slash(path);
    if (sv.empty())
        return Error::InvalidArg;
    return state->store.read_metadata(sv, out);
}

// Save metadata: if FileHandle available, update cached and mark dirty.
// Otherwise write directly to store.
static Error save_meta(FsState* state, const char* path,
                       struct fuse_file_info* fi, const Metadata& meta) {
    if (fi && fi->fh) {
        auto* h = get_handle(fi);
        h->meta = meta;
        h->meta_dirty = true;
        return Error::Ok;
    }
    auto sv = strip_slash(path);
    return state->store.write_metadata(sv, meta);
}

// FUSE callbacks
static void* cfs_init(struct fuse_conn_info* /*conn*/,
                       struct fuse_config* cfg) {
    // Why use_ino = 0: we don't manage inode numbers; let FUSE assign them.
    cfg->use_ino = 0;
    // Why no kernel_cache: with direct_io per-file we don't need global cache settings.
    return get_state();
}

static void cfs_destroy(void* private_data) {
    // BackingStore destructor handles fd cleanup. Nothing explicit needed.
}

static int cfs_getattr(const char* path, struct stat* stbuf,
                        struct fuse_file_info* fi) {
    auto* state = get_state();
    std::memset(stbuf, 0, sizeof(*stbuf));

    auto sv = strip_slash(path);

    // Root directory: synthesized, not stored in backing store
    if (sv.empty()) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_uid = state->mount_uid;
        stbuf->st_gid = state->mount_gid;
        auto ts = now_ts();
        stbuf->st_atim.tv_sec = ts.tv_sec;
        stbuf->st_mtim.tv_sec = ts.tv_sec;
        stbuf->st_ctim.tv_sec = ts.tv_sec;
        return 0;
    }

    // Virtual stats file: read-only, synthesized on demand
    if (is_stats_path(path)) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_uid = state->mount_uid;
        stbuf->st_gid = state->mount_gid;
        stbuf->st_size = 0; // size unknown until read (direct_io)
        return 0;
    }

    Metadata meta;
    Error err = load_meta(state, path, fi, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    meta_to_stat(meta, *stbuf, state->block_size);
    return 0;
}

static int cfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                        off_t offset, struct fuse_file_info* fi,
                        enum fuse_readdir_flags flags) {
    auto sv = strip_slash(path);
    if (!sv.empty())
        return -ENOENT; // only root is a directory

    filler(buf, ".", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buf, "..", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));

    auto* state = get_state();
    std::vector<std::string> files;
    Error err = state->store.list_files(files);
    if (err != Error::Ok)
        return err_to_errno(err);

    for (const auto& name : files) {
        filler(buf, name.c_str(), nullptr, 0,
               static_cast<fuse_fill_dir_flags>(0));
    }

    // Virtual stats file
    filler(buf, kStatsFileName, nullptr, 0,
           static_cast<fuse_fill_dir_flags>(0));

    return 0;
}

static int cfs_create(const char* path, mode_t mode,
                       struct fuse_file_info* fi) {
    auto* state = get_state();
    auto sv = strip_slash(path);
    if (sv.empty())
        return -EINVAL;

    auto* ctx = fuse_get_context();
    auto ts = now_ts();

    Metadata meta;
    meta.mode = static_cast<uint32_t>(mode) | S_IFREG;
    meta.uid = ctx->uid;
    meta.gid = ctx->gid;
    meta.block_size = state->block_size;
    meta.block_count = 0;
    meta.original_size = 0;
    meta.atime_sec = ts.tv_sec;
    meta.atime_nsec = static_cast<uint32_t>(ts.tv_nsec);
    meta.mtime_sec = ts.tv_sec;
    meta.mtime_nsec = static_cast<uint32_t>(ts.tv_nsec);
    meta.ctime_sec = ts.tv_sec;
    meta.ctime_nsec = static_cast<uint32_t>(ts.tv_nsec);

    Error err = state->store.write_metadata(sv, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    auto* handle = new FileHandle{std::move(meta), std::string(sv), false};
    fi->fh = reinterpret_cast<uint64_t>(handle);
    fi->direct_io = 1;
    return 0;
}

static int cfs_open(const char* path, struct fuse_file_info* fi) {
    // Virtual stats file: sentinel handle, no metadata
    if (is_stats_path(path)) {
        fi->fh = kStatsHandleSentinel;
        fi->direct_io = 1;
        return 0;
    }

    auto* state = get_state();
    auto sv = strip_slash(path);
    if (sv.empty())
        return -EISDIR;

    Metadata meta;
    Error err = state->store.read_metadata(sv, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    auto* handle = new FileHandle{std::move(meta), std::string(sv), false};
    fi->fh = reinterpret_cast<uint64_t>(handle);
    fi->direct_io = 1;
    return 0;
}

static int cfs_release(const char* path, struct fuse_file_info* fi) {
    if (fi->fh == kStatsHandleSentinel) {
        fi->fh = 0;
        return 0;
    }

    auto* handle = get_handle(fi);
    if (!handle)
        return 0;

    if (handle->meta_dirty) {
        auto* state = get_state();
        // BUG RISK: write_metadata could fail here. FUSE ignores release
        // return value, so we can't propagate the error to the caller.
        // Best-effort flush; data blocks are already on disk (write-through).
        (void)state->store.write_metadata(handle->backing_path, handle->meta);
    }

    delete handle;
    fi->fh = 0;
    return 0;
}

static int cfs_read(const char* path, char* buf, size_t size,
                     off_t offset, struct fuse_file_info* fi) {
    // Virtual stats file: format and serve on demand
    if (fi->fh == kStatsHandleSentinel) {
        auto* state = get_state();
        std::string text = format_stats(state->stats);
        if (offset < 0 || static_cast<size_t>(offset) >= text.size())
            return 0;
        size_t available = text.size() - static_cast<size_t>(offset);
        size_t to_copy = std::min(size, available);
        std::memcpy(buf, text.data() + offset, to_copy);
        return static_cast<int>(to_copy);
    }

    auto* state = get_state();
    auto* handle = get_handle(fi);
    if (!handle)
        return -EBADF;
    if (offset < 0)
        return -EINVAL;

    return state->block_mgr->read(
        handle->backing_path, handle->meta,
        buf, size, static_cast<uint64_t>(offset));
}

static int cfs_write(const char* path, const char* buf, size_t size,
                      off_t offset, struct fuse_file_info* fi) {
    if (fi->fh == kStatsHandleSentinel)
        return -EACCES;

    auto* state = get_state();
    auto* handle = get_handle(fi);
    if (!handle)
        return -EBADF;
    if (offset < 0)
        return -EINVAL;

    int ret = state->block_mgr->write(
        handle->backing_path, handle->meta,
        buf, size, static_cast<uint64_t>(offset));

    if (ret > 0) {
        set_time_now(handle->meta);
        handle->meta_dirty = true;
    }
    return ret;
}

static int cfs_truncate(const char* path, off_t newsize,
                         struct fuse_file_info* fi) {
    if (is_stats_path(path))
        return -EACCES;
    if (newsize < 0)
        return -EINVAL;

    auto* state = get_state();

    Metadata meta;
    Error err = load_meta(state, path, fi, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    auto sv = strip_slash(path);
    err = state->block_mgr->truncate(
        std::string(sv), meta, static_cast<uint64_t>(newsize));
    if (err != Error::Ok)
        return err_to_errno(err);

    set_time_now(meta);
    return err_to_errno(save_meta(state, path, fi, meta));
}

static int cfs_unlink(const char* path) {
    if (is_stats_path(path))
        return -EACCES;
    auto* state = get_state();
    auto sv = strip_slash(path);
    if (sv.empty())
        return -EISDIR;
    return err_to_errno(state->store.delete_file(sv));
}

static int cfs_rename(const char* from, const char* to,
                       unsigned int flags) {
    // RENAME_EXCHANGE requires both to exist and atomically swap - not supported
    if (flags & RENAME_EXCHANGE)
        return -ENOSYS;

    auto* state = get_state();
    auto sv_from = strip_slash(from);
    auto sv_to   = strip_slash(to);

    if (sv_from.empty() || sv_to.empty())
        return -EINVAL;

    if (!(flags & RENAME_NOREPLACE)) {
        // Default behavior: overwrite destination if it exists
        (void)state->store.delete_file(sv_to);
    }

    Error err = state->store.rename_file(sv_from, sv_to);
    if (err == Error::IO && errno == EEXIST)
        return -EEXIST; // RENAME_NOREPLACE and dest exists
    return err_to_errno(err);
}

static int cfs_chmod(const char* path, mode_t mode,
                      struct fuse_file_info* fi) {
    if (is_stats_path(path)) {
        return -EACCES;
    }
    
    auto* state = get_state();

    Metadata meta;
    Error err = load_meta(state, path, fi, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    // Preserve file type bits, update permission bits
    meta.mode = (meta.mode & static_cast<uint32_t>(S_IFMT)) |
                (static_cast<uint32_t>(mode) & ~static_cast<uint32_t>(S_IFMT));
    set_ctime_now(meta);

    return err_to_errno(save_meta(state, path, fi, meta));
}

static int cfs_chown(const char* path, uid_t uid, gid_t gid,
                      struct fuse_file_info* fi) {
    if (is_stats_path(path)) {
        return -EACCES;
    }

    auto* state = get_state();

    Metadata meta;
    Error err = load_meta(state, path, fi, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    if (uid != static_cast<uid_t>(-1))
        meta.uid = uid;
    if (gid != static_cast<gid_t>(-1))
        meta.gid = gid;
    set_ctime_now(meta);

    return err_to_errno(save_meta(state, path, fi, meta));
}

static int cfs_utimens(const char* path, const struct timespec tv[2],
                        struct fuse_file_info* fi) {
    if (is_stats_path(path)) {
        return -EACCES;
    }

    auto* state = get_state();

    Metadata meta;
    Error err = load_meta(state, path, fi, meta);
    if (err != Error::Ok)
        return err_to_errno(err);

    // tv[0] = atime, tv[1] = mtime
    // Handle UTIME_NOW and UTIME_OMIT per utimensat(2)
    if (tv[0].tv_nsec == UTIME_NOW) {
        auto ts = now_ts();
        meta.atime_sec  = ts.tv_sec;
        meta.atime_nsec = static_cast<uint32_t>(ts.tv_nsec);
    } else if (tv[0].tv_nsec != UTIME_OMIT) {
        meta.atime_sec  = tv[0].tv_sec;
        meta.atime_nsec = static_cast<uint32_t>(tv[0].tv_nsec);
    }

    if (tv[1].tv_nsec == UTIME_NOW) {
        auto ts = now_ts();
        meta.mtime_sec  = ts.tv_sec;
        meta.mtime_nsec = static_cast<uint32_t>(ts.tv_nsec);
    } else if (tv[1].tv_nsec != UTIME_OMIT) {
        meta.mtime_sec  = tv[1].tv_sec;
        meta.mtime_nsec = static_cast<uint32_t>(tv[1].tv_nsec);
    }

    set_ctime_now(meta);

    return err_to_errno(save_meta(state, path, fi, meta));
}

static int cfs_flush(const char* path, struct fuse_file_info* fi) {
    if (fi->fh == kStatsHandleSentinel)
        return 0;

    auto* handle = get_handle(fi);
    if (!handle)
        return 0;

    if (handle->meta_dirty) {
        auto* state = get_state();
        Error err = state->store.write_metadata(
            handle->backing_path, handle->meta);
        if (err != Error::Ok)
            return err_to_errno(err);
        handle->meta_dirty = false;
    }
    return 0;
}

static int cfs_fsync(const char* path, int datasync,
                      struct fuse_file_info* fi) {
    if (fi->fh == kStatsHandleSentinel)
        return 0;

    auto* handle = get_handle(fi);
    if (!handle)
        return -EBADF;

    if (handle->meta_dirty) {
        auto* state = get_state();
        Error err = state->store.write_metadata(
            handle->backing_path, handle->meta);
        if (err != Error::Ok)
            return err_to_errno(err);
        handle->meta_dirty = false;
    }
    return 0;
}

// Per-file compression stats via extended attributes.
// Readable with: getfattr -n user.compressfs.ratio /mountpoint/file.txt
static int cfs_getxattr(const char* path, const char* name,
                         char* value, size_t size) {
    if (is_stats_path(path))
        return -ENODATA;

    auto sv = strip_slash(path);
    if (sv.empty())
        return -ENODATA;

    auto* state = get_state();
    Metadata meta;
    Error err = state->store.read_metadata(sv, meta);
    if (err != Error::Ok)
        return (err == Error::NotFound) ? -ENOENT : -EIO;

    std::string val;
    if (std::strcmp(name, "user.compressfs.blocks") == 0) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%u", meta.block_count);
        val = buf;
    } else if (std::strcmp(name, "user.compressfs.codec") == 0) {
        const CodecBase* codec = get_codec(meta.codec);
        val = codec ? codec->name() : "unknown";
    } else if (std::strcmp(name, "user.compressfs.compressed_size") == 0) {
        uint64_t total = 0;
        for (uint32_t s : meta.compressed_sizes)
            total += s;
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%lu", static_cast<unsigned long>(total));
        val = buf;
    } else if (std::strcmp(name, "user.compressfs.ratio") == 0) {
        uint64_t compressed_total = 0;
        for (uint32_t s : meta.compressed_sizes)
            compressed_total += s;
        if (compressed_total > 0 && meta.original_size > 0) {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.2fx",
                         static_cast<double>(meta.original_size) /
                         static_cast<double>(compressed_total));
            val = buf;
        } else {
            val = "1.00x";
        }
    } else {
        return -ENODATA;
    }

    if (size == 0)
        return static_cast<int>(val.size());
    if (size < val.size())
        return -ERANGE;
    std::memcpy(value, val.data(), val.size());
    return static_cast<int>(val.size());
}

} // anonymous namespace

int compressfs_fuse_main(int argc, char* argv[], const FsConfig& cfg) {
    // Allocate state on heap - FUSE needs it for the lifetime of the mount.
    auto* state = new FsState{};
    state->block_size = cfg.block_size;
    state->mount_uid = ::getuid();
    state->mount_gid = ::getgid();

    auto [err, store] = BackingStore::open(cfg.backing_dir);
    if (err != Error::Ok) {
        std::fprintf(stderr, "compressfs: failed to open backing store at '%s'\n",
                     cfg.backing_dir.c_str());
        delete state;
        return 1;
    }
    state->store = std::move(store);

    int cleaned = state->store.cleanup_orphaned_tmp_files();
    if (cleaned > 0) {
        std::fprintf(stderr, "compressfs: cleaned %d orphaned .tmp file(s) from previous crash\n",
                     cleaned);
    } else if (cleaned < 0) {
        std::fprintf(stderr, "compressfs: warning: failed to scan for orphaned .tmp files\n");
    }

    const CodecBase* codec = get_codec(cfg.codec_id);
    if (!codec) {
        std::fprintf(stderr, "compressfs: unknown codec id %d\n",
                     static_cast<int>(cfg.codec_id));
        delete state;
        return 1;
    }
    // Create block cache if cache_size > 0. The FlushCallback is a no-op for
    // write-through mode - blocks are written to disk immediately in
    // compress_write_block, so cache entries are always clean.
    if (cfg.cache_size > 0) {
        auto flush_fn = [](const std::string&, uint32_t,
                           std::span<const uint8_t>) -> Error {
            // Write-through: blocks already on disk. Nothing to flush.
            return Error::Ok;
        };
        state->cache = new BlockCache(cfg.block_size, cfg.cache_size,
                                      std::move(flush_fn), &state->stats);
    }
    state->block_mgr = new BlockManager(state->store, codec,
                                        cfg.compression_level, state->cache,
                                        &state->stats);

    struct fuse_operations ops{};
    ops.init = cfs_init;
    ops.destroy = cfs_destroy;
    ops.getattr = cfs_getattr;
    ops.readdir = cfs_readdir;
    ops.create = cfs_create;
    ops.open = cfs_open;
    ops.release = cfs_release;
    ops.flush = cfs_flush;
    ops.read = cfs_read;
    ops.write = cfs_write;
    ops.truncate = cfs_truncate;
    ops.unlink = cfs_unlink;
    ops.rename = cfs_rename;
    ops.chmod = cfs_chmod;
    ops.chown = cfs_chown;
    ops.utimens = cfs_utimens;
    ops.fsync = cfs_fsync;
    ops.getxattr = cfs_getxattr;

    int ret = fuse_main(argc, argv, &ops, state);

    delete state->block_mgr;
    delete state->cache;
    delete state;
    return ret;
}

} // namespace compressfs
