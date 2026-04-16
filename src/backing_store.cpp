#include "compressfs/backing_store.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <limits.h> // PATH_MAX

namespace compressfs {

// ScopedFd implementation
ScopedFd::~ScopedFd() {
    if (fd_ >= 0)
        ::close(fd_);
}

ScopedFd::ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

ScopedFd& ScopedFd::operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
        reset(other.fd_);
        other.fd_ = -1;
    }
    return *this;
}

int ScopedFd::release() {
    int fd = fd_;
    fd_ = -1;
    return fd;
}

void ScopedFd::reset(int fd) {
    if (fd_ >= 0)
        ::close(fd_);
    fd_ = fd;
}

// Internal helpers
namespace {

Error build_block_path(std::array<char, PATH_MAX>& buf,
                       std::string_view file_path,
                       uint32_t block_index) {
    int n = std::snprintf(buf.data(), buf.size(),
                          "%.*s/blocks/%04u.blk",
                          static_cast<int>(file_path.size()), file_path.data(),
                          block_index);
    if (n < 0 || static_cast<size_t>(n) >= buf.size())
        return Error::Overflow;
    return Error::Ok;
}

Error build_block_tmp_path(std::array<char, PATH_MAX>& buf,
                           std::string_view file_path,
                           uint32_t block_index) {
    int n = std::snprintf(buf.data(), buf.size(),
                          "%.*s/blocks/%04u.blk.tmp",
                          static_cast<int>(file_path.size()), file_path.data(),
                          block_index);
    if (n < 0 || static_cast<size_t>(n) >= buf.size())
        return Error::Overflow;
    return Error::Ok;
}

Error build_meta_path(std::array<char, PATH_MAX>& buf,
                      std::string_view file_path) {
    int n = std::snprintf(buf.data(), buf.size(),
                          "%.*s/meta.bin",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= buf.size())
        return Error::Overflow;
    return Error::Ok;
}

Error build_meta_tmp_path(std::array<char, PATH_MAX>& buf,
                          std::string_view file_path) {
    int n = std::snprintf(buf.data(), buf.size(),
                          "%.*s/meta.bin.tmp",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= buf.size())
        return Error::Overflow;
    return Error::Ok;
}

Error build_blocks_dir_path(std::array<char, PATH_MAX>& buf,
                            std::string_view file_path) {
    int n = std::snprintf(buf.data(), buf.size(),
                          "%.*s/blocks",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= buf.size())
        return Error::Overflow;
    return Error::Ok;
}

// mkdirat wrapper that tolerates EEXIST.
Error ensure_dir_at(int base_fd, const char* relpath, mode_t mode = 0755) {
    if (::mkdirat(base_fd, relpath, mode) == 0)
        return Error::Ok;
    if (errno == EEXIST)
        return Error::Ok;
    return Error::IO;
}

// Write entire buffer to fd, retrying on short writes and EINTR.
Error write_full(int fd, std::span<const uint8_t> data) {
    size_t written = 0;
    while (written < data.size()) {
        ssize_t n = ::write(fd,
                            data.data() + written,
                            data.size() - written);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ENOSPC)
                return Error::NoSpace;
            return Error::IO;
        }
        written += static_cast<size_t>(n);
    }
    return Error::Ok;
}

// Read entire contents of an open fd into a vector.
// Uses fstat to pre-allocate - avoids repeated reallocs.
std::pair<Error, std::vector<uint8_t>> read_file(int fd) {
    struct stat st;
    if (::fstat(fd, &st) < 0)
        return {Error::IO, {}};

    // BUG RISK: st_size could be 0 for special files. For regular files
    // in our backing store, 0 means actually empty (or corrupt).
    if (st.st_size == 0)
        return {Error::Ok, {}};

    auto size = static_cast<size_t>(st.st_size);
    std::vector<uint8_t> buf(size);

    size_t total = 0;
    while (total < size) {
        ssize_t n = ::read(fd, buf.data() + total, size - total);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return {Error::IO, {}};
        }
        if (n == 0)
            break; // EOF before expected - file truncated underneath us
        total += static_cast<size_t>(n);
    }
    buf.resize(total);
    return {Error::Ok, std::move(buf)};
}

// Atomic write: write to .tmp, fsync, rename over final path, fsync parent dir.
Error atomic_write_at(int base_fd,
                      const char* tmp_path,
                      const char* final_path,
                      const char* parent_dir_path,
                      std::span<const uint8_t> data) {
    // Open temp file
    ScopedFd fd(::openat(base_fd, tmp_path,
                         O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
    if (!fd.valid())
        return (errno == ENOSPC) ? Error::NoSpace : Error::IO;

    // Write data
    Error err = write_full(fd.get(), data);
    if (err != Error::Ok) {
        ::unlinkat(base_fd, tmp_path, 0);
        return err;
    }

    // fsync data before rename
    if (::fsync(fd.get()) < 0) {
        ::unlinkat(base_fd, tmp_path, 0);
        return Error::IO;
    }
    fd.reset(); // close before rename

    // Atomic rename
    if (::renameat(base_fd, tmp_path, base_fd, final_path) < 0) {
        ::unlinkat(base_fd, tmp_path, 0);
        return Error::IO;
    }

    // fsync parent directory to make rename durable
    ScopedFd dir_fd(::openat(base_fd, parent_dir_path,
                             O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (dir_fd.valid())
        ::fsync(dir_fd.get());
    // BUG RISK: if parent dir fsync fails, rename happened but isn't durable.
    // On crash, we might lose the rename. This is acceptable - the old block
    // is still valid, and a retry will succeed.

    return Error::Ok;
}

} // anonymous namespace

// BackingStore implementation
std::pair<Error, BackingStore> BackingStore::open(std::string_view base_dir) {
    BackingStore bs;

    if (base_dir.empty())
        return {Error::InvalidArg, std::move(bs)};

    bs.base_dir_ = std::string(base_dir);

    // Create base directory if it doesn't exist
    if (::mkdir(bs.base_dir_.c_str(), 0755) < 0 && errno != EEXIST)
        return {Error::IO, std::move(bs)};

    int fd = ::open(bs.base_dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        return {Error::IO, std::move(bs)};

    bs.base_dir_fd_.reset(fd);
    return {Error::Ok, std::move(bs)};
}

Error BackingStore::write_block(std::string_view file_path,
                                 uint32_t block_index,
                                 std::span<const uint8_t> data) {
    if (file_path.empty() || data.empty())
        return Error::InvalidArg;
    if (block_index >= kMaxBlocks)
        return Error::Overflow;

    // Ensure file directory and blocks subdirectory exist
    std::array<char, PATH_MAX> dir_buf{};
    int n = std::snprintf(dir_buf.data(), dir_buf.size(),
                          "%.*s",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= dir_buf.size())
        return Error::Overflow;

    Error err = ensure_dir_at(base_dir_fd_.get(), dir_buf.data());
    if (err != Error::Ok) return err;

    std::array<char, PATH_MAX> blocks_dir{};
    err = build_blocks_dir_path(blocks_dir, file_path);
    if (err != Error::Ok) return err;

    err = ensure_dir_at(base_dir_fd_.get(), blocks_dir.data());
    if (err != Error::Ok) return err;

    std::array<char, PATH_MAX> tmp_path{}, final_path{};
    err = build_block_tmp_path(tmp_path, file_path, block_index);
    if (err != Error::Ok) return err;
    err = build_block_path(final_path, file_path, block_index);
    if (err != Error::Ok) return err;

    return atomic_write_at(base_dir_fd_.get(),
                           tmp_path.data(), final_path.data(),
                           blocks_dir.data(), data);
}

Error BackingStore::read_block(std::string_view file_path,
                                uint32_t block_index,
                                std::vector<uint8_t>& out) {
    out.clear();

    if (file_path.empty())
        return Error::InvalidArg;

    std::array<char, PATH_MAX> path{};
    Error err = build_block_path(path, file_path, block_index);
    if (err != Error::Ok) return err;

    ScopedFd fd(::openat(base_dir_fd_.get(), path.data(),
                         O_RDONLY | O_CLOEXEC));
    if (!fd.valid())
        return (errno == ENOENT) ? Error::NotFound : Error::IO;

    auto [read_err, data] = read_file(fd.get());
    if (read_err != Error::Ok)
        return read_err;

    out = std::move(data);
    return Error::Ok;
}

Error BackingStore::write_metadata(std::string_view file_path,
                                    const Metadata& meta) {
    if (file_path.empty())
        return Error::InvalidArg;

    auto [ser_err, buf] = metadata_serialize(meta);
    if (ser_err != Error::Ok)
        return ser_err;

    // Ensure file directory exists
    std::array<char, PATH_MAX> dir_buf{};
    int n = std::snprintf(dir_buf.data(), dir_buf.size(),
                          "%.*s",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= dir_buf.size())
        return Error::Overflow;

    Error err = ensure_dir_at(base_dir_fd_.get(), dir_buf.data());
    if (err != Error::Ok) return err;

    std::array<char, PATH_MAX> tmp_path{}, final_path{};
    err = build_meta_tmp_path(tmp_path, file_path);
    if (err != Error::Ok) return err;
    err = build_meta_path(final_path, file_path);
    if (err != Error::Ok) return err;

    return atomic_write_at(base_dir_fd_.get(),
                           tmp_path.data(), final_path.data(),
                           dir_buf.data(),
                           std::span<const uint8_t>(buf));
}

Error BackingStore::read_metadata(std::string_view file_path,
                                   Metadata& out) {
    if (file_path.empty())
        return Error::InvalidArg;

    std::array<char, PATH_MAX> path{};
    Error err = build_meta_path(path, file_path);
    if (err != Error::Ok) return err;

    ScopedFd fd(::openat(base_dir_fd_.get(), path.data(),
                         O_RDONLY | O_CLOEXEC));
    if (!fd.valid())
        return (errno == ENOENT) ? Error::NotFound : Error::IO;

    auto [read_err, buf] = read_file(fd.get());
    if (read_err != Error::Ok)
        return read_err;

    auto [de_err, meta] = metadata_deserialize(buf);
    if (de_err != Error::Ok)
        return de_err;

    out = std::move(meta);
    return Error::Ok;
}

Error BackingStore::delete_file(std::string_view file_path) {
    if (file_path.empty())
        return Error::InvalidArg;

    int base_fd = base_dir_fd_.get();

    // Build file directory path
    std::array<char, PATH_MAX> file_dir{};
    int n = std::snprintf(file_dir.data(), file_dir.size(),
                          "%.*s",
                          static_cast<int>(file_path.size()), file_path.data());
    if (n < 0 || static_cast<size_t>(n) >= file_dir.size())
        return Error::Overflow;

    // Open blocks/ subdirectory and remove all block files
    std::array<char, PATH_MAX> blocks_dir{};
    Error err = build_blocks_dir_path(blocks_dir, file_path);
    if (err != Error::Ok) return err;

    ScopedFd blocks_fd(::openat(base_fd, blocks_dir.data(),
                                O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (blocks_fd.valid()) {
        // dup because closedir closes the fd
        int dup_fd = ::dup(blocks_fd.get());
        if (dup_fd >= 0) {
            DIR* dir = ::fdopendir(dup_fd);
            if (dir) {
                struct dirent* ent;
                while ((ent = ::readdir(dir)) != nullptr) {
                    if (ent->d_name[0] == '.' &&
                        (ent->d_name[1] == '\0' ||
                         (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
                        continue;
                    ::unlinkat(blocks_fd.get(), ent->d_name, 0);
                }
                ::closedir(dir); // closes dup_fd
            } else {
                ::close(dup_fd);
            }
        }
        blocks_fd.reset();
    }

    // Remove blocks/ directory
    ::unlinkat(base_fd, blocks_dir.data(), AT_REMOVEDIR);

    // Remove meta.bin and meta.bin.tmp
    std::array<char, PATH_MAX> meta_path{}, meta_tmp{};
    build_meta_path(meta_path, file_path);
    build_meta_tmp_path(meta_tmp, file_path);
    ::unlinkat(base_fd, meta_path.data(), 0);
    ::unlinkat(base_fd, meta_tmp.data(), 0);

    // Remove file directory
    if (::unlinkat(base_fd, file_dir.data(), AT_REMOVEDIR) < 0) {
        if (errno == ENOENT)
            return Error::NotFound;
        // ENOTEMPTY means something unexpected is in there
        return Error::IO;
    }

    // fsync base dir to make removal durable
    ::fsync(base_fd);

    return Error::Ok;
}

Error BackingStore::list_files(std::vector<std::string>& out) {
    out.clear();

    // dup because closedir will close the fd
    int dup_fd = ::dup(base_dir_fd_.get());
    if (dup_fd < 0)
        return Error::IO;

    DIR* dir = ::fdopendir(dup_fd);
    if (!dir) {
        ::close(dup_fd);
        return Error::IO;
    }

    struct dirent* ent;
    while ((ent = ::readdir(dir)) != nullptr) {
        // Skip . and ..
        if (ent->d_name[0] == '.' &&
            (ent->d_name[1] == '\0' ||
             (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
            continue;

        // Only include directories (each file maps to a directory)
        if (ent->d_type == DT_DIR) {
            out.emplace_back(ent->d_name);
        } else if (ent->d_type == DT_UNKNOWN) {
            // BUG RISK: some filesystems don't fill d_type (e.g., XFS without
            // ftype=1). Fall back to fstatat.
            struct stat st;
            if (::fstatat(base_dir_fd_.get(), ent->d_name, &st, 0) == 0 &&
                S_ISDIR(st.st_mode)) {
                out.emplace_back(ent->d_name);
            }
        }
    }

    ::closedir(dir); // closes dup_fd
    return Error::Ok;
}

Error BackingStore::delete_block(std::string_view file_path,
                                  uint32_t block_index) {
    if (file_path.empty())
        return Error::InvalidArg;

    std::array<char, PATH_MAX> path{};
    Error err = build_block_path(path, file_path, block_index);
    if (err != Error::Ok) return err;

    if (::unlinkat(base_dir_fd_.get(), path.data(), 0) < 0) {
        if (errno == ENOENT)
            return Error::NotFound;
        return Error::IO;
    }
    return Error::Ok;
}

Error BackingStore::rename_file(std::string_view old_path,
                                 std::string_view new_path) {
    if (old_path.empty() || new_path.empty())
        return Error::InvalidArg;

    std::array<char, PATH_MAX> old_buf{}, new_buf{};
    int n = std::snprintf(old_buf.data(), old_buf.size(),
                          "%.*s",
                          static_cast<int>(old_path.size()), old_path.data());
    if (n < 0 || static_cast<size_t>(n) >= old_buf.size())
        return Error::Overflow;

    n = std::snprintf(new_buf.data(), new_buf.size(),
                      "%.*s",
                      static_cast<int>(new_path.size()), new_path.data());
    if (n < 0 || static_cast<size_t>(n) >= new_buf.size())
        return Error::Overflow;

    if (::renameat(base_dir_fd_.get(), old_buf.data(),
                   base_dir_fd_.get(), new_buf.data()) < 0) {
        if (errno == ENOENT)
            return Error::NotFound;
        return Error::IO;
    }

    // fsync base dir to make rename durable
    ::fsync(base_dir_fd_.get());
    return Error::Ok;
}

int BackingStore::cleanup_orphaned_tmp_files() {
    int base_fd = base_dir_fd_.get();

    // Scan top-level entries (each is a file directory)
    int dup_fd = ::dup(base_fd);
    if (dup_fd < 0)
        return -1;

    DIR* top_dir = ::fdopendir(dup_fd);
    if (!top_dir) {
        ::close(dup_fd);
        return -1;
    }

    int removed = 0;
    struct dirent* file_ent;
    while ((file_ent = ::readdir(top_dir)) != nullptr) {
        if (file_ent->d_name[0] == '.' &&
            (file_ent->d_name[1] == '\0' ||
             (file_ent->d_name[1] == '.' && file_ent->d_name[2] == '\0')))
            continue;

        // Only process directories (each virtual file maps to a directory)
        bool is_dir = false;
        if (file_ent->d_type == DT_DIR) {
            is_dir = true;
        } else if (file_ent->d_type == DT_UNKNOWN) {
            struct stat st;
            if (::fstatat(base_fd, file_ent->d_name, &st, 0) == 0 &&
                S_ISDIR(st.st_mode))
                is_dir = true;
        }
        if (!is_dir)
            continue;

        // Open the file directory
        ScopedFd file_dir_fd(::openat(base_fd, file_ent->d_name,
                                       O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        if (!file_dir_fd.valid())
            continue;

        // Remove meta.bin.tmp if it exists
        if (::unlinkat(file_dir_fd.get(), "meta.bin.tmp", 0) == 0)
            ++removed;

        // Scan blocks/ subdirectory for *.blk.tmp
        ScopedFd blocks_fd(::openat(file_dir_fd.get(), "blocks",
                                     O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        if (!blocks_fd.valid())
            continue;

        int blocks_dup = ::dup(blocks_fd.get());
        if (blocks_dup < 0)
            continue;

        DIR* blk_dir = ::fdopendir(blocks_dup);
        if (!blk_dir) {
            ::close(blocks_dup);
            continue;
        }

        struct dirent* blk_ent;
        while ((blk_ent = ::readdir(blk_dir)) != nullptr) {
            size_t len = std::strlen(blk_ent->d_name);
            if (len > 4 && std::strcmp(blk_ent->d_name + len - 4, ".tmp") == 0) {
                if (::unlinkat(blocks_fd.get(), blk_ent->d_name, 0) == 0)
                    ++removed;
            }
        }
        ::closedir(blk_dir);
    }

    ::closedir(top_dir);
    return removed;
}

} // namespace compressfs
