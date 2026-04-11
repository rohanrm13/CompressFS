#pragma once

#include "compressfs/metadata.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace compressfs {

// RAII wrapper for POSIX file descriptors.
// Why a class instead of using raw int: complex functions have multiple error
// paths - a missed close() leaks an fd. This makes fd lifetime automatic.
class ScopedFd {
public:
    ScopedFd() = default;
    explicit ScopedFd(int fd) : fd_(fd) {}
    ~ScopedFd();

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept;
    ScopedFd& operator=(ScopedFd&& other) noexcept;

    [[nodiscard]] int get() const { return fd_; }
    [[nodiscard]] bool valid() const { return fd_ >= 0; }
    int release();
    void reset(int fd = -1);

private:
    int fd_ = -1;
};

// Backing store: abstracts host filesystem I/O for the compressed FUSE fs.
//
// Maps each virtual file to a directory:
//   <base_dir>/<file_path>/meta.bin - serialized Metadata
//   <base_dir>/<file_path>/blocks/ - 0000.blk, 0001.blk, ...
//
// Why directory-per-file: individual block replacement without touching others.
// A packed file would require offset indexing and makes partial updates expensive.
class BackingStore {
public:
    // Factory - creates/opens the backing store at base_dir.
    // Creates the directory if it doesn't exist.
    [[nodiscard]] static std::pair<Error, BackingStore> open(std::string_view base_dir);

    BackingStore() = default;
    ~BackingStore() = default;

    BackingStore(const BackingStore&) = delete;
    BackingStore& operator=(const BackingStore&) = delete;
    BackingStore(BackingStore&&) noexcept = default;
    BackingStore& operator=(BackingStore&&) noexcept = default;

    // Write a single block. Uses write-to-tmp + fsync + rename for atomicity.
    [[nodiscard]] Error write_block(std::string_view file_path,
                                     uint32_t block_index,
                                     std::span<const uint8_t> data);

    // Read a single block into out. Clears and resizes out to match block size.
    [[nodiscard]] Error read_block(std::string_view file_path,
                                    uint32_t block_index,
                                    std::vector<uint8_t>& out);

    // Write file metadata. Same atomic rename pattern as write_block.
    // CALLER CONTRACT: write all blocks FIRST, then call this.
    // The backing store does NOT enforce ordering.
    [[nodiscard]] Error write_metadata(std::string_view file_path,
                                        const Metadata& meta);

    // Read file metadata. Validates checksums.
    [[nodiscard]] Error read_metadata(std::string_view file_path,
                                       Metadata& out);

    // Remove a file's directory and all its blocks + metadata.
    [[nodiscard]] Error delete_file(std::string_view file_path);

    // List all files in the backing store.
    [[nodiscard]] Error list_files(std::vector<std::string>& out);

    // Delete a single block file. Used by truncate when shrinking.
    [[nodiscard]] Error delete_block(std::string_view file_path,
                                      uint32_t block_index);

    // Rename a file's backing directory. Used by FUSE rename callback.
    [[nodiscard]] Error rename_file(std::string_view old_path,
                                     std::string_view new_path);

    [[nodiscard]] bool is_open() const { return base_dir_fd_.valid(); }

private:
    std::string base_dir_;
    ScopedFd    base_dir_fd_;
};

} // namespace compressfs
