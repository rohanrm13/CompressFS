#pragma once

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace compressfs {

// Forward-declared here, defined in backing_store.hpp.
// Duplicated as an enum class to avoid circular includes.
enum class Error : int {
    Ok =  0,
    IO = -1,
    NoMem = -2,
    NoSpace = -3,
    NotFound = -4,
    Corrupt = -5,
    Overflow = -6,
    InvalidArg = -7,
};

enum class Codec : uint16_t {
    None = 0,
    LZ4 = 1,
    Zstd = 2,
};

enum class ChecksumType : uint16_t {
    None = 0,
    CRC32C = 1,
    XXH64 = 2,
};

inline constexpr uint32_t kMetaMagic = 0x43465301; // "CFS\x01"
inline constexpr uint16_t kMetaVersion = 2;
inline constexpr uint32_t kMaxBlocks = 1u << 20;   // ~1M blocks; 64 GiB at 64 KiB/block
inline constexpr uint32_t kDefaultBlockSize = 65536;       // 64 KiB - sweet spot for compression ratio vs latency

inline constexpr uint32_t kMetaHeaderSize = 80;
inline constexpr uint32_t kMetaMinSize    = 84; // header + tables_checksum with 0 blocks

struct Metadata {
    uint32_t magic = kMetaMagic;
    uint16_t version = kMetaVersion;
    Codec codec = Codec::None;
    ChecksumType cksum_type = ChecksumType::CRC32C;
    int8_t compression_level = 0;
    uint32_t block_size = kDefaultBlockSize;
    uint64_t original_size = 0;
    uint32_t block_count = 0;

    // POSIX file attributes
    uint32_t mode = 0100644; // S_IFREG | 0644
    uint32_t uid = 0;
    uint32_t gid = 0;
    int64_t atime_sec = 0;
    uint32_t atime_nsec = 0;
    int64_t mtime_sec = 0;
    uint32_t mtime_nsec = 0;
    int64_t ctime_sec = 0;
    uint32_t ctime_nsec = 0;

    // Per-block tables
    std::vector<uint32_t> compressed_sizes;
    std::vector<uint32_t> block_checksums; // CRC32C of each compressed block; empty = not computed
};

// Serialize metadata into a wire-format buffer.
// Returns Error::Ok + populated buffer, or error + empty buffer.
[[nodiscard]] std::pair<Error, std::vector<uint8_t>>
metadata_serialize(const Metadata& meta);

// Deserialize metadata from a wire-format buffer.
// Validates magic, version, checksums, block_count bounds.
// Returns Error::Ok + populated Metadata, or error + default Metadata.
[[nodiscard]] std::pair<Error, Metadata>
metadata_deserialize(std::span<const uint8_t> buf);

// Compute CRC32C over a byte range.
[[nodiscard]] uint32_t crc32c(std::span<const uint8_t> data);

// Returns true if the hardware-accelerated CRC32C path is active.
[[nodiscard]] bool crc32c_is_hardware_accelerated();

} // namespace compressfs
