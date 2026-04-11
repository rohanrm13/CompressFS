#include "compressfs/metadata.hpp"

#include <cstring>
#include <endian.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <cpuid.h>
#include <nmmintrin.h> // SSE4.2 CRC32 intrinsics
#define CFS_HAS_X86_CRC32C 1
#else
#define CFS_HAS_X86_CRC32C 0
#endif

namespace compressfs {
namespace {

// CRC32C (Castagnoli) lookup table, computed at compile time.
constexpr std::array<uint32_t, 256> make_crc32c_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            // 0x82F63B78 is the bit-reversed CRC32C polynomial
            crc = (crc >> 1) ^ (0x82F63B78u & (-(crc & 1u)));
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kCRC32CTable = make_crc32c_table();

// Little-endian write helpers. On LE architectures these compile to plain
// stores; the htole* calls are effectively no-ops but guarantee correctness
// if this code ever runs on BE (unlikely for a FUSE fs, but costs nothing).
void write_le16(uint8_t* dst, uint16_t val) {
    val = htole16(val);
    std::memcpy(dst, &val, sizeof(val));
}

void write_le32(uint8_t* dst, uint32_t val) {
    val = htole32(val);
    std::memcpy(dst, &val, sizeof(val));
}

void write_le64(uint8_t* dst, uint64_t val) {
    val = htole64(val);
    std::memcpy(dst, &val, sizeof(val));
}

uint16_t read_le16(const uint8_t* src) {
    uint16_t val;
    std::memcpy(&val, src, sizeof(val));
    return le16toh(val);
}

uint32_t read_le32(const uint8_t* src) {
    uint32_t val;
    std::memcpy(&val, src, sizeof(val));
    return le32toh(val);
}

uint64_t read_le64(const uint8_t* src) {
    uint64_t val;
    std::memcpy(&val, src, sizeof(val));
    return le64toh(val);
}

// Helper to write a signed int8 at a byte offset
void write_i8(uint8_t* dst, int8_t val) {
    uint8_t u;
    std::memcpy(&u, &val, 1);
    *dst = u;
}

int8_t read_i8(const uint8_t* src) {
    int8_t val;
    std::memcpy(&val, src, 1);
    return val;
}

// Signed 64-bit LE helpers for timestamps (time_t can be negative)
void write_le_i64(uint8_t* dst, int64_t val) {
    uint64_t u;
    std::memcpy(&u, &val, sizeof(u));
    write_le64(dst, u);
}

int64_t read_le_i64(const uint8_t* src) {
    uint64_t u = read_le64(src);
    int64_t val;
    std::memcpy(&val, &u, sizeof(val));
    return val;
}

// CRC32C dispatch: hardware (SSE4.2) or software
using Crc32cFn = uint32_t(*)(std::span<const uint8_t>);

uint32_t crc32c_software(std::span<const uint8_t> data) {
    uint32_t crc = 0xFFFFFFFF;
    for (uint8_t byte : data) {
        crc = kCRC32CTable[(crc ^ byte) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

#if CFS_HAS_X86_CRC32C
#pragma GCC push_options
#pragma GCC target("sse4.2")

uint32_t crc32c_hardware(std::span<const uint8_t> data) {
    uint32_t crc = 0xFFFFFFFF;
    const uint8_t* p = data.data();
    size_t len = data.size();

    // Process 8 bytes at a time via 64-bit CRC instruction.
    // Why cast crc to uint64_t: _mm_crc32_u64 takes uint64_t accumulator
    // but CRC32C state is 32-bit. The upper 32 bits of the result are zero.
    while (len >= 8) {
        uint64_t val;
        std::memcpy(&val, p, 8);
        crc = static_cast<uint32_t>(
            _mm_crc32_u64(static_cast<uint64_t>(crc), val));
        p += 8;
        len -= 8;
    }

    // Process remaining bytes one at a time
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p);
        ++p;
        --len;
    }

    return crc ^ 0xFFFFFFFF;
}

#pragma GCC pop_options

bool detect_sse42() {
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return false;
    return (ecx >> 20) & 1; // SSE4.2 feature bit
}
#endif // CFS_HAS_X86_CRC32C

Crc32cFn select_crc32c_impl() {
#if CFS_HAS_X86_CRC32C
    if (detect_sse42())
        return crc32c_hardware;
#endif
    return crc32c_software;
}

// Why global function pointer initialized at load time: CPUID is checked once,
// every subsequent crc32c() call goes through a direct function pointer with
// zero dispatch overhead. std::function would add heap allocation and
// indirection. Virtual dispatch is overkill for a single function.
Crc32cFn g_crc32c_fn = select_crc32c_impl();
bool g_crc32c_is_hw = (g_crc32c_fn != crc32c_software);

} // anonymous namespace

uint32_t crc32c(std::span<const uint8_t> data) {
    return g_crc32c_fn(data);
}

bool crc32c_is_hardware_accelerated() {
    return g_crc32c_is_hw;
}

std::pair<Error, std::vector<uint8_t>>
metadata_serialize(const Metadata& meta) {
    if (meta.magic != kMetaMagic)
        return {Error::InvalidArg, {}};

    if (meta.block_count > kMaxBlocks)
        return {Error::Overflow, {}};

    if (meta.compressed_sizes.size() != meta.block_count)
        return {Error::InvalidArg, {}};

    // block_checksums must be either empty (not computed) or match block_count
    if (!meta.block_checksums.empty() &&
        meta.block_checksums.size() != meta.block_count)
        return {Error::InvalidArg, {}};

    // BUG RISK: overflow on block_count * 8 if block_count were unchecked.
    // kMaxBlocks = 1M, so max is 8 MiB - safe within size_t.
    const size_t per_block_bytes = static_cast<size_t>(meta.block_count) * 8;
    const size_t total = kMetaMinSize + per_block_bytes;

    std::vector<uint8_t> buf(total, 0);
    uint8_t* p = buf.data();

    // Header (offsets 0..79)
    write_le32(p + 0, meta.magic);
    write_le16(p + 4, meta.version);
    write_le16(p + 6, static_cast<uint16_t>(meta.codec));
    write_le16(p + 8, static_cast<uint16_t>(meta.cksum_type));
    write_i8(p + 10, meta.compression_level);
    // p[11] = 0 (reserved, already zeroed)
    write_le32(p + 12, meta.block_size);
    write_le64(p + 16, meta.original_size);
    write_le32(p + 24, meta.block_count);
    write_le32(p + 28, meta.mode);
    write_le32(p + 32, meta.uid);
    write_le32(p + 36, meta.gid);
    write_le_i64(p + 40, meta.atime_sec);
    write_le32(p + 48,  meta.atime_nsec);
    write_le_i64(p + 52, meta.mtime_sec);
    write_le32(p + 60,  meta.mtime_nsec);
    write_le_i64(p + 64, meta.ctime_sec);
    write_le32(p + 72,  meta.ctime_nsec);

    // Header checksum covers bytes 0..75
    uint32_t hdr_cksum = crc32c({p, 76});
    write_le32(p + 76, hdr_cksum);

    // Variable section: compressed_sizes then block_checksums
    uint8_t* tables_start = p + kMetaHeaderSize;
    const size_t sizes_bytes = static_cast<size_t>(meta.block_count) * 4;

    for (uint32_t i = 0; i < meta.block_count; ++i) {
        write_le32(tables_start + static_cast<size_t>(i) * 4,
                   meta.compressed_sizes[i]);
    }

    uint8_t* checksums_start = tables_start + sizes_bytes;
    for (uint32_t i = 0; i < meta.block_count; ++i) {
        // Empty block_checksums -> write zeros (checksums not computed yet)
        uint32_t ckval = meta.block_checksums.empty()
                             ? 0u
                             : meta.block_checksums[i];
        write_le32(checksums_start + static_cast<size_t>(i) * 4, ckval);
    }

    // Single checksum over both tables combined
    uint32_t tbl_cksum = crc32c({tables_start, per_block_bytes});
    write_le32(tables_start + per_block_bytes, tbl_cksum);

    return {Error::Ok, std::move(buf)};
}

std::pair<Error, Metadata>
metadata_deserialize(std::span<const uint8_t> buf) {
    if (buf.size() < kMetaMinSize)
        return {Error::Corrupt, {}};

    const uint8_t* p = buf.data();

    uint32_t magic = read_le32(p + 0);
    if (magic != kMetaMagic)
        return {Error::Corrupt, {}};

    uint16_t version = read_le16(p + 4);
    if (version != kMetaVersion)
        return {Error::Corrupt, {}};

    // Verify header checksum before trusting any other fields.
    // Covers bytes 0..75 (everything before the checksum field at offset 76).
    uint32_t stored_hdr_cksum = read_le32(p + 76);
    uint32_t computed_hdr_cksum = crc32c({p, 76});
    if (stored_hdr_cksum != computed_hdr_cksum)
        return {Error::Corrupt, {}};

    uint32_t block_count = read_le32(p + 24);
    if (block_count > kMaxBlocks)
        return {Error::Overflow, {}};

    // Both tables: compressed_sizes (N*4) + block_checksums (N*4) = N*8
    const size_t per_block_bytes = static_cast<size_t>(block_count) * 8;
    const size_t expected_size = kMetaMinSize + per_block_bytes;
    if (buf.size() < expected_size)
        return {Error::Corrupt, {}};

    // Verify combined tables checksum
    const uint8_t* tables_start = p + kMetaHeaderSize;
    uint32_t stored_tbl_cksum = read_le32(tables_start + per_block_bytes);
    uint32_t computed_tbl_cksum = crc32c({tables_start, per_block_bytes});
    if (stored_tbl_cksum != computed_tbl_cksum)
        return {Error::Corrupt, {}};

    Metadata meta;
    meta.magic = magic;
    meta.version = version;
    meta.codec = static_cast<Codec>(read_le16(p + 6));
    meta.cksum_type = static_cast<ChecksumType>(read_le16(p + 8));
    meta.compression_level = read_i8(p + 10);
    meta.block_size = read_le32(p + 12);
    meta.original_size = read_le64(p + 16);
    meta.block_count = block_count;
    meta.mode = read_le32(p + 28);
    meta.uid = read_le32(p + 32);
    meta.gid = read_le32(p + 36);
    meta.atime_sec = read_le_i64(p + 40);
    meta.atime_nsec = read_le32(p + 48);
    meta.mtime_sec = read_le_i64(p + 52);
    meta.mtime_nsec = read_le32(p + 60);
    meta.ctime_sec = read_le_i64(p + 64);
    meta.ctime_nsec = read_le32(p + 72);

    // Read compressed_sizes table
    const size_t sizes_bytes = static_cast<size_t>(block_count) * 4;
    meta.compressed_sizes.resize(block_count);
    for (uint32_t i = 0; i < block_count; ++i) {
        meta.compressed_sizes[i] = read_le32(
            tables_start + static_cast<size_t>(i) * 4);
    }

    // Read block_checksums table
    const uint8_t* checksums_start = tables_start + sizes_bytes;
    meta.block_checksums.resize(block_count);
    for (uint32_t i = 0; i < block_count; ++i) {
        meta.block_checksums[i] = read_le32(
            checksums_start + static_cast<size_t>(i) * 4);
    }

    return {Error::Ok, std::move(meta)};
}

} // namespace compressfs
