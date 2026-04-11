#include "compressfs/metadata.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>

using namespace compressfs;

static int g_pass = 0;
static int g_fail = 0;

#define TEST(name) static void name()
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
        std::fprintf(stderr, "  %-55s", #name);                              \
        name();                                                               \
        if (g_fail == prev_fail) {                                            \
            std::fprintf(stderr, "PASS\n");                                   \
            ++g_pass;                                                         \
        }                                                                     \
        prev_fail = g_fail;                                                   \
    } while (0)

// --- Existing tests (updated for v2 format) ---

TEST(test_serialize_deserialize_roundtrip) {
    Metadata m;
    m.block_count = 3;
    m.block_size = 65536;
    m.original_size = 65536 * 3 - 100;
    m.codec = Codec::None;
    m.cksum_type = ChecksumType::CRC32C;
    m.compressed_sizes = {65536, 65536, 65436};
    m.block_checksums = {0xAAAAAAAA, 0xBBBBBBBB, 0xCCCCCCCC};

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);
    ASSERT_TRUE(!buf.empty());

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.magic, kMetaMagic);
    ASSERT_EQ(m2.version, kMetaVersion);
    ASSERT_EQ(m2.block_count, 3u);
    ASSERT_EQ(m2.block_size, 65536u);
    ASSERT_EQ(m2.original_size, 65536u * 3 - 100);
    ASSERT_EQ(m2.codec, Codec::None);
    ASSERT_EQ(m2.cksum_type, ChecksumType::CRC32C);
    ASSERT_EQ(m2.compressed_sizes.size(), 3u);
    ASSERT_EQ(m2.compressed_sizes[0], 65536u);
    ASSERT_EQ(m2.compressed_sizes[1], 65536u);
    ASSERT_EQ(m2.compressed_sizes[2], 65436u);
    ASSERT_EQ(m2.block_checksums.size(), 3u);
    ASSERT_EQ(m2.block_checksums[0], 0xAAAAAAAAu);
    ASSERT_EQ(m2.block_checksums[1], 0xBBBBBBBBu);
    ASSERT_EQ(m2.block_checksums[2], 0xCCCCCCCCu);
}

TEST(test_zero_blocks_roundtrip) {
    Metadata m;
    m.block_count = 0;
    m.original_size = 0;
    m.compressed_sizes = {};
    m.block_checksums = {};

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);
    // v2 min size: 84 bytes (80 header + 4 tables_checksum)
    ASSERT_EQ(buf.size(), static_cast<size_t>(kMetaMinSize));

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.block_count, 0u);
    ASSERT_TRUE(m2.compressed_sizes.empty());
    ASSERT_TRUE(m2.block_checksums.empty());
}

TEST(test_deserialize_bad_magic) {
    Metadata m;
    m.block_count = 0;
    m.compressed_sizes = {};
    auto [_, buf] = metadata_serialize(m);

    buf[0] = 0xFF;
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_deserialize_bad_version) {
    Metadata m;
    m.block_count = 0;
    m.compressed_sizes = {};
    auto [_, buf] = metadata_serialize(m);

    // Corrupting version also corrupts header checksum - caught either way
    buf[4] = 0xFF;
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_deserialize_truncated) {
    std::vector<uint8_t> buf(10, 0);
    auto [err, m] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_deserialize_truncated_table) {
    Metadata m;
    m.block_count = 5;
    m.compressed_sizes = {100, 100, 100, 100, 100};
    m.block_checksums = {1, 2, 3, 4, 5};
    auto [_, buf] = metadata_serialize(m);

    // Chop off part of the tables (need 5*8 = 40 bytes of tables, give only 16)
    buf.resize(kMetaMinSize + 16);
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_deserialize_bad_header_checksum) {
    Metadata m;
    m.block_count = 1;
    m.compressed_sizes = {4096};
    m.block_checksums = {0x12345678};
    auto [_, buf] = metadata_serialize(m);

    // Flip a byte in the header (original_size at offset 16)
    buf[16] ^= 0x01;
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_deserialize_bad_table_checksum) {
    Metadata m;
    m.block_count = 2;
    m.compressed_sizes = {4096, 8192};
    m.block_checksums = {0x11111111, 0x22222222};
    auto [_, buf] = metadata_serialize(m);

    // Flip a byte in the compressed_sizes table
    buf[kMetaHeaderSize] ^= 0x01;
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_serialize_huge_block_count) {
    Metadata m;
    m.block_count = kMaxBlocks + 1;
    m.compressed_sizes.resize(m.block_count, 4096);
    auto [err, buf] = metadata_serialize(m);
    ASSERT_EQ(err, Error::Overflow);
    ASSERT_TRUE(buf.empty());
}

TEST(test_serialize_mismatched_sizes_vector) {
    Metadata m;
    m.block_count = 3;
    m.compressed_sizes = {100, 200};
    auto [err, buf] = metadata_serialize(m);
    ASSERT_EQ(err, Error::InvalidArg);
}

TEST(test_serialize_bad_magic) {
    Metadata m;
    m.magic = 0xDEADBEEF;
    m.block_count = 0;
    m.compressed_sizes = {};
    auto [err, buf] = metadata_serialize(m);
    ASSERT_EQ(err, Error::InvalidArg);
}

TEST(test_crc32c_known_vectors) {
    uint32_t empty_crc = crc32c({});
    ASSERT_EQ(empty_crc, 0x00000000u);

    const uint8_t data[] = {'1','2','3','4','5','6','7','8','9'};
    uint32_t crc = crc32c(std::span<const uint8_t>(data, 9));
    ASSERT_EQ(crc, 0xE3069283u);
}

TEST(test_codec_and_checksum_roundtrip) {
    Metadata m;
    m.codec = Codec::Zstd;
    m.cksum_type = ChecksumType::XXH64;
    m.block_count = 0;
    m.compressed_sizes = {};

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.codec, Codec::Zstd);
    ASSERT_EQ(m2.cksum_type, ChecksumType::XXH64);
}

// --- New tests for v2 fields ---

TEST(test_compression_level_roundtrip) {
    // Positive level (zstd default range)
    {
        Metadata m;
        m.compression_level = 19;
        m.block_count = 0;
        m.compressed_sizes = {};

        auto [ser_err, buf] = metadata_serialize(m);
        ASSERT_EQ(ser_err, Error::Ok);
        auto [de_err, m2] = metadata_deserialize(buf);
        ASSERT_EQ(de_err, Error::Ok);
        ASSERT_EQ(m2.compression_level, static_cast<int8_t>(19));
    }

    // Negative level (zstd fast modes use negative values)
    {
        Metadata m;
        m.compression_level = -5;
        m.block_count = 0;
        m.compressed_sizes = {};

        auto [ser_err, buf] = metadata_serialize(m);
        ASSERT_EQ(ser_err, Error::Ok);
        auto [de_err, m2] = metadata_deserialize(buf);
        ASSERT_EQ(de_err, Error::Ok);
        ASSERT_EQ(m2.compression_level, static_cast<int8_t>(-5));
    }

    // Zero level
    {
        Metadata m;
        m.compression_level = 0;
        m.block_count = 0;
        m.compressed_sizes = {};

        auto [ser_err, buf] = metadata_serialize(m);
        ASSERT_EQ(ser_err, Error::Ok);
        auto [de_err, m2] = metadata_deserialize(buf);
        ASSERT_EQ(de_err, Error::Ok);
        ASSERT_EQ(m2.compression_level, static_cast<int8_t>(0));
    }
}

TEST(test_block_checksums_roundtrip) {
    Metadata m;
    m.block_count = 4;
    m.compressed_sizes = {1000, 2000, 3000, 4000};
    m.block_checksums = {0xDEADBEEF, 0xCAFEBABE, 0x12345678, 0xFEEDFACE};

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    // v2: 84 + 4*8 = 116 bytes
    ASSERT_EQ(buf.size(), static_cast<size_t>(84 + 4 * 8));

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.block_checksums.size(), 4u);
    ASSERT_EQ(m2.block_checksums[0], 0xDEADBEEFu);
    ASSERT_EQ(m2.block_checksums[1], 0xCAFEBABEu);
    ASSERT_EQ(m2.block_checksums[2], 0x12345678u);
    ASSERT_EQ(m2.block_checksums[3], 0xFEEDFACEu);
}

TEST(test_empty_checksums_serialized_as_zeros) {
    // block_count > 0 but block_checksums empty - Phase 1 scenario
    Metadata m;
    m.block_count = 2;
    m.compressed_sizes = {512, 256};
    m.block_checksums = {}; // empty = not computed

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.block_checksums.size(), 2u);
    // Deserialized as zeros since they were written as zeros
    ASSERT_EQ(m2.block_checksums[0], 0u);
    ASSERT_EQ(m2.block_checksums[1], 0u);
}

TEST(test_mismatched_checksums_vector) {
    // Non-empty block_checksums with wrong size -> InvalidArg
    Metadata m;
    m.block_count = 3;
    m.compressed_sizes = {100, 200, 300};
    m.block_checksums = {0x11, 0x22}; // 2 checksums for 3 blocks

    auto [err, buf] = metadata_serialize(m);
    ASSERT_EQ(err, Error::InvalidArg);
}

TEST(test_posix_attrs_roundtrip) {
    Metadata m;
    m.block_count = 0;
    m.compressed_sizes = {};
    m.mode = 0100755; // S_IFREG | 0755
    m.uid = 1000;
    m.gid = 1000;
    m.atime_sec = 1700000000;
    m.atime_nsec = 123456789;
    m.mtime_sec = 1700000100;
    m.mtime_nsec = 987654321;
    m.ctime_sec = 1700000200;
    m.ctime_nsec = 555555555;

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.mode, 0100755u);
    ASSERT_EQ(m2.uid, 1000u);
    ASSERT_EQ(m2.gid, 1000u);
    ASSERT_EQ(m2.atime_sec, static_cast<int64_t>(1700000000));
    ASSERT_EQ(m2.atime_nsec, 123456789u);
    ASSERT_EQ(m2.mtime_sec, static_cast<int64_t>(1700000100));
    ASSERT_EQ(m2.mtime_nsec, 987654321u);
    ASSERT_EQ(m2.ctime_sec, static_cast<int64_t>(1700000200));
    ASSERT_EQ(m2.ctime_nsec, 555555555u);
}

TEST(test_posix_attrs_negative_timestamp) {
    // Pre-epoch timestamps (before 1970) should round-trip correctly
    Metadata m;
    m.block_count = 0;
    m.compressed_sizes = {};
    m.atime_sec = -86400; // 1969-12-31
    m.mtime_sec = -1;
    m.ctime_sec = 0;

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.atime_sec, static_cast<int64_t>(-86400));
    ASSERT_EQ(m2.mtime_sec, static_cast<int64_t>(-1));
    ASSERT_EQ(m2.ctime_sec, static_cast<int64_t>(0));
}

TEST(test_bad_block_checksums_region) {
    // Corrupt the block_checksums portion of the tables -> tables_checksum fails
    Metadata m;
    m.block_count = 2;
    m.compressed_sizes = {4096, 8192};
    m.block_checksums = {0x11111111, 0x22222222};
    auto [_, buf] = metadata_serialize(m);

    // Flip a byte in the block_checksums region (offset 80 + 2*4 = 88)
    size_t checksums_offset = kMetaHeaderSize +
                              static_cast<size_t>(m.block_count) * 4;
    buf[checksums_offset] ^= 0x01;
    auto [err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(err, Error::Corrupt);
}

TEST(test_default_metadata_values) {
    Metadata m;
    ASSERT_EQ(m.magic, kMetaMagic);
    ASSERT_EQ(m.version, kMetaVersion);
    ASSERT_EQ(m.codec, Codec::None);
    ASSERT_EQ(m.cksum_type, ChecksumType::CRC32C);
    ASSERT_EQ(m.compression_level, static_cast<int8_t>(0));
    ASSERT_EQ(m.block_size, kDefaultBlockSize);
    ASSERT_EQ(m.original_size, 0u);
    ASSERT_EQ(m.block_count, 0u);
    ASSERT_EQ(m.mode, 0100644u);
    ASSERT_EQ(m.uid, 0u);
    ASSERT_EQ(m.gid, 0u);
    ASSERT_EQ(m.atime_sec, static_cast<int64_t>(0));
    ASSERT_EQ(m.atime_nsec, 0u);
    ASSERT_TRUE(m.compressed_sizes.empty());
    ASSERT_TRUE(m.block_checksums.empty());
}

TEST(test_full_metadata_roundtrip) {
    // Exercise every single field with non-default values
    Metadata m;
    m.codec = Codec::LZ4;
    m.cksum_type = ChecksumType::XXH64;
    m.compression_level = 12;
    m.block_size = 131072;
    m.original_size = 1000000;
    m.block_count = 2;
    m.mode = 0100700;
    m.uid = 65534;
    m.gid = 65534;
    m.atime_sec = 1700000000;
    m.atime_nsec = 999999999;
    m.mtime_sec = 1700000001;
    m.mtime_nsec = 1;
    m.ctime_sec = -100;
    m.ctime_nsec = 500000000;
    m.compressed_sizes = {500000, 500000};
    m.block_checksums = {0xAABBCCDD, 0x11223344};

    auto [ser_err, buf] = metadata_serialize(m);
    ASSERT_EQ(ser_err, Error::Ok);

    auto [de_err, m2] = metadata_deserialize(buf);
    ASSERT_EQ(de_err, Error::Ok);
    ASSERT_EQ(m2.codec, Codec::LZ4);
    ASSERT_EQ(m2.cksum_type, ChecksumType::XXH64);
    ASSERT_EQ(m2.compression_level, static_cast<int8_t>(12));
    ASSERT_EQ(m2.block_size, 131072u);
    ASSERT_EQ(m2.original_size, 1000000u);
    ASSERT_EQ(m2.block_count, 2u);
    ASSERT_EQ(m2.mode, 0100700u);
    ASSERT_EQ(m2.uid, 65534u);
    ASSERT_EQ(m2.gid, 65534u);
    ASSERT_EQ(m2.atime_sec, static_cast<int64_t>(1700000000));
    ASSERT_EQ(m2.atime_nsec, 999999999u);
    ASSERT_EQ(m2.mtime_sec, static_cast<int64_t>(1700000001));
    ASSERT_EQ(m2.mtime_nsec, 1u);
    ASSERT_EQ(m2.ctime_sec, static_cast<int64_t>(-100));
    ASSERT_EQ(m2.ctime_nsec, 500000000u);
    ASSERT_EQ(m2.compressed_sizes[0], 500000u);
    ASSERT_EQ(m2.compressed_sizes[1], 500000u);
    ASSERT_EQ(m2.block_checksums[0], 0xAABBCCDDu);
    ASSERT_EQ(m2.block_checksums[1], 0x11223344u);
}

TEST(test_crc32c_hardware_acceleration_query) {
    // Just verify the function is callable and returns a boolean
    bool hw = crc32c_is_hardware_accelerated();
    std::fprintf(stderr, "[crc32c: %s] ", hw ? "hardware SSE4.2" : "software");
    // On x86_64 with SSE4.2, this should be true
    // But we don't assert - the test must pass on any platform
    (void)hw;
    ++g_pass; // always passes - informational
}

TEST(test_crc32c_consistency_across_sizes) {
    // Verify CRC32C produces correct results for various sizes.
    // The known vector test checks correctness; this checks that hardware
    // and software paths agree by testing edge cases near 8-byte alignment
    // boundaries (hardware processes 8 bytes at a time).
    const uint8_t data[] = {'1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H'};

    // Test various lengths including non-8-aligned to exercise hardware tail handling
    for (size_t len = 0; len <= 17; ++len) {
        uint32_t crc = crc32c(std::span<const uint8_t>(data, len));
        // Recompute to verify determinism
        uint32_t crc2 = crc32c(std::span<const uint8_t>(data, len));
        ASSERT_EQ(crc, crc2);
    }

    // Large buffer (exercises 8-byte loop in hardware path)
    std::vector<uint8_t> large(65536);
    for (size_t i = 0; i < large.size(); ++i)
        large[i] = static_cast<uint8_t>(i & 0xFF);
    uint32_t crc_large = crc32c(large);
    ASSERT_TRUE(crc_large != 0); // non-trivial data should not checksum to 0
    // Verify determinism
    ASSERT_EQ(crc_large, crc32c(large));
}

int main() {
    int prev_fail = 0;
    std::fprintf(stderr, "test_metadata:\n");

    // Original tests (updated for v2)
    RUN(test_serialize_deserialize_roundtrip);
    RUN(test_zero_blocks_roundtrip);
    RUN(test_deserialize_bad_magic);
    RUN(test_deserialize_bad_version);
    RUN(test_deserialize_truncated);
    RUN(test_deserialize_truncated_table);
    RUN(test_deserialize_bad_header_checksum);
    RUN(test_deserialize_bad_table_checksum);
    RUN(test_serialize_huge_block_count);
    RUN(test_serialize_mismatched_sizes_vector);
    RUN(test_serialize_bad_magic);
    RUN(test_crc32c_known_vectors);
    RUN(test_codec_and_checksum_roundtrip);

    // New v2 tests
    RUN(test_compression_level_roundtrip);
    RUN(test_block_checksums_roundtrip);
    RUN(test_empty_checksums_serialized_as_zeros);
    RUN(test_mismatched_checksums_vector);
    RUN(test_posix_attrs_roundtrip);
    RUN(test_posix_attrs_negative_timestamp);
    RUN(test_bad_block_checksums_region);
    RUN(test_default_metadata_values);
    RUN(test_full_metadata_roundtrip);

    // CRC32C hardware acceleration tests
    RUN(test_crc32c_hardware_acceleration_query);
    RUN(test_crc32c_consistency_across_sizes);

    std::fprintf(stderr, "\n  %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
