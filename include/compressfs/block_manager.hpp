#pragma once

#include "compressfs/backing_store.hpp"
#include "compressfs/block_cache.hpp"
#include "compressfs/codec.hpp"
#include "compressfs/metadata.hpp"
#include "compressfs/stats.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace compressfs {
// Central coordinator translating file-level I/O (arbitrary offset+length) into
// block-level compressed I/O (aligned, fixed-size units).
class BlockManager {
public:
    // cache and stats are optional - if nullptr, bypassed.
    BlockManager(BackingStore& store, const CodecBase* codec,
                 int compression_level, BlockCache* cache = nullptr,
                 FsStats* stats = nullptr);

    // Read bytes from file at offset into buf. Returns bytes read (>= 0),
    // or negative errno-style error code.
    [[nodiscard]] int read(const std::string& backing_path,
                           const Metadata& meta,
                           char* buf, size_t size, uint64_t offset);

    // Write bytes to file at offset. Returns bytes written (>= 0),
    // or negative errno-style error code.
    // Mutates meta: updates original_size, block_count, compressed_sizes,
    // block_checksums.
    [[nodiscard]] int write(const std::string& backing_path,
                            Metadata& meta,
                            const char* buf, size_t size, uint64_t offset);

    // Truncate file to new_size. Mutates meta.
    [[nodiscard]] Error truncate(const std::string& backing_path,
                                  Metadata& meta, uint64_t new_size);

private:
    BackingStore& store_;
    const CodecBase* codec_;
    int compression_level_;
    BlockCache* cache_;  // nullable - bypassed when nullptr
    FsStats* stats_;     // nullable - no stats when nullptr

    // Read a compressed block from store, verify CRC32C checksum, decompress.
    // On NotFound (sparse region), fills decompressed_out with zeros.
    // decompressed_out is resized to expect_size bytes.
    Error read_decompress_block(const std::string& path, uint32_t block_idx,
                                size_t expect_size, const Metadata& meta,
                                std::vector<uint8_t>& decompressed_out);

    // Compress decompressed data and write to store. Updates meta arrays.
    Error compress_write_block(const std::string& path, uint32_t block_idx,
                               std::span<const uint8_t> decompressed,
                               Metadata& meta);
};

} // namespace compressfs
