#include "compressfs/block_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>

namespace compressfs {

BlockManager::BlockManager(BackingStore& store, const CodecBase* codec,
                           int compression_level, BlockCache* cache,
                           FsStats* stats)
    : store_(store), codec_(codec), compression_level_(compression_level),
      cache_(cache), stats_(stats) {}

Error BlockManager::read_decompress_block(
    const std::string& path, uint32_t block_idx, size_t expect_size,
    const Metadata& meta, std::vector<uint8_t>& decompressed_out) {

    // Cache-first read: check cache before hitting disk
    if (cache_) {
        const uint8_t* cached = cache_->get(path, block_idx);
        if (cached) {
            decompressed_out.assign(cached, cached + expect_size);
            return Error::Ok;
        }
    }

    std::vector<uint8_t> compressed;
    Error err = store_.read_block(path, block_idx, compressed);

    if (err == Error::Ok && stats_)
        stats_->blocks_read.fetch_add(1, std::memory_order_relaxed);

    if (err == Error::NotFound) {
        // Sparse region: block never written. Return zeros.
        decompressed_out.assign(expect_size, 0);
        return Error::Ok;
    }
    if (err != Error::Ok)
        return err;

    if (compressed.empty()) {
        decompressed_out.assign(expect_size, 0);
        return Error::Ok;
    }

    // Verify CRC32C of compressed data before decompressing.
    // Why verify before decompress: avoids wasting CPU decompressing corrupt data,
    // and prevents codecs from misbehaving on malformed input (potential OOB reads).
    // Checksum=0 means "not computed" (backward compat with pre-checksum data).
    if (block_idx < static_cast<uint32_t>(meta.block_checksums.size())) {
        uint32_t stored_cksum = meta.block_checksums[block_idx];
        if (stored_cksum != 0) {
            uint32_t computed_cksum = crc32c(compressed);
            if (computed_cksum != stored_cksum) {
                std::fprintf(stderr,
                    "compressfs: CORRUPTION file=%s block=%u "
                    "expected_crc=%08x actual_crc=%08x\n",
                    path.c_str(), block_idx, stored_cksum, computed_cksum);
                return Error::Corrupt;
            }
        }
    }

    decompressed_out.resize(expect_size);

    // Per-block codec fallback detection: if the stored block is >= block_size,
    // it was stored uncompressed (compression expanded the data or failed).
    // Skip decompression and use the data directly.
    if (compressed.size() >= expect_size) {
        std::memcpy(decompressed_out.data(), compressed.data(), expect_size);
        if (stats_)
            stats_->decompressions.fetch_add(1, std::memory_order_relaxed);
    } else {
        size_t dec_size = codec_->decompress(compressed, decompressed_out, expect_size);
        if (dec_size > 0 && stats_)
            stats_->decompressions.fetch_add(1, std::memory_order_relaxed);
        if (dec_size == 0) {
            // Decompress failure: corruption or codec mismatch.
            // For NoopCodec where compressed IS the data, handle undersized blocks.
            if (codec_->codec_id() == Codec::None && compressed.size() < expect_size) {
                std::memcpy(decompressed_out.data(), compressed.data(),
                            compressed.size());
                std::memset(decompressed_out.data() + compressed.size(), 0,
                            expect_size - compressed.size());
            } else {
                return Error::Corrupt;
            }
        }
    }

    // Insert into cache after successful decompression
    if (cache_) {
        (void)cache_->put(path, block_idx, decompressed_out, /*dirty=*/false);
    }

    return Error::Ok;
}

Error BlockManager::compress_write_block(
    const std::string& path, uint32_t block_idx,
    std::span<const uint8_t> decompressed, Metadata& meta) {

    size_t max_out = codec_->max_compressed_size(decompressed.size());
    if (max_out == 0 && !decompressed.empty())
        max_out = decompressed.size(); // fallback: will store uncompressed

    std::vector<uint8_t> compressed(max_out);
    size_t comp_size = codec_->compress(decompressed, compressed, compression_level_);

    // Per-block codec fallback: if compression failed or expanded the data,
    // store uncompressed. This is graceful degradation - random/encrypted data
    // is handled, not rejected.
    std::span<const uint8_t> data_to_write;
    if (comp_size == 0 || comp_size >= decompressed.size()) {
        data_to_write = decompressed;
        comp_size = decompressed.size();
    } else {
        data_to_write = std::span<const uint8_t>(compressed.data(), comp_size);
    }

    if (stats_)
        stats_->compressions.fetch_add(1, std::memory_order_relaxed);

    // Compute CRC32C of whatever is actually stored on disk
    uint32_t checksum = crc32c(data_to_write);

    Error err = store_.write_block(path, block_idx, data_to_write);
    if (err != Error::Ok)
        return err;

    if (stats_)
        stats_->blocks_written.fetch_add(1, std::memory_order_relaxed);

    // Grow metadata vectors if this block extends past current block_count
    if (block_idx >= meta.block_count) {
        uint32_t new_count = block_idx + 1;
        meta.compressed_sizes.resize(new_count, 0);
        meta.block_checksums.resize(new_count, 0);
        meta.block_count = new_count;
    }

    meta.compressed_sizes[block_idx] = static_cast<uint32_t>(comp_size);
    meta.block_checksums[block_idx] = checksum;

    // Update cache with the decompressed block (write-through: data already on disk)
    if (cache_) {
        (void)cache_->put(path, block_idx, decompressed, /*dirty=*/false);
    }

    return Error::Ok;
}

int BlockManager::read(const std::string& backing_path,
                       const Metadata& meta,
                       char* buf, size_t size, uint64_t offset) {
    const uint32_t bs = meta.block_size;
    const uint64_t file_size = meta.original_size;

    if (offset >= file_size)
        return 0;
    if (offset + size > file_size)
        size = static_cast<size_t>(file_size - offset);
    if (size == 0)
        return 0;

    uint32_t first_block = static_cast<uint32_t>(offset / bs);
    uint32_t last_block  = static_cast<uint32_t>((offset + size - 1) / bs);

    size_t buf_offset = 0;
    std::vector<uint8_t> decompressed;

    for (uint32_t i = first_block; i <= last_block; ++i) {
        uint64_t block_start = static_cast<uint64_t>(i) * bs;

        // How many logical bytes this block covers within the file
        size_t block_data_size = bs;
        if (block_start + bs > file_size)
            block_data_size = static_cast<size_t>(file_size - block_start);

        size_t start_in_block = static_cast<size_t>(
            std::max(offset, block_start) - block_start);
        size_t end_in_block = static_cast<size_t>(
            std::min(offset + size, block_start + block_data_size) - block_start);
        size_t bytes_from_block = end_in_block - start_in_block;

        // Why always decompress to block_size: blocks are always compressed from
        // block_size bytes (zero-padded). Decompression must use the same size.
        // The caller reads only the relevant slice (clamped by file_size).
        Error err = read_decompress_block(backing_path, i, bs, meta, decompressed);
        if (err != Error::Ok) {
            // Return partial read if we already have some bytes,
            // otherwise propagate as negative errno
            if (buf_offset > 0)
                return static_cast<int>(buf_offset);
            return (err == Error::IO) ? -EIO : -EIO;
        }

        // BUG RISK: decompressed might be smaller than expected if the block
        // was stored with fewer bytes. Treat missing bytes as zeros.
        size_t available = decompressed.size();
        if (start_in_block < available) {
            size_t copy_len = std::min(bytes_from_block,
                                       available - start_in_block);
            std::memcpy(buf + buf_offset,
                        decompressed.data() + start_in_block, copy_len);
            if (copy_len < bytes_from_block) {
                std::memset(buf + buf_offset + copy_len, 0,
                            bytes_from_block - copy_len);
            }
        } else {
            std::memset(buf + buf_offset, 0, bytes_from_block);
        }

        buf_offset += bytes_from_block;
    }

    if (stats_ && buf_offset > 0)
        stats_->bytes_read_logical.fetch_add(buf_offset, std::memory_order_relaxed);

    return static_cast<int>(buf_offset);
}

int BlockManager::write(const std::string& backing_path,
                        Metadata& meta,
                        const char* buf, size_t size, uint64_t offset) {
    const uint32_t bs = meta.block_size;
    if (size == 0)
        return 0;

    uint64_t new_end = offset + size;
    uint32_t first_block = static_cast<uint32_t>(offset / bs);
    uint32_t last_block  = static_cast<uint32_t>((new_end - 1) / bs);

    size_t buf_offset = 0;
    std::vector<uint8_t> block_buf;

    for (uint32_t i = first_block; i <= last_block; ++i) {
        uint64_t block_start = static_cast<uint64_t>(i) * bs;
        size_t start_in_block = static_cast<size_t>(
            std::max(offset, block_start) - block_start);
        size_t end_in_block = static_cast<size_t>(
            std::min(new_end, block_start + bs) - block_start);
        size_t bytes_to_write = end_in_block - start_in_block;

        bool partial = (start_in_block != 0 || end_in_block != bs);

        if (partial) {
            // Read-modify-write: decompress existing block, overlay new data.
            // Why this overhead exists: block-level compression requires the
            // entire block to be recompressed even for a 1-byte change. Larger
            // block sizes reduce the frequency (fewer partial writes) but hurt
            // random read latency (more data to decompress).
            size_t existing_size = bs;
            // For blocks beyond current file extent, use full block_size
            // For the last existing block, use actual remaining bytes
            if (i < meta.block_count && block_start + bs > meta.original_size &&
                block_start < meta.original_size) {
                existing_size = static_cast<size_t>(meta.original_size - block_start);
            }

            Error rerr = read_decompress_block(backing_path, i, existing_size,
                                               meta, block_buf);
            if (rerr != Error::Ok) {
                if (rerr == Error::NotFound || rerr == Error::Corrupt) {
                    // New block or corrupt - start fresh
                    block_buf.assign(bs, 0);
                } else {
                    return (buf_offset > 0)
                               ? static_cast<int>(buf_offset)
                               : -EIO;
                }
            }

            // Ensure block_buf is large enough to hold the write
            if (block_buf.size() < end_in_block)
                block_buf.resize(end_in_block, 0);

            std::memcpy(block_buf.data() + start_in_block,
                        buf + buf_offset, bytes_to_write);
        } else {
            // Full block write: take data directly from user buffer
            block_buf.assign(
                reinterpret_cast<const uint8_t*>(buf + buf_offset),
                reinterpret_cast<const uint8_t*>(buf + buf_offset + bs));
        }

        // Why always compress full block_size: decompression needs to know the
        // original decompressed size. Rather than storing it separately, we
        // always compress block_size bytes (zero-padded for the tail block).
        // original_size in metadata is the authority for logical file size;
        // read() clamps output accordingly.
        if (block_buf.size() < bs)
            block_buf.resize(bs, 0);

        Error werr = compress_write_block(
            backing_path, i,
            std::span<const uint8_t>(block_buf.data(), bs),
            meta);
        if (werr != Error::Ok) {
            return (buf_offset > 0)
                       ? static_cast<int>(buf_offset)
                       : -EIO;
        }

        buf_offset += bytes_to_write;
    }

    if (new_end > meta.original_size)
        meta.original_size = new_end;

    if (stats_ && buf_offset > 0)
        stats_->bytes_written_logical.fetch_add(buf_offset, std::memory_order_relaxed);

    return static_cast<int>(buf_offset);
}

Error BlockManager::truncate(const std::string& backing_path,
                              Metadata& meta, uint64_t new_size) {
    uint32_t bs = meta.block_size;

    if (new_size < meta.original_size) {
        // Shrinking: remove excess blocks
        uint32_t new_bc = (new_size == 0)
                              ? 0u
                              : static_cast<uint32_t>((new_size - 1) / bs) + 1;

        for (uint32_t i = new_bc; i < meta.block_count; ++i) {
            (void)store_.delete_block(backing_path, i);
        }

        meta.block_count = new_bc;
        meta.compressed_sizes.resize(new_bc);
        meta.block_checksums.resize(new_bc);
    }

    // Invalidate cached blocks for this file - blocks may have been removed
    // or the file extent changed. Stale cache entries would return wrong data.
    if (cache_)
        cache_->invalidate_file(backing_path);

    meta.original_size = new_size;
    return Error::Ok;
}

} // namespace compressfs
