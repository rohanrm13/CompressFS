#include "compressfs/codec.hpp"

#include <lz4.h>

namespace compressfs {

size_t LZ4Codec::compress(
    std::span<const uint8_t> input,
    std::span<uint8_t> output,
    int level) const {
    if (input.empty())
        return 0;

    int ret = LZ4_compress_default(
        reinterpret_cast<const char*>(input.data()),
        reinterpret_cast<char*>(output.data()),
        static_cast<int>(input.size()),
        static_cast<int>(output.size()));

    // LZ4_compress_default returns 0 on failure (output buffer too small,
    // input too large for int range, etc.)
    return (ret > 0) ? static_cast<size_t>(ret) : 0;
}

size_t LZ4Codec::decompress(
    std::span<const uint8_t> input,
    std::span<uint8_t> output,
    size_t expected_size) const {
    if (input.empty() || expected_size == 0)
        return 0;

    int ret = LZ4_decompress_safe(
        reinterpret_cast<const char*>(input.data()),
        reinterpret_cast<char*>(output.data()),
        static_cast<int>(input.size()),
        static_cast<int>(output.size()));

    if (ret < 0)
        return 0;

    // BUG RISK: if ret != expected_size, the block is either corrupt or was
    // compressed from a different-sized input. Both are errors.
    if (static_cast<size_t>(ret) != expected_size)
        return 0;

    return static_cast<size_t>(ret);
}

size_t LZ4Codec::max_compressed_size(size_t input_len) const {
    // LZ4_compressBound returns 0 for inputs > LZ4_MAX_INPUT_SIZE (~2 GiB)
    int bound = LZ4_compressBound(static_cast<int>(input_len));
    return (bound > 0) ? static_cast<size_t>(bound) : 0;
}

Codec LZ4Codec::codec_id() const { return Codec::LZ4; }
const char* LZ4Codec::name() const { return "lz4"; }

} // namespace compressfs
