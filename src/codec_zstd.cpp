#include "compressfs/codec.hpp"

#include <zstd.h>

namespace compressfs {

size_t ZstdCodec::compress(
    std::span<const uint8_t> input,
    std::span<uint8_t> output,
    int level) const {
    if (input.empty())
        return 0;

    // Clamp level to zstd's valid range instead of erroring.
    // Why: level is a convenience parameter; out-of-range is not a critical error.
    int min_level = ZSTD_minCLevel();
    int max_level = ZSTD_maxCLevel();
    if (level < min_level) level = min_level;
    if (level > max_level) level = max_level;

    size_t ret = ZSTD_compress(
        output.data(), output.size(),
        input.data(), input.size(),
        level);

    if (ZSTD_isError(ret))
        return 0;

    return ret;
}

size_t ZstdCodec::decompress(
    std::span<const uint8_t> input,
    std::span<uint8_t> output,
    size_t expected_size) const {
    if (input.empty() || expected_size == 0)
        return 0;

    size_t ret = ZSTD_decompress(
        output.data(), output.size(),
        input.data(), input.size());

    if (ZSTD_isError(ret))
        return 0;

    // BUG RISK: if ret != expected_size, the compressed data was either
    // corrupt or produced from different-sized input. Treat as error.
    if (ret != expected_size)
        return 0;

    return ret;
}

size_t ZstdCodec::max_compressed_size(size_t input_len) const {
    return ZSTD_compressBound(input_len);
}

Codec ZstdCodec::codec_id() const { return Codec::Zstd; }
const char* ZstdCodec::name() const { return "zstd"; }

} // namespace compressfs
