#pragma once

#include "compressfs/metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace compressfs {
// Abstract codec interface. Concrete implementations wrap LZ4, zstd, etc.
class CodecBase {
public:
    virtual ~CodecBase() = default;

    // Compress input into output. Returns compressed size, or 0 on error.
    // Output buffer must be at least max_compressed_size(input.size()) bytes.
    [[nodiscard]] virtual size_t compress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        int level) const = 0;

    // Decompress input into output. expected_size is the known decompressed
    // size from metadata. Returns decompressed size, or 0 on error.
    [[nodiscard]] virtual size_t decompress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        size_t expected_size) const = 0;

    // Worst-case compressed size for input_len bytes.
    [[nodiscard]] virtual size_t max_compressed_size(size_t input_len) const = 0;

    // Codec identifier matching the Codec enum stored in metadata.
    [[nodiscard]] virtual Codec codec_id() const = 0;

    // Human-readable name for logging and CLI display.
    [[nodiscard]] virtual const char* name() const = 0;
};

// No-op codec: returns input unchanged. Useful as a benchmark control -
// isolates compression overhead from all other system overheads.
class NoopCodec final : public CodecBase {
public:
    [[nodiscard]] size_t compress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        int /*level*/) const override {
        if (output.size() < input.size())
            return 0;
        std::memcpy(output.data(), input.data(), input.size());
        return input.size();
    }

    [[nodiscard]] size_t decompress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        size_t expected_size) const override {
        if (input.size() < expected_size || output.size() < expected_size)
            return 0;
        std::memcpy(output.data(), input.data(), expected_size);
        return expected_size;
    }

    [[nodiscard]] size_t max_compressed_size(size_t input_len) const override {
        return input_len;
    }

    [[nodiscard]] Codec codec_id() const override { return Codec::None; }
    [[nodiscard]] const char* name() const override { return "none"; }
};

// LZ4 codec - declared here, defined in codec_lz4.cpp.
class LZ4Codec final : public CodecBase {
public:
    [[nodiscard]] size_t compress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        int level) const override;

    [[nodiscard]] size_t decompress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        size_t expected_size) const override;

    [[nodiscard]] size_t max_compressed_size(size_t input_len) const override;
    [[nodiscard]] Codec codec_id() const override;
    [[nodiscard]] const char* name() const override;
};

// Zstd codec - declared here, defined in codec_zstd.cpp.
class ZstdCodec final : public CodecBase {
public:
    [[nodiscard]] size_t compress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        int level) const override;

    [[nodiscard]] size_t decompress(
        std::span<const uint8_t> input,
        std::span<uint8_t> output,
        size_t expected_size) const override;

    [[nodiscard]] size_t max_compressed_size(size_t input_len) const override;
    [[nodiscard]] Codec codec_id() const override;
    [[nodiscard]] const char* name() const override;
};

// Returns the singleton codec for the given ID. Returns nullptr for unknown IDs.
// Why singletons: codecs are stateless. One instance per type, shared across files.
[[nodiscard]] const CodecBase* get_codec(Codec id);

} // namespace compressfs
