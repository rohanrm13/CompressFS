#include "compressfs/codec.hpp"

namespace compressfs {

static NoopCodec g_noop;
static LZ4Codec  g_lz4;
static ZstdCodec g_zstd;

const CodecBase* get_codec(Codec id) {
    switch (id) {
        case Codec::None: return &g_noop;
        case Codec::LZ4:  return &g_lz4;
        case Codec::Zstd: return &g_zstd;
    }
    return nullptr;
}

} // namespace compressfs
