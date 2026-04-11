#pragma once

#include "compressfs/metadata.hpp"

#include <cstdint>
#include <string>

namespace compressfs {

struct FsConfig {
    std::string backing_dir;
    uint32_t block_size = kDefaultBlockSize;
    Codec codec_id = Codec::None;
    int compression_level = 0;
    size_t cache_size = 128 * 1024 * 1024; // 128 MiB default
};

// Entry point: builds fuse_operations, calls fuse_main().
// argc/argv are passed to FUSE for its own option parsing.
// Returns the fuse_main() return code.
int compressfs_fuse_main(int argc, char* argv[], const FsConfig& cfg);

} // namespace compressfs
