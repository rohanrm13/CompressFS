#include "compressfs/fuse_ops.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>

static void usage(const char* progname) {
    std::fprintf(stderr,
        "Usage: %s <backing_dir> <mountpoint> [options...] [FUSE options...]\n"
        "\n"
        "  backing_dir   Directory where compressed data is stored\n"
        "  mountpoint    Where the virtual filesystem is mounted\n"
        "\n"
        "CompressFS options:\n"
        "  --codec=CODEC     Compression codec: none, lz4, zstd (default: none)\n"
        "  --level=N         Compression level (zstd: -131072..22, default: 0)\n"
        "  --block-size=N    Block size in bytes (default: 65536)\n"
        "  --cache-size=N    Cache size in bytes (default: 134217728 = 128M)\n"
        "\n"
        "FUSE options (passed through):\n"
        "  -f            Run in foreground\n"
        "  -d            Debug mode (implies -f)\n"
        "  -o opt[,opt]  Mount options\n",
        progname);
}

// Parse a --key=value option. Returns value string if key matches, nullptr otherwise.
static const char* parse_opt(const char* arg, const char* key) {
    size_t klen = std::strlen(key);
    if (std::strncmp(arg, key, klen) == 0 && arg[klen] == '=')
        return arg + klen + 1;
    return nullptr;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    const char* backing_dir = argv[1];

    struct stat st;
    if (::stat(backing_dir, &st) < 0) {
        std::fprintf(stderr, "compressfs: backing dir '%s': %s\n",
                     backing_dir, std::strerror(errno));
        return 1;
    }
    if (!S_ISDIR(st.st_mode)) {
        std::fprintf(stderr, "compressfs: '%s' is not a directory\n",
                     backing_dir);
        return 1;
    }

    compressfs::FsConfig cfg;
    cfg.backing_dir = backing_dir;

    // Separate our options from FUSE options.
    // Our options start with "--". Everything else goes to FUSE.
    // argv layout: [progname, backing_dir, mountpoint, ...]
    // We need at least argv[2] as mountpoint for FUSE.
    std::vector<char*> fuse_args;
    fuse_args.push_back(argv[0]);

    for (int i = 2; i < argc; ++i) {
        const char* val = nullptr;

        if ((val = parse_opt(argv[i], "--codec"))) {
            if (std::strcmp(val, "none") == 0)
                cfg.codec_id = compressfs::Codec::None;
            else if (std::strcmp(val, "lz4") == 0)
                cfg.codec_id = compressfs::Codec::LZ4;
            else if (std::strcmp(val, "zstd") == 0)
                cfg.codec_id = compressfs::Codec::Zstd;
            else {
                std::fprintf(stderr, "compressfs: unknown codec '%s'\n", val);
                return 1;
            }
        } else if ((val = parse_opt(argv[i], "--level"))) {
            cfg.compression_level = std::atoi(val);
        } else if ((val = parse_opt(argv[i], "--block-size"))) {
            cfg.block_size = static_cast<uint32_t>(std::atol(val));
            if (cfg.block_size < 4096) {
                std::fprintf(stderr, "compressfs: block-size must be >= 4096\n");
                return 1;
            }
        } else if ((val = parse_opt(argv[i], "--cache-size"))) {
            cfg.cache_size = static_cast<size_t>(std::atol(val));
        } else if (std::strcmp(argv[i], "--help") == 0 ||
                   std::strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            // Pass through to FUSE
            fuse_args.push_back(argv[i]);
        }
    }

    int fuse_argc = static_cast<int>(fuse_args.size());
    char** fuse_argv = fuse_args.data();

    return compressfs::compressfs_fuse_main(fuse_argc, fuse_argv, cfg);
}
