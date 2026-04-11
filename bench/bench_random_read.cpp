// Random read latency microbenchmark for CompressFS.
//
// Reads 4KB blocks at random offsets from a file and reports percentile latencies.
// Uses clock_gettime(CLOCK_MONOTONIC) for nanosecond precision.
//
// Usage: bench_random_read <file_path> <num_reads> [read_size_bytes]

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static uint64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

// Simple xorshift64 PRNG - fast, no external dependency
static uint64_t xorshift64(uint64_t& state) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr,
            "Usage: %s <file_path> <num_reads> [read_size_bytes]\n"
            "\n"
            "  Reads read_size bytes at random offsets from file_path.\n"
            "  Reports percentile latencies (p50, p99, p99.9, max).\n"
            "  Default read_size: 4096 (4 KiB)\n",
            argv[0]);
        return 1;
    }

    const char* path = argv[1];
    int num_reads = std::atoi(argv[2]);
    size_t read_size = (argc >= 4) ? static_cast<size_t>(std::atol(argv[3])) : 4096;

    if (num_reads <= 0 || read_size == 0) {
        std::fprintf(stderr, "Invalid arguments\n");
        return 1;
    }

    int fd = ::open(path, O_RDONLY);
    if (fd < 0) {
        std::fprintf(stderr, "open(%s): %s\n", path, std::strerror(errno));
        return 1;
    }

    struct stat st;
    if (::fstat(fd, &st) < 0) {
        std::fprintf(stderr, "fstat: %s\n", std::strerror(errno));
        ::close(fd);
        return 1;
    }

    auto file_size = static_cast<uint64_t>(st.st_size);
    if (file_size < read_size) {
        std::fprintf(stderr, "File too small (%lu bytes) for read_size %zu\n",
                     static_cast<unsigned long>(file_size), read_size);
        ::close(fd);
        return 1;
    }

    uint64_t max_offset = file_size - read_size;
    std::vector<uint8_t> buf(read_size);
    std::vector<uint64_t> latencies(static_cast<size_t>(num_reads));
    uint64_t prng_state = 0x123456789ABCDEF0ULL;

    // Pre-generate offsets (aligned to read_size for realistic block access)
    std::vector<uint64_t> offsets(static_cast<size_t>(num_reads));
    for (int i = 0; i < num_reads; ++i) {
        uint64_t r = xorshift64(prng_state);
        // Align to read_size boundaries
        uint64_t aligned = (r % (max_offset / read_size + 1)) * read_size;
        if (aligned > max_offset)
            aligned = 0;
        offsets[static_cast<size_t>(i)] = aligned;
    }

    // Warmup: 10 reads to fault in pages
    for (int i = 0; i < std::min(10, num_reads); ++i) {
        (void)::pread(fd, buf.data(), read_size,
                      static_cast<off_t>(offsets[static_cast<size_t>(i)]));
    }

    // Timed reads
    uint64_t total_start = now_ns();
    for (int i = 0; i < num_reads; ++i) {
        uint64_t start = now_ns();
        ssize_t n = ::pread(fd, buf.data(), read_size,
                            static_cast<off_t>(offsets[static_cast<size_t>(i)]));
        uint64_t end = now_ns();

        if (n < 0) {
            std::fprintf(stderr, "pread failed at offset %lu: %s\n",
                         static_cast<unsigned long>(offsets[static_cast<size_t>(i)]),
                         std::strerror(errno));
            ::close(fd);
            return 1;
        }
        latencies[static_cast<size_t>(i)] = end - start;
    }
    uint64_t total_ns = now_ns() - total_start;

    ::close(fd);

    // Sort for percentile computation
    std::sort(latencies.begin(), latencies.end());

    auto percentile = [&](double p) -> uint64_t {
        auto idx = static_cast<size_t>(p / 100.0 * static_cast<double>(num_reads - 1));
        return latencies[idx];
    };

    double total_sec = static_cast<double>(total_ns) / 1e9;
    double reads_per_sec = static_cast<double>(num_reads) / total_sec;
    double mb_per_sec = (static_cast<double>(num_reads) * static_cast<double>(read_size)) /
                        (1024.0 * 1024.0) / total_sec;

    std::printf("=== Random Read Latency ===\n");
    std::printf("file:       %s\n", path);
    std::printf("reads:      %d\n", num_reads);
    std::printf("read_size:  %zu bytes\n", read_size);
    std::printf("total_time: %.3f s\n", total_sec);
    std::printf("throughput: %.1f reads/s, %.2f MB/s\n", reads_per_sec, mb_per_sec);
    std::printf("\n");
    std::printf("Latency percentiles (microseconds):\n");
    std::printf("  p50:   %lu us\n", static_cast<unsigned long>(percentile(50) / 1000));
    std::printf("  p90:   %lu us\n", static_cast<unsigned long>(percentile(90) / 1000));
    std::printf("  p99:   %lu us\n", static_cast<unsigned long>(percentile(99) / 1000));
    std::printf("  p99.9: %lu us\n", static_cast<unsigned long>(percentile(99.9) / 1000));
    std::printf("  max:   %lu us\n", static_cast<unsigned long>(latencies.back() / 1000));

    return 0;
}
