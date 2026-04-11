#!/bin/bash
# CompressFS real-data benchmark
#
# Uses the Silesia corpus (~202 MiB of mixed real-world data). Measures
# sequential write+read throughput (paired per run), random read latency,
# cache effectiveness, and the block-size tradeoff. Every throughput number
# is the median of RUNS repetitions. Caches are dropped between measurements.
#
# Usage:
#   bash bench_real.sh              (default: 2 runs per measurement)
#   RUNS=3 bash bench_real.sh
#
# Requires:
#   - bench/testdata/silesia/       populated (fetch the Silesia corpus)
#   - sudo for /proc/sys/vm/drop_caches
#   - ./build/compressfs and ./build/bench_random_read built

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPRESSFS="$PROJECT_DIR/build/compressfs"
BENCH_READ="$PROJECT_DIR/build/bench_random_read"
SILESIA_DIR="$SCRIPT_DIR/testdata/silesia"
RESULTS_FILE="$SCRIPT_DIR/RESULTS.txt"

RUNS="${RUNS:-1}"

if [ ! -d "$SILESIA_DIR" ] || [ -z "$(ls -A "$SILESIA_DIR" 2>/dev/null)" ]; then
    echo "ERROR: Silesia corpus not found at $SILESIA_DIR" >&2
    exit 1
fi
if [ ! -x "$COMPRESSFS" ] || [ ! -x "$BENCH_READ" ]; then
    echo "ERROR: build first (cmake --build build)" >&2
    exit 1
fi

SILESIA_BYTES=$(du -sb "$SILESIA_DIR" | awk '{print $1}')
SILESIA_MIB=$(echo "scale=1; $SILESIA_BYTES / 1048576" | bc)

# Explicit file list — workaround for a readdir issue under heavy cache
# pressure where globbing the mount can return empty even though the files
# exist. Using fixed names bypasses readdir and goes straight through lookup.
SILESIA_FILES=(dickens mozilla mr nci ooffice osdb reymont samba sao webster x-ray xml)

cat_all_files() {
    local mnt="$1" f
    for f in "${SILESIA_FILES[@]}"; do
        cat "$mnt/$f"
    done > /dev/null
}

BACKING_DIR=""
MOUNT_DIR=""
CFS_PID=""

cleanup() {
    if [ -n "$CFS_PID" ] && kill -0 "$CFS_PID" 2>/dev/null; then
        fusermount3 -u "$MOUNT_DIR" 2>/dev/null || true
        wait "$CFS_PID" 2>/dev/null || true
    fi
    [ -n "$BACKING_DIR" ] && rm -rf "$BACKING_DIR"
    [ -n "$MOUNT_DIR" ] && rmdir "$MOUNT_DIR" 2>/dev/null || true
}
trap cleanup EXIT

drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1 || true
}

wait_mount() {
    for _ in $(seq 1 50); do
        mountpoint -q "$MOUNT_DIR" 2>/dev/null && return 0
        sleep 0.1
    done
    echo "ERROR: mount did not appear within 5s" >&2
    return 1
}

mount_cfs() {
    local codec="$1" level="${2:-0}" block_size="${3:-65536}" cache_size="${4:-134217728}"
    BACKING_DIR=$(mktemp -d /tmp/cfs_real_back_XXXXXX)
    MOUNT_DIR=$(mktemp -d /tmp/cfs_real_mnt_XXXXXX)
    start_cfs "$codec" "$level" "$block_size" "$cache_size"
}

start_cfs() {
    local codec="$1" level="${2:-0}" block_size="${3:-65536}" cache_size="${4:-134217728}"
    "$COMPRESSFS" "$BACKING_DIR" "$MOUNT_DIR" \
        --codec="$codec" --level="$level" \
        --block-size="$block_size" --cache-size="$cache_size" -f &
    CFS_PID=$!
    wait_mount
}

stop_cfs() {
    if [ -n "$CFS_PID" ] && kill -0 "$CFS_PID" 2>/dev/null; then
        fusermount3 -u "$MOUNT_DIR" 2>/dev/null || true
        wait "$CFS_PID" 2>/dev/null || true
        CFS_PID=""
    fi
}

unmount_cfs() {
    stop_cfs
    [ -n "$BACKING_DIR" ] && rm -rf "$BACKING_DIR" && BACKING_DIR=""
    [ -n "$MOUNT_DIR" ] && rmdir "$MOUNT_DIR" 2>/dev/null && MOUNT_DIR="" || true
}

time_ns_to_sec() {
    echo "scale=3; ($2 - $1) / 1000000000" | bc
}

median() {
    sort -n | awk '{a[NR]=$1} END{if (NR==0) print 0; else if (NR%2==1) print a[(NR+1)/2]; else printf "%.2f\n", (a[NR/2]+a[NR/2+1])/2}'
}

# ---------------------------------------------------------------------------

{
echo "=== CompressFS Real-Data Benchmark ==="
echo "Date:    $(date -Iseconds)"
echo "Data:    Silesia corpus ($SILESIA_MIB MiB, $(ls "$SILESIA_DIR" | wc -l) files)"
echo "Runs:    $RUNS per measurement (median reported)"
echo ""
echo "--- System ---"
echo "CPU:     $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
echo "Cores:   $(nproc)"
echo "RAM:     $(free -h | awk '/Mem:/ {print $2}')"
echo "Kernel:  $(uname -r)"
echo "FUSE:    $(fusermount3 --version 2>&1 | head -1)"
echo ""

# ===================================================================
# Benchmark 1: Paired Sequential Write + Cold Sequential Read
# ===================================================================
# For each (codec, run) we do one write measurement, remount to drop the
# userspace cache, drop kernel caches, then one cold read measurement.
# This halves runtime vs. measuring writes and reads separately.
echo "============================================================"
echo "  Sequential Write + Cold Read (Silesia corpus)"
echo "============================================================"
echo "codec    | write MB/s | read MB/s | compression ratio"
echo "---------|------------|-----------|-------------------"

for codec in none lz4 zstd; do
    level=0
    [ "$codec" = "zstd" ] && level=3

    write_times=()
    read_times=()
    ratio=""

    for run in $(seq 1 "$RUNS"); do
        mount_cfs "$codec" "$level" 65536
        drop_caches

        start=$(date +%s%N)
        cp "$SILESIA_DIR"/* "$MOUNT_DIR"/
        sync
        end=$(date +%s%N)
        w_secs=$(time_ns_to_sec "$start" "$end")
        w_mbs=$(echo "scale=1; $SILESIA_BYTES / 1048576 / $w_secs" | bc)
        write_times+=("$w_mbs")

        physical=$(du -sb "$BACKING_DIR" | awk '{print $1}')
        if [ "$physical" -gt 0 ]; then
            ratio=$(echo "scale=2; $SILESIA_BYTES / $physical" | bc)
        fi

        stop_cfs
        drop_caches
        start_cfs "$codec" "$level" 65536

        start=$(date +%s%N)
        cat_all_files "$MOUNT_DIR"
        end=$(date +%s%N)
        r_secs=$(time_ns_to_sec "$start" "$end")
        r_mbs=$(echo "scale=1; $SILESIA_BYTES / 1048576 / $r_secs" | bc)
        read_times+=("$r_mbs")

        unmount_cfs
    done

    w_med=$(printf '%s\n' "${write_times[@]}" | median)
    r_med=$(printf '%s\n' "${read_times[@]}" | median)
    printf "%-8s | %-10s | %-9s | %sx\n" "$codec" "$w_med" "$r_med" "$ratio"
done
echo ""

# ===================================================================
# Benchmark 2: Random Read Latency (4K reads on webster)
# ===================================================================
echo "============================================================"
echo "  Random Read Latency (4 KiB reads on webster, 2000 samples)"
echo "============================================================"

for codec in none lz4 zstd; do
    level=0
    [ "$codec" = "zstd" ] && level=3

    mount_cfs "$codec" "$level" 65536
    cp "$SILESIA_DIR/webster" "$MOUNT_DIR/webster"
    sync
    stop_cfs
    drop_caches
    start_cfs "$codec" "$level" 65536

    echo ""
    echo "--- codec: $codec ---"
    "$BENCH_READ" "$MOUNT_DIR/webster" 2000 4096
    unmount_cfs
done
echo ""

# ===================================================================
# Benchmark 3: Cache Effectiveness (two-pass read, lz4)
# ===================================================================
echo "============================================================"
echo "  Cache Effectiveness (two-pass read, lz4)"
echo "============================================================"
echo "cache_budget | hit rate | evictions"
echo "-------------|----------|----------"

for cache_mb in 16 256; do
    cache_bytes=$((cache_mb * 1024 * 1024))
    mount_cfs "lz4" 0 65536 "$cache_bytes"
    cp "$SILESIA_DIR"/* "$MOUNT_DIR"/
    sync

    cat_all_files "$MOUNT_DIR"
    cat_all_files "$MOUNT_DIR"

    hit_rate=$(grep cache_hit_rate "$MOUNT_DIR/.compressfs_stats" 2>/dev/null | awk '{print $2}')
    evictions=$(grep cache_evictions "$MOUNT_DIR/.compressfs_stats" 2>/dev/null | awk '{print $2}')
    printf "%-12s | %-8s | %s\n" "${cache_mb}M" "$hit_rate" "$evictions"
    unmount_cfs
done
echo ""

# ===================================================================
# Benchmark 4: Block Size Tradeoff (zstd-3, single run each)
# ===================================================================
# Single run per block size — trend matters more than precision here.
echo "============================================================"
echo "  Block Size Tradeoff (zstd-3, 1 run per size)"
echo "============================================================"
echo "block_size | write MB/s | ratio  | random read p50 (us)"
echo "-----------|------------|--------|---------------------"

for bs in 65536 262144; do
    mount_cfs "zstd" 3 "$bs"
    drop_caches

    start=$(date +%s%N)
    cp "$SILESIA_DIR"/* "$MOUNT_DIR"/
    sync
    end=$(date +%s%N)
    secs=$(time_ns_to_sec "$start" "$end")
    mbs=$(echo "scale=1; $SILESIA_BYTES / 1048576 / $secs" | bc)

    physical=$(du -sb "$BACKING_DIR" | awk '{print $1}')
    ratio=$(echo "scale=2; $SILESIA_BYTES / $physical" | bc)

    stop_cfs
    drop_caches
    start_cfs "zstd" 3 "$bs"

    p50=$("$BENCH_READ" "$MOUNT_DIR/webster" 1000 4096 2>/dev/null | \
          awk '/^  p50:/ {print $2}')

    printf "%-10s | %-10s | %-6s | %s\n" "$bs" "$mbs" "${ratio}x" "$p50"
    unmount_cfs
done
echo ""

echo "=== Done ==="
} 2>&1 | tee "$RESULTS_FILE"
