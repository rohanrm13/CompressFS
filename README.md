# CompressFS

A compressed filesystem in userspace. You mount it on an empty directory, point it at a backing directory on your real filesystem, and from then on every file you write through the mount point gets split into fixed-size blocks, compressed with LZ4 or Zstd, checksummed, and written to the backing store. Reads go the other way.

I built it to work through the concrete problems that show up when you sit between an application's `read()`/`write()` and a disk. How do you avoid decompressing a whole file for a random 4 KiB read? What do you do when compression actually makes a block bigger? How do you stay consistent across a crash without writing a journal? Where does FUSE overhead actually live? It's a learning project, not something I'd put in front of real data.

---

## Architecture

A POSIX read/write from an application crosses the kernel VFS, hits the FUSE3 callbacks in `fuse_ops.cpp`, gets translated from a byte range into a set of fixed-size blocks by the block manager, and then each block flows through the cache, codec, and metadata layers before landing on the backing store as an atomically-renamed file on the host filesystem.

Components, top to bottom:

- **FUSE callbacks** (`src/fuse_ops.cpp`) — 16 ops: getattr, readdir, create, open, release, read, write, truncate, unlink, rename, chmod, chown, utimens, fsync, flush, init/destroy. Each open file carries a `FileHandle` holding the metadata cache and a dirty flag. A virtual `/.compressfs_stats` file exposes live counters. Read-only xattrs under `user.compressfs.*` expose per-file compression info.
- **Block manager** (`src/block_manager.cpp`) — translates `(offset, size)` into block-aligned work. Read path: cache lookup → backing store → CRC32C verify → decompress → slice into user buffer. Write path: read-modify-write for partial blocks → compress → CRC32C → atomic write → metadata update. Unwritten blocks are sparse and read as zeros.
- **Block cache** (`src/block_cache.cpp`, `src/slab_allocator.cpp`) — LRU over fixed-size decompressed blocks. Hash map + doubly-linked list for O(1) hit/evict. Buffers come from an mmap-backed slab allocator with an intrusive free-list, so allocation is a single pointer swap and there's no fragmentation. Guarded by a `shared_mutex`; write-through, so the cache never holds dirty data. Default budget 128 MiB.
- **Codec layer** (`src/codec*.cpp`) — stateless `CodecBase*` singletons for `none`, `lz4`, `zstd`. Per-block fallback: if compressing a block would expand it, the raw block is stored instead (detected on read by comparing compressed size to block size).
- **Metadata** (`src/metadata.cpp`) — binary v2 format, 80-byte fixed header followed by per-block tables (compressed sizes, per-block CRC32Cs). Two header/table checksums guard the metadata itself; per-block checksums guard the compressed data. CRC32C uses the SSE4.2 `crc32` instruction (~20 GB/s) with a software table fallback (~500 MB/s) picked via CPUID at init.
- **Backing store** (`src/backing_store.cpp`) — POSIX I/O using fd-relative calls (`openat`, `fstatat`). Atomic write protocol: `write(name.tmp)` → `fsync(name.tmp)` → `rename(name.tmp, name)` → `fsync(parent)`. On-disk layout per file is `<backing>/<path>/meta.bin` plus `<backing>/<path>/blocks/NNNN.blk`.

Everything lands on whatever the host filesystem is (ext4, xfs, …) — CompressFS leans on the host for `rename()` atomicity and `fsync()` durability rather than implementing its own journal.

---

## Build & Run

```bash
sudo apt install libfuse3-dev liblz4-dev libzstd-dev cmake g++
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

mkdir -p /tmp/backing /tmp/mnt
./build/compressfs /tmp/backing /tmp/mnt --codec=zstd --level=3 -f

# in another shell
fusermount3 -u /tmp/mnt
```

CLI options: `--codec={none,lz4,zstd}`, `--level=N` (zstd levels), `--block-size=N` (default 65536, min 4096), `--cache-size=N` bytes (default 128 MiB), `-f` foreground, `-d` debug.

Tests: `cd build && ctest --output-on-failure` (9 suites).
Benchmarks: `bash bench/bench_real.sh` (runs on the Silesia corpus; requires sudo for `drop_caches`).

---

## Design decisions

The parts of the project that I actually had to think about. Each of these is a trade-off, and the shape of the project would be different if I'd gone the other way.

### Block-level compression over file-level

Block-level lets me decompress only the block a random read touches. File-level (squashfs-style) would mean decompressing from the start of the file for every random access. The cost is read-modify-write amplification — a 1-byte write in the middle of a block decompresses, overlays, recompresses, and writes the whole block. 64 KiB default is the middle ground: big enough for reasonable ratios, small enough that random reads don't decompress much.

### Slab allocator for cache buffers instead of malloc

Every decompressed block in the cache is exactly `block_size` bytes. Malloc would scatter these across the heap and fragment badly under sustained evict/allocate churn. The slab allocator pre-reserves one `mmap(MAP_PRIVATE|MAP_ANONYMOUS)` region and manages it with an intrusive free-list — each free slab's first 8 bytes hold the next pointer, so alloc/free are a single pointer swap. Fixed block size only, which is fine because the cache only holds fixed-size blocks.

### Write-through cache, not write-back

On every write I persist the compressed block to disk *and* insert the decompressed version into the cache as clean. The cache never holds dirty data. Write-back would improve write latency but introduces dirty-eviction stalls — an eviction that triggers synchronous I/O blocks every other cache operation behind the exclusive lock. Write-through keeps eviction cost predictable and makes crash recovery trivial: the cache is always a disposable acceleration layer, never a source of truth.

### Atomic `tmp → fsync → rename → fsync(parent)` instead of a WAL

Each block is written to a `.tmp` file, fsynced, atomically renamed over the target, and the parent directory is fsynced to make the directory entry durable. Metadata is always written *after* all blocks are durable, so a crash between block writes and metadata update reverts the file to its pre-write state — lost writes, never corruption. The trade-off: no multi-block transaction atomicity. A 3-block write that crashes after 2 blocks leaves those blocks committed but unreferenced by metadata. POSIX doesn't promise multi-block atomicity on a single `write()` either, so this is acceptable for a filesystem — it would not be acceptable for a database.

### CRC32C on every block, verified before decompression

CRC32C instead of CRC32 because of better error-detection properties (Castagnoli polynomial). On x86 with SSE4.2, the `crc32` instruction does 8 bytes/cycle — ~20 GB/s, essentially free at 64 KiB blocks. The software table fallback is ~500 MB/s, which would actually be a bottleneck. CPUID at startup picks between them via a function pointer. Checksum verification happens *before* decompression so we don't waste CPU decompressing corrupt data and don't feed malformed input into the codec library.

### `direct_io` to bypass the kernel page cache

Without `direct_io`, every read/write passes through two caches — the kernel page cache on top of FUSE, and the userspace LRU cache underneath. That doubles memory use for hot data and defeats LRU eviction because the kernel may keep evicted blocks alive that CompressFS doesn't know about. With `direct_io` the userspace cache is the only cache. The trade-off is losing kernel readahead, but block-granularity access doesn't align with page-granularity readahead anyway.

### Per-block codec fallback for incompressible data

If compressing a block produces output ≥ the original size, I store the raw block instead and set `compressed_sizes[i]` to the original size. The read path sees `compressed_size >= block_size` and skips decompression. Without this, already-compressed data (JPEG, MP4, encrypted files) would grow on write *and* waste decompression CPU on every read. Per-block matters because a file can be mixed — e.g. a database with compressed BLOBs next to plain text — and per-block fallback handles this without any per-file heuristic.

---

## Crash safety

Per-block atomicity, no journal. The atomic unit is a single block write:

```
write(tmp) ──► fsync(tmp) ──► rename(tmp→target) ──► fsync(parent)
```

Crash before `rename` → old block intact, orphaned `.tmp` left behind (harmless).
Crash after `rename` but before metadata update → new block on disk, but metadata still points at the old block list, so the file reads back as its pre-write state. Lost write, not corruption.
Crash after metadata update → fully consistent.

Guarantees: no torn writes, no silent corruption (CRC32C verified on read), metadata-last ordering so blocks are durable before metadata references them.

Non-guarantees: no multi-block transaction atomicity, no automatic cleanup of orphaned `.tmp` files, and the whole thing leans on the host filesystem's `rename()` atomicity and `fsync()` durability.

---

## On-disk format

Binary v2, little-endian, no parsing dependencies. Explicit field sizes with a version field for forward compatibility.

```
Offset  Size   Field
0       4      magic          0x43465301 ("CFS\x01")
4       2      version        2
6       2      codec_id       0=none, 1=lz4, 2=zstd
8       2      cksum_type     0=none, 1=crc32c
10      1      comp_level     int8, signed
11      1      reserved
12      4      block_size     default 65536
16      8      original_size  logical file size
24      4      block_count    blocks allocated
28      4      mode           POSIX mode
32      4      uid
36      4      gid
40      12     atime          sec (8) + nsec (4)
52      12     mtime
64      12     ctime
76      4      header_cksum   CRC32C of bytes 0–75
─── variable section ───
80        N×4  compressed_sizes[]   per-block compressed size
80+N×4    N×4  block_checksums[]    per-block CRC32C of compressed data
80+N×8    4    tables_cksum         CRC32C of bytes 80..80+N×8-1

Total: 84 + block_count × 8 bytes. Max file size ~64 GiB at 64 KiB blocks.
```

Deserialize validates magic, version, block_count upper bound (1M), and both checksums. Corrupt metadata fails the file with `EIO` rather than attempting any operation.

---

## Observability

`cat <mount>/.compressfs_stats` returns live counters (blocks read/written, bytes, compressions, decompressions, cache hit/miss/eviction counts, hit rate). The virtual file is synthesized on every `read()` with no buffering.

Read-only xattrs expose per-file compression info:

```bash
getfattr -n user.compressfs.ratio myfile.txt   # "2.50x"
getfattr -n user.compressfs.codec myfile.txt   # "zstd"
```

---

## Known gaps

What I know is missing or rough. Some of these I'll get to, some I won't — I'll move items out of this list as they get fixed, and add new ones as they surface.

- Flat directory structure only — no nested mkdir/rmdir. Real directory handling would need an inode table that duplicates VFS.
- No hard links or symlinks. Reference counting and a separate data path for link targets are orthogonal to compression/caching.
- `setxattr` is unimplemented; `getxattr` only exposes derived compression stats.
- No multi-block transaction atomicity — see the crash-safety section.
- `readdir` scans the backing directory every call; fine for small filesystems, bad at scale.
- No `mmap` — `direct_io` disables it. This rules out the filesystem for databases.
- No encryption. Compress-then-encrypt ordering and key management would be a whole separate subsystem, and encrypted data is incompressible anyway.
- Orphaned `.tmp` files after a crash aren't cleaned up. Harmless but they consume disk.
- No quotas or space reservation; relies on the host filesystem's `ENOSPC`.

---

## License

MIT
