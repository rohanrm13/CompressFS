cmake_minimum_required(VERSION 3.20)
project(compressfs VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(
    -Wall -Wextra -Wpedantic
    -Wshadow -Wconversion -Wsign-conversion
    -Wstrict-aliasing=2
    -fno-exceptions -fno-rtti
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

# --- Backing store static library ---
add_library(compressfs_backing STATIC
    src/metadata.cpp
    src/backing_store.cpp
)
target_include_directories(compressfs_backing PUBLIC include)
target_compile_definitions(compressfs_backing PRIVATE _GNU_SOURCE)

# --- LZ4 and Zstd compression libraries ---
find_path(LZ4_INCLUDE_DIR lz4.h)
find_library(LZ4_LIBRARY lz4)
if(NOT LZ4_INCLUDE_DIR OR NOT LZ4_LIBRARY)
    message(FATAL_ERROR "liblz4-dev not found. Install with: sudo apt install liblz4-dev")
endif()

find_path(ZSTD_INCLUDE_DIR zstd.h)
find_library(ZSTD_LIBRARY zstd)
if(NOT ZSTD_INCLUDE_DIR OR NOT ZSTD_LIBRARY)
    message(FATAL_ERROR "libzstd-dev not found. Install with: sudo apt install libzstd-dev")
endif()

# --- Codec + block manager + cache library ---
add_library(compressfs_codec STATIC
    src/codec.cpp
    src/codec_lz4.cpp
    src/codec_zstd.cpp
    src/slab_allocator.cpp
    src/block_cache.cpp
    src/block_manager.cpp
)
target_include_directories(compressfs_codec PUBLIC include ${LZ4_INCLUDE_DIR} ${ZSTD_INCLUDE_DIR})
target_link_libraries(compressfs_codec PUBLIC compressfs_backing ${LZ4_LIBRARY} ${ZSTD_LIBRARY})

# --- FUSE3 dependency ---
# Why find_path/find_library instead of pkg_check_modules: avoids requiring
# pkg-config binary to be installed. The .pc file may exist but the tool may not.
find_path(FUSE3_INCLUDE_DIR fuse3/fuse.h)
find_library(FUSE3_LIBRARY fuse3)
if(NOT FUSE3_INCLUDE_DIR OR NOT FUSE3_LIBRARY)
    message(FATAL_ERROR "libfuse3-dev not found. Install with: sudo apt install libfuse3-dev")
endif()

# --- FUSE operations library ---
add_library(compressfs_fuse STATIC
    src/fuse_ops.cpp
)
target_include_directories(compressfs_fuse PUBLIC include ${FUSE3_INCLUDE_DIR})
target_link_libraries(compressfs_fuse PUBLIC compressfs_codec ${FUSE3_LIBRARY})
target_compile_definitions(compressfs_fuse PRIVATE FUSE_USE_VERSION=31 _GNU_SOURCE)

# --- Main executable ---
add_executable(compressfs src/main.cpp)
target_link_libraries(compressfs PRIVATE compressfs_fuse)

# --- Benchmark tools (not part of ctest — run manually) ---
add_executable(bench_random_read bench/bench_random_read.cpp)
target_compile_options(bench_random_read PRIVATE -O2)

# --- Tests ---
enable_testing()

add_executable(test_metadata tests/test_metadata.cpp)
target_link_libraries(test_metadata PRIVATE compressfs_backing)
add_test(NAME metadata COMMAND test_metadata)

add_executable(test_backing_store tests/test_backing_store.cpp)
target_link_libraries(test_backing_store PRIVATE compressfs_backing)
add_test(NAME backing_store COMMAND test_backing_store)

add_executable(test_crash_safety tests/test_crash_safety.cpp)
target_link_libraries(test_crash_safety PRIVATE compressfs_backing)
add_test(NAME crash_safety COMMAND test_crash_safety)

add_executable(test_fuse_ops tests/test_fuse_ops.cpp)
target_link_libraries(test_fuse_ops PRIVATE compressfs_fuse)
add_test(NAME fuse_ops COMMAND test_fuse_ops)

add_executable(test_codec tests/test_codec.cpp)
target_link_libraries(test_codec PRIVATE compressfs_codec)
add_test(NAME codec COMMAND test_codec)

add_executable(test_block_manager tests/test_block_manager.cpp)
target_link_libraries(test_block_manager PRIVATE compressfs_codec)
add_test(NAME block_manager COMMAND test_block_manager)

add_executable(test_slab_allocator tests/test_slab_allocator.cpp)
target_link_libraries(test_slab_allocator PRIVATE compressfs_codec)
add_test(NAME slab_allocator COMMAND test_slab_allocator)

add_executable(test_block_cache tests/test_block_cache.cpp)
target_link_libraries(test_block_cache PRIVATE compressfs_codec)
add_test(NAME block_cache COMMAND test_block_cache)

add_executable(test_stats tests/test_stats.cpp)
target_include_directories(test_stats PRIVATE include)
target_link_libraries(test_stats PRIVATE pthread)
add_test(NAME stats COMMAND test_stats)
