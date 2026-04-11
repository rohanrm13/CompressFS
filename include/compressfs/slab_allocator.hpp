#pragma once

#include <cstddef>
#include <cstdint>

namespace compressfs {

// Fixed-size slab allocator backed by a single mmap region.
class SlabAllocator {
public:
    // Allocates pool_size bytes via mmap, divided into block_size-sized slabs.
    // pool_size is rounded down to a multiple of block_size.
    SlabAllocator(size_t block_size, size_t pool_size);
    ~SlabAllocator();

    SlabAllocator(const SlabAllocator&) = delete;
    SlabAllocator& operator=(const SlabAllocator&) = delete;
    SlabAllocator(SlabAllocator&& other) noexcept;
    SlabAllocator& operator=(SlabAllocator&& other) noexcept;

    // O(1) allocation. Returns nullptr if pool is exhausted.
    [[nodiscard]] uint8_t* allocate();

    // O(1) deallocation. ptr must be within the pool and aligned to block_size.
    void deallocate(uint8_t* ptr);

    // Returns true if ptr was allocated from this pool.
    [[nodiscard]] bool owns(const uint8_t* ptr) const;

    [[nodiscard]] size_t block_size() const { return block_size_; }
    [[nodiscard]] size_t capacity() const { return capacity_; }
    [[nodiscard]] size_t available() const { return available_; }

private:
    uint8_t* pool_ = nullptr;
    size_t pool_size_ = 0;
    size_t block_size_ = 0;
    size_t capacity_ = 0;   // total slabs
    size_t available_ = 0;  // free slabs

    uint8_t* free_head_ = nullptr;
};

} // namespace compressfs
