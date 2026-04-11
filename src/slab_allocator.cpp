#include "compressfs/slab_allocator.hpp"

#include <cassert>
#include <cstring>
#include <sys/mman.h>
#include <utility>

namespace compressfs {

SlabAllocator::SlabAllocator(size_t block_size, size_t pool_size) {
    // block_size must fit at least a pointer for the intrusive free-list
    assert(block_size >= sizeof(void*));
    assert(pool_size > 0);

    block_size_ = block_size;
    capacity_ = pool_size / block_size;
    pool_size_ = capacity_ * block_size; // round down

    if (capacity_ == 0 || pool_size_ == 0)
        return;

    pool_ = static_cast<uint8_t*>(
        ::mmap(nullptr, pool_size_, PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

    if (pool_ == MAP_FAILED) {
        pool_ = nullptr;
        pool_size_ = 0;
        capacity_ = 0;
        return;
    }

    // Build intrusive free-list: each slab's first bytes point to the next free slab.
    // Walk backwards so that allocations come out in forward address order
    // (cache-friendly sequential access pattern).
    free_head_ = nullptr;
    for (size_t i = capacity_; i > 0; --i) {
        uint8_t* slab = pool_ + (i - 1) * block_size_;
        uint8_t* next = free_head_;
        std::memcpy(slab, &next, sizeof(next));
        free_head_ = slab;
    }

    available_ = capacity_;
}

SlabAllocator::~SlabAllocator() {
    if (pool_ && pool_size_ > 0) {
        ::munmap(pool_, pool_size_);
    }
}

SlabAllocator::SlabAllocator(SlabAllocator&& other) noexcept
    : pool_(other.pool_),
      pool_size_(other.pool_size_),
      block_size_(other.block_size_),
      capacity_(other.capacity_),
      available_(other.available_),
      free_head_(other.free_head_) {
    other.pool_ = nullptr;
    other.pool_size_ = 0;
    other.capacity_ = 0;
    other.available_ = 0;
    other.free_head_ = nullptr;
}

SlabAllocator& SlabAllocator::operator=(SlabAllocator&& other) noexcept {
    if (this != &other) {
        if (pool_ && pool_size_ > 0)
            ::munmap(pool_, pool_size_);

        pool_ = other.pool_;
        pool_size_ = other.pool_size_;
        block_size_ = other.block_size_;
        capacity_ = other.capacity_;
        available_ = other.available_;
        free_head_ = other.free_head_;

        other.pool_ = nullptr;
        other.pool_size_ = 0;
        other.capacity_ = 0;
        other.available_ = 0;
        other.free_head_ = nullptr;
    }
    return *this;
}

uint8_t* SlabAllocator::allocate() {
    if (!free_head_)
        return nullptr;

    uint8_t* slab = free_head_;

    // Pop from free-list: read next pointer embedded in the slab
    uint8_t* next;
    std::memcpy(&next, slab, sizeof(next));
    free_head_ = next;

    --available_;
    return slab;
}

void SlabAllocator::deallocate(uint8_t* ptr) {
    assert(owns(ptr));

    // Push onto free-list: store current head in the slab's first bytes
    std::memcpy(ptr, &free_head_, sizeof(free_head_));
    free_head_ = ptr;

    ++available_;
}

bool SlabAllocator::owns(const uint8_t* ptr) const {
    if (!pool_ || !ptr)
        return false;
    if (ptr < pool_ || ptr >= pool_ + pool_size_)
        return false;
    // Must be aligned to block_size boundary within the pool
    auto offset = static_cast<size_t>(ptr - pool_);
    return (offset % block_size_) == 0;
}

} // namespace compressfs
