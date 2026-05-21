#include "common/buffer_pool.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <thread>

namespace hypersync {
namespace {

inline constexpr std::uint32_t kInvalidFreeIndex = std::numeric_limits<std::uint32_t>::max();

// Buffer-pool design notes
//
// The pipeline moves ownership, not payload bytes. RawBufferPool allocates a fixed
// array of equal-size byte slots at startup. Each slot is identified by a
// BufferHandle: pool id + slot index + generation. The generation changes every
// time a slot is released, so stale handles and double releases are caught early.
// The pool free list is a preallocated index vector protected by a short mutex;
// it allocates no memory after construction and keeps correctness simple.
//
// BufQueue is the job-to-job handoff primitive. It is a bounded MPMC ring using
// per-cell sequence numbers, with a separate available-slot counter enforcing the
// configured logical max depth even when the internal ring is rounded to a power
// of two. try_push()/try_pop() are the hot non-blocking operations. push_wait()
// and pop_wait() add condition-variable sleeping for low-CPU control paths.
//
// Queue nodes contain only BufferHandle values. The queue never owns, copies, or
// interprets payload memory. Type interpretation happens only at the edge via
// buffer_as<T>() after a job already owns a handle.

[[nodiscard]] std::size_t next_power_of_two_at_least(std::size_t value) {
    if (value == 0U) {
        return 1U;
    }
    if (value > (std::numeric_limits<std::size_t>::max() / 2U + 1U)) {
        throw std::overflow_error("capacity too large to round to power of two");
    }

    std::size_t rounded = 1U;
    while (rounded < value) {
        rounded <<= 1U;
    }
    return rounded;
}

[[nodiscard]] std::size_t round_up_to_alignment(std::size_t value, std::size_t alignment) {
    if (alignment == 0U || (alignment & (alignment - 1U)) != 0U) {
        throw std::invalid_argument("alignment must be a non-zero power of two");
    }
    if (value > std::numeric_limits<std::size_t>::max() - (alignment - 1U)) {
        throw std::overflow_error("value too large to align");
    }
    return (value + alignment - 1U) & ~(alignment - 1U);
}

}  // namespace

BufQueue::BufQueue(std::size_t requested_capacity)
    : max_size_(std::max<std::size_t>(1U, requested_capacity)),
      ring_capacity_(next_power_of_two_at_least(std::max<std::size_t>(2U, max_size_))),
      mask_(ring_capacity_ - 1U),
      cells_(ring_capacity_),
      available_slots_(max_size_) {
    for (std::size_t index = 0; index < ring_capacity_; ++index) {
        cells_[index].sequence.store(index, std::memory_order_relaxed);
    }
}

std::size_t BufQueue::capacity() const noexcept {
    return max_size_;
}

std::size_t BufQueue::available_slots() const noexcept {
    return available_slots_.load(std::memory_order_acquire);
}

std::size_t BufQueue::ring_capacity() const noexcept {
    return ring_capacity_;
}

std::size_t BufQueue::size() const noexcept {
    return depth_.load(std::memory_order_acquire);
}

std::size_t BufQueue::high_watermark() const noexcept {
    return high_watermark_.load(std::memory_order_acquire);
}

std::uint64_t BufQueue::push_count() const noexcept {
    return push_count_.load(std::memory_order_acquire);
}

std::uint64_t BufQueue::pop_count() const noexcept {
    return pop_count_.load(std::memory_order_acquire);
}

bool BufQueue::empty() const noexcept {
    return size() == 0U;
}

bool BufQueue::full() const noexcept {
    return available_slots_.load(std::memory_order_acquire) == 0U;
}

bool BufQueue::closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
}

void BufQueue::close() noexcept {
    closed_.store(true, std::memory_order_release);
    cv_not_full_.notify_all();
    cv_not_empty_.notify_all();
}

bool BufQueue::try_push(const BufferHandle& value) {
    if (closed()) {
        return false;
    }
    if (!try_reserve_slot()) {
        return false;
    }

    try {
        if (!enqueue_reserved(value)) {
            release_reserved_slot();
            return false;
        }
    } catch (...) {
        release_reserved_slot();
        throw;
    }
    return true;
}

bool BufQueue::try_pop(BufferHandle& out) {
    Cell* cell = nullptr;
    std::size_t position = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;) {
        cell = &cells_[position & mask_];
        const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
        const auto difference = static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position + 1U);
        if (difference == 0) {
            if (dequeue_pos_.compare_exchange_weak(position,
                                                   position + 1U,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
                break;
            }
            continue;
        }
        if (difference < 0) {
            return false;
        }
        position = dequeue_pos_.load(std::memory_order_relaxed);
    }

    out = cell->value;
    depth_.fetch_sub(1U, std::memory_order_acq_rel);
    pop_count_.fetch_add(1U, std::memory_order_relaxed);
    cell->sequence.store(position + ring_capacity_, std::memory_order_release);
    release_reserved_slot();
    return true;
}

bool BufQueue::push_wait(const BufferHandle& value) {
    while (!closed()) {
        if (try_push(value)) {
            return true;
        }
        std::unique_lock<std::mutex> lock(wait_mutex_);
        cv_not_full_.wait(lock, [this] {
            return closed() || available_slots_.load(std::memory_order_acquire) != 0U;
        });
    }
    return false;
}

bool BufQueue::pop_wait(BufferHandle& out) {
    while (!closed() || !empty()) {
        if (try_pop(out)) {
            return true;
        }
        std::unique_lock<std::mutex> lock(wait_mutex_);
        cv_not_empty_.wait(lock, [this] {
            return closed() || !empty();
        });
    }
    return false;
}

bool BufQueue::push_spin(const BufferHandle& value) {
    while (!closed()) {
        if (try_push(value)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool BufQueue::pop_spin(BufferHandle& out) {
    while (!closed() || !empty()) {
        if (try_pop(out)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool BufQueue::try_reserve_slot() noexcept {
    std::size_t available = available_slots_.load(std::memory_order_acquire);
    while (available != 0U) {
        if (available_slots_.compare_exchange_weak(available,
                                                   available - 1U,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

void BufQueue::release_reserved_slot() noexcept {
    available_slots_.fetch_add(1U, std::memory_order_acq_rel);
    cv_not_full_.notify_one();
}

bool BufQueue::enqueue_reserved(const BufferHandle& value) {
    if (closed()) {
        return false;
    }

    Cell* cell = nullptr;
    std::size_t position = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
        cell = &cells_[position & mask_];
        const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
        const auto difference = static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position);
        if (difference == 0) {
            if (enqueue_pos_.compare_exchange_weak(position,
                                                   position + 1U,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
                break;
            }
            continue;
        }
        if (difference < 0) {
            return false;
        }
        position = enqueue_pos_.load(std::memory_order_relaxed);
    }

    cell->value = value;
    cell->sequence.store(position + 1U, std::memory_order_release);
    const std::size_t new_depth = depth_.fetch_add(1U, std::memory_order_acq_rel) + 1U;
    update_high_watermark(new_depth);
    push_count_.fetch_add(1U, std::memory_order_relaxed);
    cv_not_empty_.notify_one();
    return true;
}

void BufQueue::update_high_watermark(std::size_t value) noexcept {
    value = std::min(value, max_size_);
    std::size_t observed = high_watermark_.load(std::memory_order_relaxed);
    while (value > observed &&
           !high_watermark_.compare_exchange_weak(observed,
                                                   value,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
    }
}

ShardedBufQueue::ShardedBufQueue(std::size_t shard_count, std::size_t per_shard_capacity) {
    if (shard_count == 0U) {
        throw std::invalid_argument("sharded buffer queue must have at least one shard");
    }
    shards_.reserve(shard_count);
    for (std::size_t index = 0; index < shard_count; ++index) {
        shards_.push_back(std::make_unique<BufQueue>(per_shard_capacity));
    }
}

std::size_t ShardedBufQueue::shard_count() const noexcept {
    return shards_.size();
}

std::size_t ShardedBufQueue::capacity() const noexcept {
    std::size_t total = 0;
    for (const auto& queue : shards_) {
        total += queue->capacity();
    }
    return total;
}

std::size_t ShardedBufQueue::size() const noexcept {
    std::size_t total = 0;
    for (const auto& queue : shards_) {
        total += queue->size();
    }
    return total;
}

std::size_t ShardedBufQueue::high_watermark() const noexcept {
    return high_watermark_.load(std::memory_order_acquire);
}

std::uint64_t ShardedBufQueue::push_count() const noexcept {
    std::uint64_t total = 0;
    for (const auto& queue : shards_) {
        total += queue->push_count();
    }
    return total;
}

std::uint64_t ShardedBufQueue::pop_count() const noexcept {
    std::uint64_t total = 0;
    for (const auto& queue : shards_) {
        total += queue->pop_count();
    }
    return total;
}

bool ShardedBufQueue::empty() const noexcept {
    for (const auto& queue : shards_) {
        if (!queue->empty()) {
            return false;
        }
    }
    return true;
}

bool ShardedBufQueue::closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
}

BufQueue& ShardedBufQueue::shard(std::size_t shard_index) {
    return *shards_[normalize_shard(shard_index)];
}

const BufQueue& ShardedBufQueue::shard(std::size_t shard_index) const {
    return *shards_[normalize_shard(shard_index)];
}

void ShardedBufQueue::close() noexcept {
    closed_.store(true, std::memory_order_release);
    for (auto& queue : shards_) {
        queue->close();
    }
    cv_not_empty_.notify_all();
}

bool ShardedBufQueue::try_push(std::size_t preferred_shard, const BufferHandle& value) {
    if (shard_count() == 1U) {
        return shards_.front()->try_push(value);
    }
    if (closed()) {
        return false;
    }
    if (!shard(preferred_shard).try_push(value)) {
        return false;
    }
    update_high_watermark(size());
    notify_not_empty();
    return true;
}

bool ShardedBufQueue::try_pop(std::size_t preferred_shard, BufferHandle& out) {
    const std::size_t shard_total = shard_count();
    const std::size_t start = normalize_shard(preferred_shard);
    if (shard_total == 1U) {
        return shards_.front()->try_pop(out);
    }
    if (shards_[start]->try_pop(out)) {
        return true;
    }

    // Work stealing drains imbalanced long-tail shards while preserving the
    // preferred-shard fast path when the pipeline is balanced.
    for (std::size_t offset = 1; offset < shard_total; ++offset) {
        const std::size_t index = (start + offset) % shard_total;
        if (shards_[index]->try_pop(out)) {
            return true;
        }
    }
    return false;
}

bool ShardedBufQueue::push_wait(std::size_t preferred_shard, const BufferHandle& value) {
    if (shard_count() == 1U) {
        return shards_.front()->push_wait(value);
    }
    while (!closed()) {
        if (try_push(preferred_shard, value)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool ShardedBufQueue::pop_wait(std::size_t preferred_shard, BufferHandle& out) {
    if (shard_count() == 1U) {
        return shards_.front()->pop_wait(out);
    }
    while (!closed() || !empty()) {
        if (try_pop(preferred_shard, out)) {
            return true;
        }
        std::unique_lock<std::mutex> lock(wait_mutex_);
        cv_not_empty_.wait_for(lock, std::chrono::milliseconds(1), [this] {
            return closed() || !empty();
        });
    }
    return false;
}

bool ShardedBufQueue::push_spin(std::size_t preferred_shard, const BufferHandle& value) {
    if (shard_count() == 1U) {
        return shards_.front()->push_spin(value);
    }
    while (!closed()) {
        if (try_push(preferred_shard, value)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool ShardedBufQueue::pop_spin(std::size_t preferred_shard, BufferHandle& out) {
    if (shard_count() == 1U) {
        return shards_.front()->pop_spin(out);
    }
    while (!closed() || !empty()) {
        if (try_pop(preferred_shard, out)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

std::size_t ShardedBufQueue::normalize_shard(std::size_t shard_index) const noexcept {
    return shard_index % shards_.size();
}

void ShardedBufQueue::notify_not_empty() noexcept {
    cv_not_empty_.notify_one();
}

void ShardedBufQueue::update_high_watermark(std::size_t value) noexcept {
    value = std::min(value, capacity());
    std::size_t observed = high_watermark_.load(std::memory_order_relaxed);
    while (value > observed &&
           !high_watermark_.compare_exchange_weak(observed,
                                                   value,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
    }
}

RawBufferPool::RawBufferPool(BufferPoolId pool_id, std::size_t capacity, std::size_t buffer_size_bytes)
    : RawBufferPool(pool_id, capacity, buffer_size_bytes, alignof(std::max_align_t)) {}

RawBufferPool::RawBufferPool(BufferPoolId pool_id,
                             std::size_t capacity,
                             std::size_t buffer_size_bytes,
                             std::size_t alignment)
    : pool_id_(pool_id),
      capacity_(capacity),
      buffer_size_bytes_(std::max<std::size_t>(1U, buffer_size_bytes)),
      stride_size_bytes_(round_up_to_alignment(buffer_size_bytes_, alignment)),
      storage_size_bytes_(checked_storage_bytes(capacity_, stride_size_bytes_)),
      storage_(allocate_storage(storage_size_bytes_, alignment)),
      free_indices_(),
      available_count_(capacity),
      generations_(capacity),
      owned_(capacity) {
    if (pool_id_ == 0U) {
        throw std::invalid_argument("pool id 0 is reserved for an empty buffer handle");
    }
    if (capacity_ > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("buffer pool capacity exceeds BufferHandle index range");
    }
    free_indices_.reserve(capacity);
    for (std::size_t index = 0; index < capacity; ++index) {
        generations_[index].store(1U, std::memory_order_relaxed);
        owned_[index].store(0U, std::memory_order_relaxed);
        free_indices_.push_back(static_cast<std::uint32_t>(capacity - index - 1U));
    }
}

std::size_t RawBufferPool::capacity() const noexcept {
    return capacity_;
}

BufferPoolId RawBufferPool::pool_id() const noexcept {
    return pool_id_;
}

std::size_t RawBufferPool::element_size_bytes() const noexcept {
    return buffer_size_bytes_;
}

std::size_t RawBufferPool::buffer_size_bytes() const noexcept {
    return buffer_size_bytes_;
}

std::size_t RawBufferPool::stride_size_bytes() const noexcept {
    return stride_size_bytes_;
}

std::size_t RawBufferPool::total_size_bytes() const noexcept {
    return storage_size_bytes_;
}

std::size_t RawBufferPool::available() const noexcept {
    return available_count_.load(std::memory_order_acquire);
}

std::size_t RawBufferPool::in_use() const noexcept {
    return in_use_.load(std::memory_order_acquire);
}

std::size_t RawBufferPool::peak_in_use() const noexcept {
    return peak_in_use_.load(std::memory_order_acquire);
}

std::optional<BufferHandle> RawBufferPool::try_acquire() {
    const std::uint32_t index = pop_free_index();
    if (index == kInvalidFreeIndex) {
        return std::nullopt;
    }

    std::uint8_t expected = 0U;
    if (!owned_[index].compare_exchange_strong(expected,
                                               1U,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        throw std::logic_error("free list returned an already owned buffer handle");
    }

    const std::size_t now_in_use = in_use_.fetch_add(1U, std::memory_order_acq_rel) + 1U;
    update_peak_in_use(now_in_use);
    return BufferHandle{pool_id_, index, generations_[index].load(std::memory_order_acquire)};
}

BufferHandle RawBufferPool::acquire_spin() {
    for (;;) {
        if (auto handle = try_acquire(); handle.has_value()) {
            return *handle;
        }
        std::this_thread::yield();
    }
}

BufferHandle RawBufferPool::acquire_wait() {
    for (;;) {
        if (auto handle = try_acquire(); handle.has_value()) {
            return *handle;
        }
        std::unique_lock<std::mutex> lock(wait_mutex_);
        cv_available_.wait(lock, [this] {
            return available_count_.load(std::memory_order_acquire) != 0U;
        });
    }
}

void RawBufferPool::release(const BufferHandle& handle) {
    validate_live_handle(handle);
    std::uint8_t expected = 1U;
    if (!owned_[handle.index].compare_exchange_strong(expected,
                                                      0U,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
        throw std::logic_error("buffer handle is not owned or was already released");
    }

    std::uint32_t next_generation = handle.generation + 1U;
    if (next_generation == 0U) {
        next_generation = 1U;
    }
    generations_[handle.index].store(next_generation, std::memory_order_release);
    in_use_.fetch_sub(1U, std::memory_order_acq_rel);
    push_free_index(handle.index);
    cv_available_.notify_one();
}

std::byte* RawBufferPool::data(const BufferHandle& handle) {
    validate_live_handle(handle);
    return slot_data_unchecked(handle.index);
}

const std::byte* RawBufferPool::data(const BufferHandle& handle) const {
    validate_live_handle(handle);
    return slot_data_unchecked(handle.index);
}

std::byte* RawBufferPool::slot_data_unchecked(std::size_t index) noexcept {
    return storage_.get() + index * stride_size_bytes_;
}

const std::byte* RawBufferPool::slot_data_unchecked(std::size_t index) const noexcept {
    return storage_.get() + index * stride_size_bytes_;
}

std::size_t RawBufferPool::checked_storage_bytes(std::size_t capacity, std::size_t stride) {
    if (stride != 0U && capacity > std::numeric_limits<std::size_t>::max() / stride) {
        throw std::overflow_error("buffer pool storage size overflow");
    }
    return capacity * stride;
}

std::unique_ptr<std::byte, RawBufferPool::RawStorageDeleter> RawBufferPool::allocate_storage(std::size_t bytes,
                                                                                             std::size_t alignment) {
    if (bytes == 0U) {
        return {nullptr, RawStorageDeleter{alignment}};
    }
    return {static_cast<std::byte*>(::operator new(bytes, std::align_val_t(alignment))),
            RawStorageDeleter{alignment}};
}

void RawBufferPool::validate_handle_index(const BufferHandle& handle) const {
    if (handle.pool_id != pool_id_) {
        throw std::logic_error("buffer handle pool id does not match pool");
    }
    if (handle.index >= capacity_) {
        throw std::logic_error("buffer handle index is out of range");
    }
}

void RawBufferPool::validate_live_handle(const BufferHandle& handle) const {
    validate_handle_index(handle);
    const std::uint32_t generation = generations_[handle.index].load(std::memory_order_acquire);
    if (handle.generation != generation) {
        throw std::logic_error("stale buffer handle generation");
    }
    if (owned_[handle.index].load(std::memory_order_acquire) == 0U) {
        throw std::logic_error("buffer handle is not currently owned");
    }
}

void RawBufferPool::update_peak_in_use(std::size_t value) noexcept {
    std::size_t observed = peak_in_use_.load(std::memory_order_relaxed);
    while (value > observed &&
           !peak_in_use_.compare_exchange_weak(observed,
                                                value,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
    }
}

std::uint32_t RawBufferPool::pop_free_index() {
    std::lock_guard<std::mutex> lock(free_mutex_);
    if (free_indices_.empty()) {
        return kInvalidFreeIndex;
    }
    const std::uint32_t index = free_indices_.back();
    free_indices_.pop_back();
    available_count_.fetch_sub(1U, std::memory_order_acq_rel);
    return index;
}

void RawBufferPool::push_free_index(std::uint32_t index) {
    std::lock_guard<std::mutex> lock(free_mutex_);
    free_indices_.push_back(index);
    available_count_.fetch_add(1U, std::memory_order_acq_rel);
}

void BufferPoolRegistry::register_pool(RawBufferPool& pool) {
    RawBufferPool*& slot = pools_[pool.pool_id()];
    if (slot != nullptr && slot != &pool) {
        throw std::logic_error("buffer pool id already registered");
    }
    slot = &pool;
}

RawBufferPool& BufferPoolRegistry::pool(BufferPoolId pool_id) const {
    RawBufferPool* result = pools_[pool_id];
    if (result == nullptr) {
        throw std::logic_error("buffer pool id is not registered");
    }
    return *result;
}

void BufferPoolRegistry::release(const BufferHandle& handle) const {
    pool(handle.pool_id).release(handle);
}

}  // namespace hypersync
