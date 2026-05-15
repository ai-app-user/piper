#ifndef HYPERSYNC_COMMON_BUFFER_POOL_HPP
#define HYPERSYNC_COMMON_BUFFER_POOL_HPP

// Raw buffer ownership primitives for the job pipeline.
//
// This layer knows only about fixed-size memory buffers and BufferHandle ownership.
// It does not know whether a buffer contains metadata, file data, hash state, or
// any future payload type. Jobs exchange BufferHandle values through BufQueue, and
// payload-specific code may interpret an owned raw buffer with buffer_as<T>().

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace hypersync {

using BufferPoolId = std::uint16_t;

struct BufferHandle {
    BufferPoolId pool_id = 0;
    std::uint32_t index = 0;
    std::uint32_t generation = 0;

    [[nodiscard]] friend bool operator==(const BufferHandle& lhs, const BufferHandle& rhs) noexcept {
        return lhs.pool_id == rhs.pool_id && lhs.index == rhs.index && lhs.generation == rhs.generation;
    }

    [[nodiscard]] friend bool operator!=(const BufferHandle& lhs, const BufferHandle& rhs) noexcept {
        return !(lhs == rhs);
    }
};

class BufQueue {
public:
    // Create a bounded MPMC queue that can hold at most requested_capacity handles.
    explicit BufQueue(std::size_t requested_capacity);

    BufQueue(const BufQueue&) = delete;
    BufQueue& operator=(const BufQueue&) = delete;

    // Configured max number of handles visible to producers and consumers.
    [[nodiscard]] std::size_t capacity() const noexcept;

    // Number of producer slots currently available before try_push() reports full.
    [[nodiscard]] std::size_t available_slots() const noexcept;

    // Internal rounded ring size; exposed for diagnostics/tests, not flow control.
    [[nodiscard]] std::size_t ring_capacity() const noexcept;

    // Approximate current depth. Under concurrency this is for stats, not ownership.
    [[nodiscard]] std::size_t size() const noexcept;

    // Highest observed depth, clamped to configured capacity.
    [[nodiscard]] std::size_t high_watermark() const noexcept;

    // Total successful pushes.
    [[nodiscard]] std::uint64_t push_count() const noexcept;

    // Total successful pops.
    [[nodiscard]] std::uint64_t pop_count() const noexcept;

    // True when no handles are currently visible to consumers.
    [[nodiscard]] bool empty() const noexcept;

    // True when try_push() would fail because the configured max depth is reached.
    [[nodiscard]] bool full() const noexcept;

    // True after close() has been called.
    [[nodiscard]] bool closed() const noexcept;

    // Stop accepting new pushes and wake waiters; queued handles remain owned by the queue.
    void close() noexcept;

    // Non-blocking ownership transfer into the queue. Returns false if full or closed.
    bool try_push(const BufferHandle& value);

    // Non-blocking ownership transfer out of the queue. Returns false if no handle is ready.
    bool try_pop(BufferHandle& out);

    // Sleep until the handle is pushed or the queue is closed.
    bool push_wait(const BufferHandle& value);

    // Sleep until a handle is popped, or return false once closed and drained.
    bool pop_wait(BufferHandle& out);

    // Yield/retry until the handle is pushed or the queue is closed.
    bool push_spin(const BufferHandle& value);

    // Yield/retry until a handle is popped, or return false once closed and drained.
    bool pop_spin(BufferHandle& out);

private:
    struct alignas(64) Cell {
        std::atomic<std::size_t> sequence {0};
        BufferHandle value {};
    };

    bool try_reserve_slot() noexcept;
    void release_reserved_slot() noexcept;
    bool enqueue_reserved(const BufferHandle& value);
    void update_high_watermark(std::size_t value) noexcept;

    const std::size_t max_size_;
    const std::size_t ring_capacity_;
    const std::size_t mask_;
    std::vector<Cell> cells_;
    alignas(64) std::atomic<std::size_t> enqueue_pos_ {0};
    alignas(64) std::atomic<std::size_t> dequeue_pos_ {0};
    alignas(64) std::atomic<std::size_t> available_slots_ {0};
    alignas(64) std::atomic<std::size_t> depth_ {0};
    std::atomic<std::size_t> high_watermark_ {0};
    std::atomic<std::uint64_t> push_count_ {0};
    std::atomic<std::uint64_t> pop_count_ {0};
    std::atomic<bool> closed_ {false};
    mutable std::mutex wait_mutex_;
    std::condition_variable cv_not_full_;
    std::condition_variable cv_not_empty_;
};

class ShardedBufQueue {
public:
    // Create shard_count independent BufQueue shards, each with per_shard_capacity.
    ShardedBufQueue(std::size_t shard_count, std::size_t per_shard_capacity);

    ShardedBufQueue(const ShardedBufQueue&) = delete;
    ShardedBufQueue& operator=(const ShardedBufQueue&) = delete;

    // Number of independent queue shards.
    [[nodiscard]] std::size_t shard_count() const noexcept;

    // Total logical capacity across all shards.
    [[nodiscard]] std::size_t capacity() const noexcept;

    // Approximate total current depth across all shards.
    [[nodiscard]] std::size_t size() const noexcept;

    // Highest observed total depth across all shards.
    [[nodiscard]] std::size_t high_watermark() const noexcept;

    // Total successful pushes across all shards.
    [[nodiscard]] std::uint64_t push_count() const noexcept;

    // Total successful pops across all shards.
    [[nodiscard]] std::uint64_t pop_count() const noexcept;

    // True when all shards are empty.
    [[nodiscard]] bool empty() const noexcept;

    // True after close() has been called on the shard set.
    [[nodiscard]] bool closed() const noexcept;

    // Access an individual shard for diagnostics and compatibility wiring.
    [[nodiscard]] BufQueue& shard(std::size_t shard_index);
    [[nodiscard]] const BufQueue& shard(std::size_t shard_index) const;

    // Close every shard and wake sharded waiters.
    void close() noexcept;

    // Push to the preferred shard. Returns false if that shard is full/closed.
    bool try_push(std::size_t preferred_shard, const BufferHandle& value);

    // Pop from preferred_shard first, then steal from other shards if it is empty.
    bool try_pop(std::size_t preferred_shard, BufferHandle& out);

    // Sleep until the handle is pushed to preferred_shard or the shard set closes.
    bool push_wait(std::size_t preferred_shard, const BufferHandle& value);

    // Sleep until any shard has work, trying preferred_shard before stealing.
    bool pop_wait(std::size_t preferred_shard, BufferHandle& out);

    // Yield/retry until the handle is pushed or the shard set closes.
    bool push_spin(std::size_t preferred_shard, const BufferHandle& value);

    // Yield/retry until any shard has work, or return false once closed and drained.
    bool pop_spin(std::size_t preferred_shard, BufferHandle& out);

private:
    [[nodiscard]] std::size_t normalize_shard(std::size_t shard_index) const noexcept;
    void notify_not_empty() noexcept;
    void update_high_watermark(std::size_t value) noexcept;

    std::vector<std::unique_ptr<BufQueue>> shards_;
    std::atomic<std::size_t> high_watermark_ {0};
    std::atomic<bool> closed_ {false};
    mutable std::mutex wait_mutex_;
    std::condition_variable cv_not_empty_;
};

class RawBufferPool {
public:
    // Create a pool of capacity raw byte slots using default max_align_t alignment.
    RawBufferPool(BufferPoolId pool_id, std::size_t capacity, std::size_t buffer_size_bytes);

    // Create a pool of capacity raw byte slots with explicit power-of-two alignment.
    RawBufferPool(BufferPoolId pool_id,
                  std::size_t capacity,
                  std::size_t buffer_size_bytes,
                  std::size_t alignment);

    RawBufferPool(const RawBufferPool&) = delete;
    RawBufferPool& operator=(const RawBufferPool&) = delete;

    // Number of buffers preallocated in this pool.
    [[nodiscard]] std::size_t capacity() const noexcept;

    // Opaque pool namespace encoded into every handle acquired from this pool.
    [[nodiscard]] BufferPoolId pool_id() const noexcept;

    // Logical bytes available in each buffer. Alias kept for stats terminology.
    [[nodiscard]] std::size_t element_size_bytes() const noexcept;

    // Logical bytes available in each buffer, excluding alignment padding.
    [[nodiscard]] std::size_t buffer_size_bytes() const noexcept;

    // Actual byte distance between adjacent slots, including alignment padding.
    [[nodiscard]] std::size_t stride_size_bytes() const noexcept;

    // Total preallocated storage bytes.
    [[nodiscard]] std::size_t total_size_bytes() const noexcept;

    // Number of currently free buffers.
    [[nodiscard]] std::size_t available() const noexcept;

    // Number of buffers currently owned outside the free list.
    [[nodiscard]] std::size_t in_use() const noexcept;

    // Highest observed in-use count.
    [[nodiscard]] std::size_t peak_in_use() const noexcept;

    // Non-blocking acquire. Returns empty when no buffers are free.
    [[nodiscard]] std::optional<BufferHandle> try_acquire();

    // Yield/retry until a buffer is acquired.
    [[nodiscard]] BufferHandle acquire_spin();

    // Block until a buffer is acquired. Use this for backpressure paths where
    // burning CPU while the downstream stage drains would hide the real limit.
    [[nodiscard]] BufferHandle acquire_wait();

    // Return an owned buffer to the pool and invalidate the old generation.
    void release(const BufferHandle& handle);

    // Raw byte pointer for an owned handle.
    [[nodiscard]] std::byte* data(const BufferHandle& handle);

    // Const raw byte pointer for an owned handle.
    [[nodiscard]] const std::byte* data(const BufferHandle& handle) const;

    // Raw byte pointer by index, for initialization paths that do not have a handle yet.
    [[nodiscard]] std::byte* slot_data_unchecked(std::size_t index) noexcept;

    // Const raw byte pointer by index, for initialization/diagnostic paths.
    [[nodiscard]] const std::byte* slot_data_unchecked(std::size_t index) const noexcept;

private:
    [[nodiscard]] static std::size_t checked_storage_bytes(std::size_t capacity, std::size_t stride);
    void validate_handle_index(const BufferHandle& handle) const;
    void validate_live_handle(const BufferHandle& handle) const;
    void update_peak_in_use(std::size_t value) noexcept;
    [[nodiscard]] std::uint32_t pop_free_index();
    void push_free_index(std::uint32_t index);

    const BufferPoolId pool_id_;
    const std::size_t capacity_;
    const std::size_t buffer_size_bytes_;
    const std::size_t stride_size_bytes_;
    std::vector<std::byte> storage_;
    mutable std::mutex free_mutex_;
    std::vector<std::uint32_t> free_indices_;
    mutable std::mutex wait_mutex_;
    std::condition_variable cv_available_;
    std::atomic<std::size_t> available_count_ {0};
    std::vector<std::atomic<std::uint32_t>> generations_;
    std::vector<std::atomic<std::uint8_t>> owned_;
    std::atomic<std::size_t> in_use_ {0};
    std::atomic<std::size_t> peak_in_use_ {0};
};

class BufferPoolRegistry {
public:
    // Register a pool so generic code can release handles by pool id.
    void register_pool(RawBufferPool& pool);

    // Look up a registered pool by id.
    [[nodiscard]] RawBufferPool& pool(BufferPoolId pool_id) const;

    // Release a handle to the pool named by handle.pool_id.
    void release(const BufferHandle& handle) const;

private:
    std::array<RawBufferPool*, static_cast<std::size_t>(std::numeric_limits<BufferPoolId>::max()) + 1U> pools_ {};
};

template <typename T>
[[nodiscard]] T& buffer_as(RawBufferPool& pool, const BufferHandle& handle) {
    // Interpret one owned raw buffer as a payload structure.
    static_assert(std::is_trivially_destructible_v<T>, "buffer payload views must not require destruction");
    if (sizeof(T) > pool.buffer_size_bytes()) {
        throw std::logic_error("typed buffer view exceeds raw buffer size");
    }
    std::byte* data = pool.data(handle);
    if (reinterpret_cast<std::uintptr_t>(data) % alignof(T) != 0U) {
        throw std::logic_error("raw buffer storage is not aligned for typed view");
    }
    return *reinterpret_cast<T*>(data);
}

template <typename T>
[[nodiscard]] const T& buffer_as(const RawBufferPool& pool, const BufferHandle& handle) {
    // Const version of buffer_as<T>().
    static_assert(std::is_trivially_destructible_v<T>, "buffer payload views must not require destruction");
    if (sizeof(T) > pool.buffer_size_bytes()) {
        throw std::logic_error("typed buffer view exceeds raw buffer size");
    }
    const std::byte* data = pool.data(handle);
    if (reinterpret_cast<std::uintptr_t>(data) % alignof(T) != 0U) {
        throw std::logic_error("raw buffer storage is not aligned for typed view");
    }
    return *reinterpret_cast<const T*>(data);
}

}  // namespace hypersync

#endif
