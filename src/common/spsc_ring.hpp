#ifndef HYPERSYNC_COMMON_SPSC_RING_HPP
#define HYPERSYNC_COMMON_SPSC_RING_HPP

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hypersync {

template <typename T>
class SpscRing {
public:
    explicit SpscRing(std::size_t capacity) : mask_(capacity - 1U), slots_(capacity) {
        if (capacity < 2U || (capacity & (capacity - 1U)) != 0U) {
            throw std::invalid_argument("SPSC ring capacity must be a power of two and at least 2");
        }
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return slots_.size();
    }

    [[nodiscard]] bool empty() const noexcept {
        return size() == 0U;
    }

    [[nodiscard]] bool full() const noexcept {
        return size() == slots_.size();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        return head - tail;
    }

    template <typename U>
    bool try_push(U&& value) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head - tail == slots_.size()) {
            return false;
        }
        slots_[head & mask_] = std::forward<U>(value);
        head_.store(head + 1U, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;
        }
        out = std::move(slots_[tail & mask_]);
        tail_.store(tail + 1U, std::memory_order_release);
        return true;
    }

private:
    const std::size_t mask_;
    std::vector<T> slots_;
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

}  // namespace hypersync

#endif
