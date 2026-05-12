#ifndef HYPERSYNC_COMMON_PREALLOCATED_RING_HPP
#define HYPERSYNC_COMMON_PREALLOCATED_RING_HPP

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hypersync {

template <typename T>
class PreallocatedRing {
public:
    PreallocatedRing() = default;

    explicit PreallocatedRing(std::size_t capacity) : slots_(capacity) {}

    void reset(std::size_t capacity) {
        slots_.assign(capacity, std::nullopt);
        head_ = 0;
        tail_ = 0;
        size_ = 0;
    }

    [[nodiscard]] std::size_t size() const {
        return size_;
    }

    [[nodiscard]] std::size_t capacity() const {
        return slots_.size();
    }

    [[nodiscard]] bool empty() const {
        return size_ == 0;
    }

    [[nodiscard]] bool full() const {
        return size_ == slots_.size();
    }

    template <typename U>
    void push(U&& value) {
        if (full()) {
            throw std::logic_error("preallocated ring is full");
        }
        slots_[tail_] = std::forward<U>(value);
        tail_ = advance(tail_);
        ++size_;
    }

    [[nodiscard]] T pop() {
        if (empty()) {
            throw std::logic_error("preallocated ring is empty");
        }
        T value = std::move(*slots_[head_]);
        slots_[head_].reset();
        head_ = advance(head_);
        --size_;
        return value;
    }

private:
    [[nodiscard]] std::size_t advance(std::size_t index) const {
        return slots_.empty() ? 0 : (index + 1U) % slots_.size();
    }

    std::vector<std::optional<T>> slots_;
    std::size_t head_ = 0;
    std::size_t tail_ = 0;
    std::size_t size_ = 0;
};

}  // namespace hypersync

#endif
