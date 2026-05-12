#ifndef HYPERSYNC_COMMON_FIXED_STRING_HPP
#define HYPERSYNC_COMMON_FIXED_STRING_HPP

#include <array>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hypersync {

template <std::size_t Capacity>
class FixedString {
public:
    FixedString() = default;

    FixedString(std::string_view value) {
        assign(value);
    }

    FixedString(const char* value) {
        assign(value != nullptr ? std::string_view(value) : std::string_view{});
    }

    FixedString& operator=(std::string_view value) {
        assign(value);
        return *this;
    }

    FixedString& operator=(const char* value) {
        assign(value != nullptr ? std::string_view(value) : std::string_view{});
        return *this;
    }

    void assign(std::string_view value) {
        if (value.size() > Capacity) {
            throw std::length_error("fixed string capacity exceeded");
        }
        std::memcpy(storage_.data(), value.data(), value.size());
        size_ = value.size();
    }

    void clear() {
        size_ = 0;
    }

    void resize_for_overwrite(std::size_t size) {
        if (size > Capacity) {
            throw std::length_error("fixed string capacity exceeded");
        }
        size_ = size;
    }

    [[nodiscard]] constexpr std::size_t capacity() const {
        return Capacity;
    }

    [[nodiscard]] std::size_t size() const {
        return size_;
    }

    [[nodiscard]] bool empty() const {
        return size_ == 0;
    }

    [[nodiscard]] char* data() {
        return storage_.data();
    }

    [[nodiscard]] const char* data() const {
        return storage_.data();
    }

    [[nodiscard]] std::string_view view() const {
        return std::string_view(storage_.data(), size());
    }

    [[nodiscard]] std::string str() const {
        return std::string(view());
    }

    [[nodiscard]] operator std::string_view() const {
        return view();
    }

    friend bool operator==(const FixedString& lhs, const FixedString& rhs) {
        return lhs.view() == rhs.view();
    }

    friend bool operator==(const FixedString& lhs, std::string_view rhs) {
        return lhs.view() == rhs;
    }

    friend bool operator==(std::string_view lhs, const FixedString& rhs) {
        return lhs == rhs.view();
    }

    friend bool operator!=(const FixedString& lhs, const FixedString& rhs) {
        return !(lhs == rhs);
    }

    friend bool operator!=(const FixedString& lhs, std::string_view rhs) {
        return !(lhs == rhs);
    }

    friend bool operator!=(std::string_view lhs, const FixedString& rhs) {
        return !(lhs == rhs);
    }

private:
    std::array<char, Capacity> storage_ {};
    std::size_t size_ = 0;
};

}  // namespace hypersync

#endif
