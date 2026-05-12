#ifndef HYPERSYNC_JOBS_JOB_HPP
#define HYPERSYNC_JOBS_JOB_HPP

#include <any>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>

namespace hypersync {

namespace message_kinds {
inline constexpr char empty[] = "empty";
inline constexpr char folder_record[] = "folder_record";
inline constexpr char file_record[] = "file_record";
inline constexpr char data_chunk[] = "data_chunk";
inline constexpr char file_snapshot[] = "file_snapshot";
}  // namespace message_kinds

struct JobMessage {
    std::string kind;
    std::any payload;

    JobMessage() = default;

    template <typename T>
    JobMessage(std::string message_kind, T value)
        : kind(std::move(message_kind)), payload(std::move(value)) {}
};

[[nodiscard]] bool is_empty_message(const JobMessage& message);
[[nodiscard]] std::string message_kind(const JobMessage& message);

template <typename T>
[[nodiscard]] bool message_is(const JobMessage& message) {
    return message.payload.type() == typeid(T);
}

template <typename T>
[[nodiscard]] const T& message_as(const JobMessage& message) {
    const T* value = std::any_cast<T>(&message.payload);
    if (value == nullptr) {
        throw std::bad_any_cast();
    }
    return *value;
}

template <typename T>
[[nodiscard]] T& message_as(JobMessage& message) {
    T* value = std::any_cast<T>(&message.payload);
    if (value == nullptr) {
        throw std::bad_any_cast();
    }
    return *value;
}

struct JobStats {
    std::string name;
    std::string primary_message_kind = message_kinds::empty;
    bool running = false;
    std::size_t accepted = 0;
    std::size_t emitted = 0;
    std::size_t push_backs = 0;
    std::size_t deferred = 0;
    std::size_t queue_depth = 0;
    std::size_t high_watermark = 0;
};

class Job {
public:
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual bool pull(JobMessage& out) = 0;
    virtual void push_back(JobMessage message) = 0;
    [[nodiscard]] virtual JobStats stats() const = 0;
    virtual ~Job() = default;
};

}  // namespace hypersync

#endif
