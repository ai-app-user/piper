#ifndef HYPERSYNC_JOBS_QUEUE_JOB_HPP
#define HYPERSYNC_JOBS_QUEUE_JOB_HPP

#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "jobs/job.hpp"

namespace hypersync {

class QueueJob : public Job {
public:
    QueueJob(std::string name, std::string primary_message_kind)
        : stats_{std::move(name), std::move(primary_message_kind), false, 0, 0, 0, 0, 0, 0} {}

    void start() override {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.running = true;
    }

    void stop() override {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.running = false;
    }

    bool pull(JobMessage& out) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            stats_.queue_depth = 0;
            return false;
        }
        out = std::move(queue_.front());
        queue_.pop_front();
        ++stats_.emitted;
        stats_.queue_depth = queue_.size();
        return true;
    }

    void push_back(JobMessage message) override {
        if (!accepts_kind(message.kind)) {
            throw std::logic_error("job " + stats_.name + " does not accept message kind " + message.kind);
        }
        std::lock_guard<std::mutex> lock(mutex_);
        do_enqueue(std::move(message));
        ++stats_.push_backs;
    }

    [[nodiscard]] JobStats stats() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        JobStats snapshot = stats_;
        snapshot.queue_depth = queue_.size();
        return snapshot;
    }

protected:
    virtual bool accepts_kind(std::string_view kind) const = 0;

    void enqueue_message(JobMessage message) {
        if (!accepts_kind(message.kind)) {
            throw std::logic_error("job " + stats_.name + " does not accept message kind " + message.kind);
        }
        std::lock_guard<std::mutex> lock(mutex_);
        do_enqueue(std::move(message));
    }

    template <typename T>
    void publish(std::string kind, T value) {
        enqueue_message(JobMessage{std::move(kind), std::move(value)});
    }

    void record_deferred(std::size_t count = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.deferred += count;
    }

    [[nodiscard]] std::size_t queue_depth() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    void do_enqueue(JobMessage message) {
        queue_.push_back(std::move(message));
        ++stats_.accepted;
        stats_.queue_depth = queue_.size();
        if (stats_.queue_depth > stats_.high_watermark) {
            stats_.high_watermark = stats_.queue_depth;
        }
    }

    mutable std::mutex mutex_;
    std::deque<JobMessage> queue_;
    JobStats stats_;
};

template <typename T>
class TypedQueueJob : public QueueJob {
public:
    TypedQueueJob(std::string name, std::string message_kind)
        : QueueJob(std::move(name), std::move(message_kind)),
          message_kind_(this->stats().primary_message_kind) {}

protected:
    bool accepts_kind(std::string_view kind) const override {
        return kind == message_kind_;
    }

    void publish_item(T value) {
        this->publish(message_kind_, std::move(value));
    }

    [[nodiscard]] const std::string& accepted_message_kind() const {
        return message_kind_;
    }

private:
    std::string message_kind_;
};

}  // namespace hypersync

#endif
