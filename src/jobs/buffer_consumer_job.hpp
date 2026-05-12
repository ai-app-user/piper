#ifndef HYPERSYNC_JOBS_BUFFER_CONSUMER_JOB_HPP
#define HYPERSYNC_JOBS_BUFFER_CONSUMER_JOB_HPP

#include <atomic>
#include <cstdint>

#include "common/buffer_pool.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

struct BufferConsumerStats {
    std::uint64_t buffers_consumed = 0;
    std::uint64_t bytes_consumed = 0;
};

// Common hot path for jobs that consume buffers from one input queue.
//
// This base owns input queue popping, graceful drain after stop/close, byte and
// buffer stats, and queue wakeup. Subclasses decide what processing means and
// whether ownership is released, forwarded, or retained by another structure.
class BufferConsumerJob : public ThreadedJob {
public:
    BufferConsumerJob(std::size_t worker_count, BufQueue& input, const BufferPoolRegistry& registry);
    BufferConsumerJob(std::size_t worker_count, ShardedBufQueue& input, const BufferPoolRegistry& registry);
    ~BufferConsumerJob() override;

    [[nodiscard]] BufferConsumerStats consumer_stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

    [[nodiscard]] RawBufferPool& pool_for(const BufferHandle& handle) const;

    // Subclasses receive ownership of a buffer handle. The subclass must move
    // that ownership somewhere explicit: release it, forward it, or retain it.
    virtual void process_buffer(const BufferHandle& handle, RawBufferPool& pool) = 0;

private:
    void process_and_record(const BufferHandle& handle);
    [[nodiscard]] bool pop_input_wait(std::size_t worker_index, BufferHandle& handle);
    [[nodiscard]] bool try_pop_input(std::size_t worker_index, BufferHandle& handle);
    void close_input();

    BufQueue* input_ = nullptr;
    ShardedBufQueue* sharded_input_ = nullptr;
    const BufferPoolRegistry& registry_;
    std::atomic<std::uint64_t> buffers_consumed_ {0};
    std::atomic<std::uint64_t> bytes_consumed_ {0};
};

}  // namespace hypersync

#endif
