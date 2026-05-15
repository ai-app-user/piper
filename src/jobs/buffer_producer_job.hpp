#ifndef HYPERSYNC_JOBS_BUFFER_PRODUCER_JOB_HPP
#define HYPERSYNC_JOBS_BUFFER_PRODUCER_JOB_HPP

#include <atomic>
#include <cstdint>

#include "common/buffer_pool.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

struct BufferProducerStats {
    std::uint64_t buffers_produced = 0;
    std::uint64_t bytes_produced = 0;
};

// Common hot path for jobs that create buffers from one pool and publish them.
//
// Subclasses only implement fill_buffer(). This base owns sequence assignment,
// optional count limiting, pool acquisition, output queue push, stats, and
// graceful release if the output queue closes before ownership can transfer.
class BufferProducerJob : public ThreadedJob {
public:
    BufferProducerJob(std::size_t worker_count,
                      std::uint64_t buffer_count,
                      RawBufferPool& pool,
                      BufQueue& output);
    BufferProducerJob(std::size_t worker_count,
                      std::uint64_t buffer_count,
                      RawBufferPool& pool,
                      ShardedBufQueue& output);
    ~BufferProducerJob() override;

    [[nodiscard]] BufferProducerStats producer_stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;
    void on_all_workers_finished() override;

    // Subclasses receive an owned buffer and the monotonically increasing
    // sequence number assigned by this producer.
    virtual void fill_buffer(const BufferHandle& handle, std::uint64_t sequence) = 0;

private:
    [[nodiscard]] bool try_take_next_sequence(std::uint64_t& sequence);
    [[nodiscard]] bool acquire_buffer(std::size_t worker_index, BufferHandle& handle);
    [[nodiscard]] bool push_output(const BufferHandle& handle, std::size_t worker_index);
    void close_output();

    const std::uint64_t buffer_count_;
    RawBufferPool& pool_;
    BufQueue* output_ = nullptr;
    ShardedBufQueue* sharded_output_ = nullptr;
    std::atomic<std::uint64_t> next_sequence_ {0};
    std::atomic<std::uint64_t> buffers_produced_ {0};
    std::atomic<std::uint64_t> bytes_produced_ {0};
};

}  // namespace hypersync

#endif
