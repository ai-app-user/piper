#ifndef HYPERSYNC_JOBS_BUFFER_TRANSFORM_JOB_HPP
#define HYPERSYNC_JOBS_BUFFER_TRANSFORM_JOB_HPP

#include <atomic>
#include <cstdint>

#include "common/buffer_pool.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

struct BufferTransformStats {
    std::uint64_t buffers_transformed = 0;
    std::uint64_t bytes_transformed = 0;
};

// Common hot path for jobs that consume buffers, modify or inspect them, then
// forward the same buffer handle to another queue. Ownership remains explicit:
// this base owns a handle after pop, the subclass processes it, and this base
// transfers the same handle downstream or releases it if forwarding fails.
class BufferTransformJob : public ThreadedJob {
public:
    BufferTransformJob(std::size_t worker_count,
                       BufQueue& input,
                       BufQueue& output,
                       const BufferPoolRegistry& registry);
    ~BufferTransformJob() override;

    [[nodiscard]] BufferTransformStats transform_stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;
    void on_all_workers_finished() override;

    [[nodiscard]] RawBufferPool& pool_for(const BufferHandle& handle) const;

    // Subclasses process one owned buffer. Return the number of payload bytes
    // processed for stats. Ownership stays with the transform base.
    virtual std::uint64_t process_buffer(const BufferHandle& handle, RawBufferPool& pool) = 0;

private:
    [[nodiscard]] bool process_and_forward(const BufferHandle& handle);

    BufQueue& input_;
    BufQueue& output_;
    const BufferPoolRegistry& registry_;
    std::atomic<std::uint64_t> buffers_transformed_ {0};
    std::atomic<std::uint64_t> bytes_transformed_ {0};
};

}  // namespace hypersync

#endif
