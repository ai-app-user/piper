#include "jobs/buffer_transform_job.hpp"

namespace hypersync {

BufferTransformJob::BufferTransformJob(std::size_t worker_count,
                                       BufQueue& input,
                                       BufQueue& output,
                                       const BufferPoolRegistry& registry)
    : ThreadedJob(worker_count),
      input_(input),
      output_(output),
      registry_(registry) {}

BufferTransformJob::~BufferTransformJob() = default;

BufferTransformStats BufferTransformJob::transform_stats() const {
    BufferTransformStats result;
    result.buffers_transformed = buffers_transformed_.load(std::memory_order_acquire);
    result.bytes_transformed = bytes_transformed_.load(std::memory_order_acquire);
    return result;
}

void BufferTransformJob::run_worker(std::size_t worker_index) {
    BufferHandle handle;
    while (!stop_requested()) {
        if (!wait_for_input(worker_index, input_, handle)) {
            break;
        }
        if (!process_and_forward(worker_index, handle)) {
            break;
        }
    }

    while (input_.try_pop(handle)) {
        if (!process_and_forward(worker_index, handle)) {
            break;
        }
    }
}

void BufferTransformJob::on_stop_requested() {
    input_.close();
    output_.close();
}

void BufferTransformJob::on_all_workers_finished() {
    output_.close();
}

RawBufferPool& BufferTransformJob::pool_for(const BufferHandle& handle) const {
    return registry_.pool(handle.pool_id);
}

bool BufferTransformJob::process_and_forward(std::size_t worker_index, const BufferHandle& handle) {
    RawBufferPool& pool = pool_for(handle);
    const std::uint64_t byte_count = process_buffer(handle, pool);
    bytes_transformed_.fetch_add(byte_count, std::memory_order_relaxed);
    buffers_transformed_.fetch_add(1U, std::memory_order_relaxed);
    if (!wait_for_output(worker_index, output_, handle)) {
        pool.release(handle);
        return false;
    }
    return true;
}

}  // namespace hypersync
