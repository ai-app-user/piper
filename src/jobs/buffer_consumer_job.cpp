#include "jobs/buffer_consumer_job.hpp"

namespace hypersync {

BufferConsumerJob::BufferConsumerJob(std::size_t worker_count,
                                     BufQueue& input,
                                     const BufferPoolRegistry& registry)
    : ThreadedJob(worker_count),
      input_(&input),
      registry_(registry) {}

BufferConsumerJob::BufferConsumerJob(std::size_t worker_count,
                                     ShardedBufQueue& input,
                                     const BufferPoolRegistry& registry)
    : ThreadedJob(worker_count),
      sharded_input_(&input),
      registry_(registry) {}

BufferConsumerJob::~BufferConsumerJob() = default;

BufferConsumerStats BufferConsumerJob::consumer_stats() const {
    BufferConsumerStats result;
    result.buffers_consumed = buffers_consumed_.load(std::memory_order_acquire);
    result.bytes_consumed = bytes_consumed_.load(std::memory_order_acquire);
    return result;
}

void BufferConsumerJob::run_worker(std::size_t worker_index) {
    BufferHandle handle;
    while (!stop_requested()) {
        if (!pop_input_wait(worker_index, handle)) {
            break;
        }
        process_and_record(handle);
    }

    while (try_pop_input(worker_index, handle)) {
        process_and_record(handle);
    }
}

void BufferConsumerJob::on_stop_requested() {
    close_input();
}

RawBufferPool& BufferConsumerJob::pool_for(const BufferHandle& handle) const {
    return registry_.pool(handle.pool_id);
}

void BufferConsumerJob::process_and_record(const BufferHandle& handle) {
    RawBufferPool& pool = pool_for(handle);
    const std::uint64_t byte_count = pool.buffer_size_bytes();
    process_buffer(handle, pool);
    bytes_consumed_.fetch_add(byte_count, std::memory_order_relaxed);
    buffers_consumed_.fetch_add(1U, std::memory_order_relaxed);
}

bool BufferConsumerJob::pop_input_wait(std::size_t worker_index, BufferHandle& handle) {
    if (sharded_input_ != nullptr) {
        return sharded_input_->pop_wait(worker_index, handle);
    }
    return input_ != nullptr && input_->pop_wait(handle);
}

bool BufferConsumerJob::try_pop_input(std::size_t worker_index, BufferHandle& handle) {
    if (sharded_input_ != nullptr) {
        return sharded_input_->try_pop(worker_index, handle);
    }
    return input_ != nullptr && input_->try_pop(handle);
}

void BufferConsumerJob::close_input() {
    if (sharded_input_ != nullptr) {
        sharded_input_->close();
        return;
    }
    if (input_ != nullptr) {
        input_->close();
    }
}

}  // namespace hypersync
