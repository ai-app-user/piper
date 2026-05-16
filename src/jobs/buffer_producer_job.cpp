#include "jobs/buffer_producer_job.hpp"

namespace hypersync {

BufferProducerJob::BufferProducerJob(std::size_t worker_count,
                                     std::uint64_t buffer_count,
                                     RawBufferPool& pool,
                                     BufQueue& output)
    : ThreadedJob(worker_count),
      buffer_count_(buffer_count),
      pool_(pool),
      output_(&output) {}

BufferProducerJob::BufferProducerJob(std::size_t worker_count,
                                     std::uint64_t buffer_count,
                                     RawBufferPool& pool,
                                     ShardedBufQueue& output)
    : ThreadedJob(worker_count),
      buffer_count_(buffer_count),
      pool_(pool),
      sharded_output_(&output) {}

BufferProducerJob::~BufferProducerJob() = default;

BufferProducerStats BufferProducerJob::producer_stats() const {
    BufferProducerStats result;
    result.buffers_produced = buffers_produced_.load(std::memory_order_acquire);
    result.bytes_produced = bytes_produced_.load(std::memory_order_acquire);
    return result;
}

void BufferProducerJob::run_worker(std::size_t worker_index) {
    (void)worker_index;
    for (;;) {
        if (stop_requested()) {
            break;
        }
        if (!wait_until_worker_active(worker_index)) {
            break;
        }

        std::uint64_t sequence = 0;
        if (!try_take_next_sequence(sequence)) {
            break;
        }

        BufferHandle handle;
        if (!acquire_buffer(worker_index, handle)) {
            break;
        }
        fill_buffer(handle, sequence);

        if (!push_output(handle, worker_index)) {
            pool_.release(handle);
            break;
        }
        buffers_produced_.fetch_add(1U, std::memory_order_relaxed);
        bytes_produced_.fetch_add(pool_.buffer_size_bytes(), std::memory_order_relaxed);
    }
}

void BufferProducerJob::on_stop_requested() {
    close_output();
}

void BufferProducerJob::on_all_workers_finished() {
    close_output();
}

bool BufferProducerJob::try_take_next_sequence(std::uint64_t& sequence) {
    sequence = next_sequence_.fetch_add(1U, std::memory_order_relaxed);
    return buffer_count_ == 0U || sequence < buffer_count_;
}

bool BufferProducerJob::acquire_buffer(std::size_t worker_index, BufferHandle& handle) {
    if (auto acquired = wait_for_pool(worker_index, pool_); acquired.has_value()) {
        handle = *acquired;
        return true;
    }
    return false;
}

bool BufferProducerJob::push_output(const BufferHandle& handle, std::size_t worker_index) {
    if (sharded_output_ != nullptr) {
        return wait_for_output(worker_index, *sharded_output_, handle);
    }
    return output_ != nullptr && wait_for_output(worker_index, *output_, handle);
}

void BufferProducerJob::close_output() {
    if (sharded_output_ != nullptr) {
        sharded_output_->close();
        return;
    }
    if (output_ != nullptr) {
        output_->close();
    }
}

}  // namespace hypersync
