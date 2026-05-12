#include "jobs/buffer_discarder/buffer_discarder.hpp"

#include <stdexcept>

#include "common/config.hpp"

namespace hypersync {

BufferDiscarderConfig::BufferDiscarderConfig()
    : BufferDiscarderConfig(load_buffer_discarder_config(ConfigStore{}).worker_count) {}

BufferDiscarderConfig::BufferDiscarderConfig(std::size_t worker_count)
    : worker_count(worker_count) {
    if (this->worker_count == 0U) {
        throw std::invalid_argument("buffer discarder worker count must be positive");
    }
}

BufferDiscarderConfig load_buffer_discarder_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("buffer_discarder"));
    return BufferDiscarderConfig(config_size_t_or(values, "worker_count", 1));
}

BufferDiscarderJob::BufferDiscarderJob(BufferDiscarderConfig config,
                                       BufQueue& input,
                                       const BufferPoolRegistry& registry)
    : BufferConsumerJob(config.worker_count, input, registry),
      config_(config) {
    if (config_.worker_count == 0U) {
        throw std::invalid_argument("buffer discarder worker count must be positive");
    }
}

BufferDiscarderJob::BufferDiscarderJob(BufferDiscarderConfig config,
                                       ShardedBufQueue& input,
                                       const BufferPoolRegistry& registry)
    : BufferConsumerJob(config.worker_count, input, registry),
      config_(config) {
    if (config_.worker_count == 0U) {
        throw std::invalid_argument("buffer discarder worker count must be positive");
    }
}

BufferDiscarderJob::~BufferDiscarderJob() {
    stop();
}

BufferDiscarderStats BufferDiscarderJob::stats() const {
    const BufferConsumerStats consumer = consumer_stats();
    BufferDiscarderStats result;
    result.running = running();
    result.worker_count = config_.worker_count;
    result.buffers_discarded = consumer.buffers_consumed;
    result.bytes_discarded = consumer.bytes_consumed;
    return result;
}

void BufferDiscarderJob::process_buffer(const BufferHandle& handle, RawBufferPool& pool) {
    pool.release(handle);
}

}  // namespace hypersync
