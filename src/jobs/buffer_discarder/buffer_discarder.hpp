#ifndef HYPERSYNC_JOBS_BUFFER_DISCARDER_HPP
#define HYPERSYNC_JOBS_BUFFER_DISCARDER_HPP

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/buffer_pool.hpp"
#include "jobs/buffer_consumer_job.hpp"

namespace hypersync {

class ConfigStore;

struct BufferDiscarderConfig {
    std::size_t worker_count = 1;

    BufferDiscarderConfig();
    explicit BufferDiscarderConfig(std::size_t worker_count);
};

struct BufferDiscarderStats {
    bool running = false;
    std::size_t worker_count = 0;
    std::uint64_t buffers_discarded = 0;
    std::uint64_t bytes_discarded = 0;
};

[[nodiscard]] BufferDiscarderConfig load_buffer_discarder_config(const ConfigStore& config);

class BufferDiscarderJob : public BufferConsumerJob {
public:
    BufferDiscarderJob(BufferDiscarderConfig config, BufQueue& input, const BufferPoolRegistry& registry);
    BufferDiscarderJob(BufferDiscarderConfig config, ShardedBufQueue& input, const BufferPoolRegistry& registry);
    ~BufferDiscarderJob();

    BufferDiscarderJob(const BufferDiscarderJob&) = delete;
    BufferDiscarderJob& operator=(const BufferDiscarderJob&) = delete;

    [[nodiscard]] BufferDiscarderStats stats() const;

private:
    void process_buffer(const BufferHandle& handle, RawBufferPool& pool) override;

    BufferDiscarderConfig config_;
};

}  // namespace hypersync

#endif
