#ifndef HYPERSYNC_JOBS_BUFFER_GENERATOR_HPP
#define HYPERSYNC_JOBS_BUFFER_GENERATOR_HPP

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/buffer_pool.hpp"
#include "jobs/buffer_producer_job.hpp"

namespace hypersync {

class ConfigStore;

enum class BufferGeneratorPattern {
    zero,
    xoshiro256,
    fast_text,
};

struct BufferGeneratorConfig {
    std::size_t worker_count = 1;
    std::uint64_t buffer_count = 0;
    BufferGeneratorPattern pattern = BufferGeneratorPattern::xoshiro256;
    std::uint64_t seed = 0x9e3779b97f4a7c15ULL;
    double compression_ratio = 1.0;

    BufferGeneratorConfig();
    BufferGeneratorConfig(std::size_t worker_count,
                          std::uint64_t buffer_count,
                          BufferGeneratorPattern pattern,
                          std::uint64_t seed = 0x9e3779b97f4a7c15ULL,
                          double compression_ratio = 1.0);
};

struct BufferGeneratorStats {
    bool running = false;
    std::size_t worker_count = 0;
    std::uint64_t buffers_generated = 0;
    std::uint64_t bytes_generated = 0;
};

[[nodiscard]] BufferGeneratorPattern parse_buffer_generator_pattern(const std::string& value);
[[nodiscard]] std::string to_string(BufferGeneratorPattern pattern);
[[nodiscard]] BufferGeneratorConfig load_buffer_generator_config(const ConfigStore& config);

class BufferGeneratorJob : public BufferProducerJob {
public:
    BufferGeneratorJob(BufferGeneratorConfig config, RawBufferPool& pool, BufQueue& output);
    BufferGeneratorJob(BufferGeneratorConfig config, RawBufferPool& pool, ShardedBufQueue& output);
    ~BufferGeneratorJob();

    BufferGeneratorJob(const BufferGeneratorJob&) = delete;
    BufferGeneratorJob& operator=(const BufferGeneratorJob&) = delete;

    [[nodiscard]] BufferGeneratorStats stats() const;

private:
    void fill_buffer(const BufferHandle& handle, std::uint64_t sequence) override;

    BufferGeneratorConfig config_;
    RawBufferPool& pool_;
};

}  // namespace hypersync

#endif
