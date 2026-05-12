#include "jobs/buffer_generator/buffer_generator.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <cmath>
#include <stdexcept>

#include "common/config.hpp"

namespace hypersync {
namespace {

[[nodiscard]] std::uint64_t splitmix64(std::uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

struct Xoshiro256State {
    std::array<std::uint64_t, 4> value {};
};

[[nodiscard]] Xoshiro256State make_xoshiro256_state(std::uint64_t seed) noexcept {
    Xoshiro256State state;
    for (std::uint64_t& value : state.value) {
        value = splitmix64(seed);
        seed = value;
    }
    return state;
}

[[nodiscard]] std::uint64_t xoshiro256plusplus(Xoshiro256State& state) noexcept {
    const std::uint64_t result = std::rotl(state.value[0] + state.value[3], 23U) + state.value[0];
    const std::uint64_t temporary = state.value[1] << 17U;

    state.value[2] ^= state.value[0];
    state.value[3] ^= state.value[1];
    state.value[1] ^= state.value[2];
    state.value[0] ^= state.value[3];
    state.value[2] ^= temporary;
    state.value[3] = std::rotl(state.value[3], 45U);
    return result;
}

void copy_tail(std::byte* data, std::size_t size, std::size_t offset, std::uint64_t word) {
    if (offset < size) {
        std::memcpy(data + offset, &word, size - offset);
    }
}

void fill_xoshiro256(std::byte* data, std::size_t size, std::uint64_t seed) {
    Xoshiro256State state = make_xoshiro256_state(seed);
    auto* words = reinterpret_cast<std::uint64_t*>(data);
    const std::size_t word_count = size / sizeof(std::uint64_t);
    std::size_t index = 0;
    while (index + 4U <= word_count) {
        words[index++] = xoshiro256plusplus(state);
        words[index++] = xoshiro256plusplus(state);
        words[index++] = xoshiro256plusplus(state);
        words[index++] = xoshiro256plusplus(state);
    }
    while (index < word_count) {
        words[index++] = xoshiro256plusplus(state);
    }
    copy_tail(data, size, word_count * sizeof(std::uint64_t), xoshiro256plusplus(state));
}

void fill_fast_text(std::byte* data, std::size_t size, std::uint64_t seed) {
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    static constexpr std::size_t chars_per_word = 10U;

    Xoshiro256State state = make_xoshiro256_state(seed);
    std::size_t offset = 0;
    while (offset < size) {
        std::uint64_t word = xoshiro256plusplus(state);
        for (std::size_t index = 0; index < chars_per_word && offset < size; ++index) {
            data[offset] = static_cast<std::byte>(alphabet[word & 0x3fU]);
            word >>= 6U;
            ++offset;
        }
    }
}

[[nodiscard]] std::size_t random_prefix_size(std::size_t buffer_size, double compression_ratio) {
    if (compression_ratio <= 1.0) {
        return buffer_size;
    }

    // Compression ratio is expressed as original/compressed. A 1.4 target means
    // 7 bytes should compress to roughly 5 bytes, so about 1 / 1.4 of the buffer
    // should remain high-entropy while the rest is easy for compressors to fold.
    const double target_size = std::ceil(static_cast<double>(buffer_size) / compression_ratio);
    return std::clamp(static_cast<std::size_t>(target_size), std::size_t{1}, buffer_size);
}

}  // namespace

BufferGeneratorConfig::BufferGeneratorConfig()
    : BufferGeneratorConfig(load_buffer_generator_config(ConfigStore{})) {}

BufferGeneratorConfig::BufferGeneratorConfig(std::size_t worker_count,
                                             std::uint64_t buffer_count,
                                             BufferGeneratorPattern pattern,
                                             std::uint64_t seed,
                                             double compression_ratio)
    : worker_count(worker_count),
      buffer_count(buffer_count),
      pattern(pattern),
      seed(seed),
      compression_ratio(compression_ratio) {
    if (this->worker_count == 0U) {
        throw std::invalid_argument("buffer generator worker count must be positive");
    }
    if (!std::isfinite(this->compression_ratio) || this->compression_ratio < 1.0) {
        throw std::invalid_argument("buffer generator compression ratio must be finite and at least 1.0");
    }
}

BufferGeneratorPattern parse_buffer_generator_pattern(const std::string& value) {
    if (value == "zero" || value == "zeros" || value == "all_zero") {
        return BufferGeneratorPattern::zero;
    }
    if (value == "random_text" || value == "text" || value == "fast_text" || value == "base64_text") {
        return BufferGeneratorPattern::fast_text;
    }
    if (value == "pseudo_random" || value == "random" || value == "non_dedupable" ||
        value == "xoshiro256" || value == "xoshiro256++" ||
        value == "xorshift64" || value == "fast_random" || value == "fast_pseudo_random" || value == "sfc64") {
        return BufferGeneratorPattern::xoshiro256;
    }
    throw std::invalid_argument("unknown buffer generator pattern: " + value);
}

std::string to_string(BufferGeneratorPattern pattern) {
    switch (pattern) {
        case BufferGeneratorPattern::zero:
            return "zero";
        case BufferGeneratorPattern::xoshiro256:
            return "xoshiro256";
        case BufferGeneratorPattern::fast_text:
            return "fast_text";
    }
    return "unknown";
}

BufferGeneratorConfig load_buffer_generator_config(const ConfigStore& config) {
    const ConfigSection values = config.merged_sections(default_job_config_sections("buffer_generator"));
    return BufferGeneratorConfig(config_size_t_or(values, "worker_count", 1),
                                 config_u64_or(values, "buffer_count", 0),
                                 parse_buffer_generator_pattern(config_string_or(values, "pattern", "xoshiro256")),
                                 config_u64_or(values, "seed", 0x9e3779b97f4a7c15ULL),
                                 config_double_or(values, "compression_ratio", 1.0));
}

BufferGeneratorJob::BufferGeneratorJob(BufferGeneratorConfig config, RawBufferPool& pool, BufQueue& output)
    : BufferProducerJob(config.worker_count, config.buffer_count, pool, output),
      config_(config),
      pool_(pool) {
    if (config_.worker_count == 0U) {
        throw std::invalid_argument("buffer generator worker count must be positive");
    }
}

BufferGeneratorJob::BufferGeneratorJob(BufferGeneratorConfig config, RawBufferPool& pool, ShardedBufQueue& output)
    : BufferProducerJob(config.worker_count, config.buffer_count, pool, output),
      config_(config),
      pool_(pool) {
    if (config_.worker_count == 0U) {
        throw std::invalid_argument("buffer generator worker count must be positive");
    }
}

BufferGeneratorJob::~BufferGeneratorJob() {
    stop();
}

BufferGeneratorStats BufferGeneratorJob::stats() const {
    const BufferProducerStats producer = producer_stats();
    BufferGeneratorStats result;
    result.running = running();
    result.worker_count = config_.worker_count;
    result.buffers_generated = producer.buffers_produced;
    result.bytes_generated = producer.bytes_produced;
    return result;
}

void BufferGeneratorJob::fill_buffer(const BufferHandle& handle, std::uint64_t sequence) {
    std::byte* data = pool_.data(handle);
    const std::size_t size = pool_.buffer_size_bytes();
    const std::uint64_t seed = config_.seed ^ sequence ^ (static_cast<std::uint64_t>(handle.index) << 32U);
    const std::size_t random_size = random_prefix_size(size, config_.compression_ratio);
    if (config_.pattern == BufferGeneratorPattern::zero) {
        std::memset(data, 0, size);
        return;
    }
    if (config_.pattern == BufferGeneratorPattern::xoshiro256) {
        fill_xoshiro256(data, random_size, seed);
        std::memset(data + random_size, 0, size - random_size);
        return;
    }

    if (config_.pattern == BufferGeneratorPattern::fast_text) {
        fill_fast_text(data, random_size, seed);
        std::memset(data + random_size, 'A', size - random_size);
        return;
    }
}

}  // namespace hypersync
