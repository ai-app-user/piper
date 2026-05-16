#ifndef PIPER_MONITORING_RUNTIME_METRICS_HPP
#define PIPER_MONITORING_RUNTIME_METRICS_HPP

// Low-overhead wait-state accounting for threaded pipeline jobs.
//
// The metrics layer tracks where each worker spends wall-clock time: processing,
// waiting for input, waiting for output capacity, waiting for a free buffer, or
// waiting inside owned I/O. Jobs should enter wait states only after a
// non-blocking fast path fails, so the common uncontended path does not take a
// timestamp per buffer.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace hypersync {

enum class RuntimeState : std::uint8_t {
    stopped = 0,
    processing = 1,
    wait_input_empty = 2,
    wait_output_full = 3,
    wait_pool_empty = 4,
    wait_io = 5,
    parked = 6,
};

inline constexpr std::size_t kRuntimeStateCount = 7;

using RuntimeStateCounts = std::array<std::uint64_t, kRuntimeStateCount>;
using RuntimeCurrentCounts = std::array<std::size_t, kRuntimeStateCount>;

struct RuntimeMetricsSnapshot {
    std::size_t worker_count = 0;
    RuntimeStateCounts state_wall_ns {};
    RuntimeCurrentCounts current_workers {};
    std::uint64_t total_wall_ns = 0;
};

[[nodiscard]] std::size_t runtime_state_index(RuntimeState state) noexcept;
[[nodiscard]] std::string_view runtime_state_name(RuntimeState state) noexcept;

class ThreadedJobRuntimeMetrics;

class RuntimeStateScope {
public:
    RuntimeStateScope() noexcept = default;
    RuntimeStateScope(ThreadedJobRuntimeMetrics* metrics,
                      std::size_t worker_index,
                      RuntimeState previous_state) noexcept;
    RuntimeStateScope(RuntimeStateScope&& other) noexcept;
    RuntimeStateScope& operator=(RuntimeStateScope&& other) noexcept;
    ~RuntimeStateScope();

    RuntimeStateScope(const RuntimeStateScope&) = delete;
    RuntimeStateScope& operator=(const RuntimeStateScope&) = delete;

private:
    void restore() noexcept;

    ThreadedJobRuntimeMetrics* metrics_ = nullptr;
    std::size_t worker_index_ = 0;
    RuntimeState previous_state_ = RuntimeState::stopped;
};

class ThreadedJobRuntimeMetrics {
public:
    explicit ThreadedJobRuntimeMetrics(std::size_t worker_count);

    ThreadedJobRuntimeMetrics(const ThreadedJobRuntimeMetrics&) = delete;
    ThreadedJobRuntimeMetrics& operator=(const ThreadedJobRuntimeMetrics&) = delete;

    // Clear counters and put all workers in the stopped state.
    void reset() noexcept;

    // Mark a worker as active before its run loop starts.
    void enter_worker(std::size_t worker_index) noexcept;

    // Mark a worker as stopped after its run loop exits.
    void leave_worker(std::size_t worker_index) noexcept;

    // Temporarily switch one worker into a wait state, restoring the previous
    // state when the returned scope is destroyed.
    [[nodiscard]] RuntimeStateScope scoped_state(std::size_t worker_index, RuntimeState state) noexcept;

    // Return a point-in-time snapshot including time accumulated in states that
    // are currently active.
    [[nodiscard]] RuntimeMetricsSnapshot snapshot() const noexcept;

private:
    friend class RuntimeStateScope;

    struct WorkerMetrics {
        std::array<std::atomic<std::uint64_t>, kRuntimeStateCount> state_wall_ns {};
        std::atomic<std::uint64_t> state_started_ns {0};
        std::atomic<std::uint8_t> current_state {static_cast<std::uint8_t>(RuntimeState::stopped)};
    };

    [[nodiscard]] RuntimeState enter_state(std::size_t worker_index, RuntimeState next_state) noexcept;
    [[nodiscard]] WorkerMetrics* worker(std::size_t worker_index) noexcept;
    [[nodiscard]] const WorkerMetrics* worker(std::size_t worker_index) const noexcept;

    std::vector<std::unique_ptr<WorkerMetrics>> workers_;
};

}  // namespace hypersync

#endif
