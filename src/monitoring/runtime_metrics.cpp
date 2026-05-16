#include "monitoring/runtime_metrics.hpp"

#include <algorithm>
#include <chrono>

namespace hypersync {
namespace {

[[nodiscard]] std::uint64_t steady_now_ns() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

std::size_t runtime_state_index(RuntimeState state) noexcept {
    return static_cast<std::size_t>(state);
}

std::string_view runtime_state_name(RuntimeState state) noexcept {
    switch (state) {
        case RuntimeState::stopped:
            return "stopped";
        case RuntimeState::processing:
            return "processing";
        case RuntimeState::wait_input_empty:
            return "wait_input";
        case RuntimeState::wait_output_full:
            return "wait_output";
        case RuntimeState::wait_pool_empty:
            return "wait_pool";
        case RuntimeState::wait_io:
            return "wait_io";
        case RuntimeState::parked:
            return "parked";
    }
    return "unknown";
}

RuntimeStateScope::RuntimeStateScope(ThreadedJobRuntimeMetrics* metrics,
                                     std::size_t worker_index,
                                     RuntimeState previous_state) noexcept
    : metrics_(metrics),
      worker_index_(worker_index),
      previous_state_(previous_state) {}

RuntimeStateScope::RuntimeStateScope(RuntimeStateScope&& other) noexcept
    : metrics_(other.metrics_),
      worker_index_(other.worker_index_),
      previous_state_(other.previous_state_) {
    other.metrics_ = nullptr;
}

RuntimeStateScope& RuntimeStateScope::operator=(RuntimeStateScope&& other) noexcept {
    if (this != &other) {
        restore();
        metrics_ = other.metrics_;
        worker_index_ = other.worker_index_;
        previous_state_ = other.previous_state_;
        other.metrics_ = nullptr;
    }
    return *this;
}

RuntimeStateScope::~RuntimeStateScope() {
    restore();
}

void RuntimeStateScope::restore() noexcept {
    if (metrics_ != nullptr) {
        (void)metrics_->enter_state(worker_index_, previous_state_);
        metrics_ = nullptr;
    }
}

ThreadedJobRuntimeMetrics::ThreadedJobRuntimeMetrics(std::size_t worker_count) {
    workers_.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        workers_.push_back(std::make_unique<WorkerMetrics>());
    }
    reset();
}

void ThreadedJobRuntimeMetrics::reset() noexcept {
    const std::uint64_t now = steady_now_ns();
    for (auto& entry : workers_) {
        if (entry == nullptr) {
            continue;
        }
        for (auto& state_value : entry->state_wall_ns) {
            state_value.store(0, std::memory_order_relaxed);
        }
        entry->state_started_ns.store(now, std::memory_order_release);
        entry->current_state.store(static_cast<std::uint8_t>(RuntimeState::stopped),
                                   std::memory_order_release);
    }
}

void ThreadedJobRuntimeMetrics::enter_worker(std::size_t worker_index) noexcept {
    (void)enter_state(worker_index, RuntimeState::processing);
}

void ThreadedJobRuntimeMetrics::leave_worker(std::size_t worker_index) noexcept {
    (void)enter_state(worker_index, RuntimeState::stopped);
}

RuntimeStateScope ThreadedJobRuntimeMetrics::scoped_state(std::size_t worker_index,
                                                         RuntimeState state) noexcept {
    const RuntimeState previous = enter_state(worker_index, state);
    return RuntimeStateScope(this, worker_index, previous);
}

RuntimeMetricsSnapshot ThreadedJobRuntimeMetrics::snapshot() const noexcept {
    RuntimeMetricsSnapshot result;
    result.worker_count = workers_.size();
    const std::uint64_t now = steady_now_ns();

    for (const auto& entry : workers_) {
        const WorkerMetrics* metrics = entry.get();
        if (metrics == nullptr) {
            continue;
        }
        const RuntimeState current =
            static_cast<RuntimeState>(metrics->current_state.load(std::memory_order_acquire));
        const std::size_t current_index =
            std::min(runtime_state_index(current), kRuntimeStateCount - 1U);
        result.current_workers[current_index] += 1U;

        for (std::size_t state_index = 0; state_index < kRuntimeStateCount; ++state_index) {
            result.state_wall_ns[state_index] +=
                metrics->state_wall_ns[state_index].load(std::memory_order_acquire);
        }

        const std::uint64_t started = metrics->state_started_ns.load(std::memory_order_acquire);
        if (started != 0U && now >= started) {
            result.state_wall_ns[current_index] += now - started;
        }
    }

    for (const std::uint64_t value : result.state_wall_ns) {
        result.total_wall_ns += value;
    }
    return result;
}

RuntimeState ThreadedJobRuntimeMetrics::enter_state(std::size_t worker_index,
                                                    RuntimeState next_state) noexcept {
    WorkerMetrics* metrics = worker(worker_index);
    if (metrics == nullptr) {
        return RuntimeState::stopped;
    }

    const std::uint64_t now = steady_now_ns();
    const auto previous_raw = metrics->current_state.exchange(static_cast<std::uint8_t>(next_state),
                                                              std::memory_order_acq_rel);
    const RuntimeState previous = static_cast<RuntimeState>(previous_raw);
    const std::uint64_t previous_started =
        metrics->state_started_ns.exchange(now, std::memory_order_acq_rel);
    if (previous_started != 0U && now >= previous_started) {
        const std::size_t previous_index =
            std::min(runtime_state_index(previous), kRuntimeStateCount - 1U);
        metrics->state_wall_ns[previous_index].fetch_add(now - previous_started,
                                                         std::memory_order_relaxed);
    }
    return previous;
}

ThreadedJobRuntimeMetrics::WorkerMetrics* ThreadedJobRuntimeMetrics::worker(
    std::size_t worker_index) noexcept {
    if (worker_index >= workers_.size()) {
        return nullptr;
    }
    return workers_[worker_index].get();
}

const ThreadedJobRuntimeMetrics::WorkerMetrics* ThreadedJobRuntimeMetrics::worker(
    std::size_t worker_index) const noexcept {
    if (worker_index >= workers_.size()) {
        return nullptr;
    }
    return workers_[worker_index].get();
}

}  // namespace hypersync
