#include "jobs/threaded_job.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>

namespace hypersync {

ThreadedJob::ThreadedJob(std::size_t worker_count)
    : worker_count_(worker_count),
      runtime_metrics_(worker_count) {
    if (worker_count_ == 0U) {
        throw std::invalid_argument("threaded job worker count must be positive");
    }
}

ThreadedJob::~ThreadedJob() = default;

void ThreadedJob::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_.load(std::memory_order_acquire)) {
        return;
    }

    on_starting();
    runtime_metrics_.reset();
    {
        std::lock_guard<std::mutex> exception_lock(exception_mutex_);
        first_exception_ = nullptr;
    }
    stop_requested_.store(false, std::memory_order_release);
    active_workers_.store(worker_count_, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
        workers_.emplace_back([this, index] {
            worker_entry(index);
        });
    }
}

void ThreadedJob::wait() {
    std::vector<std::thread> workers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        workers.swap(workers_);
    }
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    std::exception_ptr error;
    {
        std::lock_guard<std::mutex> lock(exception_mutex_);
        error = first_exception_;
    }
    if (error) {
        std::rethrow_exception(error);
    }
}

void ThreadedJob::stop() {
    stop_requested_.store(true, std::memory_order_release);
    on_stop_requested();
    wait();
}

bool ThreadedJob::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

std::size_t ThreadedJob::worker_count() const noexcept {
    return worker_count_;
}

const ThreadedJobRuntimeMetrics& ThreadedJob::runtime_metrics() const noexcept {
    return runtime_metrics_;
}

bool ThreadedJob::stop_requested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
}

void ThreadedJob::on_starting() {}

void ThreadedJob::on_stop_requested() {}

void ThreadedJob::on_all_workers_finished() {}

RuntimeStateScope ThreadedJob::runtime_state_scope(std::size_t worker_index,
                                                   RuntimeState state) noexcept {
    return runtime_metrics_.scoped_state(worker_index, state);
}

bool ThreadedJob::wait_for_input(std::size_t worker_index, BufQueue& queue, BufferHandle& handle) {
    if (queue.try_pop(handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_input_empty);
    return queue.pop_wait(handle);
}

bool ThreadedJob::wait_for_input(std::size_t worker_index, ShardedBufQueue& queue, BufferHandle& handle) {
    if (queue.try_pop(worker_index, handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_input_empty);
    return queue.pop_wait(worker_index, handle);
}

bool ThreadedJob::wait_for_output(std::size_t worker_index, BufQueue& queue, const BufferHandle& handle) {
    if (queue.try_push(handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_output_full);
    return queue.push_wait(handle);
}

bool ThreadedJob::wait_for_output(std::size_t worker_index,
                                  ShardedBufQueue& queue,
                                  const BufferHandle& handle) {
    if (queue.try_push(worker_index, handle)) {
        return true;
    }
    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_output_full);
    return queue.push_wait(worker_index, handle);
}

std::optional<BufferHandle> ThreadedJob::wait_for_pool(std::size_t worker_index, RawBufferPool& pool) {
    if (auto handle = pool.try_acquire(); handle.has_value()) {
        return handle;
    }

    auto wait_scope = runtime_state_scope(worker_index, RuntimeState::wait_pool_empty);
    while (!stop_requested()) {
        if (auto handle = pool.try_acquire(); handle.has_value()) {
            return handle;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return std::nullopt;
}

void ThreadedJob::worker_entry(std::size_t worker_index) {
    runtime_metrics_.enter_worker(worker_index);
    try {
        run_worker(worker_index);
    } catch (...) {
        capture_exception(std::current_exception());
        stop_requested_.store(true, std::memory_order_release);
        try {
            on_stop_requested();
        } catch (...) {
            capture_exception(std::current_exception());
        }
    }
    runtime_metrics_.leave_worker(worker_index);
    if (active_workers_.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
        try {
            on_all_workers_finished();
        } catch (...) {
            capture_exception(std::current_exception());
        }
        running_.store(false, std::memory_order_release);
    }
}

void ThreadedJob::capture_exception(std::exception_ptr error) {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (!first_exception_) {
        first_exception_ = std::move(error);
    }
}

}  // namespace hypersync
