#include "jobs/threaded_job.hpp"

#include <stdexcept>

namespace hypersync {

ThreadedJob::ThreadedJob(std::size_t worker_count)
    : worker_count_(worker_count) {
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

bool ThreadedJob::stop_requested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
}

void ThreadedJob::on_starting() {}

void ThreadedJob::on_stop_requested() {}

void ThreadedJob::on_all_workers_finished() {}

void ThreadedJob::worker_entry(std::size_t worker_index) {
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
