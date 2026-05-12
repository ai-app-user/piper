#ifndef HYPERSYNC_JOBS_THREADED_JOB_HPP
#define HYPERSYNC_JOBS_THREADED_JOB_HPP

#include <atomic>
#include <cstddef>
#include <exception>
#include <mutex>
#include <thread>
#include <vector>

namespace hypersync {

// Reusable lifecycle for jobs that run N equivalent worker threads.
//
// This abstraction owns start/stop/wait, worker thread creation, the shared stop
// flag, and the "last worker finished" hook. Concrete jobs keep ownership of
// their queues, pools, and payload-specific work.
class ThreadedJob {
public:
    explicit ThreadedJob(std::size_t worker_count);
    virtual ~ThreadedJob();

    ThreadedJob(const ThreadedJob&) = delete;
    ThreadedJob& operator=(const ThreadedJob&) = delete;

    // Start worker_count threads unless the job is already running.
    void start();

    // Join all currently owned worker threads.
    void wait();

    // Request a graceful stop, wake job-specific waiters, and join workers.
    void stop();

    // True while at least one worker is expected to be active.
    [[nodiscard]] bool running() const noexcept;

    // Configured number of worker threads.
    [[nodiscard]] std::size_t worker_count() const noexcept;

protected:
    // True after stop() has been requested.
    [[nodiscard]] bool stop_requested() const noexcept;

    // Called from each worker thread. Concrete jobs implement the hot path here.
    virtual void run_worker(std::size_t worker_index) = 0;

    // Optional hook executed while start() holds the lifecycle mutex.
    virtual void on_starting();

    // Optional hook executed by stop() after publishing the stop flag.
    virtual void on_stop_requested();

    // Optional hook executed by the final worker thread before running() becomes false.
    virtual void on_all_workers_finished();

private:
    void worker_entry(std::size_t worker_index);
    void capture_exception(std::exception_ptr error);

    const std::size_t worker_count_;
    mutable std::mutex mutex_;
    mutable std::mutex exception_mutex_;
    std::vector<std::thread> workers_;
    std::exception_ptr first_exception_;
    std::atomic<bool> running_ {false};
    std::atomic<bool> stop_requested_ {false};
    std::atomic<std::size_t> active_workers_ {0};
};

}  // namespace hypersync

#endif
