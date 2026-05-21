#ifndef HYPERSYNC_JOBS_THREADED_JOB_HPP
#define HYPERSYNC_JOBS_THREADED_JOB_HPP

#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "common/buffer_pool.hpp"
#include "monitoring/runtime_metrics.hpp"

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

    // Current active worker target. Workers with index >= this limit park
    // cooperatively between work items instead of being killed mid-operation.
    [[nodiscard]] std::size_t active_worker_limit() const noexcept;

    // Pin workers to base_cpu + worker_index % cpu_count when cpu_count > 0.
    // By default workers use the shared non-reactor CPU corridor.
    void set_worker_cpu_affinity(std::size_t base_cpu, std::size_t cpu_count) noexcept;

    // Adjust active workers in [1, worker_count()]. Returns the clamped value.
    std::size_t set_active_worker_limit(std::size_t active_workers) noexcept;

    // Generic runtime wait-state metrics collected by the shared job helpers.
    [[nodiscard]] const ThreadedJobRuntimeMetrics& runtime_metrics() const noexcept;

protected:
    // True after stop() has been requested.
    [[nodiscard]] bool stop_requested() const noexcept;

    // True when this worker is allowed to take another unit of work.
    [[nodiscard]] bool worker_active(std::size_t worker_index) const noexcept;

    // Park a worker while autoscaling has placed it above the active limit.
    // Concrete run loops call this between buffer/file batches.
    [[nodiscard]] bool wait_until_worker_active(std::size_t worker_index,
                                                std::chrono::microseconds sleep_interval =
                                                    std::chrono::microseconds(100)) noexcept;

    // Called from each worker thread. Concrete jobs implement the hot path here.
    virtual void run_worker(std::size_t worker_index) = 0;

    // Optional hook executed while start() holds the lifecycle mutex.
    virtual void on_starting();

    // Optional hook executed by stop() after publishing the stop flag.
    virtual void on_stop_requested();

    // Optional hook executed by the final worker thread before running() becomes false.
    virtual void on_all_workers_finished();

    // Temporarily mark a worker as waiting or doing owned I/O. Use this only
    // around code paths that can actually block; do not wrap every hot-path
    // buffer operation when the non-blocking fast path succeeds.
    [[nodiscard]] RuntimeStateScope runtime_state_scope(std::size_t worker_index,
                                                        RuntimeState state) noexcept;

    // Generic queue/pool helpers. They try the non-blocking fast path first and
    // enter the relevant wait state only when backpressure is real.
    [[nodiscard]] bool wait_for_input(std::size_t worker_index, BufQueue& queue, BufferHandle& handle);
    [[nodiscard]] bool wait_for_input(std::size_t worker_index, ShardedBufQueue& queue, BufferHandle& handle);
    [[nodiscard]] bool wait_for_output(std::size_t worker_index, BufQueue& queue, const BufferHandle& handle);
    [[nodiscard]] bool wait_for_output(std::size_t worker_index,
                                       ShardedBufQueue& queue,
                                       const BufferHandle& handle);
    [[nodiscard]] std::optional<BufferHandle> wait_for_pool(std::size_t worker_index, RawBufferPool& pool);

private:
    void worker_entry(std::size_t worker_index);
    void capture_exception(std::exception_ptr error);

    const std::size_t worker_count_;
    ThreadedJobRuntimeMetrics runtime_metrics_;
    mutable std::mutex mutex_;
    mutable std::mutex exception_mutex_;
    std::vector<std::thread> workers_;
    std::exception_ptr first_exception_;
    std::atomic<bool> running_ {false};
    std::atomic<bool> stop_requested_ {false};
    std::atomic<bool> wait_requested_ {false};
    std::atomic<std::size_t> active_worker_limit_;
    std::atomic<std::size_t> active_workers_ {0};
    std::atomic<std::size_t> affinity_base_cpu_ {0};
    std::atomic<std::size_t> affinity_cpu_count_ {0};
};

}  // namespace hypersync

#endif
