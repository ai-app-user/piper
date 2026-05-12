#ifndef HYPERSYNC_MONITORING_STATUS_MONITOR_HPP
#define HYPERSYNC_MONITORING_STATUS_MONITOR_HPP

// Lightweight runtime status monitoring for long-running commands.
//
// Jobs and queues register small snapshot callbacks with a StatusRegistry. A
// StatusServer exposes those snapshots over a local Unix-domain socket so a
// separate CLI invocation can ask a running process for human-readable status.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <atomic>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace hypersync {

struct MonitorJobSnapshot {
    std::string name;
    bool running = false;
    std::size_t worker_count = 0;
    std::uint64_t processed_count = 0;
    std::uint64_t byte_count = 0;
    std::string count_unit = "buffers";
    std::string detail;
};

struct MonitorQueueSnapshot {
    std::string name;
    std::size_t capacity = 0;
    std::size_t depth = 0;
    std::size_t high_watermark = 0;
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
    bool closed = false;
    std::string detail;
};

class StatusRegistry {
public:
    using JobProvider = std::function<MonitorJobSnapshot()>;
    using QueueProvider = std::function<MonitorQueueSnapshot()>;

    StatusRegistry();

    // Add a job snapshot source. The provider must remain callable until the
    // matching StatusServer is stopped.
    void register_job(std::string name, JobProvider provider);

    // Add a queue snapshot source. Capacity 0 means the queue is unbounded or
    // not a BufQueue, so fullness is reported as n/a.
    void register_queue(std::string name, QueueProvider provider);

    // Render one current snapshot in a form meant for humans at a terminal.
    [[nodiscard]] std::string render_human() const;

private:
    struct JobEntry {
        std::string name;
        JobProvider provider;
    };

    struct QueueEntry {
        std::string name;
        QueueProvider provider;
    };

    [[nodiscard]] double elapsed_seconds() const;

    std::chrono::steady_clock::time_point started_at_;
    mutable std::mutex mutex_;
    std::vector<JobEntry> jobs_;
    std::vector<QueueEntry> queues_;
};

class StatusServer {
public:
    StatusServer(std::filesystem::path socket_path, const StatusRegistry& registry);
    ~StatusServer();

    StatusServer(const StatusServer&) = delete;
    StatusServer& operator=(const StatusServer&) = delete;

    // Bind and start the background listener thread.
    void start();

    // Stop the listener, close the socket, and remove the socket path.
    void stop();

    [[nodiscard]] bool running() const noexcept;
    [[nodiscard]] const std::filesystem::path& socket_path() const noexcept;

private:
    void serve_loop();
    void serve_client(int client_fd) const;

    std::filesystem::path socket_path_;
    const StatusRegistry& registry_;
    std::atomic<bool> running_ {false};
    int server_fd_ = -1;
    std::thread thread_;
};

// Request one status snapshot from a running StatusServer.
[[nodiscard]] std::string request_status(const std::filesystem::path& socket_path);

// Deterministic default path for small local tests. Production runs should pass
// an explicit path to avoid two commands sharing a status socket by accident.
[[nodiscard]] std::filesystem::path default_status_socket_path(std::string_view command_name);

}  // namespace hypersync

#endif
