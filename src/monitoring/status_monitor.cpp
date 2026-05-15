#include "monitoring/status_monitor.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/socket.h>
#include <sys/resource.h>
#include <sys/un.h>
#include <poll.h>
#include <unistd.h>

namespace hypersync {
namespace {

std::string errno_message(std::string_view operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

void close_fd(int& fd) noexcept {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

sockaddr_un make_socket_address(const std::filesystem::path& socket_path) {
    const std::string path = socket_path.string();
    if (path.empty()) {
        throw std::invalid_argument("status socket path is required");
    }

    sockaddr_un address {};
    address.sun_family = AF_UNIX;
    if (path.size() >= sizeof(address.sun_path)) {
        throw std::invalid_argument("status socket path is too long: " + path);
    }
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1U);
    return address;
}

void write_all(int fd, std::string_view data) {
    const char* cursor = data.data();
    std::size_t remaining = data.size();
    while (remaining != 0U) {
        const ssize_t written = ::write(fd, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error(errno_message("status socket write failed"));
        }
        cursor += written;
        remaining -= static_cast<std::size_t>(written);
    }
}

void drain_status_request(int fd) {
    std::array<char, 256> buffer {};
    for (;;) {
        pollfd descriptor {};
        descriptor.fd = fd;
        descriptor.events = POLLIN;
        const int ready = ::poll(&descriptor, 1, 10);
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            return;
        }
        const ssize_t read_count = ::read(fd, buffer.data(), buffer.size());
        if (read_count <= 0) {
            return;
        }
        const auto end = buffer.begin() + read_count;
        if (std::find(buffer.begin(), end, '\n') != end) {
            return;
        }
    }
}

std::string human_count(double value, std::string_view unit) {
    static constexpr std::array<std::string_view, 5> suffixes {"", "K", "M", "B", "T"};
    std::size_t suffix_index = 0;
    while (value >= 1000.0 && suffix_index + 1U < suffixes.size()) {
        value /= 1000.0;
        ++suffix_index;
    }

    std::ostringstream out;
    out << std::fixed << std::setprecision(suffix_index == 0 ? 0 : 2)
        << value << suffixes[suffix_index] << ' ' << unit;
    return out.str();
}

std::string human_bytes(double value) {
    static constexpr std::array<std::string_view, 6> suffixes {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    std::size_t suffix_index = 0;
    while (value >= 1024.0 && suffix_index + 1U < suffixes.size()) {
        value /= 1024.0;
        ++suffix_index;
    }

    std::ostringstream out;
    out << std::fixed << std::setprecision(suffix_index == 0 ? 0 : 2)
        << value << ' ' << suffixes[suffix_index];
    return out.str();
}

std::string format_rate(double processed_per_second, std::string_view unit) {
    return human_count(processed_per_second, std::string(unit) + "/s");
}

std::string format_byte_rate(double bytes_per_second) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2)
        << bytes_per_second / 1'000'000'000.0 << " GB/s"
        << " (" << (bytes_per_second * 8.0 / 1'000'000'000.0) << " Gbit/s)";
    return out.str();
}

std::string format_percent(double value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(1) << value << '%';
    return out.str();
}

std::string format_fullness(const MonitorQueueSnapshot& queue) {
    if (queue.capacity == 0U) {
        return "n/a";
    }

    const double fullness = static_cast<double>(queue.depth) * 100.0 /
                            static_cast<double>(queue.capacity);
    std::ostringstream out;
    out << std::fixed << std::setprecision(1) << fullness << '%';
    return out.str();
}

double percent_of(std::uint64_t part, std::uint64_t total) {
    if (total == 0U) {
        return 0.0;
    }
    return static_cast<double>(part) * 100.0 / static_cast<double>(total);
}

std::string format_runtime_metrics(const RuntimeMetricsSnapshot& runtime) {
    if (runtime.worker_count == 0U || runtime.total_wall_ns == 0U) {
        return {};
    }

    const std::uint64_t processing =
        runtime.state_wall_ns[runtime_state_index(RuntimeState::processing)];
    const std::uint64_t wait_input =
        runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_input_empty)];
    const std::uint64_t wait_output =
        runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_output_full)];
    const std::uint64_t wait_pool =
        runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_pool_empty)];
    const std::uint64_t wait_io =
        runtime.state_wall_ns[runtime_state_index(RuntimeState::wait_io)];

    std::ostringstream out;
    out << "busy=" << format_percent(percent_of(processing, runtime.total_wall_ns))
        << " wait_in=" << format_percent(percent_of(wait_input, runtime.total_wall_ns))
        << " wait_out=" << format_percent(percent_of(wait_output, runtime.total_wall_ns))
        << " wait_pool=" << format_percent(percent_of(wait_pool, runtime.total_wall_ns))
        << " wait_io=" << format_percent(percent_of(wait_io, runtime.total_wall_ns));

    out << " now=";
    bool any = false;
    for (std::size_t index = 0; index < kRuntimeStateCount; ++index) {
        const std::size_t count = runtime.current_workers[index];
        if (count == 0U) {
            continue;
        }
        if (any) {
            out << ',';
        }
        any = true;
        out << runtime_state_name(static_cast<RuntimeState>(index)) << ':' << count;
    }
    if (!any) {
        out << "none";
    }
    return out.str();
}

}  // namespace

StatusRegistry::StatusRegistry()
    : started_at_(std::chrono::steady_clock::now()) {}

void StatusRegistry::register_job(std::string name, JobProvider provider) {
    if (!provider) {
        throw std::invalid_argument("status job provider is required");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    jobs_.push_back(JobEntry {std::move(name), std::move(provider)});
}

void StatusRegistry::register_queue(std::string name, QueueProvider provider) {
    if (!provider) {
        throw std::invalid_argument("status queue provider is required");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    queues_.push_back(QueueEntry {std::move(name), std::move(provider)});
}

std::string StatusRegistry::render_human() const {
    std::vector<JobEntry> jobs;
    std::vector<QueueEntry> queues;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jobs = jobs_;
        queues = queues_;
    }

    const double elapsed = elapsed_seconds();
    std::ostringstream out;
    const double process_cpu = process_cpu_seconds();
    const double process_cpu_percent = elapsed > 0.0 ? process_cpu * 100.0 / elapsed : 0.0;
    out << "Status elapsed=" << std::fixed << std::setprecision(2) << elapsed << "s"
        << " process_cpu=" << format_percent(process_cpu_percent) << "\n\n";

    out << "Jobs:\n";
    if (jobs.empty()) {
        out << "  none\n";
    }
    for (const JobEntry& entry : jobs) {
        MonitorJobSnapshot snapshot = entry.provider();
        if (snapshot.name.empty()) {
            snapshot.name = entry.name;
        }
        if (snapshot.count_unit.empty()) {
            snapshot.count_unit = "buffers";
        }
        const JobRateSample rates = update_job_rate_sample(snapshot);

        const double processed_rate = elapsed > 0.0
                                          ? static_cast<double>(snapshot.processed_count) / elapsed
                                          : 0.0;
        const double byte_rate = elapsed > 0.0
                                     ? static_cast<double>(snapshot.byte_count) / elapsed
                                     : 0.0;
        out << "  " << std::left << std::setw(24) << snapshot.name
            << " running=" << (snapshot.running ? "yes" : "no")
            << " workers=" << snapshot.worker_count
            << " processed=" << human_count(static_cast<double>(snapshot.processed_count),
                                             snapshot.count_unit)
            << " rate=" << format_rate(processed_rate, snapshot.count_unit)
            << " current=" << format_rate(rates.current_processed_rate, snapshot.count_unit)
            << " start=" << format_rate(rates.start_processed_rate, snapshot.count_unit)
            << " mid=" << format_rate(rates.mid_processed_rate, snapshot.count_unit)
            << " peak=" << format_rate(rates.peak_processed_rate, snapshot.count_unit)
            << " tail=";
        if (rates.tail_available) {
            out << format_rate(rates.tail_processed_rate, snapshot.count_unit);
        } else {
            out << "pending";
        }
        out
            << " bytes=" << human_bytes(static_cast<double>(snapshot.byte_count))
            << " throughput=" << format_byte_rate(byte_rate)
            << " current_bytes=" << format_byte_rate(rates.current_byte_rate);
        if (snapshot.has_runtime_metrics) {
            const std::string runtime_detail = format_runtime_metrics(snapshot.runtime_metrics);
            if (!runtime_detail.empty()) {
                out << ' ' << runtime_detail;
            }
        }
        if (!snapshot.detail.empty()) {
            out << " " << snapshot.detail;
        }
        out << '\n';
    }

    out << "\nQueues:\n";
    if (queues.empty()) {
        out << "  none\n";
    }
    for (const QueueEntry& entry : queues) {
        MonitorQueueSnapshot snapshot = entry.provider();
        if (snapshot.name.empty()) {
            snapshot.name = entry.name;
        }
        out << "  " << std::left << std::setw(24) << snapshot.name
            << " depth=" << snapshot.depth;
        if (snapshot.capacity != 0U) {
            out << "/" << snapshot.capacity;
        } else {
            out << "/unbounded";
        }
        out << " full=" << format_fullness(snapshot)
            << " high_watermark=" << snapshot.high_watermark
            << " pushed=" << snapshot.pushed
            << " popped=" << snapshot.popped
            << " closed=" << (snapshot.closed ? "yes" : "no");
        if (!snapshot.detail.empty()) {
            out << " " << snapshot.detail;
        }
        out << '\n';
    }

    return out.str();
}

double StatusRegistry::elapsed_seconds() const {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at_).count();
}

double StatusRegistry::process_cpu_seconds() const {
    rusage usage {};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0.0;
    }
    const double user_seconds = static_cast<double>(usage.ru_utime.tv_sec) +
                                static_cast<double>(usage.ru_utime.tv_usec) / 1'000'000.0;
    const double system_seconds = static_cast<double>(usage.ru_stime.tv_sec) +
                                  static_cast<double>(usage.ru_stime.tv_usec) / 1'000'000.0;
    return user_seconds + system_seconds;
}

StatusRegistry::JobRateSample StatusRegistry::update_job_rate_sample(
    const MonitorJobSnapshot& snapshot) const {
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(rate_mutex_);
    JobRateSample& sample = job_rate_samples_[snapshot.name];
    if (!sample.initialized) {
        sample.initialized = true;
        sample.sampled_at = now;
        sample.processed_count = snapshot.processed_count;
        sample.byte_count = snapshot.byte_count;
        sample.tail_available = !snapshot.running;
        return sample;
    }

    const double seconds = std::chrono::duration<double>(now - sample.sampled_at).count();
    if (snapshot.running) {
        sample.tail_available = false;
    }
    if (seconds > 0.0) {
        const std::uint64_t processed_delta =
            snapshot.processed_count >= sample.processed_count
                ? snapshot.processed_count - sample.processed_count
                : 0U;
        const std::uint64_t byte_delta =
            snapshot.byte_count >= sample.byte_count ? snapshot.byte_count - sample.byte_count : 0U;
        sample.current_processed_rate = static_cast<double>(processed_delta) / seconds;
        sample.current_byte_rate = static_cast<double>(byte_delta) / seconds;
        if (sample.interval_count == 0U) {
            sample.start_processed_rate = sample.current_processed_rate;
            sample.start_byte_rate = sample.current_byte_rate;
        } else {
            sample.mid_processed_rate_sum += sample.previous_processed_rate;
            sample.mid_byte_rate_sum += sample.previous_byte_rate;
            ++sample.mid_rate_count;
            sample.mid_processed_rate =
                sample.mid_processed_rate_sum / static_cast<double>(sample.mid_rate_count);
            sample.mid_byte_rate = sample.mid_byte_rate_sum / static_cast<double>(sample.mid_rate_count);
        }
        sample.peak_processed_rate = std::max(sample.peak_processed_rate, sample.current_processed_rate);
        sample.peak_byte_rate = std::max(sample.peak_byte_rate, sample.current_byte_rate);
        if (!snapshot.running) {
            sample.tail_available = true;
            sample.tail_processed_rate = sample.current_processed_rate;
            sample.tail_byte_rate = sample.current_byte_rate;
        }
        sample.previous_processed_rate = sample.current_processed_rate;
        sample.previous_byte_rate = sample.current_byte_rate;
        ++sample.interval_count;
    }
    sample.sampled_at = now;
    sample.processed_count = snapshot.processed_count;
    sample.byte_count = snapshot.byte_count;
    return sample;
}

StatusServer::StatusServer(std::filesystem::path socket_path, const StatusRegistry& registry)
    : socket_path_(std::move(socket_path)),
      registry_(registry) {}

StatusServer::~StatusServer() {
    stop();
}

void StatusServer::start() {
    if (running_.load(std::memory_order_acquire)) {
        throw std::logic_error("status server is already running");
    }

    const sockaddr_un address = make_socket_address(socket_path_);
    const std::filesystem::path parent = socket_path_.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
    std::error_code ignored;
    std::filesystem::remove(socket_path_, ignored);

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error(errno_message("status socket create failed"));
    }

    try {
        if (::bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
            throw std::runtime_error(errno_message("status socket bind failed"));
        }
        if (::listen(fd, 16) < 0) {
            throw std::runtime_error(errno_message("status socket listen failed"));
        }
    } catch (...) {
        close_fd(fd);
        std::filesystem::remove(socket_path_, ignored);
        throw;
    }

    server_fd_ = fd;
    running_.store(true, std::memory_order_release);
    thread_ = std::thread(&StatusServer::serve_loop, this);
}

void StatusServer::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel) && server_fd_ < 0) {
        return;
    }

    int fd = server_fd_;
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
        server_fd_ = -1;
    }
    if (thread_.joinable()) {
        thread_.join();
    }

    std::error_code ignored;
    std::filesystem::remove(socket_path_, ignored);
}

bool StatusServer::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

const std::filesystem::path& StatusServer::socket_path() const noexcept {
    return socket_path_;
}

void StatusServer::serve_loop() {
    while (running_.load(std::memory_order_acquire)) {
        const int client_fd = ::accept(server_fd_, nullptr, nullptr);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!running_.load(std::memory_order_acquire) || errno == EBADF || errno == EINVAL) {
                break;
            }
            continue;
        }

        try {
            serve_client(client_fd);
        } catch (const std::exception& ex) {
            std::cerr << "status monitor client error: " << ex.what() << '\n';
        }
        int owned_client_fd = client_fd;
        close_fd(owned_client_fd);
    }
}

void StatusServer::serve_client(int client_fd) const {
    drain_status_request(client_fd);
    const std::string response = registry_.render_human();
    write_all(client_fd, response);
}

PeriodicStatusReporter::PeriodicStatusReporter(const StatusRegistry& registry,
                                               std::chrono::milliseconds interval,
                                               Writer writer)
    : registry_(registry),
      interval_(interval),
      writer_(std::move(writer)) {
    if (interval_.count() <= 0) {
        throw std::invalid_argument("periodic status interval must be positive");
    }
    if (!writer_) {
        throw std::invalid_argument("periodic status writer is required");
    }
}

PeriodicStatusReporter::~PeriodicStatusReporter() {
    stop();
}

void PeriodicStatusReporter::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    thread_ = std::thread(&PeriodicStatusReporter::report_loop, this);
}

void PeriodicStatusReporter::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

bool PeriodicStatusReporter::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

void PeriodicStatusReporter::report_loop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (running_.load(std::memory_order_acquire)) {
        const bool stopped = cv_.wait_for(lock, interval_, [this] {
            return !running_.load(std::memory_order_acquire);
        });
        if (stopped || !running_.load(std::memory_order_acquire)) {
            break;
        }
        lock.unlock();
        try {
            writer_(registry_.render_human());
        } catch (...) {
            running_.store(false, std::memory_order_release);
        }
        lock.lock();
    }
}

std::string request_status(const std::filesystem::path& socket_path) {
    const sockaddr_un address = make_socket_address(socket_path);
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error(errno_message("status socket create failed"));
    }

    try {
        if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
            throw std::runtime_error(errno_message("status socket connect failed"));
        }
        write_all(fd, "status\n");

        std::string response;
        std::array<char, 4096> buffer {};
        for (;;) {
            const ssize_t read_count = ::read(fd, buffer.data(), buffer.size());
            if (read_count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::runtime_error(errno_message("status socket read failed"));
            }
            if (read_count == 0) {
                break;
            }
            response.append(buffer.data(), static_cast<std::size_t>(read_count));
        }

        close_fd(fd);
        return response;
    } catch (...) {
        close_fd(fd);
        throw;
    }
}

std::filesystem::path default_status_socket_path(std::string_view command_name) {
    std::string safe_name;
    safe_name.reserve(command_name.size());
    for (const char ch : command_name) {
        safe_name.push_back((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                                    (ch >= '0' && ch <= '9') || ch == '_' || ch == '-'
                                ? ch
                                : '_');
    }
    if (safe_name.empty()) {
        safe_name = "hypersync";
    }
    return std::filesystem::temp_directory_path() / (safe_name + ".status.sock");
}

}  // namespace hypersync
