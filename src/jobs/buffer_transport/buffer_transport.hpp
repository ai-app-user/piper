#ifndef HYPERSYNC_JOBS_BUFFER_TRANSPORT_HPP
#define HYPERSYNC_JOBS_BUFFER_TRANSPORT_HPP

// Generic buffer transport jobs.
//
// These jobs bridge BufQueue ownership across a stream transport. The sender
// consumes owned raw buffers, serializes one fixed-size payload frame, and
// releases the source handle. The receiver accepts frames, acquires destination
// buffers from its local pool, copies frame bytes into those buffers, and pushes
// handles into its output queue. Payload interpretation remains downstream.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

#include "common/buffer_pool.hpp"
#include "common/socket_utils.hpp"
#include "jobs/threaded_job.hpp"

namespace hypersync {

enum class BufferTransportKind {
    tcp,
    unix_socket,
};

struct BufferTransportEndpoint {
    BufferTransportKind kind = BufferTransportKind::tcp;
    std::string host = "127.0.0.1";
    std::uint16_t port = 0;
    std::filesystem::path path;

    [[nodiscard]] static BufferTransportEndpoint tcp(std::string host, std::uint16_t port);
    [[nodiscard]] static BufferTransportEndpoint unix_socket(std::filesystem::path path);
    [[nodiscard]] static BufferTransportEndpoint parse(const std::string& value);
    [[nodiscard]] std::string to_string() const;
};

struct BufferTransportStats {
    std::uint64_t buffers = 0;
    std::uint64_t payload_bytes = 0;
};

using BufferPayloadSizeFn = std::function<std::size_t(RawBufferPool&, const BufferHandle&)>;

class BufferSenderJob : public ThreadedJob {
public:
    BufferSenderJob(std::size_t worker_count,
                    BufQueue& input,
                    const BufferPoolRegistry& registry,
                    BufferTransportEndpoint endpoint,
                    BufferPayloadSizeFn payload_size_fn = {});

    [[nodiscard]] BufferTransportStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

private:
    [[nodiscard]] ScopedFd connect() const;

    BufQueue& input_;
    const BufferPoolRegistry& registry_;
    BufferTransportEndpoint endpoint_;
    BufferPayloadSizeFn payload_size_fn_;
    std::atomic<std::uint64_t> buffers_sent_ {0};
    std::atomic<std::uint64_t> payload_bytes_sent_ {0};
};

// Generic sender with priority input. It always drains priority_input first and
// sends bulk_input only when the priority queue is empty or below the configured
// low watermark. Payloads remain opaque buffers.
class BufferPrioritySenderJob : public ThreadedJob {
public:
    BufferPrioritySenderJob(std::size_t worker_count,
                            BufQueue& priority_input,
                            BufQueue& bulk_input,
                            const BufferPoolRegistry& registry,
                            BufferTransportEndpoint endpoint,
                            std::size_t priority_low_watermark = 0,
                            BufferPayloadSizeFn payload_size_fn = {});

    [[nodiscard]] BufferTransportStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

private:
    [[nodiscard]] ScopedFd connect() const;
    [[nodiscard]] bool take_next_buffer(std::size_t worker_index, BufferHandle& handle);

    BufQueue& priority_input_;
    BufQueue& bulk_input_;
    const BufferPoolRegistry& registry_;
    BufferTransportEndpoint endpoint_;
    std::size_t priority_low_watermark_ = 0;
    BufferPayloadSizeFn payload_size_fn_;
    std::atomic<std::uint64_t> buffers_sent_ {0};
    std::atomic<std::uint64_t> payload_bytes_sent_ {0};
};

class BufferReceiverJob : public ThreadedJob {
public:
    BufferReceiverJob(std::size_t worker_count,
                      RawBufferPool& output_pool,
                      BufQueue& output,
                      BufferTransportEndpoint endpoint);
    ~BufferReceiverJob() override;

    [[nodiscard]] BufferTransportStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_starting() override;
    void on_stop_requested() override;
    void on_all_workers_finished() override;

private:
    [[nodiscard]] ScopedFd accept_one() const;
    [[nodiscard]] BufferHandle acquire_buffer(std::size_t worker_index);

    RawBufferPool& output_pool_;
    BufQueue& output_;
    BufferTransportEndpoint endpoint_;
    ScopedFd listener_;
    std::atomic<std::uint64_t> buffers_received_ {0};
    std::atomic<std::uint64_t> payload_bytes_received_ {0};
};

// Generic writer for an already-open stream fd. The job does not own or
// interpret the payload; it only transfers raw buffer bytes then releases the
// source handle. This is used for full-duplex protocols where connection setup
// is handled by the pipeline owner.
class BufferStreamSenderJob : public ThreadedJob {
public:
    BufferStreamSenderJob(std::size_t worker_count,
                          BufQueue& input,
                          const BufferPoolRegistry& registry,
                          int fd,
                          BufferPayloadSizeFn payload_size_fn = {});

    [[nodiscard]] BufferTransportStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;

private:
    BufQueue& input_;
    const BufferPoolRegistry& registry_;
    int fd_ = -1;
    BufferPayloadSizeFn payload_size_fn_;
    std::atomic<std::uint64_t> buffers_sent_ {0};
    std::atomic<std::uint64_t> payload_bytes_sent_ {0};
};

// Generic reader for an already-open stream fd. Received frames are copied into
// owned buffers from output_pool_ and pushed to output_ as opaque handles.
class BufferStreamReceiverJob : public ThreadedJob {
public:
    BufferStreamReceiverJob(std::size_t worker_count,
                            RawBufferPool& output_pool,
                            BufQueue& output,
                            int fd);

    [[nodiscard]] BufferTransportStats stats() const;

protected:
    void run_worker(std::size_t worker_index) override;
    void on_stop_requested() override;
    void on_all_workers_finished() override;

private:
    [[nodiscard]] BufferHandle acquire_buffer(std::size_t worker_index);

    RawBufferPool& output_pool_;
    BufQueue& output_;
    int fd_ = -1;
    std::atomic<std::uint64_t> buffers_received_ {0};
    std::atomic<std::uint64_t> payload_bytes_received_ {0};
};

}  // namespace hypersync

#endif
