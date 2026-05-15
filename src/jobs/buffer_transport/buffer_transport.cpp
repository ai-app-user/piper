#include "jobs/buffer_transport/buffer_transport.hpp"

#include <array>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

namespace hypersync {
namespace {

constexpr std::uint64_t kBufferFrameMagic = 0x5753594e43425546ULL;  // WSYNCBUF
constexpr std::uint32_t kBufferFrameVersion = 1;

void write_u16_be(std::array<std::byte, 32>& out, std::size_t offset, std::uint16_t value) {
    out[offset] = static_cast<std::byte>((value >> 8U) & 0xFFU);
    out[offset + 1U] = static_cast<std::byte>(value & 0xFFU);
}

void write_u32_be(std::array<std::byte, 32>& out, std::size_t offset, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

void write_u64_be(std::array<std::byte, 32>& out, std::size_t offset, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out[offset++] = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xFFU);
    }
}

std::uint16_t read_u16_be(const std::array<std::byte, 32>& in, std::size_t offset) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in.data() + offset);
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[0]) << 8U) |
                                      static_cast<std::uint16_t>(bytes[1]));
}

std::uint32_t read_u32_be(const std::array<std::byte, 32>& in, std::size_t offset) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in.data() + offset);
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64_be(const std::array<std::byte, 32>& in, std::size_t offset) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in.data() + offset);
    std::uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[index]);
    }
    return value;
}

std::array<std::byte, 32> make_header(BufferPoolId pool_id, std::size_t payload_bytes) {
    if (payload_bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("buffer transport payload too large");
    }
    std::array<std::byte, 32> header {};
    write_u64_be(header, 0, kBufferFrameMagic);
    write_u32_be(header, 8, kBufferFrameVersion);
    write_u16_be(header, 12, pool_id);
    write_u32_be(header, 16, static_cast<std::uint32_t>(payload_bytes));
    return header;
}

std::size_t transport_payload_bytes(RawBufferPool& pool,
                                    const BufferHandle& handle,
                                    const BufferPayloadSizeFn& payload_size_fn) {
    if (payload_size_fn) {
        const std::size_t payload_bytes = payload_size_fn(pool, handle);
        if (payload_bytes > pool.buffer_size_bytes()) {
            throw std::runtime_error("buffer transport payload size exceeds source buffer size");
        }
        return payload_bytes;
    }
    return pool.buffer_size_bytes();
}

struct ParsedHeader {
    BufferPoolId pool_id = 0;
    std::uint32_t payload_bytes = 0;
};

ParsedHeader parse_header(const std::array<std::byte, 32>& header) {
    if (read_u64_be(header, 0) != kBufferFrameMagic) {
        throw std::runtime_error("invalid buffer transport frame magic");
    }
    if (read_u32_be(header, 8) != kBufferFrameVersion) {
        throw std::runtime_error("unsupported buffer transport frame version");
    }
    ParsedHeader parsed;
    parsed.pool_id = read_u16_be(header, 12);
    parsed.payload_bytes = read_u32_be(header, 16);
    return parsed;
}

std::string_view remove_prefix(std::string_view value, std::string_view prefix) {
    if (value.substr(0, prefix.size()) != prefix) {
        throw std::invalid_argument("invalid endpoint prefix");
    }
    return value.substr(prefix.size());
}

}  // namespace

BufferTransportEndpoint BufferTransportEndpoint::tcp(std::string host, std::uint16_t port) {
    if (host.empty() || port == 0U) {
        throw std::invalid_argument("tcp buffer transport endpoint requires host and non-zero port");
    }
    BufferTransportEndpoint endpoint;
    endpoint.kind = BufferTransportKind::tcp;
    endpoint.host = std::move(host);
    endpoint.port = port;
    return endpoint;
}

BufferTransportEndpoint BufferTransportEndpoint::unix_socket(std::filesystem::path path) {
    if (path.empty()) {
        throw std::invalid_argument("unix buffer transport endpoint requires a path");
    }
    BufferTransportEndpoint endpoint;
    endpoint.kind = BufferTransportKind::unix_socket;
    endpoint.path = std::move(path);
    return endpoint;
}

BufferTransportEndpoint BufferTransportEndpoint::parse(const std::string& value) {
    if (value.rfind("unix:", 0) == 0) {
        return unix_socket(std::filesystem::path(remove_prefix(value, "unix:")));
    }
    if (value.rfind("tcp://", 0) == 0) {
        const std::string_view rest = remove_prefix(value, "tcp://");
        const std::size_t colon = rest.rfind(':');
        if (colon == std::string_view::npos || colon + 1U >= rest.size()) {
            throw std::invalid_argument("tcp endpoint must be tcp://host:port");
        }
        const std::string host(rest.substr(0, colon));
        const unsigned long parsed_port = std::stoul(std::string(rest.substr(colon + 1U)));
        if (parsed_port == 0UL || parsed_port > 65535UL) {
            throw std::invalid_argument("tcp endpoint port out of range");
        }
        return tcp(host, static_cast<std::uint16_t>(parsed_port));
    }
    throw std::invalid_argument("buffer transport endpoint must be tcp://host:port or unix:/path");
}

std::string BufferTransportEndpoint::to_string() const {
    if (kind == BufferTransportKind::unix_socket) {
        return "unix:" + path.string();
    }
    return "tcp://" + host + ":" + std::to_string(port);
}

BufferSenderJob::BufferSenderJob(std::size_t worker_count,
                                 BufQueue& input,
                                 const BufferPoolRegistry& registry,
                                 BufferTransportEndpoint endpoint,
                                 BufferPayloadSizeFn payload_size_fn)
    : ThreadedJob(worker_count),
      input_(input),
      registry_(registry),
      endpoint_(std::move(endpoint)),
      payload_size_fn_(std::move(payload_size_fn)) {}

BufferTransportStats BufferSenderJob::stats() const {
    return {buffers_sent_.load(std::memory_order_acquire),
            payload_bytes_sent_.load(std::memory_order_acquire)};
}

void BufferSenderJob::run_worker(std::size_t worker_index) {
    ScopedFd fd;
    {
        auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
        fd = connect();
    }
    BufferHandle handle;
    while (!stop_requested()) {
        if (!wait_for_input(worker_index, input_, handle)) {
            break;
        }
        RawBufferPool& pool = registry_.pool(handle.pool_id);
        const std::size_t payload_bytes = transport_payload_bytes(pool, handle, payload_size_fn_);
        const std::array<std::byte, 32> header = make_header(handle.pool_id, payload_bytes);
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            write_all(fd.get(), header.data(), header.size());
            write_all(fd.get(), pool.data(handle), payload_bytes);
        }
        pool.release(handle);
        buffers_sent_.fetch_add(1U, std::memory_order_relaxed);
        payload_bytes_sent_.fetch_add(payload_bytes, std::memory_order_relaxed);
    }
    ::shutdown(fd.get(), SHUT_WR);
}

void BufferSenderJob::on_stop_requested() {
    input_.close();
}

ScopedFd BufferSenderJob::connect() const {
    if (endpoint_.kind == BufferTransportKind::unix_socket) {
        return connect_unix(endpoint_.path.string(), 500, 10);
    }
    return connect_tcp(endpoint_.host, endpoint_.port, 500, 10);
}

BufferReceiverJob::BufferReceiverJob(std::size_t worker_count,
                                     RawBufferPool& output_pool,
                                     BufQueue& output,
                                     BufferTransportEndpoint endpoint)
    : ThreadedJob(worker_count),
      output_pool_(output_pool),
      output_(output),
      endpoint_(std::move(endpoint)) {}

BufferReceiverJob::~BufferReceiverJob() {
    if (endpoint_.kind == BufferTransportKind::unix_socket && !endpoint_.path.empty()) {
        std::error_code ignored;
        std::filesystem::remove(endpoint_.path, ignored);
    }
}

BufferTransportStats BufferReceiverJob::stats() const {
    return {buffers_received_.load(std::memory_order_acquire),
            payload_bytes_received_.load(std::memory_order_acquire)};
}

void BufferReceiverJob::on_starting() {
    listener_ = endpoint_.kind == BufferTransportKind::unix_socket
                    ? listen_unix(endpoint_.path.string(), static_cast<int>(worker_count()))
                    : listen_tcp(endpoint_.host, endpoint_.port, static_cast<int>(worker_count()));
}

void BufferReceiverJob::run_worker(std::size_t worker_index) {
    ScopedFd fd;
    {
        auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
        fd = accept_one();
    }
    for (;;) {
        std::array<std::byte, 32> header {};
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            if (!read_exact_or_eof(fd.get(), header.data(), header.size())) {
                break;
            }
        }
        const ParsedHeader parsed = parse_header(header);
        if (parsed.payload_bytes > output_pool_.buffer_size_bytes()) {
            throw std::runtime_error("buffer transport frame exceeds receiver buffer size");
        }

        BufferHandle handle = acquire_buffer(worker_index);
        if (parsed.payload_bytes < output_pool_.buffer_size_bytes()) {
            std::memset(static_cast<std::byte*>(output_pool_.data(handle)) + parsed.payload_bytes,
                        0,
                        output_pool_.buffer_size_bytes() - parsed.payload_bytes);
        }
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            if (!read_exact_or_eof(fd.get(), output_pool_.data(handle), parsed.payload_bytes)) {
                output_pool_.release(handle);
                throw std::runtime_error("unexpected EOF while reading buffer payload");
            }
        }
        if (!wait_for_output(worker_index, output_, handle)) {
            output_pool_.release(handle);
            break;
        }
        buffers_received_.fetch_add(1U, std::memory_order_relaxed);
        payload_bytes_received_.fetch_add(parsed.payload_bytes, std::memory_order_relaxed);
    }
}

void BufferReceiverJob::on_stop_requested() {
    listener_.reset();
    output_.close();
}

void BufferReceiverJob::on_all_workers_finished() {
    output_.close();
    listener_.reset();
}

ScopedFd BufferReceiverJob::accept_one() const {
    return accept_tcp(listener_.get());
}

BufferHandle BufferReceiverJob::acquire_buffer(std::size_t worker_index) {
    if (auto handle = wait_for_pool(worker_index, output_pool_); handle.has_value()) {
        return *handle;
    }
    throw std::runtime_error("buffer receiver stopped while waiting for output buffer");
}

BufferStreamSenderJob::BufferStreamSenderJob(std::size_t worker_count,
                                             BufQueue& input,
                                             const BufferPoolRegistry& registry,
                                             int fd,
                                             BufferPayloadSizeFn payload_size_fn)
    : ThreadedJob(worker_count),
      input_(input),
      registry_(registry),
      fd_(fd),
      payload_size_fn_(std::move(payload_size_fn)) {
    if (fd_ < 0) {
        throw std::invalid_argument("buffer stream sender requires a valid fd");
    }
}

BufferTransportStats BufferStreamSenderJob::stats() const {
    return {buffers_sent_.load(std::memory_order_acquire),
            payload_bytes_sent_.load(std::memory_order_acquire)};
}

void BufferStreamSenderJob::run_worker(std::size_t worker_index) {
    BufferHandle handle;
    while (!stop_requested()) {
        if (!wait_for_input(worker_index, input_, handle)) {
            break;
        }
        RawBufferPool& pool = registry_.pool(handle.pool_id);
        const std::size_t payload_bytes = transport_payload_bytes(pool, handle, payload_size_fn_);
        const std::array<std::byte, 32> header = make_header(handle.pool_id, payload_bytes);
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            write_all(fd_, header.data(), header.size());
            write_all(fd_, pool.data(handle), payload_bytes);
        }
        pool.release(handle);
        buffers_sent_.fetch_add(1U, std::memory_order_relaxed);
        payload_bytes_sent_.fetch_add(payload_bytes, std::memory_order_relaxed);
    }
    ::shutdown(fd_, SHUT_WR);
}

void BufferStreamSenderJob::on_stop_requested() {
    input_.close();
}

BufferStreamReceiverJob::BufferStreamReceiverJob(std::size_t worker_count,
                                                 RawBufferPool& output_pool,
                                                 BufQueue& output,
                                                 int fd)
    : ThreadedJob(worker_count),
      output_pool_(output_pool),
      output_(output),
      fd_(fd) {
    if (fd_ < 0) {
        throw std::invalid_argument("buffer stream receiver requires a valid fd");
    }
}

BufferTransportStats BufferStreamReceiverJob::stats() const {
    return {buffers_received_.load(std::memory_order_acquire),
            payload_bytes_received_.load(std::memory_order_acquire)};
}

void BufferStreamReceiverJob::run_worker(std::size_t worker_index) {
    for (;;) {
        std::array<std::byte, 32> header {};
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            if (!read_exact_or_eof(fd_, header.data(), header.size())) {
                break;
            }
        }
        const ParsedHeader parsed = parse_header(header);
        if (parsed.payload_bytes > output_pool_.buffer_size_bytes()) {
            throw std::runtime_error("buffer stream frame exceeds receiver buffer size");
        }

        BufferHandle handle = acquire_buffer(worker_index);
        if (parsed.payload_bytes < output_pool_.buffer_size_bytes()) {
            std::memset(static_cast<std::byte*>(output_pool_.data(handle)) + parsed.payload_bytes,
                        0,
                        output_pool_.buffer_size_bytes() - parsed.payload_bytes);
        }
        {
            auto io_scope = runtime_state_scope(worker_index, RuntimeState::wait_io);
            if (!read_exact_or_eof(fd_, output_pool_.data(handle), parsed.payload_bytes)) {
                output_pool_.release(handle);
                throw std::runtime_error("unexpected EOF while reading buffer stream payload");
            }
        }
        if (!wait_for_output(worker_index, output_, handle)) {
            output_pool_.release(handle);
            break;
        }
        buffers_received_.fetch_add(1U, std::memory_order_relaxed);
        payload_bytes_received_.fetch_add(parsed.payload_bytes, std::memory_order_relaxed);
    }
}

void BufferStreamReceiverJob::on_stop_requested() {
    output_.close();
}

void BufferStreamReceiverJob::on_all_workers_finished() {
    output_.close();
}

BufferHandle BufferStreamReceiverJob::acquire_buffer(std::size_t worker_index) {
    if (auto handle = wait_for_pool(worker_index, output_pool_); handle.has_value()) {
        return *handle;
    }
    throw std::runtime_error("buffer stream receiver stopped while waiting for output buffer");
}

}  // namespace hypersync
