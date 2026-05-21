#include "common/socket_utils.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__linux__)
#include <netinet/tcp.h>
#endif

namespace hypersync {

ScopedFd::~ScopedFd() {
    reset();
}

ScopedFd::ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

ScopedFd& ScopedFd::operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
        reset();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

int ScopedFd::get() const {
    return fd_;
}

bool ScopedFd::valid() const {
    return fd_ >= 0;
}

int ScopedFd::release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
}

void ScopedFd::reset(int fd) {
    if (fd_ >= 0) {
        ::close(fd_);
    }
    fd_ = fd;
}

namespace {

struct AddrInfoGuard {
    struct addrinfo* info = nullptr;
    explicit AddrInfoGuard(struct addrinfo* p) : info(p) {}
    ~AddrInfoGuard() { if (info != nullptr) { ::freeaddrinfo(info); } }
    AddrInfoGuard(const AddrInfoGuard&) = delete;
    AddrInfoGuard& operator=(const AddrInfoGuard&) = delete;
};

std::string port_string(std::uint16_t port) {
    return std::to_string(static_cast<unsigned>(port));
}

void best_effort_set_int_socket_option(int fd, int level, int option, int value) noexcept {
    (void)::setsockopt(fd, level, option, &value, sizeof(value));
}

#if defined(__linux__) && defined(TCP_CONGESTION)
void best_effort_enable_bbr(int fd) noexcept {
    static constexpr char kBbr[] = "bbr";
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_CONGESTION, kBbr, sizeof(kBbr));
}
#else
void best_effort_enable_bbr(int) noexcept {}
#endif

void tune_stream_socket(int fd) noexcept {
    static constexpr int kRequestedSocketBufferBytes = 1 << 30;
    best_effort_set_int_socket_option(fd, SOL_SOCKET, SO_SNDBUF, kRequestedSocketBufferBytes);
    best_effort_set_int_socket_option(fd, SOL_SOCKET, SO_RCVBUF, kRequestedSocketBufferBytes);
#if defined(TCP_NODELAY)
    static constexpr int kNoDelay = 1;
    best_effort_set_int_socket_option(fd, IPPROTO_TCP, TCP_NODELAY, kNoDelay);
#endif
    best_effort_enable_bbr(fd);
}

bool connect_with_timeout(int fd, const struct sockaddr* address, socklen_t address_len, int timeout_ms) {
    const int original_flags = ::fcntl(fd, F_GETFL, 0);
    if (original_flags < 0) {
        return ::connect(fd, address, address_len) == 0;
    }
    if (::fcntl(fd, F_SETFL, original_flags | O_NONBLOCK) != 0) {
        return ::connect(fd, address, address_len) == 0;
    }

    const int connect_result = ::connect(fd, address, address_len);
    if (connect_result == 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        return true;
    }
    if (errno != EINPROGRESS) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }

    pollfd descriptor {};
    descriptor.fd = fd;
    descriptor.events = POLLOUT;
    const int ready = ::poll(&descriptor, 1, timeout_ms);
    if (ready <= 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        errno = ready == 0 ? ETIMEDOUT : errno;
        return false;
    }

    int socket_error = 0;
    socklen_t socket_error_len = sizeof(socket_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) != 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }
    if (socket_error != 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        errno = socket_error;
        return false;
    }

    (void)::fcntl(fd, F_SETFL, original_flags);
    return true;
}

}  // namespace

ScopedFd connect_tcp(std::string_view host, std::uint16_t port, int retries, int retry_delay_ms) {
    const std::string host_string(host);
    const std::string service = port_string(port);

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* raw = nullptr;
    const int gai = ::getaddrinfo(host_string.c_str(), service.c_str(), &hints, &raw);
    if (gai != 0) {
        throw std::runtime_error("getaddrinfo failed: " + std::string(::gai_strerror(gai)));
    }
    AddrInfoGuard guard(raw);

    for (int attempt = 0; attempt < retries; ++attempt) {
        for (struct addrinfo* current = raw; current != nullptr; current = current->ai_next) {
            ScopedFd fd(::socket(current->ai_family, current->ai_socktype, current->ai_protocol));
            if (!fd.valid()) {
                continue;
            }
            if (connect_with_timeout(fd.get(), current->ai_addr, current->ai_addrlen, retry_delay_ms)) {
                tune_stream_socket(fd.get());
                return fd;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
    }

    throw std::system_error(errno, std::generic_category(), "connect failed for " + host_string + ":" + service);
}

ScopedFd listen_tcp(std::string_view host, std::uint16_t port, int backlog) {
    const std::string host_string(host);
    const std::string service = port_string(port);

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo* raw = nullptr;
    const int gai = ::getaddrinfo(host_string.empty() ? nullptr : host_string.c_str(), service.c_str(), &hints, &raw);
    if (gai != 0) {
        throw std::runtime_error("getaddrinfo failed: " + std::string(::gai_strerror(gai)));
    }
    AddrInfoGuard guard(raw);

    for (struct addrinfo* current = raw; current != nullptr; current = current->ai_next) {
        ScopedFd fd(::socket(current->ai_family, current->ai_socktype, current->ai_protocol));
        if (!fd.valid()) {
            continue;
        }

        int reuse = 1;
        ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (::bind(fd.get(), current->ai_addr, current->ai_addrlen) != 0) {
            continue;
        }
        if (::listen(fd.get(), backlog) != 0) {
            continue;
        }
        return fd;
    }

    throw std::system_error(errno, std::generic_category(), "listen failed on " + service);
}

ScopedFd accept_tcp(int listen_fd) {
    const int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "accept failed");
    }
    tune_stream_socket(fd);
    return ScopedFd(fd);
}

ScopedFd connect_unix(const std::string& path, int retries, int retry_delay_ms) {
    if (path.empty() || path.size() >= sizeof(sockaddr_un::sun_path)) {
        throw std::invalid_argument("invalid Unix socket path");
    }

    sockaddr_un address {};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1U);

    for (int attempt = 0; attempt < retries; ++attempt) {
        ScopedFd fd(::socket(AF_UNIX, SOCK_STREAM, 0));
        if (fd.valid() &&
            ::connect(fd.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0) {
            return fd;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
    }

    throw std::system_error(errno, std::generic_category(), "connect failed for Unix socket " + path);
}

ScopedFd listen_unix(const std::string& path, int backlog) {
    if (path.empty() || path.size() >= sizeof(sockaddr_un::sun_path)) {
        throw std::invalid_argument("invalid Unix socket path");
    }

    ScopedFd fd(::socket(AF_UNIX, SOCK_STREAM, 0));
    if (!fd.valid()) {
        throw std::system_error(errno, std::generic_category(), "Unix socket creation failed");
    }

    sockaddr_un address {};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1U);
    ::unlink(path.c_str());
    if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        throw std::system_error(errno, std::generic_category(), "Unix socket bind failed: " + path);
    }
    if (::listen(fd.get(), backlog) != 0) {
        throw std::system_error(errno, std::generic_category(), "Unix socket listen failed: " + path);
    }
    return fd;
}

void write_all(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const char*>(data);
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t written = ::send(fd, bytes + offset, size - offset, 0);
        if (written < 0) {
            throw std::system_error(errno, std::generic_category(), "send failed");
        }
        offset += static_cast<std::size_t>(written);
    }
}

bool read_exact_or_eof(int fd, void* data, std::size_t size) {
    auto* bytes = static_cast<char*>(data);
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t received = ::recv(fd, bytes + offset, size - offset, 0);
        if (received == 0) {
            if (offset == 0) {
                return false;
            }
            throw std::runtime_error("unexpected EOF while reading socket");
        }
        if (received < 0) {
            throw std::system_error(errno, std::generic_category(), "recv failed");
        }
        offset += static_cast<std::size_t>(received);
    }
    return true;
}

std::uint16_t socket_port(int fd) {
    struct sockaddr_storage address {};
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&address), &length) != 0) {
        throw std::system_error(errno, std::generic_category(), "getsockname failed");
    }

    if (address.ss_family == AF_INET) {
        const auto* ipv4 = reinterpret_cast<const struct sockaddr_in*>(&address);
        return ntohs(ipv4->sin_port);
    }
    const auto* ipv6 = reinterpret_cast<const struct sockaddr_in6*>(&address);
    return ntohs(ipv6->sin6_port);
}

}  // namespace hypersync
