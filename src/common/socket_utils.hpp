#ifndef HYPERSYNC_COMMON_SOCKET_UTILS_HPP
#define HYPERSYNC_COMMON_SOCKET_UTILS_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hypersync {

class ScopedFd {
public:
    ScopedFd() = default;
    explicit ScopedFd(int fd) : fd_(fd) {}
    ~ScopedFd();

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept;
    ScopedFd& operator=(ScopedFd&& other) noexcept;

    [[nodiscard]] int get() const;
    [[nodiscard]] bool valid() const;
    int release();
    void reset(int fd = -1);

private:
    int fd_ = -1;
};

ScopedFd connect_tcp(std::string_view host, std::uint16_t port, int retries = 50, int retry_delay_ms = 20);
ScopedFd listen_tcp(std::string_view host, std::uint16_t port, int backlog = 8);
ScopedFd accept_tcp(int listen_fd);
ScopedFd connect_unix(const std::string& path, int retries = 50, int retry_delay_ms = 20);
ScopedFd listen_unix(const std::string& path, int backlog = 8);
void write_all(int fd, const void* data, std::size_t size);
bool read_exact_or_eof(int fd, void* data, std::size_t size);
std::uint16_t socket_port(int fd);

}  // namespace hypersync

#endif
