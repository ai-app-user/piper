#ifndef HYPERSYNC_COMMON_BUFFER_FORMAT_HPP
#define HYPERSYNC_COMMON_BUFFER_FORMAT_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace hypersync {

inline constexpr std::uint16_t kBufferMetadataMagic = 0x4853U;  // HS
inline constexpr std::uint16_t kBufferMetadataVersion = 1;
inline constexpr std::size_t kBufferMetadataFooterBytes = 10;
inline constexpr std::size_t kBufferMetadataHeaderBytes = 24;
inline constexpr std::size_t kBufferSubBufferEntryBytes = 32;

enum class BufferChecksumAlgorithm : std::uint16_t {
    none = 0,
    fnv1a64 = 1,
};

struct BufferSubBufferInfo {
    std::uint32_t offset = 0;
    std::uint32_t data_size = 0;
    std::uint32_t metadata_offset = 0;
    std::uint32_t metadata_size = 0;
    std::uint64_t data_checksum = 0;
    std::uint32_t flags = 0;
};

struct BufferMetadataInfo {
    std::uint32_t data_size = 0;
    std::uint16_t version = kBufferMetadataVersion;
    std::uint16_t flags = 0;
    BufferChecksumAlgorithm checksum_algorithm = BufferChecksumAlgorithm::none;
    std::uint64_t data_checksum = 0;
    std::uint64_t metadata_checksum = 0;
    std::vector<BufferSubBufferInfo> sub_buffers;
};

struct BufferMetadataWriteOptions {
    bool write_inline_copy_after_data = true;
    bool compute_data_checksum = false;
    bool compute_metadata_checksum = false;
};

[[nodiscard]] std::size_t buffer_metadata_size_for_sub_buffers(std::size_t sub_buffer_count);

[[nodiscard]] std::uint64_t compute_buffer_checksum(BufferChecksumAlgorithm algorithm,
                                                    const std::byte* data,
                                                    std::size_t size);

// Writes the canonical metadata block at the end of the physical buffer. If
// requested and enough unused bytes exist between logical data and the trailer,
// the same metadata block is also copied immediately after logical data.
void write_buffer_metadata(std::byte* buffer,
                           std::size_t buffer_size,
                           BufferMetadataInfo& info,
                           BufferMetadataWriteOptions options = {});

[[nodiscard]] std::optional<BufferMetadataInfo> read_buffer_metadata(const std::byte* buffer,
                                                                     std::size_t buffer_size);

[[nodiscard]] bool validate_buffer_metadata(const std::byte* buffer,
                                            std::size_t buffer_size,
                                            const BufferMetadataInfo& info);

[[nodiscard]] bool has_buffer_metadata(const std::byte* buffer, std::size_t buffer_size) noexcept;

}  // namespace hypersync

#endif
