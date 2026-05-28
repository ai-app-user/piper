#include "common/buffer_format.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

namespace hypersync {
namespace {

constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

void require_range(std::size_t offset, std::size_t size, std::size_t capacity, const char* what) {
    if (offset > capacity || size > capacity - offset) {
        throw std::runtime_error(std::string("buffer metadata ") + what + " out of range");
    }
}

void write_u16(std::byte* out, std::uint16_t value) noexcept {
    out[0] = static_cast<std::byte>((value >> 8U) & 0xffU);
    out[1] = static_cast<std::byte>(value & 0xffU);
}

void write_u32(std::byte* out, std::uint32_t value) noexcept {
    for (int shift = 24; shift >= 0; shift -= 8) {
        *out++ = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xffU);
    }
}

void write_u64(std::byte* out, std::uint64_t value) noexcept {
    for (int shift = 56; shift >= 0; shift -= 8) {
        *out++ = static_cast<std::byte>((value >> static_cast<unsigned>(shift)) & 0xffU);
    }
}

std::uint16_t read_u16(const std::byte* in) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in);
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[0]) << 8U) |
                                      static_cast<std::uint16_t>(bytes[1]));
}

std::uint32_t read_u32(const std::byte* in) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in);
    return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
           (static_cast<std::uint32_t>(bytes[1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[2]) << 8U) |
           static_cast<std::uint32_t>(bytes[3]);
}

std::uint64_t read_u64(const std::byte* in) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(in);
    std::uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[index]);
    }
    return value;
}

std::uint64_t non_zero_checksum(BufferChecksumAlgorithm algorithm, const std::byte* data, std::size_t size) {
    const std::uint64_t value = compute_buffer_checksum(algorithm, data, size);
    return value == 0U && algorithm != BufferChecksumAlgorithm::none ? 1U : value;
}

std::uint64_t metadata_checksum_with_zero_field(const std::byte* metadata, std::size_t metadata_size) {
    std::uint64_t hash = kFnvOffsetBasis;
    for (std::size_t index = 0; index < metadata_size; ++index) {
        std::byte byte = metadata[index];
        if (index >= 16U && index < 24U) {
            byte = std::byte {0};
        }
        hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(byte));
        hash *= kFnvPrime;
    }
    return hash == 0U ? 1U : hash;
}

}  // namespace

std::size_t buffer_metadata_size_for_sub_buffers(std::size_t sub_buffer_count) {
    if (sub_buffer_count >
        (std::numeric_limits<std::size_t>::max() - kBufferMetadataHeaderBytes - kBufferMetadataFooterBytes) /
            kBufferSubBufferEntryBytes) {
        throw std::overflow_error("too many sub-buffer descriptors");
    }
    return kBufferMetadataHeaderBytes + sub_buffer_count * kBufferSubBufferEntryBytes + kBufferMetadataFooterBytes;
}

std::uint64_t compute_buffer_checksum(BufferChecksumAlgorithm algorithm, const std::byte* data, std::size_t size) {
    if (algorithm == BufferChecksumAlgorithm::none || data == nullptr) {
        return 0;
    }
    if (algorithm != BufferChecksumAlgorithm::fnv1a64) {
        throw std::invalid_argument("unsupported buffer checksum algorithm");
    }
    std::uint64_t hash = kFnvOffsetBasis;
    for (std::size_t index = 0; index < size; ++index) {
        hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(data[index]));
        hash *= kFnvPrime;
    }
    return hash == 0U ? 1U : hash;
}

void write_buffer_metadata(std::byte* buffer,
                           std::size_t buffer_size,
                           BufferMetadataInfo& info,
                           BufferMetadataWriteOptions options) {
    if (buffer == nullptr) {
        throw std::invalid_argument("buffer metadata target is null");
    }
    if (info.data_size > buffer_size) {
        throw std::invalid_argument("buffer metadata data size exceeds buffer size");
    }
    if (info.sub_buffers.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::overflow_error("too many sub-buffer descriptors");
    }
    const std::size_t metadata_size = buffer_metadata_size_for_sub_buffers(info.sub_buffers.size());
    if (metadata_size > std::numeric_limits<std::uint16_t>::max()) {
        throw std::overflow_error("buffer metadata exceeds 16-bit metadata size field");
    }
    if (metadata_size > buffer_size) {
        throw std::invalid_argument("buffer metadata does not fit in buffer");
    }
    const std::size_t metadata_offset = buffer_size - metadata_size;
    if (info.data_size > metadata_offset) {
        throw std::invalid_argument("buffer metadata overlaps logical data");
    }

    if (info.checksum_algorithm == BufferChecksumAlgorithm::none) {
        info.data_checksum = 0;
        info.metadata_checksum = 0;
    } else if (options.compute_data_checksum) {
        info.data_checksum = non_zero_checksum(info.checksum_algorithm, buffer, info.data_size);
    }
    if (!options.compute_metadata_checksum) {
        info.metadata_checksum = 0;
    }

    std::byte* metadata = buffer + metadata_offset;
    std::fill(metadata, metadata + metadata_size, std::byte {0});

    write_u16(metadata + 0, info.flags);
    write_u16(metadata + 2, static_cast<std::uint16_t>(info.checksum_algorithm));
    write_u32(metadata + 4, static_cast<std::uint32_t>(info.sub_buffers.size()));
    write_u64(metadata + 8, info.data_checksum);
    write_u64(metadata + 16, 0);

    std::byte* entry = metadata + kBufferMetadataHeaderBytes;
    for (const BufferSubBufferInfo& sub_buffer : info.sub_buffers) {
        require_range(sub_buffer.offset, sub_buffer.data_size, info.data_size, "sub-buffer data");
        if (sub_buffer.metadata_size != 0U) {
            require_range(sub_buffer.metadata_offset, sub_buffer.metadata_size, buffer_size, "sub-buffer metadata");
        }
        write_u32(entry + 0, sub_buffer.offset);
        write_u32(entry + 4, sub_buffer.data_size);
        write_u32(entry + 8, sub_buffer.metadata_offset);
        write_u32(entry + 12, sub_buffer.metadata_size);
        write_u64(entry + 16, sub_buffer.data_checksum);
        write_u32(entry + 24, sub_buffer.flags);
        write_u32(entry + 28, 0);
        entry += kBufferSubBufferEntryBytes;
    }

    std::byte* footer = buffer + buffer_size - kBufferMetadataFooterBytes;
    write_u32(footer + 0, info.data_size);
    write_u16(footer + 4, info.version);
    write_u16(footer + 6, static_cast<std::uint16_t>(metadata_size));
    write_u16(footer + 8, kBufferMetadataMagic);

    if (info.checksum_algorithm != BufferChecksumAlgorithm::none && options.compute_metadata_checksum) {
        info.metadata_checksum = metadata_checksum_with_zero_field(metadata, metadata_size);
        write_u64(metadata + 16, info.metadata_checksum);
    }

    if (options.write_inline_copy_after_data && info.data_size + metadata_size <= metadata_offset) {
        std::memcpy(buffer + info.data_size, metadata, metadata_size);
    }
}

std::optional<BufferMetadataInfo> read_buffer_metadata(const std::byte* buffer, std::size_t buffer_size) {
    if (!has_buffer_metadata(buffer, buffer_size)) {
        return std::nullopt;
    }
    const std::byte* footer = buffer + buffer_size - kBufferMetadataFooterBytes;
    const std::uint32_t data_size = read_u32(footer + 0);
    const std::uint16_t version = read_u16(footer + 4);
    const std::uint16_t metadata_size = read_u16(footer + 6);
    if (metadata_size < kBufferMetadataHeaderBytes + kBufferMetadataFooterBytes || metadata_size > buffer_size) {
        return std::nullopt;
    }
    const std::size_t metadata_offset = buffer_size - metadata_size;
    if (data_size > metadata_offset) {
        return std::nullopt;
    }
    const std::byte* metadata = buffer + metadata_offset;
    const std::uint32_t sub_buffer_count = read_u32(metadata + 4);
    if (buffer_metadata_size_for_sub_buffers(sub_buffer_count) != metadata_size) {
        return std::nullopt;
    }

    BufferMetadataInfo info;
    info.data_size = data_size;
    info.version = version;
    info.flags = read_u16(metadata + 0);
    info.checksum_algorithm = static_cast<BufferChecksumAlgorithm>(read_u16(metadata + 2));
    info.data_checksum = read_u64(metadata + 8);
    info.metadata_checksum = read_u64(metadata + 16);
    info.sub_buffers.reserve(sub_buffer_count);

    const std::byte* entry = metadata + kBufferMetadataHeaderBytes;
    for (std::uint32_t index = 0; index < sub_buffer_count; ++index) {
        BufferSubBufferInfo sub_buffer;
        sub_buffer.offset = read_u32(entry + 0);
        sub_buffer.data_size = read_u32(entry + 4);
        sub_buffer.metadata_offset = read_u32(entry + 8);
        sub_buffer.metadata_size = read_u32(entry + 12);
        sub_buffer.data_checksum = read_u64(entry + 16);
        sub_buffer.flags = read_u32(entry + 24);
        if (sub_buffer.offset > data_size || sub_buffer.data_size > data_size - sub_buffer.offset) {
            return std::nullopt;
        }
        if (sub_buffer.metadata_size != 0U &&
            (sub_buffer.metadata_offset > buffer_size ||
             sub_buffer.metadata_size > buffer_size - sub_buffer.metadata_offset)) {
            return std::nullopt;
        }
        info.sub_buffers.push_back(sub_buffer);
        entry += kBufferSubBufferEntryBytes;
    }
    return info;
}

bool validate_buffer_metadata(const std::byte* buffer, std::size_t buffer_size, const BufferMetadataInfo& info) {
    if (buffer == nullptr || info.checksum_algorithm == BufferChecksumAlgorithm::none) {
        return true;
    }
    const auto parsed = read_buffer_metadata(buffer, buffer_size);
    if (!parsed.has_value()) {
        return false;
    }
    const std::uint16_t metadata_size = read_u16(buffer + buffer_size - 4);
    const std::byte* metadata = buffer + buffer_size - metadata_size;
    if (parsed->data_checksum != 0U &&
        parsed->data_checksum != compute_buffer_checksum(parsed->checksum_algorithm, buffer, parsed->data_size)) {
        return false;
    }
    if (parsed->metadata_checksum != 0U &&
        parsed->metadata_checksum != metadata_checksum_with_zero_field(metadata, metadata_size)) {
        return false;
    }
    return true;
}

bool has_buffer_metadata(const std::byte* buffer, std::size_t buffer_size) noexcept {
    if (buffer == nullptr || buffer_size < kBufferMetadataFooterBytes) {
        return false;
    }
    const std::byte* footer = buffer + buffer_size - kBufferMetadataFooterBytes;
    return read_u16(footer + 8) == kBufferMetadataMagic;
}

}  // namespace hypersync
