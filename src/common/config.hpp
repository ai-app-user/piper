#ifndef HYPERSYNC_COMMON_CONFIG_HPP
#define HYPERSYNC_COMMON_CONFIG_HPP

#include <filesystem>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace hypersync {

using ConfigSection = std::map<std::string, std::string>;

class ConfigStore {
public:
    ConfigStore();
    explicit ConfigStore(std::filesystem::path source);
    explicit ConfigStore(std::vector<std::filesystem::path> sources);

    void set_sources(std::vector<std::filesystem::path> sources);
    [[nodiscard]] const std::vector<std::filesystem::path>& sources() const;
    void reload();

    [[nodiscard]] bool has_section(std::string_view name) const;
    [[nodiscard]] const ConfigSection& section(std::string_view name) const;
    [[nodiscard]] ConfigSection merged_sections(const std::vector<std::string>& names) const;
    [[nodiscard]] const std::map<std::string, ConfigSection>& sections() const;

private:
    std::vector<std::filesystem::path> sources_;
    std::map<std::string, ConfigSection> sections_;
};

[[nodiscard]] std::filesystem::path default_config_path();
[[nodiscard]] std::vector<std::string> default_job_config_sections(std::string_view job_name);

[[nodiscard]] std::string config_string(const ConfigSection& section, std::string_view key);
[[nodiscard]] std::string config_string_or(const ConfigSection& section,
                                           std::string_view key,
                                           std::string default_value);
[[nodiscard]] bool config_bool(const ConfigSection& section, std::string_view key);
[[nodiscard]] bool config_bool_or(const ConfigSection& section, std::string_view key, bool default_value);
[[nodiscard]] double config_double(const ConfigSection& section, std::string_view key);
[[nodiscard]] double config_double_or(const ConfigSection& section, std::string_view key, double default_value);
[[nodiscard]] std::size_t config_size_t(const ConfigSection& section, std::string_view key);
[[nodiscard]] std::size_t config_size_t_or(const ConfigSection& section, std::string_view key, std::size_t default_value);
[[nodiscard]] std::uint64_t config_u64(const ConfigSection& section, std::string_view key);
[[nodiscard]] std::uint64_t config_u64_or(const ConfigSection& section, std::string_view key, std::uint64_t default_value);
[[nodiscard]] std::uint32_t config_u32(const ConfigSection& section, std::string_view key);
[[nodiscard]] std::uint32_t config_u32_or(const ConfigSection& section, std::string_view key, std::uint32_t default_value);
[[nodiscard]] std::uint16_t config_u16(const ConfigSection& section, std::string_view key);
[[nodiscard]] std::uint16_t config_u16_or(const ConfigSection& section, std::string_view key, std::uint16_t default_value);
[[nodiscard]] char config_char(const ConfigSection& section, std::string_view key);
[[nodiscard]] char config_char_or(const ConfigSection& section, std::string_view key, char default_value);

}  // namespace hypersync

#endif
