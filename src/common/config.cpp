#include "common/config.hpp"

#include <cstdlib>
#include <cctype>
#include <charconv>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <system_error>

namespace hypersync {

namespace {

std::string trim(std::string_view input) {
    std::size_t begin = 0;
    while (begin < input.size() && std::isspace(static_cast<unsigned char>(input[begin])) != 0) {
        ++begin;
    }

    std::size_t end = input.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }

    return std::string(input.substr(begin, end - begin));
}

std::string strip_inline_comment(std::string_view line) {
    bool in_single_quotes = false;
    bool in_double_quotes = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (ch == '\'' && !in_double_quotes) {
            in_single_quotes = !in_single_quotes;
            continue;
        }
        if (ch == '"' && !in_single_quotes) {
            in_double_quotes = !in_double_quotes;
            continue;
        }
        if (ch == '#' && !in_single_quotes && !in_double_quotes) {
            if (i == 0 || std::isspace(static_cast<unsigned char>(line[i - 1])) != 0) {
                return trim(line.substr(0, i));
            }
        }
    }

    return trim(line);
}

std::size_t leading_spaces(std::string_view line) {
    std::size_t count = 0;
    while (count < line.size() && line[count] == ' ') {
        ++count;
    }
    if (count < line.size() && line[count] == '\t') {
        throw std::runtime_error("tab indentation is not supported in YAML config");
    }
    return count;
}

std::size_t find_mapping_separator(std::string_view line) {
    bool in_single_quotes = false;
    bool in_double_quotes = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (ch == '\'' && !in_double_quotes) {
            in_single_quotes = !in_single_quotes;
            continue;
        }
        if (ch == '"' && !in_single_quotes) {
            in_double_quotes = !in_double_quotes;
            continue;
        }
        if (ch == ':' && !in_single_quotes && !in_double_quotes) {
            return i;
        }
    }

    return std::string_view::npos;
}

std::string unescape_double_quoted(std::string_view value) {
    std::string out;
    out.reserve(value.size());

    for (std::size_t i = 0; i < value.size(); ++i) {
        const char ch = value[i];
        if (ch != '\\' || i + 1 >= value.size()) {
            out.push_back(ch);
            continue;
        }

        const char escaped = value[++i];
        switch (escaped) {
            case '\\':
                out.push_back('\\');
                break;
            case '"':
                out.push_back('"');
                break;
            case 'n':
                out.push_back('\n');
                break;
            case 't':
                out.push_back('\t');
                break;
            default:
                out.push_back(escaped);
                break;
        }
    }

    return out;
}

std::string parse_scalar(std::string_view raw_value) {
    const std::string value = trim(raw_value);
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return unescape_double_quoted(std::string_view(value).substr(1, value.size() - 2));
    }
    if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

std::map<std::string, ConfigSection> parse_yaml_mapping(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open config file: " + path.string());
    }

    std::map<std::string, ConfigSection> parsed_sections;
    struct Scope {
        std::size_t indent = 0;
        std::string key;
    };
    std::vector<Scope> scopes;

    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        const std::string cleaned = strip_inline_comment(line);
        if (cleaned.empty()) {
            continue;
        }

        const std::size_t indent = leading_spaces(line);
        const std::string_view trimmed_view = std::string_view(cleaned);
        const std::size_t separator = find_mapping_separator(trimmed_view);
        if (separator == std::string_view::npos) {
            throw std::runtime_error("invalid config line " + std::to_string(line_number) + " in " + path.string());
        }

        const std::string key = trim(trimmed_view.substr(0, separator));
        if (key.empty()) {
            throw std::runtime_error("empty config key on line " + std::to_string(line_number) + " in " + path.string());
        }

        while (!scopes.empty() && indent <= scopes.back().indent) {
            scopes.pop_back();
        }

        std::string section_name;
        for (const auto& scope : scopes) {
            if (!section_name.empty()) {
                section_name.push_back('.');
            }
            section_name.append(scope.key);
        }

        const std::string value = trim(trimmed_view.substr(separator + 1));
        if (value.empty()) {
            scopes.push_back({indent, key});
            continue;
        }

        parsed_sections[section_name][key] = parse_scalar(value);
    }

    return parsed_sections;
}

const std::string* find_value(const ConfigSection& section, std::string_view key) {
    const auto it = section.find(std::string(key));
    if (it == section.end()) {
        return nullptr;
    }
    return &it->second;
}

template <typename T>
T parse_unsigned_integer(std::string_view value, std::string_view key_name) {
    T parsed {};
    const auto* begin = value.data();
    const auto* end = value.data() + value.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end) {
        throw std::runtime_error("invalid integer for config key " + std::string(key_name));
    }
    return parsed;
}

double parse_double_value(std::string_view value, std::string_view key_name) {
    std::string copy(value);
    std::size_t processed = 0;
    const double parsed = std::stod(copy, &processed);
    if (processed != copy.size()) {
        throw std::runtime_error("invalid floating-point value for config key " + std::string(key_name));
    }
    return parsed;
}

bool parse_bool_value(std::string_view value, std::string_view key_name) {
    if (value == "true" || value == "yes" || value == "on" || value == "1") {
        return true;
    }
    if (value == "false" || value == "no" || value == "off" || value == "0") {
        return false;
    }
    throw std::runtime_error("invalid boolean for config key " + std::string(key_name));
}

}  // namespace

ConfigStore::ConfigStore() : ConfigStore(default_config_path()) {}

ConfigStore::ConfigStore(std::filesystem::path source) : ConfigStore(std::vector<std::filesystem::path>{std::move(source)}) {}

ConfigStore::ConfigStore(std::vector<std::filesystem::path> sources) : sources_(std::move(sources)) {
    reload();
}

void ConfigStore::set_sources(std::vector<std::filesystem::path> sources) {
    sources_ = std::move(sources);
    reload();
}

const std::vector<std::filesystem::path>& ConfigStore::sources() const {
    return sources_;
}

void ConfigStore::reload() {
    sections_.clear();
    for (const auto& source : sources_) {
        const auto parsed = parse_yaml_mapping(source);
        for (const auto& [section_name, values] : parsed) {
            auto& merged_section = sections_[section_name];
            merged_section.insert(values.begin(), values.end());
            for (const auto& [key, value] : values) {
                merged_section[key] = value;
            }
        }
    }
}

bool ConfigStore::has_section(std::string_view name) const {
    return sections_.find(std::string(name)) != sections_.end();
}

const ConfigSection& ConfigStore::section(std::string_view name) const {
    static const ConfigSection kEmptySection;
    const auto it = sections_.find(std::string(name));
    if (it == sections_.end()) {
        return kEmptySection;
    }
    return it->second;
}

ConfigSection ConfigStore::merged_sections(const std::vector<std::string>& names) const {
    ConfigSection merged;
    for (const auto& name : names) {
        const auto it = sections_.find(name);
        if (it == sections_.end()) {
            continue;
        }
        merged.insert(it->second.begin(), it->second.end());
        for (const auto& [key, value] : it->second) {
            merged[key] = value;
        }
    }
    return merged;
}

const std::map<std::string, ConfigSection>& ConfigStore::sections() const {
    return sections_;
}

std::filesystem::path default_config_path() {
    const char* env_path = std::getenv("HYPERSYNC_CONFIG");
    if (env_path != nullptr && *env_path != '\0') {
        return std::filesystem::path(env_path);
    }
    const char* piper_env_path = std::getenv("PIPER_CONFIG");
    if (piper_env_path != nullptr && *piper_env_path != '\0') {
        return std::filesystem::path(piper_env_path);
    }
    const std::filesystem::path hypersync_default = std::filesystem::path("hypersync") / "config" / "default.yaml";
    if (std::filesystem::exists(hypersync_default)) {
        return hypersync_default;
    }
    return std::filesystem::path("config") / "default.yaml";
}

std::vector<std::string> default_job_config_sections(std::string_view job_name) {
    return {"jobs.defaults", "jobs." + std::string(job_name)};
}

std::string config_string(const ConfigSection& section, std::string_view key) {
    const std::string* value = find_value(section, key);
    if (value == nullptr) {
        throw std::runtime_error("missing config key " + std::string(key));
    }
    return *value;
}

std::string config_string_or(const ConfigSection& section, std::string_view key, std::string default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? std::move(default_value) : *value;
}

bool config_bool(const ConfigSection& section, std::string_view key) {
    return parse_bool_value(config_string(section, key), key);
}

bool config_bool_or(const ConfigSection& section, std::string_view key, bool default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_bool_value(*value, key);
}

double config_double(const ConfigSection& section, std::string_view key) {
    return parse_double_value(config_string(section, key), key);
}

double config_double_or(const ConfigSection& section, std::string_view key, double default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_double_value(*value, key);
}

std::size_t config_size_t(const ConfigSection& section, std::string_view key) {
    return parse_unsigned_integer<std::size_t>(config_string(section, key), key);
}

std::size_t config_size_t_or(const ConfigSection& section, std::string_view key, std::size_t default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_unsigned_integer<std::size_t>(*value, key);
}

std::uint64_t config_u64(const ConfigSection& section, std::string_view key) {
    return parse_unsigned_integer<std::uint64_t>(config_string(section, key), key);
}

std::uint64_t config_u64_or(const ConfigSection& section, std::string_view key, std::uint64_t default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_unsigned_integer<std::uint64_t>(*value, key);
}

std::uint32_t config_u32(const ConfigSection& section, std::string_view key) {
    return parse_unsigned_integer<std::uint32_t>(config_string(section, key), key);
}

std::uint32_t config_u32_or(const ConfigSection& section, std::string_view key, std::uint32_t default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_unsigned_integer<std::uint32_t>(*value, key);
}

std::uint16_t config_u16(const ConfigSection& section, std::string_view key) {
    return parse_unsigned_integer<std::uint16_t>(config_string(section, key), key);
}

std::uint16_t config_u16_or(const ConfigSection& section, std::string_view key, std::uint16_t default_value) {
    const std::string* value = find_value(section, key);
    return value == nullptr ? default_value : parse_unsigned_integer<std::uint16_t>(*value, key);
}

char config_char(const ConfigSection& section, std::string_view key) {
    const std::string value = config_string(section, key);
    if (value.size() != 1U) {
        throw std::runtime_error("config key " + std::string(key) + " must be a single character");
    }
    return value.front();
}

char config_char_or(const ConfigSection& section, std::string_view key, char default_value) {
    const std::string* value = find_value(section, key);
    if (value == nullptr) {
        return default_value;
    }
    if (value->size() != 1U) {
        throw std::runtime_error("config key " + std::string(key) + " must be a single character");
    }
    return value->front();
}

}  // namespace hypersync
