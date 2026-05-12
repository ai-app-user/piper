#include "jobs/job.hpp"

namespace hypersync {

bool is_empty_message(const JobMessage& message) {
    return message.kind.empty() && !message.payload.has_value();
}

std::string message_kind(const JobMessage& message) {
    if (message.kind.empty()) {
        return message_kinds::empty;
    }
    return message.kind;
}

}  // namespace hypersync
