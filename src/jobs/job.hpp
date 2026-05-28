#ifndef HYPERSYNC_JOBS_JOB_HPP
#define HYPERSYNC_JOBS_JOB_HPP

#include <string>
#include <vector>

namespace hypersync {

struct JobDescriptor {
    std::string name;
    std::vector<std::string> input_queues;
    std::vector<std::string> output_queues;
};

}  // namespace hypersync

#endif
