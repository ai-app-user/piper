#ifndef PIPER_PIPER_HPP
#define PIPER_PIPER_HPP

// Umbrella include for the reusable asynchronous pipeline layer.
//
// Applications can include this header when they want the generic buffer pool,
// queue, job, generator, discarder, config, and monitoring helpers without
// depending on Hypersync-specific NFS or metadata code.

#include "common/buffer_pool.hpp"
#include "common/config.hpp"
#include "common/fixed_string.hpp"
#include "common/preallocated_ring.hpp"
#include "common/spsc_ring.hpp"
#include "jobs/buffer_consumer_job.hpp"
#include "jobs/buffer_discarder/buffer_discarder.hpp"
#include "jobs/buffer_generator/buffer_generator.hpp"
#include "jobs/buffer_producer_job.hpp"
#include "jobs/buffer_transform_job.hpp"
#include "jobs/job.hpp"
#include "jobs/queue_job.hpp"
#include "jobs/threaded_job.hpp"
#include "monitoring/autoscaler.hpp"
#include "monitoring/runtime_metrics.hpp"
#include "monitoring/status_monitor.hpp"

#endif
