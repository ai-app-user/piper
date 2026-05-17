#ifndef PIPER_MONITORING_AUTOSCALER_HPP
#define PIPER_MONITORING_AUTOSCALER_HPP

// Generic cooperative worker-count controller for piper jobs.
//
// The autoscaler does not know about application payloads. It consumes cheap
// pressure metrics such as queue fullness, worker busy ratio, and throughput,
// then recommends an active worker limit for a ThreadedJob. Jobs stay in charge
// of their own work loops and park workers cooperatively between work items.

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "jobs/threaded_job.hpp"

namespace hypersync {

struct AutoScalePolicy {
    bool enabled = false;
    std::size_t min_workers = 1;
    std::size_t max_workers = 1;
    std::size_t initial_workers = 1;
    double scale_up_input_fullness = 0.70;
    double scale_down_input_fullness = 0.10;
    double output_blocked_fullness = 0.90;
    double scale_up_output_fullness_limit = 0.80;
    double busy_scale_up = 0.70;
    double idle_scale_down = 0.60;
    double min_improvement_ratio = 0.05;
    double initial_probe_step_ratio = 1.0;
    double min_probe_step_ratio = 0.125;
    std::uint64_t cooldown_samples = 2;
    std::uint64_t max_cooldown_samples = 2;
    std::uint64_t backoff_confirmation_samples = 3;
    std::uint64_t rejected_probe_cooldown_samples = 30;
    double overload_scale_up = 1.10;
    double overload_scale_down = 0.90;
};

struct AutoScaleMetrics {
    double input_fullness = 0.0;
    double input_available_ratio = 0.0;
    double output_fullness = 0.0;
    double busy_ratio = 0.0;
    double wait_input_ratio = 0.0;
    double wait_output_ratio = 0.0;
    double throughput_per_second = 0.0;
    // Optional app-provided pressure callback result. A value of 1.0 means the
    // lane is balanced, >1.0 asks the generic scaler for more capacity, and
    // <1.0 allows it to reclaim workers. Jobs may leave this at 1.0 and rely
    // only on generic queue/busy/throughput signals.
    double overload_score = 1.0;
};

struct AutoScaleDecision {
    std::size_t active_workers = 1;
    bool changed = false;
    const char* reason = "disabled";
};

struct AutoScaleJobProfile {
    bool autoscale = true;
    std::size_t min_workers = 1;
    std::size_t max_workers = 0;  // 0 means auto: cpu_count * 2.
    std::size_t initial_workers = 1;
    std::size_t learned_workers = 0;
};

[[nodiscard]] std::size_t default_autoscale_max_workers() noexcept;

// Persisted pipeline-scoped autoscale defaults. Unknown jobs are intentionally
// materialized with autoscale enabled, min=1, initial=1, and max=auto so first
// runs can discover a useful steady state without hand-written config.
class AutoScaleProfileStore {
public:
    AutoScaleProfileStore() = default;
    explicit AutoScaleProfileStore(std::filesystem::path path);

    void set_path(std::filesystem::path path);
    [[nodiscard]] const std::filesystem::path& path() const noexcept;
    void load();
    void save() const;

    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] AutoScaleJobProfile job_profile(const std::string& profile_name,
                                                  const std::string& job_name);
    [[nodiscard]] AutoScalePolicy job_policy(const std::string& profile_name,
                                             const std::string& job_name,
                                             std::size_t capacity_workers = 0);
    void update_learned_workers(const std::string& profile_name,
                                const std::string& job_name,
                                std::size_t learned_workers);

private:
    using JobProfiles = std::map<std::string, AutoScaleJobProfile>;
    [[nodiscard]] static AutoScaleJobProfile default_job_profile();
    [[nodiscard]] static std::size_t resolve_max_workers(const AutoScaleJobProfile& profile,
                                                         std::size_t capacity_workers) noexcept;

    std::filesystem::path path_;
    std::map<std::string, JobProfiles> profiles_;
};

class AutoScaler {
public:
    AutoScaler() = default;
    explicit AutoScaler(AutoScalePolicy policy);

    void reset(AutoScalePolicy policy);
    [[nodiscard]] const AutoScalePolicy& policy() const noexcept;
    [[nodiscard]] std::size_t active_workers() const noexcept;

    // Evaluate one metrics sample and return the desired active worker count.
    // Callers apply the returned value through ThreadedJob::set_active_worker_limit.
    [[nodiscard]] AutoScaleDecision update(const AutoScaleMetrics& metrics);

private:
    [[nodiscard]] std::size_t clamp_workers(std::size_t value) const noexcept;
    [[nodiscard]] std::size_t worker_step() const noexcept;
    void reduce_probe_step() noexcept;

    AutoScalePolicy policy_ {};
    std::size_t active_workers_ = 1;
    std::uint64_t samples_since_change_ = 0;
    double previous_throughput_ = 0.0;
    double best_throughput_ = 0.0;
    std::size_t best_workers_ = 1;
    std::size_t rejected_probe_workers_ = 0;
    std::uint64_t rejected_probe_samples_ = 0;
    std::uint64_t bad_probe_samples_ = 0;
    std::uint64_t current_cooldown_samples_ = 2;
    double probe_step_ratio_ = 1.0;
};

// Periodically samples metrics, updates the autoscaler, and applies the active
// worker limit to a ThreadedJob. The target job still scales cooperatively.
class JobAutoScaleRunner {
public:
    using MetricsProvider = std::function<AutoScaleMetrics()>;
    using DecisionCallback = std::function<void(const AutoScaleDecision&)>;

    JobAutoScaleRunner(ThreadedJob& job,
                       AutoScalePolicy policy,
                       MetricsProvider metrics_provider,
                       std::chrono::milliseconds interval = std::chrono::milliseconds(1000));
    ~JobAutoScaleRunner();

    JobAutoScaleRunner(const JobAutoScaleRunner&) = delete;
    JobAutoScaleRunner& operator=(const JobAutoScaleRunner&) = delete;

    void set_decision_callback(DecisionCallback callback);
    void start();
    void stop();
    [[nodiscard]] bool running() const noexcept;
    [[nodiscard]] std::size_t active_workers() const noexcept;

private:
    void loop();

    ThreadedJob& job_;
    AutoScaler scaler_;
    MetricsProvider metrics_provider_;
    std::chrono::milliseconds interval_;
    DecisionCallback decision_callback_;
    mutable std::mutex mutex_;
    std::thread worker_;
    std::atomic<bool> running_ {false};
};

// Ordered pipeline tuner. It tunes one stage at a time, starting from the
// first stage in pipeline order. A stage is scored only by the throughput it
// pushes to its output. When a stage rejects a probe or reaches its worker
// limit, the runner advances to the next stage.
class PipelineAutoScaleRunner {
public:
    using MetricsProvider = JobAutoScaleRunner::MetricsProvider;

    struct Stage {
        std::string name;
        ThreadedJob* job = nullptr;
        AutoScalePolicy policy;
        MetricsProvider metrics_provider;
    };

    struct StageDecision {
        std::size_t stage_index = 0;
        std::string stage_name;
        AutoScaleDecision decision;
        bool stage_advanced = false;
    };

    using DecisionCallback = std::function<void(const StageDecision&)>;

    PipelineAutoScaleRunner(std::vector<Stage> stages,
                            std::chrono::milliseconds interval = std::chrono::milliseconds(1000));
    ~PipelineAutoScaleRunner();

    PipelineAutoScaleRunner(const PipelineAutoScaleRunner&) = delete;
    PipelineAutoScaleRunner& operator=(const PipelineAutoScaleRunner&) = delete;

    void set_decision_callback(DecisionCallback callback);
    void start();
    void stop();
    [[nodiscard]] bool running() const noexcept;
    [[nodiscard]] std::size_t active_stage_index() const noexcept;

private:
    void loop();
    [[nodiscard]] bool stage_can_scale_up(const Stage& stage,
                                          const AutoScaleMetrics& metrics) const noexcept;
    [[nodiscard]] bool stage_needs_pressure_relief(const Stage& stage,
                                                   const AutoScaleMetrics& metrics) const noexcept;
    [[nodiscard]] bool stage_should_advance(const Stage& stage,
                                            const AutoScaleMetrics& metrics,
                                            const AutoScaleDecision& decision) const noexcept;
    [[nodiscard]] std::size_t select_stage(const std::vector<AutoScaleMetrics>& metrics) const noexcept;

    std::vector<Stage> stages_;
    std::vector<AutoScaler> scalers_;
    std::vector<bool> stage_exhausted_;
    std::chrono::milliseconds interval_;
    DecisionCallback decision_callback_;
    mutable std::mutex mutex_;
    std::thread worker_;
    std::atomic<bool> running_ {false};
    std::atomic<std::size_t> active_stage_index_ {0};
};

}  // namespace hypersync

#endif
