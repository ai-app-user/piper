#include "monitoring/autoscaler.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/config.hpp"

namespace hypersync {

namespace {

std::size_t parse_profile_size_or(std::string_view value, std::size_t default_value) {
    if (value.empty() || value == "auto") {
        return default_value;
    }
    std::size_t parsed = 0;
    for (const char ch : value) {
        if (ch < '0' || ch > '9') {
            return default_value;
        }
        parsed = parsed * 10U + static_cast<std::size_t>(ch - '0');
    }
    return parsed;
}

}  // namespace

std::size_t default_autoscale_max_workers() noexcept {
    const unsigned int hardware_threads = std::thread::hardware_concurrency();
    return std::max<std::size_t>(1U, static_cast<std::size_t>(hardware_threads == 0U ? 1U : hardware_threads) * 2U);
}

AutoScaleProfileStore::AutoScaleProfileStore(std::filesystem::path path)
    : path_(std::move(path)) {
    load();
}

void AutoScaleProfileStore::set_path(std::filesystem::path path) {
    path_ = std::move(path);
    load();
}

const std::filesystem::path& AutoScaleProfileStore::path() const noexcept {
    return path_;
}

void AutoScaleProfileStore::load() {
    profiles_.clear();
    if (path_.empty() || !std::filesystem::exists(path_)) {
        return;
    }

    ConfigStore config(path_);
    static constexpr std::string_view kPrefix = "autoscale_profiles.";
    static constexpr std::string_view kJobs = ".jobs.";
    for (const auto& [section_name, section] : config.sections()) {
        if (section_name.rfind(std::string(kPrefix), 0) != 0) {
            continue;
        }
        const std::string tail = section_name.substr(kPrefix.size());
        const std::size_t jobs_offset = tail.find(kJobs);
        if (jobs_offset == std::string::npos) {
            continue;
        }
        const std::string profile_name = tail.substr(0, jobs_offset);
        const std::string job_name = tail.substr(jobs_offset + kJobs.size());
        if (profile_name.empty() || job_name.empty()) {
            continue;
        }

        AutoScaleJobProfile profile = default_job_profile();
        profile.autoscale = config_bool_or(section, "autoscale", profile.autoscale);
        profile.min_workers = config_size_t_or(section, "min_workers", profile.min_workers);
        profile.initial_workers = config_size_t_or(section, "initial_workers", profile.initial_workers);
        profile.learned_workers = config_size_t_or(section, "learned_workers", profile.learned_workers);
        const auto max_it = section.find("max_workers");
        if (max_it != section.end()) {
            profile.max_workers = parse_profile_size_or(max_it->second, 0U);
        }
        if (profile.min_workers == 0U) {
            profile.min_workers = 1U;
        }
        if (profile.initial_workers == 0U) {
            profile.initial_workers = profile.min_workers;
        }
        profiles_[profile_name][job_name] = profile;
    }
}

void AutoScaleProfileStore::save() const {
    if (path_.empty()) {
        return;
    }
    if (!path_.parent_path().empty()) {
        std::filesystem::create_directories(path_.parent_path());
    }
    std::ofstream output(path_);
    if (!output) {
        throw std::runtime_error("failed to write autoscale profile: " + path_.string());
    }
    output << "autoscale_profiles:\n";
    for (const auto& [profile_name, jobs] : profiles_) {
        output << "  " << profile_name << ":\n";
        output << "    jobs:\n";
        for (const auto& [job_name, profile] : jobs) {
            output << "      " << job_name << ":\n";
            output << "        autoscale: " << (profile.autoscale ? "true" : "false") << "\n";
            output << "        min_workers: " << profile.min_workers << "\n";
            output << "        max_workers: ";
            if (profile.max_workers == 0U) {
                output << "auto\n";
            } else {
                output << profile.max_workers << "\n";
            }
            output << "        initial_workers: " << profile.initial_workers << "\n";
            output << "        learned_workers: " << profile.learned_workers << "\n";
        }
    }
}

bool AutoScaleProfileStore::empty() const noexcept {
    return profiles_.empty();
}

AutoScaleJobProfile AutoScaleProfileStore::job_profile(const std::string& profile_name,
                                                       const std::string& job_name) {
    auto& jobs = profiles_[profile_name];
    auto [it, inserted] = jobs.emplace(job_name, default_job_profile());
    (void)inserted;
    return it->second;
}

AutoScalePolicy AutoScaleProfileStore::job_policy(const std::string& profile_name,
                                                  const std::string& job_name,
                                                  std::size_t capacity_workers) {
    AutoScaleJobProfile profile = job_profile(profile_name, job_name);
    AutoScalePolicy policy;
    policy.enabled = profile.autoscale;
    policy.min_workers = std::max<std::size_t>(1U, profile.min_workers);
    policy.max_workers = resolve_max_workers(profile, capacity_workers);
    policy.initial_workers = profile.learned_workers != 0U ? profile.learned_workers : profile.initial_workers;
    policy.cooldown_samples = 1U;
    policy.max_cooldown_samples = 5U;
    return policy;
}

void AutoScaleProfileStore::update_learned_workers(const std::string& profile_name,
                                                   const std::string& job_name,
                                                   std::size_t learned_workers) {
    auto& profile = profiles_[profile_name][job_name];
    if (profile.min_workers == 0U) {
        profile = default_job_profile();
    }
    profile.learned_workers = std::max<std::size_t>(1U, learned_workers);
    profile.initial_workers = profile.learned_workers;
}

AutoScaleJobProfile AutoScaleProfileStore::default_job_profile() {
    AutoScaleJobProfile profile;
    profile.autoscale = true;
    profile.min_workers = 1U;
    profile.max_workers = 0U;
    profile.initial_workers = 1U;
    profile.learned_workers = 0U;
    return profile;
}

std::size_t AutoScaleProfileStore::resolve_max_workers(const AutoScaleJobProfile& profile,
                                                       std::size_t capacity_workers) noexcept {
    const std::size_t profile_max = profile.max_workers == 0U ? default_autoscale_max_workers()
                                                              : profile.max_workers;
    const std::size_t capacity = capacity_workers == 0U ? profile_max : capacity_workers;
    return std::max<std::size_t>(std::max<std::size_t>(1U, profile.min_workers),
                                 std::min(profile_max, capacity));
}

AutoScaler::AutoScaler(AutoScalePolicy policy) {
    reset(policy);
}

void AutoScaler::reset(AutoScalePolicy policy) {
    policy_ = policy;
    if (policy_.min_workers == 0U) {
        policy_.min_workers = 1U;
    }
    if (policy_.max_workers < policy_.min_workers) {
        policy_.max_workers = policy_.min_workers;
    }
    if (policy_.cooldown_samples == 0U) {
        policy_.cooldown_samples = 1U;
    }
    if (policy_.max_cooldown_samples < policy_.cooldown_samples) {
        policy_.max_cooldown_samples = policy_.cooldown_samples;
    }
    policy_.initial_probe_step_ratio = std::clamp(policy_.initial_probe_step_ratio, 0.125, 1.0);
    policy_.min_probe_step_ratio = std::clamp(policy_.min_probe_step_ratio, 0.01, policy_.initial_probe_step_ratio);
    active_workers_ = clamp_workers(policy_.initial_workers == 0U ? policy_.min_workers
                                                                  : policy_.initial_workers);
    samples_since_change_ = policy_.cooldown_samples;
    previous_throughput_ = 0.0;
    best_throughput_ = 0.0;
    best_workers_ = active_workers_;
    rejected_probe_workers_ = 0;
    rejected_probe_samples_ = 0;
    bad_probe_samples_ = 0;
    current_cooldown_samples_ = policy_.cooldown_samples;
    probe_step_ratio_ = policy_.initial_probe_step_ratio;
}

const AutoScalePolicy& AutoScaler::policy() const noexcept {
    return policy_;
}

std::size_t AutoScaler::active_workers() const noexcept {
    return active_workers_;
}

AutoScaleDecision AutoScaler::update(const AutoScaleMetrics& metrics) {
    AutoScaleDecision decision;
    decision.active_workers = active_workers_;
    if (!policy_.enabled) {
        decision.reason = "disabled";
        return decision;
    }

    ++samples_since_change_;
    if (rejected_probe_workers_ != 0U) {
        ++rejected_probe_samples_;
    }
    const bool in_cooldown = samples_since_change_ < current_cooldown_samples_;
    const bool output_blocked =
        metrics.output_fullness >= policy_.output_blocked_fullness ||
        metrics.wait_output_ratio >= 0.25;
    const bool input_pressure =
        (metrics.input_fullness >= policy_.scale_up_input_fullness ||
         metrics.input_available_ratio >= 0.95) &&
        metrics.busy_ratio >= policy_.busy_scale_up &&
        metrics.output_fullness < policy_.scale_up_output_fullness_limit;
    const bool input_idle =
        metrics.input_fullness <= policy_.scale_down_input_fullness &&
        (metrics.wait_input_ratio >= policy_.idle_scale_down ||
         metrics.busy_ratio <= (1.0 - policy_.idle_scale_down));

    const bool improved_best =
        metrics.throughput_per_second > 0.0 &&
        (best_throughput_ == 0.0 ||
         metrics.throughput_per_second >= best_throughput_ * (1.0 + policy_.min_improvement_ratio));
    if (improved_best) {
        best_throughput_ = metrics.throughput_per_second;
        best_workers_ = active_workers_;
        rejected_probe_workers_ = 0;
        rejected_probe_samples_ = 0;
        bad_probe_samples_ = 0;
    }
    const bool worse_than_best =
        best_throughput_ > 0.0 &&
        active_workers_ > best_workers_ &&
        metrics.throughput_per_second < best_throughput_ * (1.0 + policy_.min_improvement_ratio);

    std::size_t next_workers = active_workers_;
    const char* reason = "hold";
    if (!in_cooldown && output_blocked && active_workers_ > policy_.min_workers) {
        bad_probe_samples_ = 0;
        reduce_probe_step();
        const std::size_t step = worker_step();
        next_workers = active_workers_ > step ? active_workers_ - step : policy_.min_workers;
        next_workers = std::max(policy_.min_workers, next_workers);
        reason = "output_backpressure";
    } else if (!in_cooldown && input_pressure && !output_blocked &&
               active_workers_ < policy_.max_workers) {
        // When a job has sustained input and no downstream backpressure, the
        // pipeline is asking for more capacity. Short throughput windows are
        // often noisy for filesystem work because folder/file shapes vary, so
        // do not reject a scale-up probe solely because the last sample dipped.
        bad_probe_samples_ = 0;
        const std::size_t step = worker_step();
        next_workers = std::min(policy_.max_workers, active_workers_ + step);
        reason = "input_pressure";
    } else if (!in_cooldown && worse_than_best) {
        ++bad_probe_samples_;
        if (bad_probe_samples_ >= policy_.backoff_confirmation_samples) {
            reduce_probe_step();
            rejected_probe_workers_ = active_workers_;
            rejected_probe_samples_ = 0;
            bad_probe_samples_ = 0;
            next_workers = best_workers_;
            reason = "return_to_best";
        }
    } else if (!in_cooldown && input_idle && active_workers_ > policy_.min_workers) {
        bad_probe_samples_ = 0;
        const std::size_t step = worker_step();
        next_workers = active_workers_ > step ? active_workers_ - step : policy_.min_workers;
        next_workers = std::max(policy_.min_workers, next_workers);
        reason = "input_idle";
    }

    previous_throughput_ = metrics.throughput_per_second;
    next_workers = clamp_workers(next_workers);
    decision.active_workers = next_workers;
    decision.changed = next_workers != active_workers_;
    decision.reason = reason;
    if (decision.changed) {
        active_workers_ = next_workers;
        samples_since_change_ = 0;
        current_cooldown_samples_ = std::min(policy_.max_cooldown_samples,
                                             current_cooldown_samples_ + 1U);
    }
    return decision;
}

std::size_t AutoScaler::clamp_workers(std::size_t value) const noexcept {
    return std::clamp(value, policy_.min_workers, policy_.max_workers);
}

std::size_t AutoScaler::worker_step() const noexcept {
    const auto step = static_cast<std::size_t>(
        std::ceil(static_cast<double>(std::max<std::size_t>(1, active_workers_)) * probe_step_ratio_));
    return std::max<std::size_t>(1, step);
}

void AutoScaler::reduce_probe_step() noexcept {
    probe_step_ratio_ = std::max(policy_.min_probe_step_ratio, probe_step_ratio_ * 0.5);
}

JobAutoScaleRunner::JobAutoScaleRunner(ThreadedJob& job,
                                       AutoScalePolicy policy,
                                       MetricsProvider metrics_provider,
                                       std::chrono::milliseconds interval)
    : job_(job),
      scaler_(policy),
      metrics_provider_(std::move(metrics_provider)),
      interval_(interval) {
    if (!metrics_provider_) {
        throw std::invalid_argument("job autoscale runner requires a metrics provider");
    }
    if (interval_.count() <= 0) {
        throw std::invalid_argument("job autoscale runner interval must be positive");
    }
    job_.set_active_worker_limit(scaler_.active_workers());
}

JobAutoScaleRunner::~JobAutoScaleRunner() {
    stop();
}

void JobAutoScaleRunner::set_decision_callback(DecisionCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    decision_callback_ = std::move(callback);
}

void JobAutoScaleRunner::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }
    worker_ = std::thread([this] {
        loop();
    });
}

void JobAutoScaleRunner::stop() {
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running && worker_.joinable()) {
        worker_.join();
    }
}

bool JobAutoScaleRunner::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

std::size_t JobAutoScaleRunner::active_workers() const noexcept {
    return scaler_.active_workers();
}

void JobAutoScaleRunner::loop() {
    while (running_.load(std::memory_order_acquire)) {
        const AutoScaleMetrics metrics = metrics_provider_();
        AutoScaleDecision decision = scaler_.update(metrics);
        const std::size_t requested_workers = decision.active_workers;
        const std::size_t applied_workers = job_.set_active_worker_limit(requested_workers);
        if (applied_workers != requested_workers) {
            // ThreadedJob::wait() wakes all parked workers for shutdown. That
            // is lifecycle, not an autoscale decision, so do not report or
            // persist it as the learned worker count.
            decision.active_workers = applied_workers;
            decision.changed = false;
            decision.reason = "shutdown";
        } else {
            decision.active_workers = applied_workers;
        }
        DecisionCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = decision_callback_;
        }
        if (callback) {
            callback(decision);
        }
        std::this_thread::sleep_for(interval_);
    }
}

PipelineAutoScaleRunner::PipelineAutoScaleRunner(std::vector<Stage> stages,
                                                 std::chrono::milliseconds interval)
    : stages_(std::move(stages)),
      interval_(interval) {
    if (stages_.empty()) {
        throw std::invalid_argument("pipeline autoscale runner requires at least one stage");
    }
    if (interval_.count() <= 0) {
        throw std::invalid_argument("pipeline autoscale runner interval must be positive");
    }
    scalers_.reserve(stages_.size());
    for (auto& stage : stages_) {
        if (stage.job == nullptr) {
            throw std::invalid_argument("pipeline autoscale stage requires a job");
        }
        if (!stage.metrics_provider) {
            throw std::invalid_argument("pipeline autoscale stage requires a metrics provider");
        }
        scalers_.emplace_back(stage.policy);
        stage.job->set_active_worker_limit(scalers_.back().active_workers());
    }
    stage_exhausted_.assign(stages_.size(), false);
}

PipelineAutoScaleRunner::~PipelineAutoScaleRunner() {
    stop();
}

void PipelineAutoScaleRunner::set_decision_callback(DecisionCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    decision_callback_ = std::move(callback);
}

void PipelineAutoScaleRunner::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }
    worker_ = std::thread([this] {
        loop();
    });
}

void PipelineAutoScaleRunner::stop() {
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running && worker_.joinable()) {
        worker_.join();
    }
}

bool PipelineAutoScaleRunner::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

std::size_t PipelineAutoScaleRunner::active_stage_index() const noexcept {
    return active_stage_index_.load(std::memory_order_acquire);
}

void PipelineAutoScaleRunner::loop() {
    while (running_.load(std::memory_order_acquire)) {
        std::vector<AutoScaleMetrics> metrics;
        metrics.reserve(stages_.size());
        for (const Stage& stage : stages_) {
            metrics.push_back(stage.metrics_provider());
        }

        const std::size_t stage_index = select_stage(metrics);
        active_stage_index_.store(stage_index, std::memory_order_release);
        Stage& stage = stages_[stage_index];
        AutoScaleDecision decision = scalers_[stage_index].update(metrics[stage_index]);
        decision.active_workers = stage.job->set_active_worker_limit(decision.active_workers);

        bool advanced = false;
        if (stage_should_advance(stage, metrics[stage_index], decision)) {
            stage_exhausted_[stage_index] = true;
            const std::size_t next_stage = select_stage(metrics);
            active_stage_index_.store(next_stage, std::memory_order_release);
            advanced = next_stage != stage_index;
        }

        if (!decision.changed && stage_needs_pressure_relief(stage, metrics[stage_index])) {
            advanced = true;
        }

        DecisionCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = decision_callback_;
        }
        if (callback) {
            callback(StageDecision {stage_index, stage.name, decision, advanced});
        }
        std::this_thread::sleep_for(interval_);
    }
}

bool PipelineAutoScaleRunner::stage_can_scale_up(const Stage& stage,
                                                 const AutoScaleMetrics& metrics) const noexcept {
    return stage.job != nullptr &&
           stage.job->active_worker_limit() < stage.policy.max_workers &&
           (metrics.input_fullness >= stage.policy.scale_up_input_fullness ||
            metrics.input_available_ratio >= 0.95) &&
           metrics.busy_ratio >= stage.policy.busy_scale_up &&
           metrics.output_fullness < stage.policy.scale_up_output_fullness_limit &&
           metrics.wait_output_ratio < 0.25;
}

bool PipelineAutoScaleRunner::stage_needs_pressure_relief(const Stage& stage,
                                                          const AutoScaleMetrics& metrics) const noexcept {
    return stage.job != nullptr &&
           stage.job->active_worker_limit() > stage.policy.min_workers &&
           (metrics.output_fullness >= stage.policy.output_blocked_fullness ||
            metrics.wait_output_ratio >= 0.25);
}

bool PipelineAutoScaleRunner::stage_should_advance(const Stage& stage,
                                                   const AutoScaleMetrics& metrics,
                                                   const AutoScaleDecision& decision) const noexcept {
    const std::string_view reason(decision.reason == nullptr ? "" : decision.reason);
    if (reason == "return_to_best" || reason == "no_throughput_gain" ||
        reason == "output_backpressure") {
        return true;
    }
    const bool input_pressure =
        (metrics.input_fullness >= stage.policy.scale_up_input_fullness ||
         metrics.input_available_ratio >= 0.95) &&
        metrics.busy_ratio >= stage.policy.busy_scale_up;
    return input_pressure && decision.active_workers >= stage.policy.max_workers;
}

std::size_t PipelineAutoScaleRunner::select_stage(const std::vector<AutoScaleMetrics>& metrics) const noexcept {
    if (metrics.size() != stages_.size()) {
        return 0;
    }
    for (std::size_t index = 0; index < stages_.size(); ++index) {
        if (!stage_exhausted_[index] && stage_can_scale_up(stages_[index], metrics[index])) {
            return index;
        }
    }
    for (std::size_t reverse = stages_.size(); reverse > 0; --reverse) {
        const std::size_t index = reverse - 1U;
        if (stage_needs_pressure_relief(stages_[index], metrics[index])) {
            return index;
        }
    }
    const std::size_t current = active_stage_index_.load(std::memory_order_acquire);
    return current < stages_.size() ? current : 0;
}

}  // namespace hypersync
