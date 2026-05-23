# Piper Design

Piper is the reusable asynchronous pipeline layer used by Hypersync.

## Recommended Pipeline Pattern

Piper is a generic infrastructure library. It should stay independent of any
specific application domain. The rules in this section describe the recommended
pattern Piper is designed to support. Applications may choose how strictly to
enforce the pattern; Hypersync treats these rules as mandatory.

### Composition Notation

Piper documentation uses three levels of composition. The notation is intended
to make diagrams readable at both conceptual and implementation depth.

- `[JobName-N/options]` is a concrete job instance. `N` is the worker or lane
  count when it matters. A job has explicit input(s), explicit output(s), owned
  configuration, and one responsibility.
- `(QueueName-N/options)` is a concrete queue or queue family. `N` is capacity,
  shard count, or lane count; diagrams must state which meaning is being used
  when it is not obvious from context.
- `{PipelineName}` is a reusable pipeline block: a named sequence or graph of
  jobs and queues. Like a job, a pipeline has declared input(s), output(s), and
  configuration. Unlike a job, a pipeline is composition only; it must not hide
  new execution mechanics outside its child jobs and queues.
- `{{ScenarioName}}` is a use case or scenario: a named composition of
  pipelines, jobs, and queues that describes a user-visible workflow or a
  benchmark workflow.

Examples:

```text
{MetaReader} = [FolderSeeder-1]->(FolderQueue)->[MetaReader-<backend>-N]
{{Scanner}} = {MetaReader}->(MetaQueue)->{MetaWriter}
```

A pipeline may have variations, but the variation must be named or listed in
configuration. For example `{MetaWriter}` may have `stats-only`,
`partitioned-parquet`, and `discard` variants. A scenario should be readable at
the conceptual level first, then expandable into concrete jobs and queues when
debugging, tuning, or testing.

Pipelines do not weaken the job boundary rules. A child job still communicates
only through queues. A pipeline cannot justify direct calls between jobs,
private side channels, unbounded hidden buffers, or payload copies that would be
forbidden in the expanded graph.

### Jobs Connected by Queues

The intended Piper model is a graph of independent Jobs connected by bounded
queues of opaque buffer handles.

- A Job has explicit input queue(s) and output queue(s).
- A Job should not know which specific Job produced its input.
- A Job should not know which specific Job consumes its output.
- A Job owns its worker threads and internal settings.
- Pipeline composition should be external to the Job implementation.
- Reusable Jobs should have one responsibility and avoid application-specific
  behavior.

This keeps jobs composable: the same generator, discarder, sender, receiver,
monitor, or transform helper can be reused in benchmarks, tests, scan pipelines,
diff pipelines, sync pipelines, or application-specific tools.

### Queues Carry Ownership, Not Bytes

Piper queues are designed to store only `BufferHandle` values. A queue should
not store paths, file records, serialized payloads, `std::string`,
`std::vector`, typed records, or raw payload bytes.

Pushing to a queue transfers ownership of an existing buffer handle. Popping
from a queue transfers ownership to the consumer. The queue itself does not
copy, inspect, allocate, free, parse, or transform payload bytes.

Bounded queues are the preferred flow-control mechanism. When a downstream
stage cannot keep up, its input queue eventually fills and naturally slows the
upstream producer.

When two inputs have different priority, use separate queues and a priority
consumer/sender that always drains the high-priority queue first. Do not encode
priority into payload bytes or teach the generic queue about application record
types.

### Avoid Payload Copies

Piper encourages ownership transfer instead of memory copies.

- Buffers should be preallocated before steady-state processing starts.
- Jobs should forward work by moving the same handle to an output queue.
- Jobs should discard work by releasing the same handle to the owning pool.
- Large data should not be copied from one pipeline buffer to another.
- Serialization into temporary strings or vectors should be avoided on hot
  paths.

Some applications must copy at external API boundaries, such as a kernel socket
write or a third-party library callback. Piper keeps that concern inside backend
or transport jobs so generic queues and job interfaces still move ownership, not
bytes.

### Generic Infrastructure Stays Generic

Piper components should not understand application payloads.

- Buffer pools manage raw fixed-size memory slots.
- Queues manage buffer handles.
- Sender and receiver jobs transport generic buffers.
- Discarder and generator jobs operate on generic buffers.
- Monitoring reads counters and queue stats, not domain payloads.

Applications may interpret a buffer only after a Job owns the handle, normally
through application-defined payload view helpers.

### Waiting and Backpressure

The recommended waiting model is simple:

- Consumers may wait when an input queue is empty.
- Producers may wait when an output queue is full.
- External-I/O jobs may wait for the external API they own while maintaining
  configured concurrency.
- Shutdown may wait while owned buffers are drained, forwarded, released, or
  flushed.

Reusable Piper jobs should avoid global hot-path mutexes, direct calls into
neighboring Jobs, unbounded queues, and hidden side channels. Queue metrics
should make wait reasons visible: empty-input wait, full-output wait,
processing time, byte counts, record counts, and queue depth/high-water marks.

### Generic Runtime Instrumentation

Piper provides shared runtime metrics for `ThreadedJob` implementations. The
recommended pattern is:

- Jobs expose generic processed counters and byte counters through monitor
  snapshots.
- `ThreadedJob` tracks worker wall time by state: processing, waiting for input,
  waiting for output capacity, waiting for a free buffer, waiting in owned I/O,
  and stopped.
- Generic queue and pool helpers try the non-blocking fast path first. They
  enter a timed wait state only when the fast path fails, so instrumentation does
  not add timestamp work to the common uncontended buffer handoff.
- Application-specific metrics, such as records/s, folders/s, files/s, or
  logical bytes/s, should be exposed by setting the monitor snapshot counters
  and details. They should not require custom queue or lifecycle code.
- Periodic status output should use the same monitor snapshots to report
  cumulative rate, recent/current rate, initial observed rate, mid-run
  historical rate, peak observed rate, tail rate after a job stops, queue
  fullness, and worker wait-state percentages.
- A pipeline can expose those snapshots on demand with `StatusServer` or print
  them on an interval with `PeriodicStatusReporter`; both use the same registry
  and therefore the same counters.

This keeps observability reusable while preserving the buffer-ownership model.
When a pipeline slows down, the generic output should show whether each job is
busy, starved for input, blocked on output backpressure, waiting for pool
buffers, or inside its own external I/O.

### Cooperative Autoscaling

Piper includes a generic autoscaling controller for threaded Jobs. It is
cooperative by design: workers are never killed while they own a buffer or are
inside external I/O. Instead, `ThreadedJob` exposes an active-worker limit, and
workers above that limit park between work items.

The controller consumes only generic pressure metrics:

- input queue fullness
- output queue fullness
- worker busy ratio
- wait-for-input ratio
- wait-for-output ratio
- throughput per second
- optional overload score

It does not inspect payloads or know application semantics. A job-specific
pipeline may expose richer counters, but scaling decisions should remain based
on generic pressure unless the application explicitly owns a domain policy.
When a domain policy is required, the application should compress it into an
`overload_score` callback result: `1.0` means balanced, `>1.0` means overloaded
and eligible to scale up, and `<1.0` means underloaded and eligible to scale
down. Piper still owns the scale decision and worker-limit application; the
callback must not resize jobs or call neighboring jobs directly.

Recommended behavior:

- start at the configured initial worker count
- scale up by the current probe step while input pressure is high and output is
  not blocked; the initial step is normally 100% of the current worker count
- scale down when output backpressure is high or input stays empty
- back off when added workers do not improve throughput materially
- after each backoff, reduce the probe step: 100%, 50%, 25%, then 12.5% by
  default
- use cooldown samples to avoid oscillation
- always respect per-Job min/max bounds

Fixed worker counts remain valid and are preferred for reproducible performance
tests. For production-style adaptive runs, every job should be visible to the
autoscale profile system. If a job is missing from a profile, Piper should
materialize it with autoscale enabled, `min_workers=1`, `initial_workers=1`, and
`max_workers=auto`, where `auto` resolves to `cpu_count * 2` unless the job or
application supplies a smaller safe capacity.
`JobAutoScaleRunner` is the live adapter: it periodically samples a metrics
provider, updates the policy, and applies the resulting active-worker limit to
the target `ThreadedJob`.

For whole pipelines, use `PipelineAutoScaleRunner`. It tunes stages in pipeline
order and only one stage is probed at a time. The score for a stage is how fast
that stage pushes to its output, expressed as generic throughput per second.
When the current stage reaches its configured limit, hits output backpressure,
or rejects a probe, the runner advances to the next stage. This prevents
multiple Jobs from changing at once and hiding which stage actually improved or
hurt pipeline throughput.

Autoscale settings are pipeline-profile scoped rather than global per job. The
same job may learn different steady-state values in a scan pipeline, a data-read
pipeline, a diff pipeline, or a writer pipeline. A first run can start unknown
autoscalable jobs from one worker, persist learned values, and let later runs
start from those learned settings while still adjusting if the workload or host
changes.

## Library Scope

It owns infrastructure that is intentionally independent of NFS, metadata
schemas, hashing policy, parquet output, or sync/copy behavior:

- raw preallocated buffer pools and opaque buffer handles
- bounded buffer queues and sharded buffer queues
- generic job/thread lifecycle helpers
- generic producer, consumer, transform, generator, discarder, and transport jobs
- socket helpers used by generic transports and status listeners
- small config and monitoring helpers that are useful across pipelines

The umbrella header is `piper/src/piper.hpp`; direct includes under
`piper/src/common/`, `piper/src/jobs/`, and `piper/src/monitoring/` are also
supported for smaller compile units.

Queues transport ownership of `BufferHandle` values only. They do not inspect,
copy, allocate, or interpret payload bytes. Application code may interpret a
buffer only after a job owns the handle.

The current C++ namespace is still `hypersync` for compatibility with the
existing codebase. The physical library boundary is now separated so namespace
cleanup can be done later without mixing it with the structural refactor.
