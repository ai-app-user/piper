# Piper Design

Piper is the reusable asynchronous pipeline layer used by Hypersync.

## Recommended Pipeline Pattern

Piper is a generic infrastructure library. It should stay independent of any
specific application domain. The rules in this section describe the recommended
pattern Piper is designed to support. Applications may choose how strictly to
enforce the pattern; Hypersync treats these rules as mandatory.

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
