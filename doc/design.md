# Piper Design

Piper is the reusable asynchronous pipeline layer used by Hypersync.

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
