# Piper Development Guidelines

Piper follows the common WSync project rules from `infra/docs/project-guidelines.md`.
This file records Piper-local defaults so the rules are visible inside the
standalone repository.

## Branch Workflow

- Use `dev` as the default working branch.
- Push normal iteration commits to `dev`.
- Merge or push to `main` only when the maintainer explicitly asks for a main
  merge or release promotion.

## Pipeline Vocabulary

- Use `[JobName-N/options]` for concrete jobs. `N` is the worker/lane count when
  shown.
- Use `(QueueName-N/options)` for concrete queues. If `N` could mean shard
  count, queue depth, or lane count, say which one in the surrounding text.
- Use `{PipelineName}` for reusable pipeline blocks. A pipeline has declared
  input(s), output(s), configuration, and named variations, but it is only a
  composition of jobs and queues.
- Use `{{ScenarioName}}` for user-facing or benchmark scenarios. A scenario is
  a composition of pipelines, jobs, and queues.
- Document scenarios conceptually first, then provide the expanded concrete job
  graph when performance tuning or debugging requires it.
- Do not hide execution behind a pipeline name. Expanding a pipeline must reveal
  normal jobs connected by normal queues, with no direct job-to-job calls or
  private non-pipeline side channels.
- Keep jobs generic. A job may have zero, one, or multiple buffer queues as
  input and zero, one, or multiple buffer queues as output. Most jobs should
  treat buffers as opaque bytes and should not inspect product-specific payload
  layouts unless that is their explicit responsibility.
- Raw buffers are preallocated once and reused until app exit. Hot paths must
  not allocate or free payload buffers during steady-state work.
- If a buffer needs self-description, use the generic Piper buffer metadata
  footer: final magic bytes, metadata size, metadata version, logical data size,
  optional checksum algorithm/checksums, and optional packed sub-buffer entries.
  `checksum_algorithm=none` or an individual checksum value of `0` means that
  checksum is not used.

## AI Session Context

Keep AI session handoff context tracked in Git under `doc/ai/`.

Required layout:

- `doc/ai/chat.md`: concise timestamped conversation history with both User and
  Codex messages. Record decisions, requests, summaries of actions, and final
  outcomes. Do not paste huge command outputs.
- `doc/ai/kb.md`: durable handoff facts such as paths, commands, branch rules,
  benchmark baselines, operational state, important decisions, known failures,
  and current next steps. Keep it useful as future-session context, not as a
  raw transcript.
- `doc/ai/scripts/`: reusable scripts created or modified during AI sessions.

When the maintainer sends exactly `sync`, update `doc/ai/chat.md` and
`doc/ai/kb.md` with the latest conversation and current important facts, then
commit and push those context updates to `dev`.
