# Piper Development Guidelines

Piper follows the common WSync project rules from `infra/docs/project-guidelines.md`.
This file records Piper-local defaults so the rules are visible inside the
standalone repository.

## Branch Workflow

- Use `dev` as the default working branch.
- Push normal iteration commits to `dev`.
- Merge or push to `main` only when the maintainer explicitly asks for a main
  merge or release promotion.

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

