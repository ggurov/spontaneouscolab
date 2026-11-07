---
name: openwolf
description: OpenWolf operating protocol for this project. Load when starting multi-file work, resuming a session, wrapping up a session, or when unsure how to use .wolf/ state files (anatomy, cerebrum, memory, buglog, STATUS).
---

# OpenWolf Operating Protocol

You are working in an OpenWolf-managed project. The hooks maintain `.wolf/anatomy.md` (a machine-local derived index), track reads, and surface anatomy hints. Each agent session generates one `SESSION-<hostname>-<UUID>` ID, reused across hook processes/resume/compaction; clear starts a fresh ID. Startup shows the session's memory and handoff paths. Runtime state and snapshots are keyed by that ID.

Hooks require a stable session_id, transcript_path, CODEX_THREAD_ID, or OPENWOLF_SESSION_ID. If the adapter supplies none, export OPENWOLF_SESSION_ID once using `node .wolf/hooks/session-id.js --new` before launching each agent. Subagents sharing a parent key must supply agent_id, agent_transcript_path, or OPENWOLF_AGENT_ID. Never generate a new ID per hook or use shared _session.json. Durable protocol: `.wolf/SHARDS.md`; full legacy protocol: `.wolf/OPENWOLF.md`.

## Session resume

Read `.wolf/STATUS.md` (lean: `## 🚀 Next quest` + `## Context` only; done history is one file per milestone in `.wolf/status/done/`) and the current/relevant `.wolf/handoffs/<session ID>.md` when resuming. `/handoff` writes the current session's file; preserve independent work in shared project status.

## File navigation

1. To locate a file or size it up before opening, check `CLAUDE/hot_files.md` first (committed, curated). Second, grep `.wolf/anatomy.md` (machine-local derived index) for the path — never read it whole; it is an index.
2. If the description answers your question, skip the full read. For large files, prefer Read with offset/limit; the pre-read hook surfaces the largest sections with line ranges.
3. If a file is not indexed, search with Grep/Glob. The `openwolf` CLI is retired — hooks and tools are repo code under `.wolf/hooks/` and `.wolf/tools/`.

## Code generation and learning

1. Before generating code, grep `.wolf/cerebrum/` (5 section files): respect `do-not-repeat.md` (past mistakes), `key-learnings.md`, and `user-preferences.md`.
2. Update the cerebrum section files whenever you learn something: a user correction or preference, a project convention not obvious from code, an API surprise, a gotcha that would trip a fresh session. The bar is LOW; a missing entry repeats the discovery next session.

## Bug logging

Before fixing any bug: `node .wolf/tools/wolf_bug.js search "<error>"` or `grep -rn "<file or error>" .wolf/buglog/` (per-session append-only JSONL shards); the fix may already be known.

After fixing a real defect, failed test, failed build, or user-reported problem: `node .wolf/tools/wolf_bug.js append --file <path> --error "…" --root-cause "…" --fix "…" --tags a,b [--related BUG-…]`. For a recurrence of a known bug: `node .wolf/tools/wolf_bug.js occurs BUG-<id> --note "…"`. `.wolf/buglog.json` is a gitignored machine-local derived view the stock hooks read — never hand-edit it. Do not log ordinary refactors or repeated edits by themselves.

New IDs are `BUG-<hostname>-<crc32>`; run `node .wolf/tools/wolf_bug.js id` from the repo root (`node .wolf/hooks/bug-id.js` still works). CRC32 is standard IEEE CRC32 of that UTF-8 date/time, rendered as eight lowercase hex digits; the timestamp has six fractional digits and a trailing `Z`. `BUG-legacy-<original number>` preserves old numbers and leading zeros and is reserved for migrated bugs.

Merging wolf state across machines is union-by-construction (shards are per-writer files). Never `git add` a conflicted `.wolf/buglog.json`/cerebrum straggler — absorb it with `node .wolf/tools/wolf_migrate.js --absorb-stray <extracted-file>`, then `git rm --cached <path>` (see `.wolf/SHARDS.md`).

## Token discipline

- Never re-read a file already read this session unless it changed since.
- Prefer anatomy descriptions and targeted Grep over full file reads.
- If appending to a file, do not read the entire file first.

## Session end

Before wrapping up: run `/handoff` (or update `.wolf/handoffs/<session ID>.md`), write a one-line summary to `.wolf/session-memory/<session ID>.md` (`| HH:MM | description | file(s) | outcome | ~tokens |`), and record shared learnings or bugs in `.wolf/cerebrum/` / via `wolf_bug.js append`. Future telemetry uses `.wolf/session-ledgers/<session ID>.json`; historical memory and token-ledger files are frozen. `node .wolf/hooks/ledger.js` derives a combined report.

## On-demand skills

- `/handoff`: regenerate `.wolf/handoffs/<session ID>.md` from this session's actual state.
- `/designqc`: screenshot-based design review of the running app.
- `/reframe`: UI framework selection, migration, and anti-generic design audits.
- `/security-audit`: security review of the project.
