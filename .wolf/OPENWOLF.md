# OpenWolf Operating Protocol

You are working in an OpenWolf-managed project. These rules apply every turn.

OpenWolf's hooks handle the bookkeeping: they maintain `.wolf/anatomy.md` /
`anatomy-index.json` (machine-local derived state; the committed curated seed is
`.wolf/anatomy-seed.json`), track reads, condense Bash output, and surface anatomy hints
when you read files. Do not update the anatomy index manually.

**The durable protocol home is `.wolf/SHARDS.md`** (sharded buglog, cerebrum section
files, `status/done/`). `openwolf update` is RETIRED: the vendored hooks are frozen repo
code; recovery from any accidental update is `git checkout -- .wolf .claude/settings.json`.

## Read-only mode

If a file named `.wolf-disable` exists at the repo root, OpenWolf is in read-only mode:
read `.wolf/anatomy.md`, `.wolf/cerebrum/`, `.wolf/buglog/`, `STATUS.md`,
`OPENWOLF.md`/`SHARDS.md` as normal, but DO NOT write to ANY `.wolf/` file — no memory.md
appends, no anatomy updates, no buglog shard appends (no `wolf_bug.js append`), no
cerebrum edits. Skip every "after-action" / "log this" step below. Treat the wolf tree as
immutable reference data for the session. The hooks honor the same marker and go silent.

⚠️ This is a **repo-local patch** to `.wolf/hooks/shared.js` (`ensureWolfDir()`); stock
OpenWolf dropped `.wolf-disable` in 2.x.

## Paths

Every `.wolf/...` path in this document (and in `cerebrum/`, `buglog/`,
`anatomy.md`, `STATUS.md`) is relative to the **git repository root**, not your shell's
current working directory. Agents commonly start sessions in a subdirectory — do not
resolve `.wolf/anatomy.md` et al. against that cwd. When running Bash commands, prefer an
absolute path or `$CLAUDE_PROJECT_DIR` (set by Claude Code to the repo root regardless of
cwd); when using the Read/Write/Edit tools, always pass the absolute path.

## STATUS.md: read first, keep fresh

`.wolf/STATUS.md` is the handoff document. Read it FIRST when resuming a session; it
replaces re-reading memory, plans, and code to reconstruct context.

Each session writes its handoff to `.wolf/handoffs/<session ID>.md`. When the user
signals a quest is done ("done", "ship it", "next phase", "/clear", "wrap up"), move
finished items to that session's done section, write its next quest (objective, files,
decisions), and bump the date. Regenerate it with `/handoff`. Finished milestones also go
one-per-file into `.wolf/status/done/`; `.wolf/STATUS.md` itself stays lean (`## 🚀 Next
quest` + `## Context` only). Read relevant handoffs alongside the shared project status.
Update `.wolf/STATUS.md` only when consolidating project-wide status, preserving other
sessions' independent work.

## Session identity and isolated entries

Each agent session receives one generated `SESSION-<hostname>-<UUID>` ID. Hooks save a
machine-local mapping from the agent's session key to that ID, and reuse it for every
tool call, resume and compaction. A clear or new startup after session end starts a
fresh ID. Expired runtime state starts a fresh ID while preserving previous telemetry.
Raw harness IDs are routing keys, not Wolf entry IDs or filenames. Host and agent/subagent
identity are part of the routing key; different machines and subagents receive independent
Wolf IDs.

Hooks accept `session_id`, `OPENWOLF_SESSION_ID`, `CODEX_THREAD_ID`, or a stable
`transcript_path`. Subagents sharing a parent key must supply their own `agent_id`,
`agent_transcript_path`, or `OPENWOLF_AGENT_ID`. An adapter without a stable key must
set a token once before launching each agent:

```bash
export OPENWOLF_SESSION_ID="$(node .wolf/hooks/session-id.js --new)"
```

Reuse that token in all hook subprocesses. Do not generate a new ID per hook. Hooks
without any stable key report the adapter requirement instead of using `_session.json`.

The startup hook shows the generated ID and exact memory/handoff paths. Future entries
are isolated by session:

- `.wolf/hooks/sessions/<ID>.json`: runtime tracking; `<ID>.precompact.json`: snapshot.
- `.wolf/hooks/session-identities/`: routing mappings. Both runtime directories are ignored.
- `.wolf/session-memory/<ID>.md`: one semantic summary per milestone and session-end row.
- `.wolf/handoffs/<ID>.md`: the session's done items, next quest and context.
- `.wolf/session-ledgers/<ID>.json`: cumulative telemetry, updated idempotently by that session.

Optional legacy `.wolf/memory.md` / `.wolf/token-ledger.json` remain history snapshots if
present. Hooks do not append to them. Generate a combined telemetry report without
rewriting shared counters or summing inherited history twice:

```bash
node .wolf/hooks/ledger.js
```

Anatomy remains keyed by repository-relative file path, and cerebrum/preferences remain
shared project knowledge (as section files under `.wolf/cerebrum/`). Consolidate matching
topics there; session artifacts never need sequential renumbering. `openwolf update` is
retired — the hooks are frozen repo code.

## File navigation

1. Check the committed guides first (keep your own, e.g. a `hot files` doc) and the
   curated seed `.wolf/anatomy-seed.json`. Grep machine-local `.wolf/anatomy.md` if
   needed — never read it whole (it is an index). The `openwolf find` CLI is retired.
2. If the description suffices, do NOT read the full source. For large files use
   Read with offset/limit. Skip `.venv/`, `/tmp/`, paths outside the repo.

## Code generation

1. Read `.wolf/cerebrum/user-preferences.md`.
2. Grep `do-not-repeat.md` + `key-learnings.md` for the subsystem touched (never all
   section files). Follow matches; build commands/architecture come from your repo's
   own deep docs.

## Cerebrum learning

Update the matching `.wolf/cerebrum/` section file when you learn something that changes
what a future session does: a user correction or stated preference
(`user-preferences.md`), a project convention or API behavior that is not obvious from
the code (`key-learnings.md`), a gotcha that would trip a fresh session
(`do-not-repeat.md`, with date), or a significant technical choice and its why
(`decision-log.md`). Each section file starts with its `## <Title>` header.

**Update in place.** A redundant essay costs the next session real context. Before
appending: grep the section file for the same rule, file, or bug ID. If it exists, tighten
that bullet — do not add a second copy. New entries are one short rule plus a pointer
(a bug ID or a doc path). No session narratives or forensics that already live in
`.wolf/buglog/`. Run `node .wolf/tools/wolf_guard.js` after editing section files.

## Bug logging (full protocol: `.wolf/SHARDS.md`)

**Before fixing:** `node .wolf/tools/wolf_bug.js search "<error>"` or
`grep -rn "<text>" .wolf/buglog/` — never read the whole buglog. Log real defects
only (no refactors); recurrences become `occurs` event lines, not duplicates.

```bash
node .wolf/tools/wolf_bug.js append --file <path> --error "…" --root-cause "…" --fix "…" --tags a,b [--related BUG-…]
node .wolf/tools/wolf_bug.js occurs BUG-<id> --note "…"
node .wolf/tools/wolf_bug.js id   # BUG-<hostname>-<crc32>; never sequential; BUG-legacy-* reserved
```

THE log is `.wolf/buglog/` (append-only JSONL shards + optional frozen `0000-legacy.jsonl`);
`.wolf/buglog.json` is the gitignored derived view — never hand-edit or `git add` it.
Merges union by construction; on modify/delete conflicts never `git add` the straggler
(see `SHARDS.md` absorb procedure). Serialization is owned by `wolf_bug.js`.

## Token discipline

- Never re-read an unmodified file this session; prefer anatomy descriptions + grep.
- Never read the historical ledger/memory, every session shard, all of cerebrum, or
  the whole buglog. Grep the relevant file/shard.

## Session end

Before ending or when asked to wrap up:

1. Update `.wolf/handoffs/<session ID>.md` (or run `/handoff`).
2. Append **one** summary line to `.wolf/session-memory/<session ID>.md` per session or meaningful milestone —
   not one per file edit.
3. Review the session: did you learn anything, did the user correct you, did you fix a
   real bug? If yes, update the `.wolf/cerebrum/` section files and/or append to
   `.wolf/buglog/` (via `wolf_bug.js`) per the rules above.

## On-demand skills

- `/handoff`: regenerate `.wolf/handoffs/<session ID>.md`.
- Stock OpenWolf's `/designqc`, `/reframe`, `/security-audit` commands call the retired
  `openwolf` CLI and are not shipped in this template.
