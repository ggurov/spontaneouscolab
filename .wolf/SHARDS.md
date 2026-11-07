# Wolf shard protocol (repo-owned — `openwolf update` is RETIRED and absent)

This file is the operating protocol and the truth for this repo. `.wolf/OPENWOLF.md` is
the stock-facing document and may be overwritten by external tooling.

## The invariant

Git merges from concurrent machines must never need manual `.wolf` resolution.
Committed `.wolf/` state is exactly three kinds:

1. **Add-only shards** — one file per writer session (`.wolf/buglog/`, `.wolf/status/done/`,
   plus `handoffs/`, `session-memory/`, `session-ledgers/`). Cross-machine merges are file
   additions. Zero conflicts, by construction.
2. **Frozen archives** — optional `0000-legacy.jsonl`, `memory.md`, `token-ledger.json`.
   No writer ever appends to them.
3. **Curated docs** — cerebrum section files, lean `STATUS.md`, this file. Conflicts here
   are semantic (two sessions disagreeing) and are worth seeing; resolve by union/rebuild,
   never by side-picking.

**"Derived + committed" is banned** — that class (a single shared buglog.json / anatomy
file) is what produces manual `.wolf` merges on multi-machine projects.

## Layout

| path | what | committed? |
|---|---|---|
| `.wolf/buglog/<date>-SESSION-<host>-<uuid>.jsonl` | THE bug log: append-only JSONL, one record per line, fields `id, timestamp, error_message, file, root_cause, fix, tags, related_bugs, occurrences, last_seen, session_id?` | yes |
| `.wolf/buglog/0000-legacy.jsonl` | optional frozen import of pre-shard bugs — never appended | yes |
| `.wolf/buglog.json` | DERIVED view (`{version:1, bugs:[…]}`), rebuilt by `wolf_bug.js rebuild`; stock hooks read it here | **no** (gitignored) |
| `.wolf/cerebrum/{user-preferences, key-learnings, do-not-repeat, decision-log, project-rules}.md` | the cerebrum, split per `## section`; each file starts with its header line; update-in-place rules unchanged | yes |
| `.wolf/status/done/<date>-<HHMMSS>-<host>-<slug>.md` | one file per completed milestone (written at /handoff) | yes |
| `.wolf/STATUS.md` | lean live view: `## 🚀 Next quest` + `## Context` (⚠️ the 🚀 emoji is load-bearing — session-start.js anchors on `/^## 🚀/`) | yes |
| `.wolf/anatomy-seed.json` | curated anatomy seed (`{"version":1,"exported_at":…,"files":{…}}`); refresh deliberately, e.g. via `wolf_migrate.js --anatomy` | yes |
| `.wolf/anatomy.md`, `.wolf/anatomy-index.json` | machine-local derived state; hooks regenerate them per machine | **no** (gitignored) |
| `.wolf/tools/` | repo-owned tooling (this protocol's code) — no external update can touch it | yes |
| `.wolf/hooks/` | the vendored hook stack, committed (fresh clones need no global install) | yes |

## Daily commands

```bash
node .wolf/tools/wolf_bug.js id                          # {id, timestamp} — collision-checked across ALL shards
node .wolf/tools/wolf_bug.js append --file <path> --error "…" --root-cause "…" \
     --fix "…" --tags a,b [--related BUG-…]              # THE append path
node .wolf/tools/wolf_bug.js occurs BUG-<id> --note "…"  # a recurrence = an EVENT line in your own shard (never mutate the old record)
node .wolf/tools/wolf_bug.js search "<text>"             # bug search
node .wolf/tools/wolf_bug.js stats                       # per-shard counts + uniqueness assert
node .wolf/tools/wolf_guard.js                           # lint: shard grammar, dup ids, dup cerebrum headers, stale view
node --test .wolf/tools/wolf_bug.test.js .wolf/hooks/*.test.js   # the test suite
```

Bug IDs keep the `BUG-<hostname>-<crc32-of-UTC-ISO-micros>` scheme (host-scoped; hostnames
must stay distinct across machines). Occurrence counts are DERIVED: base `occurrences` + the
count of `occurrence_of` event lines; the derived view folds them automatically.

⚠️ Never append JSONL by hand from PowerShell 5.1 (`>>` writes UTF-16LE+CRLF); use the tool,
`node -e`, or Git Bash. Never edit `.wolf/buglog.json` (the view) — `rebuild` absorbs such
strays back into shards, but only because it assumes they were mistakes.

## Merging across machines

Shards union automatically — nothing to do. The only rule that matters:

**On a `modify/delete` conflict for a demoted/derived file (`buglog.json`, old single-file
`cerebrum.md`, old `STATUS.md`, `anatomy*`) — NEVER `git add` the conflicted path (it
resurrects the file as tracked) and NEVER resolve with plain `git rm` before rescuing the
content.** The procedure:

```bash
# while the conflicted working-tree copy still exists:
git show :2:.wolf/buglog.json > /tmp/stray.json 2>/dev/null || git show :3:.wolf/buglog.json > /tmp/stray.json
node .wolf/tools/wolf_migrate.js --absorb-stray /tmp/stray.json   # imports only unknown ids
git rm --cached .wolf/buglog.json && git commit                    # --cached keeps the local derived copy
```

For anatomy: `git rm --cached .wolf/anatomy.md .wolf/anatomy-index.json` — local copies stay
as machine-local derived state. Cerebrum conflicts: union both sides' bullets by hand (two
sessions can append to the same section — that is a semantic conflict, keep both).
STATUS `## 🚀 Next quest` conflict: rebuild from `handoffs/`, never side-pick.

## `openwolf update` is RETIRED

The CLI is not used here; the hook stack is committed, locally-patched repo code (see the
LOCAL PATCH markers in `.wolf/hooks/*.js`). If anything ever runs an update anyway, it is
just an uncommitted working-tree change:

```bash
git status --short .wolf .claude/settings.json     # shows the damage
git checkout -- .wolf .claude/settings.json        # restores the stack
node .wolf/tools/wolf_guard.js && node --test .wolf/tools/wolf_bug.test.js
```

Never follow in-hook advice to run `openwolf update` (the stock strings are patched out in
this template; a stock regression would reintroduce them — ignore it).

## Session-start bootstrap

`.claude/settings.json` runs `node .wolf/tools/bootstrap.js` BEFORE the stock session-start
hook: it rebuilds the derived buglog view when shards are newer (so a pull's shards become
visible to stock readers within one session start) and seeds machine-local anatomy from the
committed seed on fresh clones. It fails LOUD (non-zero exit) — repo-owned tools never
inherit the stock hooks' exit-0-on-error culture.

## Design notes (what v1 deliberately did not do)

No per-path anatomy shards (the seed + local regeneration cover it). No mesh compiler (the
view rebuild is ~8 ms). No automatic GC (growth is ~500 small files/yr; pruning history is a
human decision). The optional frozen files (`memory.md`, `token-ledger.json`) are untouched
if present; new telemetry goes to per-session `session-ledgers/` files instead.
