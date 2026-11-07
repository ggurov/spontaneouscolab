# .wolf/tools — repo-owned wolf tooling (update-immune)

Everything here is committed repo code. The retired `openwolf` CLI and any future external
refresh of `.wolf/hooks/` cannot touch this directory. Protocol: `../SHARDS.md`.

| file | purpose |
|---|---|
| `wolf_bug.js` | bug-log shard tool: `id` / `append` / `occurs` / `search` / `stats` / `rebuild` (rebuild absorbs view strays first) |
| `bootstrap.js` | SessionStart entry (runs before the stock session-start hook): rebuild-if-stale + fresh-clone anatomy seed. Registered in `.claude/settings.json`. |
| `wolf_guard.js` | lint/doctor: shard grammar, duplicate ids, LF-only shards, duplicate cerebrum headers, stale derived view (remedies printed with each finding) |
| `wolf_migrate.js` | one-time cutover + PERMANENT straggler absorber (`--absorb-stray <file>`) |
| `wolf_bug.test.js` | `node --test .wolf/tools/wolf_bug.test.js` — CRC32 parity vs the recorded generator pair, round-trip, fold, absorb, pre-cutover guard |

## After any (forbidden) `openwolf update`, or when hooks look degraded

```bash
git status --short .wolf .claude/settings.json   # inventory the damage
git checkout -- .wolf .claude/settings.json      # restore the committed stack
node .wolf/tools/wolf_guard.js
node --test .wolf/tools/wolf_bug.test.js .wolf/hooks/*.test.js
```

## After deliberate anatomy curation

`node .wolf/tools/wolf_migrate.js --anatomy` re-exports the committed seed
(`../anatomy-seed.json`) from the local store + `CLAUDE/hot_files.md`. The seed changes
only at curation time — never per-write — so it never churns merges.
