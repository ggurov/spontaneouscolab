Regenerate `.wolf/handoffs/<current Wolf session ID>.md` as this agent's handoff. $ARGUMENTS

Build it from the session's actual state, not from memory of the conversation alone:

1. Use the generated `SESSION-<hostname>-<UUID>` ID shown at startup. Read this session's handoff if it exists, plus `.wolf/STATUS.md` and relevant other handoffs for project context.
2. Run `git status --short` and `git log --oneline -8` to see what actually changed.
3. Skim `.wolf/session-memory/<current session ID>.md` for this agent's action log.

Then create or rewrite only `.wolf/handoffs/<current session ID>.md` with:

- The generated session ID, host, and branch for attribution.
- `## ✅ Done` : what this session completed, one line each, concrete (files, features, fixes). Preserve this session's relevant previous done items.
- `## 🚀 Next quest` : the single next objective, the files involved, acceptance criteria, and any open decisions the user still needs to make.
- `## Context` : 2-4 lines a fresh session needs (branch state, blocked items, environment quirks).
- Bump the date.

Done milestones also go to their own file — `.wolf/status/done/<YYYY-MM-DD>-<HHMMSS>-<host>-<slug>.md`, one file per milestone (sanitize the slug: lowercase, hyphens, no spaces) — so `.wolf/STATUS.md` stays lean and holds ONLY `## 🚀 Next quest` and `## Context`. ⚠️ Keep the `## 🚀` emoji exactly as written — a parser anchors on it.

If you edited `.wolf/cerebrum/` section files this session, run `node .wolf/tools/wolf_guard.js` afterwards.

On a `.wolf/STATUS.md` merge conflict: rebuild it from `.wolf/handoffs/`, never side-pick.

Preserve other agents' files. Consolidate `.wolf/STATUS.md` only when explicitly updating project-wide status, retaining independent open work. Never allocate or renumber session IDs.

Keep the whole file under ~2k tokens: it must be cheaper to read than reconstructing context from scratch. Do not pad it; a short honest handoff beats a complete-looking one.
