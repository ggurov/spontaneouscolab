import * as fs from "node:fs";
import * as path from "node:path";
import { getWolfDir, ensureWolfDir, appendMarkdown, readJSON, readBugLogFile, timeShort, estimateTokens, readStdin, recordInjectionToSessionFile, getProjectDir, hookMain, gcSessionFiles, readCerebrumSection, latestCerebrumMtime } from "./shared.js";
import { mutateJSON, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";
import { getSessionIdentity, getSessionMemoryPath, getSessionHandoffPath, detectAgent } from "./shared.js";
import { buildSessionEntry, flushSessionToLedger } from "./ledger.js";
import { topRules, scopedRulesForFiles } from "./rule-reinjection.js";
function digestBudget(wolfDir) {
    const cfg = readJSON(path.join(wolfDir, "config.json"), {});
    const ctx = cfg.openwolf?.context ?? {};
    const agent = detectAgent();
    return ctx.budgets?.[agent] ?? ctx.session_digest_budget_tokens ?? 1500;
}
/** Extract one `## heading` section (heading line through the next ## or ---). */
function extractSection(markdown, headingPattern) {
    const lines = markdown.split(/\r?\n/);
    const start = lines.findIndex((l) => headingPattern.test(l));
    if (start === -1)
        return "";
    const out = [lines[start]];
    for (let i = start + 1; i < lines.length; i++) {
        if (/^## /.test(lines[i]) || /^---\s*$/.test(lines[i]))
            break;
        out.push(lines[i]);
    }
    return out.join("\n").trim();
}
/**
 * True when this project's cerebrum was synced into Claude Code auto-memory
 * (openwolf update leaves a marker in the memory index). Claude then recalls
 * Do-Not-Repeat natively, so the digest must not inject it a second time.
 */
function claudeMemoryHasSync() {
    if (detectAgent() !== "claude")
        return false;
    try {
        const home = process.env.HOME ?? process.env.USERPROFILE ?? "";
        if (!home)
            return false;
        const slug = path.resolve(getProjectDir()).replace(/[^a-zA-Z0-9]/g, "-");
        const index = fs.readFileSync(path.join(home, ".claude", "projects", slug, "memory", "MEMORY.md"), "utf-8");
        return index.includes("<!-- openwolf:begin -->");
    }
    catch {
        return false;
    }
}
/** Parse a leading YAML-ish frontmatter block for description/always keys (2.4). */
function readFrontmatter(content) {
    if (!content.startsWith("---\n"))
        return { body: content };
    const end = content.indexOf("\n---", 4);
    if (end === -1)
        return { body: content };
    const fm = content.slice(4, end);
    const body = content.slice(content.indexOf("\n", end + 1) + 1);
    const description = fm.match(/^description:\s*(.+)$/m)?.[1]?.trim();
    const always = /^always:\s*true\s*$/m.test(fm);
    return { description, always, body };
}
/**
 * Progressive-disclosure digest (2.4). The old digest injected content
 * (mean 794 tok, 75% of it a staleness nag or placeholder text). The new
 * shape is an INDEX: one line per live .wolf file with what it is, how big,
 * and how fresh, so the model can decide what to pull with grep or find —
 * the retrieval pattern claude-mem, Letta, and the platform's own skills
 * design converged on. Only two things still inject content: the top
 * Do-Not-Repeat rules (small, highest value, unless native auto-memory
 * already carries them) and any .wolf file whose frontmatter says
 * `always: true` (budget-capped). Target: ~400 tokens.
 */
function buildSessionDigest(wolfDir, budget, sessionId) {
    budget = Math.min(budget, 400 + Math.floor(budget / 4));
    const parts = [];
    let used = 0;
    const tryAdd = (text) => {
        if (!text)
            return false;
        const cost = estimateTokens(text, "prose");
        if (used + cost > budget)
            return false;
        parts.push(text);
        used += cost;
        return true;
    };
    const isPlaceholder = (l) => /_<[^>]*>_|\{\{[^}]*\}\}|^_?<[^>]*>_?$/.test(l.trim());
    // 1. Top Do-Not-Repeat rules (skipped when native auto-memory carries them).
    // Injected BEFORE STATUS: each rule is trimmed to its first line so the
    // three of them always fit under the cap even when the next-quest section
    // is long (measured 2026-09-20: full rules + 14 STATUS lines overflowed
    // the budget and the rules were silently dropped on every cold start).
    if (!claudeMemoryHasSync()) {
        try {
            const cerebrum = readFrontmatter(readCerebrumSection(wolfDir, "Do-Not-Repeat")).body;
            const dnr = extractSection(cerebrum, /^## Do-Not-Repeat/);
            const entries = dnr.split("\n").filter((l) => l.startsWith("- ") && !isPlaceholder(l));
            if (entries.length > 0) {
                const compact = entries.slice(-3).map((e) => {
                    const one = e.replace(/\s+/g, " ").trim();
                    return one.length > 240 ? one.slice(0, 237) + "…" : one;
                });
                tryAdd("Do-Not-Repeat (top rules from .wolf/cerebrum/):\n" + compact.join("\n"));
            }
        }
        catch { }
    }
    // 2. STATUS.md "next phase": the one piece of content that always earns its
    // tokens on resume — but never template placeholders (8/28 measured digests
    // were pure placeholder before this filter).
    try {
        const handoff = getSessionHandoffPath(wolfDir, sessionId);
        const status = fs.readFileSync(fs.existsSync(handoff) ? handoff : path.join(wolfDir, "STATUS.md"), "utf-8");
        const section = extractSection(readFrontmatter(status).body, /^## 🚀/);
        const meaningful = section.split("\n").filter((l) => l.trim() && !isPlaceholder(l));
        const contentLines = meaningful.filter((l) => !/^#|^\||^-+$/.test(l.trim()));
        if (contentLines.length > 0)
            tryAdd(meaningful.slice(0, 14).join("\n"));
    }
    catch { }
    // 3. always:true files — the Letta-style explicit always-loaded set, capped.
    try {
        for (const f of fs.readdirSync(wolfDir)) {
            if (!f.endsWith(".md") || f === "STATUS.md" || f === "OPENWOLF.md" || f === "anatomy.md")
                continue;
            const raw = fs.readFileSync(path.join(wolfDir, f), "utf-8");
            const fm = readFrontmatter(raw);
            if (fm.always) {
                tryAdd(`## .wolf/${f} (always)\n` + fm.body.trim().split("\n").slice(0, 40).join("\n"));
            }
        }
    }
    catch { }
    // 4. The index: one line per live state file. Pointers, not content.
    try {
        const indexLines = [];
        const describe = {
            "cerebrum.md": "learned preferences, conventions, Do-Not-Repeat rules",
            "memory.md": "chronological action log per session",
            "STATUS.md": "session handoff (regenerate with /handoff)",
            "buglog.json": "known bugs with root causes and fixes (openwolf bug search)",
            "anatomy.md": "project file index (grep it, never read whole; the openwolf find CLI is retired)",
        };
        for (const [file, fallback] of Object.entries(describe)) {
            const p = path.join(wolfDir, file);
            let st;
            try {
                st = fs.statSync(p);
            }
            catch {
                continue;
            }
            // LOCAL PATCH (shard cutover): when the shard dirs exist they are the
            // live state — the derived buglog.json and any straggler cerebrum.md
            // must NOT be indexed (the dir lines below cover them), so a healthy
            // session is never pointed at the dead single-file paths.
            if (file === "buglog.json" && fs.existsSync(path.join(wolfDir, "buglog")))
                continue;
            if (file === "cerebrum.md" && fs.existsSync(path.join(wolfDir, "cerebrum")))
                continue;
            let desc = fallback;
            let extra = "";
            if (file === "buglog.json") {
                const bugs = readBugLogFile(wolfDir).bugs.length;
                if (bugs === 0)
                    continue;
                extra = `, ${bugs} bugs`;
            }
            else if (file.endsWith(".md")) {
                const fm = readFrontmatter(fs.readFileSync(p, "utf-8"));
                if (fm.description)
                    desc = fm.description;
                // A template-only file earns no index line.
                if (fm.body.trim().length < 80)
                    continue;
            }
            const tok = Math.ceil(st.size / 4);
            indexLines.push(`- .wolf/${file}: ${desc} (~${tok} tok${extra}, updated ${st.mtime.toISOString().slice(0, 10)})`);
        }
        // LOCAL PATCH (shard cutover 2026-09-18): index the shard directories —
        // the bug count comes from the SHARDS (view-independent), so a fresh clone
        // before bootstrap still shows the true number.
        try {
            const buglogDir = path.join(wolfDir, "buglog");
            if (fs.existsSync(buglogDir)) {
                let bugs = 0;
                for (const f of fs.readdirSync(buglogDir)) {
                    if (!f.endsWith(".jsonl"))
                        continue;
                    for (const line of fs.readFileSync(path.join(buglogDir, f), "utf8").split("\n")) {
                        if (line.trim() && !line.includes('"occurrence_of"'))
                            bugs++;
                    }
                }
                if (bugs > 0)
                    indexLines.push(`- .wolf/buglog/: known bugs, per-session shards (${bugs} bugs; grep -rn or node .wolf/tools/wolf_bug.js search)`);
            }
        }
        catch { }
        try {
            const cerebrumDir = path.join(wolfDir, "cerebrum");
            if (fs.existsSync(cerebrumDir)) {
                const files = fs.readdirSync(cerebrumDir).filter((f) => f.endsWith(".md"));
                if (files.length > 0)
                    indexLines.push(`- .wolf/cerebrum/: learned preferences/conventions/Do-Not-Repeat (${files.length} section files; grep -rn, never read all)`);
            }
        }
        catch { }
        if (indexLines.length > 0) {
            tryAdd("OpenWolf state on disk (read on demand, not preloaded):\n" + indexLines.join("\n"));
        }
    }
    catch { }
    return parts.join("\n\n");
}
async function main() {
    ensureWolfDir();
    const wolfDir = getWolfDir();
    // Clean up stale .tmp files left from failed atomic writes
    try {
        const files = fs.readdirSync(wolfDir);
        for (const f of files) {
            if (f.endsWith(".tmp")) {
                try {
                    fs.unlinkSync(path.join(wolfDir, f));
                }
                catch { }
            }
        }
    }
    catch { }
    const hooksDir = path.join(wolfDir, "hooks");
    const now = new Date();
    // SessionStart fires for startup/resume/clear/compact. Only startup and
    // clear begin a genuinely new session — on resume/compact the session is
    // still live, and resetting the session state would wipe read/write
    // tracking mid-flight (Workstream F3: compaction survival).
    let source = "startup";
    let hookInput = {};
    try {
        hookInput = JSON.parse(await readStdin());
        if (typeof hookInput.source === "string")
            source = hookInput.source;
    }
    catch { }
    gcSessionFiles();
    const identity = getSessionIdentity(hookInput, { newSession: source === "clear", startSource: source });
    const sessionFile = path.join(wolfDir, "hooks", "sessions", `${identity.id}.json`);
    const sessionId = identity.id;
    // Hook install self-test (2.2): a missing dependency file once silently
    // killed a hook for 3 weeks across 440 invocations. Verify every installed
    // hook's relative imports resolve; surface breakage in the digest.
    const brokenHooks = selfTestHooks(hooksDir);
    let created = false;
    const currentSession = mutateJSON(sessionFile, {}, HOOK_LOCK_BUDGET_MS, session => {
        if (session.session_id === sessionId) {
            if (source === "resume" || source === "compact") delete session.ended_at;
            return;
        }
        created = true;
        Object.assign(session, {
            session_id: sessionId,
            hostname: identity.hostname,
            agent: identity.agent,
            harness_session_id: identity.harness_session_id,
            agent_id: identity.agent_id,
            started: identity.started,
            files_read: {}, files_written: [], edit_counts: {},
            anatomy_hits: 0, anatomy_misses: 0, repeated_reads_warned: 0, stop_count: 0,
        });
    });
    if (!currentSession)
        return;
    const continuing = !created;
    // Compaction evicts verbatim file contents from the model's context, so a
    // post-compact re-read is legitimate: clear deny-eligibility on every
    // recorded read (the warn path still fires; only denial is disarmed).
    if (continuing && source === "compact") {
        try {
            mutateJSON(sessionFile, {}, HOOK_LOCK_BUDGET_MS, (session) => {
                for (const entry of Object.values(session.files_read ?? {})) {
                    entry.compacted = true;
                }
            });
        }
        catch { }
    }
    if (!continuing) {
        // Append session header to memory.md
        const memoryPath = getSessionMemoryPath(wolfDir, sessionId);
        const header = `\n## Session: ${sessionId} (${now.toISOString().slice(0, 10)} ${timeShort()})\n\n| Time | Action | File(s) | Outcome | ~Tokens |\n|------|--------|---------|---------|--------|\n`;
        appendMarkdown(memoryPath, header);
    }
    // Startup nudges — folded into the injected digest below (stderr from an
    // exit-0 hook never reaches the model).
    const startupNotes = [];
    startupNotes.push(`OpenWolf session: ${sessionId}. Write summaries to .wolf/session-memory/${sessionId}.md and handoffs to .wolf/handoffs/${sessionId}.md. Snapshots and telemetry are isolated by this ID.`);
    try {
        // LOCAL PATCH (shard cutover 2026-09-18): cerebrum is a directory of
        // section files; count entries across all of them, freshness = newest.
        let cerebrumContent = "";
        let newest = 0;
        const cerebrumDir = path.join(wolfDir, "cerebrum");
        if (fs.existsSync(cerebrumDir)) {
            for (const f of fs.readdirSync(cerebrumDir)) {
                if (f.endsWith(".md"))
                    cerebrumContent += fs.readFileSync(path.join(cerebrumDir, f), "utf-8") + "\n";
            }
        }
        else if (fs.existsSync(path.join(wolfDir, "cerebrum.md"))) {
            cerebrumContent = fs.readFileSync(path.join(wolfDir, "cerebrum.md"), "utf-8");
        }
        newest = latestCerebrumMtime(wolfDir);
        const daysSinceUpdate = (Date.now() - newest) / (1000 * 60 * 60 * 24);
        // Count actual entries (non-comment, non-empty lines in content sections)
        const entryLines = cerebrumContent.split("\n").filter(l => {
            const t = l.trim();
            return t.startsWith("- ") || t.startsWith("* ") || (t.startsWith("[") && t.includes("]"));
        });
        if (entryLines.length < 3) {
            startupNotes.push(`cerebrum has only ${entryLines.length} entries. Record user preferences, project conventions, and mistakes to .wolf/cerebrum/ section files as you learn them this session.`);
        }
        else if (daysSinceUpdate > 3) {
            startupNotes.push(`cerebrum hasn't been updated in ${Math.floor(daysSinceUpdate)} days. Look for opportunities to add learnings this session.`);
        }
    }
    catch { }
    try {
        // LOCAL PATCH (shard cutover): .wolf/buglog.json is a derived view over
        // .wolf/buglog/ shards, refreshed at session start by bootstrap.js. Only
        // nag when BOTH are empty, and point at the shard tool, never the view.
        const viewBugs = readBugLogFile(wolfDir).bugs.length;
        let shardCount = 0;
        try {
            shardCount = fs.readdirSync(path.join(wolfDir, "buglog")).filter((f) => f.endsWith(".jsonl")).length;
        }
        catch { }
        if (viewBugs === 0 && shardCount === 0) {
            startupNotes.push(`buglog is empty. If you encounter or fix any bugs, errors, or failed tests this session, log them: node .wolf/tools/wolf_bug.js append --file <path> --error "..." --root-cause "..." --fix "..." --tags a,b`);
        }
    }
    catch { }
    // One ledger file per generated session; repeated startup/resume is an upsert.
    flushSessionToLedger(wolfDir, buildSessionEntry(currentSession, hookInput.transcript_path));
    // Inject the budget-capped digest into the model's context.
    try {
        let digest = buildSessionDigest(wolfDir, digestBudget(wolfDir), sessionId);
        // (The anatomy-staleness banner is gone in 2.2: it led 21 of 28 measured
        // digests because a 6h rescan interval loses to normal commit cadence —
        // a warning firing 75% of the time is a tax, not a signal. Freshness is
        // the daemon's and scan's job, not the model's.)
        // Hook breakage is the one warning worth its tokens: without it, a broken
        // install silently loses every feature (happened for 3 weeks once).
        if (brokenHooks.length > 0) {
            // LOCAL PATCH (shard cutover): `openwolf update` is RETIRED here — it
            // overwrites the committed, locally-patched hook stack. Recovery is git.
            digest = `OpenWolf hooks are degraded: ${brokenHooks.join("; ")}. Restore the committed stack with \`git checkout -- .wolf\` (see .wolf/SHARDS.md; never run openwolf update in this repo).\n\n` + digest;
        }
        // Post-compaction restore (F3 + 2.4): resurface what compaction erased.
        // The platform documents that paths-scoped rules and nested CLAUDE.md are
        // LOST at compaction until a matching file is read again — restore the
        // scoped rules for files this session already touched, plus the top
        // Do-Not-Repeat rules (decay countermeasure) and the in-flight state.
        if (continuing && source === "compact") {
            const session = readJSON(sessionFile, {});
            const restoreParts = [];
            const files = [...new Set((session.files_written ?? []).map((w) => w.file))];
            if (files.length > 0) {
                restoreParts.push(`## Session in progress (context was just compacted)\nFiles already modified this session: ${files.slice(-15).join(", ")}. Do not re-read them wholesale — check .wolf/session-memory/${sessionId}.md for what was done.`);
            }
            try {
                const rules = topRules(readCerebrumSection(wolfDir, "Do-Not-Repeat"), 3);
                if (rules.length > 0) {
                    restoreParts.push(`Project rules still in effect (from .wolf/cerebrum/ Do-Not-Repeat):\n${rules.join("\n")}`);
                }
            }
            catch { }
            try {
                const projectDir = getProjectDir();
                const relFiles = [
                    ...Object.keys(session.files_read ?? {}),
                    ...files,
                ].map((f) => path.isAbsolute(f) ? path.relative(projectDir, f).split(path.sep).join("/") : f);
                restoreParts.push(...scopedRulesForFiles(projectDir, relFiles));
            }
            catch { }
            if (restoreParts.length > 0) {
                digest = restoreParts.join("\n\n") + (digest ? "\n\n" + digest : "");
            }
        }
        if (startupNotes.length > 0) {
            digest = digest
                ? `${digest}\n\n${startupNotes.map((n) => `- ${n}`).join("\n")}`
                : startupNotes.map((n) => `- ${n}`).join("\n");
        }
        if (digest) {
            recordInjectionToSessionFile(sessionFile, "digest", digest, mutateJSON);
            process.stdout.write(JSON.stringify({
                hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: digest },
            }));
        }
    }
    catch { }
}
/**
 * Verify every installed hook script's relative imports resolve (2.2). The
 * 440-crash class was a missing DEPENDENCY file, not a missing hook, so a
 * bare existence check of registered hooks would have missed it. Cheap static
 * check: scan each installed hook script for relative import specifiers and
 * verify the target files exist. Returns problem strings (empty = healthy).
 */
function selfTestHooks(hooksDir) {
    const problems = [];
    try {
        const files = fs.readdirSync(hooksDir).filter((f) => f.endsWith(".js"));
        for (const f of files) {
            let src = "";
            try {
                src = fs.readFileSync(path.join(hooksDir, f), "utf-8");
            }
            catch {
                problems.push(`${f} unreadable`);
                continue;
            }
            for (const m of src.matchAll(/from\s+["']\.\/([\w.-]+\.js)["']/g)) {
                if (!fs.existsSync(path.join(hooksDir, m[1]))) {
                    problems.push(`${f} cannot load ${m[1]}`);
                }
            }
        }
    }
    catch { }
    return [...new Set(problems)].slice(0, 3);
}
hookMain("session-start", main);
//# sourceMappingURL=session-start.js.map
