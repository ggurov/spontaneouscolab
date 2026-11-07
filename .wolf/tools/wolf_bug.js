#!/usr/bin/env node
// wolf_bug.js — repo-owned (update-immune) bug-log shard tool. See .wolf/SHARDS.md.
//
// The bug log's source of truth is .wolf/buglog/*.jsonl — one append-only file per
// writer session, so cross-machine git merges are file additions, never same-tail
// conflicts. .wolf/buglog.json survives as a GITIGNORED derived view (object shape
// {version, bugs}) that the vendored stock hooks keep reading at their hardcoded path.
//
// Commands:
//   id                                   print {id, timestamp} collision-checked against all shards
//   append --file F --error E --root-cause C --fix X [--tags a,b] [--related ID,...]
//         [--occurrence-of ID --note "..."] [--session SESSION-ID | auto]
//   occurs  <BUG-ID> [--note "..."]      shorthand for append --occurrence-of
//   search <text>                        substring search over id/file/error/cause/fix/tags
//   stats                                per-shard counts, uniqueness assert, anomalies
//   rebuild [--if-stale] [--force]       absorb strays from the view, then rewrite the view
//
// Appends are single JSON lines (LF). NEVER `>>` this by hand from PowerShell (UTF-16).
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { readBugLogFile, writeJSON } from "../hooks/shared.js";
import { createBugIdentity } from "../hooks/bug-id.js";
import { withFileLock, CLI_LOCK_BUDGET_MS } from "../hooks/anatomy-lock.js";

const TOOLS_DIR = path.dirname(fileURLToPath(import.meta.url));
/** WOLF_TEST_DIR lets the test suite point every path at a temp tree. */
export const WOLF_DIR = process.env.WOLF_TEST_DIR
    ? path.resolve(process.env.WOLF_TEST_DIR)
    : path.resolve(TOOLS_DIR, "..");
export const SHARD_DIR = path.join(WOLF_DIR, "buglog");
export const VIEW_FILE = path.join(WOLF_DIR, "buglog.json");
const LEGACY_SHARD = "0000-legacy.jsonl";
/** Pre-cutover guard: with zero shards the tracked buglog.json is authoritative. */
export function shardsExist() {
    try {
        return fs.readdirSync(SHARD_DIR).some((f) => f.endsWith(".jsonl"));
    }
    catch {
        return false;
    }
}
export function listShardFiles() {
    try {
        return fs.readdirSync(SHARD_DIR).filter((f) => f.endsWith(".jsonl")).sort();
    }
    catch {
        return [];
    }
}
/** Parse one shard file into records; returns {records, anomalies}. Blank lines skipped. */
export function readShard(file) {
    const out = { records: [], anomalies: [] };
    let raw = "";
    try {
        raw = fs.readFileSync(path.join(SHARD_DIR, file), "utf8");
    }
    catch (e) {
        out.anomalies.push(`${file}: unreadable (${e.code ?? e.message})`);
        return out;
    }
    const lines = raw.split("\n");
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (!line.trim())
            continue;
        try {
            const rec = JSON.parse(line);
            if (!rec || typeof rec.id !== "string")
                throw new Error("missing id");
            out.records.push(rec);
        }
        catch (e) {
            out.anomalies.push(`${file}:${i + 1}: unparsable line (${e.message})`);
        }
    }
    return out;
}
export function readAllShards() {
    const out = { records: [], anomalies: [] };
    for (const f of listShardFiles()) {
        const r = readShard(f);
        out.records.push(...r.records);
        out.anomalies.push(...r.anomalies);
    }
    return out;
}
/** Every id visible anywhere (shards + view) — the collision universe. */
export function collectIds() {
    const ids = new Set(readAllShards().records.map((r) => r.id));
    for (const b of readBugLogFile(WOLF_DIR).bugs)
        ids.add(b.id);
    return ids;
}
/** Newest .wolf/hooks/sessions/<SESSION-ID>.json by mtime = the calling session
 *  (its pre-bash hook touched the file immediately before this process ran).
 *  Returns null when ambiguous (two files touched within 100 ms — parallel
 *  sessions) or when nothing looks like a session file. */
export function resolveCurrentSessionId() {
    const dir = path.join(WOLF_DIR, "hooks", "sessions");
    let files = [];
    try {
        files = fs.readdirSync(dir).filter((f) => f.endsWith(".json"));
    }
    catch {
        return null;
    }
    const statted = [];
    for (const f of files) {
        try {
            statted.push({ f, mtimeMs: fs.statSync(path.join(dir, f)).mtimeMs });
        }
        catch { }
    }
    if (statted.length === 0)
        return null;
    statted.sort((a, b) => b.mtimeMs - a.mtimeMs);
    const session = statted.find((s) => /^SESSION-[\w.-]{4,120}$/.test(s.f.replace(/\.json$/, "")));
    if (!session)
        return null;
    if (statted[0] !== session && statted[0].mtimeMs - session.mtimeMs > 100)
        return null; // newest runtime file is not a session file
    if (statted.length > 1 && statted[0].mtimeMs - statted[1].mtimeMs < 100)
        return null; // parallel-session tie — don't guess
    return session.f.replace(/\.json$/, "");
}
function utcDate() {
    return new Date().toISOString().slice(0, 10);
}
export function shardPathFor(sessionId) {
    const safe = sessionId.replace(/[^\w.-]/g, "_");
    return path.join(SHARD_DIR, `${utcDate()}-${safe}.jsonl`);
}
function manualShardPath() {
    const host = (process.env.HOSTNAME || os.hostname() || "unknown").replace(/[^\w.-]/g, "_");
    return path.join(SHARD_DIR, `${utcDate()}-${host}-manual.jsonl`);
}
export function appendRecord(record, shardFile) {
    if (!fs.existsSync(SHARD_DIR))
        fs.mkdirSync(SHARD_DIR, { recursive: true });
    const session = record.session_id ?? resolveCurrentSessionId();
    const target = shardFile ?? (session ? shardPathFor(session) : manualShardPath());
    if (!path.resolve(target).startsWith(path.resolve(SHARD_DIR)))
        throw new Error(`refusing to write outside shard dir: ${target}`);
    const line = JSON.stringify(record) + "\n";
    // Per-shard lock: belt-and-suspenders for the same-host parallel case. A
    // timeout SKIPS the append loudly (never silently — a lost bug entry must
    // be visible to the operator who ran the command).
    const result = withFileLock(target + ".lock", CLI_LOCK_BUDGET_MS, () => {
        fs.appendFileSync(target, line, "utf8");
        return true;
    });
    if (result !== true)
        throw new Error(`lock timeout on ${target}; append NOT written — retry`);
    return target;
}
/** Fold occurrence events into their base records (derived view semantics). */
export function foldOccurrences(records) {
    const events = records.filter((r) => r.occurrence_of);
    const bases = records.filter((r) => !r.occurrence_of);
    const byId = new Map(bases.map((b) => [b.id, b]));
    for (const ev of events) {
        const base = byId.get(ev.occurrence_of);
        if (!base)
            continue; // orphan event: kept in shards, skipped in the view
        base.occurrences = (base.occurrences ?? 1) + 1;
        if (!base.last_seen || ev.timestamp > base.last_seen)
            base.last_seen = ev.timestamp;
    }
    return bases;
}
/**
 * Absorb-then-rebuild. Absorb rescues anything that exists only in the derived
 * view (an agent following stale prose appended to .wolf/buglog.json directly,
 * or a hand-"fixed" record): unknown ids become new records in today's shard;
 * known ids whose view copy mutated (occurrences/last_seen/fix) become a linked
 * occurrence event. Then the view is rewritten atomically from the shards.
 */
export function rebuildView({ ifStale = false, force = false, quiet = false } = {}) {
    if (!shardsExist() && !force) {
        if (!quiet)
            console.log("no shards in .wolf/buglog/ — pre-cutover, .wolf/buglog.json stays authoritative (run wolf_migrate.js --buglog)");
        return { rebuilt: false };
    }
    const shardFiles = listShardFiles();
    let newestShard = 0;
    for (const f of shardFiles) {
        try {
            newestShard = Math.max(newestShard, fs.statSync(path.join(SHARD_DIR, f)).mtimeMs);
        }
        catch { }
    }
    let viewMtime = 0;
    try {
        viewMtime = fs.statSync(VIEW_FILE).mtimeMs;
    }
    catch { }
    if (ifStale && viewMtime > newestShard)
        return { rebuilt: false, fresh: true };
    const shards = readAllShards();
    const shardIds = new Set(shards.records.map((r) => r.id));
    const viewBugs = readBugLogFile(WOLF_DIR).bugs;
    const viewById = new Map(viewBugs.map((b) => [b.id, b]));
    const todaySession = resolveCurrentSessionId();
    const rescued = [];
    const base = { session_id: todaySession ?? undefined };
    for (const id of shardIds) {
        const v = viewById.get(id);
        if (!v)
            continue;
        const s = shards.records.find((r) => r.id === id && !r.occurrence_of);
        if (s && (JSON.stringify(pickMutation(v)) !== JSON.stringify(pickMutation(s)))) {
            const { id: newId, timestamp } = createBugIdentity(collectIds());
            appendRecord({ id: newId, timestamp, ...base, occurrence_of: id, note: `absorbed view mutation: occurrences=${v.occurrences} last_seen=${v.last_seen}` });
            rescued.push(`${id} (mutation → ${newId})`);
        }
    }
    for (const v of viewBugs) {
        if (shardIds.has(v.id))
            continue;
        appendRecord({ ...v, ...base, session_id: v.session_id ?? base.session_id });
        rescued.push(`${v.id} (stray record)`);
    }
    if (rescued.length > 0 && !quiet)
        console.log(`absorbed ${rescued.length} view-only entr${rescued.length === 1 ? "y" : "ies"}:\n  ${rescued.join("\n  ")}`);
    // Re-read after absorbs so the view includes them.
    const finalRecords = readAllShards().records;
    const view = { version: 1, bugs: foldOccurrences(finalRecords.map((r) => ({ ...r }))) };
    writeJSON(VIEW_FILE, view);
    return { rebuilt: true, bugs: view.bugs.length, shards: shardFiles.length };
}
function pickMutation(rec) {
    return { occurrences: rec.occurrences, last_seen: rec.last_seen, fix: rec.fix };
}
// ─── CLI ──────────────────────────────────────────────────────────────────────
function parseArgs(argv) {
    const out = { _: [] };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a.startsWith("--")) {
            out[a.slice(2)] = argv[i + 1];
            i++;
        }
        else
            out._.push(a);
    }
    return out;
}
function fail(msg) {
    console.error(`wolf_bug: ${msg}`);
    process.exit(1);
}
function cmdAppend(args) {
    if (args["occurrence-of"]) {
        const target = args["occurrence-of"];
        const { id, timestamp } = createBugIdentity(collectIds());
        const session = args.session ?? resolveCurrentSessionId();
        const rec = { id, timestamp, occurrence_of: target, note: args.note ?? "" };
        if (session)
            rec.session_id = session;
        const shard = appendRecord(rec);
        rebuildView({ quiet: true });
        console.log(`${id}: occurrence of ${target} → ${path.relative(WOLF_DIR, shard)}`);
        return;
    }
    for (const req of ["file", "error", "root-cause", "fix"]) {
        if (!args[req])
            fail(`--${req} is required`);
    }
    const { id, timestamp } = createBugIdentity(collectIds());
    const session = args.session ?? resolveCurrentSessionId();
    const rec = {
        id,
        timestamp,
        error_message: args.error,
        file: args.file,
        root_cause: args["root-cause"],
        fix: args.fix,
        tags: args.tags ? String(args.tags).split(",").map((t) => t.trim()).filter(Boolean) : [],
        related_bugs: args.related ? String(args.related).split(",").map((t) => t.trim()).filter(Boolean) : [],
        occurrences: 1,
        last_seen: timestamp,
    };
    if (session)
        rec.session_id = session;
    const shard = appendRecord(rec);
    const view = rebuildView({ quiet: true });
    console.log(`${id} → ${path.relative(WOLF_DIR, shard)} (view: ${view.bugs} bugs)`);
}
function cmdSearch(term) {
    if (!term)
        fail("usage: wolf_bug.js search <text>");
    const needle = term.toLowerCase();
    const files = listShardFiles();
    let hits = 0;
    for (const f of files) {
        const raw = fs.readFileSync(path.join(SHARD_DIR, f), "utf8");
        raw.split("\n").forEach((line, i) => {
            if (!line.trim() || !line.toLowerCase().includes(needle))
                return;
            let rec;
            try {
                rec = JSON.parse(line);
            }
            catch {
                return;
            }
            hits++;
            if (rec.occurrence_of) {
                console.log(`${f}:${i + 1}  [occurrence] of ${rec.occurrence_of} at ${rec.timestamp}${rec.note ? " — " + rec.note : ""}`);
                return;
            }
            console.log(`${f}:${i + 1}  ${rec.id}  ${rec.file}\n  ${String(rec.error_message).slice(0, 160)}\n  cause: ${String(rec.root_cause).slice(0, 140)}\n  fix:   ${String(rec.fix).slice(0, 140)}`);
        });
    }
    // Search also the base id of any occurrence event lines pointing at a match.
    if (hits === 0)
        console.log(`no matches for "${term}" in ${files.length} shards`);
}
function cmdStats() {
    const files = listShardFiles();
    if (files.length === 0)
        fail("no shards in .wolf/buglog/ (pre-cutover)");
    const shards = readAllShards();
    const ids = shards.records.filter((r) => !r.occurrence_of).map((r) => r.id);
    const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
    for (const f of files) {
        const r = readShard(f);
        console.log(`${f}: ${r.records.length} records${r.anomalies.length ? "  ⚠ " + r.anomalies.join("; ") : ""}`);
    }
    const events = shards.records.filter((r) => r.occurrence_of).length;
    console.log(`total: ${ids.length} bugs + ${events} occurrence events across ${files.length} shards; ${dupes.length ? `⚠ DUPLICATE IDS: ${[...new Set(dupes)].join(", ")}` : "ids unique"}`);
    if (shards.anomalies.length) {
        console.log("anomalies:\n  " + shards.anomalies.join("\n  "));
        process.exitCode = 1;
    }
    if (dupes.length)
        process.exitCode = 1;
}
function main() {
    const [cmd, ...rest] = process.argv.slice(2);
    const args = parseArgs(rest);
    switch (cmd) {
        case "id":
            console.log(JSON.stringify(createBugIdentity(collectIds())));
            break;
        case "append":
            cmdAppend(args);
            break;
        case "occurs": {
            const target = args._[0];
            if (!target)
                fail("usage: wolf_bug.js occurs <BUG-ID> [--note ...]");
            cmdAppend({ ...args, "occurrence-of": target });
            break;
        }
        case "search":
            cmdSearch(args._.join(" "));
            break;
        case "stats":
            cmdStats();
            break;
        case "rebuild":
            console.log(JSON.stringify(rebuildView({ ifStale: args["if-stale"] !== undefined, force: args.force !== undefined })));
            break;
        default:
            fail(`unknown command "${cmd ?? ""}" — see the header of this file`);
    }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    main();
}
