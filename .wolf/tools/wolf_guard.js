#!/usr/bin/env node
// wolf_guard.js — lint/doctor for the sharded wolf state. Repo-owned, update-immune.
// Exits 1 with findings; 0 clean. Findings come with copy-pasteable remedies —
// this tool must never inherit the stock hooks' exit-0-on-error culture.
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { WOLF_DIR, SHARD_DIR, VIEW_FILE, listShardFiles, readAllShards } from "./wolf_bug.js";

const CEREBRUM_DIR = path.join(WOLF_DIR, "cerebrum");
const KNOWN_SECTIONS = new Map([
    ["user-preferences", "User Preferences"],
    ["key-learnings", "Key Learnings"],
    ["do-not-repeat", "Do-Not-Repeat"],
    ["decision-log", "Decision Log"],
    ["project-rules", "Project Rules"],
]);
const SHARD_NAME = /^(?:\d{4}-\d{2}-\d{2}-(?:SESSION-[\w.-]+|[\w.-]+-manual)|0000-legacy)\.jsonl$/;

const findings = [];
const bad = (msg, remedy) => findings.push(remedy ? `${msg}\n  fix: ${remedy}` : msg);
const ok = (msg) => console.log(`ok: ${msg}`);

// 1. Shard filename grammar + JSONL hygiene + id uniqueness.
const shardFiles = listShardFiles();
if (shardFiles.length === 0 && fs.existsSync(SHARD_DIR)) {
    bad(".wolf/buglog/ exists but has no *.jsonl shards", "run node .wolf/tools/wolf_migrate.js --buglog");
}
for (const f of fs.existsSync(SHARD_DIR) ? fs.readdirSync(SHARD_DIR) : []) {
    if (f.endsWith(".jsonl") && !SHARD_NAME.test(f))
        bad(`shard name violates grammar: ${f}`, "expected <YYYY-MM-DD>-SESSION-<host>-<uuid>.jsonl | <date>-<host>-manual.jsonl | 0000-legacy.jsonl");
}
if (shardFiles.length > 0) {
    const { records, anomalies } = readAllShards();
    for (const a of anomalies)
        bad(`unparsable shard line: ${a}`);
    const ids = records.filter((r) => !r.occurrence_of).map((r) => r.id);
    const dupes = [...new Set(ids.filter((id, i) => ids.indexOf(id) !== i))];
    if (dupes.length)
        bad(`DUPLICATE bug ids across shards: ${dupes.join(", ")}`, "keep both entries but re-id one with wolf_bug.js id + a manual shard edit on YOUR OWN shard only");
    else
        ok(`${ids.length} bugs + ${records.length - ids.length} occurrence events in ${shardFiles.length} shards, ids unique`);
    for (const f of shardFiles) {
        const raw = fs.readFileSync(path.join(SHARD_DIR, f), "utf8");
        if (raw.length > 0 && !raw.endsWith("\n"))
            bad(`shard ${f} does not end with a newline (torn append)`);
        if (raw.includes("\r"))
            bad(`shard ${f} contains CR (CRLF contamination)`, "rewrite the line via wolf_bug.js; never append with PowerShell >>");
    }
    // 2. Derived view freshness: view-only ids = stale-prose strays awaiting absorb.
    try {
        const view = JSON.parse(fs.readFileSync(VIEW_FILE, "utf8"));
        const viewBugs = Array.isArray(view) ? view : (view.bugs ?? []);
        const shardIds = new Set(records.map((r) => r.id));
        const viewOnly = viewBugs.filter((b) => b.id && !shardIds.has(b.id)).map((b) => b.id);
        if (viewOnly.length)
            bad(`derived .wolf/buglog.json holds ${viewOnly.length} ids no shard knows (${viewOnly.slice(0, 5).join(", ")}…)`, "node .wolf/tools/wolf_bug.js rebuild   (absorbs them into shards first)");
    }
    catch (e) {
        if (e.code !== "ENOENT")
            bad(`derived .wolf/buglog.json unparsable: ${e.message}`, "node .wolf/tools/wolf_bug.js rebuild --force");
    }
}
// 3. Cerebrum sections: one file per title, exactly one header, no cross-file dupes.
if (fs.existsSync(CEREBRUM_DIR)) {
    const legacy = path.join(WOLF_DIR, "cerebrum.md");
    if (fs.existsSync(legacy))
        bad(".wolf/cerebrum.md still exists alongside .wolf/cerebrum/ (post-cutover straggler)", "git rm --cached .wolf/cerebrum.md and delete the local copy");
    for (const f of fs.readdirSync(CEREBRUM_DIR).filter((f) => f.endsWith(".md"))) {
        const raw = fs.readFileSync(path.join(CEREBRUM_DIR, f), "utf8");
        const headers = raw.split("\n").filter((l) => /^## /.test(l));
        const slug = f.replace(/\.md$/, "");
        if (headers.length === 0)
            bad(`cerebrum/${f} has no "## " header`, `first line must be "## ${KNOWN_SECTIONS.get(slug) ?? "…"}"`);
        else if (headers.length > 1)
            bad(`cerebrum/${f} has ${headers.length} "## " headers (duplicate-section failure class)`, "merge them by hand, keeping one header");
        else if (KNOWN_SECTIONS.has(slug) && !headers[0].startsWith(`## ${KNOWN_SECTIONS.get(slug)}`))
            bad(`cerebrum/${f} header is "${headers[0].slice(3)}" but the file name promises "${KNOWN_SECTIONS.get(slug)}"`);
    }
    ok(`cerebrum: ${fs.readdirSync(CEREBRUM_DIR).length} section files, headers checked`);
}
// 4. Committed seed present (only a warning — a fresh checkout before cutover has none).
if (!fs.existsSync(path.join(WOLF_DIR, "anatomy-seed.json")) && fs.existsSync(SHARD_DIR))
    console.log("note: .wolf/anatomy-seed.json missing — fresh clones will start with hot-files-only anatomy (node .wolf/tools/wolf_migrate.js --anatomy)");

if (findings.length > 0) {
    console.error(`wolf_guard: ${findings.length} finding(s):\n`);
    for (const f of findings)
        console.error(`- ${f}\n`);
    process.exit(1);
}
console.log("wolf_guard: clean");
