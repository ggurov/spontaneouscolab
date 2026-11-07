#!/usr/bin/env node
// wolf_migrate.js — one-time shard cutover + PERMANENT straggler absorber.
// Idempotent: each step is skipped when its target already exists (--force to redo).
// See .wolf/SHARDS.md. The cerebrum/status outputs are MECHANICAL (verbatim
// moves, merged duplicate sections marked, near-duplicates reported, never
// auto-deduped) — a human reviews the cutover diff before pushing.
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { readBugLogFile, writeJSON } from "../hooks/shared.js";
import { WOLF_DIR, SHARD_DIR, listShardFiles, readShard, appendRecord, resolveCurrentSessionId } from "./wolf_bug.js";

const PROJECT_ROOT = path.resolve(WOLF_DIR, "..");
const CEREBRUM_DIR = path.join(WOLF_DIR, "cerebrum");
const STATUS_DIR = path.join(WOLF_DIR, "status", "done");
const VIEW_FILE = path.join(WOLF_DIR, "buglog.json");
const SECTIONS = [
    { title: "User Preferences", file: "user-preferences.md" },
    { title: "Key Learnings", file: "key-learnings.md" },
    { title: "Do-Not-Repeat", file: "do-not-repeat.md" },
    { title: "Decision Log", file: "decision-log.md" },
];

function note(msg) { console.log(msg); }
function die(msg) { console.error(`wolf_migrate: ${msg}`); process.exit(1); }

// ─── --buglog: split .wolf/buglog.json into shards ────────────────────────────
function hostOf(id) {
    // BUG-<host>-<crc32 8 hex> — hostnames may contain dashes, so anchor on the
    // trailing 8-hex crc, not on dash-splitting.
    const m = id.match(/^BUG-(.+)-([0-9a-f]{8})$/);
    return m ? m[1] : null;
}
function migrateBuglog({ dryRun }) {
    if (fs.existsSync(SHARD_DIR) && listShardFiles().length > 0)
        return note("buglog: shards already exist — skipping (use --force after deleting .wolf/buglog/ to redo)");
    const { bugs } = readBugLogFile(WOLF_DIR);
    if (bugs.length === 0)
        die("buglog: .wolf/buglog.json has no entries");
    const buckets = new Map(); // filename -> records
    const bucket = (name) => { if (!buckets.has(name)) buckets.set(name, []); return buckets.get(name); };
    for (const b of bugs) {
        if (!b.id)
            die(`buglog: entry without id: ${JSON.stringify(b).slice(0, 120)}`);
        if (b.id.startsWith("BUG-legacy-")) {
            bucket("0000-legacy.jsonl").push(b);
            continue;
        }
        const date = String(b.timestamp ?? "").slice(0, 10) || "1970-01-01";
        if (b.session_id && /^SESSION-[\w.-]{4,120}$/.test(b.session_id)) {
            bucket(`${date}-${b.session_id}.jsonl`).push(b);
        }
        else {
            const host = (hostOf(b.id) ?? "unknown").replace(/[^\w.-]/g, "_");
            bucket(`${date}-${host}-manual.jsonl`).push(b);
        }
    }
    const total = [...buckets.values()].reduce((n, v) => n + v.length, 0);
    const ids = bugs.map((b) => b.id);
    const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
    note(`buglog: ${bugs.length} entries → ${buckets.size} shards (${total} out); legacy bucket: ${(buckets.get("0000-legacy.jsonl") ?? []).length}`);
    if (total !== bugs.length)
        die(`buglog: count mismatch ${total} != ${bugs.length}`);
    if (dupes.length)
        die(`buglog: DUPLICATE IDS in source (resolve first): ${[...new Set(dupes)].join(", ")}`);
    if (dryRun) {
        for (const [name, recs] of [...buckets.entries()].sort())
            note(`  would write ${name} (${recs.length} records)`);
        return;
    }
    fs.mkdirSync(SHARD_DIR, { recursive: true });
    for (const [name, recs] of buckets) {
        fs.writeFileSync(path.join(SHARD_DIR, name), recs.map((r) => JSON.stringify(r)).join("\n") + "\n", "utf8");
        note(`  wrote ${name} (${recs.length} records)`);
    }
    note("buglog: shards are now the source of truth. Next: `git rm --cached .wolf/buglog.json` (keeps the local file as the first derived view), then add .wolf/buglog/.");
}

// ─── --cerebrum: split into section files ─────────────────────────────────────
function migrateCerebrum({ dryRun }) {
    const src = path.join(WOLF_DIR, "cerebrum.md");
    if (!fs.existsSync(src))
        return note("cerebrum: no cerebrum.md — skipping");
    if (fs.existsSync(CEREBRUM_DIR) && fs.readdirSync(CEREBRUM_DIR).length > 0)
        return note("cerebrum: section dir already populated — skipping");
    const lines = fs.readFileSync(src, "utf8").split("\n");
    // Split on EXACT known section titles only. The pseudo-header at old L17
    // ("## Do-Not-Repeat` and `## Key Learnings" — a wrap fragment of the header
    // quote) must not split anything; it is dropped from the preamble below.
    const isTitle = (l) => SECTIONS.some((s) => l === `## ${s.title}` || l === `## ${s.title}\r`);
    const chunks = { preamble: [] };
    for (const s of SECTIONS)
        chunks[s.title] = [];
    let target = "preamble";
    for (const l of lines) {
        if (isTitle(l.trimEnd())) {
            const title = l.trimEnd().slice(3);
            target = chunks[title].length === 0 && !chunks[`${title}#2`] ? title : `${title}#2`;
            if (target.endsWith("#2"))
                (chunks[`${title}#2`] ??= []).push(`<!-- merged duplicate "## ${title}" section during the 2026-09-18 shard cutover; near-duplicates reported by wolf_migrate --cerebrum; review before push -->`);
            continue;
        }
        chunks[target].push(l);
    }
    const rules = chunks.preamble.filter((l) => l.trimEnd() !== "## Do-Not-Repeat` and `## Key Learnings");
    const nearDupes = [];
    for (const s of SECTIONS) {
        if (!chunks[`${s.title}#2`])
            continue;
        const norm = (l) => l.toLowerCase().replace(/\d{4}-\d{2}-\d{2}/g, "").replace(/[^a-z0-9 ]/g, " ").split(/\s+/).filter((w) => w.length > 3);
        const first = chunks[s.title].filter((l) => l.trimStart().startsWith("- "));
        const second = chunks[`${s.title}#2`].filter((l) => l.trimStart().startsWith("- "));
        for (const a of first) {
            for (const b of second) {
                const wa = new Set(norm(a)), wb = new Set(norm(b));
                const inter = [...wa].filter((w) => wb.has(w)).length;
                const union = new Set([...wa, ...wb]).size;
                if (union > 0 && inter / union > 0.55)
                    nearDupes.push(`${s.title}:\n  A: ${a.trim().slice(0, 140)}\n  B: ${b.trim().slice(0, 140)}`);
            }
        }
    }
    if (dryRun) {
        note(`cerebrum: would write ${SECTIONS.map((s) => s.file).join(", ")} + project-rules.md (preamble ${rules.length} lines)`);
    }
    else {
        fs.mkdirSync(CEREBRUM_DIR, { recursive: true });
        const write = (file, header, body) => fs.writeFileSync(path.join(CEREBRUM_DIR, file), `## ${header}\n${body.join("\n").replace(/^\n+/, "").trimEnd()}\n`, "utf8");
        write("project-rules.md", "Project Rules (preamble + undated kernels — grep, do not read whole)", rules);
        for (const s of SECTIONS)
            write(s.file, s.title, [...chunks[s.title], ...(chunks[`${s.title}#2`] ? ["", ...chunks[`${s.title}#2`]] : [])]);
        note(`cerebrum: wrote ${SECTIONS.length + 1} section files (duplicate sections merged with markers)`);
    }
    if (nearDupes.length) {
        note(`cerebrum: ${nearDupes.length} NEAR-DUPLICATE rule pair(s) between merged duplicate sections — HUMAN REVIEW before push:`);
        for (const d of nearDupes)
            note("  " + d);
    }
    else
        note("cerebrum: no near-duplicate pairs detected between merged sections");
    if (!dryRun)
        note("cerebrum: after review: `git rm --cached .wolf/cerebrum.md` and delete the local copy (or keep until hook patches verified).");
}

// ─── --status: extract Done blocks ─────────────────────────────────────────────
function slugify(s) {
    return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 70).replace(/-+$/g, "") || "entry";
}
function migrateStatus({ dryRun }) {
    const src = path.join(WOLF_DIR, "STATUS.md");
    if (!fs.existsSync(src))
        return note("status: no STATUS.md — skipping");
    if (fs.existsSync(STATUS_DIR) && fs.readdirSync(STATUS_DIR).length > 0)
        return note("status: done dir already populated — skipping");
    const lines = fs.readFileSync(src, "utf8").split("\n");
    const doneIdx = lines.findIndex((l) => l.trimEnd() === "## ✅ Done");
    if (doneIdx === -1)
        die("status: no `## ✅ Done` header found");
    const nextH2 = lines.findIndex((l, i) => i > doneIdx && /^## /.test(l));
    const doneBlock = lines.slice(doneIdx + 1, nextH2 === -1 ? lines.length : nextH2);
    const rest = nextH2 === -1 ? [] : lines.slice(nextH2);
    const head = lines.slice(0, doneIdx + 1);
    // Group the Done section into ### blocks; leading non-block lines stay inline.
    const blocks = [];
    let inline = [];
    for (const l of doneBlock) {
        if (l.startsWith("### ")) {
            blocks.push({ header: l, body: [] });
        }
        else if (blocks.length > 0) {
            blocks[blocks.length - 1].body.push(l);
        }
        else {
            inline.push(l);
        }
    }
    note(`status: ${blocks.length} done blocks → .wolf/status/done/`);
    if (dryRun) {
        for (const b of blocks)
            note(`  ${b.header.slice(4, 100)}`);
        return;
    }
    fs.mkdirSync(STATUS_DIR, { recursive: true });
    const used = new Set();
    for (const b of blocks) {
        const date = (b.header.match(/\d{4}-\d{2}-\d{2}/) ?? ["undated"])[0];
        const title = b.header.replace(/^###\s*/, "").replace(/^\d{4}-\d{2}-\d{2}[^\s]*\s*[—-]\s*/, "");
        let name = `${date}-${slugify(title)}.md`;
        let n = 2;
        while (used.has(name))
            name = `${date}-${slugify(title)}-${n++}.md`;
        used.add(name);
        fs.writeFileSync(path.join(STATUS_DIR, name), `${b.header.trimEnd()}\n${b.body.join("\n").trimEnd()}\n`, "utf8");
    }
    const lean = [
        ...head,
        ...inline,
        ``,
        `(${blocks.length} done blocks archived to \`.wolf/status/done/\` — one file per milestone, newest by filename sort. This file keeps the live view only.)`,
        ``,
        ...rest,
    ].join("\n").replace(/\n{3,}/g, "\n\n") + "\n";
    fs.writeFileSync(src, lean, "utf8");
    note("status: lean STATUS.md written (## 🚀 Next quest + ## Context preserved verbatim)");
}

// ─── --anatomy: export the committed seed ──────────────────────────────────────
function migrateAnatomy({ dryRun }) {
    const index = path.join(WOLF_DIR, "anatomy-index.json");
    if (!fs.existsSync(index))
        die("anatomy: no anatomy-index.json");
    const store = JSON.parse(fs.readFileSync(index, "utf8"));
    const hotRows = new Set();
    const doc = path.join(PROJECT_ROOT, "CLAUDE", "hot_files.md");
    if (fs.existsSync(doc)) {
        for (const m of fs.readFileSync(doc, "utf8").matchAll(/^\|\s*`([^`]+)`/gm))
            hotRows.add(m[1]);
    }
    const files = {};
    let mdImport = 0, hotOnly = 0;
    for (const [p, entry] of Object.entries(store.files ?? {})) {
        const isMd = entry?.source === "md-import";
        const isHot = hotRows.has(p);
        if (isMd || isHot) {
            files[p] = entry;
            if (isMd && isHot)
                mdImport++;
            else if (isMd)
                mdImport++;
            else
                hotOnly++;
        }
    }
    note(`anatomy: seed = ${Object.keys(files).length} entries (${mdImport} md-import, ${hotOnly} hot-files-only)`);
    if (dryRun)
        return;
    writeJSON(path.join(WOLF_DIR, "anatomy-seed.json"), { version: 1, exported_at: new Date().toISOString(), files });
    note("anatomy: wrote .wolf/anatomy-seed.json (committed; refresh with wolf_migrate.js --anatomy after deliberate curation)");
    note("anatomy: next: `git rm --cached .wolf/anatomy.md .wolf/anatomy-index.json` — local copies stay as machine-local derived state.");
}

// ─── --absorb-stray: straggler recovery (permanent) ────────────────────────────
function absorbStray(file) {
    if (!file || !fs.existsSync(file))
        die(`absorb-stray: no such file: ${file}`);
    const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
    const stray = Array.isArray(parsed) ? parsed : (parsed.bugs ?? []);
    const known = new Set();
    for (const f of listShardFiles())
        for (const r of readShard(f).records)
            known.add(r.id);
    const session = resolveCurrentSessionId();
    let rescued = 0;
    for (const b of stray) {
        if (!b?.id || known.has(b.id))
            continue;
        appendRecord({ ...b, session_id: b.session_id ?? session });
        known.add(b.id);
        rescued++;
        note(`  rescued ${b.id}`);
    }
    note(`absorb-stray: ${rescued} of ${stray.length} entries imported (rest already known)`);
}

// ─── CLI ──────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const dryRun = argv.includes("--dry-run");
const force = argv.includes("--force");
const strayIdx = argv.indexOf("--absorb-stray");
if (strayIdx !== -1) {
    absorbStray(argv[strayIdx + 1]);
}
else if (argv.includes("--buglog"))
    migrateBuglog({ dryRun });
else if (argv.includes("--cerebrum"))
    migrateCerebrum({ dryRun });
else if (argv.includes("--status"))
    migrateStatus({ dryRun });
else if (argv.includes("--anatomy"))
    migrateAnatomy({ dryRun });
else if (argv.includes("--all")) {
    migrateBuglog({ dryRun });
    migrateCerebrum({ dryRun });
    migrateStatus({ dryRun });
    migrateAnatomy({ dryRun });
}
else {
    console.log(`usage: node .wolf/tools/wolf_migrate.js (--all | --buglog | --cerebrum | --status | --anatomy | --absorb-stray <file>) [--dry-run] [--force]`);
    console.log(`  --dry-run prints the plan (counts, file lists, near-duplicate report) without writing.`);
    process.exit(argv.length ? 1 : 0);
}
