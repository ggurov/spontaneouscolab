#!/usr/bin/env node
// bootstrap.js — SessionStart entry (registered BEFORE the stock session-start
// hook in .claude/settings.json). Repo-owned and update-immune; see .wolf/SHARDS.md.
//
// 1. Rebuild the derived .wolf/buglog.json view when shards are newer (bounds
//    cross-machine staleness to one session start — a pull delivers shards, this
//    makes them visible to the stock hooks' readers), absorbing any view-only
//    strays first.
// 2. Seed the machine-local anatomy store from committed .wolf/anatomy-seed.json
//    on a fresh clone (no scanner exists in-repo; without this a fresh clone has
//    no anatomy at all).
//
// Prints ONE status line (SessionStart stdout becomes model context). Exits
// non-zero with a loud stderr message on real failure — this tool must NOT
// inherit the stock hooks' exit-0-on-error culture.
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { rebuildView, SHARD_DIR, WOLF_DIR } from "./wolf_bug.js";
import { newStore, renderToFile, saveStore } from "../hooks/anatomy-store.js";

const SEED_FILE = path.join(WOLF_DIR, "anatomy-seed.json");
const STORE_FILE = path.join(WOLF_DIR, "anatomy-index.json");

function seedAnatomyIfAbsent() {
    if (fs.existsSync(STORE_FILE) || !fs.existsSync(SEED_FILE))
        return 0;
    const seed = JSON.parse(fs.readFileSync(SEED_FILE, "utf8"));
    const store = newStore();
    Object.assign(store.files, seed.files ?? {});
    store.meta.lastScanned = `seeded ${new Date().toISOString()}`;
    saveStore(WOLF_DIR, store);
    renderToFile(WOLF_DIR, store);
    return Object.keys(store.files).length;
}

const parts = [];
try {
    const r = rebuildView({ ifStale: true, quiet: true });
    if (r.rebuilt)
        parts.push(`buglog view rebuilt: ${r.bugs} bugs / ${r.shards} shards`);
    else {
        const shards = fs.existsSync(SHARD_DIR)
            ? fs.readdirSync(SHARD_DIR).filter((f) => f.endsWith(".jsonl")).length
            : 0;
        if (shards > 0)
            parts.push(`buglog: ${shards} shard files, view fresh`);
    }
}
catch (e) {
    console.error(`wolf bootstrap: buglog view rebuild FAILED: ${e.message}`);
    process.exit(1);
}
try {
    const seeded = seedAnatomyIfAbsent();
    if (seeded > 0)
        parts.push(`anatomy seeded: ${seeded} entries`);
}
catch (e) {
    console.error(`wolf bootstrap: anatomy seed FAILED: ${e.message}`);
    process.exit(1);
}
if (parts.length > 0)
    console.log(`wolf: ${parts.join("; ")}`);
