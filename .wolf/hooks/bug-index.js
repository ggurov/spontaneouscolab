import * as fs from "node:fs";
import * as path from "node:path";
import { createRequire } from "node:module";
let sqliteModule = null;
let sqliteChecked = false;
function getSqlite() {
    if (sqliteChecked)
        return sqliteModule;
    sqliteChecked = true;
    try {
        const require = createRequire(import.meta.url);
        sqliteModule = require("node:sqlite");
    }
    catch {
        sqliteModule = null;
    }
    return sqliteModule;
}
export function sqliteAvailable() {
    return getSqlite() !== null;
}
/**
 * Normalize an error message into a matchable signature: lowercase, digits
 * collapsed to N, hex ids and filesystem paths stripped, punctuation spaced.
 */
export function normalizeSignature(text) {
    return text
        .toLowerCase()
        .replace(/\b0x[0-9a-f]+\b/g, " ")
        .replace(/\b[0-9a-f]{8,}\b/g, " ")
        .replace(/[a-z0-9_.~-]*[/\\][a-z0-9_.~/\\-]+/gi, " ")
        .replace(/\d+/g, "n")
        .replace(/[^\w\s]/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}
function dbPath(wolfDir) {
    return path.join(wolfDir, "cache", "buglog.db");
}
/**
 * Open (and if stale, rebuild) the FTS index for a project's buglog.
 * Returns null when node:sqlite is unavailable or anything fails.
 */
function openIndex(wolfDir) {
    const sqlite = getSqlite();
    if (!sqlite)
        return null;
    const bugLogPath = path.join(wolfDir, "buglog.json");
    let bugMtime = 0;
    let bugs = [];
    try {
        bugMtime = fs.statSync(bugLogPath).mtimeMs;
        const parsed = JSON.parse(fs.readFileSync(bugLogPath, "utf-8"));
        bugs = Array.isArray(parsed?.bugs) ? parsed.bugs : [];
    }
    catch {
        return null;
    }
    if (bugs.length === 0)
        return null;
    try {
        fs.mkdirSync(path.dirname(dbPath(wolfDir)), { recursive: true });
        const db = new sqlite.DatabaseSync(dbPath(wolfDir));
        db.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)");
        const row = db.prepare("SELECT value FROM meta WHERE key = 'buglog_mtime'").get();
        const indexedMtime = row?.value ? Number(row.value) : -1;
        if (indexedMtime !== bugMtime) {
            db.exec("DROP TABLE IF EXISTS bugs_fts");
            db.exec("CREATE VIRTUAL TABLE bugs_fts USING fts5(id UNINDEXED, error_message, root_cause, fix, tags, signature)");
            const insert = db.prepare("INSERT INTO bugs_fts (id, error_message, root_cause, fix, tags, signature) VALUES (?, ?, ?, ?, ?, ?)");
            for (const b of bugs) {
                insert.run(String(b.id ?? ""), String(b.error_message ?? ""), String(b.root_cause ?? ""), String(b.fix ?? ""), Array.isArray(b.tags) ? b.tags.join(" ") : "", normalizeSignature(`${b.error_message ?? ""} ${b.root_cause ?? ""}`));
            }
            db.prepare("INSERT INTO meta (key, value) VALUES ('buglog_mtime', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(String(bugMtime));
        }
        return db;
    }
    catch {
        // Corrupt/locked db: it is disposable — drop it and let the next call rebuild.
        try {
            fs.unlinkSync(dbPath(wolfDir));
        }
        catch { }
        return null;
    }
}
/** Build a safe FTS5 OR-query from free text (quoted tokens only). */
function ftsQuery(text) {
    const tokens = normalizeSignature(text).split(/\s+/).filter((t) => t.length > 2 && t !== "n");
    return [...new Set(tokens)].slice(0, 12).map((t) => `"${t}"`).join(" OR ");
}
/**
 * Full-text search over the buglog, ranked by FTS5 relevance. Returns the
 * matching bug entries from buglog.json (source of truth), or null when the
 * index is unavailable (caller falls back to the Jaccard matcher).
 */
export function searchBugsFTS(wolfDir, query, limit = 5) {
    const db = openIndex(wolfDir);
    if (!db)
        return null;
    try {
        const match = ftsQuery(query);
        if (!match)
            return null;
        const rows = db
            .prepare("SELECT id FROM bugs_fts WHERE bugs_fts MATCH ? ORDER BY rank LIMIT ?")
            .all(match, limit);
        if (rows.length === 0)
            return [];
        const parsed = JSON.parse(fs.readFileSync(path.join(wolfDir, "buglog.json"), "utf-8"));
        const byId = new Map((parsed.bugs ?? []).map((b) => [String(b.id), b]));
        return rows.map((r) => byId.get(r.id)).filter((b) => b !== undefined);
    }
    catch {
        return null;
    }
    finally {
        try {
            db.close();
        }
        catch { }
    }
}
//# sourceMappingURL=bug-index.js.map