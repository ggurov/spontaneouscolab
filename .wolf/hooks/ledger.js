import * as path from "node:path";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";
import { readJSON, writeJSON, readTranscriptUsage, detectAgent, getWolfDir } from "./shared.js";
import { withFileLock, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";
import { sessionArtifactPath } from "./session-id.js";
import { emptyLedger, buildSessionTotals, numericFields, emptyMaps, recomputeLifetime } from "./ledger-math.js";
// ─────────────────────────────────────────────────────────────────────────────
// Token-ledger writer shared by the Stop and SessionEnd hooks.
//
// Stop fires at the end of EVERY turn, so the ledger write must be idempotent:
// each session owns a separate file and replaces its cumulative entry. Reports
// fold the frozen historical snapshot and latest entry per generated ID.
//
// The pure math (folds, lifetime derivation, migrations) lives in
// ledger-math.ts; this module owns IO and session-entry assembly.
// ─────────────────────────────────────────────────────────────────────────────
export * from "./ledger-math.js";
/** Build the ledger entry for the current session state (idempotent). */
export function buildSessionEntry(session, transcriptPath) {
    const reads = Object.entries(session.files_read).map(([file, data]) => ({
        file,
        tokens_estimated: data.tokens,
        was_repeated: data.count > 1,
        anatomy_had_description: data.anatomy_hit === true,
    }));
    const writes = session.files_written.map((w) => ({
        file: w.file,
        tokens_estimated: w.tokens,
        action: w.action,
    }));
    const entry = {
        id: session.session_id,
        agent: session.agent || detectAgent(),
        hostname: session.hostname,
        harness_session_id: session.harness_session_id,
        agent_id: session.agent_id,
        started: session.started,
        ended: new Date().toISOString(),
        reads,
        writes,
        totals: buildSessionTotals(session, reads, writes),
    };
    if (session.injected_by_source && Object.keys(session.injected_by_source).length > 0) {
        entry.injected_by_source = { ...session.injected_by_source };
    }
    if (transcriptPath) {
        // readTranscriptUsage dedupes by message id, so this is the cumulative
        // total for the whole session — correct to REPLACE, never to add.
        const real = readTranscriptUsage(transcriptPath);
        if (real)
            entry.real_usage = real;
    }
    return entry;
}
export function getSessionLedgerPath(wolfDir, id) {
    return sessionArtifactPath(wolfDir, "session-ledgers", id, "json");
}

/** Each session updates only its own file; the committed legacy ledger is frozen. */
export function flushSessionToLedger(wolfDir, entry) {
    if (!entry.id)
        return;
    const ledgerPath = getSessionLedgerPath(wolfDir, entry.id);
    fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
    withFileLock(ledgerPath + ".lock", HOOK_LOCK_BUDGET_MS, () => {
        const ledger = readJSON(ledgerPath, emptyLedger());
        ledger.created_at ||= entry.started;
        // Keep committed telemetry compact; detailed paths remain in runtime state.
        const { reads, writes, ...storedEntry } = entry;
        ledger.sessions = [storedEntry];
        ledger.lifetime.total_sessions = 1;
        recomputeLifetime(ledger);
        writeJSON(ledgerPath, ledger);
    });
}

/** Derive a combined report without rewriting shared counters or inherited history. */
export function readCombinedLedger(wolfDir) {
    const history = readJSON(path.join(wolfDir, "token-ledger.json"), emptyLedger());
    const combined = {
        ...emptyLedger(),
        lifetime: { total_sessions: history.lifetime?.total_sessions || 0 },
        lifetime_baseline: numericFields(history.lifetime),
        lifetime_baseline_maps: history.lifetime_maps || history.lifetime_baseline_maps || emptyMaps(),
        historical_sessions: history.sessions || [],
    };
    const directory = path.join(wolfDir, "session-ledgers");
    let files = [];
    try { files = fs.readdirSync(directory).sort(); } catch (error) {
        if (error.code !== "ENOENT") throw error;
    }
    for (const file of files) {
        if (!/^[\w.-]+\.json$/.test(file)) continue;
        const ledger = readJSON(path.join(directory, file), null);
        const entry = ledger?.sessions?.[0];
        if (entry && `${entry.id}.json` === file) {
            combined.sessions.push(entry);
            combined.lifetime.total_sessions++;
        }
    }
    recomputeLifetime(combined);
    return combined;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
    console.log(JSON.stringify(readCombinedLedger(getWolfDir()), null, 2));
//# sourceMappingURL=ledger.js.map
