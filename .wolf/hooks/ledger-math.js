// ─────────────────────────────────────────────────────────────────────────────
// Pure token-ledger math: types, folds, lifetime derivation, migrations.
// Deliberately free of value imports so tests (and any consumer) can load it
// straight from source under Node's type stripping. IO and session assembly
// live in ledger.ts.
// ─────────────────────────────────────────────────────────────────────────────
export const MAX_LEDGER_SESSIONS = 200;
/** Sum of two per-key maps, used for both family and per-model rollups. */
export function foldMap(target, source) {
    for (const [key, value] of Object.entries(source ?? {})) {
        if (!value || typeof value !== "object")
            continue;
        const bucket = (target[key] ??= {});
        for (const [field, n] of Object.entries(value)) {
            if (typeof n !== "number" || !isFinite(n))
                continue;
            bucket[field] = (bucket[field] ?? 0) + n;
        }
    }
}
export function emptyLedger() {
    return {
        version: 1,
        created_at: "",
        lifetime: {
            total_tokens_estimated: 0,
            total_reads: 0,
            total_writes: 0,
            total_sessions: 0,
            anatomy_hits: 0,
            anatomy_misses: 0,
            repeated_reads_blocked: 0,
            repeated_reads_warned: 0,
            estimated_savings_vs_bare_cli: 0,
            injection_tokens_estimated: 0,
        },
        sessions: [],
        daemon_usage: [],
        waste_flags: [],
        optimization_report: { last_generated: null, patterns: [] },
    };
}
/** The totals block of a session entry, derived from live session state. */
export function buildSessionTotals(session, reads, writes) {
    return {
        input_tokens_estimated: reads.reduce((sum, r) => sum + r.tokens_estimated, 0),
        output_tokens_estimated: writes.reduce((sum, w) => sum + w.tokens_estimated, 0),
        reads_count: reads.length,
        writes_count: writes.length,
        // Honest accounting: only reads the hook actually denied count as
        // blocked (warnings do not prevent the read from happening).
        repeated_reads_blocked: session.reads_denied ?? 0,
        repeated_reads_warned: session.repeated_reads_warned ?? 0,
        anatomy_lookups: session.anatomy_hits,
        anatomy_misses: session.anatomy_misses,
        // Honest savings: tokens of reads that were denied, nothing else.
        savings_estimated: session.denied_tokens_saved ?? 0,
        // The other side of the scale: what OpenWolf's own context injection cost.
        injection_tokens_estimated: session.injected_tokens_estimated ?? 0,
        // Bash governor deltas: original-vs-entered is measured at the rewrite
        // point, which nothing else in the ecosystem can observe.
        bash_governed_calls: (session.bash_governed ?? []).length || undefined,
        bash_governed_original_tokens: sumBy(session.bash_governed, (g) => g.original_tokens),
        bash_governed_entered_tokens: sumBy(session.bash_governed, (g) => g.entered_tokens),
        bash_governed_by_family: byFamily(session.bash_governed),
        wolf_internal_tokens: typeof session.wolf_internal_tokens === "number" ? session.wolf_internal_tokens : undefined,
    };
}
/**
 * Group the session's governed calls by command family. The session file keeps
 * only the last 200 records, so this is computed at flush time while they are
 * still there; the ledger then carries the rollup forever.
 */
function byFamily(governed) {
    if (!governed || governed.length === 0)
        return undefined;
    const out = {};
    for (const g of governed) {
        const key = typeof g.family === "string" && g.family ? g.family : "other";
        const bucket = (out[key] ??= { calls: 0, original_tokens: 0, entered_tokens: 0 });
        bucket.calls++;
        if (isFinite(g.original_tokens))
            bucket.original_tokens += g.original_tokens;
        if (isFinite(g.entered_tokens))
            bucket.entered_tokens += g.entered_tokens;
    }
    return out;
}
function sumBy(arr, fn) {
    if (!arr || arr.length === 0)
        return undefined;
    return arr.reduce((sum, item) => sum + fn(item), 0);
}
/**
 * Position-weighted context cost (2.2). Fresh input is ~0.004% of a session's
 * input-side tokens; the real cost of a byte is that it sits in the cached
 * prefix and is re-read at the cache-read rate on EVERY subsequent API call:
 *   cost = tokens x (total_calls - call_index) x rate_per_token
 * A 10k-token read at call 500 of 1,458 costs ~$2.87 (Sonnet-class rates);
 * the same read at call 1,450 costs ~$0.02. Waste rankings must use this,
 * not raw token counts.
 */
export function positionWeightedCostUsd(tokens, callIndex, totalCalls, cacheReadUsdPerMTok) {
    const remaining = Math.max(0, totalCalls - callIndex);
    return (tokens * remaining * cacheReadUsdPerMTok) / 1_000_000;
}
export function addInto(target, key, value) {
    if (typeof value !== "number" || !isFinite(value))
        return;
    target[key] = (target[key] ?? 0) + value;
}
/** Copy only real numeric fields out of a possibly-partial totals object. */
export function numericFields(source) {
    const out = {};
    for (const [k, v] of Object.entries(source ?? {})) {
        if (typeof v === "number" && isFinite(v))
            out[k] = v;
    }
    return out;
}
/**
 * Fold one session's per-key rollups into a LifetimeMaps accumulator. Kept
 * separate from foldEntry because those maps cannot live in the scalar
 * accumulator that numericFields() produces.
 */
export function emptyMaps() {
    return { bash_governed_by_family: {}, real_by_model: {} };
}
export function foldEntryMaps(acc, e) {
    foldMap(acc.bash_governed_by_family, e.totals?.bash_governed_by_family);
    foldMap(acc.real_by_model, e.real_usage?.per_model);
}
export function foldEntry(acc, e) {
    addInto(acc, "total_tokens_estimated", e.totals.input_tokens_estimated + e.totals.output_tokens_estimated);
    addInto(acc, "total_reads", e.totals.reads_count);
    addInto(acc, "total_writes", e.totals.writes_count);
    addInto(acc, "anatomy_hits", e.totals.anatomy_lookups);
    addInto(acc, "anatomy_misses", e.totals.anatomy_misses);
    addInto(acc, "repeated_reads_blocked", e.totals.repeated_reads_blocked);
    addInto(acc, "repeated_reads_warned", e.totals.repeated_reads_warned);
    addInto(acc, "estimated_savings_vs_bare_cli", e.totals.savings_estimated);
    addInto(acc, "injection_tokens_estimated", e.totals.injection_tokens_estimated);
    addInto(acc, "bash_governed_calls", e.totals.bash_governed_calls);
    addInto(acc, "bash_governed_original_tokens", e.totals.bash_governed_original_tokens);
    addInto(acc, "bash_governed_entered_tokens", e.totals.bash_governed_entered_tokens);
    addInto(acc, "wolf_internal_tokens", e.totals.wolf_internal_tokens);
    if (e.real_usage) {
        addInto(acc, "real_input_tokens", e.real_usage.input_tokens);
        addInto(acc, "real_output_tokens", e.real_usage.output_tokens);
        addInto(acc, "real_cache_read_tokens", e.real_usage.cache_read_input_tokens);
        addInto(acc, "real_cache_creation_tokens", e.real_usage.cache_creation_input_tokens);
        addInto(acc, "real_api_calls", e.real_usage.api_calls);
    }
}
/**
 * Derive lifetime = baseline + fold(sessions). total_sessions is intentionally
 * NOT derived here — session-start counts it once per new session.
 */
export function recomputeLifetime(ledger) {
    const acc = numericFields(ledger.lifetime_baseline);
    delete acc.total_sessions;
    const maps = emptyMaps();
    foldMap(maps.bash_governed_by_family, ledger.lifetime_baseline_maps?.bash_governed_by_family);
    foldMap(maps.real_by_model, ledger.lifetime_baseline_maps?.real_by_model);
    for (const e of ledger.sessions) {
        foldEntry(acc, e);
        foldEntryMaps(maps, e);
    }
    ledger.lifetime_maps = maps;
    const totalSessions = ledger.lifetime?.total_sessions ?? 0;
    ledger.lifetime = {
        total_tokens_estimated: 0,
        total_reads: 0,
        total_writes: 0,
        anatomy_hits: 0,
        anatomy_misses: 0,
        repeated_reads_blocked: 0,
        repeated_reads_warned: 0,
        estimated_savings_vs_bare_cli: 0,
        injection_tokens_estimated: 0,
        ...acc,
        total_sessions: totalSessions,
    };
}
/**
 * One-time legacy migration: sessions written before 2.0.5 stored the number
 * of duplicate-read WARNINGS in totals.repeated_reads_blocked (the field
 * predates deny mode). Under the current semantics a blocked read always
 * credits savings_estimated > 0 (a denial saves the previous read's tokens,
 * and denyEligible requires tokens > 0), so blocked > 0 with zero savings can
 * only be legacy data. Move those counts to repeated_reads_warned and zero
 * the blocked field. Also migrates the lifetime_baseline, which may hold
 * folded-off legacy sessions. Returns the number of records rewritten.
 * Caller is responsible for recomputeLifetime() and persisting.
 */
export function migrateLegacyBlockedCounts(ledger) {
    let migrated = 0;
    for (const s of ledger.sessions ?? []) {
        const t = s?.totals;
        if (!t)
            continue;
        if ((t.repeated_reads_blocked ?? 0) > 0 && !((t.savings_estimated ?? 0) > 0)) {
            t.repeated_reads_warned = (t.repeated_reads_warned ?? 0) + t.repeated_reads_blocked;
            t.repeated_reads_blocked = 0;
            migrated++;
        }
    }
    const base = ledger.lifetime_baseline;
    if (base &&
        (base.repeated_reads_blocked ?? 0) > 0 &&
        !((base.estimated_savings_vs_bare_cli ?? 0) > 0)) {
        base.repeated_reads_warned = (base.repeated_reads_warned ?? 0) + (base.repeated_reads_blocked ?? 0);
        base.repeated_reads_blocked = 0;
        migrated++;
    }
    return migrated;
}
//# sourceMappingURL=ledger-math.js.map