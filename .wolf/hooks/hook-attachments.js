import * as fs from "node:fs";
function isOpenWolfCommand(command) {
    return typeof command === "string" && command.includes(".wolf/hooks/");
}
/** The hook script name out of the registered command, e.g. "pre-read.js". */
function hookFileOf(command) {
    const m = command.match(/\.wolf\/hooks\/([\w.-]+\.js)/);
    return m ? m[1] : command.slice(0, 60);
}
/**
 * Parse the transcript and reconcile OpenWolf hook activity. Returns null
 * when the transcript is unreadable or the line format fails the schema
 * probe — callers must then present self-reported numbers as estimates.
 */
export function verifyHookDelivery(transcriptPath) {
    let raw;
    try {
        raw = fs.readFileSync(transcriptPath, "utf-8");
    }
    catch {
        return null;
    }
    const lines = raw.split("\n").filter((l) => l.trim());
    if (lines.length === 0)
        return null;
    // Schema probe: the format is declared unstable, so verify the envelope
    // still looks like what we expect before trusting any counts.
    let parsed = 0;
    let probed = 0;
    const records = [];
    for (const line of lines) {
        let entry;
        try {
            entry = JSON.parse(line);
            parsed++;
        }
        catch {
            continue;
        }
        if (probed < 20) {
            probed++;
            if (typeof entry !== "object" || entry === null || typeof entry.type !== "string") {
                return null;
            }
        }
        if (entry.type === "attachment" && entry.attachment && typeof entry.attachment === "object") {
            records.push(entry.attachment);
        }
    }
    if (parsed < lines.length * 0.5)
        return null;
    const result = {
        hooks_fired: 0,
        hooks_failed: 0,
        injections_delivered: 0,
        injection_tokens_delivered: 0,
        per_hook: {},
    };
    for (const att of records) {
        const isHookRecord = att.type === "hook_success" || att.type === "hook_non_blocking_error" || att.type === "hook_failure";
        if (!isHookRecord || !isOpenWolfCommand(att.command))
            continue;
        const hook = hookFileOf(att.command);
        const entry = result.per_hook[hook] ?? (result.per_hook[hook] = { fired: 0, failed: 0, last_exit: 0 });
        result.hooks_fired++;
        entry.fired++;
        const exit = typeof att.exitCode === "number" ? att.exitCode : 0;
        entry.last_exit = exit;
        if (exit !== 0 || att.type !== "hook_success") {
            result.hooks_failed++;
            entry.failed++;
            result.last_failure = { hook, stderr_head: (att.stderr ?? "").slice(0, 200) };
        }
        // Delivered injection: the harness recorded our stdout, and it carried
        // an additionalContext payload (which the harness inserts into context).
        if (typeof att.stdout === "string" && att.stdout.includes("additionalContext")) {
            try {
                const out = JSON.parse(att.stdout);
                const ctx = out?.hookSpecificOutput?.additionalContext;
                if (typeof ctx === "string" && ctx.length > 0) {
                    result.injections_delivered++;
                    result.injection_tokens_delivered += Math.ceil(ctx.length / 4);
                }
            }
            catch { }
        }
    }
    return result;
}
//# sourceMappingURL=hook-attachments.js.map