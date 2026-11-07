import * as fs from "node:fs";
import * as path from "node:path";
import { getWolfDir, ensureWolfDir, readJSON, readStdin, emitHookJSON, hookMain, getSessionFilePath, recordInjection, readCerebrumSection } from "./shared.js";
import { mutateJSON, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";
import { topRules } from "./rule-reinjection.js";
// ─────────────────────────────────────────────────────────────────────────────
// PostToolBatch decay countermeasure (2.4). Compliance with instructions
// decays within a session (~5.6% lower odds per generated function in the
// only factorial study of the question, arXiv 2605.10039); file size does
// not fix that — cadence does. Every N tool batches, the top Do-Not-Repeat
// rules are re-surfaced as short factual statements (~100 tokens). Config:
// openwolf.context.reinjection_interval (batches; 0 disables; default 25).
// ─────────────────────────────────────────────────────────────────────────────
function reinjectionInterval(wolfDir) {
    const cfg = readJSON(path.join(wolfDir, "config.json"), {});
    const n = cfg.openwolf?.context?.reinjection_interval;
    if (typeof n !== "number" || !isFinite(n))
        return 25;
    return Math.max(0, Math.floor(n));
}
async function main() {
    ensureWolfDir();
    const wolfDir = getWolfDir();
    let input = {};
    try {
        input = JSON.parse(await readStdin());
    }
    catch { }
    const sessionFile = getSessionFilePath(input);
    const interval = reinjectionInterval(wolfDir);
    if (interval === 0)
        return;
    let rules = [];
    try {
        rules = topRules(readCerebrumSection(wolfDir, "Do-Not-Repeat"), 3);
    }
    catch { }
    // tool_batches is a counter driving an every-Nth-batch reinjection: an
    // unlocked increment both undercounts and can fire the note twice (#83).
    let note = null;
    mutateJSON(sessionFile, {}, HOOK_LOCK_BUDGET_MS, (session) => {
        session.tool_batches = (session.tool_batches ?? 0) + 1;
        if (session.tool_batches % interval !== 0)
            return;
        if (rules.length === 0)
            return;
        note = `Project rules still in effect (from .wolf/cerebrum/ Do-Not-Repeat):\n${rules.join("\n")}`;
        recordInjection(session, "decay_reinjection", note);
    });
    if (note !== null)
        emitHookJSON("PostToolBatch", { additionalContext: note });
}
hookMain("post-batch", main);
//# sourceMappingURL=post-batch.js.map