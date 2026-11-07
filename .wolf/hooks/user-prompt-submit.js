import { ensureWolfDir, emitHookJSON, recordInjection, readStdin, hookMain, getSessionFilePath } from "./shared.js";
import { mutateJSON, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";
async function main() {
    ensureWolfDir();
    let input = {};
    try {
        input = JSON.parse(await readStdin());
    }
    catch { }
    const sessionFile = getSessionFilePath(input);
    // Drain-and-clear: read and clear must be one transaction or a concurrent
    // writer's reminder is dropped without ever being shown (#83).
    let drained = [];
    mutateJSON(sessionFile, {}, HOOK_LOCK_BUDGET_MS, (session) => {
        drained = session.pending_reminders ?? [];
        if (drained.length === 0)
            return;
        session.pending_reminders = [];
        recordInjection(session, "reminders", drained.join("\n\n"));
    });
    if (drained.length === 0)
        return;
    emitHookJSON("UserPromptSubmit", { additionalContext: drained.join("\n\n") });
}
hookMain("user-prompt-submit", main);
//# sourceMappingURL=user-prompt-submit.js.map