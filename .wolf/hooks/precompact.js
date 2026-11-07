import { ensureWolfDir, readJSON, writeJSON, readStdin, timestamp, hookMain, getSessionFilePath } from "./shared.js";
// PreCompact hook (Workstream F3: compaction survival).
//
// Fires just before Claude Code / Codex compacts the context window. The
// in-flight session state survives on disk, but nothing in
// the compacted context tells the model what already happened. This hook:
//   1. snapshots the session state (belt-and-braces for post-mortems), and
//   2. after compaction, SessionStart fires with source "compact" — the
//      session-start hook reads the same state and re-injects a digest via
//      additionalContext. That pair is the survival mechanism.
async function main() {
    ensureWolfDir();
    let input = {};
    try {
        input = JSON.parse(await readStdin());
    }
    catch { }
    try {
        const sessionFile = getSessionFilePath(input);
        const session = readJSON(sessionFile, {});
        writeJSON(sessionFile.replace(/\.json$/, ".precompact.json"), {
            at: timestamp(),
            trigger: input.trigger ?? "unknown",
            session,
        });
    }
    catch { }
}
hookMain("precompact", main);
//# sourceMappingURL=precompact.js.map
