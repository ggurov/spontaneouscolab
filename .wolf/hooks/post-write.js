import * as fs from "node:fs";
import * as path from "node:path";
import { getWolfDir, ensureWolfDir, readJSON, writeJSON, readBugLogFile, extractDescription, estimateTokens, appendMarkdown, timeShort, readStdin, normalizePath, isSensitiveFile, getProjectDir, emitHookJSON, recordInjection, hookMain, getSessionFilePath } from "./shared.js";
import { loadStoreReconciled, saveStore, renderToFile, sha256 } from "./anatomy-store.js";
import { withAnatomyLock, withFileLock, mutateJSON, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";
import { extractSymbols, symbolsSupported, SYMBOL_MIN_TOKENS } from "./symbol-extractor.js";
import { createBugIdentity } from "./bug-id.js";
// File types where a value/string change is normal content editing, not a bug
// fix — auto bug detection never runs on these (see autoDetectBugFix). Without
// this, a version bump in a README or a key change in a JSON/YAML config is
// logged as a "wrong-value" bug, since the detector matches quoted spans
// (including markdown backticks) regardless of file type.
const NON_CODE_EXTS = new Set([
    ".md", ".mdx", ".markdown", ".txt", ".rst", ".adoc",
    ".json", ".jsonc", ".yaml", ".yml", ".toml", ".ini", ".env",
    ".lock", ".csv", ".tsv",
]);
async function main() {
    ensureWolfDir();
    const wolfDir = getWolfDir();
    const projectRoot = getProjectDir();
    const raw = await readStdin();
    let input;
    try {
        input = JSON.parse(raw);
    }
    catch {
        return;
    }
    const sessionFile = getSessionFilePath(input);
    const toolName = input.tool_name ?? "Write";
    const filePath = input.tool_input?.file_path ?? input.tool_input?.path ?? "";
    if (!filePath) {
        return;
    }
    const absolutePath = path.isAbsolute(filePath) ? filePath : path.join(projectRoot, filePath);
    // .wolf/ state files skip anatomy/memory bookkeeping (self-referential),
    // but 2.4 adds the budget-enforcement loop the platform's native memory
    // uses: measure after every write, warn factually when a state file
    // outgrows its budget. Oversized state gets injected/read every session,
    // so bloat here is a recurring tax nothing else pushes back on.
    const relPath = normalizePath(path.relative(projectRoot, absolutePath));
    if (relPath.startsWith(".wolf/")) {
        try {
            const budget = stateBudgetFor(wolfDir, relPath);
            if (budget !== null) {
                const content = fs.readFileSync(absolutePath, "utf-8");
                const tokens = estimateTokens(content, "prose");
                if (tokens > budget) {
                    // Test-and-set the once-per-file flag in one transaction (#83).
                    let warn = null;
                    mutateJSON(sessionFile, { files_written: [], edit_counts: {} }, HOOK_LOCK_BUDGET_MS, (session) => {
                        const warned = (session.budget_warned ?? {});
                        if (warned[relPath])
                            return;
                        warned[relPath] = true;
                        session.budget_warned = warned;
                        warn = `OpenWolf: ${relPath} is now ~${tokens} tokens; its budget is ${budget}. It is read at session starts, so size is a recurring cost. Consolidate or move detail into topic files, keeping the most recent and most important entries.`;
                        recordInjection(session, "budget_warn", warn);
                    });
                    if (warn !== null)
                        emitHookJSON("PostToolUse", { additionalContext: warn });
                }
            }
        }
        catch { }
        return;
    }
    // Never track files outside the project root (e.g. the Claude Code scratchpad under
    // /private/tmp). path.relative() yields ../.. section keys that pollute anatomy.md and are
    // wiped again by every full `openwolf scan`, so the index churns instead of converging.
    if (relPath.startsWith("..") || path.isAbsolute(relPath)) {
        return;
    }
    // Never track secret-bearing files in anatomy/memory (issue #54): .env is
    // not the only file whose *description* would leak sensitive content.
    const baseName = path.basename(absolutePath);
    if (isSensitiveFile(baseName)) {
        return;
    }
    const oldStr = input.tool_input?.old_string ?? "";
    const newStr = input.tool_input?.new_string ?? "";
    // 1. Update the anatomy store, then re-render anatomy.md from it.
    //    All of this happens under the anatomy lock; if the lock cannot be
    //    acquired within budget we skip — a later writer converges the state.
    try {
        const relPathLocal = normalizePath(path.relative(projectRoot, absolutePath));
        let fileContent = "";
        try {
            fileContent = fs.readFileSync(absolutePath, "utf-8");
        }
        catch {
            fileContent = input.tool_input?.content ?? "";
        }
        const desc = extractDescription(absolutePath).slice(0, 100);
        const ext = path.extname(absolutePath).toLowerCase();
        const codeExts = new Set([".ts", ".js", ".tsx", ".jsx", ".py", ".json", ".yaml", ".yml", ".css"]);
        const proseExts = new Set([".md", ".txt", ".rst"]);
        const type = codeExts.has(ext) ? "code" : proseExts.has(ext) ? "prose" : "mixed";
        const tokens = estimateTokens(fileContent, type);
        let size;
        let mtimeMs;
        try {
            const st = fs.statSync(absolutePath);
            size = st.size;
            mtimeMs = st.mtimeMs;
        }
        catch { }
        // Symbols are recomputed on every write (never carried over — the
        // content just changed, so old line ranges would misdirect slice reads).
        const symbols = tokens >= SYMBOL_MIN_TOKENS && symbolsSupported(ext)
            ? extractSymbols(fileContent, ext)
            : undefined;
        withAnatomyLock(wolfDir, HOOK_LOCK_BUDGET_MS, () => {
            const store = loadStoreReconciled(wolfDir, projectRoot);
            store.files[relPathLocal] = {
                description: desc,
                tokens,
                hash: sha256(fileContent).slice(0, 16),
                size,
                mtimeMs,
                updatedAt: new Date().toISOString(),
                source: "hook",
                symbols: symbols && symbols.length > 0 ? symbols : undefined,
            };
            store.meta.lastScanned = new Date().toISOString();
            renderToFile(wolfDir, store);
            saveStore(wolfDir, store);
        });
    }
    catch { }
    // 2. LOCAL PATCH (epicefi_fw): no per-edit memory.md row.
    //    Stock OpenWolf appends one "| HH:MM | Edited <file> | ... |" line per
    //    write, which refills memory.md with per-edit noise the team pruned in
    //    2fdb49a. session-end.js already writes one aggregate line per session,
    //    and .wolf/OPENWOLF.md asks for one semantic summary per session.
    //    RE-APPLY AFTER `openwolf update`, which overwrites this file.

    // 3. Record in session tracker + track edit counts
    try {
        const normalizedFile = normalizePath(filePath);
        const action = toolName === "Write" ? "create" : "edit";
        const fileContent = input.tool_input?.content ?? "";
        const tokens = estimateTokens(fileContent || newStr, "code");
        const editKey = normalizePath(path.relative(projectRoot, absolutePath));
        // files_written is an append, edit_counts an increment, edit_warned a
        // test-and-set: all three lose data under an unlocked read-modify-write,
        // and parallel Edit calls are ordinary agent behavior (#83).
        let editWarn = "";
        mutateJSON(sessionFile, { files_written: [], edit_counts: {} }, HOOK_LOCK_BUDGET_MS, (session) => {
            if (!session.edit_counts)
                session.edit_counts = {};
            if (!Array.isArray(session.files_written))
                session.files_written = [];
            session.files_written.push({
                file: normalizedFile,
                action,
                tokens,
                at: new Date().toISOString(),
            });
            session.edit_counts[editKey] = (session.edit_counts[editKey] || 0) + 1;
            if (session.files_read && session.files_read[normalizedFile]) {
                delete session.files_read[normalizedFile];
            }
            // Once per file per session: firing on the 3rd edit AND every edit after
            // it would hit ~39% of all write operations (measured) — pure noise.
            if (!session.edit_warned)
                session.edit_warned = {};
            editWarn = session.edit_counts[editKey] >= 3 && !session.edit_warned[editKey]
                ? `OpenWolf: ${baseName} has been edited ${session.edit_counts[editKey]} times this session. If you're fixing a bug, log it: node .wolf/tools/wolf_bug.js append --file <path> --error "..." --root-cause "..." --fix "..." --tags a,b`
                : "";
            if (editWarn)
                session.edit_warned[editKey] = true;
            if (editWarn)
                recordInjection(session, "edit_warn", editWarn);
        });
        if (editWarn) {
            emitHookJSON("PostToolUse", { additionalContext: editWarn });
        }
    }
    catch { }
    // 4. Auto-detect bug-fix patterns and log them
    try {
        if (oldStr && newStr) {
            autoDetectBugFix(wolfDir, absolutePath, projectRoot, oldStr, newStr, path.basename(sessionFile, ".json"));
        }
    }
    catch { }
}
/** Token budget for a .wolf state file, or null when unbudgeted (2.4). */
function stateBudgetFor(wolfDir, relPath) {
    const defaults = {
        // LOCAL PATCH (shard cutover 2026-09-18): cerebrum budget keys moved to
        // the section files (one key each — the bloat guard must cover all five);
        // keep the legacy key for the migration window.
        ".wolf/cerebrum/user-preferences.md": 2000,
        ".wolf/cerebrum/key-learnings.md": 2000,
        ".wolf/cerebrum/do-not-repeat.md": 2000,
        ".wolf/cerebrum/decision-log.md": 2000,
        ".wolf/cerebrum/project-rules.md": 2000,
        ".wolf/cerebrum.md": 2000,
        ".wolf/STATUS.md": 1000,
    };
    const cfg = readJSON(path.join(wolfDir, "config.json"), {});
    const merged = { ...defaults, ...(cfg.openwolf?.context?.state_budgets ?? {}) };
    return merged[relPath] ?? null;
}
// ─── Edit Summarizer ─────────────────────────────────────────────
function summarizeEdit(oldStr, newStr, filename) {
    const oldLines = oldStr.split("\n");
    const newLines = newStr.split("\n");
    const oldCount = oldLines.length;
    const newCount = newLines.length;
    const ext = path.extname(filename).toLowerCase();
    // --- Structural fixes ---
    if (/\btry\b/.test(newStr) && hasCatchConstruct(newStr, ext) && !hasCatchConstruct(oldStr, ext)) {
        return "added error handling";
    }
    if (newStr.includes("?.") && !oldStr.includes("?."))
        return "added optional chaining";
    if (newStr.includes("?? ") && !oldStr.includes("?? "))
        return "added nullish coalescing";
    // --- Deleted code ---
    if (!newStr.trim() || newStr.trim().length < oldStr.trim().length * 0.2) {
        return `removed ${oldCount} lines`;
    }
    // --- Import changes ---
    const oldImports = oldLines.filter(l => /^\s*(import|require|use |from )/.test(l)).length;
    const newImports = newLines.filter(l => /^\s*(import|require|use |from )/.test(l)).length;
    if (newImports > oldImports && Math.abs(newCount - oldCount) <= newImports - oldImports + 1) {
        return `added ${newImports - oldImports} import(s)`;
    }
    // --- Value/string replacement (common bug fix: wrong value) ---
    if (oldCount === 1 && newCount === 1) {
        const o = oldStr.trim();
        const n = newStr.trim();
        // String literal change
        const oStr = o.match(/['"`]([^'"`]+)['"`]/);
        const nStr = n.match(/['"`]([^'"`]+)['"`]/);
        if (oStr && nStr && oStr[1] !== nStr[1]) {
            return `"${oStr[1].slice(0, 25)}" → "${nStr[1].slice(0, 25)}"`;
        }
        // Number change
        const oNum = o.match(/\b(\d+\.?\d*)\b/);
        const nNum = n.match(/\b(\d+\.?\d*)\b/);
        if (oNum && nNum && oNum[1] !== nNum[1] && o.replace(oNum[1], "") === n.replace(nNum[1], "")) {
            return `${oNum[1]} → ${nNum[1]}`;
        }
        return "inline fix";
    }
    // --- Method/function call changes ---
    const oldCalls = extractCalls(oldStr);
    const newCalls = extractCalls(newStr);
    const addedCalls = newCalls.filter(c => !oldCalls.includes(c));
    const removedCalls = oldCalls.filter(c => !newCalls.includes(c));
    if (removedCalls.length === 1 && addedCalls.length === 1) {
        return `${removedCalls[0]}() → ${addedCalls[0]}()`;
    }
    // --- CSS/style changes ---
    if (ext === ".css" || ext === ".scss" || ext === ".vue" || ext === ".tsx" || ext === ".jsx") {
        const oldProps = (oldStr.match(/[\w-]+\s*:/g) || []).map(p => p.replace(/\s*:/, ""));
        const newProps = (newStr.match(/[\w-]+\s*:/g) || []).map(p => p.replace(/\s*:/, ""));
        const changed = newProps.filter(p => !oldProps.includes(p));
        if (changed.length > 0 && changed.length <= 3) {
            return `CSS: ${changed.join(", ")}`;
        }
    }
    // --- Condition changes ---
    const oldConds = (oldStr.match(/if\s*\(([^)]+)\)/g) || []);
    const newConds = (newStr.match(/if\s*\(([^)]+)\)/g) || []);
    if (newConds.length > oldConds.length) {
        return `added ${newConds.length - oldConds.length} condition(s)`;
    }
    // --- Function modified ---
    const fnMatch = newStr.match(/(?:function|def|fn|func|async\s+function)\s+(\w+)/);
    if (fnMatch) {
        return `modified ${fnMatch[1]}()`;
    }
    // --- Class/method context ---
    const methodMatch = newStr.match(/(?:public|private|protected)?\s*(?:async\s+)?(\w+)\s*\([^)]*\)\s*[:{]/);
    if (methodMatch) {
        return `modified ${methodMatch[1]}()`;
    }
    // --- Size-based fallback ---
    if (newCount > oldCount + 5)
        return `expanded (+${newCount - oldCount} lines)`;
    if (oldCount > newCount + 5)
        return `reduced (-${oldCount - newCount} lines)`;
    return `${oldCount}→${newCount} lines`;
}
function extractCalls(code) {
    return [...new Set((code.match(/(\w+)\s*\(/g) || [])
            .map(m => m.match(/(\w+)/)?.[1] || "")
            .filter(n => n.length > 2 && !["if", "for", "while", "switch", "catch", "function", "return", "new", "typeof", "instanceof", "const", "let", "var"].includes(n)))];
}
// ─── Auto Bug Detection ──────────────────────────────────────────
function bugAutoDetectEnabled(wolfDir) {
    try {
        const cfg = readJSON(path.join(wolfDir, "config.json"), {});
        // Default on; only an explicit `false` disables auto bug detection.
        return cfg.openwolf?.buglog?.auto_detect !== false;
    }
    catch {
        return true;
    }
}
function autoDetectBugFix(wolfDir, absolutePath, projectRoot, oldStr, newStr, sessionId) {
    const basename = path.basename(absolutePath);
    const ext = path.extname(basename).toLowerCase();
    // Bug-fix detection is a code concept — never fire on prose/docs/data files.
    if (NON_CODE_EXTS.has(ext))
        return;
    // Respect an explicit opt-out in .wolf/config.json (default: enabled).
    if (!bugAutoDetectEnabled(wolfDir))
        return;
    const bugLogPath = path.join(wolfDir, "buglog.json");
    const relFile = normalizePath(path.relative(projectRoot, absolutePath));
    // Detect what kind of fix this is
    const detection = detectFixPattern(oldStr, newStr, ext, basename);
    if (!detection)
        return;
    withFileLock(bugLogPath + ".lock", HOOK_LOCK_BUDGET_MS, () => {
        const bugLog = readBugLogFile(wolfDir);
        // Check for recent duplicate (same file + same category within 5 min)
        const recentDupe = bugLog.bugs.find(b => {
            if (path.basename(b.file) !== basename)
                return false;
            if (!b.tags.includes("auto-detected"))
                return false;
            if (!b.tags.includes(detection.category))
                return false;
            const bugTime = new Date(b.last_seen).getTime();
            return (Date.now() - bugTime) < 5 * 60 * 1000;
        });
        if (recentDupe) {
            recentDupe.occurrences++;
            recentDupe.last_seen = new Date().toISOString();
            // Append additional context
            if (detection.context && !recentDupe.fix.includes(detection.context)) {
                recentDupe.fix += ` | Also: ${detection.context}`;
            }
            writeJSON(bugLogPath, bugLog);
            return;
        }
        const identity = createBugIdentity(new Set(bugLog.bugs.map(bug => bug.id)));
        bugLog.bugs.push({
            ...identity,
            session_id: sessionId,
            error_message: detection.summary,
            file: relFile,
            root_cause: detection.rootCause,
            fix: detection.fix,
            tags: ["auto-detected", detection.category, ext.replace(".", "") || "unknown"],
            related_bugs: [],
            occurrences: 1,
            last_seen: new Date().toISOString(),
        });
        writeJSON(bugLogPath, bugLog);
    });
}
function detectFixPattern(oldStr, newStr, ext, basename) {
    const oldLines = oldStr.split("\n");
    const newLines = newStr.split("\n");
    const isTest = isTestFile(basename);
    // --- Error handling added ---
    if (!isTest && hasCatchConstruct(newStr, ext) && !hasCatchConstruct(oldStr, ext)) {
        const fn = newStr.match(/(?:function|def|async)\s+(\w+)/)?.[1];
        return {
            category: "error-handling",
            summary: `Missing error handling in ${fn ? `${fn}()` : basename}`,
            rootCause: "Code path had no error handling — exceptions would propagate uncaught",
            fix: `Added try/catch block`,
            context: extractChangedLines(oldStr, newStr),
        };
    }
    // --- Null/undefined safety ---
    if ((newStr.includes("?.") && !oldStr.includes("?.")) ||
        (newStr.includes("?? ") && !oldStr.includes("?? ")) ||
        (/!==?\s*(null|undefined)/.test(newStr) && !/!==?\s*(null|undefined)/.test(oldStr))) {
        return {
            category: "null-safety",
            summary: `Null/undefined access in ${basename}`,
            rootCause: "Property access on potentially null/undefined value",
            fix: `Added null safety (optional chaining or null check)`,
            context: extractChangedLines(oldStr, newStr),
        };
    }
    // --- Guard clause / early return added ---
    if (!isTest &&
        /if\s*\([^)]*\)\s*(return|throw|continue|break)/.test(newStr) &&
        !/if\s*\([^)]*\)\s*(return|throw|continue|break)/.test(oldStr)) {
        const condition = newStr.match(/if\s*\(([^)]+)\)/)?.[1]?.trim().slice(0, 60) || "condition";
        return {
            category: "guard-clause",
            summary: `Missing guard clause`,
            rootCause: `No early return/throw for edge case: ${condition}`,
            fix: `Added guard clause: if (${condition.slice(0, 40)})`,
        };
    }
    // --- Wrong value / string fix (very common bug) ---
    if (oldLines.length <= 3 && newLines.length <= 3) {
        const oldJoined = oldStr.trim();
        const newJoined = newStr.trim();
        // String literal changed
        const oStrs = oldJoined.match(/['"`]([^'"`]{2,})['"`]/g) || [];
        const nStrs = newJoined.match(/['"`]([^'"`]{2,})['"`]/g) || [];
        if (oStrs.length > 0 && nStrs.length > 0) {
            for (let i = 0; i < Math.min(oStrs.length, nStrs.length); i++) {
                if (oStrs[i] !== nStrs[i]) {
                    return {
                        category: "wrong-value",
                        summary: `Incorrect value in code`,
                        rootCause: `Had ${oStrs[i].slice(0, 50)}`,
                        fix: `Changed to ${nStrs[i].slice(0, 50)}`,
                    };
                }
            }
        }
        // Variable name / method call changed
        const oldTokens = tokenizeCode(oldJoined);
        const newTokens = tokenizeCode(newJoined);
        const changed = [];
        for (let i = 0; i < Math.min(oldTokens.length, newTokens.length); i++) {
            if (oldTokens[i] !== newTokens[i]) {
                changed.push([oldTokens[i], newTokens[i]]);
            }
        }
        if (changed.length === 1 && changed[0][0].length > 2) {
            return {
                category: "wrong-reference",
                summary: `Wrong reference: ${changed[0][0]} should be ${changed[0][1]}`,
                rootCause: `Used "${changed[0][0]}" instead of "${changed[0][1]}"`,
                fix: `Changed ${changed[0][0]} → ${changed[0][1]}`,
            };
        }
    }
    // --- Logic fix (condition changed) ---
    const oldCond = oldStr.match(/if\s*\(([^)]+)\)/)?.[1];
    const newCond = newStr.match(/if\s*\(([^)]+)\)/)?.[1];
    if (oldCond && newCond && oldCond !== newCond && oldLines.length <= 5) {
        return {
            category: "logic-fix",
            summary: `Wrong condition in logic`,
            rootCause: `Condition was: if (${oldCond.slice(0, 50)})`,
            fix: `Changed to: if (${newCond.slice(0, 50)})`,
        };
    }
    // --- Operator fix (=== vs ==, > vs >=, etc.) ---
    const opChange = findOperatorChange(oldStr, newStr);
    if (opChange) {
        return {
            category: "operator-fix",
            summary: `Wrong operator: ${opChange.old} should be ${opChange.new}`,
            rootCause: `Used "${opChange.old}" instead of "${opChange.new}"`,
            fix: `Changed operator ${opChange.old} → ${opChange.new}`,
        };
    }
    // --- Missing import/require ---
    const oldImports = new Set((oldStr.match(/(?:import|require)\s*\(?['"]([^'"]+)['"]\)?/g) || []).map(m => m));
    const newImports = (newStr.match(/(?:import|require)\s*\(?['"]([^'"]+)['"]\)?/g) || []);
    const addedImports = newImports.filter(i => !oldImports.has(i));
    if (addedImports.length > 0 && newLines.length - oldLines.length <= addedImports.length + 2) {
        const modules = addedImports.map(i => i.match(/['"]([^'"]+)['"]/)?.[1] || "").filter(Boolean);
        return {
            category: "missing-import",
            summary: `Missing import: ${modules.join(", ")}`,
            rootCause: `Module(s) not imported: ${modules.join(", ")}`,
            fix: `Added import(s) for ${modules.join(", ")}`,
        };
    }
    // --- Return value fix ---
    const oldReturn = oldStr.match(/return\s+(.+)/)?.[1]?.trim();
    const newReturn = newStr.match(/return\s+(.+)/)?.[1]?.trim();
    if (oldReturn && newReturn && oldReturn !== newReturn && oldLines.length <= 5) {
        return {
            category: "return-value",
            summary: `Wrong return value`,
            rootCause: `Was returning: ${oldReturn.slice(0, 50)}`,
            fix: `Now returns: ${newReturn.slice(0, 50)}`,
        };
    }
    // --- Async/await fix ---
    if (newStr.includes("await ") && !oldStr.includes("await ")) {
        return {
            category: "async-fix",
            summary: `Missing await`,
            rootCause: `Async call without await — returned Promise instead of value`,
            fix: `Added await to async call`,
            context: extractChangedLines(oldStr, newStr),
        };
    }
    if (newStr.includes("async ") && !oldStr.includes("async ")) {
        return {
            category: "async-fix",
            summary: `Function not marked async`,
            rootCause: `Function uses await but wasn't declared async`,
            fix: `Added async modifier`,
        };
    }
    // --- Type annotation/cast fix ---
    if (ext === ".ts" || ext === ".tsx") {
        if ((newStr.includes(" as ") && !oldStr.includes(" as ")) ||
            (newStr.includes(": ") && !oldStr.includes(": ") && oldLines.length <= 3)) {
            return {
                category: "type-fix",
                summary: `Type error`,
                rootCause: `Missing or incorrect type annotation`,
                fix: `Added type assertion/annotation`,
                context: extractChangedLines(oldStr, newStr),
            };
        }
    }
    // --- CSS/style fix ---
    if (ext === ".css" || ext === ".scss" || ext === ".vue" || ext === ".tsx" || ext === ".jsx") {
        const oldProps = extractCSSProps(oldStr);
        const newProps = extractCSSProps(newStr);
        const changedProps = [...newProps.entries()].filter(([k, v]) => oldProps.get(k) !== v && oldProps.has(k));
        if (changedProps.length > 0 && changedProps.length <= 3) {
            const desc = changedProps.map(([k, v]) => `${k}: ${oldProps.get(k)} → ${v}`).join("; ");
            return {
                category: "style-fix",
                summary: `CSS fix: ${changedProps.map(([k]) => k).join(", ")}`,
                rootCause: desc,
                fix: `Changed ${desc}`,
            };
        }
    }
    // --- Significant diff (catch-all for substantial edits) ---
    const diffRatio = Math.abs(newStr.length - oldStr.length) / Math.max(oldStr.length, 1);
    if (diffRatio > 0.3 && oldLines.length >= 3 && newLines.length >= 3) {
        // Only log if there's meaningful structural change, not just additions
        const removedLines = oldLines.filter(l => l.trim() && !newLines.some(nl => nl.trim() === l.trim()));
        if (removedLines.length >= 2) {
            return {
                category: "refactor",
                summary: `Significant refactor of ${basename}`,
                rootCause: `${removedLines.length} lines replaced/restructured`,
                fix: `Rewrote ${oldLines.length}→${newLines.length} lines (${removedLines.length} removed)`,
                context: removedLines.slice(0, 2).map(l => l.trim().slice(0, 50)).join("; "),
            };
        }
    }
    return null;
}
function hasCatchConstruct(code, ext) {
    if (ext === ".py")
        return /\bexcept\b[^\n]*:/.test(code);
    return /\bcatch\s*[({]/.test(code);
}
function isTestFile(basename) {
    return /(\.test\.|\.spec\.|_test\.|_spec\.)/i.test(basename) ||
        /(Test|Tests|IT|Spec)\.\w+$/.test(basename);
}
function extractChangedLines(oldStr, newStr) {
    const oldLines = new Set(oldStr.split("\n").map(l => l.trim()).filter(Boolean));
    const newLines = newStr.split("\n").map(l => l.trim()).filter(Boolean);
    const added = newLines.filter(l => !oldLines.has(l));
    return added.slice(0, 2).map(l => l.slice(0, 60)).join("; ");
}
function tokenizeCode(code) {
    return code.replace(/[^\w$]/g, " ").split(/\s+/).filter(t => t.length > 0);
}
function findOperatorChange(oldStr, newStr) {
    const operators = ["===", "!==", "==", "!=", ">=", "<=", ">>", "<<", "&&", "||", "??"];
    for (const op of operators) {
        if (oldStr.includes(op) && !newStr.includes(op)) {
            for (const op2 of operators) {
                if (op2 !== op && newStr.includes(op2) && !oldStr.includes(op2)) {
                    return { old: op, new: op2 };
                }
            }
        }
    }
    return null;
}
function extractCSSProps(code) {
    const props = new Map();
    const matches = code.matchAll(/([\w-]+)\s*:\s*([^;}\n]+)/g);
    for (const m of matches) {
        props.set(m[1].trim(), m[2].trim());
    }
    return props;
}
hookMain("post-write", main);
//# sourceMappingURL=post-write.js.map
