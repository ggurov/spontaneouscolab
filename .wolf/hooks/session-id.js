import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { withFileLock, HOOK_LOCK_BUDGET_MS } from "./anatomy-lock.js";

export function agentHarness() {
    if (process.env.CLAUDECODE || process.env.CLAUDE_CODE_ENTRYPOINT || process.env.CLAUDE_PROJECT_DIR)
        return "claude";
    if (process.env.CODEX_PROJECT_ROOT || process.env.CODEX_SANDBOX)
        return "codex";
    if (process.env.OPENCODE || process.env.OPENCODE_PROJECT_ROOT)
        return "opencode";
    return "default";
}

export function newSessionId() {
    const host = (process.env.HOSTNAME || os.hostname()).replace(/[^\w.-]/g, "_").slice(0, 63);
    return `SESSION-${host}-${crypto.randomUUID()}`;
}

const validId = /^SESSION-[\w.-]+-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const nonempty = value => typeof value === "string" && value.length > 0;

export function sessionArtifactPath(wolfDir, directory, id, extension) {
    if (typeof id !== "string" || !/^[\w.-]{4,128}$/.test(id))
        throw new Error("Invalid Wolf session ID for artifact path");
    return path.join(wolfDir, directory, `${id}.${extension}`);
}

/** A persistent, machine-local mapping from a harness session to a Wolf session. */
export function resolveSessionIdentity(wolfDir, agent, input = {}, { newSession = false, startSource = null } = {}) {
    if (fs.existsSync(path.join(wolfDir, "..", ".wolf-disable")))
        throw new Error("OpenWolf session state is read-only (.wolf-disable)");
    const supplied = [input.session_id, process.env.OPENWOLF_SESSION_ID, process.env.CODEX_THREAD_ID].find(nonempty);
    const transcript = input.agent_transcript_path || input.transcript_path;
    const key = supplied ? ["session", supplied] : nonempty(transcript) ? ["transcript", path.resolve(transcript)] : null;
    if (!key) {
        const error = new Error("OpenWolf needs a stable session key: pass session_id or transcript_path, or set OPENWOLF_SESSION_ID once when launching the agent (node .wolf/hooks/session-id.js --new). Shared _session.json fallback is disabled.");
        error.code = "OPENWOLF_SESSION_ID_REQUIRED";
        throw error;
    }
    const host = process.env.HOSTNAME || os.hostname();
    const subagent = input.agent_id || input.subagent_id || input.agent_transcript_path || process.env.OPENWOLF_AGENT_ID || "";
    const route = crypto.createHash("sha256").update(JSON.stringify([host, agent, subagent, key])).digest("hex");
    const directory = path.join(wolfDir, "hooks", "session-identities");
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, `${route}.json`);
    const identity = withFileLock(file + ".lock", HOOK_LOCK_BUDGET_MS, () => {
        let previous;
        try { previous = JSON.parse(fs.readFileSync(file, "utf8")); } catch { }
        if (!newSession && validId.test(previous?.id)) {
            let state;
            try { state = JSON.parse(fs.readFileSync(path.join(wolfDir, "hooks", "sessions", `${previous.id}.json`), "utf8")); } catch { }
            const expired = !state && fs.existsSync(sessionArtifactPath(wolfDir, "session-ledgers", previous.id, "json"));
            if (!startSource || (!expired && !(startSource === "startup" && state?.ended_at)))
                return previous;
        }
        const created = {
            id: newSessionId(), hostname: host, agent,
            harness_session_id: supplied || null, agent_id: subagent || null,
            started: new Date().toISOString(),
        };
        const tmp = `${file}.${crypto.randomUUID()}.tmp`;
        try {
            fs.writeFileSync(tmp, JSON.stringify(created, null, 2), "utf8");
            fs.renameSync(tmp, file);
        } finally {
            fs.rmSync(tmp, { force: true });
        }
        return created;
    });
    if (!identity)
        throw new Error("OpenWolf session identity is busy; retry this hook");
    return identity;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    if (process.argv.includes("--new")) {
        console.log(newSessionId());
    } else {
        const root = process.env.CLAUDE_PROJECT_DIR || process.env.CODEX_PROJECT_ROOT ||
            process.env.OPENWOLF_PROJECT_ROOT || path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
        const index = process.argv.indexOf("--session-id");
        const input = index < 0 ? {} : { session_id: process.argv[index + 1] };
        console.log(JSON.stringify(resolveSessionIdentity(path.join(root, ".wolf"), agentHarness(), input)));
    }
}
