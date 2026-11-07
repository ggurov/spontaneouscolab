import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { resolveSessionIdentity, newSessionId } from "./session-id.js";
import { countSemanticEntries, getSessionMemoryPath, getSessionHandoffPath } from "./shared.js";
import { emptyLedger, flushSessionToLedger, readCombinedLedger } from "./ledger.js";

const scripts = fileURLToPath(new URL(".", import.meta.url));
function fixture(t, autoDetect = false) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "wolf-session-test-"));
    const wolf = path.join(root, ".wolf");
    fs.mkdirSync(wolf);
    fs.writeFileSync(path.join(wolf, "config.json"), JSON.stringify({ openwolf: { buglog: { auto_detect: autoDetect } } }));
    fs.writeFileSync(path.join(wolf, "memory.md"), "# Historical memory\n");
    fs.writeFileSync(path.join(wolf, "STATUS.md"), "# Project status\n\n## 🚀 Next quest\n\n- Shared project objective\n");
    fs.writeFileSync(path.join(wolf, "buglog.json"), "[]");
    const history = emptyLedger();
    history.lifetime.total_sessions = 10;
    history.lifetime.total_tokens_estimated = 100;
    fs.writeFileSync(path.join(wolf, "token-ledger.json"), JSON.stringify(history));
    t.after(() => fs.rmSync(root, { recursive: true, force: true }));
    return { root, wolf };
}
function options(root, input, host = "test-machine") {
    return {
        cwd: root, encoding: "utf8", input: JSON.stringify(input),
        env: {
            ...process.env, CLAUDE_PROJECT_DIR: root, HOSTNAME: host,
            OPENWOLF_SESSION_ID: "", CODEX_THREAD_ID: "", OPENWOLF_AGENT_ID: "",
        },
    };
}
function hook(root, script, input, host) {
    const result = spawnSync(process.execPath, [path.join(scripts, `${script}.js`)], options(root, input, host));
    assert.equal(result.status, 0, result.stderr);
    return result;
}
function concurrentHook(root, script, input, host) {
    return new Promise((resolve, reject) => {
        const opts = options(root, input, host);
        const child = spawn(process.execPath, [path.join(scripts, `${script}.js`)], opts);
        let stderr = "";
        child.stdout.resume();
        child.stderr.on("data", data => stderr += data);
        child.on("error", reject);
        child.on("close", code => code === 0 ? resolve() : reject(new Error(stderr)));
        child.stdin.end(opts.input);
    });
}
function identities(wolf) {
    const directory = path.join(wolf, "hooks", "session-identities");
    return fs.readdirSync(directory).filter(file => file.endsWith(".json")).map(file => JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")));
}
function state(wolf, id) {
    return path.join(wolf, "hooks", "sessions", `${id}.json`);
}

test("generated IDs remain unique when wall-clock time is identical", () => {
    const originalNow = Date.now;
    Date.now = () => 0;
    try {
        const ids = new Set(Array.from({ length: 1000 }, () => newSessionId()));
        assert.equal(ids.size, 1000);
        for (const id of ids) assert.match(id, /^SESSION-[\w.-]+-[0-9a-f-]{36}$/);
    } finally { Date.now = originalNow; }
});

test("routing survives separate processes and unsafe harness IDs never become filenames", t => {
    const { root, wolf } = fixture(t);
    const input = { session_id: "../../unsafe/session key" };
    hook(root, "session-start", input);
    const first = identities(wolf)[0];
    hook(root, "session-start", input);
    assert.equal(identities(wolf)[0].id, first.id);
    assert.ok(fs.existsSync(state(wolf, first.id)));
    assert.equal(fs.readdirSync(path.join(wolf, "session-ledgers")).length, 1);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 11);
});

test("different hosts and subagents sharing a parent key receive independent identities", t => {
    const { root, wolf } = fixture(t);
    for (const [host, agent] of [["host-one", "a"], ["host-one", "b"], ["host-two", "a"]])
        hook(root, "session-start", { session_id: "parent-session", agent_id: agent }, host);
    assert.equal(new Set(identities(wolf).map(identity => identity.id)).size, 3);
    assert.equal(readCombinedLedger(wolf).sessions.length, 3);
});

test("transcript-only hooks reuse a stable identity and distinct transcripts stay isolated", t => {
    const { root, wolf } = fixture(t);
    const first = { transcript_path: path.join(root, "first.jsonl") };
    hook(root, "session-start", first);
    hook(root, "precompact", first);
    hook(root, "session-start", { ...first, source: "compact" });
    hook(root, "session-start", { transcript_path: path.join(root, "second.jsonl") });
    assert.equal(identities(wolf).length, 2);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 12);
});

test("hooks lacking any stable key report the launcher requirement and never share state", t => {
    const { root, wolf } = fixture(t);
    const result = hook(root, "session-start", {});
    assert.match(result.stdout, /OPENWOLF_SESSION_ID/);
    assert.equal(fs.existsSync(path.join(wolf, "hooks", "_session.json")), false);
    assert.equal(fs.existsSync(path.join(wolf, "hooks", "sessions")), false);
});

test("explicit launcher tokens persist across hook processes", t => {
    const { root, wolf } = fixture(t);
    const opts = options(root, {});
    opts.env.OPENWOLF_SESSION_ID = "launcher-session-token";
    for (const script of ["session-start", "precompact", "session-start"]) {
        const result = spawnSync(process.execPath, [path.join(scripts, `${script}.js`)], opts);
        assert.equal(result.status, 0, result.stderr);
    }
    assert.equal(identities(wolf).length, 1);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 11);
});

test("standalone session-ID lookup reuses the same routing identity as hooks", t => {
    const { root, wolf } = fixture(t);
    const result = spawnSync(process.execPath, [path.join(scripts, "session-id.js"), "--session-id", "cli-session"], options(root, {}));
    assert.equal(result.status, 0, result.stderr);
    const identity = JSON.parse(result.stdout);
    hook(root, "session-start", { session_id: "cli-session" });
    assert.equal(identities(wolf)[0].id, identity.id);
    const created = spawnSync(process.execPath, [path.join(scripts, "session-id.js"), "--new"], options(root, {}));
    assert.equal(created.status, 0, created.stderr);
    assert.match(created.stdout.trim(), /^SESSION-test-machine-[0-9a-f-]{36}$/);
});

test("resume and compaction preserve bookkeeping; clear generates a new identity", t => {
    const { root, wolf } = fixture(t);
    const input = { session_id: "stable-session" };
    hook(root, "session-start", input);
    const id = identities(wolf)[0].id;
    const saved = JSON.parse(fs.readFileSync(state(wolf, id), "utf8"));
    saved.files_read["sample.cpp"] = { tokens: 25, count: 1, anatomy_hit: true };
    saved.files_written.push({ file: "sample.cpp", tokens: 30, action: "edit" });
    fs.writeFileSync(state(wolf, id), JSON.stringify(saved));
    const handoff = getSessionHandoffPath(wolf, id);
    fs.mkdirSync(path.dirname(handoff));
    fs.writeFileSync(handoff, "# Handoff\n\n## 🚀 Next quest\n\n- Resume only this agent's objective\n");
    hook(root, "precompact", input);
    const compact = hook(root, "session-start", { ...input, source: "compact" });
    assert.match(compact.stdout, /Resume only this agent/);
    hook(root, "session-start", { ...input, source: "resume" });
    const resumed = JSON.parse(fs.readFileSync(state(wolf, id), "utf8"));
    assert.equal(resumed.files_read["sample.cpp"].compacted, true);
    assert.equal(resumed.files_written.length, 1);
    assert.ok(fs.existsSync(state(wolf, id).replace(/\.json$/, ".precompact.json")));
    hook(root, "session-start", { ...input, source: "clear" });
    assert.notEqual(identities(wolf)[0].id, id);
    assert.equal(readCombinedLedger(wolf).sessions.length, 2);
});

test("concurrent sessions retain separate state, memory, snapshots and ledgers", async t => {
    const { root, wolf } = fixture(t);
    const history = fs.readFileSync(path.join(wolf, "token-ledger.json"));
    const memory = fs.readFileSync(path.join(wolf, "memory.md"));
    const inputs = Array.from({ length: 12 }, (_, index) => ({ session_id: `agent-session-${index}` }));
    await Promise.all(inputs.map(input => concurrentHook(root, "session-start", input)));
    await Promise.all(inputs.map(input => concurrentHook(root, "precompact", input)));
    const all = identities(wolf);
    assert.equal(new Set(all.map(identity => identity.id)).size, 12);
    for (const identity of all) {
        assert.equal(JSON.parse(fs.readFileSync(state(wolf, identity.id))).session_id, identity.id);
        const snapshot = JSON.parse(fs.readFileSync(state(wolf, identity.id).replace(/\.json$/, ".precompact.json")));
        assert.equal(snapshot.session.session_id, identity.id);
        assert.match(fs.readFileSync(getSessionMemoryPath(wolf, identity.id), "utf8"), new RegExp(identity.id));
    }
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 22);
    assert.deepEqual(fs.readFileSync(path.join(wolf, "token-ledger.json")), history);
    assert.deepEqual(fs.readFileSync(path.join(wolf, "memory.md")), memory);
});

test("simultaneous startup hooks for the same session register exactly once", async t => {
    const { root, wolf } = fixture(t);
    await Promise.all(Array.from({ length: 8 }, () => concurrentHook(root, "session-start", { session_id: "same-agent" })));
    const all = identities(wolf);
    assert.equal(all.length, 1);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 11);
    const memory = fs.readFileSync(getSessionMemoryPath(wolf, all[0].id), "utf8");
    assert.equal(memory.match(/## Session:/g).length, 1);
});

test("ledger upserts replace cumulative usage once and reports preserve historical totals", t => {
    const { wolf } = fixture(t);
    const entry = { id: newSessionId(), started: "2026-09-15", totals: { input_tokens_estimated: 20, output_tokens_estimated: 0, reads_count: 1 } };
    flushSessionToLedger(wolf, entry);
    flushSessionToLedger(wolf, { ...entry, totals: { input_tokens_estimated: 30, output_tokens_estimated: 0, reads_count: 2 } });
    const combined = readCombinedLedger(wolf);
    assert.equal(combined.lifetime.total_sessions, 11);
    assert.equal(combined.lifetime.total_tokens_estimated, 130);
    assert.equal(combined.lifetime.total_reads, 2);
    assert.equal(combined.sessions.length, 1);
    assert.equal(combined.sessions[0].reads, undefined);
    assert.equal(combined.sessions[0].writes, undefined);
});

test("session end is idempotent and a new startup reusing its routing key gets a fresh ID", t => {
    const { root, wolf } = fixture(t);
    const input = { session_id: "reused-harness-key" };
    hook(root, "session-start", input);
    const id = identities(wolf)[0].id;
    const saved = JSON.parse(fs.readFileSync(state(wolf, id)));
    saved.files_written.push({ file: "sample.cpp", tokens: 30, action: "edit" });
    fs.writeFileSync(state(wolf, id), JSON.stringify(saved));
    hook(root, "session-end", input);
    hook(root, "session-end", input);
    assert.equal(fs.readFileSync(getSessionMemoryPath(wolf, id), "utf8").match(/Session end/g).length, 1);
    hook(root, "session-start", input);
    assert.notEqual(identities(wolf)[0].id, id);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 12);
});

test("expired runtime state cannot overwrite a previous session's ledger", t => {
    const { root, wolf } = fixture(t);
    const input = { session_id: "expired-session" };
    hook(root, "session-start", input);
    const id = identities(wolf)[0].id;
    const file = path.join(wolf, "session-ledgers", `${id}.json`);
    const original = fs.readFileSync(file);
    fs.unlinkSync(state(wolf, id));
    hook(root, "session-start", { ...input, source: "resume" });
    assert.notEqual(identities(wolf)[0].id, id);
    assert.deepEqual(fs.readFileSync(file), original);
    assert.equal(readCombinedLedger(wolf).lifetime.total_sessions, 12);
});

test("summary detection counts only the current session's semantic entries", t => {
    const { wolf } = fixture(t);
    const ids = [newSessionId(), newSessionId()];
    fs.mkdirSync(path.join(wolf, "session-memory"));
    fs.writeFileSync(getSessionMemoryPath(wolf, ids[0]), "## Session: first\n| 12:00 | Fixed a defect | sample.cpp | passed | 10 |\n");
    fs.writeFileSync(getSessionMemoryPath(wolf, ids[1]), `## Session: second\n| 12:00 | Session end ${ids[1]}: 2 writes | 0 reads | ~10 tok |\n`);
    assert.equal(countSemanticEntries(wolf, ids[0]), 1);
    assert.equal(countSemanticEntries(wolf, ids[1]), 0);
});

test("read-only mode prevents identity and session artifacts from being written", t => {
    const { root, wolf } = fixture(t);
    fs.writeFileSync(path.join(root, ".wolf-disable"), "");
    hook(root, "session-start", { session_id: "read-only-agent" });
    assert.equal(fs.existsSync(path.join(wolf, "hooks")), false);
    assert.throws(() => resolveSessionIdentity(wolf, "test", { session_id: "read-only-agent" }), /read-only/);
});

test("concurrent bug-fix hooks retain all independent additions", async t => {
    const { root, wolf } = fixture(t, true);
    const inputs = Array.from({ length: 12 }, (_, index) => {
        const file = path.join(root, `sample-${index}.js`);
        fs.writeFileSync(file, "const value = obj?.value;\n");
        return {
            session_id: `bug-agent-${index}`, tool_name: "Edit",
            tool_input: { file_path: file, old_string: "obj.value", new_string: "obj?.value" },
        };
    });
    await Promise.all(inputs.map(input => concurrentHook(root, "post-write", input)));
    const raw = JSON.parse(fs.readFileSync(path.join(wolf, "buglog.json")));
    const bugs = Array.isArray(raw) ? raw : raw.bugs;
    assert.equal(bugs.length, 12);
    assert.equal(new Set(bugs.map(bug => bug.id)).size, 12);
});
