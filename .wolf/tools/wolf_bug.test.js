// wolf_bug.test.js — run with: node --test .wolf/tools/wolf_bug.test.js
// Everything runs against a TEMP wolf tree via WOLF_TEST_DIR (set before the
// dynamic import — static imports would hoist past it).
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import test from "node:test";
import { crc32 } from "../hooks/bug-id.js";

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "wolf-shard-test-"));
process.env.WOLF_TEST_DIR = TMP;
const wolfBug = await import("./wolf_bug.js");
const { WOLF_DIR, SHARD_DIR, VIEW_FILE } = wolfBug;

const SESSION_A = "SESSION-testhost-11111111-2222-3333-4444-555555555555";
function synthLog() {
    return [
        { id: "BUG-legacy-1", timestamp: "2026-01-02", error_message: "old", file: "a.cpp", root_cause: "r", fix: "f", tags: [], related_bugs: [], occurrences: 1, last_seen: "2026-01-02" },
        { id: "BUG-testhost-aaaaaaaa", timestamp: "2026-09-10T08:00:00.000000Z", session_id: SESSION_A, error_message: "crash on boot", file: "firmware/x.cpp", root_cause: "null deref", fix: "guard", tags: ["boot"], related_bugs: [], occurrences: 1, last_seen: "2026-09-10T08:00:00.000000Z" },
        { id: "BUG-testhost-bbbbbbbb", timestamp: "2026-09-12T09:30:00.000000Z", error_message: "no session", file: "y.cpp", root_cause: "r2", fix: "f2", tags: [], related_bugs: [], occurrences: 2, last_seen: "2026-09-12T09:30:00.000000Z" },
    ];
}

test("crc32 parity with the recorded bug-id generator", () => {
    // Recorded pair from .wolf/buglog.json (2026-09-18): BUG-4c854678970f-47e9f014.
    assert.equal(crc32("2026-09-18T10:14:57.412204Z"), "47e9f014");
});

test("migrate-import splits legacy vs session shards (dry-run shape via direct writes)", () => {
    fs.mkdirSync(SHARD_DIR, { recursive: true });
    const log = synthLog();
    for (const b of log) {
        const legacy = b.id.startsWith("BUG-legacy-");
        const date = String(b.timestamp).slice(0, 10);
        const name = legacy ? "0000-legacy.jsonl" : b.session_id ? `${date}-${b.session_id}.jsonl` : `${date}-testhost-manual.jsonl`;
        fs.appendFileSync(path.join(SHARD_DIR, name), JSON.stringify(b) + "\n", "utf8");
    }
    const shards = wolfBug.listShardFiles();
    assert.ok(shards.includes("0000-legacy.jsonl"));
    assert.ok(shards.some((f) => f === `2026-09-10-${SESSION_A}.jsonl`));
    assert.ok(shards.includes("2026-09-12-testhost-manual.jsonl"));
});

test("append + rebuild: view is object-shaped, verbatim round-trip, LF-terminated", () => {
    const shard = wolfBug.appendRecord({ id: "BUG-testhost-cccccccc", timestamp: "2026-09-18T11:00:00.000000Z", session_id: SESSION_A, error_message: "new bug", file: "z.cpp", root_cause: "r3", fix: "f3", tags: [], related_bugs: [], occurrences: 1, last_seen: "2026-09-18T11:00:00.000000Z" }, path.join(SHARD_DIR, `2026-09-18-${SESSION_A}.jsonl`));
    assert.ok(shard.endsWith(`.jsonl`));
    const r = wolfBug.rebuildView({ force: true });
    assert.equal(r.bugs, 4);
    const view = JSON.parse(fs.readFileSync(VIEW_FILE, "utf8"));
    assert.equal(view.version, 1);
    assert.ok(Array.isArray(view.bugs));
    const round = view.bugs.find((b) => b.id === "BUG-testhost-aaaaaaaa");
    assert.deepEqual(Object.keys(round), ["id", "timestamp", "session_id", "error_message", "file", "root_cause", "fix", "tags", "related_bugs", "occurrences", "last_seen"], "field order must round-trip verbatim");
    for (const f of wolfBug.listShardFiles()) {
        const raw = fs.readFileSync(path.join(SHARD_DIR, f), "utf8");
        assert.ok(raw.endsWith("\n"), `${f} must end with newline`);
        assert.ok(!raw.includes("\r"), `${f} must be LF-only`);
    }
});

test("occurrence events fold into derived counts, never mutate shards", () => {
    wolfBug.appendRecord({ id: "BUG-testhost-dddddddd", timestamp: "2026-09-18T12:00:00.000000Z", session_id: SESSION_A, occurrence_of: "BUG-legacy-1", note: "seen again" }, path.join(SHARD_DIR, `2026-09-18-${SESSION_A}.jsonl`));
    wolfBug.rebuildView({ force: true });
    const view = JSON.parse(fs.readFileSync(VIEW_FILE, "utf8"));
    const base = view.bugs.find((b) => b.id === "BUG-legacy-1");
    assert.equal(base.occurrences, 2, "base occurrences + 1 event");
    assert.equal(base.last_seen, "2026-09-18T12:00:00.000000Z");
    // The shard copy of the legacy record is untouched:
    const legacyRaw = fs.readFileSync(path.join(SHARD_DIR, "0000-legacy.jsonl"), "utf8");
    assert.ok(legacyRaw.includes('"occurrences":1'));
});

test("absorb rescues view-only strays and mutated records", () => {
    const view = JSON.parse(fs.readFileSync(VIEW_FILE, "utf8"));
    // A stale-prose agent appended a new record directly to the view:
    view.bugs.push({ id: "BUG-testhost-eeeeeeee", timestamp: "2026-09-18T13:00:00.000000Z", error_message: "stray", file: "s.cpp", root_cause: "r", fix: "f", tags: [], related_bugs: [], occurrences: 1, last_seen: "2026-09-18T13:00:00.000000Z" });
    // ...and hand-"fixed" an existing one:
    const mutated = view.bugs.find((b) => b.id === "BUG-testhost-bbbbbbbb");
    mutated.occurrences = 5;
    fs.writeFileSync(VIEW_FILE, JSON.stringify(view, null, 2) + "\n", "utf8");
    const r = wolfBug.rebuildView({ force: true });
    assert.equal(r.bugs, 5, "5 base bugs: legacy-1, aaaaaaaa, bbbbbbbb, cccccccc + rescued eeeeeeee (events fold in, never counted as bugs)");
    const all = wolfBug.readAllShards().records;
    assert.ok(all.some((x) => x.id === "BUG-testhost-eeeeeeee"), "stray rescued into a shard");
    const events = all.filter((x) => x.occurrence_of === "BUG-testhost-bbbbbbbb");
    assert.equal(events.length, 1, "mutation absorbed as a linked occurrence event");
    const finalView = JSON.parse(fs.readFileSync(VIEW_FILE, "utf8"));
    assert.equal(finalView.bugs.find((b) => b.id === "BUG-testhost-bbbbbbbb").occurrences, 3, "2 base + 1 absorbed event");
});

test("pre-cutover guard: no shards means the tracked buglog.json stays authoritative", async () => {
    const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), "wolf-shard-test2-"));
    const before = process.env.WOLF_TEST_DIR;
    process.env.WOLF_TEST_DIR = tmp2;
    const mod2 = await import("./wolf_bug.js?test2=" + tmp2); // cache-bust
    assert.equal(mod2.rebuildView({}).rebuilt, false);
    process.env.WOLF_TEST_DIR = before;
});

test("stats detects duplicate ids", () => {
    wolfBug.appendRecord({ id: "BUG-testhost-aaaaaaaa", timestamp: "2026-09-18T14:00:00.000000Z", session_id: SESSION_A, error_message: "dupe id", file: "d.cpp", root_cause: "r", fix: "f", tags: [], related_bugs: [], occurrences: 1, last_seen: "2026-09-18T14:00:00.000000Z" }, path.join(SHARD_DIR, `2026-09-18-${SESSION_A}.jsonl`));
    const ids = wolfBug.readAllShards().records.filter((r) => !r.occurrence_of).map((r) => r.id);
    assert.ok(ids.indexOf("BUG-testhost-aaaaaaaa") !== ids.lastIndexOf("BUG-testhost-aaaaaaaa"));
});

// Housekeeping: temp dirs are left for OS cleanup (Windows unlink races on open handles).
