import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { crc32, createBugIdentity, utcTimestamp } from "./bug-id.js";

test("IEEE CRC32 uses unsigned, zero-padded hexadecimal", () => {
    assert.equal(crc32(""), "00000000");
    assert.equal(crc32("123456789"), "cbf43926");
    assert.equal(crc32("hello"), "3610a686");
});

test("IDs include the host and checksum of the stored timestamp", () => {
    const timestamp = "2026-09-15T12:34:56.123456Z";
    const first = createBugIdentity(new Set(), { host: "machine-one", timestamp: () => timestamp });
    const second = createBugIdentity(new Set(), { host: "machine-two", timestamp: () => timestamp });
    assert.deepEqual(first, { id: `BUG-machine-one-${crc32(timestamp)}`, timestamp });
    assert.notEqual(first.id, second.id);
});

test("a local collision retries with a fresh timestamp", () => {
    const oldTime = "2026-09-15T12:34:56.123456Z";
    const newTime = "2026-09-15T12:34:56.123457Z";
    const existing = new Set([`BUG-dev-host-${crc32(oldTime)}`, "BUG-legacy-001"]);
    const times = [oldTime, newTime];
    assert.deepEqual(createBugIdentity(existing, { host: "dev-host", timestamp: () => times.shift() }), {
        id: `BUG-dev-host-${crc32(newTime)}`, timestamp: newTime,
    });
    assert.equal(existing.size, 2);
});

test("an unchanging colliding timestamp fails without an infinite loop", () => {
    assert.throws(() => createBugIdentity(new Set([`BUG-dev-${crc32("same")}`]), {
        host: "dev", timestamp: () => "same",
    }), /fresh UTC timestamp/);
});

test("default timestamps use UTC microseconds and current wall-clock time", () => {
    const before = Date.now();
    const timestamp = utcTimestamp();
    assert.match(timestamp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
    assert.ok(Math.abs(Date.parse(timestamp) - before) < 1000);
});

test("rapid creation preserves unique IDs in a sparse legacy log", () => {
    const ids = new Set(["BUG-legacy-001", "BUG-legacy-797"]);
    for (let i = 0; i < 1000; i++) {
        const identity = createBugIdentity(ids);
        assert.equal(ids.has(identity.id), false);
        ids.add(identity.id);
    }
    assert.equal(ids.size, 1002);
});

test("manual generation uses HOSTNAME and falls back to the OS hostname", () => {
    const script = fileURLToPath(new URL("./bug-id.js", import.meta.url));
    for (const host of ["test-machine", ""]) {
        const result = spawnSync(process.execPath, [script], {
            env: { ...process.env, HOSTNAME: host }, encoding: "utf8", cwd: os.tmpdir(),
        });
        assert.equal(result.status, 0, result.stderr);
        const identity = JSON.parse(result.stdout);
        assert.equal(identity.id, `BUG-${host || os.hostname()}-${crc32(identity.timestamp)}`);
    }
});

test("post-write automatically logs the shared host/timestamp identity", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "wolf-bug-id-test-"));
    try {
        const wolf = path.join(root, ".wolf");
        fs.mkdirSync(wolf);
        fs.writeFileSync(path.join(wolf, "config.json"), JSON.stringify({
            openwolf: { buglog: { auto_detect: true } },
        }));
        fs.writeFileSync(path.join(wolf, "buglog.json"), "[]");
        const file = path.join(root, "sample.js");
        fs.writeFileSync(file, "const value = obj?.value;\n");
        const script = fileURLToPath(new URL("./post-write.js", import.meta.url));
        const result = spawnSync(process.execPath, [script], {
            cwd: root, encoding: "utf8",
            env: { ...process.env, CLAUDE_PROJECT_DIR: root, HOSTNAME: "hook-test-host" },
            input: JSON.stringify({
                session_id: "bug-id-test", tool_name: "Edit",
                tool_input: { file_path: file, old_string: "obj.value", new_string: "obj?.value" },
            }),
        });
        assert.equal(result.status, 0, result.stderr);
        const log = JSON.parse(fs.readFileSync(path.join(wolf, "buglog.json"), "utf8"));
        assert.equal(log.bugs.length, 1);
        const bug = log.bugs[0];
        assert.equal(bug.id, `BUG-hook-test-host-${crc32(bug.timestamp)}`);
        assert.match(bug.session_id, /^SESSION-hook-test-host-/);
        assert.equal(bug.file, "sample.js");
    }
    finally {
        fs.rmSync(root, { recursive: true, force: true });
    }
});
