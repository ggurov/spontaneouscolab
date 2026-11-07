import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

/** Standard IEEE CRC32 of the exact UTF-8 timestamp, as eight lowercase hex digits. */
export function crc32(text) {
    let crc = 0xffffffff;
    for (const byte of Buffer.from(text, "utf8")) {
        crc ^= byte;
        for (let bit = 0; bit < 8; bit++)
            crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
    return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
}

/** UTC ISO 8601 wall-clock time with six fractional digits. */
export function utcTimestamp() {
    const micros = BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1000));
    const millis = Number(micros / 1000n);
    const fraction = String(micros % 1000n).padStart(3, "0");
    return new Date(millis).toISOString().replace("Z", `${fraction}Z`);
}

export function createBugIdentity(existingIds = new Set(), {
    host = process.env.HOSTNAME || os.hostname(),
    timestamp = utcTimestamp,
} = {}) {
    for (let attempt = 0; attempt < 1000; attempt++) {
        const dateTime = timestamp();
        const id = `BUG-${host}-${crc32(dateTime)}`;
        if (!existingIds.has(id))
            return { id, timestamp: dateTime };
    }
    throw new Error("Bug ID collision: generate again with a fresh UTC timestamp");
}

// Manual authoring uses the same generator as the post-write hook, from any cwd.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    let bugs = [];
    try {
        const log = JSON.parse(fs.readFileSync(new URL("../buglog.json", import.meta.url), "utf8"));
        bugs = Array.isArray(log) ? log : log.bugs;
    }
    catch (error) {
        if (error.code !== "ENOENT")
            throw error;
    }
    console.log(JSON.stringify(createBugIdentity(new Set(bugs.map(bug => bug.id)))));
}
