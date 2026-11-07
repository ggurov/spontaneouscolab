# Repository Guidelines

## Project Overview

Agent Relay is a single-file, zero-dependency HTTP message bus for agent-to-agent
collaboration. A caller creates a channel with `POST /channels`; the returned
`/c/<token>` URL is both the credential and the documentation. Anyone holding
that URL can read and post; channels are isolated by 128-bit random tokens.
Typical use: a hardware-in-the-loop rig agent and a firmware agent exchange
requirements and results without humans copy/pasting chat messages.

## Architecture & Data Flow

`relay.py` has three layers:

1. **State** (`ApiError`, `Channel`, `Relay`): an in-memory dict of channels.
   Each channel owns a monotonic `seq`, a bounded `deque` ring buffer, a
   participant registry, waiter count, and a `threading.Condition` that shares
   the relay-wide `RLock`. TTL expiry is lazy on access plus a background GC
   thread.
2. **Documentation** (`SERVICE_GUIDE`, `CHANNEL_GUIDE`, `channel_guide`,
   `channel_guide_json`): Markdown and JSON self-descriptions rendered with the
   request's base URL, so a bare channel URL teaches the full protocol.
3. **HTTP** (`RelayHandler`, `RelayHTTPServer`, `make_server`, `main`): a
   `ThreadingHTTPServer` with manual path/query routing, body validation, long-
   poll, and server-sent events.

Request flow: route path/query → validate token under `relay.lock` → parse and
validate body/params → mutate or snapshot `Relay` under the lock → send JSON or
Markdown. A post appends to the ring buffer, increments `seq`, updates the
sender's participant record, and `notify_all()`s the channel condition. Readers
block until `seq > since` or `wait` expires. SSE holds the connection thread,
writes `id:`/`data:` frames, and pings every 15 s. `seq` is the cursor; delivery
is at-least-once; `since` older than the retained range returns
`truncated: true`.

```
client ──POST /channels──────────────► Relay.create_channel ──► token + URL
client ──POST /c/<token>/messages───► validate ──► Channel.messages (deque)
                                                     │ notify_all
peer   ──GET  /c/<token>/messages───► Relay.wait ◄────┘ (long-poll, cursor=seq)
peer   ──GET  /c/<token>/events─────► SSE loop (history + live, 15 s pings)
```

## Key Directories

```
.                  # service, docs, and build files (flat by design)
tests/             # pytest end-to-end suite over the real HTTP surface
scripts/           # live smoke test
```

There is no package layout, plugin system, or database; everything the service
needs is in `relay.py`.

## Agent Memory (epicwolf)

The repo carries a vendored `.wolf/` memory tree and `.claude/` hooks from
[ggurov/epicwolf](https://github.com/ggurov/epicwolf) (AGPL-3.0):

- Read `.wolf/SHARDS.md` before writing project memory; it is the protocol of
  record.
- `node .wolf/tools/bootstrap.js` rebuilds the derived buglog view; run it once
  per clone and at session start.
- Log real defects with `node .wolf/tools/wolf_bug.js append …`, search with
  `… search`, and lint cerebrum edits with `node .wolf/tools/wolf_guard.js`.
- Keep `.wolf/STATUS.md` to its two sections; finished milestones go under
  `.wolf/status/done/`, and substantial sessions end with a handoff.
- Derived files (`buglog.json`, `anatomy.md`, `anatomy-index.json`,
  `hooks/sessions/`) are machine-local and gitignored; never commit them.
- `.wolf-disable` at the repo root silences all hooks for read-only work.
- Hooks are wired for Claude Code (`.claude/settings.json`); other harnesses can
  call the same `.wolf/hooks/*.js` scripts or update the tree manually.

## Development Commands

```bash
make run                    # python3 relay.py --host 127.0.0.1 --port 8787
make test                   # pytest -q (49 end-to-end tests)
make smoke                  # bash scripts/smoke.sh on SMOKE_PORT=8791
python3 -m py_compile relay.py   # fast syntax check
PORT=9000 make run          # alternate port
make run HOST=0.0.0.0       # expose widely only on a trusted network
python3 relay.py --help     # all flags, defaults, and env vars
docker compose up -d --build   # build + run on :8787 (RELAY_PORT to override)
docker compose logs -f relay
docker compose down
```

`make run` binds `$(HOST)` (default `127.0.0.1`); `make smoke` uses
`$(SMOKE_PORT)` (default 8791), so it does not collide with `make run`.

`make test` invokes the `pytest` executable rather than `python3 -m pytest`:
the `python3` on PATH may not have pytest installed, while the standalone
`pytest` entry point does. The service itself runs on Python 3.9+ with no
third-party packages.

## Code Conventions & Common Patterns

- **Stdlib only, one file.** Do not introduce runtime dependencies, frameworks,
  or a package split without a concrete reason. Match the existing section
  banner comments and ordering: helpers → state → guides → HTTP → wiring/CLI.
- **Formatting and naming.** 4-space indent, double quotes, type hints with
  `from __future__ import annotations`, module constants in `UPPER_SNAKE`
  (`MAX_WAIT`, `SSE_HEARTBEAT`), private helpers prefixed `_`, public state
  classes `Channel`/`Relay`.
- **HTTP responses go through `_send` / `_send_json`.** They set CORS,
  `Cache-Control: no-store`, and `Content-Length` (except 204), skip bodies for
  HEAD, and guard double-sends via `_headers_sent` (reset per request in
  `handle_one_request`). Raise `ApiError(status, message, headers?)` from
  handlers; `_dispatch` converts it to `{"error": ...}` and maps unexpected
  exceptions to 500 without leaking internals.
- **Locking discipline.** All channel state is guarded by `relay.lock`; each
  channel condition uses that same lock. Never hold the lock across blocking
  I/O: long-poll uses `cond.wait`, SSE snapshots under the lock and writes
  outside it. Blocking work in a post handler must stay short.
- **Validation and limits.** Reuse `_clean_name` (1–64 printable characters for
  `sender`, `to`, join `name`, `role`), `_int_query`, `_wait_query`, and the
  `MAX_*` constants. Oversized bodies are drained up to 8 MiB where possible
  and rejected with JSON 413; oversized message text/data is rejected before
  mutation.
- **Wire schema is stable.** Messages always carry `seq`, `ts`, `sender`,
  `kind`, `text`, `to`, `reply_to`, `data`. `to` is addressing, not access
  control; all URL holders can read everything. A reader using `as=<name>` sees
  broadcasts plus messages addressed to it plus its own messages, even when its
  own message is addressed elsewhere. Cursors past `latest_seq` are clamped to
  the head; an empty filtered read also returns the head as `cursor`, so `as`
  readers always advance. Extend with new `kind` values rather than changing or
  reusing fields.
- **Self-documenting behavior is part of the API.** Any endpoint, limit, flag,
  or semantic change must update `SERVICE_GUIDE`, `CHANNEL_GUIDE`, their JSON
  variants, and `README.md`. Guides use `%`-formatting with `%(key)s`
  placeholders because the templates contain literal JSON braces.
- **Capability-URL hygiene.** Never log full tokens (`mask_tokens`), never add
  channel listings, keep `no-store`, and require `--admin-token` for creation
  only when the deployment is exposed beyond a trusted network. The URL also
  authorizes `DELETE`, which is the only revocation path.
- **License.** The repository is AGPL-3.0 (`LICENSE`); `.wolf/` and `.claude/`
  are vendored epicwolf files under the same license. Keep attribution intact.

## Important Files

| Path | What it is |
| --- | --- |
| `relay.py` | Entire service: state, guides, HTTP handler, SSE, CLI |
| `tests/test_relay.py` | 49 end-to-end tests over ephemeral real servers |
| `tests/conftest.py` | Adds repo root to `sys.path` so `import relay` works |
| `scripts/smoke.sh` | Curl+python3 smoke test; starts its own server on `PORT` (set from `SMOKE_PORT`) |
| `Makefile` | `run`, `test`, `smoke` targets and overridable variables |
| `README.md` | User-facing quick start, protocol table, security/deployment |
| `problem.txt` | Original requirements (traceability, not used at runtime) |
| `Dockerfile` | Non-root, read-only runtime image (stdlib Python, port 8787) |
| `compose.yaml` | Deployment defaults, healthcheck, capability/read-only hardening |
| `DEPLOY.md` | Build, run, TLS proxy, operations, and branch-protection runbook |
| `CONTRIBUTING.md` | PR → CI → review → squash-merge policy |
| `.github/workflows/ci.yml` | `test` and `docker` required checks |
| `.wolf/`, `.claude/` | Vendored epicwolf memory tree and hooks (AGPL-3.0) |
| `LICENSE` | AGPL-3.0 |

Key entry points: `main()` for the process, `make_server()` for tests/embedding,
`RelayHandler._handle_get/_handle_post/_handle_delete` for routing, and
`Relay.wait()` for the receive path.

## Runtime/Tooling Preferences

- **Python 3.9+**, standard library only; no requirements file, virtualenv, or
  package manager is needed for runtime. `pytest` is the only dev tool.
- **HTTP/1.1** with keep-alive and one thread per connection; long-poll is
  capped at 60 s and the handler socket timeout is 120 s. Bodies require
  `Content-Length`; chunked uploads are not supported. `TCP_NODELAY` is on
  (`disable_nagle_algorithm`) to avoid ~40 ms keep-alive stalls; SSE honors
  `Last-Event-ID` and emits `retry: 3000`.
- **State is in memory.** A restart forgets all channels; channels expire after
  `--channel-ttl-hours` without a new message (reads do not refresh the clock)
  and are capped at `--max-messages` retained messages. Do not assume
  persistence.
- **Configuration is flags plus environment.** `--host` (127.0.0.1),
  `--port` (8787), `--public-url`, `--max-messages` (2000),
  `--channel-ttl-hours` (168; 0 disables expiry), `--max-channels` (10000),
  `--admin-token` (or `RELAY_ADMIN_TOKEN`; the flag wins), `--log-level`
  (invalid values fall back to `info`). `RELAY_REVISION` reports the build.
  `RELAY_PUBLIC_URL` pins generated links to an origin including a path prefix;
  otherwise `X-Forwarded-Prefix` is honored. `--restore-channel
  TOKEN[:NAME[:NEXT_SEQ]]` (or comma-separated `RELAY_RESTORE_CHANNELS`)
  recreates known channel URLs at startup; messages are not persisted, so
  resumed readers see `truncated:true` until their cursor reaches the restore
  point. There is no config file.
- **TLS and path prefixes are handled outside the process.** The service honors
  `X-Forwarded-Proto`/`X-Forwarded-Host`/`X-Forwarded-Prefix`, or use
  `--public-url`/`RELAY_PUBLIC_URL`; reverse proxies must disable buffering for
  SSE (`proxy_buffering off`).
- **Guide URLs derive from headers when `--public-url`/`RELAY_PUBLIC_URL` is
  unset** (`X-Forwarded-Proto`, `X-Forwarded-Host`, then `Host`, plus
  `X-Forwarded-Prefix`). A hostile `Host` header can poison generated links, so
  pin the public URL behind a proxy.
- **Docker is the deployment path** (`Dockerfile` + `compose.yaml`; port 8787).
  Node.js is needed only for the vendored epicwolf hooks, not the service.

## Testing & QA

- Run `make test` for the full suite; it binds `127.0.0.1:0`, so parallel runs
  are safe. Run `make smoke` for a curl-only scenario against a live process on
  `PORT` (set from `SMOKE_PORT`, default 8791).
- Tests are black-box and function-scoped: `start_server` builds a real
  `Relay` + `ThreadingHTTPServer` on `127.0.0.1:0`, and `server` is the default
  configuration (`max_messages=500`, `ttl_seconds=3600`). Do not inspect or
  mutate `server.relay`; drive every assertion over HTTP. Build URLs with
  `server.base` + `path_of(url)`, keep one server/channel set per test, and
  close SSE connections in `finally`.
- The suite proves: capability isolation (wrong/absent token → 404), cursor and
  truncation semantics, stale-cursor clamping, restore gaps, ring-buffer
  eviction, long-poll wakeup/timeout/waiter counts, 503 drain, waiter release on
  client disconnect and channel deletion, SSE history/live delivery, SSE drain
  events, `as` filtering, `Last-Event-ID` resume and SSE cursor clamping, tail
  reads (`last=N`), `to` addressing and sender visibility,
  join/presence, reply/data round-trip, TTL expiry, capacity limits,
  forwarded-header link generation, plain-text and JSON-sniffed posts, JSON error
  positions, clean oversized-body 413s, auto-named channels, per-path `Allow`,
  build revision reporting, admin-token gating,
  delete, HEAD/OPTIONS/health, and that the guides reflect effective limits.
- Add tests at the HTTP boundary with the existing helpers (`start_server`,
  `api`, `post`, `read`, `join`, `sse`, `read_sse_event`, `wait_for_waiters`);
  assert both status code and payload. For timing-sensitive cases follow the
  existing pattern: background thread + `wait_for_waiters` (status polling) +
  generous join timeouts.
- No coverage tool, linter, or formatter is configured. Keep `py_compile`
  clean and match surrounding style.
- Known gaps: the background `gc_loop` thread, TLS/proxy termination, and
  persistence (which does not exist) are not tested.
- Manual verification pattern: create a channel, `join`, start a long-poll in
  the background, post from another connection, and confirm the poll returns
  immediately — the smoke script covers the same path with curl.
- CI (`.github/workflows/ci.yml`) runs `test` (pytest + smoke) and `docker`
  (image build + container health probe) on every PR. `main` is
  ruleset-protected: PR required, green checks, resolved threads, linear
  history, squash merges only; see `CONTRIBUTING.md`.
