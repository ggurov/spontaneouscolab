# Agent Relay

[![CI](https://github.com/ggurov/spontaneouscolab/actions/workflows/ci.yml/badge.svg)](https://github.com/ggurov/spontaneouscolab/actions/workflows/ci.yml)

A tiny HTTP message bus for agents that need to talk to each other. One URL is
the whole interface: create a channel, hand the URL to the other agent, done.
No accounts, no SDK, no runtime dependencies — `curl` is enough (tests need
`pytest`).

```
Agent A ── POST /channels ──> relay <── GET  /c/<token>/messages?since=…  (long-poll)
   │                              │
   └──── channel URL (secret) ────┴──> Agent B
```

## Why

Two agents working on the same problem often sit behind different humans (for
example a hardware-in-the-loop rig operator and a firmware developer). Relay
replaces copy/pasting chat messages: it gives the agents a private, ordered,
append-only conversation identified by an unguessable URL.

## Quick start

```bash
python3 relay.py --port 8787        # binds 127.0.0.1 by default
# or: make run                      # make run HOST=0.0.0.0 to listen widely
```

Create a channel:

```bash
curl -sS -X POST http://localhost:8787/channels \
  -H 'Content-Type: application/json' -d '{"name":"hil-rig"}'
```

```json
{
  "token": "9f2c...32 hex chars...",
  "url": "http://localhost:8787/c/9f2c...",
  "read_token": "41ab...32 hex chars...",
  "read_url": "http://localhost:8787/c/41ab...",
  "channel": {"name": "hil-rig", "created_at": "...", "cursor": 0, ...}
}
```

The returned `url` is the read-write capability. Give it to the other agent.
`read_url` is a read-only capability for observers; it cannot join, post,
acknowledge, or close the channel. Requesting either URL returns the channel
guide: protocol, participants, and copy-paste commands. Then:


```bash
URL=http://localhost:8787/c/9f2c...

# 1. announce yourself (posts a kind="join" message)
curl -sS -X POST "$URL/join" -H 'Content-Type: application/json' \
  -d '{"name":"hil-agent","role":"runs the rig"}'

# 2. read: long-poll returns as soon as a message arrives (or after wait seconds)
curl -sS "$URL/messages?since=0&wait=60&as=hil-agent"

# 3. send
curl -sS -X POST "$URL/messages" -H 'Content-Type: application/json' \
  -d '{"sender":"hil-agent","text":"need the new CAN bitrate","reply_to":7}'
```

## Docker

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:8787/healthz
```

The published host port is `RELAY_PORT` (default `8787`). Set
`RELAY_ADMIN_TOKEN` to gate channel creation. The container runs as a non-root
user with a read-only filesystem; see [`DEPLOY.md`](DEPLOY.md) for the full
runbook, TLS proxy examples, and operations.

For a redeploy that must not orphan agents, set `RELAY_RESTORE_CHANNELS` to
`token[:name[:next_seq]],…`; the container recreates those channel URLs at
startup (messages are not preserved).

## Protocol

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Service guide (Markdown, JSON with `Accept: application/json` or `?format=json`) |
| `POST` | `/channels` | Create a channel; returns `token` + capability `url`, plus `read_token` + `read_url` |
| `GET` | `/c/<token>` | Channel guide + live status (self-documenting URL) |
| `POST` | `/c/<token>/join` | Register a participant (`name`, `role?`, `meta?`), post a `kind:"join"` announcement |
| `POST` | `/c/<token>/messages` | Send `{sender, text, to?, reply_to?, data?, kind?, state?, supersedes?, requires_ack?, priority?, expires_at?, tags?, correlation_id?}` |
| `GET` | `/c/<token>/messages` | Read: `since`, `wait` (long-poll, max 60 s), `last=N`, `thread=<seq>`, `as`, `limit` (default 100, max 1000) |
| `GET` | `/c/<token>/messages/<seq>` | Read one retained message |
| `GET` | `/c/<token>/events` | Stream as SSE (`since`, `as`); resumes from `Last-Event-ID`, sends `retry: 3000` |
| `POST` | `/c/<token>/ack` | Acknowledge a message: `{name, seq, note?}` |
| `POST` | `/c/<token>/presence` | Update liveness/status without posting: `{name, role?, status?, meta?}` |
| `GET` | `/c/<token>/state` | Open tasks, alerts, recent decisions, latest status, unacked messages, artifacts |
| `POST` | `/c/<token>/artifacts` | Upload raw bytes (`name`, `sender`, optional `sha256`); posts `kind:"artifact"` metadata |
| `GET` | `/c/<token>/artifacts` | List artifact metadata |
| `GET` | `/c/<token>/artifacts/<id>` | Download artifact (`?meta=1` for JSON); includes SHA-256 header |
| `DELETE` | `/c/<token>/artifacts/<id>` | Delete an artifact (read-write URL only) |
| `GET` | `/c/<token>/status` | Participants, counters, and read cursors as JSON |
| `DELETE` | `/c/<token>` | Close the channel (204; read-write URL only) |
| `GET` | `/healthz` | Liveness, `revision`, protocol version, channel count, configured limits |
| `GET` | `/monitor` | Human monitor UI: all channels, participants, messages, timing, inject form |
| `GET` | `/monitor/api/state` | Authenticated monitor snapshot (`Authorization: Bearer <monitor token>`) |
| `GET` | `/monitor/api/channels/<token>/messages` | Authenticated channel history: `limit` (max 1000), `before=<seq>` pagination |
| `POST` | `/monitor/api/channels/<token>/messages` | Authenticated message injection into one channel |

Messages are JSON and per-channel ordered:

```json
{
  "seq": 7,
  "ts": "2026-11-07T17:54:06.468Z",
  "sender": "hil-agent",
  "kind": "message",
  "text": "need the new CAN bitrate",
  "to": null,
  "reply_to": null,
  "data": null,
  "supersedes": [],
  "superseded_by": null,
  "requires_ack": false,
  "acks": {},
  "priority": "normal",
  "expires_at": null,
  "state": null,
  "tags": [],
  "correlation_id": null
}
```

- `seq` is monotonic per channel. Clients pass the last seen `seq` as `since`
  and continue from the returned `cursor`. A cursor past `latest_seq` is clamped
  to the head, so a stale cursor self-heals. When a filtered read has nothing
  visible, `cursor` advances to the channel head too, so `as` readers do not
  loop on the same seq. Delivery is at-least-once; deduplicate on `seq`.
- `to` is *addressing*, not access control. Everyone holding the URL can read
  everything; `as=<name>` filters out messages addressed to someone else, but a
  sender always sees its own messages.
- `reply_to` threads a reply to a `seq`; `GET /messages?thread=N` returns the
  root and all transitive replies. `data` carries arbitrary JSON.
- `kind` is free-form; `message`, `join`, `task`, `status`, `decision`, `alert`,
  `metric`, and `artifact` are recognized by `/state`. `state` is `open`,
  `blocked`, `done`, or `cancelled` for tasks.
- `supersedes` lists earlier `seq` values replaced by this message. Those
  messages gain `superseded_by`; `/state` ignores them. Messages are immutable,
  so corrections are explicit rather than edits.
- `requires_ack` plus `POST /ack {name, seq, note?}` records acknowledgements.
  `/state.unacked` lists messages still missing one. Reads with `as=<name>` also
  update that participant's `last_read_seq`/`last_read_at`.
- `priority` is `normal`, `high`, or `urgent`; `expires_at` marks temporary
  instructions; `tags` and `correlation_id` group related work.
- `wait` is the effective long-poll request (capped at 60 s); `wait_elapsed`
  reports how long the call actually blocked.
- `?last=N` (with optional `as`) returns the newest `N` retained messages
  without long-polling — useful for catching up on join.
- `truncated:true` also appears after a restore: the channel URL survives but
  its transcript is gone until a cursor reaches the restore point.
- `sender`, `to`, and join `name`/`role` are 1–64 printable characters; channel
  names follow the same rule, and an omitted name becomes
  `channel-<token-prefix>`.
- Posts accept `Content-Type: application/json`, or plain text with
  `?sender=<name>`; a body starting with `{` is parsed as JSON.
- Artifacts are raw bytes up to 128 KiB each, 32 per channel, 2 MiB total. The
  upload posts a `kind:"artifact"` message with `id`, `sha256`, and size;
  downloads include `X-Checksum-SHA256` and are served as attachments.

## Security model

- A channel URL contains 128 bits of randomness. Possession is the credential.
  Creation returns a read-write URL and a separate read-only URL; the
  read-only URL is suitable for observers and cannot mutate the channel.
- Channel URLs are secrets: the service never logs full tokens, responses are
  `Cache-Control: no-store`, and there is no unauthenticated channel listing;
  the authenticated monitor is the only aggregate view.
- The monitor UI is protected by a separate bearer token (`--monitor-token` /
  `RELAY_MONITOR_TOKEN`). `/monitor` itself contains no data; its APIs list every
  channel and can inject messages, so treat the token as an administrator
  credential. If the token is unset, the monitor APIs return 403.
- Reverse proxies and browsers retain URLs in logs and history: redact
  `/c/<token>` in proxy access logs. The read-write URL also authorizes
  `DELETE`, which is the only way to revoke a channel.
- Do not post plaintext credentials or private keys. Every URL holder can read
  them and the server keeps them in memory; encrypt client-side or use an
  out-of-band secret channel.
- The service speaks plain HTTP. For anything beyond a trusted network, put it
  behind a TLS terminator. It honors `X-Forwarded-Proto` /
  `X-Forwarded-Host` / `X-Forwarded-Prefix` when generating links, or set
  `--public-url` / `RELAY_PUBLIC_URL` including any path prefix (recommended
  behind a proxy, because `Host` headers are attacker-controlled).
- Optionally require a token for channel creation: `--admin-token` (or
  `RELAY_ADMIN_TOKEN`). Agents only need the channel URL, not this token.

```bash
curl -sS -X POST http://localhost:8787/channels \
  -H "Authorization: Bearer $RELAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"hil-rig"}'
```

`X-Admin-Token: <token>` works too, and the flag overrides the env var.

```caddy
relay.example.com {
    reverse_proxy 127.0.0.1:8787
}
```

nginx needs buffering off for SSE:

```nginx
location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
}
```

## Limits and persistence

Everything is in memory, on purpose: it is a conversation relay, not an archive.

| Knob | Default | Flag |
| --- | --- | --- |
| Messages retained per channel | 2000 | `--max-messages` |
| Idle lifetime (since last message) | 168 h | `--channel-ttl-hours` (0 = never) |
| Live channels | 10000 | `--max-channels` |
| Message text or `data`, each | 64 KiB | constant in `relay.py` |
| Artifact bytes, each | 128 KiB | constant in `relay.py` |
| Artifacts per channel / total | 32 / 2 MiB | constants in `relay.py` |
| Participant `meta` | 16 KiB | constant in `relay.py` |
| Long-poll wait | 60 s max | request parameter `wait` |
| Request body | 1 MiB | constant in `relay.py` |
| Log level | `info` | `--log-level` |
| Monitor messages per channel | 50 in state snapshot; up to 1000 in history | `limit` on `/monitor/api/state` or channel history |
| Monitor token | empty (monitor APIs disabled) | `--monitor-token` / `RELAY_MONITOR_TOKEN` |

A restart forgets all channels, messages, and artifacts. Expired channels are
removed by a background sweep; new requests to a dead URL get `404`, an
in-flight long-poll gets `410`, and an open SSE stream ends. Oversized request
bodies are drained up to 8 MiB where possible and rejected with JSON `413`.

## Tests

```bash
make test     # pytest: 64 end-to-end tests over the real HTTP surface
make smoke    # curl + python3 against a live process (channel, long-poll, SSE)
```

The suite covers capability isolation, cursor/truncation semantics, long-poll
wakeup/timeout/disconnect, graceful restart 503s and SSE drain events, SSE
history/live delivery, `Last-Event-ID` resume and `as` filtering, tail reads,
restore gaps, `to` addressing and sender visibility, join/presence, read
cursors, acknowledgements, supersession, thread reads, derived state,
artifacts, read-only URLs, monitor auth/snapshot/injection, idle-channel expiry,
capacity limits, proxy-derived guide links, per-path `Allow`, build revision
reporting, validation, admin-token gating, and channel deletion.

## Layout

```
relay.py             # server: state, HTTP handler, guides (stdlib only)
tests/test_relay.py  # end-to-end tests
tests/conftest.py    # puts the repo root on sys.path for `import relay`
scripts/smoke.sh     # live curl smoke test
Dockerfile           # non-root, read-only runtime image
compose.yaml         # deployment defaults, healthcheck, hardening
.github/workflows/   # CI: tests, smoke, image build + container probe
Makefile             # make run | test | smoke
DEPLOY.md            # deployment and operations runbook
CONTRIBUTING.md      # contribution and merge policy
AGENTS.md            # contributor/agent guidelines
.wolf/, .claude/     # epicwolf agent memory and hooks (AGPL-3.0)
LICENSE              # AGPL-3.0
```

## License

AGPL-3.0; see `LICENSE`. The `.wolf/` agent-memory tree and `.claude/` hooks
are vendored from [ggurov/epicwolf](https://github.com/ggurov/epicwolf), which
is also AGPL-3.0.
