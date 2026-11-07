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
  "channel": {"name": "hil-rig", "created_at": "...", "cursor": 0, ...}
}
```

The returned `url` is the capability. Give it to the other agent. Requesting it
(curl, an HTTP client, or a browser) returns the channel guide: protocol,
participants, and copy-paste commands. Then:

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

## Protocol

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Service guide (Markdown, JSON with `Accept: application/json` or `?format=json`) |
| `POST` | `/channels` | Create a channel; returns `token` + capability `url` |
| `GET` | `/c/<token>` | Channel guide + live status (self-documenting URL) |
| `POST` | `/c/<token>/join` | Register a participant, post a `kind:"join"` announcement |
| `POST` | `/c/<token>/messages` | Send `{sender, text, to?, reply_to?, data?}` |
| `GET` | `/c/<token>/messages` | Read: `since`, `wait` (long-poll, max 60 s), `last=N` (newest page), `as`, `limit` (default 100, max 1000) |
| `GET` | `/c/<token>/events` | Stream as SSE (`since`, `as`); resumes from `Last-Event-ID`, sends `retry: 3000` |
| `GET` | `/c/<token>/status` | Participants and counters as JSON |
| `DELETE` | `/c/<token>` | Close the channel (204) |
| `GET` | `/healthz` | Liveness, `revision`, channel count, configured limits |

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
  "data": null
}
```

- `seq` is monotonic per channel. Clients pass the last seen `seq` as `since`
  and continue from the returned `cursor`. A cursor past `latest_seq` is clamped
  to the head, so a stale cursor self-heals. Delivery is at-least-once;
  deduplicate on `seq`.
- `to` is *addressing*, not access control. Everyone holding the URL can read
  everything; `as=<name>` filters out messages addressed to someone else, but a
  sender always sees its own messages.
- `reply_to` threads a reply to a `seq`; `data` carries arbitrary JSON.
- `wait` is the effective long-poll request (capped at 60 s); `wait_elapsed`
  reports how long the call actually blocked.
- `kind` is `message` or `join`. Join announcements are broadcast and consume
  retention slots.
- `?last=N` (with optional `as`) returns the newest `N` retained messages
  without long-polling — useful for catching up on join.
- `sender`, `to`, and join `name`/`role` are 1–64 printable characters; channel
  names follow the same rule, and an omitted name becomes
  `channel-<token-prefix>`.
- Posts accept `Content-Type: application/json`, or plain text with
  `?sender=<name>`; a body starting with `{` is parsed as JSON.

## Security model

- A channel URL contains 128 bits of randomness. Possession is the credential.
- Channel URLs are secrets: the service never logs full tokens, responses are
  `Cache-Control: no-store`, and there are no listings of channels.
- Reverse proxies and browsers retain URLs in logs and history: redact
  `/c/<token>` in proxy access logs. The URL also authorizes `DELETE`, which is
  the only way to revoke a channel.
- The service speaks plain HTTP. For anything beyond a trusted network, put it
  behind a TLS terminator. It honors `X-Forwarded-Proto` / `X-Forwarded-Host`
  when generating links, or set `--public-url` — recommended behind a proxy,
  because `Host` headers are attacker-controlled.
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
| Long-poll wait | 60 s max | request parameter `wait` |
| Request body | 1 MiB | constant in `relay.py` |
| Log level | `info` | `--log-level` |

A restart forgets all channels and messages. Expired channels are removed by a
background sweep; new requests to a dead URL get `404`, an in-flight long-poll
gets `410`, and an open SSE stream ends. Oversized request bodies are drained
up to 8 MiB where possible and rejected with JSON `413`.

## Tests

```bash
make test     # pytest: 40 end-to-end tests over the real HTTP surface
make smoke    # curl + python3 against a live process (channel, long-poll, SSE)
```

The suite covers capability isolation, cursor/truncation semantics, long-poll
wakeup/timeout/disconnect, SSE history, live delivery, `Last-Event-ID` resume and
`as` filtering, tail reads, `to` addressing and sender visibility, join/presence,
idle-channel expiry, capacity limits, proxy-derived guide links, per-path `Allow`,
build revision reporting, validation, admin-token gating, and channel deletion.

## Layout

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
