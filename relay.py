#!/usr/bin/env python3
"""Agent Relay: a tiny capability-URL message bus for collaborating agents.

A channel is created with one HTTP call, and the returned URL is the credential:
anyone holding it can read and post, nobody else can.  The channel endpoint is
itself the documentation, so an agent only needs the URL to get started.

Zero third-party dependencies; Python 3.9+ standard library only.
"""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterable

LOG = logging.getLogger("agent-relay")

TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
TOKEN_IN_TEXT_RE = re.compile(r"\b[0-9a-f]{32}\b")

DEFAULT_MAX_MESSAGES = 2000
DEFAULT_TTL_HOURS = 168.0
DEFAULT_MAX_CHANNELS = 10_000
MAX_MESSAGE_BYTES = 64 * 1024
MAX_BODY_BYTES = 1024 * 1024
MAX_SENDER_LEN = 64
MAX_WAIT = 60.0
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
SSE_HEARTBEAT = 15.0
PROTOCOL_VERSION = 1


# ---------------------------------------------------------------------------
# helpers


def now_iso() -> str:
    """Current UTC time as ISO-8601 with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def mask_tokens(text: str) -> str:
    """Hide capability tokens in log lines."""
    return TOKEN_IN_TEXT_RE.sub(lambda m: m.group(0)[:8] + "...", text)


def _expiry_phrase(ttl_seconds: float) -> str:
    if ttl_seconds <= 0:
        return "channels never expire"
    hours = ttl_seconds / 3600
    unit = "hour" if hours == 1 else "hours"
    return "channels are deleted after %g %s without a new message" % (hours, unit)


class ApiError(Exception):
    """An error that maps directly onto an HTTP status code."""

    def __init__(self, status: int, message: str, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers or {}


# ---------------------------------------------------------------------------
# state


class Channel:
    """One conversation.  Guarded by ``Relay.lock``."""

    def __init__(self, token: str, name: str, max_messages: int, lock: threading.RLock):
        self.token = token
        self.name = name
        self.created_at = now_iso()
        self.last_activity_at = self.created_at
        self.next_seq = 1
        self.messages: deque[dict[str, Any]] = deque(maxlen=max_messages)
        self.participants: dict[str, dict[str, Any]] = {}
        self.waiters = 0
        self.closed = False
        self._last_seen_mono = time.monotonic()
        self.cond = threading.Condition(lock)

    # -- snapshots; callers must hold the relay lock ------------------------

    def oldest_seq(self) -> int | None:
        return self.messages[0]["seq"] if self.messages else None

    def latest_seq(self) -> int:
        return self.next_seq - 1

    def snapshot(self, since: int, as_name: str | None, limit: int) -> list[dict[str, Any]]:
        """Messages with seq > since, optionally filtered by recipient."""
        out: list[dict[str, Any]] = []
        for message in self.messages:
            if message["seq"] <= since:
                continue
            if as_name and message["to"] and message["to"] != as_name:
                continue
            out.append(message)
            if len(out) >= limit:
                break
        return out

    def info(self, ttl_seconds: float) -> dict[str, Any]:
        oldest = self.oldest_seq()
        expires_in = None
        if ttl_seconds > 0:
            expires_in = max(0, int(ttl_seconds - (time.monotonic() - self._last_seen_mono)))
        return {
            "name": self.name,
            "created_at": self.created_at,
            "last_activity_at": self.last_activity_at,
            "cursor": self.latest_seq(),
            "retained": len(self.messages),
            "oldest_seq": oldest,
            "waiters": self.waiters,
        }

    def participant_list(self) -> list[dict[str, Any]]:
        return sorted(
            self.participants.values(),
            key=lambda p: (p["first_seen"], p["name"]),
        )


class Relay:
    """All channels plus the lock that protects them."""

    def __init__(
        self,
        *,
        max_messages: int = DEFAULT_MAX_MESSAGES,
        ttl_seconds: float = DEFAULT_TTL_HOURS * 3600,
        max_channels: int = DEFAULT_MAX_CHANNELS,
        base_url: str | None = None,
        admin_token: str | None = None,
    ):
        self.lock = threading.RLock()
        self.channels: dict[str, Channel] = {}
        self.max_messages = max(1, int(max_messages))
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self.max_channels = max(1, int(max_channels))
        self.base_url = (base_url or "").rstrip("/") or None
        self.admin_token = admin_token or None
        self.started_mono = time.monotonic()

    # -- channels -----------------------------------------------------------

    def create_channel(self, name: str = "") -> Channel:
        with self.lock:
            if len(self.channels) >= self.max_channels:
                raise ApiError(503, "channel limit reached; try again later")
            token = secrets.token_hex(16)
            channel = Channel(token, name.strip()[:128], self.max_messages, self.lock)
            self.channels[token] = channel
            LOG.info("channel created: token=%s name=%r", mask_tokens(token), channel.name)
            return channel

    def get_channel(self, token: str) -> Channel:
        with self.lock:
            channel = self.channels.get(token)
            if channel is None or channel.closed:
                raise ApiError(404, "unknown channel")
            if self.ttl_seconds > 0 and time.monotonic() - channel._last_seen_mono > self.ttl_seconds:
                self._remove_locked(channel)
                raise ApiError(404, "channel expired")
            return channel

    def close_channel(self, channel: Channel) -> None:
        with self.lock:
            self._remove_locked(channel)

    def _remove_locked(self, channel: Channel) -> None:
        channel.closed = True
        channel.cond.notify_all()
        self.channels.pop(channel.token, None)
        LOG.info("channel removed: token=%s", mask_tokens(channel.token))

    def gc(self) -> int:
        if self.ttl_seconds <= 0:
            return 0
        removed = 0
        with self.lock:
            cutoff = time.monotonic() - self.ttl_seconds
            for channel in list(self.channels.values()):
                if channel._last_seen_mono < cutoff:
                    self._remove_locked(channel)
                    removed += 1
        return removed

    # -- messages -----------------------------------------------------------

    def post(
        self,
        channel: Channel,
        *,
        sender: str,
        text: str,
        to: str | None = None,
        reply_to: int | None = None,
        data: Any = None,
        kind: str = "message",
    ) -> tuple[dict[str, Any], int]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            message = {
                "seq": channel.next_seq,
                "ts": now_iso(),
                "sender": sender,
                "kind": kind,
                "text": text,
                "to": to,
                "reply_to": reply_to,
                "data": data,
            }
            channel.next_seq += 1
            channel.messages.append(message)
            channel.last_activity_at = message["ts"]
            channel._last_seen_mono = time.monotonic()
            participant = channel.participants.get(sender)
            if participant is None:
                participant = {
                    "name": sender,
                    "role": None,
                    "first_seen": message["ts"],
                    "last_seen": message["ts"],
                    "messages": 0,
                }
                channel.participants[sender] = participant
            participant["last_seen"] = message["ts"]
            participant["messages"] += 1
            waiters = channel.waiters
            channel.cond.notify_all()
            return message, waiters

    def join(self, channel: Channel, *, name: str, role: str | None) -> tuple[dict[str, Any], dict[str, Any]]:
        """Register a participant and announce it as a kind="join" message."""
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            participant = channel.participants.get(name)
            if participant is None:
                channel.participants[name] = {
                    "name": name,
                    "role": role,
                    "first_seen": now_iso(),
                    "last_seen": now_iso(),
                    "messages": 0,
                }
            elif role:
                participant["role"] = role
        message, _ = self.post(
            channel,
            sender=name,
            text="joined" + (": " + role if role else ""),
            data={"role": role} if role else None,
            kind="join",
        )
        with self.lock:
            return message, dict(channel.participants[name])

    def wait(
        self,
        channel: Channel,
        *,
        since: int,
        as_name: str | None,
        wait_seconds: float,
        limit: int,
    ) -> dict[str, Any]:
        """Long-poll: return once a message is available or the wait expires."""
        deadline = time.monotonic() + wait_seconds
        with self.lock:
            channel.waiters += 1
            try:
                while True:
                    if channel.closed:
                        raise ApiError(410, "channel closed")
                    messages = channel.snapshot(since, as_name, limit)
                    if messages:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    channel.cond.wait(remaining)
            finally:
                channel.waiters -= 1
            return self.page(channel, messages, since)

    def page(self, channel: Channel, messages: list[dict[str, Any]], since: int) -> dict[str, Any]:
        oldest = channel.oldest_seq()
        return {
            "messages": messages,
            "cursor": messages[-1]["seq"] if messages else since,
            "latest_seq": channel.latest_seq(),
            "oldest_seq": oldest,
            "truncated": oldest is not None and since < oldest - 1,
        }

    def health(self) -> dict[str, Any]:
        with self.lock:
            return {
                "ok": True,
                "channels": len(self.channels),
                "uptime_s": round(time.monotonic() - self.started_mono, 1),
                "limits": {
                    "messages_per_channel": self.max_messages,
                    "max_channels": self.max_channels,
                    "channel_ttl_s": self.ttl_seconds,
                    "max_message_bytes": MAX_MESSAGE_BYTES,
                    "max_body_bytes": MAX_BODY_BYTES,
                    "max_wait_s": MAX_WAIT,
                },
            }

    def check_admin(self, provided: str | None) -> None:
        if not self.admin_token:
            return
        if not provided or not hmac.compare_digest(provided, self.admin_token):
            raise ApiError(401, "channel creation requires the relay admin token")


# ---------------------------------------------------------------------------
# documentation rendered by the service


SERVICE_GUIDE = """# Agent Relay

A tiny, private message bus for agents that need to collaborate. One URL is the
whole interface: hand it to another agent and you have a channel.

## Create a channel

```
curl -sS -X POST %(base)s/channels \\
  -H 'Content-Type: application/json' \\
  -d '{"name":"hil-rig"}'
```

The response contains a `url` such as `%(base)s/c/<token>`. That URL is the
capability: **anyone holding it can read and post; nobody else can.** Send it to
the other agent over a channel you already trust.

## Then

1. `GET <url>` returns that channel's guide, live participants, and cursor.
2. `POST <url>/join` with `{"name":"...","role":"..."}` announces who you are.
3. `GET <url>/messages?since=<cursor>&wait=60&as=<name>` long-polls for messages.
4. `POST <url>/messages` with `{"sender":"...","text":"..."}` sends one.
5. `GET <url>/events?since=<cursor>&as=<name>` streams messages as server-sent
   events instead of polling.

## Limits and semantics

- Messages are per-channel, ordered by a monotonic `seq`; delivery is
  at-least-once, so deduplicate on `seq`.
- Each channel keeps the last %(retained)d messages in memory;
  %(expiry_phrase)s.
- Messages up to %(message_kib)d KiB. No edits, no deletes, no accounts.
- Plain HTTP here; terminate TLS in front of the service (see README).
- A restart forgets all channels.

This page is available as JSON with `Accept: application/json` or `?format=json`.
"""

SERVICE_GUIDE_JSON = {
    "service": "agent-relay",
    "protocol_version": PROTOCOL_VERSION,
    "summary": "Capability-URL message bus: create a channel, share its URL, exchange JSON messages.",
    "create_channel": {
        "method": "POST",
        "path": "/channels",
        "body": {"name": "optional human-readable channel name"},
        "response": {"token": "<32 hex chars>", "url": "<base>/c/<token>"},
    },
    "channel_protocol": {
        "transport": "HTTP/1.1, JSON bodies",
        "receive": ["long-poll GET /c/<token>/messages", "SSE GET /c/<token>/events"],
        "send": "POST /c/<token>/messages",
        "delivery": "at-least-once; deduplicate by seq",
        "auth": "possession of the channel URL",
    },
    "limits": {
        "messages_per_channel": DEFAULT_MAX_MESSAGES,
        "max_channels": DEFAULT_MAX_CHANNELS,
        "channel_ttl_s": DEFAULT_TTL_HOURS * 3600,
        "max_message_bytes": MAX_MESSAGE_BYTES,
        "max_body_bytes": MAX_BODY_BYTES,
        "max_wait_s": MAX_WAIT,
    },
}

CHANNEL_GUIDE = """# Agent Relay channel%(name_suffix)s

Private message bus. **Anyone holding this URL can read and post**; share it only
with the agents you want in this conversation.

- channel URL: %(url)s
- created: %(created)s, last activity: %(last_activity)s
- participants: %(participants)s
- long-poll readers waiting: %(waiters)d
- messages retained: %(retained)d (cursor %(cursor)d, oldest %(oldest)s)
- expiry: %(ttl)s

## Protocol

JSON bodies, ISO-8601 UTC timestamps, per-channel monotonic `seq`. `curl` is
enough; examples below use this channel's URL.

### 1. Announce yourself

```
curl -sS -X POST %(url)s/join -H 'Content-Type: application/json' \\
  -d '{"name":"my-agent","role":"what I do"}'
```

Joining posts a `kind:"join"` message so peers notice you; the response includes
the current `cursor`. To read the backlog first, fetch `since=0` below.

### 2. Read messages

Long-poll (recommended; returns when a message arrives or `wait` seconds pass):

```
curl -sS '%(url)s/messages?since=%(cursor)d&wait=60&as=my-agent'
```

Response: `{"messages":[...],"cursor":N,"latest_seq":N,"oldest_seq":N,"truncated":false}`.
Always continue from the returned `cursor`. `as` filters out messages addressed
to a different participant with `to`. `truncated:true` means older messages were
dropped from the ring buffer.

Streaming alternative (server-sent events):

```
curl -N '%(url)s/events?since=%(cursor)d&as=my-agent'
```

### 3. Send a message

```
curl -sS -X POST %(url)s/messages -H 'Content-Type: application/json' \\
  -d '{"sender":"my-agent","text":"hello","reply_to":12,"to":"other-agent"}'
```

Plain-text convenience (`curl -d` sends form content type, which is accepted):

```
curl -sS -X POST '%(url)s/messages?sender=my-agent' -d 'hello'
```

Sending requires `sender`; `text` and/or `data` must carry content. `to` is
addressing, not access control: everyone with the URL can read everything.
Use `reply_to` with a `seq` to thread a reply. The response reports how many
long-poll readers were waiting.

### Message shape

```
{"seq":12,"ts":"2026-01-01T00:00:00.000Z","sender":"my-agent","kind":"message",
 "text":"hello","to":null,"reply_to":null,"data":null}
```

`kind` is `message` or `join`. `data` is arbitrary JSON.

### Other endpoints

- `GET %(url)s/status` - participants and counters as JSON
- `DELETE %(url)s` - close the channel immediately (204)
- `GET %(url)s` - this guide; `Accept: application/json` returns the same as JSON

## Rules of the road

- Continue from `cursor`; deduplicate on `seq`.
- Messages are held in memory only: last %(retained_max)d per channel, channels
  expire after %(ttl)s idle, and a service restart clears everything.
- Maximum message size: %(message_kib)d KiB.
- This channel URL is a secret. Do not paste it into logs, tickets, or chat
  history you would not trust with the conversation.
"""


def channel_guide(channel: Channel, base: str, ttl_seconds: float, max_messages: int) -> str:
    info = channel.info(ttl_seconds)
    participants = channel.participant_list()
    if participants:
        rendered = ", ".join(
            "%s (%s; %d msgs, last %s)" % (p["name"], p["role"] or "role unknown", p["messages"], p["last_seen"])
            for p in participants
        )
    else:
        rendered = "none yet"
    if ttl_seconds <= 0:
        ttl = "never"
    else:
        hours = ttl_seconds / 3600
        ttl = "%g %s after the last message" % (hours, "hour" if hours == 1 else "hours")
    return CHANNEL_GUIDE % {
        "name_suffix": ": " + channel.name if channel.name else "",
        "url": "%s/c/%s" % (base, channel.token),
        "created": info["created_at"],
        "last_activity": info["last_activity_at"],
        "participants": rendered,
        "waiters": info["waiters"],
        "retained": info["retained"],
        "cursor": info["cursor"],
        "oldest": info["oldest_seq"] if info["oldest_seq"] is not None else "-",
        "ttl": ttl,
        "retained_max": max_messages,
        "message_kib": MAX_MESSAGE_BYTES // 1024,
    }


def channel_guide_json(channel: Channel, base: str, ttl_seconds: float) -> dict[str, Any]:
    url = "%s/c/%s" % (base, channel.token)
    return {
        "service": "agent-relay",
        "protocol_version": PROTOCOL_VERSION,
        "channel": {"token": channel.token, "url": url, **channel.info(ttl_seconds)},
        "participants": channel.participant_list(),
        "endpoints": {
            "guide": url,
            "status": url + "/status",
            "join": url + "/join",
            "messages": url + "/messages",
            "events": url + "/events",
            "close": url,
        },
        "protocol": {
            "send": "POST messages {sender, text, to?, reply_to?, data?}",
            "receive": "GET messages?since=<seq>&wait=<s>&as=<name> or GET events (SSE)",
            "message": {
                "seq": "int, per-channel monotonic",
                "ts": "ISO-8601 UTC",
                "sender": "string",
                "kind": "message | join",
                "text": "string",
                "to": "recipient name or null (addressing only)",
                "reply_to": "seq or null",
                "data": "arbitrary JSON or null",
            },
            "delivery": "at-least-once; deduplicate by seq",
            "auth": "possession of the channel URL",
        },
    }


# ---------------------------------------------------------------------------
# HTTP layer


class RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "AgentRelay/1.0"
    timeout = 120

    # -- plumbing -----------------------------------------------------------

    @property
    def relay(self) -> Relay:
        return self.server.relay  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003 - stdlib signature
        LOG.info("%s %s", self.address_string(), mask_tokens(fmt % args))

    def do_GET(self) -> None:  # noqa: N802 - stdlib signature
        self._dispatch(self._handle_get)

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch(self._handle_get)

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch(self._handle_post)

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch(self._handle_delete)

    def handle_one_request(self) -> None:
        self._headers_sent = False
        self._body_read = False
        super().handle_one_request()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send(
            204,
            b"",
            extra={
                "Allow": "GET, HEAD, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Methods": "GET, HEAD, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Admin-Token",
            },
        )

    def _dispatch(self, handler) -> None:
        try:
            handler()
        except ApiError as exc:
            if self.command == "POST" and not getattr(self, "_body_read", False):
                self.close_connection = True
            self._send_json(exc.status, {"error": exc.message}, extra=exc.headers)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:  # pragma: no cover - defensive
            LOG.exception("unhandled error: %s %s", self.command, mask_tokens(self.path))
            if not self._headers_sent:
                self._send_json(500, {"error": "internal server error"})
            self.close_connection = True

    def _send(
        self,
        status: int,
        body: bytes,
        content_type: str = "application/json; charset=utf-8",
        extra: dict[str, str] | None = None,
    ) -> None:
        if getattr(self, "_headers_sent", False):
            return
        self._headers_sent = True
        self.send_response(status)
        if status != 204:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD" and body:
            self.wfile.write(body)

    def _send_json(self, status: int, payload: Any, extra: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self._send(status, body, extra=extra)

    def _read_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length")
        if not raw_length:
            self._body_read = True
            return b""
        try:
            length = int(raw_length)
        except ValueError:
            self.close_connection = True
            raise ApiError(400, "invalid Content-Length")
        if length < 0 or length > MAX_BODY_BYTES:
            self.close_connection = True
            raise ApiError(413, "request body too large")
        body = self.rfile.read(length)
        if len(body) != length:
            self.close_connection = True
            raise ApiError(400, "truncated request body")
        self._body_read = True
        return body

    def _base_url(self) -> str:
        if self.relay.base_url:
            return self.relay.base_url
        proto = (self.headers.get("X-Forwarded-Proto") or "http").split(",")[0].strip()
        host = (
            self.headers.get("X-Forwarded-Host")
            or self.headers.get("Host")
            or "%s:%d" % self.server.server_address[:2]
        )
        return "%s://%s" % (proto, host.split(",")[0].strip())

    def _route(self) -> tuple[list[str], dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        parts = [urllib.parse.unquote(part) for part in parsed.path.strip("/").split("/") if part]
        return parts, query

    def _channel(self, parts: list[str]) -> Channel:
        if len(parts) < 2 or parts[0] != "c" or not TOKEN_RE.match(parts[1]):
            raise ApiError(404, "unknown endpoint")
        return self.relay.get_channel(parts[1])

    def _wants_json(self, query: dict[str, list[str]]) -> bool:
        explicit = (query.get("format") or [""])[0].lower()
        if explicit:
            return explicit == "json"
        accept = self.headers.get("Accept", "")
        return "application/json" in accept

    @staticmethod
    def _int_query(query: dict[str, list[str]], name: str, default: int, minimum: int = 0) -> int:
        raw = (query.get(name) or [None])[0]
        if raw is None or raw == "":
            return default
        try:
            value = int(raw)
        except ValueError:
            raise ApiError(400, "%s must be an integer" % name)
        if value < minimum:
            raise ApiError(400, "%s must be >= %d" % (name, minimum))
        return value

    @staticmethod
    def _wait_query(query: dict[str, list[str]]) -> float:
        raw = (query.get("wait") or [None])[0]
        if raw is None or raw == "":
            return 0.0
        try:
            value = float(raw)
        except ValueError:
            raise ApiError(400, "wait must be a number")
        return max(0.0, min(value, MAX_WAIT))

    @staticmethod
    def _name_query(query: dict[str, list[str]], name: str) -> str | None:
        raw = (query.get(name) or [None])[0]
        if raw is None:
            return None
        raw = raw.strip()
        if not raw:
            return None
        if len(raw) > MAX_SENDER_LEN or any(ord(ch) < 32 for ch in raw):
            raise ApiError(400, "%s must be 1-%d printable characters" % (name, MAX_SENDER_LEN))
        return raw

    # -- GET ----------------------------------------------------------------

    def _handle_get(self) -> None:
        parts, query = self._route()
        if not parts:
            if self._wants_json(query):
                payload = dict(SERVICE_GUIDE_JSON)
                payload["limits"] = self.relay.health()["limits"]
                self._send_json(200, payload)
            else:
                self._send(
                    200,
                    (SERVICE_GUIDE % self._service_values()).encode("utf-8"),
                    "text/markdown; charset=utf-8",
                )
            return
        if parts == ["healthz"]:
            self._send_json(200, self.relay.health())
            return
        if parts == ["channels"]:
            raise ApiError(405, "use POST /channels to create a channel", headers={"Allow": "POST"})
        if parts[0] != "c":
            raise ApiError(404, "unknown endpoint")
        channel = self._channel(parts)
        rest = parts[2:]
        if not rest:
            wants_json = self._wants_json(query)
            base = self._base_url()
            with self.relay.lock:
                if wants_json:
                    payload = channel_guide_json(channel, base, self.relay.ttl_seconds)
                else:
                    text = channel_guide(channel, base, self.relay.ttl_seconds, self.relay.max_messages)
            if wants_json:
                self._send_json(200, payload)
            else:
                self._send(200, text.encode("utf-8"), "text/markdown; charset=utf-8")
            return
        if rest == ["status"]:
            base = self._base_url()
            with self.relay.lock:
                channel_payload = channel_guide_json(channel, base, self.relay.ttl_seconds)
            self._send_json(200, {
                "channel": channel_payload["channel"],
                "participants": channel_payload["participants"],
            })
            return
        if rest == ["messages"]:
            self._get_messages(channel, query)
            return
        if rest == ["events"]:
            self._events(channel, query)
            return
        raise ApiError(404, "unknown endpoint")

    def _service_values(self) -> dict[str, Any]:
        return {
            "base": self._base_url(),
            "retained": self.relay.max_messages,
            "expiry_phrase": _expiry_phrase(self.relay.ttl_seconds),
            "message_kib": MAX_MESSAGE_BYTES // 1024,
        }

    def _get_messages(self, channel: Channel, query: dict[str, list[str]]) -> None:
        since = self._int_query(query, "since", 0, 0)
        limit = min(self._int_query(query, "limit", DEFAULT_LIMIT, 1), MAX_LIMIT)
        wait_seconds = self._wait_query(query)
        as_name = self._name_query(query, "as")
        page = self.relay.wait(channel, since=since, as_name=as_name, wait_seconds=wait_seconds, limit=limit)
        page["wait"] = wait_seconds
        self._send_json(200, page)

    def _events(self, channel: Channel, query: dict[str, list[str]]) -> None:
        since = self._int_query(query, "since", 0, 0)
        as_name = self._name_query(query, "as")
        self._headers_sent = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        if self.command == "HEAD":
            self.close_connection = True
            return
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            last_write = time.monotonic()
            while True:
                with self.relay.lock:
                    messages = channel.snapshot(since, as_name, MAX_LIMIT)
                    if not messages and not channel.closed:
                        channel.cond.wait(SSE_HEARTBEAT)
                        messages = channel.snapshot(since, as_name, MAX_LIMIT)
                    closed = channel.closed
                if messages:
                    chunks = []
                    for message in messages:
                        chunks.append("id: %d\ndata: %s\n\n" % (message["seq"], json.dumps(message, ensure_ascii=False)))
                        since = message["seq"]
                    self.wfile.write("".join(chunks).encode("utf-8"))
                    self.wfile.flush()
                    last_write = time.monotonic()
                elif closed:
                    break
                elif time.monotonic() - last_write >= SSE_HEARTBEAT:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    last_write = time.monotonic()
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    # -- POST ---------------------------------------------------------------

    def _handle_post(self) -> None:
        parts, query = self._route()
        if parts == ["channels"]:
            self._create_channel()
            return
        if not parts or parts[0] != "c":
            raise ApiError(404, "unknown endpoint")
        channel = self._channel(parts)
        rest = parts[2:]
        if rest == ["messages"]:
            self._post_message(channel, query)
            return
        if rest == ["join"]:
            self._join(channel)
            return
        raise ApiError(404, "unknown endpoint")

    def _create_channel(self) -> None:
        try:
            self.relay.check_admin(self._admin_token())
        except ApiError:
            self.close_connection = True
            raise
        body = self._read_body()
        name = ""
        if body.strip():
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ApiError(400, "invalid JSON: %s" % exc.msg)
            if not isinstance(payload, dict):
                raise ApiError(400, "JSON body must be an object")
            name = self._clean_name(payload.get("name"), "name", required=False) or ""
        channel = self.relay.create_channel(name)
        base = self._base_url()
        self._send_json(201, {
            "token": channel.token,
            "url": "%s/c/%s" % (base, channel.token),
            "channel": channel.info(self.relay.ttl_seconds),
        })

    def _admin_token(self) -> str | None:
        header = self.headers.get("Authorization") or ""
        if header.lower().startswith("bearer "):
            return header[7:].strip()
        return self.headers.get("X-Admin-Token")

    def _join(self, channel: Channel) -> None:
        payload = self._json_body()
        name = self._clean_name(payload.get("name"), "name", required=True)
        role = self._clean_name(payload.get("role"), "role", required=False)
        message, participant = self.relay.join(channel, name=name, role=role)
        with self.relay.lock:
            channel_json = channel_guide_json(channel, self._base_url(), self.relay.ttl_seconds)
        self._send_json(200, {
            "ok": True,
            "participant": participant,
            "cursor": message["seq"],
            "channel": channel_json["channel"],
            "participants": channel_json["participants"],
        })

    def _post_message(self, channel: Channel, query: dict[str, list[str]]) -> None:
        raw = self._read_body()
        content_type = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        payload: dict[str, Any] = {}
        if content_type == "application/json":
            try:
                parsed = json.loads(raw or b"{}")
            except json.JSONDecodeError as exc:
                raise ApiError(400, "invalid JSON: %s" % exc.msg)
            if not isinstance(parsed, dict):
                raise ApiError(400, "JSON body must be an object")
            payload = parsed
        elif content_type != "text/plain" and raw.lstrip().startswith(b"{"):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                raise ApiError(400, "body starts with '{' but is not valid JSON; send plain text with Content-Type: text/plain")
            if not isinstance(parsed, dict):
                raise ApiError(400, "JSON body must be an object")
            payload = parsed
        else:
            payload = {"text": raw.decode("utf-8", "replace")}

        sender = self._clean_name(payload.get("sender") or self._name_query(query, "sender"), "sender", required=True)
        text = payload.get("text", "")
        if text is None:
            text = ""
        if not isinstance(text, str):
            raise ApiError(400, "text must be a string")
        to = payload.get("to")
        if to is not None:
            to = self._clean_name(to, "to", required=False)
        reply_to = payload.get("reply_to")
        if reply_to is not None:
            if not isinstance(reply_to, int) or isinstance(reply_to, bool) or reply_to < 0:
                raise ApiError(400, "reply_to must be a non-negative integer")
        data = payload.get("data")
        if not text and data is None:
            raise ApiError(400, "text or data is required")
        if len(text.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ApiError(413, "text exceeds %d KiB" % (MAX_MESSAGE_BYTES // 1024))
        if data is not None and len(json.dumps(data, ensure_ascii=False).encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ApiError(413, "data exceeds %d KiB" % (MAX_MESSAGE_BYTES // 1024))

        message, waiters = self.relay.post(
            channel, sender=sender, text=text, to=to, reply_to=reply_to, data=data
        )
        with self.relay.lock:
            latest_seq = channel.latest_seq()
        self._send_json(201, {
            "ok": True,
            "message": message,
            "cursor": message["seq"],
            "latest_seq": latest_seq,
            "waiters": waiters,
        })

    @staticmethod
    def _clean_name(value: Any, field: str, *, required: bool) -> str | None:
        if value is None:
            if required:
                raise ApiError(400, "%s is required" % field)
            return None
        if not isinstance(value, str):
            raise ApiError(400, "%s must be a string" % field)
        value = value.strip()
        if not value:
            if required:
                raise ApiError(400, "%s is required" % field)
            return None
        if len(value) > MAX_SENDER_LEN or any(ord(ch) < 32 for ch in value):
            raise ApiError(400, "%s must be 1-%d printable characters" % (field, MAX_SENDER_LEN))
        return value

    def _json_body(self) -> dict[str, Any]:
        body = self._read_body()
        if not body.strip():
            return {}
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ApiError(400, "invalid JSON: %s" % exc.msg)
        if not isinstance(payload, dict):
            raise ApiError(400, "JSON body must be an object")
        return payload

    # -- DELETE -------------------------------------------------------------

    def _handle_delete(self) -> None:
        parts, _query = self._route()
        channel = self._channel(parts)
        if len(parts) != 2:
            raise ApiError(404, "unknown endpoint")
        self.relay.close_channel(channel)
        self._send(204, b"")


# ---------------------------------------------------------------------------
# wiring


class RelayHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, relay: Relay):
        self.relay = relay
        super().__init__(address, handler)


def make_server(host: str, port: int, relay: Relay) -> RelayHTTPServer:
    return RelayHTTPServer((host, port), RelayHandler, relay)


def gc_loop(relay: Relay, interval: float = 60.0) -> None:
    while True:
        time.sleep(interval)
        try:
            removed = relay.gc()
        except Exception:  # pragma: no cover - defensive
            LOG.exception("channel gc failed")
            continue
        if removed:
            LOG.info("expired %d channel(s)", removed)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agent Relay - capability-URL message bus for agents")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="bind port (default: 8787; 0 picks a free port)")
    parser.add_argument("--public-url", default=None, help="base URL used in generated guides (behind a proxy)")
    parser.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES, help="messages retained per channel")
    parser.add_argument("--channel-ttl-hours", type=float, default=DEFAULT_TTL_HOURS, help="idle hours before a channel is removed (0 disables)")
    parser.add_argument("--max-channels", type=int, default=DEFAULT_MAX_CHANNELS, help="maximum live channels")
    parser.add_argument("--admin-token", default=None, help="require this token on POST /channels (env: RELAY_ADMIN_TOKEN)")
    parser.add_argument("--log-level", default="info", help="debug, info, warning, error")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    relay = Relay(
        max_messages=args.max_messages,
        ttl_seconds=max(0.0, args.channel_ttl_hours) * 3600,
        max_channels=args.max_channels,
        base_url=args.public_url,
        admin_token=args.admin_token or os.environ.get("RELAY_ADMIN_TOKEN"),
    )
    httpd = make_server(args.host, args.port, relay)
    host, port = httpd.server_address[:2]
    display_host = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    base = relay.base_url or "http://%s:%d" % (display_host, port)
    LOG.info("Agent Relay listening on %s (create: POST %s/channels)", base, base)

    threading.Thread(target=gc_loop, args=(relay,), name="relay-gc", daemon=True).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("shutting down")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
