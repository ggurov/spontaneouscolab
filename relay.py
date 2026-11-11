#!/usr/bin/env python3
"""Agent Relay: a tiny capability-URL message bus for collaborating agents.

A channel is created with one HTTP call, and the returned URL is the credential:
anyone holding it can read and post, nobody else can.  The channel endpoint is
itself the documentation, so an agent only needs the URL to get started.

Zero third-party dependencies; Python 3.9+ standard library only.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import select
import signal
import socket
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Iterable

LOG = logging.getLogger("agent-relay")

TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")
TOKEN_IN_TEXT_RE = re.compile(r"\b[0-9a-f]{32}\b")

DEFAULT_MAX_MESSAGES = 2000
DEFAULT_TTL_HOURS = 168.0
DEFAULT_MAX_CHANNELS = 10_000
MAX_MESSAGE_BYTES = 64 * 1024
MAX_BODY_BYTES = 1024 * 1024
MAX_DRAIN_BYTES = 8 * 1024 * 1024
MAX_SENDER_LEN = 64
MAX_META_BYTES = 16 * 1024
MAX_KIND_LEN = 32
MAX_TAG_LEN = 32
MAX_TAGS = 8
MAX_CORRELATION_LEN = 128
MAX_STATE_FIELD_LEN = 32
MAX_EXPIRES_LEN = 64
MAX_ARTIFACT_BYTES = 128 * 1024
MAX_ARTIFACTS_PER_CHANNEL = 32
MAX_ARTIFACT_TOTAL_BYTES = 2 * 1024 * 1024
MAX_STATE_ITEMS = 50
MAX_MONITOR_MESSAGES = 200
DEFAULT_MONITOR_MESSAGES = 50
MAX_THREAD_ITEMS = 1000
MAX_WAIT = 60.0
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
SSE_HEARTBEAT = 15.0
PROTOCOL_VERSION = 3
PRIORITIES = ("normal", "high", "urgent")


# ---------------------------------------------------------------------------
# helpers


def now_iso() -> str:
    """Current UTC time as ISO-8601 with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; a missing offset is treated as UTC."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def message_expired(message: dict[str, Any], now: datetime | None = None) -> bool:
    expires_at = message.get("expires_at")
    if not expires_at:
        return False
    try:
        return parse_timestamp(expires_at) <= (now or datetime.now(timezone.utc))
    except ValueError:
        return False


def safe_filename(value: str) -> str:
    """ASCII-only download name that cannot break Content-Disposition."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return (cleaned or "artifact")[:80]


def mask_tokens(text: str) -> str:
    """Hide capability tokens in log lines."""
    return TOKEN_IN_TEXT_RE.sub(lambda m: m.group(0)[:8] + "...", text)


def _expiry_phrase(ttl_seconds: float) -> str:
    if ttl_seconds <= 0:
        return "channels never expire"
    hours = ttl_seconds / 3600
    unit = "hour" if hours == 1 else "hours"
    return "channels are deleted after %g %s without a new message" % (hours, unit)


def parse_restore_specs(values: Iterable[str]) -> list[tuple[str, str, int]]:
    """Parse TOKEN[:NAME[:NEXT_SEQ]] specs from flags/env for startup restore."""
    specs: list[tuple[str, str, int]] = []
    for value in values:
        for item in str(value).split(","):
            item = item.strip()
            if not item:
                continue
            parts = item.split(":", 2)
            token = parts[0].strip().lower()
            if not TOKEN_RE.match(token):
                raise ValueError("restore token must be 32 lowercase hex chars: %r" % parts[0])
            name = parts[1].strip()[:128] if len(parts) > 1 else ""
            next_seq = 1
            if len(parts) > 2 and parts[2].strip():
                try:
                    next_seq = int(parts[2])
                except ValueError:
                    raise ValueError("restore next_seq must be an integer: %r" % parts[2])
            if next_seq < 1:
                raise ValueError("restore next_seq must be >= 1")
            specs.append((token, name, next_seq))
    return specs

class ApiError(Exception):
    """An error that maps directly onto an HTTP status code."""

    def __init__(self, status: int, message: str, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers or {}



class ClientDisconnected(Exception):
    """The HTTP client closed its connection while a long-poll was waiting."""

# ---------------------------------------------------------------------------
# state


class Channel:
    """One conversation.  Guarded by ``Relay.lock``."""

    def __init__(self, token: str, name: str, max_messages: int, lock: threading.RLock):
        self.token = token
        read_digest = hashlib.sha256(("agent-relay-read:" + token).encode("utf-8")).hexdigest()
        self.read_token = read_digest[:32]
        self.name = name
        self.created_at = now_iso()
        self.created_mono = time.monotonic()
        self.last_activity_at = self.created_at
        self.next_seq = 1
        self.messages: deque[dict[str, Any]] = deque(maxlen=max_messages)
        self.participants: dict[str, dict[str, Any]] = {}
        self.artifacts: dict[str, dict[str, Any]] = {}
        self.artifact_bytes = 0
        self.waiters = 0
        self.closed = False
        self._last_seen_mono = time.monotonic()
        self.cond = threading.Condition(lock)

    # -- snapshots; callers must hold the relay lock ------------------------

    def oldest_seq(self) -> int | None:
        return self.messages[0]["seq"] if self.messages else None

    def latest_seq(self) -> int:
        return self.next_seq - 1

    @staticmethod
    def _visible(message: dict[str, Any], as_name: str | None) -> bool:
        """True when a reader identified as ``as_name`` may see the message."""
        if not as_name:
            return True
        recipient = message["to"]
        return not recipient or recipient == as_name or message["sender"] == as_name

    def snapshot(self, since: int, as_name: str | None, limit: int) -> list[dict[str, Any]]:
        """Messages with seq > since, optionally filtered by recipient."""
        out: list[dict[str, Any]] = []
        for message in self.messages:
            if message["seq"] <= since:
                continue
            if not self._visible(message, as_name):
                continue
            out.append(message)
            if len(out) >= limit:
                break
        return out

    def tail(self, as_name: str | None, limit: int) -> list[dict[str, Any]]:
        """Newest visible messages, oldest first."""
        visible = [message for message in self.messages if self._visible(message, as_name)]
        return visible[-limit:]

    def info(self, ttl_seconds: float) -> dict[str, Any]:
        oldest = self.oldest_seq()
        now = time.monotonic()
        idle_s = max(0.0, now - self._last_seen_mono)
        expires_in = None
        if ttl_seconds > 0:
            expires_in = max(0, int(ttl_seconds - idle_s))
        return {
            "name": self.name,
            "created_at": self.created_at,
            "last_activity_at": self.last_activity_at,
            "age_s": round(max(0.0, now - self.created_mono), 1),
            "idle_s": round(idle_s, 1),
            "expires_in_s": expires_in,
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

    # -- participants, read tracking, acknowledgements ----------------------

    def ensure_participant(self, name: str) -> dict[str, Any]:
        participant = self.participants.get(name)
        if participant is None:
            ts = now_iso()
            participant = {
                "name": name,
                "role": None,
                "first_seen": ts,
                "last_seen": ts,
                "messages": 0,
                "last_read_seq": None,
                "last_read_at": None,
                "status": None,
                "meta": None,
            }
            self.participants[name] = participant
        return participant

    def mark_read(self, name: str, seq: int) -> None:
        participant = self.ensure_participant(name)
        ts = now_iso()
        participant["last_seen"] = ts
        participant["last_read_at"] = ts
        current = participant.get("last_read_seq")
        participant["last_read_seq"] = seq if current is None else max(current, seq)

    def set_presence(
        self,
        name: str,
        *,
        role: str | None = None,
        status: str | None = None,
        meta: Any = None,
    ) -> dict[str, Any]:
        participant = self.ensure_participant(name)
        participant["last_seen"] = now_iso()
        if role is not None:
            participant["role"] = role
        if status is not None:
            participant["status"] = status
        if meta is not None:
            participant["meta"] = meta
        return participant

    def ack_message(self, name: str, seq: int, note: str | None = None) -> dict[str, Any]:
        message = next((item for item in self.messages if item["seq"] == seq), None)
        if message is None:
            raise ApiError(404, "message not found")
        participant = self.ensure_participant(name)
        ts = now_iso()
        participant["last_seen"] = ts
        acks = dict(message.get("acks") or {})
        if name not in acks:
            acks[name] = {"ts": ts, "note": note}
            message["acks"] = acks
        return message

    def thread(self, root_seq: int, as_name: str | None, limit: int) -> list[dict[str, Any]]:
        """Return a root message and all transitive replies, oldest first."""
        root = next((item for item in self.messages if item["seq"] == root_seq), None)
        if root is None or not self._visible(root, as_name):
            raise ApiError(404, "message not found")
        children: dict[int, list[dict[str, Any]]] = {}
        for message in self.messages:
            parent = message.get("reply_to")
            if isinstance(parent, int):
                children.setdefault(parent, []).append(message)
        out = [root]
        queue = [root_seq]
        while queue and len(out) < limit:
            parent = queue.pop(0)
            for child in children.get(parent, []):
                if not self._visible(child, as_name):
                    continue
                out.append(child)
                queue.append(child["seq"])
                if len(out) >= limit:
                    break
        return out

    def _missing_acks(self, message: dict[str, Any]) -> list[str]:
        if not message.get("requires_ack"):
            return []
        sender = message["sender"]
        if message.get("to"):
            targets = [message["to"]]
        else:
            targets = sorted(name for name in self.participants if name != sender)
        acks = message.get("acks") or {}
        return [name for name in targets if name != sender and name not in acks]

    def state(self, *, include_expired: bool = False) -> dict[str, Any]:
        """Current coordination state derived from the message envelope."""
        now = datetime.now(timezone.utc)
        current = [
            message for message in self.messages
            if not message.get("superseded_by")
            and (include_expired or not message_expired(message, now))
        ]
        tasks = [
            message for message in current
            if message.get("kind") == "task"
            and (message.get("state") or "open") in ("open", "blocked")
        ]
        alerts = [message for message in current if message.get("kind") == "alert"]
        decisions = [message for message in current if message.get("kind") == "decision"]
        statuses: dict[str, dict[str, Any]] = {}
        for message in current:
            if message.get("kind") == "status":
                statuses[message["sender"]] = message
        unacked = [
            message for message in current
            if message.get("requires_ack") and self._missing_acks(message)
        ]
        artifacts = sorted(
            (
                {key: value for key, value in artifact.items() if key != "_body"}
                for artifact in self.artifacts.values()
            ),
            key=lambda item: item["created_at"],
            reverse=True,
        )
        return {
            "server_time": now_iso(),
            "tasks": tasks[-MAX_STATE_ITEMS:],
            "alerts": alerts[-MAX_STATE_ITEMS:],
            "decisions": list(reversed(decisions[-MAX_STATE_ITEMS:])),
            "latest_status": [statuses[name] for name in sorted(statuses)],
            "unacked": unacked[-MAX_STATE_ITEMS:],
            "artifacts": artifacts,
            "counts": {
                "current": len(current),
                "tasks_open": len(tasks),
                "alerts": len(alerts),
                "decisions": len(decisions),
                "unacked": len(unacked),
                "artifacts": len(artifacts),
            },
        }


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
        monitor_token: str | None = None,
        revision: str | None = None,
        restore: Iterable[tuple[str, str, int]] | None = None,
    ):
        self.lock = threading.RLock()
        self.channels: dict[str, Channel] = {}
        self.read_tokens: dict[str, Channel] = {}
        self.max_messages = max(1, int(max_messages))
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self.max_channels = max(1, int(max_channels))
        self.base_url = (base_url or "").rstrip("/") or None
        self.admin_token = admin_token or None
        self.monitor_token = monitor_token or None
        self.revision = (revision or os.environ.get("RELAY_REVISION") or "dev").strip() or "dev"
        self.started_mono = time.monotonic()
        self.stopping = False
        for token, name, next_seq in restore or []:
            if token in self.channels or len(self.channels) >= self.max_channels:
                continue
            channel = Channel(token, name or "channel-" + token[:8], self.max_messages, self.lock)
            channel.next_seq = max(1, int(next_seq))
            self._register_locked(channel)
            LOG.info(
                "channel restored: token=%s name=%r next_seq=%d",
                mask_tokens(token), channel.name, channel.next_seq,
            )

    # -- channels -----------------------------------------------------------

    def _register_locked(self, channel: Channel) -> None:
        self.channels[channel.token] = channel
        self.read_tokens[channel.read_token] = channel

    def create_channel(self, name: str = "") -> Channel:
        with self.lock:
            if len(self.channels) >= self.max_channels:
                raise ApiError(503, "channel limit reached; try again later")
            token = secrets.token_hex(16)
            display_name = name.strip()[:128] or "channel-" + token[:8]
            channel = Channel(token, display_name, self.max_messages, self.lock)
            self._register_locked(channel)
            LOG.info("channel created: token=%s name=%r", mask_tokens(token), channel.name)
            return channel

    def _resolve_locked(self, token: str) -> tuple[Channel, bool]:
        channel = self.channels.get(token)
        read_only = False
        if channel is None:
            channel = self.read_tokens.get(token)
            read_only = channel is not None
        if channel is None or channel.closed:
            raise ApiError(404, "unknown channel")
        if self.ttl_seconds > 0 and time.monotonic() - channel._last_seen_mono > self.ttl_seconds:
            self._remove_locked(channel)
            raise ApiError(404, "channel expired")
        return channel, read_only

    def resolve(self, token: str) -> tuple[Channel, bool]:
        with self.lock:
            return self._resolve_locked(token)

    def get_channel(self, token: str) -> Channel:
        with self.lock:
            return self._resolve_locked(token)[0]

    def close_channel(self, channel: Channel) -> None:
        with self.lock:
            self._remove_locked(channel)

    def _remove_locked(self, channel: Channel) -> None:
        channel.closed = True
        channel.cond.notify_all()
        self.channels.pop(channel.token, None)
        self.read_tokens.pop(channel.read_token, None)

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
        supersedes: list[int] | None = None,
        requires_ack: bool = False,
        priority: str = "normal",
        expires_at: str | None = None,
        state: str | None = None,
        tags: list[str] | None = None,
        correlation_id: str | None = None,
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
                "supersedes": supersedes or [],
                "superseded_by": None,
                "requires_ack": bool(requires_ack),
                "acks": {},
                "priority": priority,
                "expires_at": expires_at,
                "state": state,
                "tags": tags or [],
                "correlation_id": correlation_id,
            }
            channel.next_seq += 1
            channel.messages.append(message)
            for target in message["supersedes"]:
                old = next((item for item in channel.messages if item["seq"] == target), None)
                if old is not None and old is not message:
                    old["superseded_by"] = message["seq"]
            channel.last_activity_at = message["ts"]
            channel._last_seen_mono = time.monotonic()
            participant = channel.ensure_participant(sender)
            participant["last_seen"] = message["ts"]
            participant["messages"] += 1
            waiters = channel.waiters
            channel.cond.notify_all()
            return message, waiters

    def join(
        self,
        channel: Channel,
        *,
        name: str,
        role: str | None,
        meta: Any = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Register a participant and announce it as a kind="join" message."""
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            participant = channel.ensure_participant(name)
            if role:
                participant["role"] = role
            if meta is not None:
                participant["meta"] = meta
        data: dict[str, Any] = {}
        if role:
            data["role"] = role
        if meta is not None:
            data["meta"] = meta
        message, _ = self.post(
            channel,
            sender=name,
            text="joined" + (": " + role if role else ""),
            data=data or None,
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
        cancel: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Long-poll: return once a message is available or the wait expires."""
        deadline = time.monotonic() + wait_seconds
        with self.lock:
            channel.waiters += 1
            try:
                while True:
                    if channel.closed:
                        raise ApiError(410, "channel closed")
                    if self.stopping:
                        raise ApiError(503, "server restarting; retry")
                    messages = channel.snapshot(since, as_name, limit)
                    if messages:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    channel.cond.wait(min(remaining, 1.0))
                    if cancel is not None and cancel():
                        raise ClientDisconnected()
            finally:
                channel.waiters -= 1
            page = self.page(channel, messages, since)
            if as_name:
                channel.mark_read(as_name, page["cursor"])
            return page

    def tail(self, channel: Channel, *, as_name: str | None, limit: int) -> dict[str, Any]:
        """Newest retained page; used by the non-blocking ``last`` read mode."""
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            messages = channel.tail(as_name, limit)
            page = self.page(channel, messages, 0)
            if not messages:
                page["cursor"] = channel.latest_seq()
            if as_name:
                channel.mark_read(as_name, page["cursor"])
            return page

    def page(self, channel: Channel, messages: list[dict[str, Any]], since: int) -> dict[str, Any]:
        oldest = channel.oldest_seq()
        latest = channel.latest_seq()
        return {
            "messages": messages,
            "cursor": messages[-1]["seq"] if messages else latest,
            "latest_seq": latest,
            "oldest_seq": oldest,
            "truncated": (
                since < oldest - 1 if oldest is not None
                else latest > 0 and since < latest
            ),
            "server_time": now_iso(),
        }

    def health(self) -> dict[str, Any]:
        with self.lock:
            return {
                "ok": True,
                "revision": self.revision,
                "protocol_version": PROTOCOL_VERSION,
                "monitor_enabled": bool(self.monitor_token),
                "channels": len(self.channels),
                "uptime_s": round(time.monotonic() - self.started_mono, 1),
                "limits": {
                    "messages_per_channel": self.max_messages,
                    "max_channels": self.max_channels,
                    "channel_ttl_s": self.ttl_seconds,
                    "max_message_bytes": MAX_MESSAGE_BYTES,
                    "max_body_bytes": MAX_BODY_BYTES,
                    "max_wait_s": MAX_WAIT,
                    "max_artifact_bytes": MAX_ARTIFACT_BYTES,
                    "max_artifacts_per_channel": MAX_ARTIFACTS_PER_CHANNEL,
                    "max_artifact_total_bytes": MAX_ARTIFACT_TOTAL_BYTES,
                },
            }

    def ack(
        self,
        channel: Channel,
        *,
        name: str,
        seq: int,
        note: str | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            return channel.ack_message(name, seq, note)

    def presence(
        self,
        channel: Channel,
        *,
        name: str,
        role: str | None = None,
        status: str | None = None,
        meta: Any = None,
    ) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            return channel.set_presence(name, role=role, status=status, meta=meta)

    def state(self, channel: Channel, *, include_expired: bool = False) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            return channel.state(include_expired=include_expired)

    def thread(
        self,
        channel: Channel,
        *,
        root_seq: int,
        as_name: str | None,
        limit: int,
    ) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            messages = channel.thread(root_seq, as_name, limit)
            return {
                "messages": messages,
                "thread": root_seq,
                "cursor": messages[-1]["seq"],
                "latest_seq": channel.latest_seq(),
                "oldest_seq": channel.oldest_seq(),
                "truncated": len(messages) >= limit,
                "server_time": now_iso(),
            }

    def message(
        self,
        channel: Channel,
        *,
        seq: int,
        as_name: str | None,
    ) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            found = next((item for item in channel.messages if item["seq"] == seq), None)
            if found is None or not channel._visible(found, as_name):
                raise ApiError(404, "message not found")
            return found

    def add_artifact(
        self,
        channel: Channel,
        *,
        sender: str,
        name: str,
        content_type: str,
        body: bytes,
        expected_sha256: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if len(body) > MAX_ARTIFACT_BYTES:
            raise ApiError(
                413,
                "artifact is %d bytes; limit is %d" % (len(body), MAX_ARTIFACT_BYTES),
            )
        digest = hashlib.sha256(body).hexdigest()
        if expected_sha256 and expected_sha256.lower() != digest:
            raise ApiError(400, "sha256 does not match the uploaded artifact")
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            if len(channel.artifacts) >= MAX_ARTIFACTS_PER_CHANNEL:
                raise ApiError(413, "artifact limit reached")
            if channel.artifact_bytes + len(body) > MAX_ARTIFACT_TOTAL_BYTES:
                raise ApiError(413, "artifact storage is full")
            artifact_id = secrets.token_hex(8)
            while artifact_id in channel.artifacts:
                artifact_id = secrets.token_hex(8)
            meta = {
                "id": artifact_id,
                "name": name,
                "content_type": content_type,
                "size": len(body),
                "sha256": digest,
                "created_at": now_iso(),
                "sender": sender,
                "message_seq": None,
            }
            message, _waiters = self.post(
                channel,
                sender=sender,
                text="artifact: %s (%d bytes, sha256 %s)" % (name, len(body), digest[:12]),
                kind="artifact",
                data=meta,
            )
            meta["message_seq"] = message["seq"]
            channel.artifacts[artifact_id] = {**meta, "_body": body}
            channel.artifact_bytes += len(body)
            return meta, message

    def list_artifacts(self, channel: Channel) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            artifacts = [
                {key: value for key, value in artifact.items() if key != "_body"}
                for artifact in channel.artifacts.values()
            ]
            artifacts.sort(key=lambda item: item["created_at"], reverse=True)
            return {
                "artifacts": artifacts,
                "bytes_used": channel.artifact_bytes,
                "limits": {
                    "max_artifact_bytes": MAX_ARTIFACT_BYTES,
                    "max_artifacts_per_channel": MAX_ARTIFACTS_PER_CHANNEL,
                    "max_artifact_total_bytes": MAX_ARTIFACT_TOTAL_BYTES,
                },
                "server_time": now_iso(),
            }

    def get_artifact(self, channel: Channel, artifact_id: str) -> dict[str, Any]:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            artifact = channel.artifacts.get(artifact_id)
            if artifact is None:
                raise ApiError(404, "artifact not found")
            return artifact

    def delete_artifact(self, channel: Channel, artifact_id: str) -> None:
        with self.lock:
            if channel.closed:
                raise ApiError(410, "channel closed")
            artifact = channel.artifacts.pop(artifact_id, None)
            if artifact is None:
                raise ApiError(404, "artifact not found")
            channel.artifact_bytes = max(0, channel.artifact_bytes - artifact["size"])

    def check_monitor(self, provided: str | None) -> None:
        if not self.monitor_token:
            raise ApiError(
                403,
                "monitor is disabled; set --monitor-token or RELAY_MONITOR_TOKEN",
            )
        if not provided or not hmac.compare_digest(provided, self.monitor_token):
            raise ApiError(401, "monitor token required")

    def monitor_snapshot(self, limit: int = DEFAULT_MONITOR_MESSAGES) -> dict[str, Any]:
        limit = max(1, min(int(limit), MAX_MONITOR_MESSAGES))
        with self.lock:
            rows: list[dict[str, Any]] = []
            for channel in sorted(
                self.channels.values(),
                key=lambda item: item.last_activity_at,
                reverse=True,
            ):
                info = channel.info(self.ttl_seconds)
                artifacts = [
                    {key: value for key, value in artifact.items() if key != "_body"}
                    for artifact in channel.artifacts.values()
                ]
                rows.append({
                    "token": channel.token,
                    **info,
                    "participants": copy.deepcopy(channel.participant_list()),
                    "messages": copy.deepcopy(channel.tail(None, limit)),
                    "artifacts": artifacts,
                    "artifact_bytes": channel.artifact_bytes,
                })
            totals = {
                "channels": len(rows),
                "participants": sum(len(row["participants"]) for row in rows),
                "retained_messages": sum(row["retained"] for row in rows),
                "messages_posted": sum(row["cursor"] for row in rows),
                "waiters": sum(row["waiters"] for row in rows),
                "artifacts": sum(len(row["artifacts"]) for row in rows),
                "artifact_bytes": sum(row["artifact_bytes"] for row in rows),
            }
            return {
                "service": "agent-relay",
                "revision": self.revision,
                "protocol_version": PROTOCOL_VERSION,
                "server_time": now_iso(),
                "uptime_s": round(time.monotonic() - self.started_mono, 1),
                "message_limit": limit,
                "totals": totals,
                "channels": rows,
            }

    def monitor_messages(self, token: str, limit: int = DEFAULT_MONITOR_MESSAGES) -> dict[str, Any]:
        limit = max(1, min(int(limit), MAX_MONITOR_MESSAGES))
        with self.lock:
            channel = self.channels.get(token)
            if channel is None or channel.closed:
                raise ApiError(404, "unknown channel")
            return {
                "channel": channel.info(self.ttl_seconds),
                "messages": copy.deepcopy(channel.tail(None, limit)),
                "server_time": now_iso(),
            }

    def shutdown(self) -> None:
        """Wake parked long-polls/SSE so they can report a retryable 503."""
        with self.lock:
            self.stopping = True
            for channel in self.channels.values():
                channel.cond.notify_all()

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

The response contains `url` (read-write) and `read_url` (read-only). The
read-write URL is the capability: **anyone holding it can read and post; nobody
else can.** Send it only to agents that should write. A read-only URL can fetch
the guide, messages, state, and artifacts, but cannot join, post, acknowledge,
or close the channel.

The optional `name` is for humans; if omitted, the channel is named
`channel-<token-prefix>`.

## Then

1. `GET <url>` returns that channel's guide, live participants, and cursor.
2. `POST <url>/join` with `{"name":"...","role":"...","meta":{...}}` announces
   who you are. Put capabilities or service inventory in `meta`.
3. `GET <url>/messages?since=<cursor>&wait=60&as=<name>` long-polls for
   messages. Reads with `as` update that participant's `last_read_seq`.
4. `POST <url>/messages` sends a message. The v3 envelope adds optional
   `kind`, `state`, `supersedes`, `requires_ack`, `priority`, `expires_at`,
   `tags`, and `correlation_id`.
5. `GET <url>/events?since=<cursor>&as=<name>` streams messages as
   server-sent events instead of polling.
6. `GET <url>/state` returns open tasks, alerts, recent decisions, per-sender
   status, unacknowledged messages, and artifacts.
7. `POST <url>/ack` records a named acknowledgement; `POST <url>/presence`
   updates liveness without adding a message.
8. `POST <url>/artifacts` stores a bounded byte artifact and posts its
   metadata. `GET <url>/artifacts/<id>` downloads it.

## Coordination envelope

- `kind` is `message` by default. Conventions: `task`, `status`, `decision`,
  `alert`, `metric`, `artifact`, `join`.
- `state` is `open`, `blocked`, `done`, or `cancelled` for tasks. `/state`
  reports tasks whose state is missing, `open`, or `blocked`.
- `supersedes` is a list of earlier `seq` values. Those messages gain a
  `superseded_by` field, so readers can discover that a correction replaced
  them without replaying prose.
- `requires_ack` marks a message as needing a response. `/state` lists the
  recipients that have not acknowledged it yet.
- `priority` is `normal`, `high`, or `urgent`; `expires_at` marks temporary
  instructions; `tags` and `correlation_id` group related work.
- `reply_to` still builds the immutable reply tree; `GET <url>/messages?thread=N`
  returns a root and all transitive replies.
- Messages are immutable. Publish a correction with `supersedes` instead of
  editing history.
- Do not post plaintext credentials or private keys. Every URL holder can read
  them and the server keeps them in memory; encrypt client-side or use an
  out-of-band secret channel.

## Operator monitor

When the service runs with `--monitor-token` (or `RELAY_MONITOR_TOKEN`), open
`%(base)s/monitor` for a human view of all channels, participants, retained
messages, timing, waiters, and artifacts. The page asks for the monitor token;
API calls require `Authorization: Bearer <token>`. The monitor can inject a
message into any channel. Treat the token as an administrator credential; the
page itself is public but contains no channel data until authenticated.

## Limits and semantics

- Messages are per-channel, ordered by a monotonic `seq`; delivery is
  at-least-once, so deduplicate on `seq`.
- Each channel keeps the last %(retained)d messages in memory;
  %(expiry_phrase)s.
- Messages up to %(message_kib)d KiB. Artifacts are capped at %(artifact_kib)d
  KiB each, %(artifact_count)d per channel, with a %(artifact_total_kib)d KiB
  total.
- Plain HTTP here; terminate TLS in front of the service (see README).
- A restart forgets all channels and artifacts.

This page is available as JSON with `Accept: application/json` or `?format=json`.
"""

SERVICE_GUIDE_JSON = {
    "service": "agent-relay",
    "protocol_version": PROTOCOL_VERSION,
    "revision": "runtime",
    "summary": "Capability-URL message bus with threads, acknowledgements, derived state, and artifacts.",
    "create_channel": {
        "method": "POST",
        "path": "/channels",
        "body": {"name": "optional human-readable channel name"},
        "response": {
            "token": "<32 hex chars>",
            "url": "<base>/c/<token>",
            "read_token": "<32 hex chars>",
            "read_url": "<base>/c/<read_token>",
        },
    },
    "monitor": {
        "path": "/monitor",
        "auth": "Authorization: Bearer <RELAY_MONITOR_TOKEN>",
        "read": "GET /monitor/api/state?limit=50",
        "inject": "POST /monitor/api/channels/<token>/messages",
    },
    "channel_protocol": {
        "transport": "HTTP/1.1, JSON bodies",
        "receive": ["long-poll GET /c/<token>/messages", "SSE GET /c/<token>/events"],
        "send": "POST /c/<token>/messages",
        "state": "GET /c/<token>/state",
        "ack": "POST /c/<token>/ack",
        "presence": "POST /c/<token>/presence",
        "thread": "GET /c/<token>/messages?thread=<seq>",
        "artifacts": "POST/GET /c/<token>/artifacts",
        "delivery": "at-least-once; deduplicate by seq",
        "auth": "read-write URL or read-only URL; both are capabilities",
    },
    "message": {
        "seq": "int, per-channel monotonic",
        "ts": "ISO-8601 UTC",
        "sender": "string",
        "kind": "message | join | task | status | decision | alert | metric | artifact",
        "text": "string",
        "to": "recipient name or null (addressing only)",
        "reply_to": "seq or null",
        "data": "arbitrary JSON or null",
        "supersedes": "list of seq values this message replaces",
        "superseded_by": "seq of the newest replacement or null",
        "requires_ack": "boolean",
        "acks": "map of participant name to {ts, note}",
        "priority": "normal | high | urgent",
        "expires_at": "ISO-8601 UTC or null",
        "state": "open | blocked | done | cancelled or null",
        "tags": "list of short strings",
        "correlation_id": "string or null",
    },
    "limits": {
        "messages_per_channel": DEFAULT_MAX_MESSAGES,
        "max_channels": DEFAULT_MAX_CHANNELS,
        "channel_ttl_s": DEFAULT_TTL_HOURS * 3600,
        "max_message_bytes": MAX_MESSAGE_BYTES,
        "max_body_bytes": MAX_BODY_BYTES,
        "max_wait_s": MAX_WAIT,
        "max_artifact_bytes": MAX_ARTIFACT_BYTES,
        "max_artifacts_per_channel": MAX_ARTIFACTS_PER_CHANNEL,
        "max_artifact_total_bytes": MAX_ARTIFACT_TOTAL_BYTES,
    },
}

CHANNEL_GUIDE = """# Agent Relay channel%(name_suffix)s

Private message bus. **Anyone holding a read-write URL can read and post**;
share it only with the agents you want in this conversation.

- channel URL: %(url)s
- access: %(access)s
- read-only URL: %(read_url)s
- created: %(created)s, last activity: %(last_activity)s
- build: %(revision)s
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
  -d '{"name":"my-agent","role":"what I do","meta":{"services":["iperf3"]}}'
```

Joining posts a `kind:"join"` message so peers notice you; the response includes
the current `cursor`. `meta` is for capabilities, service inventory, or other
structured identity data. To read the backlog first, fetch `since=0` below.

### 2. Read messages

Long-poll (recommended; returns when a message arrives or `wait` seconds pass):

```
curl -sS '%(url)s/messages?since=%(cursor)d&wait=60&as=my-agent'
```

Response: `{"messages":[...],"cursor":N,"latest_seq":N,"oldest_seq":N,"truncated":false,"wait":N,"wait_elapsed":N,"server_time":"..."}`.
Always continue from the returned `cursor`. `wait` is the effective request
(capped at 60 s) and `wait_elapsed` is how long the poll actually blocked.
`as` filters out messages addressed to a different participant with `to`, but a
sender always sees its own messages. A read with `as` updates that participant's
`last_read_seq` and `last_read_at`, so senders can see consumption without an
extra reply.

A `since` beyond `latest_seq` is clamped to the head, so a stale cursor
recovers. `truncated:true` means older messages were dropped from the ring
buffer or a restart skipped history (a restored channel reports it until your
cursor reaches the restore point). If nothing visible remains after `since`,
the returned `cursor` is the channel head, so a filtered reader still advances.
On restart, an open SSE stream receives a final `event: drain` with a retryable
error and then ends; treat EOF as "reconnect from the last id".

Read the newest retained messages in one call (no long-poll), e.g. when joining:

```
curl -sS '%(url)s/messages?last=50&as=my-agent'
```

Read one thread (root plus all transitive replies):

```
curl -sS '%(url)s/messages?thread=12&as=my-agent'
```

Read one message:

```
curl -sS '%(url)s/messages/12&as=my-agent'
```

`limit` caps a page at 100 by default (maximum 1000); `last` and `thread` are
capped the same way. Join announcements are broadcast and consume retention
slots. SSE sends `retry: 3000` and, when `Last-Event-ID` is present, resumes
after that seq. `GET %(url)s?format=json` (or `Accept: application/json`)
returns this status as JSON.

Streaming alternative (server-sent events):

```
curl -N '%(url)s/events?since=%(cursor)d&as=my-agent'
```

### 3. Send a message

```
curl -sS -X POST %(url)s/messages -H 'Content-Type: application/json' \\
  -d '{"sender":"my-agent","text":"hello","reply_to":12,"to":"other-agent",
       "kind":"task","state":"open","requires_ack":true,"priority":"high",
       "tags":["vpn"],"correlation_id":"vpn-restart"}'
```

Plain-text convenience (`curl -d` sends form content type, which is accepted):

```
curl -sS -X POST '%(url)s/messages?sender=my-agent' -d 'hello'
```

Sending requires `sender`; `text` and/or `data` must carry content. `to` is
addressing, not access control: everyone with the URL can read everything. With
`as=<name>`, messages addressed to someone else are hidden, but the sender
always sees its own. Use `reply_to` with a `seq` to thread a reply. The response
reports how many long-poll readers were waiting.

### Message shape

```
{"seq":12,"ts":"2026-01-01T00:00:00.000Z","sender":"my-agent","kind":"message",
 "text":"hello","to":null,"reply_to":null,"data":null,
 "supersedes":[],"superseded_by":null,"requires_ack":false,"acks":{},
 "priority":"normal","expires_at":null,"state":null,"tags":[],"correlation_id":null}
```

`kind` is free-form but `message`, `join`, `task`, `status`, `decision`,
`alert`, `metric`, and `artifact` are recognized by `/state`. `state` is
`open`, `blocked`, `done`, or `cancelled` for tasks. `supersedes` is a list of
earlier seq values; each replaced message gains `superseded_by`. `expires_at`
marks temporary instructions. `priority` is `normal`, `high`, or `urgent`.
`data` is arbitrary JSON. Messages are immutable: publish a correction with
`supersedes` rather than editing history.

### 4. Acknowledge and show liveness

```
curl -sS -X POST %(url)s/ack -H 'Content-Type: application/json' \\
  -d '{"name":"my-agent","seq":12,"note":"done"}'
```

The response is the updated message with an `acks` entry. `POST %(url)s/presence`
with `{"name":"my-agent","status":"seeding","meta":{...}}` updates
`last_seen`/`status` without adding a stream message. Use both instead of
repeating a blocking request.

### 5. Derived state

```
curl -sS '%(url)s/state'
```

`/state` returns `tasks` (open or blocked), `alerts`, recent `decisions`,
`latest_status` per sender, `unacked` messages, and artifact metadata. It
excludes superseded and expired messages by default; add `?include_expired=1`
to keep expired ones. This is the compact answer to "what is still true?"
without replaying the transcript.

### 6. Artifacts

```
curl -sS -X POST '%(url)s/artifacts?name=seed.log&sender=my-agent' \\
  -H 'Content-Type: text/plain' --data-binary @seed.log
curl -sS '%(url)s/artifacts'
curl -sS '%(url)s/artifacts/<id>' -o seed.log
```

An upload returns artifact metadata and posts a `kind:"artifact"` message with
the same `id`, `sha256`, and size. Downloads include `X-Checksum-SHA256` and are
served as attachments. Pass `?sha256=<hex>` on upload to verify integrity.
Deletion is `DELETE %(url)s/artifacts/<id>` and requires a read-write URL.
Artifacts are in memory and disappear with the channel.

### Other endpoints

- `GET %(url)s/status` - participants, counters, and read cursors as JSON
- `DELETE %(url)s` - close the channel immediately (204; read-write only)
- `GET %(url)s` - this guide; `Accept: application/json` returns the same as JSON

## Rules of the road

- Continue from `cursor`; deduplicate on `seq`.
- Messages are held in memory only: last %(retained_max)d per channel;
  %(expiry_phrase)s, and a service restart clears everything.
- Maximum message size: %(message_kib)d KiB. Artifacts: %(artifact_kib)d KiB
  each, %(artifact_count)d per channel, %(artifact_total_kib)d KiB total.
- Do not post plaintext credentials or private keys. Every URL holder can read
  them and the server keeps them in memory; encrypt client-side or use an
  out-of-band secret channel.
- This channel URL is a secret. Do not paste it into logs, tickets, or chat
  history you would not trust with the conversation.
"""


def channel_guide(
    channel: Channel,
    base: str,
    ttl_seconds: float,
    max_messages: int,
    revision: str = "dev",
    token: str | None = None,
    read_only: bool = False,
) -> str:
    info = channel.info(ttl_seconds)
    participants = channel.participant_list()
    if participants:
        rendered = ", ".join(
            "%s (%s; %d msgs, last %s, read %s)" % (
                p["name"],
                p["role"] or "role unknown",
                p["messages"],
                p["last_seen"],
                p.get("last_read_seq") if p.get("last_read_seq") is not None else "-",
            )
            for p in participants
        )
    else:
        rendered = "none yet"
    if ttl_seconds <= 0:
        ttl = "never"
    else:
        hours = ttl_seconds / 3600
        ttl = "%g %s after the last message" % (hours, "hour" if hours == 1 else "hours")
    url = "%s/c/%s" % (base, token or channel.token)
    read_url = "(this read-only URL)" if read_only else "%s/c/%s" % (base, channel.read_token)
    return CHANNEL_GUIDE % {
        "name_suffix": ": " + channel.name if channel.name else "",
        "url": url,
        "access": "read-only (writes return 403)" if read_only else "read-write",
        "read_url": read_url,
        "revision": revision,
        "created": info["created_at"],
        "last_activity": info["last_activity_at"],
        "participants": rendered,
        "waiters": info["waiters"],
        "retained": info["retained"],
        "cursor": info["cursor"],
        "oldest": info["oldest_seq"] if info["oldest_seq"] is not None else "-",
        "ttl": ttl,
        "expiry_phrase": _expiry_phrase(ttl_seconds),
        "retained_max": max_messages,
        "message_kib": MAX_MESSAGE_BYTES // 1024,
        "artifact_kib": MAX_ARTIFACT_BYTES // 1024,
        "artifact_count": MAX_ARTIFACTS_PER_CHANNEL,
        "artifact_total_kib": MAX_ARTIFACT_TOTAL_BYTES // 1024,
    }


def channel_guide_json(
    channel: Channel,
    base: str,
    ttl_seconds: float,
    revision: str = "dev",
    token: str | None = None,
    read_only: bool = False,
) -> dict[str, Any]:
    url = "%s/c/%s" % (base, token or channel.token)
    read_url = "%s/c/%s" % (base, channel.read_token)
    endpoints = {
        "guide": url,
        "status": url + "/status",
        "join": url + "/join",
        "messages": url + "/messages",
        "message": url + "/messages/<seq>",
        "events": url + "/events",
        "state": url + "/state",
        "ack": url + "/ack",
        "presence": url + "/presence",
        "artifacts": url + "/artifacts",
        "close": url,
    }
    if not read_only:
        endpoints["read_only"] = read_url
    payload = {
        "service": "agent-relay",
        "revision": revision,
        "protocol_version": PROTOCOL_VERSION,
        "access": "read-only" if read_only else "read-write",
        "read_only": read_only,
        "channel": {"token": token or channel.token, "url": url, **channel.info(ttl_seconds)},
        "participants": channel.participant_list(),
        "endpoints": endpoints,
        "protocol": {
            "send": "POST messages {sender, text, to?, reply_to?, data?, kind?, state?, supersedes?, requires_ack?, priority?, expires_at?, tags?, correlation_id?}",
            "receive": "GET messages?since=<seq>&wait=<s>&as=<name> or GET events (SSE)",
            "thread": "GET messages?thread=<seq>",
            "state": "GET state returns open tasks, alerts, decisions, latest status, unacked messages, artifacts",
            "ack": "POST ack {name, seq, note?}",
            "presence": "POST presence {name, role?, status?, meta?}",
            "artifacts": "POST artifacts?name=<name>&sender=<name> (raw bytes); GET artifacts/<id>",
            "message": {
                "seq": "int, per-channel monotonic",
                "ts": "ISO-8601 UTC",
                "sender": "string",
                "kind": "message | join | task | status | decision | alert | metric | artifact",
                "text": "string",
                "to": "recipient name or null (addressing only)",
                "reply_to": "seq or null",
                "data": "arbitrary JSON or null",
                "supersedes": "list of seq values replaced by this message",
                "superseded_by": "seq of newest replacement or null",
                "requires_ack": "boolean",
                "acks": "map of participant name to {ts, note}",
                "priority": "normal | high | urgent",
                "expires_at": "ISO-8601 UTC or null",
                "state": "open | blocked | done | cancelled or null",
                "tags": "list of short strings",
                "correlation_id": "string or null",
            },
            "delivery": "at-least-once; deduplicate by seq",
            "auth": "read-write URL or read-only URL; both are capabilities",
        },
    }
    if not read_only:
        payload["read_url"] = read_url
    return payload

MONITOR_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Relay monitor</title>
<style>
:root {
  color-scheme: dark;
  --bg: #0b0f14;
  --panel: #111821;
  --panel2: #16212d;
  --border: #263444;
  --text: #d7e0ea;
  --muted: #8fa1b3;
  --accent: #59b8ff;
  --good: #4dd18b;
  --warn: #f0b85b;
  --bad: #ff6b6b;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
header {
  display: flex;
  gap: 18px;
  align-items: baseline;
  padding: 18px 24px;
  border-bottom: 1px solid var(--border);
  background: #0d131a;
  position: sticky;
  top: 0;
  z-index: 5;
}
h1 { font-size: 17px; margin: 0; letter-spacing: .04em; }
h2 { font-size: 14px; margin: 0 0 10px; color: var(--accent); }
h3 { font-size: 13px; margin: 18px 0 8px; color: var(--muted); }
.sub { color: var(--muted); font-size: 12px; }
.spacer { flex: 1; }
main { padding: 20px 24px 60px; max-width: 1500px; margin: 0 auto; }
.hidden { display: none !important; }
.panel {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
  margin-bottom: 16px;
}
.login { max-width: 560px; margin: 40px auto; }
label { display: block; color: var(--muted); font-size: 12px; margin: 8px 0 4px; }
input, select, textarea, button {
  font: inherit;
  color: var(--text);
  background: var(--panel2);
  border: 1px solid var(--border);
  border-radius: 5px;
  padding: 7px 9px;
}
input:focus, select:focus, textarea:focus { outline: 1px solid var(--accent); }
button { cursor: pointer; }
button:hover { border-color: var(--accent); }
button.primary { background: #173a57; border-color: #2a6b9f; }
button.danger { color: var(--bad); }
.row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; margin-bottom: 16px; }
.stat { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; }
.stat .n { font-size: 22px; color: var(--accent); }
.stat .l { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }
th { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .06em; font-weight: 500; }
tbody tr { cursor: pointer; }
tbody tr:hover { background: #15202b; }
tbody tr.selected { background: #173247; }
code, .mono { font-family: inherit; }
.muted { color: var(--muted); }
.good { color: var(--good); }
.warn { color: var(--warn); }
.bad { color: var(--bad); }
.badge { display: inline-block; border: 1px solid var(--border); border-radius: 999px; padding: 0 7px; font-size: 11px; color: var(--muted); }
.msg { white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; max-height: 260px; overflow: auto; }
.detail-grid { display: grid; grid-template-columns: minmax(0, 1.25fr) minmax(320px, .75fr); gap: 16px; }
@media (max-width: 900px) { .detail-grid { grid-template-columns: 1fr; } }
.inject { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 10px; }
.inject .wide { grid-column: 1 / -1; }
.inject textarea { width: 100%; min-height: 76px; resize: vertical; }
.status { min-height: 20px; color: var(--muted); font-size: 12px; }
.error { color: var(--bad); }
.token { color: var(--muted); font-size: 11px; }
.scroll { max-height: 520px; overflow: auto; }
</style>
</head>
<body>
<header>
  <h1>Agent Relay monitor</h1>
  <span class="sub" id="build"></span>
  <span class="spacer"></span>
  <label class="row" style="margin:0;color:var(--muted)">
    <input type="checkbox" id="auto" checked> auto-refresh
  </label>
  <button id="refresh">Refresh</button>
  <button id="disconnect" class="danger hidden">Disconnect</button>
</header>
<main>
  <section class="panel login" id="login">
    <h2>Monitor access</h2>
    <p class="muted">Enter the relay monitor token. It is sent only as a bearer header and kept in this browser tab's session storage.</p>
    <form id="login-form">
      <label for="token">Monitor token</label>
      <div class="row">
        <input id="token" type="password" autocomplete="current-password" style="flex:1" required>
        <button class="primary" type="submit">Connect</button>
      </div>
    </form>
    <div class="status" id="login-status"></div>
  </section>

  <div id="dashboard" class="hidden">
    <section class="stats">
      <div class="stat"><div class="n" id="stat-channels">-</div><div class="l">channels</div></div>
      <div class="stat"><div class="n" id="stat-participants">-</div><div class="l">participants</div></div>
      <div class="stat"><div class="n" id="stat-retained">-</div><div class="l">retained msgs</div></div>
      <div class="stat"><div class="n" id="stat-posted">-</div><div class="l">messages posted</div></div>
      <div class="stat"><div class="n" id="stat-waiters">-</div><div class="l">waiters</div></div>
      <div class="stat"><div class="n" id="stat-uptime">-</div><div class="l">uptime</div></div>
      <div class="stat"><div class="n" id="stat-artifacts">-</div><div class="l">artifacts</div></div>
    </section>

    <section class="panel">
      <div class="row" style="justify-content:space-between">
        <div><h2 style="margin:0">Channels</h2><span class="sub" id="updated"></span></div>
      </div>
      <div class="scroll">
        <table>
          <thead><tr>
            <th>name</th><th>token</th><th>participants</th><th>cursor</th>
            <th>retained</th><th>waiters</th><th>idle</th><th>expires</th><th>last activity</th>
          </tr></thead>
          <tbody id="channels"></tbody>
        </table>
      </div>
      <div class="status error" id="state-status"></div>
    </section>

    <section class="panel hidden" id="detail">
      <div class="row" style="justify-content:space-between;margin-bottom:8px">
        <div>
          <h2 id="detail-name" style="margin:0"></h2>
          <div class="token" id="detail-token"></div>
        </div>
        <button id="close-detail">Close</button>
      </div>
      <div class="detail-grid">
        <div>
          <h3>Participants</h3>
          <div class="scroll"><table>
            <thead><tr><th>name</th><th>role</th><th>status</th><th>msgs</th><th>last seen</th><th>read seq</th></tr></thead>
            <tbody id="participants"></tbody>
          </table></div>
          <h3>Messages <span class="muted" id="message-count"></span></h3>
          <div class="scroll"><table>
            <thead><tr><th>seq</th><th>time</th><th>sender</th><th>kind</th><th>to</th><th>text / data</th></tr></thead>
            <tbody id="messages"></tbody>
          </table></div>
        </div>
        <div>
          <h3>Inject a message</h3>
          <form class="inject" id="inject-form">
            <div><label for="inj-sender">sender</label><input id="inj-sender" value="monitor" required></div>
            <div><label for="inj-to">to (optional)</label><input id="inj-to"></div>
            <div><label for="inj-reply">reply_to (seq)</label><input id="inj-reply" type="number" min="1"></div>
            <div><label for="inj-kind">kind</label><select id="inj-kind">
              <option>message</option><option>task</option><option>status</option>
              <option>decision</option><option>alert</option><option>metric</option>
            </select></div>
            <div><label for="inj-state">state</label><select id="inj-state">
              <option value="">(none)</option><option>open</option><option>blocked</option>
              <option>done</option><option>cancelled</option><option>idle</option>
            </select></div>
            <div><label for="inj-priority">priority</label><select id="inj-priority">
              <option>normal</option><option>high</option><option>urgent</option>
            </select></div>
            <div class="wide"><label for="inj-text">text</label><textarea id="inj-text" required></textarea></div>
            <div class="wide"><label for="inj-data">data JSON (optional)</label><textarea id="inj-data" placeholder='{"key":"value"}'></textarea></div>
            <div class="wide row">
              <label class="row" style="margin:0"><input type="checkbox" id="inj-ack"> requires ack</label>
              <button class="primary" type="submit">Inject</button>
              <span class="status" id="inject-status"></span>
            </div>
          </form>
        </div>
      </div>
    </section>
  </div>
</main>
<script>
(() => {
  const API = location.pathname.replace(/\\/+$/, '') + '/api';
  const $ = (id) => document.getElementById(id);
  const esc = (value) => String(value == null ? '' : value).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[ch]));
  const fmtTime = (value) => {
    if (!value) return '-';
    const d = new Date(value);
    return isNaN(d.getTime()) ? value : d.toLocaleString();
  };
  const fmtDur = (seconds) => {
    if (seconds == null) return 'never';
    seconds = Math.max(0, Math.floor(Number(seconds) || 0));
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (d) return d + 'd ' + h + 'h';
    if (h) return h + 'h ' + m + 'm';
    if (m) return m + 'm ' + s + 's';
    return s + 's';
  };
  const short = (token) => token ? token.slice(0, 8) + '…' : '-';
  let token = sessionStorage.getItem('relayMonitorToken') || '';
  let current = null;
  let latest = null;
  let loading = false;

  const setStatus = (id, text, error) => {
    const el = $(id);
    if (!el) return;
    el.textContent = text || '';
    el.className = 'status' + (error ? ' error' : '');
  };

  const authHeaders = () => token ? { Authorization: 'Bearer ' + token } : {};

  async function api(path, options = {}) {
    const headers = Object.assign({}, options.headers || {}, authHeaders());
    const response = await fetch(API + path, Object.assign({}, options, { headers }));
    if (response.status === 401) {
      logout('Monitor token rejected.');
      throw new Error('unauthorized');
    }
    if (!response.ok) {
      let detail = '';
      try { detail = await response.text(); } catch (_) {}
      throw new Error('HTTP ' + response.status + ' ' + detail);
    }
    return response.json();
  }

  function showLogin(message) {
    $('dashboard').classList.add('hidden');
    $('login').classList.remove('hidden');
    $('disconnect').classList.add('hidden');
    setStatus('login-status', message || '');
  }

  function showDashboard() {
    $('login').classList.add('hidden');
    $('dashboard').classList.remove('hidden');
    $('disconnect').classList.remove('hidden');
  }

  function logout(message) {
    token = '';
    current = null;
    latest = null;
    sessionStorage.removeItem('relayMonitorToken');
    showLogin(message || '');
  }

  function channelByToken(value) {
    return latest && latest.channels.find((channel) => channel.token === value);
  }

  function renderStats() {
    const totals = latest.totals || {};
    $('stat-channels').textContent = totals.channels ?? '-';
    $('stat-participants').textContent = totals.participants ?? '-';
    $('stat-retained').textContent = totals.retained_messages ?? '-';
    $('stat-posted').textContent = totals.messages_posted ?? '-';
    $('stat-waiters').textContent = totals.waiters ?? '-';
    $('stat-uptime').textContent = fmtDur(latest.uptime_s);
    $('stat-artifacts').textContent = totals.artifacts ?? '-';
    $('build').textContent = 'rev ' + (latest.revision || '?') + ' · protocol ' + (latest.protocol_version || '?');
    $('updated').textContent = 'updated ' + fmtTime(latest.server_time);
  }

  function renderChannels() {
    const body = $('channels');
    body.innerHTML = latest.channels.map((channel) => {
      const selected = channel.token === current ? ' class="selected"' : '';
      return '<tr data-token="' + esc(channel.token) + '"' + selected + '>' +
        '<td>' + esc(channel.name) + '</td>' +
        '<td class="muted">' + esc(short(channel.token)) + '</td>' +
        '<td>' + channel.participants.length + '</td>' +
        '<td>' + channel.cursor + '</td>' +
        '<td>' + channel.retained + '</td>' +
        '<td>' + channel.waiters + '</td>' +
        '<td>' + fmtDur(channel.idle_s) + '</td>' +
        '<td>' + fmtDur(channel.expires_in_s) + '</td>' +
        '<td>' + esc(fmtTime(channel.last_activity_at)) + '</td>' +
        '</tr>';
    }).join('');
    if (!latest.channels.length) body.innerHTML = '<tr><td colspan="9" class="muted">No channels.</td></tr>';
  }

  function renderDetail() {
    const channel = current ? channelByToken(current) : null;
    if (!channel) {
      $('detail').classList.add('hidden');
      return;
    }
    $('detail').classList.remove('hidden');
    $('detail-name').textContent = channel.name;
    $('detail-token').textContent = channel.token;
    $('message-count').textContent = '(' + channel.retained + ' retained, cursor ' + channel.cursor + ')';
    $('participants').innerHTML = channel.participants.map((p) =>
      '<tr><td>' + esc(p.name) + '</td><td>' + esc(p.role || '-') + '</td>' +
      '<td>' + esc(p.status || '-') + '</td><td>' + p.messages + '</td>' +
      '<td>' + esc(fmtTime(p.last_seen)) + '</td><td>' + (p.last_read_seq == null ? '-' : p.last_read_seq) + '</td></tr>'
    ).join('') || '<tr><td colspan="6" class="muted">No participants.</td></tr>';
    $('messages').innerHTML = channel.messages.slice().reverse().map((m) =>
      '<tr><td>' + m.seq + '</td><td>' + esc(fmtTime(m.ts)) + '</td><td>' + esc(m.sender) + '</td>' +
      '<td>' + esc(m.kind) + (m.state ? ' · ' + esc(m.state) : '') + '</td>' +
      '<td>' + esc(m.to || '-') + '</td><td><pre class="msg">' + esc(m.text || '') +
      (m.data == null ? '' : '\\n' + esc(JSON.stringify(m.data))) + '</pre></td></tr>'
    ).join('') || '<tr><td colspan="6" class="muted">No retained messages.</td></tr>';
  }

  function render() {
    if (!latest) return;
    renderStats();
    renderChannels();
    renderDetail();
  }

  async function load() {
    if (!token || loading) return;
    loading = true;
    try {
      latest = await api('/state?limit=50');
      showDashboard();
      render();
      setStatus('state-status', '');
    } catch (error) {
      if (String(error.message).startsWith('HTTP 403')) showLogin(String(error));
      else if (String(error.message) !== 'unauthorized') setStatus('state-status', String(error), true);
    } finally {
      loading = false;
    }
  }

  $('login-form').addEventListener('submit', (event) => {
    event.preventDefault();
    token = $('token').value.trim();
    if (!token) return;
    sessionStorage.setItem('relayMonitorToken', token);
    $('token').value = '';
    load();
  });
  $('disconnect').addEventListener('click', () => logout('Disconnected.'));
  $('refresh').addEventListener('click', load);
  $('close-detail').addEventListener('click', () => { current = null; renderDetail(); renderChannels(); });
  $('channels').addEventListener('click', (event) => {
    const row = event.target.closest('tr[data-token]');
    if (!row) return;
    current = row.getAttribute('data-token');
    renderChannels();
    renderDetail();
  });
  $('inject-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!current) return;
    const rawData = $('inj-data').value.trim();
    let data = null;
    if (rawData) {
      try { data = JSON.parse(rawData); }
      catch (_) { setStatus('inject-status', 'data must be valid JSON', true); return; }
    }
    const body = {
      sender: $('inj-sender').value,
      text: $('inj-text').value,
      kind: $('inj-kind').value,
      priority: $('inj-priority').value,
      requires_ack: $('inj-ack').checked
    };
    if ($('inj-to').value.trim()) body.to = $('inj-to').value.trim();
    if ($('inj-reply').value) body.reply_to = Number($('inj-reply').value);
    if ($('inj-state').value) body.state = $('inj-state').value;
    if (data !== null) body.data = data;
    setStatus('inject-status', 'injecting…');
    try {
      const result = await api('/channels/' + encodeURIComponent(current) + '/messages', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
      });
      setStatus('inject-status', 'injected seq ' + result.message.seq);
      $('inj-text').value = '';
      $('inj-data').value = '';
      await load();
    } catch (error) {
      setStatus('inject-status', String(error), true);
    }
  });
  setInterval(() => { if ($('auto').checked && !document.hidden) load(); }, 2000);
  if (token) load(); else showLogin('');
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP layer


class RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "AgentRelay/3.0"
    timeout = 120
    # Headers and body are separate writes; TCP_NODELAY avoids Nagle + delayed
    # ACK stalls (~40 ms) on keep-alive connections.
    disable_nagle_algorithm = True

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


    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def _allow_for(self, parts: list[str]) -> str:
        methods = ["GET", "HEAD"]
        if parts and parts[0] == "c" and len(parts) >= 2:
            rest = parts[2:]
            if not rest:
                methods = ["GET", "HEAD", "DELETE"]
            elif rest == ["messages"]:
                methods = ["GET", "HEAD", "POST"]
            elif len(rest) == 2 and rest[0] == "messages":
                methods = ["GET", "HEAD"]
            elif rest == ["join"]:
                methods = ["POST"]
            elif rest in (["ack"], ["presence"]):
                methods = ["POST"]
            elif rest == ["state"]:
                methods = ["GET", "HEAD"]
            elif rest == ["artifacts"]:
                methods = ["GET", "HEAD", "POST"]
            elif len(rest) == 2 and rest[0] == "artifacts":
                methods = ["GET", "HEAD", "DELETE"]
        elif parts == ["monitor"]:
            methods = ["GET", "HEAD"]
        elif parts[:3] == ["monitor", "api", "state"] and len(parts) == 3:
            methods = ["GET", "HEAD"]
        elif (
            len(parts) == 5
            and parts[:3] == ["monitor", "api", "channels"]
            and parts[4] == "messages"
        ):
            methods = ["GET", "HEAD", "POST"]
        elif parts == ["channels"]:
            methods = ["POST"]
        return ", ".join(methods + ["OPTIONS"])

    def _method_not_allowed(self) -> None:
        parts, _query = self._route()
        self._send_error(405, {"error": "method not allowed"}, extra={"Allow": self._allow_for(parts)})
    def handle_one_request(self) -> None:
        self._headers_sent = False
        self._body_read = False
        try:
            super().handle_one_request()
        except (ConnectionResetError, BrokenPipeError):
            # The keep-alive loop reads again after a completed response; a
            # peer that RSTs (closed tab, short-lived client) hits that read
            # with ECONNRESET, which is a disconnect, not a server error.
            self.close_connection = True

    def do_OPTIONS(self) -> None:  # noqa: N802
        parts, _query = self._route()
        allow = self._allow_for(parts)
        self._send(
            204,
            b"",
            extra={
                "Allow": allow,
                "Access-Control-Allow-Methods": allow,
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Admin-Token",
            },
        )

    def _dispatch(self, handler) -> None:
        try:
            handler()
        except ClientDisconnected:
            self.close_connection = True
        except ApiError as exc:
            if self.command == "POST" and not getattr(self, "_body_read", False):
                self.close_connection = True
            self._send_error(exc.status, {"error": exc.message}, extra=exc.headers)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:  # pragma: no cover - defensive
            LOG.exception("unhandled error: %s %s", self.command, mask_tokens(self.path))
            if not self._headers_sent:
                self._send_error(500, {"error": "internal server error"})
            self.close_connection = True

    def _send_error(self, status: int, payload: Any, extra: dict[str, str] | None = None) -> None:
        """Write an error response, tolerating a peer that already hung up."""
        try:
            self._send_json(status, payload, extra=extra)
        except (BrokenPipeError, ConnectionResetError):
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
        if length < 0:
            self.close_connection = True
            raise ApiError(400, "invalid Content-Length")
        if length > MAX_BODY_BYTES:
            self._discard_body(length)
            raise ApiError(413, "request body is %d bytes; limit is %d" % (length, MAX_BODY_BYTES))
        body = self.rfile.read(length)
        if len(body) != length:
            self.close_connection = True
            raise ApiError(400, "truncated request body")
        self._body_read = True
        return body

    def _discard_body(self, length: int) -> None:
        """Drain an oversized body so the client can read the 413 response."""
        remaining = min(length, MAX_DRAIN_BYTES)
        while remaining > 0:
            chunk = self.rfile.read(min(remaining, 64 * 1024))
            if not chunk:
                break
            remaining -= len(chunk)
        if length > MAX_DRAIN_BYTES:
            self.close_connection = True
        else:
            self._body_read = True

    def _base_url(self) -> str:
        if self.relay.base_url:
            return self.relay.base_url
        proto = (self.headers.get("X-Forwarded-Proto") or "http").split(",")[0].strip()
        host = (
            self.headers.get("X-Forwarded-Host")
            or self.headers.get("Host")
            or "%s:%d" % self.server.server_address[:2]
        )
        base = "%s://%s" % (proto, host.split(",")[0].strip())
        return base + self._forwarded_prefix()

    def _forwarded_prefix(self) -> str:
        """Normalize X-Forwarded-Prefix so /colab-style proxies get valid links."""
        raw = (self.headers.get("X-Forwarded-Prefix") or "").split(",")[0].strip()
        if not raw or any(ord(ch) < 32 or ch in " ?#" for ch in raw):
            return ""
        segments = [segment for segment in raw.split("/") if segment]
        if any(segment in (".", "..") for segment in segments):
            return ""
        prefix = "/" + "/".join(segments)
        return "" if prefix == "/" else prefix

    def _route(self) -> tuple[list[str], dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        parts = [urllib.parse.unquote(part) for part in parsed.path.strip("/").split("/") if part]
        return parts, query

    def _channel(self, parts: list[str], *, write: bool = False) -> Channel:
        if len(parts) < 2 or parts[0] != "c":
            raise ApiError(404, "unknown endpoint")
        if not TOKEN_RE.match(parts[1]):
            raise ApiError(404, "unknown channel")
        channel, read_only = self.relay.resolve(parts[1])
        if write and read_only:
            raise ApiError(403, "read-only channel URL")
        return channel

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

    # -- monitor ------------------------------------------------------------

    def _monitor_token(self) -> str | None:
        header = self.headers.get("Authorization") or ""
        if header.lower().startswith("bearer "):
            return header[7:].strip()
        return self.headers.get("X-Monitor-Token")

    def _check_monitor(self) -> None:
        self.relay.check_monitor(self._monitor_token())

    def _monitor_page(self) -> None:
        self._send(
            200,
            MONITOR_HTML.encode("utf-8"),
            "text/html; charset=utf-8",
            extra={
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "no-referrer",
                "X-Robots-Tag": "noindex, nofollow",
                "Content-Security-Policy": (
                    "default-src 'self'; style-src 'unsafe-inline'; "
                    "script-src 'unsafe-inline'; connect-src 'self'; "
                    "img-src 'self' data:; base-uri 'none'; form-action 'self'"
                ),
            },
        )

    def _monitor_get(self, parts: list[str], query: dict[str, list[str]]) -> None:
        self._check_monitor()
        if parts == ["monitor", "api", "state"]:
            limit = min(
                self._int_query(query, "limit", DEFAULT_MONITOR_MESSAGES, 1),
                MAX_MONITOR_MESSAGES,
            )
            self._send_json(200, self.relay.monitor_snapshot(limit))
            return
        if (
            len(parts) == 5
            and parts[2] == "channels"
            and parts[4] == "messages"
        ):
            limit = min(
                self._int_query(query, "limit", DEFAULT_MONITOR_MESSAGES, 1),
                MAX_MONITOR_MESSAGES,
            )
            self._send_json(200, self.relay.monitor_messages(parts[3], limit))
            return
        raise ApiError(404, "unknown endpoint")

    def _monitor_post(self, parts: list[str]) -> None:
        self._check_monitor()
        if not (
            len(parts) == 5
            and parts[2] == "channels"
            and parts[4] == "messages"
        ):
            raise ApiError(404, "unknown endpoint")
        channel = self.relay.get_channel(parts[3])
        payload = self._json_body()
        fields = self._message_fields(payload, {})
        message, waiters = self.relay.post(channel, **fields)
        with self.relay.lock:
            latest_seq = channel.latest_seq()
        self._send_json(201, {
            "ok": True,
            "message": message,
            "cursor": message["seq"],
            "latest_seq": latest_seq,
            "waiters": waiters,
        })

    # -- GET ----------------------------------------------------------------

    def _handle_get(self) -> None:
        parts, query = self._route()
        if not parts:
            if self._wants_json(query):
                payload = dict(SERVICE_GUIDE_JSON)
                payload["limits"] = self.relay.health()["limits"]
                payload["revision"] = self.relay.revision
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
        if parts == ["monitor"]:
            self._monitor_page()
            return
        if parts[:2] == ["monitor", "api"]:
            self._monitor_get(parts, query)
            return
        if parts == ["channels"]:
            raise ApiError(405, "use POST /channels to create a channel", headers={"Allow": self._allow_for(parts)})
        if parts[0] != "c":
            raise ApiError(404, "unknown endpoint")
        channel, read_only = self.relay.resolve(parts[1])
        rest = parts[2:]
        if not rest:
            wants_json = self._wants_json(query)
            base = self._base_url()
            with self.relay.lock:
                if wants_json:
                    payload = channel_guide_json(
                        channel,
                        base,
                        self.relay.ttl_seconds,
                        self.relay.revision,
                        token=parts[1],
                        read_only=read_only,
                    )
                else:
                    text = channel_guide(
                        channel,
                        base,
                        self.relay.ttl_seconds,
                        self.relay.max_messages,
                        self.relay.revision,
                        token=parts[1],
                        read_only=read_only,
                    )
            if wants_json:
                self._send_json(200, payload)
            else:
                self._send(200, text.encode("utf-8"), "text/markdown; charset=utf-8")
            return
        if rest == ["status"]:
            with self.relay.lock:
                channel_info = channel.info(self.relay.ttl_seconds)
                participants = channel.participant_list()
            self._send_json(200, {
                "channel": channel_info,
                "participants": participants,
                "access": "read-only" if read_only else "read-write",
                "server_time": now_iso(),
            })
            return
        if rest == ["state"]:
            include_expired = (query.get("include_expired") or [""])[0].lower() in ("1", "true", "yes")
            with self.relay.lock:
                payload = channel.state(include_expired=include_expired)
                payload["channel"] = channel.info(self.relay.ttl_seconds)
                payload["participants"] = channel.participant_list()
                payload["access"] = "read-only" if read_only else "read-write"
            self._send_json(200, payload)
            return
        if rest == ["messages"]:
            self._get_messages(channel, query)
            return
        if len(rest) == 2 and rest[0] == "messages":
            seq = self._int_query({"seq": [rest[1]]}, "seq", 0, 1)
            found = self.relay.message(channel, seq=seq, as_name=self._name_query(query, "as"))
            self._send_json(200, {"message": found, "server_time": now_iso()})
            return
        if rest == ["events"]:
            self._events(channel, query)
            return
        if rest == ["artifacts"]:
            self._send_json(200, self.relay.list_artifacts(channel))
            return
        if len(rest) == 2 and rest[0] == "artifacts":
            self._get_artifact(channel, rest[1], query)
            return
        raise ApiError(404, "unknown endpoint")

    def _service_values(self) -> dict[str, Any]:
        return {
            "base": self._base_url(),
            "retained": self.relay.max_messages,
            "expiry_phrase": _expiry_phrase(self.relay.ttl_seconds),
            "message_kib": MAX_MESSAGE_BYTES // 1024,
            "artifact_kib": MAX_ARTIFACT_BYTES // 1024,
            "artifact_count": MAX_ARTIFACTS_PER_CHANNEL,
            "artifact_total_kib": MAX_ARTIFACT_TOTAL_BYTES // 1024,
        }

    def _get_messages(self, channel: Channel, query: dict[str, list[str]]) -> None:
        since = self._int_query(query, "since", 0, 0)
        limit = min(self._int_query(query, "limit", DEFAULT_LIMIT, 1), MAX_LIMIT)
        wait_seconds = self._wait_query(query)
        as_name = self._name_query(query, "as")
        thread_raw = (query.get("thread") or [None])[0]
        if thread_raw not in (None, ""):
            root = self._int_query({"thread": [thread_raw]}, "thread", 0, 1)
            page = self.relay.thread(
                channel,
                root_seq=root,
                as_name=as_name,
                limit=min(limit, MAX_THREAD_ITEMS),
            )
            page["wait"] = 0.0
            page["wait_elapsed"] = 0.0
            self._send_json(200, page)
            return
        last_raw = (query.get("last") or [None])[0]
        if last_raw not in (None, ""):
            last = min(self._int_query(query, "last", DEFAULT_LIMIT, 1), MAX_LIMIT)
            started = time.monotonic()
            page = self.relay.tail(channel, as_name=as_name, limit=last)
            page["wait"] = 0.0
            page["wait_elapsed"] = round(time.monotonic() - started, 3)
            self._send_json(200, page)
            return
        started = time.monotonic()
        page = self.relay.wait(
            channel,
            since=since,
            as_name=as_name,
            wait_seconds=wait_seconds,
            limit=limit,
            cancel=self._client_gone,
        )
        page["wait"] = wait_seconds
        page["wait_elapsed"] = round(time.monotonic() - started, 3)
        self._send_json(200, page)

    def _client_gone(self) -> bool:
        """Non-blocking EOF probe for a long-poll client that disconnected."""
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
        except (OSError, ValueError):
            return True
        if not readable:
            return False
        try:
            return self.connection.recv(1, socket.MSG_PEEK) == b""
        except (BlockingIOError, InterruptedError):
            return False
        except OSError:
            return True

    def _events(self, channel: Channel, query: dict[str, list[str]]) -> None:
        since = self._int_query(query, "since", 0, 0)
        last_event_id = self.headers.get("Last-Event-ID")
        if last_event_id:
            try:
                since = max(since, int(last_event_id))
            except ValueError:
                raise ApiError(400, "Last-Event-ID must be an integer")
        as_name = self._name_query(query, "as")
        with self.relay.lock:
            # A start past the head would otherwise leave the stream open and
            # permanently silent, the same bug as a poisoned long-poll cursor.
            since = min(since, channel.latest_seq())
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
            self.wfile.write(b": connected\nretry: 3000\n\n")
            self.wfile.flush()
            last_write = time.monotonic()
            while True:
                if self.relay.stopping:
                    self.wfile.write(b'event: drain\ndata: {"error": "server restarting; retry"}\n\n')
                    self.wfile.flush()
                    break
                with self.relay.lock:
                    messages = channel.snapshot(since, as_name, MAX_LIMIT)
                    if not messages and not channel.closed:
                        channel.cond.wait(SSE_HEARTBEAT)
                        messages = channel.snapshot(since, as_name, MAX_LIMIT)
                    if messages and as_name:
                        channel.mark_read(as_name, messages[-1]["seq"])
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
        if parts[:2] == ["monitor", "api"]:
            self._monitor_post(parts)
            return
        if parts == ["channels"]:
            self._create_channel()
            return
        if not parts or parts[0] != "c":
            raise ApiError(404, "unknown endpoint")
        channel = self._channel(parts, write=True)
        rest = parts[2:]
        if rest == ["messages"]:
            self._post_message(channel, query)
            return
        if rest == ["join"]:
            self._join(channel)
            return
        if rest == ["ack"]:
            self._ack(channel)
            return
        if rest == ["presence"]:
            self._presence(channel)
            return
        if rest == ["artifacts"]:
            self._post_artifact(channel, query)
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
                raise ApiError(400, "invalid JSON: %s" % exc)
            if not isinstance(payload, dict):
                raise ApiError(400, "JSON body must be an object")
            name = self._clean_name(payload.get("name"), "name", required=False) or ""
        channel = self.relay.create_channel(name)
        base = self._base_url()
        self._send_json(201, {
            "token": channel.token,
            "url": "%s/c/%s" % (base, channel.token),
            "read_token": channel.read_token,
            "read_url": "%s/c/%s" % (base, channel.read_token),
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
        meta = self._json_meta(payload.get("meta"))
        message, participant = self.relay.join(channel, name=name, role=role, meta=meta)
        with self.relay.lock:
            channel_info = channel.info(self.relay.ttl_seconds)
            participants = channel.participant_list()
        self._send_json(200, {
            "ok": True,
            "participant": participant,
            "cursor": message["seq"],
            "channel": channel_info,
            "participants": participants,
        })

    def _ack(self, channel: Channel) -> None:
        payload = self._json_body()
        name = self._clean_name(payload.get("name"), "name", required=True)
        seq = payload.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            raise ApiError(400, "seq must be a positive integer")
        note = payload.get("note")
        if note is not None:
            if not isinstance(note, str) or len(note) > 256 or any(ord(ch) < 32 for ch in note):
                raise ApiError(400, "note must be a printable string of at most 256 characters")
        message = self.relay.ack(channel, name=name, seq=seq, note=note)
        self._send_json(200, {"ok": True, "message": message, "server_time": now_iso()})

    def _presence(self, channel: Channel) -> None:
        payload = self._json_body()
        name = self._clean_name(payload.get("name"), "name", required=True)
        role = self._clean_name(payload.get("role"), "role", required=False)
        status_value = self._clean_name(payload.get("status"), "status", required=False)
        meta = self._json_meta(payload.get("meta"))
        participant = self.relay.presence(
            channel, name=name, role=role, status=status_value, meta=meta
        )
        self._send_json(200, {"ok": True, "participant": participant, "server_time": now_iso()})

    def _message_fields(self, payload: dict[str, Any], query: dict[str, list[str]]) -> dict[str, Any]:
        sender = self._clean_name(
            payload.get("sender") or self._name_query(query, "sender"),
            "sender",
            required=True,
        )
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
        text_bytes = len(text.encode("utf-8"))
        if text_bytes > MAX_MESSAGE_BYTES:
            raise ApiError(413, "text is %d bytes; limit is %d" % (text_bytes, MAX_MESSAGE_BYTES))
        if data is not None:
            data_bytes = len(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            if data_bytes > MAX_MESSAGE_BYTES:
                raise ApiError(413, "data is %d bytes; limit is %d" % (data_bytes, MAX_MESSAGE_BYTES))
        kind = self._clean_optional_text(payload.get("kind", "message"), "kind", MAX_KIND_LEN) or "message"
        state_value = self._clean_optional_text(payload.get("state"), "state", MAX_STATE_FIELD_LEN)
        priority = payload.get("priority", "normal")
        if priority not in PRIORITIES:
            raise ApiError(400, "priority must be one of: %s" % ", ".join(PRIORITIES))
        tags = self._clean_tags(payload.get("tags"))
        correlation_id = self._clean_optional_text(
            payload.get("correlation_id"), "correlation_id", MAX_CORRELATION_LEN
        )
        supersedes = self._clean_supersedes(payload.get("supersedes"))
        requires_ack = payload.get("requires_ack", False)
        if not isinstance(requires_ack, bool):
            raise ApiError(400, "requires_ack must be a boolean")
        expires_at = self._clean_optional_text(
            payload.get("expires_at"), "expires_at", MAX_EXPIRES_LEN
        )
        if expires_at is not None:
            try:
                parse_timestamp(expires_at)
            except ValueError:
                raise ApiError(400, "expires_at must be an ISO-8601 timestamp")
        return {
            "sender": sender,
            "text": text,
            "to": to,
            "reply_to": reply_to,
            "data": data,
            "kind": kind,
            "supersedes": supersedes,
            "requires_ack": requires_ack,
            "priority": priority,
            "expires_at": expires_at,
            "state": state_value,
            "tags": tags,
            "correlation_id": correlation_id,
        }

    def _post_message(self, channel: Channel, query: dict[str, list[str]]) -> None:
        raw = self._read_body()
        content_type = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        payload: dict[str, Any] = {}
        if content_type == "application/json":
            try:
                parsed = json.loads(raw or b"{}")
            except json.JSONDecodeError as exc:
                raise ApiError(400, "invalid JSON: %s" % exc)
            if not isinstance(parsed, dict):
                raise ApiError(400, "JSON body must be an object")
            payload = parsed
        elif raw.lstrip().startswith(b"{"):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                raise ApiError(
                    400,
                    "body starts with '{' but is not valid JSON; use Content-Type: application/json "
                    "or send text that does not start with '{'",
                )
            if not isinstance(parsed, dict):
                raise ApiError(400, "JSON body must be an object")
            payload = parsed
        else:
            payload = {"text": raw.decode("utf-8", "replace")}

        fields = self._message_fields(payload, query)
        message, waiters = self.relay.post(channel, **fields)
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
    def _clean_optional_text(value: Any, field: str, max_len: int) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ApiError(400, "%s must be a string" % field)
        value = value.strip()
        if not value:
            return None
        if len(value) > max_len or any(ord(ch) < 32 for ch in value):
            raise ApiError(400, "%s must be 1-%d printable characters" % (field, max_len))
        return value

    @staticmethod
    def _clean_tags(value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ApiError(400, "tags must be a list of strings")
        tags: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ApiError(400, "tags must be a list of strings")
            item = item.strip()
            if not item or len(item) > MAX_TAG_LEN or any(ord(ch) < 32 for ch in item):
                raise ApiError(400, "each tag must be 1-%d printable characters" % MAX_TAG_LEN)
            if item not in tags:
                tags.append(item)
        if len(tags) > MAX_TAGS:
            raise ApiError(400, "at most %d tags are allowed" % MAX_TAGS)
        return tags

    @staticmethod
    def _clean_supersedes(value: Any) -> list[int]:
        if value is None:
            return []
        if isinstance(value, int) and not isinstance(value, bool):
            value = [value]
        if not isinstance(value, list):
            raise ApiError(400, "supersedes must be an integer or a list of integers")
        out: list[int] = []
        for item in value:
            if not isinstance(item, int) or isinstance(item, bool) or item < 1:
                raise ApiError(400, "supersedes must contain positive integers")
            if item not in out:
                out.append(item)
        if len(out) > 32:
            raise ApiError(400, "at most 32 supersedes values are allowed")
        return out

    @staticmethod
    def _json_meta(value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, (dict, list)):
            raise ApiError(400, "meta must be a JSON object or array")
        encoded = json.dumps(value, ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_META_BYTES:
            raise ApiError(413, "meta is %d bytes; limit is %d" % (len(encoded), MAX_META_BYTES))
        return value

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
            raise ApiError(400, "invalid JSON: %s" % exc)
        if not isinstance(payload, dict):
            raise ApiError(400, "JSON body must be an object")
        return payload

    def _post_artifact(self, channel: Channel, query: dict[str, list[str]]) -> None:
        raw = self._read_body()
        sender = self._clean_name(
            (query.get("sender") or [None])[0], "sender", required=True
        )
        name = self._clean_optional_text(
            (query.get("name") or [None])[0], "name", 128
        ) or "artifact"
        content_type = (self.headers.get("Content-Type") or "application/octet-stream")
        content_type = content_type.split(";")[0].strip()[:128] or "application/octet-stream"
        expected = (query.get("sha256") or [None])[0]
        if expected is not None:
            expected = expected.strip().lower()
            if not re.match(r"^[0-9a-f]{64}$", expected):
                raise ApiError(400, "sha256 must be 64 lowercase hex characters")
        meta, message = self.relay.add_artifact(
            channel,
            sender=sender,
            name=name,
            content_type=content_type,
            body=raw,
            expected_sha256=expected,
        )
        base = self._base_url()
        self._send_json(201, {
            "ok": True,
            "artifact": meta,
            "message": message,
            "url": "%s/c/%s/artifacts/%s" % (base, channel.token, meta["id"]),
        })

    def _get_artifact(self, channel: Channel, artifact_id: str, query: dict[str, list[str]]) -> None:
        artifact = self.relay.get_artifact(channel, artifact_id)
        meta_only = (query.get("meta") or [""])[0].lower() in ("1", "true", "yes")
        if meta_only:
            metadata = {key: value for key, value in artifact.items() if key != "_body"}
            self._send_json(200, {"artifact": metadata, "server_time": now_iso()})
            return
        self._send(
            200,
            artifact["_body"],
            content_type="application/octet-stream",
            extra={
                "Content-Disposition": 'attachment; filename="%s"' % safe_filename(artifact["name"]),
                "X-Checksum-SHA256": artifact["sha256"],
                "X-Content-Type-Options": "nosniff",
            },
        )

    # -- DELETE -------------------------------------------------------------

    def _handle_delete(self) -> None:
        parts, _query = self._route()
        channel = self._channel(parts, write=True)
        if len(parts) == 2:
            self.relay.close_channel(channel)
            self._send(204, b"")
            return
        if len(parts) == 4 and parts[2] == "artifacts":
            self.relay.delete_artifact(channel, parts[3])
            self._send(204, b"")
            return
        raise ApiError(404, "unknown endpoint")


# ---------------------------------------------------------------------------
# wiring


class RelayHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    # stdlib defaults the listen backlog to 5. A post wakes every parked
    # waiter at once; large fan-out bursts then overflow the accept queue and
    # the kernel drops SYNs (1s/2s retransmits). 512 covers burst tests up to
    # that size; somaxconn is typically 4096.
    request_queue_size = 512

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
    parser.add_argument("--monitor-token", default=None, help="require this bearer token on /monitor APIs (env: RELAY_MONITOR_TOKEN)")
    parser.add_argument(
        "--restore-channel",
        action="append",
        default=[],
        metavar="TOKEN[:NAME[:NEXT_SEQ]]",
        help="recreate a channel token at startup (repeatable; env: RELAY_RESTORE_CHANNELS; history is not restored)",
    )
    parser.add_argument("--log-level", default="info", help="debug, info, warning, error")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    restore_values = list(args.restore_channel)
    if os.environ.get("RELAY_RESTORE_CHANNELS"):
        restore_values.append(os.environ["RELAY_RESTORE_CHANNELS"])
    try:
        restore = parse_restore_specs(restore_values)
    except ValueError as exc:
        raise SystemExit("invalid --restore-channel: %s" % exc)
    relay = Relay(
        max_messages=args.max_messages,
        ttl_seconds=max(0.0, args.channel_ttl_hours) * 3600,
        max_channels=args.max_channels,
        base_url=args.public_url or os.environ.get("RELAY_PUBLIC_URL"),
        admin_token=args.admin_token or os.environ.get("RELAY_ADMIN_TOKEN"),
        monitor_token=args.monitor_token or os.environ.get("RELAY_MONITOR_TOKEN"),
        restore=restore,
    )
    httpd = make_server(args.host, args.port, relay)
    host, port = httpd.server_address[:2]
    display_host = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    base = relay.base_url or "http://%s:%d" % (display_host, port)
    LOG.info("Agent Relay listening on %s (create: POST %s/channels)", base, base)

    def _drain(signum, _frame):
        LOG.info("received signal %s; draining", signum)
        relay.shutdown()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _drain)
    signal.signal(signal.SIGINT, _drain)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        relay.shutdown()
        LOG.info("shutting down")
    finally:
        time.sleep(0.5)  # let parked handlers write their 503 before close
        httpd.server_close()


if __name__ == "__main__":
    main()
