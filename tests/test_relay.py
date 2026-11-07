"""End-to-end tests for the relay HTTP surface."""

import http.client
import json
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from types import SimpleNamespace

import pytest

import relay


# ---------------------------------------------------------------------------
# fixtures and helpers


@pytest.fixture()
def start_server():
    servers = []

    def _start(**relay_kwargs):
        relay_obj = relay.Relay(**relay_kwargs)
        httpd = relay.make_server("127.0.0.1", 0, relay_obj)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        host, port = httpd.server_address[:2]
        server = SimpleNamespace(
            base="http://%s:%d" % (host, port),
            host=host,
            port=port,
            relay=relay_obj,
            httpd=httpd,
            thread=thread,
        )
        servers.append(server)
        return server

    yield _start

    for server in servers:
        server.httpd.shutdown()
        server.httpd.server_close()
        server.thread.join(timeout=5)


@pytest.fixture()
def server(start_server):
    return start_server(max_messages=500, ttl_seconds=3600)


def request(base, method, path, *, json_body=None, data=None, headers=None, timeout=15):
    url = base + path
    request_headers = {"Accept": "application/json"}
    body = None
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    elif data is not None:
        body = data.encode("utf-8") if isinstance(data, str) else data
    request_headers.update(headers or {})
    req = urllib.request.Request(url, data=body, method=method, headers=request_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as err:
        return err.code, err.read(), dict(err.headers)


def api(base, method, path, **kwargs):
    status, raw, headers = request(base, method, path, **kwargs)
    return status, (json.loads(raw) if raw else None), headers


def create_channel(server, name="test"):
    status, payload, _ = api(server.base, "POST", "/channels", json_body={"name": name})
    assert status == 201, payload
    return payload["token"], payload["url"], payload


def path_of(url):
    return urllib.parse.urlsplit(url).path


def post(server, url, sender="alice", text="hello", **extra):
    body = {"sender": sender, "text": text}
    body.update(extra)
    status, payload, _ = api(server.base, "POST", path_of(url) + "/messages", json_body=body)
    assert status == 201, payload
    return payload


def read(server, url, since=0, wait=0, as_name=None, limit=None):
    query = "?since=%d&wait=%g" % (since, wait)
    if as_name:
        query += "&as=" + urllib.parse.quote(as_name)
    if limit is not None:
        query += "&limit=%d" % limit
    status, payload, _ = api(server.base, "GET", path_of(url) + "/messages" + query)
    assert status == 200, payload
    return payload


def join(server, url, name, role=None):
    body = {"name": name}
    if role is not None:
        body["role"] = role
    status, payload, _ = api(server.base, "POST", path_of(url) + "/join", json_body=body)
    assert status == 200, payload
    return payload


def sse(server, url, since=0, as_name=None, headers=None):
    query = "/events?since=%d" % since
    if as_name:
        query += "&as=" + urllib.parse.quote(as_name)
    connection = http.client.HTTPConnection(server.host, server.port, timeout=10)
    connection.request("GET", path_of(url) + query, headers=headers or {})
    response = connection.getresponse()
    return connection, response


def read_sse_event(response, timeout=8.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = response.fp.readline()
        if not line:
            raise AssertionError("stream closed before an event arrived")
        if line.startswith(b"data:"):
            return json.loads(line.split(b"data:", 1)[1])
    raise AssertionError("no SSE event within %gs" % timeout)


def wait_for_waiters(server, url, count=1, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, payload, _ = api(server.base, "GET", path_of(url) + "/status")
        assert status == 200, payload
        if payload["channel"]["waiters"] >= count:
            return payload
        time.sleep(0.05)
    raise AssertionError("no %d long-poll waiter(s) within %gs" % (count, timeout))


# ---------------------------------------------------------------------------
# guides and creation


def test_service_guide_markdown(server):
    status, raw, headers = request(
        server.base, "GET", "/", headers={"Accept": "text/markdown"}
    )
    text = raw.decode("utf-8")
    assert status == 200
    assert headers["Content-Type"].startswith("text/markdown")
    assert "POST %s/channels" % server.base in text
    assert "long-poll" in text
    assert "capability" in text


def test_service_guide_json_reflects_limits(server):
    status, payload, _ = api(server.base, "GET", "/")
    assert status == 200
    assert payload["service"] == "agent-relay"
    assert payload["limits"]["messages_per_channel"] == 500

def test_create_channel_and_fetch_guide(server):
    token, url, created = create_channel(server, "hil-rig")
    assert len(token) == 32
    assert url.endswith("/c/" + token)
    assert created["channel"]["name"] == "hil-rig"

    status, raw, headers = request(
        server.base, "GET", path_of(url), headers={"Accept": "text/markdown"}
    )
    guide = raw.decode("utf-8")
    assert status == 200
    assert headers["Cache-Control"] == "no-store"
    assert url in guide
    assert "Anonymous" not in guide
    assert "hil-rig" in guide
    assert "/messages?since=" in guide

    status, payload, _ = api(server.base, "GET", path_of(url))
    assert status == 200
    assert payload["channel"]["token"] == token
    assert payload["endpoints"]["events"].endswith("/events")
    assert payload["protocol"]["delivery"].startswith("at-least-once")

    status, raw, _ = request(
        server.base, "GET", path_of(url) + "?format=json", headers={"Accept": "text/markdown"}
    )
    assert status == 200
    assert json.loads(raw)["channel"]["token"] == token


def test_guide_links_follow_forwarded_headers(server):
    token, url, _ = create_channel(server)
    forwarded = {
        "Accept": "text/markdown",
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": "relay.example.com",
    }
    status, raw, _ = request(server.base, "GET", path_of(url), headers=forwarded)
    assert status == 200
    assert ("https://relay.example.com/c/" + token) in raw.decode("utf-8")

    status, payload, _ = api(
        server.base,
        "GET",
        path_of(url),
        headers={"X-Forwarded-Proto": "https", "X-Forwarded-Host": "relay.example.com"},
    )
    assert payload["channel"]["url"].startswith("https://relay.example.com/c/")


def test_channel_creation_invalid_json(server):
    status, payload, _ = api(server.base, "POST", "/channels", data="{not json",
                             headers={"Content-Type": "application/json"})
    assert status == 400
    assert "invalid JSON" in payload["error"]


def test_channel_name_must_be_single_line(server):
    status, payload, _ = api(server.base, "POST", "/channels", json_body={"name": "bad\nname"})
    assert status == 400
    assert "name" in payload["error"]


def test_unnamed_channel_gets_generated_name(server):
    status, payload, _ = api(server.base, "POST", "/channels", json_body={})
    assert status == 201
    name = payload["channel"]["name"]
    assert name.startswith("channel-")
    assert len(name) == len("channel-") + 8


def test_channel_creation_requires_admin_token_when_configured(start_server):
    server = start_server(admin_token="s3cret")
    status, payload, _ = api(server.base, "POST", "/channels", json_body={})
    assert status == 401
    status, payload, _ = api(
        server.base, "POST", "/channels", json_body={}, headers={"Authorization": "Bearer wrong"}
    )
    assert status == 401
    status, payload, _ = api(
        server.base, "POST", "/channels", json_body={}, headers={"Authorization": "Bearer s3cret"}
    )
    assert status == 201
    status, payload, _ = api(
        server.base, "POST", "/channels", json_body={}, headers={"X-Admin-Token": "s3cret"}
    )
    assert status == 201


# ---------------------------------------------------------------------------
# isolation and cursors


def test_channels_are_isolated_by_url(server):
    token_a, url_a, _ = create_channel(server, "a")
    token_b, url_b, _ = create_channel(server, "b")
    post(server, url_a, text="secret for a")
    assert read(server, url_b)["messages"] == []

    status, payload, _ = api(server.base, "GET", "/c/" + "0" * 32 + "/messages?since=0")
    assert status == 404
    status, payload, _ = api(server.base, "GET", "/c/not-a-token")
    assert status == 404
    assert token_a != token_b


def test_cursor_semantics(server):
    _, url, _ = create_channel(server)
    post(server, url, text="one")
    post(server, url, text="two")
    post(server, url, text="three")

    page = read(server, url, since=0)
    assert [m["seq"] for m in page["messages"]] == [1, 2, 3]
    assert page["cursor"] == 3
    assert page["latest_seq"] == 3
    assert page["truncated"] is False

    page = read(server, url, since=1)
    assert [m["seq"] for m in page["messages"]] == [2, 3]
    assert page["cursor"] == 3

    page = read(server, url, since=3)
    assert page["messages"] == []
    assert page["cursor"] == 3


def test_cursor_ahead_of_head_is_clamped(server):
    _, url, _ = create_channel(server)
    post(server, url, text="one")

    page = read(server, url, since=999999)
    assert page["messages"] == []
    assert page["cursor"] == 1

    post(server, url, text="two")
    page = read(server, url, since=page["cursor"])
    assert [m["text"] for m in page["messages"]] == ["two"]


def test_limit_and_cursor_resume(server):
    _, url, _ = create_channel(server)
    for index in range(5):
        post(server, url, text="m%d" % index)

    first = read(server, url, limit=2)
    assert [m["seq"] for m in first["messages"]] == [1, 2]
    assert first["cursor"] == 2

    second = read(server, url, since=first["cursor"], limit=2)
    assert [m["seq"] for m in second["messages"]] == [3, 4]


def test_tail_returns_newest_messages(server):
    _, url, _ = create_channel(server)
    for index in range(5):
        post(server, url, text="m%d" % index)

    status, payload, _ = api(server.base, "GET", path_of(url) + "/messages?last=2")
    assert status == 200
    assert [m["text"] for m in payload["messages"]] == ["m3", "m4"]
    assert payload["cursor"] == 5
    assert payload["latest_seq"] == 5
    assert payload["oldest_seq"] == 1
    assert payload["truncated"] is False


def test_restored_channel_token_and_sequence(start_server):
    token = "a" * 32
    server = start_server(restore=[(token, "restored", 10)])

    status, payload, _ = api(server.base, "GET", "/c/" + token)
    assert status == 200
    assert payload["channel"]["name"] == "restored"
    assert payload["channel"]["cursor"] == 9

    url = server.base + "/c/" + token
    sent = post(server, url, text="after-restore")
    assert sent["message"]["seq"] == 10
    page = read(server, url, since=5)
    assert [m["seq"] for m in page["messages"]] == [10]


def test_parse_restore_specs():
    first, second = "a" * 32, "b" * 32
    assert relay.parse_restore_specs([first + ":name:5", second]) == [
        (first, "name", 5),
        (second, "", 1),
    ]
    with pytest.raises(ValueError):
        relay.parse_restore_specs(["not-a-token"])


def test_restored_channel_reports_history_gap(start_server):
    token = "b" * 32
    server = start_server(restore=[(token, "restored", 100)])
    url = server.base + "/c/" + token

    page = read(server, url, since=0)
    assert page["messages"] == []
    assert page["latest_seq"] == 99
    assert page["truncated"] is True

    page = read(server, url, since=99)
    assert page["truncated"] is False

    post(server, url, text="new")
    page = read(server, url, since=99)
    assert [m["seq"] for m in page["messages"]] == [100]
    assert page["truncated"] is False


def test_ring_buffer_truncation(start_server):
    server = start_server(max_messages=3, ttl_seconds=0)
    _, url, _ = create_channel(server)
    for index in range(5):
        post(server, url, text="m%d" % index)

    page = read(server, url, since=1)
    assert [m["seq"] for m in page["messages"]] == [3, 4, 5]
    assert page["oldest_seq"] == 3
    assert page["truncated"] is True

    page = read(server, url, since=3)
    assert [m["seq"] for m in page["messages"]] == [4, 5]
    assert page["truncated"] is False


def test_idle_channel_expires(start_server):
    server = start_server(ttl_seconds=0.2)
    _, url, _ = create_channel(server)
    time.sleep(0.35)
    status, payload, _ = api(server.base, "GET", path_of(url))
    assert status == 404
    assert "expired" in payload["error"]


def test_channel_capacity_limit(start_server):
    server = start_server(max_channels=1)
    create_channel(server)
    status, payload, _ = api(server.base, "POST", "/channels", json_body={})
    assert status == 503
    assert "limit" in payload["error"]


# ---------------------------------------------------------------------------
# long-poll and SSE


def test_long_poll_returns_on_new_message(server):
    _, url, _ = create_channel(server)

    def later():
        time.sleep(0.4)
        post(server, url, sender="bob", text="wake up")

    thread = threading.Thread(target=later, daemon=True)
    thread.start()
    started = time.monotonic()
    page = read(server, url, since=0, wait=10)
    elapsed = time.monotonic() - started
    thread.join(timeout=5)

    assert [m["text"] for m in page["messages"]] == ["wake up"]
    assert elapsed < 8.0


def test_long_poll_times_out_empty(server):
    _, url, _ = create_channel(server)
    started = time.monotonic()
    page = read(server, url, since=0, wait=1)
    elapsed = time.monotonic() - started
    assert page["messages"] == []
    assert page["cursor"] == 0
    assert 0.8 <= elapsed < 5.0


def test_long_poll_reports_waiters(server):
    _, url, _ = create_channel(server)
    result = {}

    def waiter():
        status, payload, _ = api(
            server.base, "GET", path_of(url) + "/messages?since=0&wait=10"
        )
        assert status == 200
        result["payload"] = payload

    thread = threading.Thread(target=waiter, daemon=True)
    thread.start()
    wait_for_waiters(server, url)
    sent = post(server, url, text="after-wait")
    thread.join(timeout=10)

    assert sent["waiters"] == 1
    assert result["payload"]["messages"][0]["text"] == "after-wait"


def test_wait_reports_effective_and_elapsed(server):
    _, url, _ = create_channel(server)
    post(server, url, text="ready")

    page = read(server, url, since=0, wait=5)
    assert page["wait"] == 5
    assert 0 <= page["wait_elapsed"] < 2

    page = read(server, url, since=page["cursor"], wait=0.3)
    assert page["messages"] == []
    assert page["wait"] == 0.3
    assert 0.2 <= page["wait_elapsed"] < 2


def test_waiter_released_on_disconnect(server):
    _, url, _ = create_channel(server)
    sock = socket.create_connection((server.host, server.port), timeout=5)
    try:
        path = path_of(url) + "/messages?since=0&wait=30"
        request_text = "GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n" % path
        sock.sendall(request_text.encode("ascii"))
        wait_for_waiters(server, url)
    finally:
        sock.close()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status, payload, _ = api(server.base, "GET", path_of(url) + "/status")
        assert status == 200
        if payload["channel"]["waiters"] == 0:
            return
        time.sleep(0.1)
    raise AssertionError("waiter count did not drop after client disconnect")


def test_shutdown_returns_retryable_503(server):
    _, url, _ = create_channel(server)
    result = {}

    def waiter():
        status, payload, _ = api(
            server.base, "GET", path_of(url) + "/messages?since=0&wait=30"
        )
        result["status"] = status
        result["payload"] = payload

    thread = threading.Thread(target=waiter, daemon=True)
    thread.start()
    wait_for_waiters(server, url)
    server.relay.shutdown()
    thread.join(timeout=5)

    assert result["status"] == 503
    assert "restarting" in result["payload"]["error"]


def test_sse_receives_drain_event(server):
    _, url, _ = create_channel(server)
    connection, response = sse(server, url, since=0)
    try:
        assert response.fp.readline() == b": connected\n"
        server.relay.shutdown()
        event = read_sse_event(response)
        assert "restarting" in event["error"]
    finally:
        connection.close()


def test_server_backlog_covers_waiter_bursts():
    assert relay.RelayHTTPServer.request_queue_size >= 256


def test_delete_channel_wakes_waiter(server):
    _, url, _ = create_channel(server)
    result = {}

    def waiter():
        status, payload, _ = api(
            server.base, "GET", path_of(url) + "/messages?since=0&wait=10"
        )
        result["status"] = status
        result["payload"] = payload

    thread = threading.Thread(target=waiter, daemon=True)
    thread.start()
    wait_for_waiters(server, url)

    status, payload, _ = api(server.base, "DELETE", path_of(url))
    assert status == 204
    thread.join(timeout=5)

    assert result["status"] == 410
    assert "closed" in result["payload"]["error"]


def test_sse_delivers_history_and_live_messages(server):
    _, url, _ = create_channel(server)
    post(server, url, text="history")

    connection, response = sse(server, url, since=0)
    try:
        assert response.status == 200
        assert response.getheader("Content-Type").startswith("text/event-stream")
        first = read_sse_event(response)
        assert first["text"] == "history"

        post(server, url, sender="bob", text="live")
        second = read_sse_event(response)
        assert second["text"] == "live"
        assert second["seq"] > first["seq"]
    finally:
        connection.close()


def test_sse_resumes_from_last_event_id(server):
    _, url, _ = create_channel(server)
    post(server, url, text="one")
    post(server, url, text="two")

    connection, response = sse(server, url, since=0, headers={"Last-Event-ID": "1"})
    try:
        assert response.fp.readline() == b": connected\n"
        assert response.fp.readline() == b"retry: 3000\n"
        event = read_sse_event(response)
        assert event["seq"] == 2
        assert event["text"] == "two"
    finally:
        connection.close()


def test_sse_cursor_past_head_still_delivers(server):
    _, url, _ = create_channel(server)
    post(server, url, text="before")

    connection, response = sse(server, url, since=999999)
    try:
        assert response.status == 200
        sent = post(server, url, sender="bob", text="after")
        event = read_sse_event(response)
        assert event["seq"] == sent["message"]["seq"]
        assert event["text"] == "after"
    finally:
        connection.close()


def test_sse_recipient_filter(server):
    _, url, _ = create_channel(server)
    join(server, url, "alice")
    join(server, url, "bob")

    connection, response = sse(server, url, since=0, as_name="bob")
    try:
        assert response.status == 200
        assert response.getheader("Content-Type").startswith("text/event-stream")
        # both join messages are unaddressed, so both are visible to bob
        assert read_sse_event(response)["kind"] == "join"
        assert read_sse_event(response)["kind"] == "join"
        post(server, url, text="for alice", to="alice")
        post(server, url, text="for bob", to="bob")
        event = read_sse_event(response)
        assert event["text"] == "for bob"
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# addressing, presence, lifecycle


def test_to_addressing_filters_readers(server):
    _, url, _ = create_channel(server)
    join(server, url, "alice")
    join(server, url, "bob")
    post(server, url, text="for bob only", to="bob")

    alice = read(server, url, as_name="alice")
    assert [m["text"] for m in alice["messages"]] == ["joined", "joined", "for bob only"]
    assert alice["cursor"] == 3

    bob = read(server, url, since=2, as_name="bob")
    assert [m["text"] for m in bob["messages"]] == ["for bob only"]

    everyone = read(server, url, since=2)
    assert [m["text"] for m in everyone["messages"]] == ["for bob only"]

    carol = read(server, url, since=2, as_name="carol")
    assert carol["messages"] == []
    assert carol["cursor"] == 3


def test_filtered_empty_read_advances_to_head(server):
    _, url, _ = create_channel(server)
    post(server, url, text="for alice", to="alice")

    page = read(server, url, since=0, as_name="bob")
    assert page["messages"] == []
    assert page["cursor"] == 1
    assert page["latest_seq"] == 1


def test_guide_links_use_forwarded_prefix(server):
    token, url, _ = create_channel(server)
    headers = {
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": "relay.example.com",
        "X-Forwarded-Prefix": "/colab/",
    }
    status, raw, _ = request(
        server.base, "GET", path_of(url), headers={**headers, "Accept": "text/markdown"}
    )
    assert status == 200
    assert ("https://relay.example.com/colab/c/" + token) in raw.decode("utf-8")

    status, payload, _ = api(server.base, "GET", path_of(url), headers=headers)
    assert payload["channel"]["url"] == "https://relay.example.com/colab/c/" + token

    bad = dict(headers, **{"X-Forwarded-Prefix": "/../evil"})
    status, payload, _ = api(server.base, "GET", path_of(url), headers=bad)
    assert payload["channel"]["url"] == "https://relay.example.com/c/" + token


def test_join_registers_participant_and_announces(server):
    _, url, _ = create_channel(server)
    joined = join(server, url, "alice", "HIL rig")
    assert joined["participant"]["role"] == "HIL rig"
    assert joined["cursor"] == 1
    assert "token" not in joined["channel"]
    assert "url" not in joined["channel"]

    join(server, url, "bob", "firmware agent")
    status, payload, _ = api(server.base, "GET", path_of(url) + "/status")
    assert status == 200
    assert [p["name"] for p in payload["participants"]] == ["alice", "bob"]
    assert payload["participants"][1]["role"] == "firmware agent"
    assert "token" not in payload["channel"]
    assert "url" not in payload["channel"]

    page = read(server, url)
    assert [m["kind"] for m in page["messages"]] == ["join", "join"]
    assert page["messages"][0]["data"] == {"role": "HIL rig"}


def test_delete_channel(server):
    _, url, _ = create_channel(server)
    status, raw, headers = request(server.base, "DELETE", path_of(url))
    assert status == 204
    assert raw == b""

    status, payload, _ = api(server.base, "GET", path_of(url))
    assert status == 404
    status, payload, _ = api(server.base, "POST", path_of(url) + "/messages",
                             json_body={"sender": "a", "text": "x"})
    assert status == 404


# ---------------------------------------------------------------------------
# sending, validation, protocol edges


def test_plain_text_post(server):
    _, url, _ = create_channel(server)
    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages?sender=curl-agent",
        data="hello from curl",
        headers={"Content-Type": "text/plain"},
    )
    assert status == 201
    assert payload["message"]["sender"] == "curl-agent"
    assert payload["message"]["text"] == "hello from curl"


def test_json_sniffed_without_content_type(server):
    _, url, _ = create_channel(server)
    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages",
        data=json.dumps({"sender": "a", "text": "sniffed"}),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert status == 201
    assert payload["message"]["text"] == "sniffed"

    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages",
        data=json.dumps({"sender": "b", "text": "sniffed from text/plain"}),
        headers={"Content-Type": "text/plain"},
    )
    assert status == 201
    assert payload["message"]["text"] == "sniffed from text/plain"


def test_reply_to_and_data_roundtrip(server):
    _, url, _ = create_channel(server)
    post(server, url, text="question", data={"topic": "can"})
    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages",
        json_body={"sender": "bob", "text": "answer", "reply_to": 1, "data": {"answer": 42}},
    )
    assert status == 201
    assert payload["message"]["reply_to"] == 1
    assert payload["message"]["data"] == {"answer": 42}


def test_validation_errors(server):
    _, url, _ = create_channel(server)
    path = path_of(url) + "/messages"

    status, payload, _ = api(server.base, "POST", path, json_body={"text": "no sender"})
    assert status == 400
    assert "sender" in payload["error"]

    status, payload, _ = api(server.base, "POST", path, json_body={"sender": "a"})
    assert status == 400
    assert "text or data" in payload["error"]

    status, payload, _ = api(
        server.base, "POST", path, json_body={"sender": "a", "text": "x" * (64 * 1024 + 1)}
    )
    assert status == 413

    status, payload, _ = api(
        server.base, "POST", path, json_body={"sender": "a", "text": "x", "reply_to": "1"}
    )
    assert status == 400
    assert "reply_to" in payload["error"]

    status, payload, _ = api(
        server.base, "POST", path, json_body={"sender": "a", "text": "x", "to": "z" * 65}
    )
    assert status == 400

    status, payload, _ = api(server.base, "POST", path, json_body={"sender": "a", "text": "x"},
                             headers={"Content-Type": "application/json"})
    assert status == 201
    assert "reply_to" in payload["message"]

    for query in ("since=-1", "limit=0", "wait=abc"):
        status, payload, _ = api(server.base, "GET", path_of(url) + "/messages?" + query)
        assert status == 400

    status, payload, _ = api(
        server.base,
        "POST",
        path,
        json_body={"sender": "a", "text": "x", "data": {"blob": "y" * (64 * 1024)}},
    )
    assert status == 413


def test_oversized_body_returns_clean_413(server):
    _, url, _ = create_channel(server)
    body = b"x" * (1024 * 1024 + 1024)
    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages",
        data=body,
        headers={"Content-Type": "text/plain"},
        timeout=20,
    )
    assert status == 413
    assert "request body is" in payload["error"]


def test_json_error_includes_position(server):
    _, url, _ = create_channel(server)
    status, payload, _ = api(
        server.base,
        "POST",
        path_of(url) + "/messages",
        data='{"sender":"a","text":"unterminated',
        headers={"Content-Type": "application/json"},
    )
    assert status == 400
    assert "line 1" in payload["error"]
    assert "column" in payload["error"]


def test_unknown_routes_and_methods(server):
    status, payload, headers = api(server.base, "GET", "/nope")
    assert status == 404
    assert "error" in payload

    status, payload, headers = api(server.base, "GET", "/channels")
    assert status == 405
    assert "POST /channels" in payload["error"]
    assert headers["Allow"] == "POST, OPTIONS"

    for method in ("PUT", "PATCH"):
        status, payload, headers = api(server.base, method, "/")
        assert status == 405
        assert payload["error"] == "method not allowed"
        assert headers["Allow"].startswith("GET")

    _, url, _ = create_channel(server)
    status, payload, headers = api(server.base, "PUT", path_of(url) + "/messages")
    assert status == 405
    assert headers["Allow"] == "GET, HEAD, POST, OPTIONS"
    status, payload, headers = api(server.base, "PUT", path_of(url))
    assert status == 405
    assert headers["Allow"] == "GET, HEAD, DELETE, OPTIONS"
    status, payload, headers = api(server.base, "PUT", "/channels")
    assert status == 405
    assert headers["Allow"] == "POST, OPTIONS"

    status, raw, headers = request(
        server.base, "OPTIONS", "/channels", headers={"Origin": "http://example.com"}
    )
    assert status == 204
    assert headers["Access-Control-Allow-Origin"] == "*"
    assert "POST" in headers["Access-Control-Allow-Methods"]


def test_head_has_no_body(server):
    connection = http.client.HTTPConnection(server.host, server.port, timeout=5)
    connection.request("HEAD", "/")
    response = connection.getresponse()
    assert response.status == 200
    assert int(response.getheader("Content-Length")) > 0
    assert response.read() == b""
    connection.close()


def test_head_events_returns_headers_only(server):
    _, url, _ = create_channel(server)
    connection = http.client.HTTPConnection(server.host, server.port, timeout=5)
    connection.request("HEAD", path_of(url) + "/events?since=0")
    response = connection.getresponse()
    assert response.status == 200
    assert response.getheader("Content-Type").startswith("text/event-stream")
    assert response.read() == b""
    connection.close()


def test_health(server):
    status, payload, _ = api(server.base, "GET", "/healthz")
    assert status == 200
    assert payload["ok"] is True
    assert payload["channels"] == 0
    assert isinstance(payload["uptime_s"], (int, float))
    create_channel(server)
    status, payload, _ = api(server.base, "GET", "/healthz")
    assert payload["channels"] == 1


def test_health_and_guide_report_revision(server):
    status, health, _ = api(server.base, "GET", "/healthz")
    assert status == 200
    assert health["revision"]

    _, url, _ = create_channel(server)
    status, guide, _ = api(server.base, "GET", path_of(url))
    assert status == 200
    assert guide["revision"] == health["revision"]


def test_rst_after_completed_response_logs_no_traceback(server, capfd):
    _, url, _ = create_channel(server)

    connection = http.client.HTTPConnection(server.host, server.port, timeout=5)
    connection.request("GET", path_of(url))
    response = connection.getresponse()
    response.read()
    # SO_LINGER(1, 0) turns close() into an RST, like a tab closed mid-stream.
    connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    connection.close()

    captured = ""
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        captured += capfd.readouterr().err
        if "Exception occurred during processing of request" in captured:
            break
        time.sleep(0.05)

    assert "Exception occurred during processing of request" not in captured
