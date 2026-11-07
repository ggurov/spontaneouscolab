#!/usr/bin/env bash
# End-to-end smoke test against a real relay process using only curl + python3.
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${PORT:-8791}"
BASE="http://127.0.0.1:${PORT}"
TMP="$(mktemp -d)"
SERVER_LOG="${TMP}/relay.log"
trap 'kill "${SERVER_PID:-0}" 2>/dev/null || true; rm -rf "${TMP}"' EXIT

json() { python3 -c 'import json,sys; data=json.load(sys.stdin); print(eval(sys.argv[1], {"d": data}))' "$1"; }

echo "== starting relay on ${BASE}"
python3 relay.py --host 127.0.0.1 --port "${PORT}" --log-level warning >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 100); do
  if curl -sf "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.05
done
curl -sf "${BASE}/healthz" >/dev/null || { echo "relay did not start"; cat "${SERVER_LOG}"; exit 1; }

echo "== creating channel"
CREATE="$(curl -sS -f -X POST "${BASE}/channels" -H 'Content-Type: application/json' -d '{"name":"smoke"}')"
TOKEN="$(echo "${CREATE}" | json 'd["token"]')"
URL="$(echo "${CREATE}" | json 'd["url"]')"
[ "${URL}" = "${BASE}/c/${TOKEN}" ] || { echo "bad channel url: ${URL}"; exit 1; }
echo "   ${URL}"

echo "== guide is self-describing"
curl -sS -f "${URL}" | grep -q "Agent Relay channel: smoke"
curl -sS -f "${URL}" | grep -q "/messages?since="

echo "== joining and posting"
JOIN="$(curl -sS -f -X POST "${URL}/join" -H 'Content-Type: application/json' -d '{"name":"alice","role":"smoke A"}')"
CURSOR="$(echo "${JOIN}" | json 'd["cursor"]')"
[ "${CURSOR}" = "1" ] || { echo "unexpected join cursor: ${CURSOR}"; exit 1; }
curl -sS -f -X POST "${URL}/messages" -H 'Content-Type: application/json' \
  -d '{"sender":"alice","text":"hello"}' >/dev/null

echo "== long-poll backlog"
PAGE="$(curl -sS -f "${URL}/messages?since=0&wait=2&as=alice")"
echo "${PAGE}" | python3 -c '
import json,sys
page=json.load(sys.stdin)
assert [m["text"] for m in page["messages"]] == ["joined: smoke A", "hello"], page
assert page["cursor"] == 2, page
'
CURSOR=2

echo "== SSE live delivery"
curl -sS -N --max-time 5 "${URL}/events?since=${CURSOR}&as=bob" >"${TMP}/sse.txt" 2>/dev/null &
SSE_PID=$!
sleep 0.3
curl -sS -f -X POST "${URL}/messages" -H 'Content-Type: application/json' \
  -d '{"sender":"bob","text":"streamed"}' >/dev/null
wait "${SSE_PID}" || true
grep -q "streamed" "${TMP}/sse.txt" || { echo "SSE did not deliver"; cat "${TMP}/sse.txt"; exit 1; }

echo "== channel isolation"
OTHER="$(curl -sS -f -X POST "${BASE}/channels" -H 'Content-Type: application/json' -d '{}')"
OTHER_URL="$(echo "${OTHER}" | json 'd["url"]')"
curl -sS -f "${OTHER_URL}/messages?since=0" | python3 -c '
import json,sys
page=json.load(sys.stdin)
assert page["messages"] == [], page
'
curl -sS -o /dev/null -w '%{http_code}' "${BASE}/c/00000000000000000000000000000000" | grep -q 404

echo "== status"
curl -sS -f "${BASE}/healthz" | python3 -c '
import json,sys
health=json.load(sys.stdin)
assert health["ok"] is True, health
assert health["channels"] == 2, health
'

echo "SMOKE OK"
