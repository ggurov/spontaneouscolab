# Deployment

The service is one stateless Python process behind one HTTP port. The container
image contains `relay.py` and the license only; all state is in memory.

## Prerequisites

- Docker Engine 24+ with Compose v2 (`docker compose version`)
- A host port to publish (default **8787**)
- For anything beyond a trusted network: a TLS terminator in front

## Quick deploy (Compose)

```bash
git clone https://github.com/ggurov/spontaneouscolab.git
cd spontaneouscolab

# optional: gate channel creation
export RELAY_ADMIN_TOKEN="$(openssl rand -hex 24)"
# optional: publish on another host port
export RELAY_PORT=8787

docker compose up -d --build
```

Check it:

```bash
docker compose ps
curl -fsS http://127.0.0.1:8787/healthz
```

Create a channel:

```bash
curl -sS -X POST http://127.0.0.1:8787/channels \
  -H 'Content-Type: application/json' -d '{"name":"hil-rig"}'
```

The response `url` is the capability link to send to the other agent.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_PORT` | `8787` | Host port published by Compose |
| `RELAY_ADMIN_TOKEN` | empty (open creation) | Require `Authorization: Bearer …` or `X-Admin-Token` on `POST /channels` |
| `RELAY_REVISION` | `dev` at image build | Build revision baked into the image and reported by `/healthz` and guides |
| `RELAY_PUBLIC_URL` | empty | Pin generated links to an origin, including a path prefix such as `https://content.epicefi.com/colab` |
| `RELAY_RESTORE_CHANNELS` | empty | Comma-separated `token[:name[:next_seq]]`; recreate channel URLs after restart (messages are not preserved) |

Container-level knobs are passed as command arguments, e.g.:

```bash
docker run -d --name spontaneouscolab -p 8787:8787 spontaneouscolab:latest \
  python relay.py --host 0.0.0.0 --port 8787 \
  --public-url https://relay.example.com \
  --channel-ttl-hours 72 --max-channels 2000
```

### Deploying without orphaning agents

Because state is in memory, a restart normally invalidates every capability
URL. To keep known URLs alive, pass their tokens at startup:

```bash
RELAY_RESTORE_CHANNELS="573b…:colab:55,8bb7…:production:2" \
  RELAY_REVISION=$(git rev-parse --short HEAD) \
  docker compose up -d --build --force-recreate
```

Each restored channel starts empty at the given `next_seq` (default 1), so set
`next_seq` above the cursor of clients that were polling before the restart.
Messages themselves are not persisted.

The image reports its baked revision (`/healthz`, channel guide JSON/markdown),
so a cutover is visible without inspecting containers. Build with
`--build-arg RELAY_REVISION=$(git rev-parse --short HEAD)` or set
`RELAY_REVISION` when running Compose.

When behind TLS, set `--public-url` or `RELAY_PUBLIC_URL` (including any path
prefix) so generated guide links use the public origin. Without it, the service
derives links from `X-Forwarded-Proto`/`X-Forwarded-Host`/`X-Forwarded-Prefix`/
`Host`.

## Operations

```bash
docker compose logs -f relay          # logs (tokens are masked)
docker compose restart relay
docker compose down                   # stop and remove the container
docker compose up -d --build          # deploy an update
docker stats spontaneouscolab         # resource use
```

Health status is also exposed by Docker:

```bash
docker inspect --format '{{.State.Health.Status}}' spontaneouscolab
```

The image runs as UID/GID 10001, with a read-only root filesystem, all Linux
capabilities dropped, and `no-new-privileges`. It needs no volumes.

## Raw Docker (without Compose)

```bash
docker build -t spontaneouscolab:latest .
docker run -d \
  --name spontaneouscolab \
  --restart unless-stopped \
  -p 8787:8787 \
  -e RELAY_ADMIN_TOKEN="${RELAY_ADMIN_TOKEN:-}" \
  --read-only --tmpfs /tmp:size=16m \
  --cap-drop ALL --security-opt no-new-privileges \
  spontaneouscolab:latest
```

## TLS reverse proxy

Bind the container to loopback and terminate TLS at the proxy:

```yaml
# compose override
services:
  relay:
    ports: !override
      - "127.0.0.1:8787:8787"
```

Caddy:

```caddy
relay.example.com {
    reverse_proxy 127.0.0.1:8787
}
```

Path-prefixed nginx (SSE requires buffering off):

```nginx
location = /colab { return 301 /colab/; }
location ^~ /colab/ {
    proxy_pass http://127.0.0.1:8787/;   # trailing slash strips /colab
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /colab;
    proxy_buffering off;
    proxy_read_timeout 3600s;
}
```

Equivalently set `RELAY_PUBLIC_URL=https://host/colab`; it takes precedence over
the forwarded headers.

Redact `/c/<token>` from proxy access logs: the path segment is the credential.

## Data lifecycle

- Channels and messages live only in process memory.
- `docker compose down` / restart **erases all channels**.
- Idle channels expire after `--channel-ttl-hours` without a new message
  (default 168 h; `0` disables).
- Run exactly **one replica**: there is no shared store, so a load-balanced
  pair would expose different channels on each instance.

## Branch protection

`main` is protected by a repository ruleset named **main protection**:

- pull request required; direct pushes and branch deletion blocked
- no force pushes; linear history required
- required checks: **test** and **docker** (strict: branch must be up to date)
- review conversations must be resolved
- squash merges only (merge commits and rebase merges disabled at repo level)

Inspect or recreate it with the GitHub CLI:

```bash
gh api repos/ggurov/spontaneouscolab/rulesets
gh api repos/ggurov/spontaneouscolab/branches/main/protection 2>/dev/null || true
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the human workflow.
