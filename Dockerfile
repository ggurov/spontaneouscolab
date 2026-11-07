# syntax=docker/dockerfile:1
FROM python:3.12-slim

ARG RELAY_REVISION=dev
ENV RELAY_REVISION=${RELAY_REVISION}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --system --gid 10001 relay \
    && useradd --system --uid 10001 --gid relay --home-dir /app --shell /usr/sbin/nologin relay

COPY relay.py LICENSE ./
RUN chown -R relay:relay /app

USER relay
EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8787/healthz', timeout=2).read()"]

CMD ["python", "relay.py", "--host", "0.0.0.0", "--port", "8787"]
