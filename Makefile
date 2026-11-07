PYTHON ?= python3
PYTEST ?= pytest
HOST ?= 127.0.0.1
PORT ?= 8787
SMOKE_PORT ?= 8791

.PHONY: run test smoke

run:
	$(PYTHON) relay.py --host $(HOST) --port $(PORT)

test:
	$(PYTEST) -q

smoke:
	PORT=$(SMOKE_PORT) bash scripts/smoke.sh
