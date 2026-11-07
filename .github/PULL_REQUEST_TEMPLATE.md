<!-- Keep this short; the policy lives in CONTRIBUTING.md. -->

## What changed

<!-- One or two sentences. Link the issue if there is one. -->

## Why

<!-- The problem this solves, not the implementation details. -->

## Verification

- [ ] `make test` (30 tests) passes
- [ ] `make smoke` passes
- [ ] `python3 -m py_compile relay.py tests/test_relay.py tests/conftest.py` passes
- [ ] Docker image builds and `/healthz` responds (if Dockerfile/compose changed)
- [ ] Guides/README/AGENTS updated for any protocol, limit, or flag change

## Merge checklist

- [ ] Squash merge only
- [ ] Review conversations resolved
- [ ] No secrets, tokens, or private URLs in the diff
