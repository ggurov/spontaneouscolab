# Contributing

## Merge policy

`main` is protected. Every code or documentation change lands through a pull
request; direct pushes are rejected.

A pull request can be merged only when all of the following hold:

1. **CI is green** on the latest commit: the `test` job (syntax, 30 end-to-end
   tests, curl smoke test) and the `docker` job (image build + container health
   probe) must pass.
2. **The branch is up to date with `main`** (required status checks are strict).
3. **Every review conversation is resolved.**
4. **History stays linear**: use **Squash and merge**; merge commits and rebase
   merges are disabled, and force pushes/deletions of `main` are blocked.
5. **A maintainer has reviewed the change.** GitHub cannot enforce self-review
   for the solo maintainer; for external contributors, do not self-merge.

The repository ruleset enforcing this is described in
[`DEPLOY.md`](DEPLOY.md#branch-protection) and can be inspected under
**Settings → Rules → Rulesets** on GitHub.

## Workflow

```bash
git switch -c feat/short-description     # or fix/, docs/, chore/
# edit, then run the same gates CI runs:
make test
make smoke
python3 -m py_compile relay.py tests/test_relay.py tests/conftest.py
git add -A && git commit -m "feat: short description"
git push -u origin feat/short-description
gh pr create --fill
```

Open the PR, wait for `test` and `docker`, resolve review threads, then
**Squash and merge**. Delete the branch after merge (the repo is configured to
do this automatically).

## Scope and design rules

- Runtime code stays **stdlib-only Python 3.9+** in `relay.py`; do not add
  frameworks, package managers, or a package split without an explicit design
  discussion in an issue.
- The channel URL is a capability and the channel response is the product's
  documentation. Any endpoint, parameter, limit, or semantic change must update
  `SERVICE_GUIDE`, `CHANNEL_GUIDE`, their JSON variants, `README.md`, and
  `AGENTS.md` where relevant.
- Keep the wire schema stable (`seq`, `ts`, `sender`, `kind`, `text`, `to`,
  `reply_to`, `data`). Add new `kind` values instead of changing fields.
- New behavior needs a test at the HTTP boundary in `tests/test_relay.py`; use
  the existing fixtures and helpers. Run `make smoke` for process-level changes
  and rebuild the image for Dockerfile changes.
- Never commit secrets, capability tokens, or operator-specific URLs.

## License

This repository is **AGPL-3.0** (see `LICENSE`). By contributing you agree that
your contribution is licensed under the same terms. `.wolf/` and `.claude/` are
vendored from [ggurov/epicwolf](https://github.com/ggurov/epicwolf) and remain
under its AGPL-3.0 license.
