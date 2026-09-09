# Working in this repository

Guidance for any coding agent or contributor. Agent-neutral on purpose: every
tool working here should follow the same procedures rather than a private copy
that drifts out of date.

## Runbooks

| Task | Follow |
|---|---|
| Applying a correctness or performance fix across the engine repos | [`docs/runbooks/applying-a-vepyr-fix.md`](docs/runbooks/applying-a-vepyr-fix.md) |

The fix runbook covers the whole path: a performance and md5 parity baseline
captured before any edit, implementation across `datafusion-bio-formats`,
`datafusion-bio-functions` and `vepyr`, one draft pull request per repository
with the pin cascade, the review loop, re-verification against the baseline, and
the hand-off. It never merges. Merging and the post-merge re-pin are a human's
call.

Its executables are in `tools/vepyr-fix/`:

- `compare_runs.py` — the performance gate. One verdict over phase durations,
  wall time and peak RSS, at both worker counts. Exits nonzero on a regression,
  and also when there is nothing to compare, so an empty measurement can never
  read as a pass.
- `handoff.sh` — the hand-off. Five ordered phases, refusing if any check is
  red or still running, if the check that marking ready triggers never appears,
  or if new review feedback arrived that nobody has answered.

## Conventions that bite

- **Build for measurement with**
  `RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`.
  `maturin develop` is the fast-iteration build; comparing a run from one
  against a run from the other compares binaries, not code.
- **Unset `CONDA_PREFIX` before `maturin develop`.** Both `VIRTUAL_ENV` and
  `CONDA_PREFIX` are commonly set on developer machines, maturin refuses that
  combination, and the refusal is quiet — the previously built extension keeps
  serving, so you test old code believing it is new.
- **Lint through pre-commit, never `uv run ruff` directly.** It finds nothing in
  `.venv` and falls through to a different interpreter. Note the hook passes
  `--fix` and rewrites files, so a commit can be rejected for changes the hook
  itself just made.
- **Normalise VCF input** with `bcftools norm -m -both` before comparing against
  Ensembl VEP. Raw multi-allelic input is a different measurement, and a
  separate correctness question.

## Repository layout

| Path | What it holds |
|---|---|
| `src/` | the Rust extension module and the Python package |
| `tools/vepyr-fix/` | executables the fix runbook drives |
| `performance-tests/` | worker-scaling benchmarks, including the WGS runner |
| `e2e-testing/` | parity comparison against Ensembl VEP, md5 concordance, the gate |
| `docs/runbooks/` | procedures meant to be followed step by step |
