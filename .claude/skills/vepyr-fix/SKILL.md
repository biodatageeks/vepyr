---
name: vepyr-fix
description: Apply a vepyr correctness or performance fix end to end across datafusion-bio-formats, datafusion-bio-functions and vepyr. It always begins by analysing all three repos in parallel, writing a plan, asking the questions that raises and waiting for a human to confirm it, with no edit to any repo before that. Then capture a perf and md5 parity baseline, implement, open one draft PR per repo with the pin cascade, drive the codex and claude review loop to green, and re-verify against the baseline before declaring anything ready. It never merges. It hands green, verified PRs to a human for the final review and leaves the merge and the post-merge re-pin to them. Use this whenever the user wants to fix, implement or land a VEP parity bug, act on a filed vepyr issue, change the annotation engine or the VCF reader, or asks for PRs across the engine repos, even when they name only one repo, because a pin bump in the others is almost always required.
---

# Applying a vepyr fix

Read and follow `docs/runbooks/applying-a-vepyr-fix.md`. It is the single source
of truth for this workflow, and it is agent-neutral so that every tool working in
this repository follows the same steps rather than a private copy that drifts.

Start at its step 1 and do not skip it: analyse all three repos with one agent
each in a single message, write the plan to `docs/superpowers/plans/`, ask the
questions it raises, and wait for an explicit human go-ahead. Editing any file —
including the failing test — before that go-ahead breaks the workflow, because
which repo owns the defect is the analysis's output and not something to assume.

Its two executables live beside it, not under any agent's configuration
directory:

- `tools/vepyr-fix/compare_runs.py` — the performance gate, one verdict over
  phase durations, wall time and peak RSS
- `tools/vepyr-fix/handoff.sh` — the hand-off, five ordered phases ending in a
  refusal if new review feedback arrived

This file exists only so the runbook is discoverable from Claude Code. Do not
copy its content here: two copies of a procedure means one of them is wrong and
nobody knows which.
