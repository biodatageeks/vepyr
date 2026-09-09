---
name: vepyr-fix
description: Apply a vepyr correctness or performance fix end to end across datafusion-bio-formats, datafusion-bio-functions and vepyr. Capture a perf and md5 parity baseline before any edit, implement, open one draft PR per repo with the pin cascade, drive the codex and claude review loop to green, then re-verify against the baseline before declaring anything ready. It never merges. It hands green, verified PRs to a human for the final review and leaves the merge and the post-merge re-pin to them. Use this whenever the user wants to fix, implement or land a VEP parity bug, act on a filed vepyr issue, change the annotation engine or the VCF reader, or asks for PRs across the engine repos, even when they name only one repo, because a pin bump in the others is almost always required.
---

# Applying a vepyr fix

Read and follow `docs/runbooks/applying-a-vepyr-fix.md`. It is the single source
of truth for this workflow, and it is agent-neutral so that every tool working in
this repository follows the same steps rather than a private copy that drifts.

Its two executables live beside it, not under any agent's configuration
directory:

- `tools/vepyr-fix/compare_runs.py` — the performance gate, one verdict over
  phase durations, wall time and peak RSS
- `tools/vepyr-fix/handoff.sh` — the hand-off, five ordered phases ending in a
  refusal if new review feedback arrived

This file exists only so the runbook is discoverable from Claude Code. Do not
copy its content here: two copies of a procedure means one of them is wrong and
nobody knows which.
