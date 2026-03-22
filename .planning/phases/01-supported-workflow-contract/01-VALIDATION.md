---
phase: 01
slug: supported-workflow-contract
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-22
---

# Phase 01 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.2 |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run pytest tests/test_supported_workflow_contract.py -q -x` |
| **Full suite command** | `uv run pytest -q` |
| **Estimated runtime** | ~2 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/test_supported_workflow_contract.py -q -x`
- **After every plan wave:** Run `uv run pytest -q`
- **Before `$gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 0 | SCOPE-01 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |
| 01-01-02 | 01 | 0 | SCOPE-02 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |
| 01-02-01 | 02 | 1 | SCOPE-02 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |
| 01-02-02 | 02 | 1 | API-02 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |
| 01-03-01 | 03 | 1 | CACHE-03 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |
| 01-03-02 | 03 | 1 | SCOPE-01 | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_supported_workflow_contract.py` — stubs for `SCOPE-01`, `SCOPE-02`, `CACHE-03`, and `API-02`
- [ ] `tests/test_supported_workflow_contract.py` — temp-dir manifest fixture helper to avoid live cache downloads in contract tests

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| User-facing supported workflow documentation matches runtime and cache manifest wording | SCOPE-01 | Repo docs phrasing and developer-facing artifacts are easier to audit by reading than by asserting every sentence in tests | Read `.planning/phases/01-supported-workflow-contract/01-CONTEXT.md`, updated README/docs, and sample manifest output; verify they all name `homo_sapiens`, `GRCh38`, release `115`, and `vep` cache only |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
