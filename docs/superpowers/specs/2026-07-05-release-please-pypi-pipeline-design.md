# Robust semantic-versioned release pipeline

**Date:** 2026-07-05
**Status:** Approved design, pending implementation plan

## Goal

Replace the current manual bump + tag-triggered release with a robust,
semantic-versioned pipeline that:

- derives versions automatically from Conventional Commit messages,
- gates the PyPI upload on a passing test suite,
- publishes to PyPI via OIDC Trusted Publishing (no long-lived token),
- keeps `Cargo.toml` and `pyproject.toml` versions in lockstep,
- avoids the `GITHUB_TOKEN`-doesn't-trigger-workflows trap in the current setup.

## Background: current setup and its gaps

Three workflows exist today:

- **`version-bump.yml`** — manual `workflow_dispatch`; operator picks
  patch/minor/major; edits `Cargo.toml` + `pyproject.toml`, commits, tags,
  pushes to master.
- **`release.yml`** — triggers on a `X.Y.Z` tag push; builds sdist + wheels
  (Linux x86_64, macOS x86_64+arm64, Windows x64); publishes to PyPI with the
  `MATURIN_PYPI_TOKEN` API token; generates attestations; cuts a GitHub Release.
- **`ci.yml`** — lint + Rust/Python tests + wheel builds on push/PR to master.

Robustness gaps this design closes:

1. **Broken trigger chain.** `version-bump.yml` pushes the tag using
   `GITHUB_TOKEN`. Events raised by `GITHUB_TOKEN` do **not** trigger other
   workflows, so `release.yml` (which listens on tag push) would never fire from
   an automated bump.
2. **No test gate on release.** `release.yml` publishes wheels without running
   the test suite; a broken build can reach PyPI.
3. **Long-lived credential.** PyPI upload uses a stored API token rather than
   OIDC Trusted Publishing, despite the workflow already declaring
   `id-token: write` and a `pypi` environment.
4. **Manual, non-semantic bumps.** Version type is hand-picked rather than
   derived from commit history.

## Decisions

| Area | Decision |
| --- | --- |
| Versioning model | release-please (Conventional Commits → semver), human gate at Release-PR merge |
| PyPI auth | OIDC Trusted Publishing via `pypa/gh-action-pypi-publish` |
| Wheel matrix | Unchanged: Linux x86_64, macOS x86_64 + arm64, Windows x64 |
| Release gate | Full test suite (Rust + Python) must pass before publish |
| TestPyPI staging | Not used |
| Wheel smoke-test | Not used |

## Architecture

### Flow

```
commits to master (Conventional Commits: feat:/fix:/feat!:)
        │
        ▼
release-please job ──► maintains a "chore: release X.Y.Z" PR
        │              (running CHANGELOG.md + version bump preview)
        │
   merge that PR  ◄──── human gate
        │
        ▼
same workflow run: release_created == true
        │
        ├─► test gate (reusable CI) ── must pass
        ├─► build sdist + wheels (Linux x86_64, macOS x86_64+arm64, Win x64)
        ├─► publish to PyPI via OIDC Trusted Publishing (+ attestations)
        └─► attach wheels to the GitHub Release release-please created
```

### Single-workflow design (the core robustness choice)

Everything lives in **one workflow**, `release-please.yml`, triggered on
`push` to `master`. The release-please action runs on every push and keeps the
Release PR up to date. When that PR is merged, the same run reports
`release_created == true`, and the downstream jobs (test → build → publish →
attach) run, each guarded by
`if: needs.release-please.outputs.release_created`.

This intentionally chains off the release-please job's **output within the same
run**, rather than off a new tag/release event. A tag or Release created by
`GITHUB_TOKEN` would not trigger a separately-triggered workflow, so a
two-workflow design would silently never publish. One workflow avoids that.

### Components

1. **`release-please` job**
   - Uses `googleapis/release-please-action`.
   - Config: `release-please-config.json` + `.release-please-manifest.json`.
   - Permissions: `contents: write`, `pull-requests: write`.
   - Outputs consumed downstream: `release_created`, `tag_name`,
     `upload_url` (for asset attach).

2. **Version sync (manifest mode)**
   - `release-type: rust` bumps `Cargo.toml`.
   - `extra-files` generic updater bumps `pyproject.toml` via an annotation
     comment `# x-release-please-version` on the `version =` line.
   - `.release-please-manifest.json` seeded to the current version (`0.1.0`).
   - `Cargo.lock` is not committed in this repo, so no lockfile update is
     required.
   - Version bump rules: `fix:` → patch, `feat:` → minor,
     `feat!:` / `BREAKING CHANGE:` → major.

3. **Test gate**
   - `ci.yml` gains a `workflow_call` trigger so it can be reused.
   - The release flow calls it; `publish` declares `needs` on both the test job
     and the build jobs. A failing suite blocks the upload.

4. **Build jobs** (`if: release_created`)
   - `sdist`, `linux` (x86_64, manylinux), `macos` (x86_64 + aarch64),
     `windows` (x64), reusing the existing `PyO3/maturin-action` steps and
     artifact naming (`wheels-*`).

5. **`publish` job** (`if: release_created`)
   - `environment: pypi`, `permissions: id-token: write`.
   - Downloads all `wheels-*` artifacts.
   - Publishes with `pypa/gh-action-pypi-publish` using OIDC (no token),
     `skip-existing: true` for idempotent re-runs, attestations enabled.

6. **GitHub Release assets**
   - release-please already created the Release; wheels are uploaded to it
     (via `gh release upload <tag>` or `softprops/action-gh-release` against the
     existing tag). Release notes come from release-please's CHANGELOG.

## Files changed

- **New**
  - `.github/workflows/release-please.yml`
  - `release-please-config.json`
  - `.release-please-manifest.json`
  - Commit-convention note (see below)
- **Removed**
  - `.github/workflows/version-bump.yml`
  - `.github/workflows/release.yml` (folded into `release-please.yml`)
- **Edited**
  - `.github/workflows/ci.yml` — add `workflow_call` trigger.
  - `pyproject.toml` — add `# x-release-please-version` annotation on `version`.

## Commit convention

Versioning now depends on Conventional Commit prefixes, so this must be
documented and lightly enforced:

- Add a short **"Commit convention"** section (in `CONTRIBUTING.md` or the
  README) listing the prefixes and their version impact:
  - `fix:` → patch, `feat:` → minor, `feat!:` / `BREAKING CHANGE:` → major,
  - `chore:`/`docs:`/`refactor:`/`test:`/`ci:` → no release.
- Recommend **squash-merge** with a Conventional-Commit-formatted PR title as
  the practical enforcement point, since the squash title becomes the commit on
  master. Optionally add a PR-title lint check (e.g. an `amannn/action-semantic-pull-request`
  step) as a follow-up if title discipline slips.

## One-time external setup (out of band)

- Register a **PyPI Trusted Publisher** for the `vepyr` project:
  owner `biodatageeks`, repo `vepyr`, workflow filename `release-please.yml`,
  environment `pypi`.
- Once OIDC is confirmed working, delete the `MATURIN_PYPI_TOKEN` secret.

## Non-goals

- No TestPyPI staging.
- No post-build wheel install smoke-test.
- No expansion of the wheel matrix (no Linux arm64 / musllinux).
- No automatic tag push on every merge (semantic-release style); a human still
  gates each release by merging the Release PR.
