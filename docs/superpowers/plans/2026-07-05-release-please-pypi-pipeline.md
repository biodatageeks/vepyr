# Release-please + PyPI Trusted-Publishing Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the manual `version-bump.yml` + tag-triggered `release.yml` with a single release-please workflow that derives semver from Conventional Commits, gates PyPI publishing on the test suite, and uploads via OIDC Trusted Publishing.

**Architecture:** One workflow (`release-please.yml`) runs on every push to `master`. The release-please action maintains a "Release PR"; merging it makes the same run report `release_created == true`, which unblocks a reusable-CI test gate, the wheel builds, and an OIDC PyPI publish. Version numbers live in `Cargo.toml` (release-type `rust`) and `pyproject.toml` (kept in sync via a `x-release-please-version` annotation). Downstream jobs never depend on a tag/release *event*, avoiding the `GITHUB_TOKEN`-doesn't-trigger-workflows trap.

**Tech Stack:** GitHub Actions, `googleapis/release-please-action@v4`, `PyO3/maturin-action@v1`, `pypa/gh-action-pypi-publish@release/v1`, `actionlint` for validation.

## Global Constraints

- Repo: `biodatageeks/vepyr`; default branch: `master`.
- Current version everywhere is `0.1.0` (both `Cargo.toml:3` and `pyproject.toml:3`).
- `Cargo.lock` is **not** committed — do not add lockfile updates to release-please.
- Wheel matrix stays exactly: Linux `x86_64` (manylinux auto), macOS `x86_64` + `aarch64`, Windows `x64`. Do not add/remove targets.
- Artifact naming stays `wheels-*` (so download-by-pattern keeps working).
- PyPI publish must use OIDC (no `MATURIN_PYPI_TOKEN`), in the existing `pypi` GitHub Environment.
- Keep `--skip-existing` semantics so re-runs are idempotent.
- Conventional Commit prefixes drive versioning: `fix:`→patch, `feat:`→minor, `feat!:`/`BREAKING CHANGE:`→major.

## File Structure

- Create: `.release-please-manifest.json` — tracks last-released version per package.
- Create: `release-please-config.json` — release-type + extra-files config.
- Create: `.github/workflows/release-please.yml` — the whole release pipeline.
- Create: `CONTRIBUTING.md` — commit-convention note.
- Modify: `.github/workflows/ci.yml` — add `workflow_call` trigger so the release flow can reuse it as the test gate.
- Modify: `pyproject.toml:3` — add `# x-release-please-version` annotation.
- Delete: `.github/workflows/version-bump.yml`, `.github/workflows/release.yml`.

> **One-time external setup (not a code task; do before first release):** On PyPI, add a Trusted Publisher for project `vepyr` → owner `biodatageeks`, repo `vepyr`, workflow `release-please.yml`, environment `pypi`. After a successful OIDC publish, delete the `MATURIN_PYPI_TOKEN` repo secret.

> **Validation tool:** several tasks validate with `actionlint`. Install once:
> `brew install actionlint` (macOS) or
> `go install github.com/rhysd/actionlint/cmd/actionlint@latest`.

---

### Task 1: Make CI reusable as a release test gate

Add a `workflow_call` trigger to `ci.yml` so the release workflow can invoke the existing lint/test jobs as a gate. Existing `push`/`pull_request` triggers are unchanged.

**Files:**
- Modify: `.github/workflows/ci.yml:3-15` (the `on:` block)

**Interfaces:**
- Produces: a callable workflow at `./.github/workflows/ci.yml` invokable via `uses: ./.github/workflows/ci.yml` with no inputs. Its jobs (`lint`, `test-rust`, `linux-tests`, and the wheel-build jobs) run unchanged.

- [ ] **Step 1: Add the `workflow_call` trigger**

Edit the `on:` block in `.github/workflows/ci.yml` so it reads:

```yaml
on:
  push:
    branches: [master]
    paths-ignore:
      - "docs/**"
      - "mkdocs.yml"
      - "*.md"
  pull_request:
    branches: [master]
    paths-ignore:
      - "docs/**"
      - "mkdocs.yml"
      - "*.md"
  workflow_call:
```

- [ ] **Step 2: Validate the workflow syntax**

Run: `actionlint .github/workflows/ci.yml`
Expected: no output (exit 0). If `actionlint` is unavailable, fall back to:
`python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: make CI reusable via workflow_call"
```

---

### Task 2: Add release-please config and version annotation

Create the two release-please config files and annotate `pyproject.toml` so its version is bumped in lockstep with `Cargo.toml`.

**Files:**
- Create: `.release-please-manifest.json`
- Create: `release-please-config.json`
- Modify: `pyproject.toml:3`

**Interfaces:**
- Produces: a manifest-mode release-please configuration for the root package (`.`) with `release-type: rust`, that also bumps `pyproject.toml`'s annotated `version` line. Consumed by the workflow in Task 3 via the action's default `config-file`/`manifest-file` paths.

- [ ] **Step 1: Create the manifest file**

Create `.release-please-manifest.json` with the current released version:

```json
{
  ".": "0.1.0"
}
```

- [ ] **Step 2: Create the config file**

Create `release-please-config.json`:

```json
{
  "$schema": "https://raw.githubusercontent.com/googleapis/release-please/main/schemas/config.json",
  "packages": {
    ".": {
      "release-type": "rust",
      "changelog-path": "CHANGELOG.md",
      "extra-files": [
        {
          "type": "generic",
          "path": "pyproject.toml"
        }
      ]
    }
  }
}
```

Note: `release-type: rust` updates `Cargo.toml`. The `generic` extra-file updater edits any line carrying an `x-release-please-version` annotation, which is added next.

- [ ] **Step 3: Annotate the pyproject version line**

In `pyproject.toml`, change line 3 from:

```toml
version = "0.1.0"
```

to:

```toml
version = "0.1.0" # x-release-please-version
```

- [ ] **Step 4: Validate the JSON files parse**

Run:
```bash
python3 -c "import json; json.load(open('.release-please-manifest.json')); json.load(open('release-please-config.json')); print('json ok')"
```
Expected: `json ok`

- [ ] **Step 5: Validate pyproject still parses as TOML**

Run:
```bash
python3 -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb')); print(d['project']['version'])"
```
Expected: `0.1.0`

- [ ] **Step 6: Commit**

```bash
git add .release-please-manifest.json release-please-config.json pyproject.toml
git commit -m "ci: add release-please config and version annotation"
```

---

### Task 3: Add the release-please pipeline workflow

Create the single workflow that runs release-please, and on a created release, gates on tests, builds wheels, publishes to PyPI via OIDC, and attaches wheels to the GitHub Release. This replaces both old workflows.

**Files:**
- Create: `.github/workflows/release-please.yml`
- Delete: `.github/workflows/version-bump.yml`
- Delete: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: reusable `./.github/workflows/ci.yml` (Task 1); `release-please-config.json` + `.release-please-manifest.json` (Task 2).
- Produces: the `pypi` release pipeline. No outputs consumed elsewhere.

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/release-please.yml`:

```yaml
name: Release Please

on:
  push:
    branches: [master]

permissions: {}

jobs:
  release-please:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      pull-requests: write
    outputs:
      release_created: ${{ steps.release.outputs.release_created }}
      tag_name: ${{ steps.release.outputs.tag_name }}
    steps:
      - uses: googleapis/release-please-action@v4
        id: release
        with:
          config-file: release-please-config.json
          manifest-file: .release-please-manifest.json

  test:
    needs: release-please
    if: needs.release-please.outputs.release_created == 'true'
    permissions:
      contents: read
    uses: ./.github/workflows/ci.yml

  sdist:
    needs: release-please
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          command: sdist
          args: --out dist
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-sdist
          path: dist

  linux:
    needs: release-please
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: ubuntu-latest
    permissions:
      contents: read
    strategy:
      matrix:
        target: [x86_64]
    steps:
      - uses: actions/checkout@v4
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist --find-interpreter
          sccache: true
          manylinux: auto
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-manylinux-${{ matrix.target }}
          path: dist

  macos:
    needs: release-please
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: ${{ matrix.target == 'aarch64' && 'macos-latest' || 'macos-14' }}
    permissions:
      contents: read
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v4
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist --find-interpreter
          sccache: true
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-macos-${{ matrix.target }}
          path: dist

  windows:
    needs: release-please
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: windows-latest
    permissions:
      contents: read
    strategy:
      matrix:
        target: [x64]
    steps:
      - uses: actions/checkout@v4
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          maturin-version: v1.13.3
          args: --release --out dist --find-interpreter
          sccache: true
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-win-${{ matrix.target }}
          path: dist

  publish:
    name: Publish to PyPI
    needs: [release-please, test, sdist, linux, macos, windows]
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: wheels-*
          merge-multiple: true
          path: dist
      - name: Publish to PyPI (OIDC Trusted Publishing)
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          skip-existing: true
          attestations: true

  attach-assets:
    needs: [release-please, publish]
    if: needs.release-please.outputs.release_created == 'true'
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: wheels-*
          merge-multiple: true
          path: dist
      - name: Upload wheels to the GitHub Release
        env:
          GH_TOKEN: ${{ github.token }}
        run: gh release upload "${{ needs.release-please.outputs.tag_name }}" dist/* --clobber
```

- [ ] **Step 2: Delete the superseded workflows**

Run:
```bash
git rm .github/workflows/version-bump.yml .github/workflows/release.yml
```

- [ ] **Step 3: Validate the new workflow**

Run: `actionlint .github/workflows/release-please.yml`
Expected: no output (exit 0). If `actionlint` is unavailable, fall back to:
`python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release-please.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Confirm no other workflow still references the removed files or the old token**

Run:
```bash
grep -rEl "version-bump.yml|release\.yml|MATURIN_PYPI_TOKEN" .github/workflows/ || echo "clean"
```
Expected: `clean`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release-please.yml
git commit -m "ci: replace bump+release with release-please pipeline"
```

---

### Task 4: Document the commit convention

Add a `CONTRIBUTING.md` explaining that versioning is driven by Conventional Commits, so contributors (and squash-merge titles) use the right prefixes.

**Files:**
- Create: `CONTRIBUTING.md`

**Interfaces:**
- Produces: contributor-facing documentation. No code dependency.

- [ ] **Step 1: Create CONTRIBUTING.md**

Create `CONTRIBUTING.md`:

```markdown
# Contributing to vepyr

## Commit convention

Releases are automated with [release-please](https://github.com/googleapis/release-please).
The next version and changelog are derived from commit messages on `master`,
so commits (and **squash-merge PR titles**, which become the commit on
`master`) must follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Effect on version |
| --- | --- |
| `fix: ...` | patch release (`0.1.0` → `0.1.1`) |
| `feat: ...` | minor release (`0.1.0` → `0.2.0`) |
| `feat!: ...` or a `BREAKING CHANGE:` footer | major release (`0.1.0` → `1.0.0`) |
| `chore:`, `docs:`, `refactor:`, `test:`, `ci:` | no release |

## How a release happens

1. Merge Conventional-Commit PRs to `master` as usual.
2. release-please maintains an open **"chore: release X.Y.Z"** pull request
   with the computed version bump and `CHANGELOG.md`.
3. Merging that release PR builds the wheels, publishes to PyPI via OIDC
   Trusted Publishing, and attaches the wheels to a GitHub Release.

You never edit `version` in `Cargo.toml` or `pyproject.toml` by hand;
release-please keeps them in sync.
```

- [ ] **Step 2: Verify the file exists and is non-empty**

Run: `test -s CONTRIBUTING.md && echo ok`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "docs: document Conventional Commit release convention"
```

---

## Self-Review

**Spec coverage:**
- Versioning model (release-please, human gate at PR merge) → Tasks 2 + 3 (`release-please` job, `release_created` gating).
- Single-workflow / no event-trigger trap → Task 3 (downstream jobs `needs: release-please`, guarded by output).
- Version sync Cargo/pyproject → Task 2 (`release-type: rust` + `x-release-please-version` annotation).
- OIDC Trusted Publishing → Task 3 (`pypa/gh-action-pypi-publish`, `id-token: write`, `pypi` environment, no token).
- Test gate before publish → Task 1 + Task 3 (`publish` `needs: test`).
- Wheel matrix unchanged → Task 3 (identical targets/artifact names).
- GitHub Release assets → Task 3 (`attach-assets` job, `gh release upload`).
- Remove version-bump.yml + release.yml → Task 3 Step 2.
- Commit-convention note → Task 4.
- PyPI Trusted Publisher registration + token deletion → called out as one-time external setup (not a code task).

**Placeholder scan:** No TBD/TODO; all files shown in full.

**Type/name consistency:** Job outputs `release_created`/`tag_name` are defined in the `release-please` job and referenced consistently as `needs.release-please.outputs.*`. Artifact names all match the `wheels-*` download pattern. `x-release-please-version` annotation string matches between `pyproject.toml` (Task 2) and the `generic` updater (Task 2 config).
