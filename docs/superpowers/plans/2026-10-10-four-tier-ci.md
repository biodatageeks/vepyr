# Four-tier CI/CD Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every vepyr PR and every PyPI release is gated by four test tiers: unit, Ensembl VEP porting tests, chr22 integration parity, and nf-core module/subworkflow tests. All four run against the wheel built from the code under review.

**Architecture:** Each heavy tier is a reusable workflow (`_tier-*.yml`) that consumes a manylinux x86_64 wheel artifact. `ci.yml` calls the nf-core tier on non-draft PRs. A new admin-only `parity-tests.yml` dispatch calls porting and integration and posts required commit statuses on the PR head SHA. `publish_to_pypi.yml` calls all three before publishing. The workflows share a small, unit-tested helper module, `ci/ci_helpers.py`, instead of growing inline shell logic.

**Tech Stack:** GitHub Actions (reusable workflows, commit statuses, `actions/cache`), Python 3.12 stdlib, uv, maturin, Docker, micromamba, Nextflow + nf-test, Hugging Face Hub, Git LFS.

**Spec:** `docs/superpowers/specs/2026-10-10-four-tier-ci-design.md`

## Global Constraints

- Tiers 2–4 test the wheel built from the PR head or release tag, never a PyPI release.
- Draft PRs run unit tests only. Non-draft PRs, pushes to master and releases also run nf-core.
- Porting and integration run on PRs only by admin dispatch, are skipped on pushes to master, and run automatically on release.
- Required status contexts: `parity/porting`, `parity/integration`. Informational: `parity/integration/<profile>`.
- Jobs that execute PR code get `permissions: contents: read` only; only jobs that never check out PR code hold `statuses: write`.
- Hugging Face cache shards download fresh every run. Only the Ensembl FASTA `.fa.gz` (0.9 GB) and the chr22 goldens/inputs are kept in `actions/cache`, and both are re-verified after restore.
- Porting tests live in `porting-tests/` as their own uv project (Python >= 3.12); their dependencies never enter vepyr's environment.
- Integration runs one job per profile from `e2e-testing/golden/116/chr22/manifest.json`; today 10 profiles.
- nf-core runs as two jobs, `nfcore-module` and `nfcore-subworkflow-parity`, on a container built locally from the wheel and never pushed.
- Runners: GitHub-hosted `ubuntu-latest`. The org runner (id 4682) is out of scope.
- Live-repo actions (branch protection, archiving `vepyr-porting-tests`, closing #144) are prepared as scripts or checklists and executed only on the maintainer's explicit go-ahead.
- Commit messages follow Conventional Commits and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Docs-only PRs blocked forever.** `ci.yml` has `paths-ignore` for `docs/**` and `*.md`, so a docs-only PR never reports the required checks and can never merge. Expected: every PR can merge once green. Owned by Task 9 (drop `paths-ignore` for `pull_request`, keep it for `push`).
2. **A missing verdict is read as a pass.** If a matrix job is cancelled, times out, or never uploads its result, the reporter must post `error`, never `success`. Owned by Task 10 (`statuses()` tests for missing files and an empty profile list).
3. **Wrong or ambiguous wheel.** An artifact with zero wheels, two wheels, or only an aarch64 wheel must fail the job loudly instead of testing whatever `*.whl` globbed first. Owned by Task 1 (`find_wheel()` tests).
4. **A snapshot version rewrite that silently does nothing.** If the upstream `.snap` format changes and no `[process, "vepyr", version]` entry is found, the rewrite must fail, not leave nf-test comparing against a stale version. Owned by Task 8 (`set_snapshot_version()` tests).
5. **A push during a dispatched run.** The verdict must land on the SHA that was tested, and the new head must stay blocked. Owned by Task 10: SHA is resolved once in `authorize` and every later job uses that output, never the PR ref. Verified in Task 13.

---

## File Structure

```
ci/
  ci_helpers.py                 # NEW: tested helpers called by the workflows (one subcommand each)
  apply_branch_protection.py    # NEW: builds/applies the master protection payload (dry run by default)
tests/
  test_ci_helpers.py            # NEW
  test_apply_branch_protection.py  # NEW
porting-tests/                  # NEW: snapshot of vepyr-porting-tests@6f59db4 (subset)
  pyproject.toml, uv.lock, PINS.toml, README.md, .gitignore
  run_tests, bless, check_env, check_test_dir, check_normalised_input
  docs/dataset-pins.md
  ledger/assertions.csv
  tests/data/**, tests/INDEX.csv
  tools/**                      # minus issue/PR/campaign tooling
e2e-testing/                    # + PR #144 files (cherry-picked)
nf-core-module/
  dev/Dockerfile.pr             # NEW: task image from an unreleased wheel
  dev/nf-test-local.sh          # MODIFY: VEPYR_NF_TESTS subset + VEPYR_NF_TESTDATA=published
  modules/nf-core/vepyr/annotate/tests/main.nf.test.snap         # NEW (upstream copy)
  subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap # NEW (upstream copy)
  README.md                     # MODIFY: snapshot + CI notes
.github/workflows/
  _tier-porting.yml             # NEW
  _tier-integration.yml         # NEW
  _tier-nfcore.yml              # NEW
  parity-tests.yml              # NEW
  ci.yml                        # MODIFY
  publish_to_pypi.yml           # MODIFY
pyproject.toml                  # MODIFY: pytest testpaths
.gitattributes                  # MODIFY: porting-tests text/generated attributes
README.md                       # MODIFY: "Continuous integration" section
```

## Setup (before Task 1)

- [ ] Create an isolated worktree for branch `ci/four-tier-testing`, which already holds the spec commits. Use superpowers:using-git-worktrees. The main checkout has a dirty `Cargo.lock` and untracked `.snap` files that must not be touched; switch the main checkout back to `master` first (`git -C /Users/mwiewior/research/git/vepyr switch master`), then `git worktree add .worktrees/four-tier-ci ci/four-tier-testing`.
- [ ] In the worktree: `unset CONDA_PREFIX; uv sync`. `maturin develop` refuses to run while both `VIRTUAL_ENV` and `CONDA_PREFIX` are set.
- [ ] Clone the porting repo, read-only, at the pinned commit: `git clone https://github.com/biodatageeks/vepyr-porting-tests "$SCRATCH/pt" && git -C "$SCRATCH/pt" checkout 6f59db4`. `$SCRATCH` is the session scratchpad. Later tasks call this `$PT`.
- [ ] Install `actionlint` 1.7.7 for local checks: `docker pull rhysd/actionlint:1.7.7`. It runs as `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7`.

---

### Task 1: CI helper module and pytest scoping

**Files:**
- Create: `ci/ci_helpers.py`
- Create: `tests/test_ci_helpers.py`
- Modify: `pyproject.toml` (add pytest `testpaths`)

**Interfaces:**
- Produces: `ci_helpers.find_wheel(directory: Path) -> Path`, `ci_helpers.wheel_version(wheel: Path) -> str`, `ci_helpers.result_record(name: str, outcome: str, summary: str) -> dict`, `class CiError(Exception)`. CLI: `python3 ci/ci_helpers.py find-wheel DIR`, `... wheel-version WHEEL`, `... result --name N --outcome O --summary S --out FILE`. Later tasks add subcommands to the same module.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_ci_helpers.py
"""ci/ci_helpers.py: the logic the workflows rely on, tested off-CI."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ci"))
import ci_helpers  # noqa: E402

X86 = "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
ARM = "vepyr-0.9.2-cp310-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"


def touch(directory: Path, name: str) -> Path:
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def test_find_wheel_returns_the_single_x86_64_wheel(tmp_path):
    expected = touch(tmp_path / "wheels-manylinux-x86_64", X86)
    touch(tmp_path, ARM)
    assert ci_helpers.find_wheel(tmp_path) == expected


@pytest.mark.parametrize("names", [[], [ARM], [X86, X86.replace("0.9.2", "0.9.3")]])
def test_find_wheel_refuses_zero_or_ambiguous(tmp_path, names):
    for name in names:
        touch(tmp_path, name)
    with pytest.raises(ci_helpers.CiError, match="exactly one"):
        ci_helpers.find_wheel(tmp_path)


def test_wheel_version_reads_the_file_name():
    assert ci_helpers.wheel_version(Path(X86)) == "0.9.2"


def test_wheel_version_rejects_other_files():
    with pytest.raises(ci_helpers.CiError):
        ci_helpers.wheel_version(Path("polars-1.0-py3-none-any.whl"))


@pytest.mark.parametrize(
    ("outcome", "conclusion"),
    [("success", "success"), ("failure", "failure"), ("cancelled", "error"),
     ("skipped", "error"), ("", "error")],
)
def test_result_record_maps_step_outcomes(outcome, conclusion):
    record = ci_helpers.result_record("porting", outcome, "s")
    assert record == {"name": "porting", "conclusion": conclusion, "summary": "s"}


def test_cli_result_writes_json(tmp_path):
    out = tmp_path / "result-porting.json"
    code = ci_helpers.main(
        ["result", "--name", "porting", "--outcome", "success",
         "--summary", "204 pass", "--out", str(out)]
    )
    assert code == 0
    assert json.loads(out.read_text())["conclusion"] == "success"


def test_cli_error_exits_nonzero(tmp_path, capsys):
    assert ci_helpers.main(["find-wheel", str(tmp_path)]) == 1
    assert "exactly one" in capsys.readouterr().err
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_ci_helpers.py -v`
Expected: collection error `ModuleNotFoundError: No module named 'ci_helpers'`

- [ ] **Step 3: Write the implementation**

```python
# ci/ci_helpers.py
"""Small helpers for the CI workflows in .github/workflows/.

Each subcommand prints its answer on stdout. When its input is not what the
workflow expects, it prints a message on stderr and exits 1, so a workflow
step fails at the cause instead of later.
Stdlib only: it runs on a bare runner before any environment exists.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

WHEEL = re.compile(r"^vepyr-(?P<version>[^-]+)-[^-]+-[^-]+-.*manylinux.*x86_64\.whl$")
CONCLUSIONS = {"success": "success", "failure": "failure"}


class CiError(Exception):
    """An input the workflow cannot proceed with."""


def find_wheel(directory: Path) -> Path:
    """The one manylinux x86_64 vepyr wheel under ``directory``."""
    found = sorted(p for p in directory.rglob("vepyr-*.whl") if WHEEL.match(p.name))
    if len(found) != 1:
        names = [p.name for p in found]
        raise CiError(
            f"expected exactly one manylinux x86_64 vepyr wheel under {directory}, "
            f"found {len(found)}: {names}"
        )
    return found[0]


def wheel_version(wheel: Path) -> str:
    match = WHEEL.match(wheel.name)
    if not match:
        raise CiError(f"not a manylinux x86_64 vepyr wheel: {wheel.name}")
    return match["version"]


def result_record(name: str, outcome: str, summary: str) -> dict:
    """A verdict for the dispatch reporter; anything but success/failure is an error."""
    return {
        "name": name,
        "conclusion": CONCLUSIONS.get(outcome, "error"),
        "summary": summary,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ci_helpers.py")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("find-wheel")
    p.add_argument("directory", type=Path)
    p = sub.add_parser("wheel-version")
    p.add_argument("wheel", type=Path)
    p = sub.add_parser("result")
    p.add_argument("--name", required=True)
    p.add_argument("--outcome", required=True)
    p.add_argument("--summary", default="")
    p.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "find-wheel":
            print(find_wheel(args.directory))
        elif args.command == "wheel-version":
            print(wheel_version(args.wheel))
        elif args.command == "result":
            record = result_record(args.name, args.outcome, args.summary)
            args.out.write_text(json.dumps(record) + "\n")
    except CiError as exc:
        print(f"ci_helpers: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Add to `pyproject.toml` (new table, after `[tool.maturin]` or at the end). Root pytest has no `testpaths` today and would otherwise collect `porting-tests/tools/test_*.py`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
```

First check that `pyproject.toml` has no existing `[tool.pytest.ini_options]`; `grep -n pytest pyproject.toml` showed none. If one has appeared since, add the key to it instead.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_ci_helpers.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add ci/ci_helpers.py tests/test_ci_helpers.py pyproject.toml
git commit -m "ci: add tested helper module for the CI workflows"
```

---

### Task 2: Import the porting tests snapshot

**Files:**
- Create: `porting-tests/**` (copied from `$PT` at `6f59db4`)
- Modify: `.gitattributes`

**Interfaces:**
- Produces: `porting-tests/run_tests`, `porting-tests/tools/run_tests/{cli,install,suite}.py`, plus the check entry points `porting-tests/tools/build_test_index`, `porting-tests/tools/check_vep_version`, `porting-tests/tools/check_ledger` and `porting-tests/check_normalised_input`. All paths resolve relative to `porting-tests/`: every tool computes its root from `Path(__file__)`, as verified during design.

- [ ] **Step 1: Copy the kept subset**

```bash
cd .worktrees/four-tier-ci
mkdir -p porting-tests
rsync -a \
  --exclude '.git/' --exclude '.github/' --exclude '.claude/' \
  --exclude 'AGENTS.md' --exclude 'CLAUDE.md' --exclude 'LICENSE' \
  --exclude 'docs/porting/' --exclude 'docs/upstream-issue-templates.md' \
  --exclude '/issue_check' --exclude '/issue_status' --exclude '/pr_status' --exclude '/set_state' \
  --exclude 'tools/issue_check/' --exclude 'tools/issue_status/' \
  --exclude 'tools/pr_status/' --exclude 'tools/set_state/' \
  --exclude 'tools/port_campaign.py' --exclude 'tools/check_campaign.py' \
  --exclude 'tools/report_campaign.py' \
  --exclude 'tools/test_issue_check.py' --exclude 'tools/test_issue_status.py' \
  --exclude 'tools/test_pr_status.py' --exclude 'tools/test_set_state.py' \
  --exclude 'tools/test_port_campaign.py' --exclude 'tools/test_check_campaign.py' \
  --exclude 'tools/test_dt.py' \
  --exclude 'tools/fixtures/issue_check/' --exclude 'tools/fixtures/issue_status/' \
  --exclude 'tools/fixtures/pr_status/' --exclude 'tools/fixtures/gh_stub/' \
  "$PT/" porting-tests/
```

- [ ] **Step 2: Check nothing kept imports a dropped module**

Run:
```bash
grep -rnE '^(from|import) (issue_check|issue_status|pr_status|set_state|port_campaign|check_campaign|report_campaign)\b' porting-tests/tools porting-tests/*.py 2>/dev/null
```
Expected: no output.

- [ ] **Step 3: Check the root `.gitignore` hides nothing that was copied**

Run:
```bash
diff <(cd "$PT" && git ls-files | sort) <(cd porting-tests && find . -type f ! -path './.git/*' | sed 's|^\./||' | sort) | grep '^>' ; \
git add -n porting-tests | wc -l; (cd porting-tests && find . -type f | wc -l)
```
Expected: the `>` list is empty (nothing in the copy that isn't tracked upstream), and the two counts are equal. If they differ, `git check-ignore -v <file>` names the root `.gitignore` rule responsible (likely `*.log`, `lib/`, `var/`, `*.manifest`, `*.spec`). Add a negation for that path to `porting-tests/.gitignore`, for example `!tests/data/**/*.log`, and re-run until the counts match.

- [ ] **Step 4: Trim `porting-tests/pyproject.toml`**

Replace the `known-first-party` list with the modules that still exist:

```toml
[tool.ruff.lint.isort]
known-first-party = ["bless", "check_normalised_input", "check_test_dir", "fixture_match", "pins", "run_tests", "vcf_records", "vep_pin"]
```

Run: `uv lock --check --project porting-tests`
Expected: `Resolved ... packages` with no lock change. The dependencies are unchanged.

- [ ] **Step 5: Add attributes to the root `.gitattributes`**

Append:
```
# Porting data tests (porting-tests/): VCF bodies are compared by md5, so keep LF.
porting-tests/tests/data/** -text
porting-tests/tests/INDEX.csv linguist-generated=true
```

- [ ] **Step 6: Run the imported checks in their new location**

Run:
```bash
cd porting-tests
uv run --frozen pytest tools/ -q
tools/build_test_index --check
tools/check_vep_version
tools/check_ledger --csv ledger/assertions.csv
cd ..
```
Expected: pytest passes; `build_test_index --check` exits 0; `check_vep_version` exits 0. `check_ledger` exits 0 or reports drift against upstream that also shows in `$PT` (run the same command in `$PT` to compare). Only drift that appears here and not in `$PT` is a regression.

If a test fails only because it covers dropped tooling (it imports a removed module, or reads a removed fixture), delete that test and note it in the commit message. Fix any other failure in place, typically a path that assumed the repo root.

- [ ] **Step 7: Write `porting-tests/README.md`**

Replace the copied README with the upstream sections for `./run_tests`, `./bless`, the test index, the VEP pin and the ledger, kept verbatim. Drop the `issue_check`, `pr_status`, `issue_status` and `set_state` sections. Add this preface:

```markdown
# Ensembl VEP porting tests

Curated data-problem tests ported from Ensembl VEP, moved here from
`biodatageeks/vepyr-porting-tests` (snapshot of commit 6f59db4, without
history). This directory is its own uv project: run every command from
`porting-tests/`, never from the repository root.

In CI, `./run_tests --wheel <wheel>` runs against the wheel built from the
code under review (see "Continuous integration" in the root README).
```

- [ ] **Step 8: Commit (no reformatting yet)**

```bash
git add .gitattributes porting-tests
git commit --no-verify -m "test(porting): import vepyr-porting-tests@6f59db4 (tests, runner, ledger)

Snapshot without history; issue/PR process tooling, campaign scripts,
skills and audit docs stay in the archived repository."
```

`--no-verify` keeps the verbatim import separate from formatter changes.

- [ ] **Step 9: Apply the root hooks as their own commit**

Run: `uv run pre-commit run ruff --files $(git ls-files porting-tests) ; uv run pre-commit run ruff-format --files $(git ls-files porting-tests)`
Then re-run Step 6's commands. Expected: still passing. Then:
```bash
git add -u porting-tests
git commit -m "style(porting): apply the repository ruff hooks to porting-tests"
```
If the hooks changed nothing, skip the commit.

---

### Task 3: `run_tests --wheel`, `--summary-md`, `--keep-failed`

**Files:**
- Modify: `porting-tests/tools/run_tests/cli.py`
- Modify: `porting-tests/tools/run_tests/install.py`
- Modify: `porting-tests/tools/run_tests/suite.py`
- Test: `porting-tests/tools/test_run_tests_cli.py`, `porting-tests/tools/test_run_tests_wheel.py` (new)

**Interfaces:**
- Consumes: `install.validate_target(str) -> str`, `install.CliBuild`, `suite.Report`, `fixtures.Fixture` (existing).
- Produces:
  - `cli.parse_args(argv) -> argparse.Namespace` with `.vepyr: str | None`, `.wheel: Path | None`, `.summary_md: Path | None`, `.keep_failed: Path | None`
  - `install.wheel_target(wheel: Path) -> str`, which returns `"wheel-<sha256>"`
  - `install.install(target: str | None, cache_root: Path, *, wheel: Path | None = None) -> CliBuild`
  - `suite.run(..., keep_failed: Path | None = None) -> Report`
  - `Report.notes: dict[str, tuple[str, str]]`, mapping test id to (fixture name, detail)
  - `suite.write_summary(report: Report, path: Path) -> None`
  - CLI: `./run_tests (VERSION_OR_SHA | --wheel PATH) [--summary-md PATH] [--keep-failed DIR]`

- [ ] **Step 1: Write the failing tests**

In `porting-tests/tools/test_run_tests_cli.py`, change the existing positional test to read the namespace:

```python
@pytest.mark.parametrize("target", ["0.9.0", "0.10.0rc1", "a" * 40])
def test_one_positional_argument(target):
    assert cli.parse_args([target]).vepyr == target
```

Create `porting-tests/tools/test_run_tests_wheel.py`:

```python
"""--wheel, --summary-md and --keep-failed (vepyr CI)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from run_tests import cli, install, suite
from run_tests.verdict import Exit, RunTestsError


def test_wheel_flag_parses(tmp_path):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"x")
    args = cli.parse_args(["--wheel", str(wheel), "--summary-md", "s.md"])
    assert args.vepyr is None and args.wheel == wheel and args.summary_md == Path("s.md")


@pytest.mark.parametrize("argv", [[], ["0.9.0", "--wheel", "x.whl"]])
def test_exactly_one_of_version_or_wheel(argv):
    with pytest.raises(SystemExit):
        cli.parse_args(argv)


def test_wheel_target_is_content_addressed(tmp_path):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"abc")
    assert install.wheel_target(wheel) == "wheel-" + hashlib.sha256(b"abc").hexdigest()


@pytest.mark.parametrize("name", ["missing.whl", "vepyr.tar.gz"])
def test_wheel_target_rejects_non_wheels(tmp_path, name):
    path = tmp_path / name
    if name.endswith(".tar.gz"):
        path.write_bytes(b"x")
    with pytest.raises(RunTestsError) as info:
        install.wheel_target(path)
    assert info.value.code == Exit.USAGE


def test_install_wheel_installs_that_file(tmp_path, monkeypatch):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"abc")
    calls = []
    monkeypatch.setattr(install, "_run", lambda argv, **kw: calls.append(argv) or "")
    build = install.install(None, tmp_path / "cache", wheel=wheel)
    assert build.source == "local wheel"
    assert build.target == install.wheel_target(wheel)
    pip = next(c for c in calls if c[:3] == ["uv", "pip", "install"])
    assert pip[-1] == str(wheel.resolve())


def test_write_summary_lists_every_non_pass(tmp_path):
    report = suite.Report(
        passed=["ok"],
        skipped=[("sk", "symbolic deletion")],
        mismatched=["mm"],
        errors=[("er", RunTestsError(Exit.ENGINE, "vepyr annotate exited 1 (run 1)"))],
    )
    report.notes.update(
        sk=("fx_skip", "symbolic deletion"),
        mm=("fx_mm", "expected aaa, got bbb; run 1, record 3"),
        er=("fx_er", "vepyr annotate exited 1 (run 1)"),
    )
    out = tmp_path / "summary.md"
    suite.write_summary(report, out)
    text = out.read_text()
    assert "1 pass, 1 mismatch, 1 error, 1 skipped" in text
    for needle in ("`mm`", "`fx_mm`", "MISMATCH", "`er`", "ERROR", "`sk`", "SKIP"):
        assert needle in text
    assert "`ok`" not in text


def test_write_summary_escapes_pipes(tmp_path):
    report = suite.Report(mismatched=["mm"])
    report.notes["mm"] = ("fx", "a|b")
    out = tmp_path / "s.md"
    suite.write_summary(report, out)
    assert "a\\|b" in out.read_text()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd porting-tests && uv run --frozen pytest tools/test_run_tests_wheel.py tools/test_run_tests_cli.py -q`
Expected: FAIL. `parse_args` returns a str, and `wheel_target`, `write_summary` and `Report.notes` don't exist.

- [ ] **Step 3: Implement `install.wheel_target` and the wheel path in `install.install`**

In `install.py`, add after `validate_target`:

```python
def wheel_target(wheel: Path) -> str:
    """Cache key for a local wheel: its content, never its file name."""
    if wheel.suffix != ".whl" or not wheel.is_file():
        raise RunTestsError(Exit.USAGE, f"--wheel must name an existing .whl file: {wheel}")
    with wheel.open("rb") as handle:
        return "wheel-" + hashlib.file_digest(handle, "sha256").hexdigest()
```

Change the head of `install()`:

```python
def install(target: str | None, cache_root: Path, *, wheel: Path | None = None) -> CliBuild:
    if wheel is None:
        target = validate_target(target)
        from_git = bool(SHA.fullmatch(target))
        source = "Git build with uv" if from_git else "PyPI wheel"
    else:
        target = wheel_target(wheel)
        from_git = False
        source = "local wheel"
```

In the requirement selection, replace

```python
        else:
            requirement = f"vepyr=={target}"
```

with

```python
        elif wheel is not None:
            requirement = str(wheel.resolve())
        else:
            requirement = f"vepyr=={target}"
```

The existing `--only-binary :all:` still applies to the wheel's dependencies, which are resolved from PyPI.

- [ ] **Step 4: Implement `Report.notes`, `keep_failed` and `write_summary` in `suite.py`**

Add the field to `Report`:

```python
    notes: dict[str, tuple[str, str]] = field(default_factory=dict)
    """Test id -> (fixture directory name, one-line detail) for every non-pass."""
```

Add `import shutil` and give `run()` a keyword parameter `keep_failed: Path | None = None`. Inside the fixture loop:
- At the start of each fixture, set `first_diff = ""`.
- In the differing-row branch, before `break`, set `first_diff = f"run {index}, record {row + 1}"`.
- Right after the mismatch is first detected (where `mismatch = actual` is set), add:

```python
                        if keep_failed is not None:
                            kept = keep_failed / fixture.directory.name
                            kept.mkdir(parents=True, exist_ok=True)
                            shutil.copyfile(output, kept / output.name)
```

- In the `skip` branch: `report.notes[id_] = (fixture.directory.name, fixture.skip)`.
- In the `except` branch: `report.notes[id_] = (fixture.directory.name, str(exc))`.
- In the mismatch branch: `report.notes[id_] = (fixture.directory.name, f"expected {fixture.expected}, got {mismatch}; {first_diff}")`.

Add at module level:

```python
def write_summary(report: Report, path: Path) -> None:
    """Markdown for $GITHUB_STEP_SUMMARY: one row per test that did not pass."""
    rows = (
        [(id_, "MISMATCH") for id_ in report.mismatched]
        + [(id_, "ERROR") for id_, _ in report.errors]
        + [(id_, "SKIP") for id_, _ in report.skipped]
    )
    lines = [f"### Porting tests: {report.detail}", ""]
    if rows:
        lines += ["| test | fixture | verdict | detail |", "|---|---|---|---|"]
        for id_, verdict in rows:
            fixture, detail = report.notes.get(id_, ("", ""))
            detail = detail.replace("|", "\\|")
            lines.append(f"| `{id_}` | `{fixture}` | {verdict} | {detail} |")
    path.write_text("\n".join(lines) + "\n")
```

- [ ] **Step 5: Implement the CLI in `cli.py`**

Replace `parse_args`:

```python
def parse_args(argv) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="./run_tests",
        allow_abbrev=False,
        description="Run data tests with a vepyr PyPI wheel, a Git build, or a local wheel.",
    )
    parser.add_argument(
        "vepyr",
        nargs="?",
        metavar="VERSION_OR_SHA",
        help="PyPI release (e.g. 0.9.0) or a full 40-character Git commit SHA",
    )
    parser.add_argument("--wheel", type=Path, help="test this local vepyr wheel instead")
    parser.add_argument(
        "--summary-md", type=Path, help="write a Markdown table of every test that did not pass"
    )
    parser.add_argument(
        "--keep-failed", type=Path, help="copy each mismatching run's vepyr output here"
    )
    args = parser.parse_args(argv)
    if (args.vepyr is None) == (args.wheel is None):
        parser.error("give exactly one of VERSION_OR_SHA or --wheel")
    if args.vepyr is not None:
        args.vepyr = install.validate_target(args.vepyr)
    return args
```

Change `run_selection`:
- Signature: `run_selection(target, directories, *, wheel=None, summary_md=None, keep_failed=None, root=None, repo=None, installer=None, preparer=None, runner=None)`.
- First line: `target = install.validate_target(target) if wheel is None else None`.
- All-skipped label: `install.CliBuild(Path(sys.executable), target or "local wheel", "not installed (all skipped)")`.
- Install call: `build = (installer or (lambda t, r: install.install(t, r, wheel=wheel)))(target, root)`.
- Suite call: `suite.run(cases, build=build, cache_root=root, fasta=fasta, runner=runner, keep_failed=keep_failed)`.
- After the suite: `if summary_md is not None: suite.write_summary(report, summary_md)`.

Change `main`:

```python
        args = parse_args(sys.argv[1:] if argv is None else argv)
        ...
        return run_selection(
            args.vepyr, directories,
            wheel=args.wheel, summary_md=args.summary_md, keep_failed=args.keep_failed,
        )
```

- [ ] **Step 6: Run the whole tools suite**

Run: `cd porting-tests && uv run --frozen pytest tools/ -q`
Expected: all PASS, including the existing `test_run_tests_cli.py` cases that pass `installer=`/`preparer=` doubles. Their two-argument call shape is unchanged.

- [ ] **Step 7: Smoke-test against a real wheel (local, optional but recommended)**

Run: `uv run maturin build --release --out "$SCRATCH/whl"`, then `cd porting-tests && VEPYR_CACHE_ROOT=$SCRATCH/vcache ./run_tests --wheel $SCRATCH/whl/vepyr-*.whl --summary-md $SCRATCH/s.md`.
Expected: `204 pass, 0 mismatch, 0 error, 1 skipped`, matching the 2026-10-10 probe. `s.md` lists the one SKIP. This downloads about 5 GB, so skip it if the disk is tight; the CI probe in Task 12 covers it.

- [ ] **Step 8: Update `porting-tests/README.md`** with the three new flags in the `./run_tests` section, then commit.

```bash
git add porting-tests
git commit -m "feat(porting): run_tests --wheel, --summary-md and --keep-failed"
```

---

### Task 4: Porting checks in tier 1, plus actionlint

**Files:**
- Modify: `.github/workflows/ci.yml` (add the `porting-checks` job, add actionlint to `lint`)

**Interfaces:**
- Produces: a required check named `porting-checks`.

- [ ] **Step 1: Add the job to `ci.yml`** (after `test-rust`)

```yaml
  # Cheap checks of the imported porting suite (porting-tests/): no cache or
  # FASTA download. The data suite itself runs in parity-tests.yml / release.
  porting-checks:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    defaults:
      run:
        working-directory: porting-tests
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - name: Set up bcftools 1.23 / htslib 1.23.1 (the pin in tools/normalize_input)
        uses: mamba-org/setup-micromamba@v2
        with:
          environment-name: bcftools
          create-args: -c conda-forge -c bioconda bcftools=1.23 htslib=1.23.1
          init-shell: bash
      - name: Runner unit tests
        run: uv run --frozen pytest tools/ -q
      - name: tests/INDEX.csv is current
        run: tools/build_test_index --check
      - name: Every data test matches the VEP pin
        run: tools/check_vep_version
      - name: Data-test inputs are normalised
        shell: bash -el {0}
        run: ./check_normalised_input
      - name: Ledger matches upstream
        run: |
          tools/check_ledger --sweep
          tools/check_ledger --sweep --glob 't/*.pm'
          tools/check_ledger --csv ledger/assertions.csv
```

Before committing, compare the micromamba step with `$PT/.github/workflows/input-normalised-check.yml` and copy its exact `create-args` and any banner check. The pins above come from the step's title there.

- [ ] **Step 2: Add actionlint to the `lint` job** (after `cargo clippy`)

```yaml
      - name: actionlint
        run: docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7 -color
```

- [ ] **Step 3: Lint locally**

Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7`
Expected: no findings. If findings exist in workflows this plan doesn't touch, fix them in this commit only if trivial; otherwise add `-ignore` patterns naming them, and list them in the PR description.

- [ ] **Step 4: Run the porting checks locally**, using the commands from Task 2 Step 6. Expected: green.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run porting-suite checks and actionlint in tier 1"
```

---

### Task 5: Reusable porting tier

**Files:**
- Create: `.github/workflows/_tier-porting.yml`

**Interfaces:**
- Consumes: `ci_helpers.py find-wheel` and `ci_helpers.py result` (Task 1); `./run_tests --wheel --summary-md --keep-failed` (Task 3).
- Produces: `workflow_call` with inputs `ref` (string, required) and `wheel-artifact` (string, default `wheels-manylinux-x86_64`); uploads artifact `result-porting` containing `result-porting.json` with `name: "porting"`.

- [ ] **Step 1: Write the workflow**

```yaml
name: Tier 2 - porting tests

# Reusable: the Ensembl VEP porting data suite (porting-tests/) against a
# prebuilt manylinux x86_64 wheel. Called by parity-tests.yml (admin dispatch
# on a PR) and publish_to_pypi.yml (release).

on:
  workflow_call:
    inputs:
      ref:
        description: Commit SHA or tag to test
        type: string
        required: true
      wheel-artifact:
        type: string
        default: wheels-manylinux-x86_64

permissions:
  contents: read

jobs:
  porting:
    runs-on: ubuntu-latest
    timeout-minutes: 90
    env:
      VEPYR_CACHE_ROOT: ${{ runner.temp }}/vcache
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ inputs.ref }}
      - uses: astral-sh/setup-uv@v5
      - uses: actions/download-artifact@v4
        with:
          name: ${{ inputs.wheel-artifact }}
          path: dist
      - id: wheel
        run: echo "path=$(python3 ci/ci_helpers.py find-wheel dist)" >> "$GITHUB_OUTPUT"
      - id: pins
        name: Read the FASTA pin
        run: |
          python3 - >> "$GITHUB_OUTPUT" <<'PY'
          import tomllib
          pin = tomllib.load(open("porting-tests/PINS.toml", "rb"))["grch38_fasta"]
          print(f"sha={pin['sha']}")
          print(f"gz={pin['ref']}")
          print(f"sum={pin['ensembl_sum']}")
          PY
      # Only the Ensembl FTP download is cached (slow, occasionally down); HF
      # shards download fresh. PR code can write this cache, so the restored
      # .gz is re-checked against the Ensembl BSD sum and dropped on mismatch.
      - uses: actions/cache@v4
        with:
          path: ${{ env.VEPYR_CACHE_ROOT }}/fasta/${{ steps.pins.outputs.gz }}
          key: porting-fasta-${{ steps.pins.outputs.sha }}
      - name: Verify the restored FASTA archive
        run: |
          gz="$VEPYR_CACHE_ROOT/fasta/${{ steps.pins.outputs.gz }}"
          if [ -f "$gz" ]; then
            got=$(sum "$gz" | awk '{print $1+0, $2+0}')
            if [ "$got" != "${{ steps.pins.outputs.sum }}" ]; then
              echo "::warning::cached FASTA sum '$got' != pin; discarding"; rm -f "$gz"
            fi
          fi
      - id: run
        name: run_tests --wheel
        working-directory: porting-tests
        run: |
          ./run_tests --wheel "${{ steps.wheel.outputs.path }}" \
            --summary-md "$RUNNER_TEMP/porting-summary.md" \
            --keep-failed "$RUNNER_TEMP/porting-failed"
      - name: Step summary
        if: always()
        run: cat "$RUNNER_TEMP/porting-summary.md" >> "$GITHUB_STEP_SUMMARY" 2>/dev/null || echo "no summary (run did not reach the suite)" >> "$GITHUB_STEP_SUMMARY"
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: porting-failed-outputs
          path: ${{ runner.temp }}/porting-failed
          if-no-files-found: ignore
      - name: Record the verdict
        if: always()
        run: |
          python3 ci/ci_helpers.py result --name porting --outcome "${{ steps.run.outcome }}" \
            --summary "$(head -1 "$RUNNER_TEMP/porting-summary.md" 2>/dev/null | sed 's/^### //')" \
            --out result-porting.json
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: result-porting
          path: result-porting.json
```

- [ ] **Step 2: Lint**

Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7`
Expected: no findings.

- [ ] **Step 3: Check the BSD sum command matches the pin format**

Run on Linux, or in Docker: `docker run --rm -v "$SCRATCH:/d" ubuntu:24.04 sh -c 'printf hello > /d/h; sum /d/h | awk "{print \$1+0, \$2+0}"'`
Expected: `36978 1` (GNU `sum` defaults to the BSD algorithm with 1 KiB blocks, which is the format of `ensembl_sum`).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/_tier-porting.yml
git commit -m "ci: add reusable porting tier workflow"
```

---

### Task 6: Carry PR #144 onto the branch

**Files:**
- Cherry-pick: `b97377e`, `a0242e0`, `6dcd163` from `origin/feat/chr22-reviewer-docker`

**Interfaces:**
- Produces: `e2e-testing/scripts/download_chr22.py` (with `--work-dir`, `--profiles`, `--offline`), `e2e-testing/golden/116/chr22/manifest.json` (keys `inputs`, `caches`, `profiles`), `run_comparison.py --output-dir`, and the LFS goldens.

- [ ] **Step 1: Cherry-pick**

```bash
git fetch origin feat/chr22-reviewer-docker
git cherry-pick b97377e a0242e0 6dcd163
```

Resolve any conflicts in `README.md` or `tests/test_comparison_cli.py` by keeping both sides' content: master's newer sections plus #144's additions.

- [ ] **Step 2: Fetch the goldens and run #144's tests**

Run:
```bash
git lfs pull --include 'e2e-testing/golden/116/chr22/*'
uv run pytest tests/test_download_chr22.py tests/test_comparison_cli.py tests/test_comparison_profiles.py -q
```
Expected: PASS. The PR reported 84 passed across its targeted tests.

- [ ] **Step 3: Run one profile natively (local smoke)**

Run:
```bash
uv run python e2e-testing/scripts/download_chr22.py --work-dir "$SCRATCH/int" --profiles merged
DATA_VEPYR_DIR="$SCRATCH/int" uv run python e2e-testing/scripts/run_comparison.py --release 116 \
  --profile merged --chroms 22 --vcf "$SCRATCH/int/input/input_chr22.vcf.gz" \
  --fasta "$SCRATCH/int/input/chr22.fa.gz" --output-dir "$SCRATCH/int" \
  --comparison-mode md5 --md5-mode both --bgzf --no-normalize
```
Expected: `md5 strict: body MATCH (50,861 records)` and `md5 canonical: body MATCH`. The CI probe got the same with vepyr 0.9.2.

- [ ] **Step 4: No extra commit** (the cherry-picks are the commits). If conflict resolution was needed, the resolved cherry-pick commits carry it.

---

### Task 7: Reusable integration tier

**Files:**
- Modify: `ci/ci_helpers.py` (add `profiles`)
- Modify: `tests/test_ci_helpers.py`
- Create: `.github/workflows/_tier-integration.yml`

**Interfaces:**
- Consumes: `find-wheel`, `result` (Task 1); `download_chr22.py`, `run_comparison.py` (Task 6).
- Produces: `ci_helpers.profiles(manifest: Path) -> list[str]` and CLI `profiles MANIFEST`, which prints a JSON list. `workflow_call` inputs `ref` and `wheel-artifact`; output `profiles` (JSON list string); artifacts `result-integration-<profile>` each holding `result-integration-<profile>.json` with `name: "integration/<profile>"`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_ci_helpers.py`)

```python
def test_profiles_reads_manifest_keys_sorted(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"profiles": {"refseq": {}, "ensembl": {}, "merged": {}}}))
    assert ci_helpers.profiles(manifest) == ["ensembl", "merged", "refseq"]


@pytest.mark.parametrize("body", [{}, {"profiles": {}}])
def test_profiles_refuses_empty(tmp_path, body):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(body))
    with pytest.raises(ci_helpers.CiError, match="no profiles"):
        ci_helpers.profiles(manifest)


def test_profiles_cli_prints_json(tmp_path, capsys):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"profiles": {"merged": {}}}))
    assert ci_helpers.main(["profiles", str(manifest)]) == 0
    assert json.loads(capsys.readouterr().out) == ["merged"]


def test_profiles_matches_the_committed_manifest():
    manifest = Path(__file__).resolve().parents[1] / "e2e-testing/golden/116/chr22/manifest.json"
    assert len(ci_helpers.profiles(manifest)) == 10
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_ci_helpers.py -k profiles -v`
Expected: FAIL with `AttributeError: module 'ci_helpers' has no attribute 'profiles'`

- [ ] **Step 3: Implement**

In `ci_helpers.py`:

```python
def profiles(manifest: Path) -> list[str]:
    """Integration profiles = the golden manifest's ``profiles`` keys."""
    names = sorted(json.loads(manifest.read_text()).get("profiles") or {})
    if not names:
        raise CiError(f"no profiles in {manifest}")
    return names
```

In `main()`:

```python
    p = sub.add_parser("profiles")
    p.add_argument("manifest", type=Path)
    ...
        elif args.command == "profiles":
            print(json.dumps(profiles(args.manifest)))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_ci_helpers.py -v`
Expected: all PASS

- [ ] **Step 5: Write the workflow**

```yaml
name: Tier 3 - integration (chr22 VEP 116 parity)

# Reusable: one job per golden profile, so a failure names its profile.
# Goldens and fixtures come from Git LFS once per manifest change (cached);
# HF cache shards download fresh in each job (only that profile's flavour).

on:
  workflow_call:
    inputs:
      ref:
        type: string
        required: true
      wheel-artifact:
        type: string
        default: wheels-manylinux-x86_64
    outputs:
      profiles:
        description: JSON list of the profiles that were run
        value: ${{ jobs.profiles.outputs.list }}

permissions:
  contents: read

env:
  # The cached paths: per-profile goldens plus the inputs download_chr22.py
  # verifies from the manifest. Keyed on the manifest that pins their sha256s.
  FIXTURE_PATHS: |
    e2e-testing/golden/116/chr22/*.vcf.gz
    tests/data/hg002_chr22/input_chr22.vcf.gz
    tests/data/hg002_chr22/input_chr22.vcf.gz.tbi
    tests/data/hg002_chr22/chr22.fa.gz
    tests/data/hg002_chr22/chr22.fa.gz.fai
    tests/data/hg002_chr22/chr22.fa.gz.gzi

jobs:
  profiles:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    outputs:
      list: ${{ steps.list.outputs.list }}
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ inputs.ref }}
      - id: cache
        uses: actions/cache@v4
        with:
          path: ${{ env.FIXTURE_PATHS }}
          key: integration-fixtures-${{ hashFiles('e2e-testing/golden/116/chr22/manifest.json') }}
      - name: Fetch fixtures from LFS (cache miss only)
        if: steps.cache.outputs.cache-hit != 'true'
        run: git lfs pull --include 'e2e-testing/golden/116/chr22/*,tests/data/hg002_chr22/input_chr22.vcf.gz*,tests/data/hg002_chr22/chr22.fa.gz*'
      - id: list
        run: echo "list=$(python3 ci/ci_helpers.py profiles e2e-testing/golden/116/chr22/manifest.json)" >> "$GITHUB_OUTPUT"

  profile:
    needs: profiles
    name: profile (${{ matrix.profile }})
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      fail-fast: false
      matrix:
        profile: ${{ fromJSON(needs.profiles.outputs.list) }}
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ inputs.ref }}
          lfs: false
      # Restored files are re-verified by download_chr22.py against the
      # manifest sha256s; a mismatch makes it re-fetch from LFS.
      - uses: actions/cache/restore@v4
        with:
          path: ${{ env.FIXTURE_PATHS }}
          key: integration-fixtures-${{ hashFiles('e2e-testing/golden/116/chr22/manifest.json') }}
      - uses: astral-sh/setup-uv@v5
      - name: Install VCF command-line tools
        run: |
          sudo apt-get update
          sudo apt-get install -y bcftools tabix
      - uses: actions/download-artifact@v4
        with:
          name: ${{ inputs.wheel-artifact }}
          path: dist
      - name: Install the wheel
        run: |
          uv venv --python 3.12 "$RUNNER_TEMP/venv"
          uv pip install --python "$RUNNER_TEMP/venv/bin/python" \
            "$(python3 ci/ci_helpers.py find-wheel dist)" "huggingface_hub==1.28.0"
      - name: Download the ${{ matrix.profile }} cache
        run: |
          "$RUNNER_TEMP/venv/bin/python" e2e-testing/scripts/download_chr22.py \
            --work-dir "$RUNNER_TEMP/work" --profiles "${{ matrix.profile }}"
      - id: compare
        name: Compare with VEP 116 (strict + canonical md5)
        env:
          DATA_VEPYR_DIR: ${{ runner.temp }}/work
        run: |
          set -o pipefail
          "$RUNNER_TEMP/venv/bin/python" e2e-testing/scripts/run_comparison.py \
            --release 116 --profile "${{ matrix.profile }}" --chroms 22 \
            --vcf "$RUNNER_TEMP/work/input/input_chr22.vcf.gz" \
            --fasta "$RUNNER_TEMP/work/input/chr22.fa.gz" \
            --output-dir "$RUNNER_TEMP/work" \
            --comparison-mode md5 --md5-mode both --bgzf --no-normalize \
            | tee "$RUNNER_TEMP/compare.log"
      - name: Step summary
        if: always()
        run: |
          {
            echo "### ${{ matrix.profile }}"
            grep -E 'md5 (strict|canonical)|PASS|FAIL' "$RUNNER_TEMP/compare.log" 2>/dev/null || echo "comparison did not run"
          } >> "$GITHUB_STEP_SUMMARY"
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: integration-${{ matrix.profile }}-outputs
          path: ${{ runner.temp }}/work/output
          if-no-files-found: ignore
      - name: Record the verdict
        if: always()
        run: |
          python3 ci/ci_helpers.py result --name "integration/${{ matrix.profile }}" \
            --outcome "${{ steps.compare.outcome }}" \
            --summary "$(grep -E 'md5 strict' "$RUNNER_TEMP/compare.log" 2>/dev/null | head -1 | xargs)" \
            --out "result-integration-${{ matrix.profile }}.json"
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: result-integration-${{ matrix.profile }}
          path: result-integration-${{ matrix.profile }}.json
```

- [ ] **Step 6: Check `download_chr22.py` accepts a work dir that holds no Docker paths**

Run: `uv run python e2e-testing/scripts/download_chr22.py --help`
Expected: `--work-dir`, `--profiles`, `--offline`. Task 6 Step 3 already ran it natively.

- [ ] **Step 7: Lint and commit**

```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7
git add ci/ci_helpers.py tests/test_ci_helpers.py .github/workflows/_tier-integration.yml
git commit -m "ci: add reusable integration tier with one job per profile"
```

---

### Task 8: nf-core PR container, runner switches and snapshots

**Files:**
- Modify: `ci/ci_helpers.py` (add `conda-specs` and `snap-version`)
- Modify: `tests/test_ci_helpers.py`
- Create: `nf-core-module/dev/Dockerfile.pr`
- Modify: `nf-core-module/dev/nf-test-local.sh`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test.snap`
- Create: `nf-core-module/subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap`
- Modify: `nf-core-module/README.md`

**Interfaces:**
- Produces:
  - `ci_helpers.conda_specs(environment_yml: Path) -> list[str]`, which returns every dependency except `vepyr`. CLI `conda-specs FILE` prints them space-separated.
  - `ci_helpers.set_snapshot_version(text: str, version: str) -> str`, which raises `CiError` if no entry was found. CLI `snap-version --snap FILE --version V` rewrites in place.
  - `dev/nf-test-local.sh` reads `VEPYR_NF_TESTS` (space-separated test files) and treats `VEPYR_NF_TESTDATA=published` as "use nf-core/test-datasets".
  - The image is referenced as `docker.io/library/vepyr-pr:<tag>`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_ci_helpers.py`)

```python
ENV_YML = """---
# yaml-language-server: $schema=https://example/schema.json
channels:
  - conda-forge
  - bioconda
dependencies:
  # renovate: datasource=conda depName=bioconda/htslib
  - bioconda::htslib=1.24
  # renovate: datasource=conda depName=bioconda/vepyr
  - bioconda::vepyr=0.9.0
"""


def test_conda_specs_drops_vepyr_and_channels(tmp_path):
    env = tmp_path / "environment.yml"
    env.write_text(ENV_YML)
    assert ci_helpers.conda_specs(env) == ["bioconda::htslib=1.24"]


def test_conda_specs_of_the_committed_module():
    env = (Path(__file__).resolve().parents[1]
           / "nf-core-module/modules/nf-core/vepyr/annotate/environment.yml")
    specs = ci_helpers.conda_specs(env)
    assert specs and not any("vepyr" in s for s in specs)


SNAP = {
    "homo_sapiens - vcf": {
        "content": [{"vcf": ["test_vepyr.vcf.gz,variantsMD5:fdddbf25"],
                     "versions_vepyr": [["VEPYR_ANNOTATE", "vepyr", "0.9.0"]]}],
        "meta": {"nf-test": "0.9.2", "nextflow": "25.04.6"},
    },
    "stub": {"content": [{"versions_vepyr": [["VEPYR_ANNOTATE", "vepyr", "0.9.0"]],
                          "versions_bcftools": [["BCFTOOLS_NORM", "bcftools", "1.22"]]}]},
}


def test_set_snapshot_version_rewrites_only_vepyr():
    out = json.loads(ci_helpers.set_snapshot_version(json.dumps(SNAP), "0.9.3"))
    assert out["homo_sapiens - vcf"]["content"][0]["versions_vepyr"] == [["VEPYR_ANNOTATE", "vepyr", "0.9.3"]]
    assert out["stub"]["content"][0]["versions_bcftools"] == [["BCFTOOLS_NORM", "bcftools", "1.22"]]
    assert out["homo_sapiens - vcf"]["content"][0]["vcf"] == ["test_vepyr.vcf.gz,variantsMD5:fdddbf25"]
    assert out["homo_sapiens - vcf"]["meta"] == SNAP["homo_sapiens - vcf"]["meta"]


def test_set_snapshot_version_fails_when_nothing_matches():
    with pytest.raises(ci_helpers.CiError, match="no .*vepyr"):
        ci_helpers.set_snapshot_version(json.dumps({"t": {"content": [1]}}), "0.9.3")


def test_snap_version_cli_rewrites_in_place(tmp_path):
    snap = tmp_path / "main.nf.test.snap"
    snap.write_text(json.dumps(SNAP))
    assert ci_helpers.main(["snap-version", "--snap", str(snap), "--version", "1.0.0"]) == 0
    assert '"1.0.0"' in snap.read_text() and '"0.9.0"' not in snap.read_text()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_ci_helpers.py -k "conda or snap" -v`
Expected: FAIL (`AttributeError`).

- [ ] **Step 3: Implement** in `ci_helpers.py`

```python
def conda_specs(environment_yml: Path) -> list[str]:
    """The module's conda dependencies minus vepyr (installed from the wheel).

    A line-level reader for the nf-core environment.yml layout (no PyYAML on a
    bare runner): ``- spec`` items under the top-level ``dependencies:`` key.
    """
    specs, in_deps = [], False
    for line in environment_yml.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith((" ", "-")) or stripped == "---":
            in_deps = stripped == "dependencies:"
            continue
        if in_deps and stripped.startswith("- "):
            spec = stripped[2:].strip()
            name = spec.split("::")[-1].split("=")[0].split("<")[0].split(">")[0]
            if name != "vepyr":
                specs.append(spec)
    if not specs:
        raise CiError(f"no non-vepyr dependencies in {environment_yml}")
    return specs


def set_snapshot_version(text: str, version: str) -> str:
    """Point every ``[process, "vepyr", version]`` entry at ``version``.

    Only the vepyr version changes; md5s and every other entry stay strict.
    """
    data = json.loads(text)
    hits = 0

    def walk(node):
        nonlocal hits
        if isinstance(node, list):
            if len(node) == 3 and all(isinstance(x, str) for x in node) and node[1] == "vepyr":
                node[2] = version
                hits += 1
                return
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for item in node.values():
                walk(item)

    walk(data)
    if hits == 0:
        raise CiError('no [process, "vepyr", version] entry in the snapshot')
    return json.dumps(data, indent=4) + "\n"
```

In `main()`:

```python
    p = sub.add_parser("conda-specs")
    p.add_argument("environment_yml", type=Path)
    p = sub.add_parser("snap-version")
    p.add_argument("--snap", type=Path, required=True)
    p.add_argument("--version", required=True)
    ...
        elif args.command == "conda-specs":
            print(" ".join(conda_specs(args.environment_yml)))
        elif args.command == "snap-version":
            args.snap.write_text(set_snapshot_version(args.snap.read_text(), args.version))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_ci_helpers.py -v`
Expected: all PASS

- [ ] **Step 5: Write `nf-core-module/dev/Dockerfile.pr`**

```dockerfile
# CI only: the VEPYR_ANNOTATE task image built from an unreleased vepyr wheel.
# Same conda packages as the module's environment.yml except vepyr, which
# comes from the wheel (with its Python dependencies). Build context: a
# directory holding exactly one vepyr-*.whl. See .github/workflows/_tier-nfcore.yml.
FROM mambaorg/micromamba:2.0.5
ARG CONDA_SPECS
COPY --chown=$MAMBA_USER:$MAMBA_USER *.whl /tmp/wheel/
# procps: Nextflow needs `ps` inside task containers for its metrics.
RUN micromamba install -y -n base -c conda-forge -c bioconda \
        python=3.12 pip procps-ng ${CONDA_SPECS} \
    && micromamba run -n base pip install --no-cache-dir /tmp/wheel/*.whl \
    && micromamba clean -a -y
ENV PATH=/opt/conda/bin:$PATH
```

- [ ] **Step 6: Build and smoke-test the image locally (linux/amd64)**

```bash
uv run maturin build --release --out "$SCRATCH/whl" --manylinux 2014 --target x86_64-unknown-linux-gnu 2>/dev/null \
  || echo "cross-build unavailable on this host: download the wheels-manylinux-x86_64 artifact from the latest master CI run into \$SCRATCH/whl instead"
mkdir -p "$SCRATCH/ctx" && cp "$SCRATCH"/whl/vepyr-*manylinux*x86_64*.whl "$SCRATCH/ctx/"
docker build --platform linux/amd64 -f nf-core-module/dev/Dockerfile.pr \
  --build-arg CONDA_SPECS="$(python3 ci/ci_helpers.py conda-specs nf-core-module/modules/nf-core/vepyr/annotate/environment.yml)" \
  -t vepyr-pr:local "$SCRATCH/ctx"
docker run --rm --platform linux/amd64 vepyr-pr:local bash -c 'vepyr --version && tabix --version | head -1 && ps --version'
```

Expected: the vepyr version printed, plus the tabix and procps banners. On Apple Silicon the amd64 image may die with SIGILL under emulation, which is an emulator limitation (no AVX), not a defect. If that happens, defer this check to Task 12's CI run and say so in the task report.

- [ ] **Step 7: Add the switches to `dev/nf-test-local.sh`**

Replace the block from `testdata="${module_root}/.testdata"` to the end of the file with:

```bash
testdata="${module_root}/.testdata"
fixture="${module_root}/../tests/data/hg002_chr22"

# VEPYR_NF_TESTDATA=published: run the submission tests against nf-core's
# published test-datasets (what the committed snapshots were made from) and
# skip local staging. Anything else (or unset) stages .testdata/ as before.
if [[ "${VEPYR_NF_TESTDATA:-}" == "published" ]]; then
    unset VEPYR_NF_TESTDATA
else
    shopt -s nullglob
    stage_inputs=(
        stage-testdata.sh
        stage_testdata.py
        "${fixture}/prepare.py"
        "${fixture}"/input_chr22.vcf.gz*
        "${fixture}"/chr22.fa.gz*
        "${fixture}"/cache/*/*
    )
    shopt -u nullglob
    if command -v sha256sum >/dev/null 2>&1; then
        stage_hash="$(cat "${stage_inputs[@]}" | sha256sum | cut -d' ' -f1)"
    else
        stage_hash="$(cat "${stage_inputs[@]}" | shasum -a 256 | cut -d' ' -f1)"
    fi
    stage_stamp="${testdata}/stage-inputs.sha256"
    if [[ "$(cat "${stage_stamp}" 2>/dev/null)" != "${stage_hash}" ]]; then
        ./stage-testdata.sh "${testdata}"
        echo "${stage_hash}" > "${stage_stamp}"
    fi
    export VEPYR_NF_TESTDATA="${testdata}/data/"
fi

# VEPYR_NF_TESTS: space-separated subset of test files (default: all four).
read -r -a nf_tests <<< "${VEPYR_NF_TESTS:-modules/nf-core/vepyr/annotate/tests/main.nf.test subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test dev/tests/hg002_chr22.nf.test dev/tests/hg002_chr22_normalize.nf.test}"

# dev/tests/hg002_chr22*.nf.test read the offline VEP parity fixture in place.
VEPYR_HG002_CHR22="${fixture}" \
    "${nf_test}" test "${nf_tests[@]}" --config dev/nf-test.config "$@"
```

Before replacing, diff the current block against what's quoted here, which was read on 2026-10-10. If the file has changed since, merge rather than overwrite.

Run: `bash -n nf-core-module/dev/nf-test-local.sh`
Expected: no output.

- [ ] **Step 8: Commit the upstream snapshots**

```bash
ref=$(gh api repos/nf-core/modules/commits/master --jq .sha)
for p in modules/nf-core/vepyr/annotate/tests/main.nf.test.snap \
         subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap; do
  gh api "repos/nf-core/modules/contents/$p?ref=$ref" --jq .content | base64 -d > "nf-core-module/$p"
done
echo "$ref"
grep -c '"vepyr"' nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test.snap \
  nf-core-module/subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap
grep -n 'versions.yml' nf-core-module/*/nf-core/*/tests/main.nf.test.snap nf-core-module/*/nf-core/*/*/tests/main.nf.test.snap
```

Expected: a nonzero `"vepyr"` count in both files, and **no** `versions.yml:md5` lines. A versions file stored as an md5 can't be rewritten and would break on every PR; if one exists, stop and report it, because the snapshot approach needs revisiting.

The maintainer's untracked local `.snap` files in the main checkout are not a reference (they came from local `.testdata`) and must not be touched; only the upstream copies are committed.

Then check the mirrored `main.nf.test` files match upstream at the same ref:
```bash
for p in modules/nf-core/vepyr/annotate/tests/main.nf.test subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test; do
  diff <(gh api "repos/nf-core/modules/contents/$p?ref=$ref" --jq .content | base64 -d) "nf-core-module/$p" && echo "same: $p"
done
```
Expected: `same:` for both. If they differ, the mirror is stale. Stop and report, because snapshot and test must come from the same upstream commit.

- [ ] **Step 9: Update `nf-core-module/README.md`**

In the "Snapshots" paragraph, replace "Commit one only when it comes from nf-core's published URLs (`VEPYR_NF_TESTDATA` unset), after #2270 merges." with:

```markdown
The committed `main.nf.test.snap` files are verbatim copies from
nf-core/modules (commit `<ref from Step 8>`). Refresh them with the module
and subworkflow when upstream changes. CI runs the submission tests against the
published test data (`VEPYR_NF_TESTDATA=published`) with `--ci`, after
rewriting only the vepyr version in its working copy to the version of the
wheel under test (`ci/ci_helpers.py snap-version`). A local run against
staged `.testdata/` will not match them; do not commit snapshots it produces.
```

In the overrides table, add rows for `VEPYR_NF_TESTS` and `VEPYR_NF_TESTDATA=published`.

- [ ] **Step 10: Commit**

```bash
git add ci/ci_helpers.py tests/test_ci_helpers.py nf-core-module/dev/Dockerfile.pr \
  nf-core-module/dev/nf-test-local.sh nf-core-module/README.md \
  nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test.snap \
  nf-core-module/subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap
git commit -m "test(nf-core): PR-wheel task image, published-data mode and upstream snapshots"
```

---

### Task 9: Reusable nf-core tier, wired into `ci.yml`

**Files:**
- Create: `.github/workflows/_tier-nfcore.yml`
- Modify: `.github/workflows/ci.yml` (triggers, `nfcore` job)

**Interfaces:**
- Consumes: `find-wheel`, `wheel-version`, `conda-specs`, `snap-version` (Tasks 1 and 8); `Dockerfile.pr` and the `nf-test-local.sh` switches (Task 8).
- Produces: required checks `nfcore / nfcore-module` and `nfcore / nfcore-subworkflow-parity`. `workflow_call` inputs `ref` and `wheel-artifact`.

- [ ] **Step 1: Write `_tier-nfcore.yml`**

```yaml
name: Tier 4 - nf-core module and subworkflow

# Reusable: nf-test with a task image built from the wheel under test (never
# pushed). Two jobs so a failure says whether the module or the
# subworkflow/parity tests broke.

on:
  workflow_call:
    inputs:
      ref:
        type: string
        required: true
      wheel-artifact:
        type: string
        default: wheels-manylinux-x86_64

permissions:
  contents: read

jobs:
  nfcore:
    name: ${{ matrix.suite }}
    runs-on: ubuntu-latest
    timeout-minutes: 60
    strategy:
      fail-fast: false
      matrix:
        include:
          - suite: nfcore-module
            tests: modules/nf-core/vepyr/annotate/tests/main.nf.test
            lfs: ""
          - suite: nfcore-subworkflow-parity
            tests: >-
              subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test
              dev/tests/hg002_chr22.nf.test
              dev/tests/hg002_chr22_normalize.nf.test
            lfs: tests/data/hg002_chr22/**
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ inputs.ref }}
          lfs: false
      - name: Fetch the offline parity fixture from LFS
        if: matrix.lfs != ''
        run: git lfs pull --include '${{ matrix.lfs }}'
      - uses: actions/download-artifact@v4
        with:
          name: ${{ inputs.wheel-artifact }}
          path: dist
      - id: wheel
        run: |
          wheel=$(python3 ci/ci_helpers.py find-wheel dist)
          echo "path=$wheel" >> "$GITHUB_OUTPUT"
          echo "version=$(python3 ci/ci_helpers.py wheel-version "$wheel")" >> "$GITHUB_OUTPUT"
      - name: Build the task image from the wheel
        run: |
          mkdir -p "$RUNNER_TEMP/ctx"
          cp "${{ steps.wheel.outputs.path }}" "$RUNNER_TEMP/ctx/"
          docker build -f nf-core-module/dev/Dockerfile.pr \
            --build-arg CONDA_SPECS="$(python3 ci/ci_helpers.py conda-specs nf-core-module/modules/nf-core/vepyr/annotate/environment.yml)" \
            -t "vepyr-pr:${{ inputs.ref }}" "$RUNNER_TEMP/ctx"
          docker run --rm "vepyr-pr:${{ inputs.ref }}" vepyr --version
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
      - uses: nf-core/setup-nextflow@v2
        with:
          version: "24.10.5"
      - uses: nf-core/setup-nf-test@v1
        with:
          version: "0.9.2"
      - name: Point the snapshots at the wheel's version
        run: |
          for snap in nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test.snap \
                      nf-core-module/subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test.snap; do
            python3 ci/ci_helpers.py snap-version --snap "$snap" --version "${{ steps.wheel.outputs.version }}"
          done
      - name: nf-test
        working-directory: nf-core-module
        env:
          # docker.io/library/ is what Docker resolves the local tag to; the
          # explicit registry stops tests/config/nf-test.config prepending quay.io.
          VEPYR_CONTAINER: docker.io/library/vepyr-pr:${{ inputs.ref }}
          VEPYR_DOCKER_PLATFORM: linux/amd64
          VEPYR_NF_TESTDATA: published
          VEPYR_NF_TESTS: ${{ matrix.tests }}
        run: ./dev/nf-test-local.sh --ci
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: ${{ matrix.suite }}-nf-test
          path: |
            nf-core-module/.nf-test/tests/*/meta
            nf-core-module/.nf-test.log
          if-no-files-found: ignore
```

Before committing, check that `24.10.5` and `0.9.2` exist as release tags: `gh api repos/nextflow-io/nextflow/releases/tags/v24.10.5 --jq .tag_name` and `gh api repos/askimed/nf-test/releases/tags/v0.9.2 --jq .tag_name`. If either is missing, use the newest patch of that minor and update the step.

- [ ] **Step 2: Wire into `ci.yml`**

Change the triggers so docs-only PRs still report the required checks (Review Focus 1), and so marking a draft ready starts the run:

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
    types: [opened, synchronize, reopened, ready_for_review]
```

Add the job:

```yaml
  # Tier 4 on non-draft PRs and pushes to master. A draft's skipped job is
  # harmless: GitHub never merges drafts, and ready_for_review re-runs CI.
  nfcore:
    needs: linux
    if: github.event_name != 'pull_request' || !github.event.pull_request.draft
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-nfcore.yml
    with:
      ref: ${{ github.event.pull_request.head.sha || github.sha }}
```

`linux` already uploads `wheels-manylinux-x86_64`.

- [ ] **Step 3: Lint and commit**

```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7
git add .github/workflows/_tier-nfcore.yml .github/workflows/ci.yml
git commit -m "ci: run nf-core tier on non-draft PRs with a PR-built task image"
```

---

### Task 10: Admin-dispatched parity workflow and status reporter

**Files:**
- Modify: `ci/ci_helpers.py` (add `statuses`)
- Modify: `tests/test_ci_helpers.py`
- Create: `.github/workflows/parity-tests.yml`

**Interfaces:**
- Consumes: `_tier-porting.yml`, `_tier-integration.yml` (output `profiles`), and the `result-*.json` files (Tasks 5 and 7).
- Produces: `ci_helpers.statuses(results_dir: Path, profiles: list[str]) -> list[tuple[str, str, str]]`, a list of (context, state, description). CLI `statuses --results-dir D --profiles JSON` prints tab-separated lines. Commit statuses: `parity/porting`, `parity/integration` and `parity/integration/<profile>`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_ci_helpers.py`)

```python
def write_result(directory: Path, name: str, conclusion: str, summary: str = "") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    safe = name.replace("/", "-")
    (directory / f"result-{safe}.json").write_text(
        json.dumps({"name": name, "conclusion": conclusion, "summary": summary})
    )


def as_dict(rows):
    return {ctx: (state, desc) for ctx, state, desc in rows}


def test_statuses_all_green(tmp_path):
    write_result(tmp_path / "a", "porting", "success", "204 pass")
    for p in ("ensembl", "merged"):
        write_result(tmp_path / p, f"integration/{p}", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"]))
    assert rows["parity/porting"][0] == "success"
    assert rows["parity/integration"][0] == "success"
    assert rows["parity/integration/merged"][0] == "success"


def test_statuses_failed_profile_fails_aggregate_and_is_named(tmp_path):
    write_result(tmp_path, "porting", "success")
    write_result(tmp_path, "integration/ensembl", "success")
    write_result(tmp_path, "integration/merged", "failure", "md5 strict: MISMATCH")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"]))
    assert rows["parity/integration/merged"] == ("failure", "md5 strict: MISMATCH")
    assert rows["parity/integration"][0] == "failure"
    assert "merged" in rows["parity/integration"][1]


def test_statuses_missing_result_is_error_never_success(tmp_path):
    write_result(tmp_path, "integration/ensembl", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"]))
    assert rows["parity/porting"][0] == "error"
    assert rows["parity/integration/merged"][0] == "error"
    assert rows["parity/integration"][0] != "success"


def test_statuses_without_profile_list_is_error(tmp_path):
    write_result(tmp_path, "porting", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, []))
    assert rows["parity/integration"][0] == "error"


def test_statuses_descriptions_fit_github_limit(tmp_path):
    write_result(tmp_path, "porting", "failure", "x" * 500)
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"]))
    assert all(len(desc) <= 140 for _, desc in rows.values())


def test_statuses_cli_tab_separated(tmp_path, capsys):
    write_result(tmp_path, "porting", "success")
    write_result(tmp_path, "integration/merged", "success")
    assert ci_helpers.main(["statuses", "--results-dir", str(tmp_path),
                            "--profiles", '["merged"]']) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert {line.split("\t")[0] for line in lines} == {
        "parity/porting", "parity/integration/merged", "parity/integration"}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_ci_helpers.py -k statuses -v`
Expected: FAIL (`AttributeError`).

- [ ] **Step 3: Implement**

```python
STATUS_LIMIT = 140  # GitHub's commit status description limit


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= STATUS_LIMIT else text[: STATUS_LIMIT - 1] + "…"


def statuses(results_dir: Path, profiles: list[str]) -> list[tuple[str, str, str]]:
    """Commit statuses for one dispatch run; a missing verdict is an error."""
    records = {}
    for path in sorted(results_dir.rglob("result-*.json")):
        record = json.loads(path.read_text())
        records[record["name"]] = record

    def state(name: str) -> tuple[str, str]:
        record = records.get(name)
        if record is None:
            return "error", "no result reported (job cancelled, timed out or never ran)"
        return record["conclusion"], record.get("summary") or record["conclusion"]

    rows = [("parity/porting", *state("porting"))]
    if not profiles:
        rows.append(("parity/integration", "error", "profile list missing (profiles job failed)"))
    else:
        bad = []
        for profile in profiles:
            conclusion, summary = state(f"integration/{profile}")
            rows.append((f"parity/integration/{profile}", conclusion, summary))
            if conclusion != "success":
                bad.append(profile)
        if bad:
            rows.append(("parity/integration", "failure",
                         f"{len(bad)} of {len(profiles)} profiles not passing: {', '.join(bad)}"))
        else:
            rows.append(("parity/integration", "success", f"{len(profiles)} profiles pass"))
    return [(ctx, st, _clip(desc)) for ctx, st, desc in rows]
```

In `main()`:

```python
    p = sub.add_parser("statuses")
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--profiles", default="[]", help="JSON list (may be empty)")
    ...
        elif args.command == "statuses":
            names = json.loads(args.profiles or "[]")
            for ctx, st, desc in statuses(args.results_dir, names):
                print(f"{ctx}\t{st}\t{desc}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_ci_helpers.py -v`
Expected: all PASS

- [ ] **Step 5: Write `parity-tests.yml`**

```yaml
name: Parity tests

# Admin-only gate for a PR: porting (tier 2) and integration (tier 3) on the
# PR head, reported as commit statuses that branch protection requires.
#   gh workflow run parity-tests.yml -f pr=123
# The SHA is resolved once by `authorize`; a push during the run leaves the new
# head without a status, so it stays blocked until an admin re-runs this.

on:
  workflow_dispatch:
    inputs:
      pr:
        description: Pull request number
        required: true
        type: string

permissions: {}

concurrency:
  group: parity-${{ inputs.pr }}
  cancel-in-progress: true

jobs:
  authorize:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      statuses: write
      pull-requests: read
    outputs:
      sha: ${{ steps.pr.outputs.sha }}
    env:
      GH_TOKEN: ${{ github.token }}
      REPO: ${{ github.repository }}
      PR: ${{ inputs.pr }}
    steps:
      - name: Only admins may run this
        run: |
          perm=$(gh api "repos/$REPO/collaborators/$GITHUB_ACTOR/permission" --jq .permission)
          if [ "$perm" != "admin" ]; then
            echo "::error::$GITHUB_ACTOR has '$perm' permission; parity tests are admin-only"; exit 1
          fi
      - id: pr
        name: Resolve the PR head SHA
        run: |
          [[ "$PR" =~ ^[0-9]+$ ]] || { echo "::error::pr must be a number, got '$PR'"; exit 1; }
          state=$(gh api "repos/$REPO/pulls/$PR" --jq .state)
          [ "$state" = "open" ] || { echo "::error::PR #$PR is $state"; exit 1; }
          sha=$(gh api "repos/$REPO/pulls/$PR" --jq .head.sha)
          echo "sha=$sha" >> "$GITHUB_OUTPUT"
          echo "Testing PR #$PR at $sha" >> "$GITHUB_STEP_SUMMARY"
      - name: Mark pending
        run: |
          url="$GITHUB_SERVER_URL/$REPO/actions/runs/$GITHUB_RUN_ID"
          for ctx in parity/porting parity/integration; do
            gh api -X POST "repos/$REPO/statuses/${{ steps.pr.outputs.sha }}" \
              -f state=pending -f context="$ctx" -f description="running" -f target_url="$url"
          done

  build-wheel:
    needs: authorize
    runs-on: ubuntu-latest
    timeout-minutes: 60
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ needs.authorize.outputs.sha }}
      - uses: PyO3/maturin-action@v1
        with:
          target: x86_64
          args: --release --out dist --find-interpreter
          sccache: true
          manylinux: auto
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-manylinux-x86_64
          path: dist

  porting:
    needs: [authorize, build-wheel]
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-porting.yml
    with:
      ref: ${{ needs.authorize.outputs.sha }}

  integration:
    needs: [authorize, build-wheel]
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-integration.yml
    with:
      ref: ${{ needs.authorize.outputs.sha }}

  report:
    needs: [authorize, porting, integration]
    if: always() && needs.authorize.result == 'success'
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
      statuses: write
    env:
      GH_TOKEN: ${{ github.token }}
      REPO: ${{ github.repository }}
      SHA: ${{ needs.authorize.outputs.sha }}
    steps:
      # The workflow's own ref (master), never the PR: this job holds statuses: write.
      - uses: actions/checkout@v4
        with:
          sparse-checkout: ci
      - uses: actions/download-artifact@v4
        with:
          pattern: result-*
          path: results
      - name: Post statuses
        run: |
          url="$GITHUB_SERVER_URL/$REPO/actions/runs/$GITHUB_RUN_ID"
          mkdir -p results
          python3 ci/ci_helpers.py statuses --results-dir results \
            --profiles '${{ needs.integration.outputs.profiles }}' > statuses.tsv
          cat statuses.tsv
          while IFS=$'\t' read -r ctx state desc; do
            gh api -X POST "repos/$REPO/statuses/$SHA" \
              -f state="$state" -f context="$ctx" -f description="$desc" -f target_url="$url"
          done < statuses.tsv
```

Note: when the integration tier's `profiles` job fails, its output is empty. `--profiles ''` reaches the helper as an empty string, which `json.loads(args.profiles or "[]")` treats as `[]`, so the result is `error` (tested in Step 1).

- [ ] **Step 6: Lint and commit**

```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7
git add ci/ci_helpers.py tests/test_ci_helpers.py .github/workflows/parity-tests.yml
git commit -m "ci: admin-dispatched parity tests posting required commit statuses"
```

---

### Task 11: Release gate in `publish_to_pypi.yml`, with a dry run

**Files:**
- Modify: `.github/workflows/publish_to_pypi.yml`

**Interfaces:**
- Consumes: the three reusable tiers (Tasks 5, 7 and 9) and the existing `linux` job's `wheels-manylinux-x86_64` artifact.
- Produces: new `workflow_dispatch` input `dry_run` (boolean, default false). Output `prepare.outputs.ref`, which is the tag, or `github.sha` in a dry run.

- [ ] **Step 1: Add the input and the `ref` output**

Under `inputs:`:

```yaml
      dry_run:
        description: "Dry run: build and run every test tier at the dispatched commit; no tag, no publish"
        required: false
        type: boolean
        default: false
```

In `prepare`, add `outputs: { ref: ${{ steps.ref.outputs.ref }} }`. Guard the tag step with `if: ${{ !inputs.dry_run }}`, and add after it:

```yaml
      - id: ref
        run: echo "ref=${{ inputs.dry_run && github.sha || inputs.version }}" >> "$GITHUB_OUTPUT"
```

In a dry run, keep the version-match check: run it as a separate step, without `if:`, that executes the `FILE_VER`/`PY_VER` comparison. Splitting it out of the tag step lets a dry run still prove the files agree.

- [ ] **Step 2: Use `needs.prepare.outputs.ref` everywhere**

Replace every `ref: ${{ inputs.version }}` in the file with `ref: ${{ needs.prepare.outputs.ref }}`. That covers `test-rust`, `test-python`, `sdist`, `linux`, `macos`, `windows` and `github-release`. Each of those already has `needs: prepare`, or gets it.

- [ ] **Step 3: Add the tier jobs and gate publishing**

```yaml
  tier-porting:
    needs: [prepare, linux]
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-porting.yml
    with:
      ref: ${{ needs.prepare.outputs.ref }}

  tier-integration:
    needs: [prepare, linux]
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-integration.yml
    with:
      ref: ${{ needs.prepare.outputs.ref }}

  tier-nfcore:
    needs: [prepare, linux]
    permissions:
      contents: read
    uses: ./.github/workflows/_tier-nfcore.yml
    with:
      ref: ${{ needs.prepare.outputs.ref }}
```

Change `publish`:

```yaml
  publish:
    name: Publish to PyPI
    if: ${{ !inputs.dry_run }}
    needs: [prepare, test-rust, test-python, sdist, linux, macos, windows, tier-porting, tier-integration, tier-nfcore]
```

`github-release` needs `publish`, so it's skipped in a dry run automatically.

- [ ] **Step 4: Lint**

Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7`
Expected: no findings.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/publish_to_pypi.yml
git commit -m "ci: gate PyPI publish on porting, integration and nf-core tiers; add dry run"
```

---

### Task 12: Exercise the tiers on the branch before merge

`parity-tests.yml` can't be dispatched until it exists on `master`. The tier workflows can be exercised now through a temporary trigger, and `publish_to_pypi.yml` already exists on master, so it can be dispatched against the branch.

**Files:**
- Create, then delete: `.github/workflows/zz-branch-probe.yml`

- [ ] **Step 1: Add the temporary probe**

```yaml
name: branch-probe (temporary, remove before merge)
on:
  push:
    branches: [ci/four-tier-testing]
permissions:
  contents: read
jobs:
  build-wheel:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with: {target: x86_64, args: --release --out dist --find-interpreter, sccache: true, manylinux: auto}
      - uses: actions/upload-artifact@v4
        with: {name: wheels-manylinux-x86_64, path: dist}
  porting:
    needs: build-wheel
    uses: ./.github/workflows/_tier-porting.yml
    with: {ref: "${{ github.sha }}"}
  integration:
    needs: build-wheel
    uses: ./.github/workflows/_tier-integration.yml
    with: {ref: "${{ github.sha }}"}
  nfcore:
    needs: build-wheel
    uses: ./.github/workflows/_tier-nfcore.yml
    with: {ref: "${{ github.sha }}"}
```

- [ ] **Step 2: Push the branch and watch** (the push is to the branch only; tell the maintainer before pushing)

```bash
git add .github/workflows/zz-branch-probe.yml
git commit -m "ci: temporary branch probe (to be removed)"
git push -u origin ci/four-tier-testing
gh run list --branch ci/four-tier-testing --workflow branch-probe --limit 1
```

Expected:
- `porting` green: `204 pass, 0 mismatch, 0 error, 1 skipped` in the step summary.
- 10 `profile (...)` jobs green.
- `nfcore-module` and `nfcore-subworkflow-parity` green.
- A second push (an empty commit) shows a cache hit on the FASTA and fixtures, with porting's FASTA download skipped.

- [ ] **Step 3: Prove failures are attributed correctly**

On a throwaway branch `probe/ci-negative` created from this branch, make three deliberate changes and push:
- Edit one byte of one fixture's `porting-tests/tests/data/<one fixture>/expected_output.vcf` body.
- Edit the `sha256` of the `merged_per_gene` profile in `manifest.json`, which makes `download_chr22.py` fail verification for that profile only.
- Change one `variantsMD5` value in the module snapshot.

Expected:
- `porting` fails, with that fixture as `MISMATCH` in the summary and a `porting-failed-outputs` artifact.
- Only `profile (merged_per_gene)` fails.
- Only `nfcore-module` fails.

Delete the branch afterwards: `git push origin --delete probe/ci-negative`.

- [ ] **Step 4: Dry-run the release path**

```bash
gh workflow run publish_to_pypi.yml --ref ci/four-tier-testing -f version="$(grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)"/\1/')" -f dry_run=true
```

Expected: every job green, `tier-*` jobs present, `publish` and `github-release` **skipped**, and no new tag (`git ls-remote --tags origin | tail -3` unchanged).

- [ ] **Step 5: Remove the probe and commit**

```bash
git rm .github/workflows/zz-branch-probe.yml
git commit -m "ci: remove temporary branch probe"
```

Record the run IDs and timings from Steps 2–4 for the PR description.

---

### Task 13: Branch protection script, docs, PR and handover

**Files:**
- Create: `ci/apply_branch_protection.py`
- Create: `tests/test_apply_branch_protection.py`
- Modify: `README.md` (new section "Continuous integration")

**Interfaces:**
- Produces: `apply_branch_protection.REQUIRED: list[str]` and `apply_branch_protection.build_payload(current: dict) -> dict`. CLI: `python3 ci/apply_branch_protection.py [--apply]`, which defaults to printing the payload.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_apply_branch_protection.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ci"))
import apply_branch_protection as abp  # noqa: E402

CURRENT = {  # trimmed GET .../branches/master/protection, 2026-10-10
    "required_pull_request_reviews": {
        "dismiss_stale_reviews": False, "require_code_owner_reviews": False,
        "require_last_push_approval": False, "required_approving_review_count": 1},
    "enforce_admins": {"enabled": False},
    "required_linear_history": {"enabled": False},
    "allow_force_pushes": {"enabled": False},
    "allow_deletions": {"enabled": False},
    "required_conversation_resolution": {"enabled": False},
}


def test_payload_preserves_existing_settings():
    payload = abp.build_payload(CURRENT)
    assert payload["required_pull_request_reviews"]["required_approving_review_count"] == 1
    assert payload["enforce_admins"] is False
    assert payload["allow_force_pushes"] is False
    assert payload["restrictions"] is None


def test_payload_requires_every_tier():
    contexts = {c["context"] for c in abp.build_payload(CURRENT)["required_status_checks"]["checks"]}
    for needed in ("lint", "test-rust", "porting-checks", "windows-tests (3.12)",
                   "linux-arm64-tests (3.12)", "nfcore / nfcore-module",
                   "nfcore / nfcore-subworkflow-parity", "parity/porting", "parity/integration"):
        assert needed in contexts
    assert {f"linux-tests (x86_64, {v})" for v in ("3.10", "3.11", "3.12", "3.13", "3.14")} <= contexts
    assert not any(c.startswith("parity/integration/") for c in contexts)


def test_checks_come_from_github_actions():
    checks = abp.build_payload(CURRENT)["required_status_checks"]["checks"]
    assert {c["app_id"] for c in checks} == {abp.GITHUB_ACTIONS_APP_ID}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_apply_branch_protection.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

```python
# ci/apply_branch_protection.py
"""Require the four CI tiers on master, keeping every other protection setting.

Dry run by default (prints the PUT payload). --apply sends it. Apply only after
each check below has reported at least once: GitHub can only require a check
it has already seen.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

REPO = "biodatageeks/vepyr"
BRANCH = "master"
GITHUB_ACTIONS_APP_ID = 15368
REQUIRED = [
    "lint",
    "test-rust",
    *(f"linux-tests (x86_64, {v})" for v in ("3.10", "3.11", "3.12", "3.13", "3.14")),
    "windows-tests (3.12)",
    "linux-arm64-tests (3.12)",
    "porting-checks",
    "nfcore / nfcore-module",
    "nfcore / nfcore-subworkflow-parity",
    "parity/porting",
    "parity/integration",
]


def _enabled(current: dict, key: str) -> bool:
    return bool((current.get(key) or {}).get("enabled", False))


def build_payload(current: dict) -> dict:
    reviews = current.get("required_pull_request_reviews")
    if reviews is not None:
        reviews = {k: reviews[k] for k in (
            "dismiss_stale_reviews", "require_code_owner_reviews",
            "require_last_push_approval", "required_approving_review_count") if k in reviews}
    return {
        "required_status_checks": {
            "strict": False,
            "checks": [{"context": c, "app_id": GITHUB_ACTIONS_APP_ID} for c in REQUIRED],
        },
        "enforce_admins": _enabled(current, "enforce_admins"),
        "required_pull_request_reviews": reviews,
        "restrictions": None,
        "required_linear_history": _enabled(current, "required_linear_history"),
        "allow_force_pushes": _enabled(current, "allow_force_pushes"),
        "allow_deletions": _enabled(current, "allow_deletions"),
        "required_conversation_resolution": _enabled(current, "required_conversation_resolution"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="send the payload (default: print it)")
    args = parser.parse_args(argv)
    path = f"repos/{REPO}/branches/{BRANCH}/protection"
    current = json.loads(subprocess.check_output(["gh", "api", path]))
    payload = build_payload(current)
    print(json.dumps(payload, indent=2))
    if args.apply:
        subprocess.run(["gh", "api", "-X", "PUT", path, "--input", "-"],
                       input=json.dumps(payload), text=True, check=True)
        print(f"applied to {REPO}@{BRANCH}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Before trusting `REQUIRED`, compare it with the check names GitHub reports on the Task 12 branch run: `gh api repos/biodatageeks/vepyr/commits/<sha>/check-runs --jq '.check_runs[].name' | sort -u`. Correct any naming differences in both the list and the test.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_apply_branch_protection.py -v`
Expected: all PASS

- [ ] **Step 5: Add "Continuous integration" to the root `README.md`** (before the license section)

```markdown
## Continuous integration

Four tiers gate every change; tiers 2–4 test the wheel built from the code
under review.

| Tier | What | Draft PR | PR | master | Release |
|---|---|---|---|---|---|
| 1 Unit | lint, `cargo test`, pytest, wheels, `porting-checks` | ✓ | ✓ required | ✓ | ✓ |
| 2 Porting | `porting-tests/` data suite (205 tests) | – | admin dispatch, required | – | ✓ |
| 3 Integration | chr22 VEP 116 parity, one job per profile | – | admin dispatch, required | – | ✓ |
| 4 nf-core | module + subworkflow nf-test on a PR-built image | – | ✓ required | ✓ | ✓ |

Admins run tiers 2–3 for a PR with `gh workflow run parity-tests.yml -f pr=<n>`
(or Actions → Parity tests). They report `parity/porting` and
`parity/integration` on the PR head; a new push needs a new run. Releases
(`publish_to_pypi.yml`) run all tiers and publish only if all pass;
`dry_run=true` runs everything without tagging or publishing.
```

- [ ] **Step 6: Commit and open the PR**

```bash
git add ci/apply_branch_protection.py tests/test_apply_branch_protection.py README.md
git commit -m "ci: branch-protection script and CI documentation"
git push
gh pr create --base master --head ci/four-tier-testing --title "ci: four-tier testing (unit, porting, integration, nf-core)" --body-file "$SCRATCH/pr-body.md"
```

The PR body (`$SCRATCH/pr-body.md`) contains:
- **Summary:** the tier table.
- **What's included:** a section "Includes #144", listing its three commits and noting they land here unreviewed except by this PR's review.
- **Porting import:** the source commit `6f59db4` and what was left behind.
- **Evidence:** the Task 12 evidence, meaning the run links, timings, the negative-test attribution and the dry-run result.
- **Post-merge checklist:** the checklist from Step 7.
- It ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

- [ ] **Step 7: Post-merge checklist (maintainer go-ahead required for each live action)**

Do not execute these without explicit approval. They go in the PR description and the final report.

1. After merge, dispatch from a non-admin account, which should be refused at `authorize`. Then dispatch as admin on an open PR: `gh workflow run parity-tests.yml -f pr=<n>`. Confirm the `parity/*` statuses appear on its head SHA, and that a later push leaves the new head without them.
2. Run `python3 ci/apply_branch_protection.py` to review the payload, then `--apply`.
3. Close #144 with a comment pointing at the merged PR.
4. In `biodatageeks/vepyr-porting-tests`: replace its README with a pointer to `vepyr/porting-tests/`, then archive the repository.

---

## Self-Review Notes

- **Spec coverage:**
  - §1 is covered by Tasks 4, 9, 10 and 11.
  - §2 is covered by Tasks 2, 3, 4 and 5.
  - §3 is covered by Tasks 6 and 7.
  - §4 is covered by Tasks 8 and 9.
  - §5 is covered by Tasks 10, 11, 12 and 13.
  - Retiring the porting repo is Task 13 Step 7.4.
- **Deviation from spec:** `ci.yml` drops `paths-ignore` for `pull_request` (Review Focus 1). The spec didn't mention it, but without the change, docs-only PRs can never merge once checks are required. Consequence to accept: docs-only PRs also need an admin parity dispatch.
- **Known unknowns, each with a verification step:**
  - upstream snapshot form (Task 8 Step 8)
  - Nextflow and nf-test pin tags (Task 9 Step 1)
  - required check names (Task 13 Step 3)
  - root `.gitignore` collisions (Task 2 Step 3)
