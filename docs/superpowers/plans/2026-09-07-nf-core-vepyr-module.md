# nf-core `vepyr/annotate` Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal `vepyr annotate` command-line interface and an nf-core module that wraps it, ready to submit to nf-core/modules once the bioconda package ships.

**Architecture:** `src/vepyr/cli.py` is a thin VCF-in / VCF-out shell over `vepyr.annotate(..., output_vcf=...)`. It is split into three pure-ish pieces — `build_parser()` (argparse construction), `annotate_kwargs()` (Namespace → kwargs, no I/O), and `main()` (dispatch and exit codes) — so the flag mapping is unit-testable without touching the engine. The nf-core module lives in `nf-core-module/`, laid out as a miniature nf-core/modules repository so `nf-core modules lint` runs against it and the directory copies verbatim into a fork.

**Tech Stack:** Python 3.10+, stdlib `argparse`, pytest, Nextflow DSL2, nf-core/tools 4.1.0 (via `uvx`), conda/bioconda packaging.

**Spec:** `docs/superpowers/specs/2026-09-07-nf-core-vepyr-module-design.md`

## Global Constraints

- **Version is `0.6.0`** everywhere: `pyproject.toml:3`, `Cargo.toml:3`, `bioconda-recipe/meta.yaml:2` (and the comment at `:73`).
- **No new runtime dependency.** `argparse` is stdlib. The bioconda recipe's `run:` list must stay exactly `python`, `pyarrow >=18.0`, `polars >=1.37.1`, `tqdm >=4.60`.
- **Flag names use Ensembl VEP's spelling with underscores** (`--dir_cache`, `--cache_version`), never kebab-case.
- **Unknown flags hard-error.** Never add `parse_known_args` or a catch-all; a silently ignored flag would produce quietly wrong annotations.
- **The CLI is VCF-in / VCF-out only.** Do not expose `skip_csq`, `fields`, `on_batch_written`, or the LazyFrame path.
- **Exactly ten flags.** Do not add `--pick`, `--hgvs`, `--af`, `--compress_output`, or any other VEP flag, however easy. They are explicitly out of scope in the spec (§7).
- **Rebuilding the extension needs `CONDA_PREFIX` unset.** Both `VIRTUAL_ENV` and `CONDA_PREFIX` are set on this machine; `uv run maturin develop` refuses and the stale `.venv` extension silently keeps running. Always: `env -u CONDA_PREFIX uv run maturin develop`.
- **Python 3.10+ syntax.** `from __future__ import annotations` at the top of new modules, matching `src/vepyr/__init__.py`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/vepyr/cli.py` (create) | Parser construction, Namespace→kwargs mapping, `main()` dispatch and exit codes. The whole CLI; it stays one focused file. |
| `src/vepyr/__main__.py` (create) | Three lines, so `python -m vepyr` works without the console script installed. Tests rely on this. |
| `pyproject.toml` (modify) | `[project.scripts]` entry; version bump. |
| `tests/test_cli.py` (create) | Flag mapping, exit codes, and one subprocess end-to-end run over the golden fixture. |
| `docs/cli.md` (create) | Published CLI reference. `meta.yml`'s `documentation:` URL must resolve to it. |
| `mkdocs.yml` (modify) | Nav entry for the new page. |
| `Cargo.toml`, `Cargo.lock` (modify) | Version bump. |
| `bioconda-recipe/meta.yaml` (modify) | Version bump plus a CLI smoke test. |
| `nf-core-module/.nf-core.yml` (create) | Marks the directory as a modules repository so `nf-core modules lint` runs. |
| `nf-core-module/modules/nf-core/vepyr/annotate/main.nf` (create) | The process definition. |
| `nf-core-module/modules/nf-core/vepyr/annotate/meta.yml` (create) | nf-core input/output schema. |
| `nf-core-module/modules/nf-core/vepyr/annotate/environment.yml` (create) | conda spec — vepyr + htslib. |
| `nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test` (create) | nf-test stub + real test. |
| `nf-core-module/modules/nf-core/vepyr/annotate/tests/nextflow.config` (create) | `ext.args` for the real test. |
| `nf-core-module/stage-testdata.sh` (create) | Stages the golden fixture into the nf-core/test-datasets layout without duplicating 6 MB into git. |
| `nf-core-module/README.md` (create) | Copy target and the upstream blocker checklist. |

---

### Task 1: Flag parser and kwarg mapping

The pure core of the CLI: argparse construction and the Namespace→kwargs translation. No engine calls, so every flag is testable in milliseconds.

**Files:**
- Create: `src/vepyr/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `build_parser() -> argparse.ArgumentParser`; `annotate_kwargs(args: argparse.Namespace) -> dict`. Namespace attribute names: `command`, `input_file`, `output_file`, `dir_cache`, `fasta`, `everything`, `fork`, `cache_version`, `plugin_cache_root`, `plugins`, `no_progress`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cli.py`:

```python
"""Tests for the `vepyr` command-line interface."""

from __future__ import annotations

import pytest

from vepyr.cli import annotate_kwargs, build_parser


def _parse(*argv: str):
    return build_parser().parse_args(["annotate", *argv])


MINIMAL = ("-i", "in.vcf", "-o", "out.vcf.gz", "--dir_cache", "/cache")


def test_required_flags_populate_the_namespace():
    args = _parse(*MINIMAL)
    assert args.command == "annotate"
    assert args.input_file == "in.vcf"
    assert args.output_file == "out.vcf.gz"
    assert args.dir_cache == "/cache"


def test_minimal_invocation_maps_to_kwargs():
    kwargs = annotate_kwargs(_parse(*MINIMAL))
    assert kwargs == {
        "output_vcf": "out.vcf.gz",
        "show_progress": True,
        "workers": 1,
    }


def test_everything_and_fasta_are_forwarded():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--everything", "--fasta", "ref.fa"))
    assert kwargs["everything"] is True
    assert kwargs["reference_fasta"] == "ref.fa"


@pytest.mark.parametrize("flag", ["--fork", "--workers"])
def test_fork_and_workers_are_the_same_knob(flag):
    kwargs = annotate_kwargs(_parse(*MINIMAL, flag, "8"))
    assert kwargs["workers"] == 8


def test_cache_version_is_passed_as_a_string():
    # annotate() validates expected_cache_version as a string ("116"), but the
    # CLI takes an int so `--cache_version 116x` is rejected by argparse.
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--cache_version", "116"))
    assert kwargs["expected_cache_version"] == "116"


def test_repeated_plugin_flags_preserve_order():
    kwargs = annotate_kwargs(
        _parse(
            *MINIMAL,
            "--plugin_cache_root", "/plugins",
            "--plugin", "cadd",
            "--plugin", "clinvar",
        )
    )
    assert kwargs["plugin_cache_root"] == "/plugins"
    assert kwargs["plugins"] == ["cadd", "clinvar"]


def test_plugin_cache_root_alone_is_forwarded_without_plugins():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--plugin_cache_root", "/plugins"))
    assert kwargs["plugin_cache_root"] == "/plugins"
    assert "plugins" not in kwargs


def test_no_progress_disables_the_bar():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--no_progress"))
    assert kwargs["show_progress"] is False


def test_missing_required_flag_exits_2():
    with pytest.raises(SystemExit) as excinfo:
        _parse("-i", "in.vcf", "-o", "out.vcf.gz")
    assert excinfo.value.code == 2


def test_unknown_flag_exits_2():
    # An ext.args string copied from ensemblvep must fail loudly, never be
    # silently ignored.
    with pytest.raises(SystemExit) as excinfo:
        _parse(*MINIMAL, "--pick")
    assert excinfo.value.code == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cli.py -v`
Expected: collection error — `ModuleNotFoundError: No module named 'vepyr.cli'`

- [ ] **Step 3: Write the implementation**

Create `src/vepyr/cli.py`:

```python
"""Command-line interface for vepyr.

A thin VCF-in / VCF-out shell over :func:`vepyr.annotate`. Flag names follow
Ensembl VEP's own spelling so that ``ext.args`` strings written for the
``ensemblvep/vep`` nf-core module carry over unchanged.

The flag set is deliberately small: it covers the configuration the golden
parity suite actually validates (``--everything`` with ``--fasta``) plus the
options an nf-core module needs. Everything else stays on the Python API.
"""

from __future__ import annotations

import argparse
import sys
from importlib.metadata import version as _package_version

_EPILOG = """\
examples:
  vepyr annotate -i in.vcf.gz -o out.vcf.gz --dir_cache CACHE \\
      --fasta GRCh38.fa --everything --fork 8
"""


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level ``vepyr`` parser."""
    parser = argparse.ArgumentParser(
        prog="vepyr",
        description="Rust-powered Ensembl VEP variant annotation.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"vepyr {_package_version('vepyr')}",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    annotate = subcommands.add_parser(
        "annotate",
        help="Annotate a VCF against a vepyr Parquet cache.",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    required = annotate.add_argument_group("required arguments")
    required.add_argument(
        "-i", "--input_file", required=True, metavar="FILE",
        help="Input VCF (plain, gzip or bgzip).",
    )
    required.add_argument(
        "-o", "--output_file", required=True, metavar="FILE",
        help="Output VCF. A .gz/.bgz suffix selects bgzf compression.",
    )
    required.add_argument(
        "--dir_cache", required=True, metavar="DIR",
        help="Parquet cache directory, e.g. .../116_GRCh38_ensembl.",
    )

    vep = annotate.add_argument_group("Ensembl VEP options")
    vep.add_argument(
        "--fasta", metavar="FILE",
        help="Reference FASTA. Required by --everything.",
    )
    vep.add_argument(
        "--everything", action="store_true",
        help="Enable all annotation features (80-field CSQ).",
    )
    vep.add_argument(
        "--fork", "--workers", dest="fork", type=int, default=1, metavar="N",
        help="Annotation pipelines to run. N>1 needs a tabix-indexed input.",
    )
    vep.add_argument(
        "--cache_version", type=int, metavar="N",
        help="Assert the cache version recorded in the Parquet metadata.",
    )

    vepyr_options = annotate.add_argument_group("vepyr options")
    vepyr_options.add_argument(
        "--plugin_cache_root", metavar="DIR",
        help="Root of a plugin cache tree holding plugin/<name>/ directories.",
    )
    vepyr_options.add_argument(
        "--plugin", action="append", dest="plugins", metavar="NAME",
        help="Restrict to this plugin. Repeatable; order is CSQ block order.",
    )
    vepyr_options.add_argument(
        "--no_progress", action="store_true",
        help="Suppress the progress bar.",
    )
    return parser


def annotate_kwargs(args: argparse.Namespace) -> dict:
    """Translate parsed arguments into :func:`vepyr.annotate` keyword arguments.

    The input VCF and cache directory are positional on the API side and are
    passed separately by :func:`main`.
    """
    kwargs: dict = {
        "output_vcf": args.output_file,
        "show_progress": not args.no_progress,
        "workers": args.fork,
    }
    if args.fasta is not None:
        kwargs["reference_fasta"] = args.fasta
    if args.everything:
        kwargs["everything"] = True
    if args.cache_version is not None:
        # annotate() validates this as a string.
        kwargs["expected_cache_version"] = str(args.cache_version)
    if args.plugin_cache_root is not None:
        kwargs["plugin_cache_root"] = args.plugin_cache_root
    if args.plugins is not None:
        kwargs["plugins"] = list(args.plugins)
    return kwargs
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_cli.py -v`
Expected: 11 passed.

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/vepyr/cli.py tests/test_cli.py && uv run ruff format --check src/vepyr/cli.py tests/test_cli.py`
Expected: no findings.

- [ ] **Step 6: Commit**

```bash
git add src/vepyr/cli.py tests/test_cli.py
git commit -m "feat(cli): flag parser and annotate() kwarg mapping"
```

---

### Task 2: `main()`, exit codes, and the console script

Wires the parser to the engine, converts API errors into a clean exit code, and installs the `vepyr` executable.

**Files:**
- Modify: `src/vepyr/cli.py` (append)
- Create: `src/vepyr/__main__.py`
- Modify: `pyproject.toml`
- Test: `tests/test_cli.py` (append)

**Interfaces:**
- Consumes: `build_parser()`, `annotate_kwargs()` from Task 1.
- Produces: `main(argv: list[str] | None = None) -> int`, returning 0 on success and 2 on a `ValueError`/`FileNotFoundError` from the API. Console script `vepyr = "vepyr.cli:main"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cli.py`:

```python
def test_main_forwards_positionals_and_kwargs(monkeypatch):
    import vepyr

    calls = []

    def spy(vcf, cache_dir, **kwargs):
        calls.append((vcf, cache_dir, kwargs))
        return kwargs["output_vcf"]

    monkeypatch.setattr(vepyr, "annotate", spy)

    from vepyr.cli import main

    code = main(
        [
            "annotate",
            "-i", "in.vcf",
            "-o", "out.vcf.gz",
            "--dir_cache", "/cache",
            "--everything",
            "--fasta", "ref.fa",
            "--no_progress",
        ]
    )

    assert code == 0
    assert len(calls) == 1
    vcf, cache_dir, kwargs = calls[0]
    assert vcf == "in.vcf"
    assert cache_dir == "/cache"
    assert kwargs["everything"] is True
    assert kwargs["reference_fasta"] == "ref.fa"
    assert kwargs["output_vcf"] == "out.vcf.gz"
    assert kwargs["show_progress"] is False


@pytest.mark.parametrize("error", [ValueError, FileNotFoundError])
def test_api_errors_become_exit_2_without_a_traceback(monkeypatch, capsys, error):
    import vepyr

    def boom(vcf, cache_dir, **kwargs):
        raise error("cache is unusable")

    monkeypatch.setattr(vepyr, "annotate", boom)

    from vepyr.cli import main

    code = main(["annotate", "-i", "in.vcf", "-o", "o.vcf", "--dir_cache", "/c"])

    assert code == 2
    captured = capsys.readouterr()
    assert "cache is unusable" in captured.err
    assert "Traceback" not in captured.err
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cli.py -k "main_forwards or api_errors" -v`
Expected: FAIL with `ImportError: cannot import name 'main' from 'vepyr.cli'`

- [ ] **Step 3: Write the implementation**

Append to `src/vepyr/cli.py`:

```python
def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``vepyr`` console script.

    Returns a process exit code: 0 on success, 2 when the annotation API
    rejects the request.
    """
    args = build_parser().parse_args(argv)

    # Imported here rather than at module scope so `vepyr --help` and
    # `vepyr --version` do not pay for loading the native extension.
    import vepyr

    try:
        vepyr.annotate(args.input_file, args.dir_cache, **annotate_kwargs(args))
    except (ValueError, FileNotFoundError) as exc:
        print(f"vepyr: error: {exc}", file=sys.stderr)
        return 2
    return 0
```

Create `src/vepyr/__main__.py`:

```python
"""Allow `python -m vepyr` to run the command-line interface."""

import sys

from vepyr.cli import main

if __name__ == "__main__":
    sys.exit(main())
```

In `pyproject.toml`, add immediately after the `[project.optional-dependencies]` block's final entry and before `[dependency-groups]`:

```toml
[project.scripts]
vepyr = "vepyr.cli:main"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_cli.py -v`
Expected: 14 passed.

- [ ] **Step 5: Reinstall so the console script lands on PATH**

Run: `env -u CONDA_PREFIX uv sync --reinstall-package vepyr`
Then: `uv run vepyr --version`
Expected: `vepyr 0.5.0` (the bump to 0.6.0 happens in Task 4).

- [ ] **Step 6: Verify `python -m vepyr` works too**

Run: `uv run python -m vepyr annotate --help`
Expected: the annotate help text, listing exactly `-i/--input_file`, `-o/--output_file`, `--dir_cache`, `--fasta`, `--everything`, `--fork/--workers`, `--cache_version`, `--plugin_cache_root`, `--plugin`, `--no_progress`.

- [ ] **Step 7: Commit**

```bash
git add src/vepyr/cli.py src/vepyr/__main__.py tests/test_cli.py pyproject.toml uv.lock
git commit -m "feat(cli): add the vepyr console script entry point"
```

---

### Task 3: End-to-end CLI run over the golden fixture

Proves the CLI actually annotates, through a real subprocess, against the 5.1 MB chr1 cache already in the repo.

**Files:**
- Modify: `tests/test_cli.py` (append)

**Interfaces:**
- Consumes: the `python -m vepyr` entry point from Task 2; `copy_cache_with_source_metadata` from `tests/cache_metadata.py`, whose signature is `(source_dir, target_dir, cache_source_type, cache_version) -> Path`.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_cli.py`. Note the imports go at the top of the file with the existing ones:

```python
# --- add to the imports at the top of the file ---
import gzip
import os
import subprocess
import sys
from pathlib import Path

from tests.cache_metadata import copy_cache_with_source_metadata

GOLDEN_DIR = Path(__file__).parent / "data" / "golden"
GOLDEN_CACHE = GOLDEN_DIR / "cache"
GOLDEN_INPUT = GOLDEN_DIR / "input.vcf.gz"
GOLDEN_FASTA = GOLDEN_DIR / "reference.fa"


# --- add at the end of the file ---
@pytest.fixture(scope="module")
def golden_cache(tmp_path_factory):
    """The golden cache, stamped with the source metadata annotate() requires."""
    if not GOLDEN_CACHE.is_dir():
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("cli_golden_cache")
    return str(copy_cache_with_source_metadata(GOLDEN_CACHE, target, "ensembl", "115"))


def test_cli_annotates_the_golden_fixture(tmp_path, golden_cache):
    output = tmp_path / "annotated.vcf.gz"

    result = subprocess.run(
        [
            sys.executable, "-m", "vepyr", "annotate",
            "-i", str(GOLDEN_INPUT),
            "-o", str(output),
            "--dir_cache", golden_cache,
            "--fasta", str(GOLDEN_FASTA),
            "--everything",
            "--cache_version", "115",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert output.exists()

    with gzip.open(output, "rt") as handle:
        lines = handle.read().splitlines()

    header = [line for line in lines if line.startswith("##")]
    records = [line for line in lines if not line.startswith("#")]

    # The fixture holds 100 variants; annotation must not drop or duplicate any.
    assert len(records) == 100
    assert any(line.startswith("##INFO=<ID=CSQ,") for line in header)
    assert all("CSQ=" in line.split("\t")[7] for line in records)


def test_cli_reports_a_bad_cache_version_and_exits_2(tmp_path, golden_cache):
    output = tmp_path / "annotated.vcf.gz"

    result = subprocess.run(
        [
            sys.executable, "-m", "vepyr", "annotate",
            "-i", str(GOLDEN_INPUT),
            "-o", str(output),
            "--dir_cache", golden_cache,
            "--cache_version", "116",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Traceback" not in result.stderr
```

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/test_cli.py -k "golden_fixture or bad_cache_version" -v`

Unlike Tasks 1 and 2, these are integration tests over units that already exist, so
they may pass on the first run. That is the correct outcome — they are a regression
gate on the wiring, not a driver for new code. What matters is that you *run* them
and read the output rather than assuming.

- [ ] **Step 3: Fix whatever they reveal**

Two failures are plausible and each has a specific fix:

- `test_cli_annotates_the_golden_fixture` exits non-zero. Read `result.stderr`. If it
  names a missing cache metadata key, the `copy_cache_with_source_metadata` arguments
  are wrong — compare against `tests/test_annotate.py:36`, which stamps the same
  fixture as `("ensembl", "115")`.
- `test_cli_reports_a_bad_cache_version_and_exits_2` exits 1 rather than 2. The
  version assertion raises an exception type not in `main()`'s `except` clause. Read
  the type out of `result.stderr` and add it to the tuple, then extend the docstring
  to say so. Do not add a bare `except Exception` — an unexpected crash must keep its
  traceback.

- [ ] **Step 4: Run the whole file**

Run: `uv run pytest tests/test_cli.py -v`
Expected: 16 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_cli.py
git commit -m "test(cli): end-to-end annotation run over the golden fixture"
```

---

### Task 4: Version bump to 0.6.0 and bioconda recipe

**Files:**
- Modify: `pyproject.toml:3`, `Cargo.toml:3`, `Cargo.lock`
- Modify: `bioconda-recipe/meta.yaml:2` and `:73`
- Test: `tests/test_cli.py` (append)

**Interfaces:**
- Consumes: `main()` and the console script from Task 2.
- Produces: package version `0.6.0`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_cli.py`:

```python
def test_cli_version_matches_the_installed_package():
    import vepyr

    result = subprocess.run(
        [sys.executable, "-m", "vepyr", "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == f"vepyr {vepyr.__version__}"
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/test_cli.py -k version_matches -v`
Expected: PASS at 0.5.0. This test guards the bump rather than driving it — it must keep passing after the version changes.

- [ ] **Step 3: Bump the version**

```bash
sed -i '' '3s/version = "0.5.0"/version = "0.6.0"/' pyproject.toml
sed -i '' '3s/version = "0.5.0"/version = "0.6.0"/' Cargo.toml
sed -i '' '2s/0\.5\.0/0.6.0/' bioconda-recipe/meta.yaml
sed -i '' '73s/0\.5\.0/0.6.0/' bioconda-recipe/meta.yaml
```

Verify each landed on the intended line:

```bash
sed -n '3p' pyproject.toml Cargo.toml; sed -n '2p;73p' bioconda-recipe/meta.yaml
```

- [ ] **Step 4: Refresh `Cargo.lock` and rebuild**

Run: `cargo check --quiet && env -u CONDA_PREFIX uv sync --reinstall-package vepyr`
Then: `uv run vepyr --version`
Expected: `vepyr 0.6.0`

- [ ] **Step 5: Add a CLI smoke test to the bioconda recipe**

In `bioconda-recipe/meta.yaml`, under `test:` → `commands:`, add these two lines directly after the existing `- pip check` line:

```yaml
    - vepyr --version
    - vepyr annotate --help
```

Leave the `sha256:` in `source:` untouched. It cannot be computed until the 0.6.0 sdist is on PyPI; updating it is part of the handoff in Task 7's README, not this task.

- [ ] **Step 6: Run the full Python suite**

Run: `uv run pytest tests/test_cli.py -v`
Expected: 17 passed, including `test_cli_version_matches_the_installed_package` now reporting 0.6.0.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml Cargo.toml Cargo.lock bioconda-recipe/meta.yaml tests/test_cli.py uv.lock
git commit -m "chore(release): 0.6.0, add CLI smoke tests to the conda recipe"
```

---

### Task 5: CLI documentation page

`meta.yml`'s `documentation:` URL must resolve, so this has to exist before the module is submitted.

**Files:**
- Create: `docs/cli.md`
- Modify: `mkdocs.yml`

**Interfaces:**
- Consumes: the flag set from Task 1.
- Produces: `https://biodatageeks.org/vepyr/cli/`, referenced by `meta.yml` in Task 6.

- [ ] **Step 1: Write the page**

Create `docs/cli.md`:

````markdown
# Command line

`vepyr annotate` is a VCF-in / VCF-out shell over [`annotate()`](api.md). It
covers the configuration validated against Ensembl VEP — `--everything` with a
reference FASTA — plus the options a workflow engine needs. Everything else,
including the Polars `LazyFrame` path, stays on the Python API.

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache /data/vep/116_GRCh38_ensembl \
    --fasta GRCh38.fa \
    --everything \
    --fork 8
```

## Options

| Flag | Description |
|---|---|
| `-i`, `--input_file FILE` | Input VCF (plain, gzip or bgzip). Required. |
| `-o`, `--output_file FILE` | Output VCF. A `.gz`/`.bgz` suffix selects bgzf. Required. |
| `--dir_cache DIR` | Parquet cache directory. Required. |
| `--fasta FILE` | Reference FASTA. Required by `--everything`. |
| `--everything` | Enable all annotation features (80-field CSQ). |
| `--fork N`, `--workers N` | Annotation pipelines to run. Default 1. |
| `--cache_version N` | Assert the cache version in the Parquet metadata. |
| `--plugin_cache_root DIR` | Root of a plugin cache tree. |
| `--plugin NAME` | Restrict to this plugin. Repeatable; order is CSQ block order. |
| `--no_progress` | Suppress the progress bar. |

Flag names follow Ensembl VEP's own spelling, so `ext.args` strings written for
`vep` carry over as the flag set grows.

## Notes

**Compression is inferred from the output suffix.** `-o out.vcf.gz` writes bgzf;
`-o out.vcf` writes plain text. There is no `--compress_output` flag.

**`--fork` above 1 needs an indexed input.** The input VCF must be bgzip-compressed
with a `.tbi` or `.csi` beside it, or the run fails. Results are identical to
`--fork 1`, row for row and in the same order.

**No index is written.** The output is bgzf but unindexed; run `tabix` afterwards
if you need one.

**Unknown flags are an error.** A VEP flag this interface does not implement — say
`--pick` — fails the run rather than being ignored, so a stale command line can
never silently produce differently-annotated output.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 2 | Bad arguments, or the annotation request was rejected (unusable cache, missing FASTA, unindexed input with `--fork` > 1). |
````

- [ ] **Step 2: Add the nav entry**

In `mkdocs.yml`, insert into the `nav:` → `Home:` list directly after the `- Quick start: quickstart.md` line:

```yaml
      - Command line: cli.md
```

- [ ] **Step 3: Verify the site builds with no broken links**

Run: `uv run mkdocs build --strict`
Expected: build succeeds, no warnings. `--strict` turns a bad `api.md` link into a failure.

- [ ] **Step 4: Commit**

```bash
git add docs/cli.md mkdocs.yml
git commit -m "docs: add the command-line reference page"
```

---

### Task 6: The nf-core module

**Files:**
- Create: `nf-core-module/.nf-core.yml`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/main.nf`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/meta.yml`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/environment.yml`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test`
- Create: `nf-core-module/modules/nf-core/vepyr/annotate/tests/nextflow.config`

**Interfaces:**
- Consumes: the `vepyr annotate` flag set from Task 1; the docs URL from Task 5.
- Produces: process `VEPYR_ANNOTATE` with five input channels in this order — `tuple val(meta), path(vcf), path(tbi)`; `tuple val(meta2), path(cache)`; `tuple val(meta3), path(fasta)`; `val cache_version`; `tuple val(meta4), path(plugin_cache)`. Emits `vcf` and `tbi`.

- [ ] **Step 1: Create the repository marker**

Create `nf-core-module/.nf-core.yml`:

```yaml
repository_type: modules
org_path: nf-core
```

This is what makes `nf-core modules lint` treat the directory as a modules repo. It is not copied upstream — nf-core/modules has its own.

- [ ] **Step 2: Write `environment.yml`**

Create `nf-core-module/modules/nf-core/vepyr/annotate/environment.yml`:

```yaml
---
# yaml-language-server: $schema=https://raw.githubusercontent.com/nf-core/modules/master/modules/environment-schema.json
channels:
  - conda-forge
  - bioconda
dependencies:
  # renovate: datasource=conda depName=bioconda/vepyr
  - bioconda::vepyr=0.6.0
  # renovate: datasource=conda depName=bioconda/htslib
  - bioconda::htslib=1.23.1
```

htslib is here because `vepyr annotate` writes bgzf but no index; `tabix` produces the `.tbi`, exactly as `ensemblvep/vep` does.

- [ ] **Step 3: Write `main.nf`**

Create `nf-core-module/modules/nf-core/vepyr/annotate/main.nf`:

```nextflow
process VEPYR_ANNOTATE {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    // TODO Replace both URIs once bioconda-recipes#68869 ships vepyr 0.6.0 and
    // the Seqera Wave image for this environment.yml has been built.
    container "${workflow.containerEngine in ['singularity', 'apptainer'] && !task.ext.singularity_pull_docker_container
        ? 'PLACEHOLDER_SINGULARITY_URI'
        : 'PLACEHOLDER_DOCKER_URI'}"

    input:
    tuple val(meta), path(vcf), path(tbi)
    tuple val(meta2), path(cache)
    tuple val(meta3), path(fasta)
    val cache_version
    tuple val(meta4), path(plugin_cache)

    output:
    tuple val(meta), path("${prefix}.vcf.gz"), emit: vcf
    tuple val(meta), path("${prefix}.vcf.gz.tbi"), emit: tbi
    tuple val("${task.process}"), val('vepyr'), eval("vepyr --version | cut -d' ' -f2"), topic: versions, emit: versions_vepyr
    tuple val("${task.process}"), val('tabix'), eval("tabix -h 2>&1 | grep -oP 'Version:\\s*\\K[^\\s]+'"), topic: versions, emit: versions_tabix

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def args2 = task.ext.args2 ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    def reference = fasta ? "--fasta ${fasta}" : ''
    def version_arg = cache_version ? "--cache_version ${cache_version}" : ''
    def plugin_arg = plugin_cache ? "--plugin_cache_root ${plugin_cache}" : ''
    // --fork above 1 requires a tabix/CSI index on the input; vepyr raises
    // without one. Fall back to a single pipeline so a missing index costs
    // throughput rather than failing the task.
    def fork = tbi ? task.cpus : 1
    """
    vepyr annotate \\
        -i ${vcf} \\
        -o ${prefix}.vcf.gz \\
        --dir_cache ${cache} \\
        ${reference} \\
        ${version_arg} \\
        ${plugin_arg} \\
        --fork ${fork} \\
        --no_progress \\
        ${args}

    tabix ${args2} ${prefix}.vcf.gz
    """

    stub:
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo "" | gzip > ${prefix}.vcf.gz
    touch ${prefix}.vcf.gz.tbi
    """
}
```

`--everything` is intentionally not hardcoded: it arrives through `ext.args`, matching how `ensemblvep/vep` handles its output-format flags.

- [ ] **Step 4: Write `meta.yml`**

Create `nf-core-module/modules/nf-core/vepyr/annotate/meta.yml`:

```yaml
name: vepyr_annotate
description: Annotate a VCF with Ensembl VEP consequences using vepyr, a Rust
  reimplementation of the Variant Effect Predictor. Annotation features are
  controlled through `task.ext.args`.
keywords:
  - annotation
  - vcf
  - variant
  - vep
tools:
  - vepyr:
      description: |
        vepyr (VEP Yielding Performant Results) is a Python interface to a Rust
        reimplementation of Ensembl's Variant Effect Predictor, built on Apache
        Arrow and DataFusion. It annotates variants against a Parquet cache
        converted from an Ensembl VEP offline cache.
      homepage: https://biodatageeks.org/vepyr/
      documentation: https://biodatageeks.org/vepyr/cli/
      tool_dev_url: https://github.com/biodatageeks/vepyr
      licence:
        - "Apache-2.0"
      identifier: ""
  - tabix:
      description: Generic indexer for TAB-delimited genome position files.
      homepage: https://www.htslib.org/doc/tabix.html
      documentation: https://www.htslib.org/doc/tabix.html
      licence:
        - "MIT"
      identifier: ""
input:
  - - meta:
        type: map
        description: |
          Groovy Map containing sample information
          e.g. [ id:'test', single_end:false ]
    - vcf:
        type: file
        description: VCF to annotate
        pattern: "*.{vcf,vcf.gz}"
        ontologies:
          - edam: http://edamontology.org/format_3016
    - tbi:
        type: file
        description: |
          tabix/CSI index of the input VCF (optional). Required to use more
          than one annotation pipeline; without it the process falls back to
          a single pipeline.
        pattern: "*.{tbi,csi}"
        ontologies: []
  - - meta2:
        type: map
        description: |
          Groovy Map containing cache information
          e.g. [ id:'116_GRCh38_ensembl' ]
    - cache:
        type: directory
        description: Parquet cache directory produced by vepyr's cache build
  - - meta3:
        type: map
        description: |
          Groovy Map containing reference information
          e.g. [ id:'GRCh38' ]
    - fasta:
        type: file
        description: |
          reference FASTA (optional). Required when `ext.args` contains
          `--everything`.
        pattern: "*.{fasta,fa}"
        ontologies: []
  - cache_version:
      type: integer
      description: |
        Ensembl cache version to assert against the cache metadata (optional)
  - - meta4:
        type: map
        description: |
          Groovy Map containing plugin cache information
          e.g. [ id:'plugin_cache_v0.1.1' ]
    - plugin_cache:
        type: directory
        description: |
          root of a vepyr plugin cache tree holding plugin/<name>/ directories
          (optional)
output:
  vcf:
    - - meta:
          type: map
          description: |
            Groovy Map containing sample information
      - ${prefix}.vcf.gz:
          type: file
          description: annotated VCF
          pattern: "*.vcf.gz"
          ontologies:
            - edam: http://edamontology.org/format_3989
  tbi:
    - - meta:
          type: map
          description: |
            Groovy Map containing sample information
      - ${prefix}.vcf.gz.tbi:
          type: file
          description: tabix index of the annotated VCF
          pattern: "*.vcf.gz.tbi"
          ontologies: []
  versions_vepyr:
    - - ${task.process}:
          type: string
          description: The process
      - vepyr:
          type: string
          description: The tool name
      - "vepyr --version | cut -d' ' -f2":
          type: eval
          description: The expression to obtain the version of the tool
  versions_tabix:
    - - ${task.process}:
          type: string
          description: The process
      - tabix:
          type: string
          description: The tool name
      - tabix -h 2>&1 | grep -oP 'Version:\s*\K[^\s]+':
          type: eval
          description: The expression to obtain the version of the tool
topics:
  versions:
    - - ${task.process}:
          type: string
          description: The process
      - vepyr:
          type: string
          description: The tool name
      - "vepyr --version | cut -d' ' -f2":
          type: eval
          description: The expression to obtain the version of the tool
    - - ${task.process}:
          type: string
          description: The process
      - tabix:
          type: string
          description: The tool name
      - tabix -h 2>&1 | grep -oP 'Version:\s*\K[^\s]+':
          type: eval
          description: The expression to obtain the version of the tool
authors:
  - "@mwiewior"
maintainers:
  - "@mwiewior"
```

- [ ] **Step 5: Write the nf-test files**

Create `nf-core-module/modules/nf-core/vepyr/annotate/tests/nextflow.config`:

```groovy
process {
    withName: VEPYR_ANNOTATE {
        ext.args = '--everything'
    }
}
```

Create `nf-core-module/modules/nf-core/vepyr/annotate/tests/main.nf.test`:

```groovy
nextflow_process {

    name "Test Process VEPYR_ANNOTATE"
    script "../main.nf"
    process "VEPYR_ANNOTATE"

    tag "modules"
    tag "modules_nfcore"
    tag "vepyr"
    tag "vepyr/annotate"

    test("homo_sapiens - vcf - everything") {
        config "./nextflow.config"

        when {
            process {
                """
                input[0] = channel.of([
                    [ id:'test' ],
                    file(params.modules_testdata_base_path + 'genomics/homo_sapiens/vepyr/input.vcf.gz', checkIfExists: true),
                    file(params.modules_testdata_base_path + 'genomics/homo_sapiens/vepyr/input.vcf.gz.tbi', checkIfExists: true)
                ])
                input[1] = channel.value([
                    [ id:'115_GRCh38_ensembl' ],
                    file(params.modules_testdata_base_path + 'genomics/homo_sapiens/vepyr/cache/', checkIfExists: true)
                ])
                input[2] = channel.value([
                    [ id:'GRCh38' ],
                    file(params.modules_testdata_base_path + 'genomics/homo_sapiens/vepyr/reference.fa', checkIfExists: true)
                ])
                input[3] = 115
                input[4] = [[], []]
                """
            }
        }

        then {
            assertAll(
                { assert process.success },
                { assert snapshot(
                    file(process.out.vcf[0][1]).name + ",variantsMD5:" + path(process.out.vcf[0][1]).vcf.variantsMD5,
                    file(process.out.tbi[0][1]).name,
                    process.out.findAll { key, val -> key.startsWith("versions") }
                ).match() }
            )
        }
    }

    test("homo_sapiens - vcf - stub") {
        options "-stub"

        when {
            process {
                """
                input[0] = channel.of([
                    [ id:'test' ],
                    file(params.modules_testdata_base_path + 'genomics/homo_sapiens/vepyr/input.vcf.gz', checkIfExists: true),
                    []
                ])
                input[1] = channel.value([[ id:'cache' ], []])
                input[2] = channel.value([[ id:'GRCh38' ], []])
                input[3] = []
                input[4] = [[], []]
                """
            }
        }

        then {
            assertAll(
                { assert process.success },
                { assert snapshot(process.out).match() }
            )
        }
    }
}
```

There is no `main.nf.test.snap` yet. It can only be generated by a real run, which needs both the container (Task 7 handoff item 2) and the test data (item 3).

- [ ] **Step 6: Lint the module**

Run:

```bash
cd nf-core-module && uvx --from nf-core nf-core modules lint vepyr/annotate; cd -
```

Two checks are **expected to fail** at this stage and must not be "fixed" by
weakening the module:

- the container URI check, because `PLACEHOLDER_DOCKER_URI` is not a resolvable
  image — it clears at handoff item 3;
- the nf-test snapshot check, because `main.nf.test.snap` cannot exist until there
  is a container and test data — handoff item 5.

Every *other* finding is real and must be fixed before committing: `meta.yml`
structure, input/output count or ordering mismatches against `main.nf`, a missing
`when:` block, missing `tag` directives, or a malformed `environment.yml`. Re-run
until those two known failures are the only ones left, and record the exact
remaining list in the commit message so the next person can tell drift from the
expected baseline.

- [ ] **Step 7: Commit**

```bash
git add nf-core-module
git commit -m "feat(nf-core): add the vepyr/annotate module"
```

---

### Task 7: Test-data staging script and handoff README

Turns the remaining outward-facing work into something mechanical.

**Files:**
- Create: `nf-core-module/stage-testdata.sh`
- Create: `nf-core-module/README.md`

**Interfaces:**
- Consumes: the fixture paths under `tests/data/golden/`; the test file paths asserted in Task 6's `main.nf.test`.
- Produces: a directory tree matching `genomics/homo_sapiens/vepyr/` in nf-core/test-datasets.

- [ ] **Step 1: Write the staging script**

Create `nf-core-module/stage-testdata.sh`:

```bash
#!/usr/bin/env bash
# Stage the golden fixture into the layout nf-core/test-datasets expects.
#
# Usage: ./stage-testdata.sh <output-dir>
#
# Copy the resulting genomics/ tree into a clone of the `modules` branch of
# nf-core/test-datasets and open a PR. The paths produced here are exactly the
# ones modules/nf-core/vepyr/annotate/tests/main.nf.test reads.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <output-dir>" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
golden="${repo_root}/tests/data/golden"
target="$1/genomics/homo_sapiens/vepyr"

if [[ ! -d "${golden}/cache" ]]; then
    echo "golden fixture not found at ${golden}/cache" >&2
    exit 1
fi

mkdir -p "${target}"
cp -R "${golden}/cache" "${target}/cache"
cp "${golden}/input.vcf.gz" "${golden}/input.vcf.gz.tbi" "${target}/"
cp "${golden}/reference.fa" "${golden}/reference.fa.fai" "${target}/"

echo "staged to ${target}"
du -sh "${target}"
```

- [ ] **Step 2: Make it executable and run it**

```bash
chmod +x nf-core-module/stage-testdata.sh
./nf-core-module/stage-testdata.sh /tmp/vepyr-testdata
```

Expected: prints `staged to /tmp/vepyr-testdata/genomics/homo_sapiens/vepyr` and a total of roughly 6 MB.

- [ ] **Step 3: Verify the staged paths match the nf-test**

```bash
(cd /tmp/vepyr-testdata && find genomics -maxdepth 4 -not -path '*/cache/*' | sort)
```

Expected, exactly:

```
genomics
genomics/homo_sapiens
genomics/homo_sapiens/vepyr
genomics/homo_sapiens/vepyr/cache
genomics/homo_sapiens/vepyr/input.vcf.gz
genomics/homo_sapiens/vepyr/input.vcf.gz.tbi
genomics/homo_sapiens/vepyr/reference.fa
genomics/homo_sapiens/vepyr/reference.fa.fai
```

Every path referenced by `main.nf.test` must appear here. If one does not, fix the script, not the test.

- [ ] **Step 4: Write the handoff README**

Create `nf-core-module/README.md`:

````markdown
# nf-core module: `vepyr/annotate`

Staging area for the nf-core/modules submission. `modules/nf-core/vepyr/annotate/`
mirrors the upstream path, so it copies verbatim into a nf-core/modules fork:

```bash
cp -R modules/nf-core/vepyr /path/to/modules-fork/modules/nf-core/
```

`.nf-core.yml` here only exists so `nf-core modules lint` treats this directory as
a modules repository. Do not copy it upstream.

## Linting

```bash
uvx --from nf-core nf-core modules lint vepyr/annotate
```

## Remaining work, in order

1. **Release vepyr 0.6.0 to PyPI.** The CLI this module wraps first ships in 0.6.0.
2. **Update [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869) to 0.6.0.**
   Bump `version`, replace `sha256` with the 0.6.0 sdist digest, and keep the
   `vepyr --version` / `vepyr annotate --help` test commands. Bumping the open PR
   rather than filing a follow-up avoids ever publishing a container without a
   `vepyr` executable.
3. **Resolve the container URIs.** Replace `PLACEHOLDER_DOCKER_URI` and
   `PLACEHOLDER_SINGULARITY_URI` in `main.nf` with the Seqera Wave image built from
   `environment.yml`.
4. **PR the test data.** Run `./stage-testdata.sh <dir>` and copy the resulting
   `genomics/` tree into the `modules` branch of nf-core/test-datasets. About 6 MB.
5. **Generate the snapshot.** With 3 and 4 done:
   `nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test --update-snapshot`
6. **Open the nf-core/modules PR.**

## Scope

Ten flags, covering the configuration validated against Ensembl VEP
(`--everything` with `--fasta`) plus what a workflow engine needs. The pick family,
HGVS sub-flags, AF sub-flags and `--fields` are all additive later and need no
engine work — see `docs/superpowers/specs/2026-09-07-nf-core-vepyr-module-design.md`.
````

- [ ] **Step 5: Commit**

```bash
git add nf-core-module/stage-testdata.sh nf-core-module/README.md
git commit -m "chore(nf-core): stage test data and document the submission handoff"
```

---

## Final verification

- [ ] `uv run pytest tests/test_cli.py -v` — 17 passed
- [ ] `uv run pytest` — no regressions elsewhere
- [ ] `uv run ruff check . && uv run ruff format --check .`
- [ ] `uv run mkdocs build --strict`
- [ ] `cd nf-core-module && uvx --from nf-core nf-core modules lint vepyr/annotate` — the only remaining failures are the container URI and the missing nf-test snapshot
- [ ] `uv run vepyr --version` prints `vepyr 0.6.0`
