# Download published caches — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `vepyr.download_cache()` / `vepyr.download_plugin_cache()` and the CLI
subcommands `vepyr download-cache` / `vepyr download-plugin-cache` fetch a
published vepyr cache or plugin cache from Hugging Face, optionally for selected
contigs, into a directory that `annotate()` accepts as is.

**Architecture:** A pin table (`vepyr/_published.py`) maps release, assembly and
cache or plugin name to a dataset repository and a pinned commit. A download
module (`vepyr/_download.py`) resolves the revision, validates the requested
contigs against the dataset's file list, builds `allow_patterns`, and calls
`huggingface_hub.snapshot_download` once, translating library exceptions into
`ValueError` / `RuntimeError`. `vepyr/__init__.py` re-exports the two public
functions; `vepyr/cli.py` adds two subcommands that follow the `build-cache`
pattern.

**Tech Stack:** Python 3.10+, `huggingface_hub>=1.0,<3`, argparse, pytest.

**Spec:** `docs/superpowers/specs/2026-10-12-download-cache-design.md`

## Global Constraints

- Dependency: `huggingface_hub>=1.0,<3` in `pyproject.toml` base dependencies
  and `huggingface_hub >=1.0,<3` under `run` in `bioconda-recipe/meta.yaml`.
- Core repos: `biodatageeks/vepyr_{release}_{assembly}_{cache_type}`; plugin
  repos: `biodatageeks/vepyr_{release}_{assembly}_plugin_{plugin}`; always
  `repo_type="dataset"`.
- Pins (release 116, GRCh38): ensembl `058d887ec86163333fcbdd20c28ef75683faa8b5`,
  refseq `ae28273ce0b4744201ddaecd71a7ea8bf2c19d95`,
  merged `5b83dd8d249106c6cc3f1c04c522b4bec716cc97`,
  plugin_cadd `bcfca1e030b6d372086f9b9f2bf7aae3295d321a`,
  plugin_spliceai `7158a9c34758b1afe8a56b9e433640b9d3845920`,
  plugin_alphamissense `00558a86d2ec30c5ed68ff291164786228534b19`,
  plugin_clinvar `44b0029ed830bd5d4666b5575610e6f3ae56a2ba`,
  plugin_phenotypeorthologous `08b9b2ff0ae9a8d09c04ff1e6cb4ebb6838fd534`.
- Destinations: `<cache_dir>/<release>_<assembly>_<cache_type>` (the same
  helper `_converted_cache_dir()` that `build_cache()` uses) and
  `<plugin_cache_root>/plugin/<plugin>`; `~` expanded; directories created.
- `allow_patterns` always set; never fetch `README.md` or `.gitattributes`.
  Core all: `*/*.parquet`, `*/chrom_manifest.json`, `reference_policy.json`,
  `chr_synonyms.txt`. Core subset: `*/<contig>.parquet` per contig plus the
  three sidecar patterns. Plugin all: `*.parquet`, `*.json`. Plugin subset:
  `<contig>.parquet` per contig plus `*.json`.
- Errors: no pin and no revision, unknown `cache_type`, dbNSFP, unknown or
  empty `chroms`, `RepositoryNotFoundError`, `RevisionNotFoundError` →
  `ValueError`; other HTTP errors, connection errors, `LocalEntryNotFoundError`
  → `RuntimeError`. Every message names the repository and revision.
- CLI exit codes: 0 success, 2 for bad arguments or a rejected request; errors
  are one line `vepyr: error: …` on stderr.
- Tests use no network unless `VEPYR_NETWORK_TESTS=1`.
- Run Python with the worktree venv: `.venv/bin/python -m pytest …`. Do not
  run `uv run` or `uv sync` (they rebuild and replace the installed package).
- Lint: `.venv/bin/pre-commit run ruff --files <files>` and
  `.venv/bin/pre-commit run ruff-format --files <files>`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Mitochondrial name:** the datasets name it `chrMT`, not `chrM`. `--chrom MT`
   must resolve to `chrMT`; `--chrom chrM` must fail with a message that lists
   the available main contigs so the user sees `chrMT`. (Task 3.)
2. **No partial side effects on rejection:** an unknown contig, an unpinned
   release or dbNSFP must raise before any directory is created or any file is
   downloaded. (Tasks 3 and 4.)
3. **Plugin names that escape the tree:** `plugin="../x"` or `"a/b"` must be
   rejected with `ValueError` before any path or repository id is built.
   (Task 4.)
4. **Transport errors from the HTTP client:** `huggingface_hub` 1.x raises
   `httpx` errors and 2.x `httpx2` errors, which are not `OSError`; both must
   become `RuntimeError`, not tracebacks. (Task 2.)
5. **Progress state restored on failure:** `show_progress=False` must re-enable
   the library's bars after the call even when the download raises, and must
   not enable them if they were already disabled (e.g.
   `HF_HUB_DISABLE_PROGRESS_BARS=1`). (Task 2.)

---

## File Structure

| File | Responsibility |
|---|---|
| `src/vepyr/_published.py` (create) | Pin table, repository ids, revision resolution and its error messages |
| `src/vepyr/_download.py` (create) | Contig selection, `allow_patterns`, progress toggle, error translation, the two public functions |
| `src/vepyr/__init__.py` (modify) | Re-export `download_cache`, `download_plugin_cache`; add to `__all__` |
| `src/vepyr/cli.py` (modify) | `download-cache` and `download-plugin-cache` subcommands |
| `tests/test_published.py` (create) | Pin table and revision resolution |
| `tests/test_download.py` (create) | API behaviour with a fake hub; opt-in network test |
| `tests/test_cli_download.py` (create) | CLI mapping, output and errors |
| `pyproject.toml`, `uv.lock`, `bioconda-recipe/meta.yaml` (modify) | Dependency |
| `docs/downloads.md`, `docs/cli.md`, `docs/api.md` (modify) | Documentation |

---

### Task 1: Pin table and dependency

**Files:**
- Create: `src/vepyr/_published.py`
- Modify: `pyproject.toml` (`dependencies` line), `uv.lock`, `bioconda-recipe/meta.yaml` (`run:`)
- Test: `tests/test_published.py`

**Interfaces:**
- Produces:
  - `PINS: dict[tuple[int, str, str], str]`
  - `cache_repo_id(release: int, assembly: str, cache_type: str) -> str`
  - `plugin_repo_id(release: int, assembly: str, plugin: str) -> str`
  - `cache_revision(release: int, assembly: str, cache_type: str, revision: str | None) -> str`
  - `plugin_revision(release: int, assembly: str, plugin: str, revision: str | None) -> str`

- [ ] **Step 1: Install the dependency into the worktree venv**

```bash
uv pip install --python .venv/bin/python "huggingface_hub>=1.0,<3"
```

- [ ] **Step 2: Write the failing tests** — `tests/test_published.py`

```python
"""Tests for the published-cache pin table."""

from __future__ import annotations

import re

import pytest

from vepyr import _published

RELEASE_116 = {
    "ensembl",
    "merged",
    "refseq",
    "plugin_cadd",
    "plugin_spliceai",
    "plugin_alphamissense",
    "plugin_clinvar",
    "plugin_phenotypeorthologous",
}


def test_every_pin_is_a_full_lowercase_commit_hash():
    for key, revision in _published.PINS.items():
        assert re.fullmatch(r"[0-9a-f]{40}", revision), key


def test_release_116_pins_every_published_cache_and_plugin():
    names = {name for (release, assembly, name) in _published.PINS if (release, assembly) == (116, "GRCh38")}
    assert names == RELEASE_116


def test_repository_ids():
    assert _published.cache_repo_id(116, "GRCh38", "merged") == "biodatageeks/vepyr_116_GRCh38_merged"
    assert (
        _published.plugin_repo_id(116, "GRCh38", "clinvar")
        == "biodatageeks/vepyr_116_GRCh38_plugin_clinvar"
    )


def test_pinned_revision_is_the_default():
    assert (
        _published.cache_revision(116, "GRCh38", "merged", None)
        == "5b83dd8d249106c6cc3f1c04c522b4bec716cc97"
    )
    assert (
        _published.plugin_revision(116, "GRCh38", "clinvar", None)
        == "44b0029ed830bd5d4666b5575610e6f3ae56a2ba"
    )


@pytest.mark.parametrize("revision", ["main", "v1.0", "0123456"])
def test_explicit_revision_is_passed_through_even_when_unpinned(revision):
    assert _published.cache_revision(116, "GRCh38", "merged", revision) == revision
    assert _published.cache_revision(999, "GRCh37", "merged", revision) == revision
    assert _published.plugin_revision(999, "GRCh38", "revel", revision) == revision


def test_unpinned_cache_lists_what_is_published():
    with pytest.raises(ValueError) as excinfo:
        _published.cache_revision(115, "GRCh38", "merged", None)
    message = str(excinfo.value)
    assert "biodatageeks/vepyr_115_GRCh38_merged" in message
    assert "116 GRCh38: ensembl, merged, refseq" in message
    assert "build_cache" in message


def test_unpinned_plugin_lists_what_is_published():
    with pytest.raises(ValueError) as excinfo:
        _published.plugin_revision(116, "GRCh38", "revel", None)
    message = str(excinfo.value)
    assert "alphamissense, cadd, clinvar, phenotypeorthologous, spliceai" in message
    assert "build_plugin_cache" in message


@pytest.mark.parametrize("revision", [None, "main"])
def test_dbnsfp_explains_why_it_is_not_published(revision):
    with pytest.raises(ValueError, match="redistribution"):
        _published.plugin_revision(116, "GRCh38", "dbnsfp", revision)
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_published.py -q`
Expected: FAIL, `ImportError: cannot import name '_published'`.

- [ ] **Step 4: Implement** — `src/vepyr/_published.py`

```python
"""Published vepyr caches on Hugging Face and the revisions this release pins.

Each cache and plugin cache is a public dataset repository. A pin is the full
commit hash of the dataset revision this vepyr release was validated against;
:func:`vepyr.download_cache` and :func:`vepyr.download_plugin_cache` fetch it
unless another revision is requested.
"""

from __future__ import annotations

HF_ORG = "biodatageeks"

# (release, assembly, name) -> commit; name is a cache type or "plugin_<plugin>".
PINS: dict[tuple[int, str, str], str] = {
    (116, "GRCh38", "ensembl"): "058d887ec86163333fcbdd20c28ef75683faa8b5",
    (116, "GRCh38", "refseq"): "ae28273ce0b4744201ddaecd71a7ea8bf2c19d95",
    (116, "GRCh38", "merged"): "5b83dd8d249106c6cc3f1c04c522b4bec716cc97",
    (116, "GRCh38", "plugin_cadd"): "bcfca1e030b6d372086f9b9f2bf7aae3295d321a",
    (116, "GRCh38", "plugin_spliceai"): "7158a9c34758b1afe8a56b9e433640b9d3845920",
    (116, "GRCh38", "plugin_alphamissense"): "00558a86d2ec30c5ed68ff291164786228534b19",
    (116, "GRCh38", "plugin_clinvar"): "44b0029ed830bd5d4666b5575610e6f3ae56a2ba",
    (116, "GRCh38", "plugin_phenotypeorthologous"): "08b9b2ff0ae9a8d09c04ff1e6cb4ebb6838fd534",
}

_PLUGIN_PREFIX = "plugin_"


def cache_repo_id(release: int, assembly: str, cache_type: str) -> str:
    """Dataset repository of a core cache."""
    return f"{HF_ORG}/vepyr_{release}_{assembly}_{cache_type}"


def plugin_repo_id(release: int, assembly: str, plugin: str) -> str:
    """Dataset repository of a plugin cache."""
    return f"{HF_ORG}/vepyr_{release}_{assembly}_{_PLUGIN_PREFIX}{plugin}"


def _published(plugins: bool) -> str:
    """``"116 GRCh38: a, b; ..."`` for the pinned caches or plugin caches."""
    groups: dict[tuple[int, str], list[str]] = {}
    for (release, assembly, name) in PINS:
        if name.startswith(_PLUGIN_PREFIX) == plugins:
            groups.setdefault((release, assembly), []).append(
                name.removeprefix(_PLUGIN_PREFIX)
            )
    return "; ".join(
        f"{release} {assembly}: {', '.join(sorted(names))}"
        for (release, assembly), names in sorted(groups.items())
    )


def cache_revision(
    release: int, assembly: str, cache_type: str, revision: str | None
) -> str:
    """The revision to fetch: ``revision`` if given, else the pin."""
    if revision is not None:
        return revision
    pin = PINS.get((release, assembly, cache_type))
    if pin is None:
        raise ValueError(
            f"no published revision of {cache_repo_id(release, assembly, cache_type)} "
            f"is pinned in this vepyr release. Published caches: {_published(False)}. "
            "Pass revision= (e.g. 'main') to fetch an unpinned dataset, or convert "
            "an Ensembl VEP cache with build_cache()."
        )
    return pin


def plugin_revision(
    release: int, assembly: str, plugin: str, revision: str | None
) -> str:
    """The revision to fetch: ``revision`` if given, else the pin."""
    if plugin == "dbnsfp":
        raise ValueError(
            "dbNSFP is supported, but its licence does not allow redistribution, "
            "so no dbNSFP plugin cache is published; build it locally with "
            "build_plugin_cache()."
        )
    if revision is not None:
        return revision
    pin = PINS.get((release, assembly, _PLUGIN_PREFIX + plugin))
    if pin is None:
        raise ValueError(
            f"no published revision of {plugin_repo_id(release, assembly, plugin)} "
            f"is pinned in this vepyr release. Published plugin caches: "
            f"{_published(True)}. Pass revision= (e.g. 'main') to fetch an "
            "unpinned dataset, or build one with build_plugin_cache()."
        )
    return pin
```

- [ ] **Step 5: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_published.py -q`
Expected: 11 passed.

- [ ] **Step 6: Add the dependency**

In `pyproject.toml` change the `dependencies` line to:

```toml
dependencies = ["pyarrow>=23.0.1,<25", "polars>=1.40.0,<2", "tqdm>=4.67.0,<5", "huggingface_hub>=1.0,<3"]
```

In `bioconda-recipe/meta.yaml`, under `run:` after `- tqdm >=4.67.0,<5`, add:

```yaml
    - huggingface_hub >=1.0,<3
```

Then update the lock file only (no sync):

```bash
uv lock
git diff --stat uv.lock   # expect huggingface-hub and its dependencies added
```

- [ ] **Step 7: Lint and commit**

```bash
.venv/bin/pre-commit run ruff --files src/vepyr/_published.py tests/test_published.py
.venv/bin/pre-commit run ruff-format --files src/vepyr/_published.py tests/test_published.py
git add src/vepyr/_published.py tests/test_published.py pyproject.toml uv.lock bioconda-recipe/meta.yaml
git commit -m "feat(download): pin table for published caches; depend on huggingface_hub

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Download helpers (seams, progress toggle, error translation)

**Files:**
- Create: `src/vepyr/_download.py`
- Test: `tests/test_download.py`

**Interfaces:**
- Consumes: nothing from Task 1 yet.
- Produces (module `vepyr._download`):
  - `_list_repo_files(repo_id: str, *, revision: str) -> list[str]` — seam over `HfApi().list_repo_files(repo_id, repo_type="dataset", revision=revision)`
  - `_snapshot_download(repo_id: str, *, revision: str, local_dir: str, allow_patterns: list[str]) -> str` — seam over `huggingface_hub.snapshot_download(..., repo_type="dataset")`
  - `_progress(show: bool)` — context manager
  - `_hub_errors(repo_id: str, revision: str)` — context manager translating exceptions

- [ ] **Step 1: Write the failing tests** — `tests/test_download.py`

```python
"""Tests for vepyr.download_cache and vepyr.download_plugin_cache."""

from __future__ import annotations

import pytest
from huggingface_hub import errors
from huggingface_hub import utils as hub_utils

from vepyr import _download


def hub_error(cls, message="fake"):
    """An instance of a huggingface_hub HTTP error without an HTTP response."""
    error = cls.__new__(cls)
    OSError.__init__(error, message)
    return error


# A transport error as httpx (huggingface_hub 1.x) or httpx2 (2.x) raises it.
TransportError = type("ConnectError", (Exception,), {"__module__": "httpx2._exceptions"})


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (hub_error(errors.RepositoryNotFoundError), ValueError),
        (hub_error(errors.RevisionNotFoundError), ValueError),
        (hub_error(errors.HfHubHTTPError), RuntimeError),
        (errors.LocalEntryNotFoundError("offline"), RuntimeError),
        (ConnectionError("refused"), RuntimeError),
        (TransportError("refused"), RuntimeError),
    ],
)
def test_hub_errors_are_translated_and_name_the_dataset(error, expected):
    with pytest.raises(expected) as excinfo:
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise error
    assert "biodatageeks/x" in str(excinfo.value)
    assert "abc123" in str(excinfo.value)


def test_other_errors_pass_through():
    with pytest.raises(KeyError):
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise KeyError("bug")


def test_progress_false_disables_and_restores_even_on_error():
    hub_utils.enable_progress_bars()
    with pytest.raises(RuntimeError):
        with _download._progress(False):
            assert hub_utils.are_progress_bars_disabled()
            raise RuntimeError("boom")
    assert not hub_utils.are_progress_bars_disabled()


def test_progress_false_keeps_bars_disabled_if_they_already_were():
    hub_utils.disable_progress_bars()
    try:
        with _download._progress(False):
            pass
        assert hub_utils.are_progress_bars_disabled()
    finally:
        hub_utils.enable_progress_bars()


def test_progress_true_leaves_the_state_alone():
    hub_utils.enable_progress_bars()
    with _download._progress(True):
        assert not hub_utils.are_progress_bars_disabled()
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_download.py -q`
Expected: FAIL, `ImportError: cannot import name '_download'`.

- [ ] **Step 3: Implement** — `src/vepyr/_download.py`

```python
"""Download published vepyr caches and plugin caches from Hugging Face."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator


def _list_repo_files(repo_id: str, *, revision: str) -> list[str]:
    """File paths of a dataset repository at ``revision``."""
    from huggingface_hub import HfApi

    return HfApi().list_repo_files(repo_id, repo_type="dataset", revision=revision)


def _snapshot_download(
    repo_id: str, *, revision: str, local_dir: str, allow_patterns: list[str]
) -> str:
    """Fetch the matching files of a dataset repository into ``local_dir``."""
    from huggingface_hub import snapshot_download

    return snapshot_download(
        repo_id,
        repo_type="dataset",
        revision=revision,
        local_dir=local_dir,
        allow_patterns=allow_patterns,
    )


@contextlib.contextmanager
def _progress(show: bool) -> Iterator[None]:
    """Hide the library's progress bars for the block when ``show`` is false."""
    from huggingface_hub import utils

    if show:
        yield
        return
    already_disabled = utils.are_progress_bars_disabled()
    utils.disable_progress_bars()
    try:
        yield
    finally:
        if not already_disabled:
            utils.enable_progress_bars()


def _is_transport_error(exc: Exception) -> bool:
    # huggingface_hub 1.x uses httpx, 2.x httpx2; neither error is an OSError.
    return type(exc).__module__.split(".")[0] in ("httpx", "httpx2")


@contextlib.contextmanager
def _hub_errors(repo_id: str, revision: str) -> Iterator[None]:
    """Translate huggingface_hub exceptions into ValueError / RuntimeError."""
    from huggingface_hub import errors

    where = f"{repo_id} at revision {revision}"
    try:
        yield
    except (errors.RepositoryNotFoundError, errors.RevisionNotFoundError) as exc:
        raise ValueError(f"{where} was not found on Hugging Face: {exc}") from exc
    except errors.LocalEntryNotFoundError as exc:
        raise RuntimeError(
            f"{where} is not available offline and could not be downloaded: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(f"could not download {where}: {exc}") from exc
    except Exception as exc:
        if _is_transport_error(exc):
            raise RuntimeError(f"could not download {where}: {exc}") from exc
        raise
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_download.py -q`
Expected: 10 passed.

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/pre-commit run ruff --files src/vepyr/_download.py tests/test_download.py
.venv/bin/pre-commit run ruff-format --files src/vepyr/_download.py tests/test_download.py
git add src/vepyr/_download.py tests/test_download.py
git commit -m "feat(download): Hugging Face seams, progress toggle and error translation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `download_cache()`

**Files:**
- Modify: `src/vepyr/_download.py` (append), `src/vepyr/__init__.py` (import and `__all__`)
- Test: `tests/test_download.py` (append)

**Interfaces:**
- Consumes: `_published.cache_repo_id`, `_published.cache_revision` (Task 1); `_list_repo_files`, `_snapshot_download`, `_progress`, `_hub_errors` (Task 2); `vepyr._validate_cache_type(cache_type) -> None` and `vepyr._converted_cache_dir(cache_dir, release, cache_type, assembly) -> str` (existing, `src/vepyr/__init__.py`).
- Produces:
  - `download_cache(release: int, cache_dir: str, *, cache_type: str, assembly: str = "GRCh38", chroms: list[str] | None = None, revision: str | None = None, show_progress: bool = True) -> str`
  - `_select_contigs(requested: list[str] | None, available: set[str], where: str) -> list[str] | None`
  - `_CORE_SIDECARS: list[str]`

- [ ] **Step 1: Write the failing tests** — append to `tests/test_download.py`

```python
import fnmatch
import os

import vepyr

CORE_FILES = [
    ".gitattributes",
    "README.md",
    "chr_synonyms.txt",
    "reference_policy.json",
    *(
        f"{entity}/{contig}.parquet"
        for entity in ("exon", "variation")
        for contig in ("chr1", "chr22", "chrX", "chrMT", "GL000008.2")
    ),
    "exon/chrom_manifest.json",
    "variation/chrom_manifest.json",
]
CORE_SIDECARS = ["*/chrom_manifest.json", "reference_policy.json", "chr_synonyms.txt"]
MERGED_PIN = "5b83dd8d249106c6cc3f1c04c522b4bec716cc97"


class FakeHub:
    """Records calls and writes the files the patterns select into local_dir."""

    def __init__(self, files, error=None):
        self.files = files
        self.error = error
        self.listed = []
        self.snapshots = []

    def list_repo_files(self, repo_id, *, revision):
        self.listed.append((repo_id, revision))
        if self.error is not None:
            raise self.error
        return list(self.files)

    def snapshot_download(self, repo_id, *, revision, local_dir, allow_patterns):
        self.snapshots.append(
            {
                "repo_id": repo_id,
                "revision": revision,
                "local_dir": local_dir,
                "allow_patterns": allow_patterns,
            }
        )
        if self.error is not None:
            raise self.error
        for name in self.files:
            if any(fnmatch.fnmatch(name, p) for p in allow_patterns):
                path = os.path.join(local_dir, name)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    f.write("x")
        return local_dir


@pytest.fixture
def hub(monkeypatch):
    def install(files, error=None):
        fake = FakeHub(files, error)
        monkeypatch.setattr(_download, "_list_repo_files", fake.list_repo_files)
        monkeypatch.setattr(_download, "_snapshot_download", fake.snapshot_download)
        return fake

    return install


def test_full_cache_uses_the_pin_and_skips_readme(hub, tmp_path):
    fake = hub(CORE_FILES)
    path = vepyr.download_cache(116, str(tmp_path), cache_type="merged", show_progress=False)
    assert path == str(tmp_path / "116_GRCh38_merged")
    (call,) = fake.snapshots
    assert call["repo_id"] == "biodatageeks/vepyr_116_GRCh38_merged"
    assert call["revision"] == MERGED_PIN
    assert call["local_dir"] == path
    assert call["allow_patterns"] == ["*/*.parquet", *CORE_SIDECARS]
    assert fake.listed == []  # no listing without chroms
    assert not os.path.exists(os.path.join(path, "README.md"))
    assert os.path.isfile(os.path.join(path, "variation", "chr22.parquet"))


@pytest.mark.parametrize(
    ("chroms", "contigs"),
    [
        (["22"], ["chr22"]),
        (["chr22"], ["chr22"]),
        (["X", "chrX"], ["chrX"]),
        (["MT"], ["chrMT"]),
        (["GL000008.2"], ["GL000008.2"]),
        (["22", "1"], ["chr22", "chr1"]),
    ],
)
def test_chroms_select_shards_plus_sidecars(hub, tmp_path, chroms, contigs):
    fake = hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", chroms=chroms, show_progress=False
    )
    assert fake.listed == [("biodatageeks/vepyr_116_GRCh38_merged", MERGED_PIN)]
    patterns = fake.snapshots[0]["allow_patterns"]
    assert patterns == [*(f"*/{c}.parquet" for c in contigs), *CORE_SIDECARS]


def test_unknown_contig_fails_before_downloading_and_lists_main_contigs(hub, tmp_path):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError) as excinfo:
        vepyr.download_cache(
            116, str(tmp_path / "c"), cache_type="merged", chroms=["chrM", "22"]
        )
    message = str(excinfo.value)
    assert "chrM" in message and "chrMT" in message
    assert "biodatageeks/vepyr_116_GRCh38_merged" in message and MERGED_PIN in message
    assert fake.snapshots == []
    assert not (tmp_path / "c").exists()


def test_empty_chroms_is_rejected(hub, tmp_path):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError, match="at least one"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=[])
    assert fake.snapshots == []


def test_explicit_revision_is_passed_through(hub, tmp_path):
    fake = hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="ensembl", revision="main", show_progress=False
    )
    assert fake.snapshots[0]["revision"] == "main"


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"cache_type": "vep"}, "Invalid cache_type"),
        ({"cache_type": "merged", "assembly": "GRCh37"}, "116 GRCh38: ensembl, merged, refseq"),
    ],
)
def test_rejected_requests_create_nothing(hub, tmp_path, kwargs, match):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError, match=match):
        vepyr.download_cache(116, str(tmp_path / "c"), **kwargs)
    assert fake.snapshots == [] and not (tmp_path / "c").exists()


def test_tilde_is_expanded(hub, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    hub(CORE_FILES)
    path = vepyr.download_cache(116, "~/caches", cache_type="merged", show_progress=False)
    assert path == str(tmp_path / "caches" / "116_GRCh38_merged")


def test_download_errors_are_translated(hub, tmp_path):
    hub(CORE_FILES, error=hub_error(errors.RepositoryNotFoundError))
    with pytest.raises(ValueError, match="vepyr_116_GRCh38_merged at revision"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", show_progress=False)


def test_listing_errors_are_translated(hub, tmp_path):
    hub(CORE_FILES, error=ConnectionError("refused"))
    with pytest.raises(RuntimeError, match="could not download"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_download.py -q`
Expected: the new tests FAIL with `AttributeError: module 'vepyr' has no attribute 'download_cache'`.

- [ ] **Step 3: Implement** — append to `src/vepyr/_download.py`

```python
import os
import re

from vepyr import _published

_CORE_SIDECARS = ["*/chrom_manifest.json", "reference_policy.json", "chr_synonyms.txt"]


def _natural_key(name: str) -> list:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", name)]


def _select_contigs(
    requested: list[str] | None, available: set[str], where: str
) -> list[str] | None:
    """Map requested names to dataset contigs; ``None`` means all contigs.

    A name is matched as given; failing that, a name without a ``chr`` prefix
    is tried with one (``22`` -> ``chr22``, ``MT`` -> ``chrMT``). Duplicates
    after matching are dropped, keeping the first occurrence.
    """
    if requested is None:
        return None
    if not requested:
        raise ValueError("chroms must name at least one contig, or be None for all")
    selected: list[str] = []
    missing: list[str] = []
    for name in requested:
        if name in available:
            contig = name
        elif not name.startswith("chr") and f"chr{name}" in available:
            contig = f"chr{name}"
        else:
            missing.append(name)
            continue
        if contig not in selected:
            selected.append(contig)
    if missing:
        main = sorted((c for c in available if c.startswith("chr")), key=_natural_key)
        others = len(available) - len(main)
        listing = ", ".join(main) + (f" and {others} other contigs" if others else "")
        raise ValueError(f"{where} has no contig {', '.join(missing)}; available: {listing}")
    return selected


def _shard_contigs(files: list[str], nested: bool) -> set[str]:
    """Contig names of the ``.parquet`` shards (``<entity>/<contig>`` if nested)."""
    depth = 2 if nested else 1
    return {
        name.rsplit("/", 1)[-1].removesuffix(".parquet")
        for name in files
        if name.endswith(".parquet") and name.count("/") == depth - 1
    }


def download_cache(
    release: int,
    cache_dir: str,
    *,
    cache_type: str,
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:
    """Download a published vepyr cache from Hugging Face.

    Parameters
    ----------
    release : int
        Ensembl release, e.g. ``116``.
    cache_dir : str
        Root directory; the cache is written to
        ``<cache_dir>/<release>_<assembly>_<cache_type>``, the layout
        :func:`build_cache` produces. ``~`` is expanded.
    cache_type : {"ensembl", "merged", "refseq"}
        Cache type.
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    chroms : list of str or None
        Contigs to download, e.g. ``["22"]`` or ``["chr22", "chrX"]``; a name
        without ``chr`` also matches the prefixed one. ``None`` downloads every
        contig. The per-entity manifests and the root sidecars are always
        included.
    revision : str or None
        Dataset revision. ``None`` uses the revision pinned by this vepyr
        release; any branch, tag or commit (e.g. ``"main"``) is passed on.
    show_progress : bool
        Show download progress bars (default: True).

    Returns
    -------
    str
        The cache directory, ready for ``annotate(cache_dir=...)``.

    Re-running resumes and skips files already present.
    """
    import vepyr

    vepyr._validate_cache_type(cache_type)
    repo_id = _published.cache_repo_id(release, assembly, cache_type)
    resolved = _published.cache_revision(release, assembly, cache_type, revision)
    where = f"{repo_id} at revision {resolved}"
    contigs = None
    if chroms is not None:
        if not chroms:
            _select_contigs(chroms, set(), where)
        with _hub_errors(repo_id, resolved):
            files = _list_repo_files(repo_id, revision=resolved)
        contigs = _select_contigs(chroms, _shard_contigs(files, nested=True), where)
    shards = ["*/*.parquet"] if contigs is None else [f"*/{c}.parquet" for c in contigs]
    target = vepyr._converted_cache_dir(
        os.path.expanduser(cache_dir), release, cache_type, assembly
    )
    os.makedirs(target, exist_ok=True)
    with _progress(show_progress), _hub_errors(repo_id, resolved):
        _snapshot_download(
            repo_id,
            revision=resolved,
            local_dir=target,
            allow_patterns=[*shards, *_CORE_SIDECARS],
        )
    return target
```

Move the new `import os` / `import re` / `from vepyr import _published` lines to the top of the module with the other imports.

In `src/vepyr/__init__.py`, add after the existing `from vepyr._vcf_metadata import attach as _attach_vcf_metadata` line:

```python
from vepyr._download import download_cache
```

and add `"download_cache",` to `__all__` (alphabetical: after `"cache_contig_identity",`).

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_download.py tests/test_published.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/pre-commit run ruff --files src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
.venv/bin/pre-commit run ruff-format --files src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
git add src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
git commit -m "feat(download): download_cache() fetches a published core cache

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `download_plugin_cache()`

**Files:**
- Modify: `src/vepyr/_download.py` (append), `src/vepyr/__init__.py`
- Test: `tests/test_download.py` (append)

**Interfaces:**
- Consumes: `_published.plugin_repo_id`, `_published.plugin_revision` (Task 1); `_select_contigs`, `_shard_contigs`, `_progress`, `_hub_errors`, seams (Tasks 2–3).
- Produces: `download_plugin_cache(plugin: str, release: int, plugin_cache_root: str, *, assembly: str = "GRCh38", chroms: list[str] | None = None, revision: str | None = None, show_progress: bool = True) -> str`

- [ ] **Step 1: Write the failing tests** — append to `tests/test_download.py`

```python
PLUGIN_FILES = [
    ".gitattributes",
    "README.md",
    "manifest.json",
    "qa_profile.json",
    *(f"chr{c}.parquet" for c in ("1", "22", "X", "MT")),
]
CLINVAR_PIN = "44b0029ed830bd5d4666b5575610e6f3ae56a2ba"


def test_full_plugin_cache(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    path = vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    assert path == str(tmp_path / "plugin" / "clinvar")
    (call,) = fake.snapshots
    assert call["repo_id"] == "biodatageeks/vepyr_116_GRCh38_plugin_clinvar"
    assert call["revision"] == CLINVAR_PIN
    assert call["allow_patterns"] == ["*.parquet", "*.json"]
    assert os.path.isfile(os.path.join(path, "manifest.json"))
    assert not os.path.exists(os.path.join(path, "README.md"))


def test_plugin_chroms(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    vepyr.download_plugin_cache(
        "clinvar", 116, str(tmp_path), chroms=["22", "MT"], show_progress=False
    )
    assert fake.snapshots[0]["allow_patterns"] == ["chr22.parquet", "chrMT.parquet", "*.json"]


@pytest.mark.parametrize("plugin", ["../x", "a/b", "", "clin var"])
def test_plugin_names_that_are_not_plain_are_rejected(hub, tmp_path, plugin):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(ValueError, match="plugin name"):
        vepyr.download_plugin_cache(plugin, 116, str(tmp_path / "pc"))
    assert fake.snapshots == [] and not (tmp_path / "pc").exists()


@pytest.mark.parametrize(
    ("plugin", "match"),
    [("dbnsfp", "redistribution"), ("revel", "alphamissense, cadd, clinvar")],
)
def test_unpublished_plugins_create_nothing(hub, tmp_path, plugin, match):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(ValueError, match=match):
        vepyr.download_plugin_cache(plugin, 116, str(tmp_path / "pc"))
    assert fake.snapshots == [] and not (tmp_path / "pc").exists()


def test_unknown_plugin_contig(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(ValueError, match="no contig chrM; available: chr1, chr22, chrMT, chrX"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), chroms=["chrM"])
    assert fake.snapshots == []
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_download.py -q`
Expected: the new tests FAIL with `AttributeError: module 'vepyr' has no attribute 'download_plugin_cache'`.

- [ ] **Step 3: Implement** — append to `src/vepyr/_download.py`

```python
_PLUGIN_NAME = re.compile(r"[A-Za-z0-9_-]+")


def download_plugin_cache(
    plugin: str,
    release: int,
    plugin_cache_root: str,
    *,
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:
    """Download a published vepyr plugin cache from Hugging Face.

    Parameters
    ----------
    plugin : str
        Plugin name, e.g. ``"clinvar"``. dbNSFP is supported by
        :func:`build_plugin_cache` but not published.
    release : int
        Ensembl release, e.g. ``116``.
    plugin_cache_root : str
        Root of the plugin cache tree; the cache is written to
        ``<plugin_cache_root>/plugin/<plugin>``. ``~`` is expanded.
    assembly : str
        Genome assembly (default: ``"GRCh38"``).
    chroms : list of str or None
        Contigs to download, as for :func:`download_cache`. The manifest is
        always included.
    revision : str or None
        Dataset revision; ``None`` uses the pinned one.
    show_progress : bool
        Show download progress bars (default: True).

    Returns
    -------
    str
        The plugin cache directory. Pass ``plugin_cache_root`` and
        ``plugins=[plugin]`` to :func:`annotate`.
    """
    if not _PLUGIN_NAME.fullmatch(plugin):
        raise ValueError(
            f"invalid plugin name {plugin!r}: use letters, digits, '_' or '-'"
        )
    repo_id = _published.plugin_repo_id(release, assembly, plugin)
    resolved = _published.plugin_revision(release, assembly, plugin, revision)
    where = f"{repo_id} at revision {resolved}"
    contigs = None
    if chroms is not None:
        if not chroms:
            _select_contigs(chroms, set(), where)
        with _hub_errors(repo_id, resolved):
            files = _list_repo_files(repo_id, revision=resolved)
        contigs = _select_contigs(chroms, _shard_contigs(files, nested=False), where)
    shards = ["*.parquet"] if contigs is None else [f"{c}.parquet" for c in contigs]
    target = os.path.join(os.path.expanduser(plugin_cache_root), "plugin", plugin)
    os.makedirs(target, exist_ok=True)
    with _progress(show_progress), _hub_errors(repo_id, resolved):
        _snapshot_download(
            repo_id,
            revision=resolved,
            local_dir=target,
            allow_patterns=[*shards, "*.json"],
        )
    return target
```

The error message says "invalid plugin name"; the test matches `"plugin name"`.

In `src/vepyr/__init__.py` change the import to
`from vepyr._download import download_cache, download_plugin_cache` and add
`"download_plugin_cache",` to `__all__` after `"download_cache",`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_download.py tests/test_published.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/pre-commit run ruff --files src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
.venv/bin/pre-commit run ruff-format --files src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
git add src/vepyr/_download.py src/vepyr/__init__.py tests/test_download.py
git commit -m "feat(download): download_plugin_cache() fetches a published plugin cache

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: CLI subcommands

**Files:**
- Modify: `src/vepyr/cli.py` (parsers after `_add_build_plugin_cache_parser`, kwargs helpers, summaries, `main()`)
- Test: `tests/test_cli_download.py` (create)

**Interfaces:**
- Consumes: `vepyr.download_cache`, `vepyr.download_plugin_cache` (Tasks 3–4); `_published.cache_repo_id`, `plugin_repo_id`, `cache_revision`, `plugin_revision` (Task 1).
- Produces: `download_cache_kwargs(args) -> dict`, `download_plugin_cache_kwargs(args) -> dict` in `vepyr.cli`.

- [ ] **Step 1: Write the failing tests** — `tests/test_cli_download.py`

```python
"""Tests for `vepyr download-cache` and `vepyr download-plugin-cache`."""

from __future__ import annotations

import pytest

import vepyr
from vepyr.cli import (
    build_parser,
    download_cache_kwargs,
    download_plugin_cache_kwargs,
    main,
)

CACHE = ("download-cache", "--release", "116", "--cache_type", "merged", "--dir_cache", "/c")
PLUGIN = (
    "download-plugin-cache",
    "--plugin",
    "clinvar",
    "--release",
    "116",
    "--plugin_cache_root",
    "/pc",
)


def _parse(*argv):
    return build_parser().parse_args(list(argv))


def test_download_cache_minimal():
    args = _parse(*CACHE)
    assert (args.release, args.dir_cache) == (116, "/c")
    assert download_cache_kwargs(args) == {"cache_type": "merged", "show_progress": True}


def test_download_cache_options():
    args = _parse(
        *CACHE, "--assembly", "GRCh38", "--chrom", "22", "--chrom", "X",
        "--revision", "main", "--no_progress",
    )
    assert download_cache_kwargs(args) == {
        "cache_type": "merged",
        "show_progress": False,
        "assembly": "GRCh38",
        "chroms": ["22", "X"],
        "revision": "main",
    }


def test_download_plugin_cache_options():
    args = _parse(*PLUGIN, "--chrom", "22", "--revision", "abc", "--no_progress")
    assert (args.plugin, args.release, args.plugin_cache_root) == ("clinvar", 116, "/pc")
    assert download_plugin_cache_kwargs(args) == {
        "show_progress": False,
        "chroms": ["22"],
        "revision": "abc",
    }


@pytest.mark.parametrize(
    "argv",
    [
        CACHE[:1] + CACHE[3:],  # no --release
        CACHE[:3] + CACHE[5:],  # no --cache_type
        CACHE[:5],  # no --dir_cache
        (*CACHE[:4], "vep", *CACHE[5:]),  # bad --cache_type
        PLUGIN[:1] + PLUGIN[3:],  # no --plugin
        PLUGIN[:5],  # no --plugin_cache_root
    ],
)
def test_required_and_invalid_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        _parse(*argv)
    assert excinfo.value.code == 2


def test_download_cache_prints_source_and_next_step(monkeypatch, capsys):
    calls = []

    def spy(release, cache_dir, **kwargs):
        calls.append((release, cache_dir, kwargs))
        return "/c/116_GRCh38_merged"

    monkeypatch.setattr(vepyr, "download_cache", spy)
    assert main([*CACHE, "--chrom", "22"]) == 0
    assert calls == [(116, "/c", {"cache_type": "merged", "show_progress": True, "chroms": ["22"]})]
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "Downloaded biodatageeks/vepyr_116_GRCh38_merged @ 5b83dd8 (22) -> /c/116_GRCh38_merged",
        "Next: vepyr annotate --dir_cache /c/116_GRCh38_merged ...",
    ]


def test_download_plugin_cache_prints_source_and_next_step(monkeypatch, capsys):
    monkeypatch.setattr(vepyr, "download_plugin_cache", lambda *a, **k: "/pc/plugin/clinvar")
    assert main([*PLUGIN, "--revision", "main"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "Downloaded biodatageeks/vepyr_116_GRCh38_plugin_clinvar @ main -> /pc/plugin/clinvar",
        "Next: vepyr annotate ... --plugin_cache_root /pc --plugin clinvar",
    ]


@pytest.mark.parametrize("error", [ValueError, RuntimeError])
@pytest.mark.parametrize(("argv", "name"), [(CACHE, "download_cache"), (PLUGIN, "download_plugin_cache")])
def test_api_errors_become_one_line_and_exit_2(monkeypatch, capsys, error, argv, name):
    def boom(*args, **kwargs):
        raise error("no such dataset")

    monkeypatch.setattr(vepyr, name, boom)
    assert main(list(argv)) == 2
    assert capsys.readouterr().err == "vepyr: error: no such dataset\n"


def test_dbnsfp_exits_2_without_a_network_call(capsys, tmp_path):
    argv = ["download-plugin-cache", "--plugin", "dbnsfp", "--release", "116"]
    assert main([*argv, "--plugin_cache_root", str(tmp_path)]) == 2
    assert "redistribution" in capsys.readouterr().err
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_cli_download.py -q`
Expected: FAIL, `ImportError: cannot import name 'download_cache_kwargs'`.

- [ ] **Step 3: Implement** in `src/vepyr/cli.py`

Add after `_add_build_plugin_cache_parser(subcommands)` in `build_parser()`:

```python
    _add_download_cache_parser(subcommands)
    _add_download_plugin_cache_parser(subcommands)
```

Add after the `_add_build_plugin_cache_parser` function:

```python
def _add_download_options(options) -> None:
    """Options shared by the two download subcommands."""
    options.add_argument(
        "--assembly", metavar="NAME", help="Genome assembly (default: GRCh38)."
    )
    options.add_argument(
        "--chrom",
        action="append",
        dest="chroms",
        metavar="NAME",
        help="Download only this contig, e.g. 22 or chr22. Repeatable.",
    )
    options.add_argument(
        "--revision",
        metavar="REV",
        help="Dataset branch, tag or commit (default: the revision pinned by "
        "this vepyr release).",
    )
    options.add_argument(
        "--no_progress", action="store_true", help="Suppress progress bars."
    )


def _add_download_cache_parser(subcommands) -> None:
    download = subcommands.add_parser(
        "download-cache",
        help="Download a published vepyr cache from Hugging Face.",
        description="Download a published vepyr Parquet cache from Hugging Face "
        "into DIR_CACHE/<release>_<assembly>_<cache_type>.",
        allow_abbrev=False,
    )
    required = download.add_argument_group("required arguments")
    required.add_argument(
        "--release", required=True, type=int, metavar="N", help="Ensembl release."
    )
    required.add_argument(
        "--cache_type", required=True, choices=_CACHE_TYPES, help="Cache type."
    )
    required.add_argument(
        "--dir_cache",
        required=True,
        metavar="DIR",
        help="Root directory under which the cache directory is created.",
    )
    _add_download_options(download.add_argument_group("options"))


def _add_download_plugin_cache_parser(subcommands) -> None:
    download = subcommands.add_parser(
        "download-plugin-cache",
        help="Download a published plugin cache from Hugging Face.",
        description="Download a published vepyr plugin cache from Hugging Face "
        "into PLUGIN_CACHE_ROOT/plugin/<plugin>/.",
        allow_abbrev=False,
    )
    required = download.add_argument_group("required arguments")
    required.add_argument(
        "--plugin", required=True, metavar="NAME", help="Plugin name, e.g. clinvar."
    )
    required.add_argument(
        "--release", required=True, type=int, metavar="N", help="Ensembl release."
    )
    required.add_argument(
        "--plugin_cache_root",
        required=True,
        metavar="DIR",
        help="Root of the plugin cache tree.",
    )
    _add_download_options(download.add_argument_group("options"))
```

Add after `build_plugin_cache_kwargs`:

```python
def _download_options(args: argparse.Namespace) -> dict:
    kwargs: dict = {"show_progress": not args.no_progress}
    for name in ("assembly", "chroms", "revision"):
        value = getattr(args, name)
        if value is not None:
            kwargs[name] = value
    return kwargs


def download_cache_kwargs(args: argparse.Namespace) -> dict:
    """Translate ``download-cache`` arguments into keywords; unset options keep the API defaults."""
    return {"cache_type": args.cache_type, **_download_options(args)}


def download_plugin_cache_kwargs(args: argparse.Namespace) -> dict:
    """Translate ``download-plugin-cache`` arguments into keywords."""
    return _download_options(args)


def _print_download_summary(args, repo_id: str, revision: str, path: str) -> None:
    shown = revision[:7] if re.fullmatch(r"[0-9a-f]{40}", revision) else revision
    contigs = f" ({', '.join(args.chroms)})" if args.chroms else ""
    print(f"Downloaded {repo_id} @ {shown}{contigs} -> {path}")
```

In `main()`, add before the final `else:` of the command dispatch:

```python
        elif args.command in ("download-cache", "download-plugin-cache"):
            from vepyr import _published

            assembly = args.assembly or "GRCh38"
            if args.command == "download-cache":
                path = vepyr.download_cache(
                    args.release, args.dir_cache, **download_cache_kwargs(args)
                )
                repo_id = _published.cache_repo_id(args.release, assembly, args.cache_type)
                revision = _published.cache_revision(
                    args.release, assembly, args.cache_type, args.revision
                )
                _print_download_summary(args, repo_id, revision, path)
                print(f"Next: vepyr annotate --dir_cache {path} ...")
            else:
                path = vepyr.download_plugin_cache(
                    args.plugin, args.release, args.plugin_cache_root,
                    **download_plugin_cache_kwargs(args),
                )
                repo_id = _published.plugin_repo_id(args.release, assembly, args.plugin)
                revision = _published.plugin_revision(
                    args.release, assembly, args.plugin, args.revision
                )
                _print_download_summary(args, repo_id, revision, path)
                print(
                    "Next: vepyr annotate ... "
                    f"--plugin_cache_root {args.plugin_cache_root} --plugin {args.plugin}"
                )
```

Also replace `"GRCh38"` in that block with `vepyr._DEFAULT_ASSEMBLY` (the constant already used by `build_cache`), so the default is defined once.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_cli_download.py tests/test_cli.py tests/test_cli_build.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/pre-commit run ruff --files src/vepyr/cli.py tests/test_cli_download.py
.venv/bin/pre-commit run ruff-format --files src/vepyr/cli.py tests/test_cli_download.py
git add src/vepyr/cli.py tests/test_cli_download.py
git commit -m "feat(cli): download-cache and download-plugin-cache subcommands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Opt-in network test and documentation

**Files:**
- Modify: `tests/test_download.py` (append), `docs/downloads.md`, `docs/cli.md`, `docs/api.md`, `docs/superpowers/specs/2026-10-12-download-cache-design.md` (network-test paragraph)

- [ ] **Step 1: Network test** — append to `tests/test_download.py`

```python
network = pytest.mark.skipif(
    os.environ.get("VEPYR_NETWORK_TESTS") != "1",
    reason="set VEPYR_NETWORK_TESTS=1 to download from Hugging Face",
)


@network
def test_network_download_opens_in_the_engine(tmp_path):
    """Real downloads of one small contig; the engine opens the result."""
    cache = vepyr.download_cache(
        116, str(tmp_path / "c"), cache_type="merged", chroms=["MT"], show_progress=False
    )
    assert os.path.isfile(os.path.join(cache, "variation", "chrMT.parquet"))
    assert os.path.isfile(os.path.join(cache, "reference_policy.json"))
    # The resume metadata stays inside the cache and must not disturb the engine.
    assert os.path.isdir(os.path.join(cache, ".cache", "huggingface"))
    identity = vepyr.cache_contig_identity(cache, "chrMT")
    assert identity

    plugin = vepyr.download_plugin_cache(
        "phenotypeorthologous", 116, str(tmp_path / "pc"), chroms=["22"], show_progress=False
    )
    assert sorted(os.listdir(plugin)) == [".cache", "chr22.parquet", "manifest.json"]
```

Run: `VEPYR_NETWORK_TESTS=1 .venv/bin/python -m pytest tests/test_download.py -k network -q`
Expected: 1 passed. (Full annotation of downloaded caches is verified on real
data outside CI, in the supplement's chromosome-22 snippets.)

Update the spec's "Network test" paragraph to say the test checks
`cache_contig_identity()` on the downloaded core cache and the plugin file set,
and that annotation of downloaded caches is verified on HG002 chromosome 22.

- [ ] **Step 2: `docs/api.md`** — add after the `::: vepyr.build_plugin_cache` block, in the same format as the existing entries:

```markdown
::: vepyr.download_cache

::: vepyr.download_plugin_cache
```

- [ ] **Step 3: `docs/cli.md`** — add a section after "Building caches":

````markdown
## Downloading caches

`vepyr download-cache` fetches a published cache from Hugging Face, wrapping
[`download_cache()`](api.md#vepyr.download_cache). It writes
`<dir_cache>/<release>_<assembly>_<cache_type>`, the directory `annotate`
takes as `--dir_cache`:

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache ~/vepyr_cache --chrom 22
```

| Flag | Description |
|---|---|
| `--release N` | Ensembl release. Required. |
| `--cache_type {ensembl,merged,refseq}` | Cache type. Required. |
| `--dir_cache DIR` | Root directory for the cache. Required. |
| `--assembly NAME` | Default `GRCh38`. |
| `--chrom NAME` | Download only this contig (`22` or `chr22`). Repeatable. The manifests and sidecar files are always included. |
| `--revision REV` | Dataset branch, tag or commit. Default: the revision pinned by this vepyr release. |
| `--no_progress` | Suppress progress bars. |

`vepyr download-plugin-cache` does the same for a plugin cache, writing
`<plugin_cache_root>/plugin/<plugin>/`:

```bash
vepyr download-plugin-cache --plugin clinvar --release 116 \
    --plugin_cache_root ~/vepyr_plugin_cache --chrom 22
```

It takes `--plugin NAME`, `--release N` and `--plugin_cache_root DIR`
(required) and the same `--assembly`, `--chrom`, `--revision` and
`--no_progress` options. dbNSFP is not published (its licence does not allow
redistribution); build it with `build-plugin-cache`.

Re-running a download resumes and skips files already present.
````

- [ ] **Step 4: `docs/downloads.md`** — in "## Hugging Face (recommended)", put the vepyr commands first and keep `hf download` as the manual alternative. Replace the paragraph and block that starts with "Install the client once:" up to (not including) "The download is resumable" with:

````markdown
vepyr downloads them itself; the Hugging Face client is installed with it:

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache ~/vepyr_cache
```

or from Python:

```python
import vepyr

cache = vepyr.download_cache(116, "~/vepyr_cache", cache_type="merged")
```

`--cache_type` is `merged`, `ensembl` or `refseq`. The default revision is the
one pinned by your vepyr release, so a given vepyr version always fetches the
same cache; pass `--revision main` (or `revision="main"`) for the latest.

??? note "Manual alternative: the `hf` command"
    === "merged"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_merged \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_merged
        ```

    === "ensembl"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_ensembl \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_ensembl
        ```

    === "refseq"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_refseq \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_refseq
        ```
````

In "### Downloading only part of a cache", put this before the existing `hf download … --include` block and introduce that block with "With `hf`:":

````markdown
```bash
vepyr download-cache --release 116 --cache_type merged \
  --dir_cache ~/vepyr_cache --chrom 22
```

`--chrom` adds the manifests and sidecar files itself.
````

In "### Downloading" under "## Plugin caches", add before the existing `hf download` tabs:

````markdown
```bash
vepyr download-plugin-cache --plugin clinvar --release 116 \
  --plugin_cache_root ~/vepyr_plugin_cache
```
````

- [ ] **Step 5: Build the docs and run the whole suite**

```bash
.venv/bin/python -m mkdocs build --strict 2>&1 | tail -3   # if mkdocs is installed in the venv; otherwise skip
.venv/bin/python -m pytest -q -x
```

Expected: no new failures (network test skipped).

- [ ] **Step 6: Commit**

```bash
git add tests/test_download.py docs/api.md docs/cli.md docs/downloads.md docs/superpowers/specs/2026-10-12-download-cache-design.md
git commit -m "docs(download): document download-cache and download-plugin-cache; opt-in network test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: End-to-end verification on HG002 chromosome 22 (outside CI)

Not part of the PR's tests; this is the check that the downloaded caches
annotate exactly as the `hf download` ones. Uses the supplement's Quick start
data in `$Q` (`HG002_chr22.vcf.gz`, `chr22.fa`, and the outputs made with
`hf download` caches: `HG002_chr22.annotated.vcf.gz`,
`HG002_chr22.plugins.vcf.gz`).

- [ ] **Step 1: Download with the new commands**

```bash
cd $(mktemp -d) && Q=<quick start dir>
vepyr download-cache --release 116 --cache_type ensembl --dir_cache vepyr_cache --chrom 22
for p in clinvar alphamissense; do
  vepyr download-plugin-cache --plugin $p --release 116 \
    --plugin_cache_root vepyr_plugin_cache --chrom 22
done
```

- [ ] **Step 2: Annotate and compare**

```bash
vepyr annotate -i $Q/HG002_chr22.vcf.gz -o core.vcf.gz \
  --dir_cache vepyr_cache/116_GRCh38_ensembl --fasta $Q/chr22.fa --fork 4
vepyr annotate -i $Q/HG002_chr22.vcf.gz -o plugins.vcf.gz \
  --dir_cache vepyr_cache/116_GRCh38_ensembl --fasta $Q/chr22.fa --fork 4 \
  --plugin_cache_root vepyr_plugin_cache --plugin clinvar --plugin alphamissense
for f in core:HG002_chr22.annotated plugins:HG002_chr22.plugins; do
  a=${f%%:*}; b=${f#*:}
  diff <(gzip -dc $a.vcf.gz | grep -v '^##vepyr') <(gzip -dc $Q/$b.vcf.gz | grep -v '^##vepyr') \
    && echo "$a identical"
done
```

Expected: `core identical`, `plugins identical`. Header lines recording the
command or cache path are excluded by the `^##vepyr` filter; if other header
lines differ only in the cache path, compare bodies with `grep -v '^#'` and
report that.
