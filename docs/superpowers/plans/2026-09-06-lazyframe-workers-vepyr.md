# LazyFrame Workers: vepyr Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `vepyr.annotate(..., workers=N).collect()` run N annotation pipelines per contig on the engine's new streaming run pool, prove it equals `workers=1` row for row on fixtures and real data, and correct the documentation that calls the LazyFrame path serial.

**Architecture:** The engine does the work (see the engine plan). vepyr adds one early check (an index is required for `workers>1` on both output paths), pins the engine revision, tests parity on the golden fixtures at `workers>1` (plain, with a pushed-down predicate, with `head()`, with the CSQ column), adds a real-data gate and a timing sweep, and rewrites the `workers` documentation.

**Tech Stack:** Python 3.10+, Polars, PyO3 0.28 + maturin, pytest, `e2e-testing/scripts/comparison` helpers (`profiles`, `vcfio`).

**Spec:** `docs/superpowers/specs/2026-09-06-lazyframe-parallel-workers-design.md`. Engine prerequisite: `docs/superpowers/plans/2026-09-06-lazyframe-workers-engine.md` merged upstream.

## Global Constraints

- Contract: `annotate(..., workers=N).collect()` equals `annotate(..., workers=1).collect()` including row order, on every cache source, with and without a pushed-down predicate, with and without a row limit, with `skip_csq` in both settings.
- `workers > 1` on either output path requires `<vcf>.tbi` or `<vcf>.csi` next to the input; `annotate()` raises `ValueError` before any engine call otherwise. The message names the missing index.
- Build with `CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr` (both env vars are set in this shell and make maturin refuse).
- Run tests with `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest <path> -v`. `uv run ruff check .` and `uv run ruff format .` must be clean before each commit; `cargo clippy` and `cargo fmt` for the Rust bridge when it changes.
- Work on the existing branch `feat/lazyframe-parallel-workers` (it already carries the spec commit `76f2d1f`). Commit messages follow the repo's conventional style (`feat(annotate): ...`, `test: ...`, `docs: ...`, `chore: ...`) and end with the session trailer from the harness instructions.
- `CLAUDE.md` asks for a GSD entry point before edits; `.planning/ROADMAP.md` is absent, so `/gsd:quick` fails. The user approved this design and plan in chat; proceed outside GSD.
- Benchmarks: discard the first run of a session and check the load average before trusting any timing (`uptime`); the host is shared.

---

## File structure

| File | Responsibility |
|---|---|
| `src/vepyr/__init__.py` | `_require_index_for_workers(vcf, workers)` and its call in `annotate()`; docstring for `workers`. |
| `src/annotate.rs` | Comment correction only (the streaming path is not drained in parallel by Polars). |
| `tests/test_annotate.py` | Index-check tests on both paths (fake annotator / fake writer). |
| `tests/test_lazyframe_workers.py` (new) | Golden-fixture parity at `workers>1` on Ensembl and merged caches. |
| `Cargo.toml`, `Cargo.lock` | Engine pin bump. |
| `docs/quickstart.md`, `docs/performance.md`, `docs/dataframes.md` | `workers` applies to both paths; timing table. |
| `e2e-testing/scripts/lazyframe_workers_parity.py` (new) | Real-data parity gate and `workers` sweep on HG002 contig slices. |

Reference points in `src/vepyr/__init__.py` at `d37e16e`:

- `annotate()` signature starts at 1075 (`workers: int = 1` at 1118, `output_vcf` at 1125); the `workers` docstring entry at 1231-1235.
- Validation: `if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:` at 1387.
- `if workers > 1:` adds the option at 1464.
- `if output_vcf is not None:` at 1511 (the VCF-sink branch).
- `_batch_source` at 1740; `annotator = _create_annotator(` at 1788.

---

### Task 1: Index check for `workers > 1`

**Files:**
- Modify: `src/vepyr/__init__.py`
- Test: `tests/test_annotate.py`

**Interfaces:**
- Produces:
  ```python
  def _require_index_for_workers(vcf: str, workers: int) -> None
  ```
  Raises `ValueError` when `workers > 1` and neither `vcf + ".tbi"` nor `vcf + ".csi"` exists.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_annotate.py` in the class that holds `test_workers_rejects_invalid_values` (the golden `input.vcf` is a plain, unindexed copy of `input.vcf.gz`):

```python
    UNINDEXED_VCF = str(GOLDEN_DIR / "input.vcf")

    def test_workers_above_one_requires_index_on_lazyframe_path(self, monkeypatch):
        import vepyr

        calls = []
        monkeypatch.setattr(
            vepyr, "_create_annotator", lambda *a, **k: calls.append(a) or None
        )
        with pytest.raises(ValueError, match=r"workers>1 requires a tabix-indexed input"):
            vepyr.annotate(self.UNINDEXED_VCF, CACHE_DIR, workers=2)
        assert calls == [], "the engine must not be called"

    def test_workers_above_one_requires_index_on_vcf_path(self, monkeypatch, tmp_path):
        import vepyr

        calls = []
        monkeypatch.setattr(
            vepyr, "_annotate_vcf", lambda *a, **k: calls.append(a) or 0
        )
        with pytest.raises(ValueError, match=r"input\.vcf\.tbi"):
            vepyr.annotate(
                self.UNINDEXED_VCF,
                CACHE_DIR,
                output_vcf=str(tmp_path / "out.vcf"),
                show_progress=False,
                workers=2,
            )
        assert calls == []

    def test_workers_one_does_not_need_index(self, monkeypatch):
        import pyarrow as pa
        import vepyr

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        monkeypatch.setattr(vepyr, "_create_annotator", lambda *a, **k: FakeAnnotator())
        lf = vepyr.annotate(self.UNINDEXED_VCF, CACHE_DIR, workers=1)
        assert isinstance(lf, pl.LazyFrame)

    def test_workers_above_one_accepts_csi_index(self, monkeypatch, tmp_path):
        import pyarrow as pa
        import vepyr

        vcf = tmp_path / "in.vcf.gz"
        vcf.write_bytes(b"")
        (tmp_path / "in.vcf.gz.csi").write_bytes(b"")

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        monkeypatch.setattr(vepyr, "_create_annotator", lambda *a, **k: FakeAnnotator())
        lf = vepyr.annotate(str(vcf), CACHE_DIR, workers=2)
        assert isinstance(lf, pl.LazyFrame)
```

The VCF-writer binding is `_annotate_vcf` (`from vepyr._core import annotate_vcf as _annotate_vcf`, line 15); `test_workers_forward_to_vcf_writer` monkeypatches the same attribute.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_annotate.py -k "requires_index or does_not_need_index or csi_index" -v 2>&1 | tail -15`
Expected: the two `requires_index` tests fail (no `ValueError`; the fake is called or the engine is reached); the other two pass.

- [ ] **Step 3: Implement the check**

Add above `annotate()` (near the other `_`-prefixed helpers):

```python
def _require_index_for_workers(vcf: str, workers: int) -> None:
    """``workers>1`` reads each run's position window by index seek on both
    output paths; without an index every run would parse the whole file."""
    if workers <= 1:
        return
    if os.path.exists(vcf + ".tbi") or os.path.exists(vcf + ".csi"):
        return
    raise ValueError(
        f"workers>1 requires a tabix-indexed input ({vcf}.tbi or .csi); "
        "compress with bgzip and index with tabix, or use workers=1"
    )
```

Call it right after the `workers` type check in `annotate()`:

```python
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer")
    _require_index_for_workers(vcf, workers)
```

(`vcf` is the first parameter of `annotate()`.) Keep the engine's own sink-side check; this one only runs earlier. `tests/test_comparison_annotate.py` passes `workers=4` with a fake `vepyr` module, so it is unaffected.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_annotate.py -v 2>&1 | tail -15`
Expected: all pass. `test_workers_forward_to_streaming_annotator` and `test_workers_forward_to_vcf_writer` use `INPUT_VCF` (`input.vcf.gz`, indexed) and keep passing.

- [ ] **Step 5: Commit**

```bash
uv run ruff check . && uv run ruff format .
git add src/vepyr/__init__.py tests/test_annotate.py
git commit -m "feat(annotate): require an indexed input for workers>1 on both paths"
```

---

### Task 2: Pin the engine revision

**Files:**
- Modify: `Cargo.toml` (line 79, `datafusion-bio-function-vep = { git = ..., rev = "<sha>" ... }` and the comment block above it), `Cargo.lock`

**Interfaces:**
- Consumes: the merge SHA of the engine PR from `docs/superpowers/plans/2026-09-06-lazyframe-workers-engine.md` Task 7. Until it merges, pin the PR head to let this branch's tests run, and re-pin after merge (as PR #81 did for #239).

- [ ] **Step 1: Bump the rev**

```bash
SHA=<merge sha of the engine PR>
sed -i '' "s|datafusion-bio-function-vep = { git = \"https://github.com/biodatageeks/datafusion-bio-functions.git\", rev = \"[0-9a-f]*\"|datafusion-bio-function-vep = { git = \"https://github.com/biodatageeks/datafusion-bio-functions.git\", rev = \"$SHA\"|" Cargo.toml
grep -n "bio-function-vep" Cargo.toml
cargo update -p datafusion-bio-function-vep 2>&1 | tail -2
git diff --stat Cargo.lock
```

Add a line to the comment block above the pin: `# bio-functions <sha7>: streaming run pool, workers>1 on the LazyFrame path (#<PR>)`. Never commit a local `[patch]`.

- [ ] **Step 2: Rebuild and run the whole suite**

```bash
CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr 2>&1 | tail -2
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest -q 2>&1 | tail -5
```

Expected: the full suite passes (golden suites included).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: bump bio-functions to the streaming run pool"
```

---

### Task 3: Golden-fixture parity at `workers>1`

**Files:**
- Create: `tests/test_lazyframe_workers.py`

**Interfaces:**
- Consumes: `tests.cache_metadata.copy_cache_with_source_metadata`, the golden fixtures (`tests/data/golden/input.vcf.gz` + `.tbi`, `tests/data/golden/cache`, `tests/data/golden_merged/cache`, `tests/data/golden/reference.fa`), engine env knob `VEP_STREAM_RUN_BUFFERS`.

- [ ] **Step 1: Write the tests**

```python
"""workers>1 on the LazyFrame path must equal workers=1 row for row, in order.

``buffer_size=7`` turns the 100-variant fixture into ~15 input buffers and
``VEP_STREAM_RUN_BUFFERS=1`` makes every buffer its own run on the Ensembl
cache, so the ordered release crosses a seam at every buffer. The merged cache
keeps its four-buffer floor (stateful warm-up), so it is also run with the
default run length.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


@pytest.fixture(scope="module")
def ensembl_cache_dir(tmp_path_factory):
    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("ensembl_cache_with_metadata")
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl", "115"))


@pytest.fixture(scope="module")
def merged_cache_dir(tmp_path_factory):
    src = MERGED_GOLDEN_DIR / "cache"
    if not src.is_dir():
        pytest.skip("Merged golden test cache not available")
    target = tmp_path_factory.mktemp("merged_cache_with_metadata")
    return str(copy_cache_with_source_metadata(str(src), target, "merged", "115"))


@pytest.fixture(params=["ensembl", "merged"])
def cache_dir(request, ensembl_cache_dir, merged_cache_dir):
    return ensembl_cache_dir if request.param == "ensembl" else merged_cache_dir


def _lazy(cache_dir, workers, **kwargs):
    import vepyr

    return vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        everything=True,
        reference_fasta=REFERENCE_FASTA,
        buffer_size=7,
        workers=workers,
        **kwargs,
    )


def _assert_same(actual: pl.DataFrame, expected: pl.DataFrame) -> None:
    assert actual.columns == expected.columns
    assert actual.height == expected.height, (actual.height, expected.height)
    assert actual.equals(expected), "frames differ (row order included)"


@pytest.mark.parametrize("workers", [2, 3])
def test_collect_equals_serial(cache_dir, workers, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    assert serial.height == 100
    _assert_same(_lazy(cache_dir, workers).collect(), serial)


def test_collect_equals_serial_with_default_run_length(merged_cache_dir, monkeypatch):
    monkeypatch.delenv("VEP_STREAM_RUN_BUFFERS", raising=False)
    serial = _lazy(merged_cache_dir, 1).collect()
    _assert_same(_lazy(merged_cache_dir, 4).collect(), serial)


def test_csq_column_equals_serial(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1, skip_csq=False).collect()
    parallel = _lazy(cache_dir, 3, skip_csq=False).collect()
    assert "CSQ" in parallel.columns
    _assert_same(parallel, serial)


def test_pushed_down_predicate_composes_with_workers(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    starts = serial["start"].to_list()
    predicates = [
        (pl.col("chrom") == "chr1") & pl.col("start").is_between(starts[10], starts[40]),
        (pl.col("chrom") == "chr1") & (pl.col("start") >= starts[73]),
        ((pl.col("chrom") == "chr1") & pl.col("start").is_between(starts[5], starts[12]))
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(starts[60], starts[71])),
    ]
    for predicate in predicates:
        pushed = _lazy(cache_dir, 3).filter(predicate).collect()
        _assert_same(pushed, serial.filter(predicate))


def test_head_matches_serial_prefix(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    for n in (1, 7, 23):
        _assert_same(_lazy(cache_dir, 3).head(n).collect(), serial.head(n))


def test_select_composes_with_workers(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    columns = ["chrom", "start", "SYMBOL", "most_severe_consequence"]
    serial = _lazy(cache_dir, 1).select(columns).collect()
    _assert_same(_lazy(cache_dir, 2).select(columns).collect(), serial)


def test_lazyframe_is_rerunnable_at_workers_above_one(ensembl_cache_dir):
    lf = _lazy(ensembl_cache_dir, 2)
    first = lf.collect()
    second = lf.collect()
    _assert_same(second, first)
```

- [ ] **Step 2: Run the tests**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_lazyframe_workers.py -v 2>&1 | tail -25`
Expected: all pass on the pinned engine. If a merged case fails, diff `SYMBOL`/`HGNC_ID` first (a seam without warm-up); if an Ensembl case fails on row order, rerun with `VEP_PIPELINE_TRACE=1` and check that `run_pool release` events come out in ascending `run` order.

- [ ] **Step 3: Commit**

```bash
uv run ruff check . && uv run ruff format .
git add tests/test_lazyframe_workers.py
git commit -m "test: LazyFrame workers>1 parity on the golden fixtures"
```

---

### Task 4: Documentation and the bridge comment

**Files:**
- Modify: `src/vepyr/__init__.py` (docstring lines 1231-1235), `src/annotate.rs` (comment at 283-288), `docs/quickstart.md` (189-201), `docs/performance.md` (tuning table row 108; a new subsection), `docs/dataframes.md` (any "serial" claim near 234/282/515)

- [ ] **Step 1: Docstring**

Replace the `workers` entry with:

```
    workers : int
        Number of within-contig annotation pipelines (default: 1) on both
        output paths. Values greater than 1 require a tabix-indexed (bgzip +
        ``.tbi`` or ``.csi``) input VCF. Results are identical to ``workers=1``
        row for row and in the same order; on the ``LazyFrame`` path the
        engine holds at most a few runs of output in memory ahead of the
        consumer.
```

- [ ] **Step 2: Bridge comment**

In `src/annotate.rs` replace the comment above `config.workers = workers;`:

```rust
    // Single annotation-concurrency knob (vepyr `workers` -> engine `workers`).
    // The sink's DataFusion `target_partitions` stays 1 so the annotated VCF is
    // written as a single ordered output; the streaming path runs the engine's
    // run pool per contig and releases runs in order to one consumer.
```

- [ ] **Step 3: `docs/quickstart.md`**

Replace the paragraph at 189-192 with:

```
`workers` controls how many within-contig annotation pipelines run
concurrently, on both the LazyFrame and the `output_vcf` path. It requires a
tabix-indexed (bgzip + `.tbi` or `.csi`) input VCF. Output is identical to
`workers=1`, row order included.
```

- [ ] **Step 4: `docs/performance.md`**

Tuning-table row:

```
| `workers` | `1` | Within-contig annotation pipelines on both output paths; values greater than 1 require a tabix-indexed (bgzip + `.tbi`/`.csi`) input VCF. Output is identical to `workers=1`, row order included. |
```

Add a subsection after "Region filters", filled from Task 5's sweep:

```
### Workers on the LazyFrame path

Measured with `e2e-testing/scripts/lazyframe_workers_parity.py --release 116 --sweep 1 2 4 8`
on HG002 contig slices, `everything=True`, a FASTA, on an Apple Silicon M3 Max
(16 cores, 64 GiB). Every frame equalled the `workers=1` frame.

| Input | workers | Ensembl | Merged | Peak RSS (Ensembl) |
|---|---|---|---|---|
| chr22, 50,861 variants | 1 | <s> | <s> | <GB> |
| | 2 | <s> | <s> | <GB> |
| | 4 | <s> | <s> | <GB> |
| | 8 | <s> | <s> | <GB> |
| chr1, 323,430 variants | 1 | <s> | <s> | <GB> |
| | 2 | <s> | <s> | <GB> |
| | 4 | <s> | <s> | <GB> |
| | 8 | <s> | <s> | <GB> |

Each contig is cut into grid-aligned runs; Merged and RefSeq runs replay a
bounded warm-up at every seam, so they gain a little less than Ensembl. The
per-contig prepare stays serial, which bounds the speedup on small contigs.
```

(The `<s>`/`<GB>` cells are filled in Task 5, Step 4; do not commit the table with placeholders.)

- [ ] **Step 5: `docs/dataframes.md`**

`grep -n "serial\|workers" docs/dataframes.md`; the measurement notes that say `workers=1` are statements of how a number was taken and stay. Remove any sentence that says the LazyFrame path does not support `workers>1`; if none, no change.

- [ ] **Step 6: Build the docs if the toolchain is present, then commit**

```bash
uv run mkdocs build --strict 2>&1 | tail -3 || echo "mkdocs not installed; skipped"
cargo fmt
git add src/vepyr/__init__.py src/annotate.rs docs/quickstart.md docs/dataframes.md
git commit -m "docs: workers applies to the LazyFrame path"
```

`docs/performance.md` is committed in Task 5 once the table has numbers.

---

### Task 5: Real-data parity gate and `workers` sweep

**Files:**
- Create: `e2e-testing/scripts/lazyframe_workers_parity.py`
- Modify: `docs/performance.md` (fill the table from Task 4)

**Interfaces:**
- Consumes: `comparison.vcfio.slice_contig(vcf_gz, chrom, out_dir, force=False) -> path`, `comparison.vcfio.canonical_contig(chrom)`, `comparison.profiles.default_input(name)`, `comparison.profiles.cache_dir_for(profile, release)`, `comparison.profiles.PROFILES`, `comparison.profiles.RELEASES`.
- Produces: `e2e-testing/results/lazyframe_workers/<release>/report.json` and `report.md`; exit status 1 on any mismatch.

- [ ] **Step 1: Write the runner**

```python
#!/usr/bin/env python3
"""LazyFrame workers>1 parity gate and sweep on real data.

For a contig slice of the HG002 benchmark VCF and each cache profile, the
reference is `annotate(slice, workers=1).collect()`; every candidate
`annotate(slice, workers=N).collect()` must equal it, row order included.
With the CSQ column on, the LazyFrame CSQ strings are also compared to the
INFO/CSQ field of an `output_vcf` run at the same worker count, row by row.
Wall time and peak RSS per worker count are written to the report.

Examples:
    lazyframe_workers_parity.py --release 116
    lazyframe_workers_parity.py --release 116 --chrom 1 --sweep 1 2 4 8 --profiles ensembl merged
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import polars as pl  # noqa: E402

from comparison import profiles, vcfio  # noqa: E402

INPUT_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
DEFAULT_PROFILES = tuple(
    name for name, profile in profiles.PROFILES.items() if profile.flavour == name
)


def peak_rss_gb():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return rss / 1e9 if sys.platform == "darwin" else rss / 1e6


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t


def profile_annotate_kwargs(profile, release):
    spec = profiles.PROFILES[profile]
    kwargs = dict(spec.annotate_kwargs)
    if spec.plugins:
        kwargs["plugin_cache_root"] = profiles.plugin_cache_dir_for(release)
        kwargs["plugins"] = list(spec.plugins)
    return kwargs


def vcf_csq_by_row(path):
    """INFO/CSQ per data line, in file order ('' when absent)."""
    out = []
    with vcfio.open_text(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            info = line.rstrip("\n").split("\t")[7]
            csq = ""
            for item in info.split(";"):
                if item.startswith("CSQ="):
                    csq = item[len("CSQ=") :]
                    break
            out.append(csq)
    return out


def run_profile(vepyr, profile, release, slice_gz, fasta, sweep, out_dir):
    cache_dir = profiles.cache_dir_for(profile, release)
    kwargs = profile_annotate_kwargs(profile, release)

    def lazy(workers, **extra):
        return vepyr.annotate(
            slice_gz,
            cache_dir,
            everything=True,
            reference_fasta=fasta,
            workers=workers,
            **kwargs,
            **extra,
        )

    rows = []
    ok = True
    reference, ref_s = timed(lambda: lazy(1).collect())
    rows.append(
        {
            "profile": profile,
            "workers": 1,
            "rows": reference.height,
            "wall_s": round(ref_s, 2),
            "peak_rss_gb": round(peak_rss_gb(), 2),
            "equal": True,
        }
    )
    for workers in sorted(set(sweep) - {1}):
        frame, wall = timed(lambda w=workers: lazy(w).collect())
        equal = frame.equals(reference)
        ok &= equal
        rows.append(
            {
                "profile": profile,
                "workers": workers,
                "rows": frame.height,
                "wall_s": round(wall, 2),
                "peak_rss_gb": round(peak_rss_gb(), 2),
                "equal": equal,
            }
        )
    # CSQ string parity against the VCF sink at the largest worker count.
    workers = max(sweep)
    with_csq = lazy(workers, skip_csq=False).collect()
    vcf_out = os.path.join(out_dir, f"{profile}_w{workers}.vcf.gz")
    vepyr.annotate(
        slice_gz,
        cache_dir,
        everything=True,
        reference_fasta=fasta,
        workers=workers,
        output_vcf=vcf_out,
        show_progress=False,
        **kwargs,
    )
    vcf_csq = vcf_csq_by_row(vcf_out)
    lf_csq = [c or "" for c in with_csq["CSQ"].to_list()]
    csq_equal = vcf_csq == lf_csq
    ok &= csq_equal
    rows.append(
        {
            "profile": profile,
            "workers": workers,
            "rows": with_csq.height,
            "wall_s": None,
            "peak_rss_gb": None,
            "equal": csq_equal,
            "check": "CSQ vs output_vcf",
        }
    )
    return ok, rows


def write_report(out_dir, rows):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "report.json"), "w") as f:
        json.dump(rows, f, indent=2)
    lines = [
        "| profile | workers | rows | wall s | peak RSS GB | equal | check |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['profile']} | {r['workers']} | {r['rows']} | {r['wall_s']} | "
            f"{r['peak_rss_gb']} | {r['equal']} | {r.get('check', 'frame vs workers=1')} |"
        )
    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--release", required=True, choices=profiles.RELEASES)
    p.add_argument(
        "--profiles",
        nargs="+",
        default=list(DEFAULT_PROFILES),
        choices=sorted(profiles.PROFILES),
    )
    p.add_argument("--chrom", default="22")
    p.add_argument("--sweep", nargs="+", type=int, default=[1, 4])
    p.add_argument(
        "--input",
        default=None,
        help=f"Indexed benchmark VCF (default: $DATA/input/{INPUT_NAME})",
    )
    p.add_argument("--fasta", default=None)
    p.add_argument("--out-dir", default=None)
    args = p.parse_args(argv)

    import vepyr

    vcf = args.input or profiles.default_input(INPUT_NAME)
    fasta = args.fasta or profiles.default_input(FASTA_NAME)
    for path in (vcf, fasta):
        if not os.path.exists(path):
            raise SystemExit(f"missing input: {path}")
    if shutil.which("bgzip") is None or shutil.which("tabix") is None:
        raise SystemExit("bgzip and tabix are required")
    out_dir = args.out_dir or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "results",
        "lazyframe_workers",
        args.release,
    )
    slice_dir = os.path.join(out_dir, "input")
    contig = vcfio.canonical_contig(args.chrom)
    slice_gz = vcfio.slice_contig(vcf, contig, slice_dir)
    if 1 not in args.sweep:
        args.sweep.append(1)

    all_ok = True
    rows = []
    for profile in args.profiles:
        ok, profile_rows = run_profile(
            vepyr, profile, args.release, slice_gz, fasta, args.sweep, out_dir
        )
        all_ok &= ok
        rows.extend(profile_rows)
    write_report(out_dir, rows)
    print("PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

Peak RSS is process-wide and monotonic, so the per-row value is the peak up to that point; report the `workers=1` row from a separate invocation (`--sweep 1`) when a per-count figure matters. `vcfio.open_text` handles bgzf.

- [ ] **Step 2: Gate on chr22, both cache flavours**

```bash
uptime   # load average must be near idle
cd e2e-testing/scripts
CONDA_PREFIX= VIRTUAL_ENV= uv run python lazyframe_workers_parity.py --release 116 --chrom 22 --sweep 1 2 4 8 --profiles ensembl merged refseq 2>&1 | tail -20
```

Expected: every `equal` is `True` and the run prints `PASS`. The CSQ check compares the LazyFrame CSQ strings at `workers=8` to the VCF sink at `workers=8`, which is the spec's LazyFrame-versus-VCF gate.

- [ ] **Step 3: Gate on chr1**

```bash
CONDA_PREFIX= VIRTUAL_ENV= uv run python lazyframe_workers_parity.py --release 116 --chrom 1 --sweep 1 2 4 8 --profiles ensembl merged 2>&1 | tail -20
```

Expected: `PASS`. Run it twice and keep the second (the first run of a session reads slow). If `workers=8` is not faster than `workers=4`, rerun with `VEP_PROFILE=1` and read `head_wait` and `run_pool_runs` on the per-contig `pipeline_profile` line before touching any default; see the spec's measurement plan for the sweep over `VEP_STREAM_RUN_BUFFERS` and `VEP_STREAM_LOOKAHEAD_RUNS`.

- [ ] **Step 4: Fill the table and commit**

Copy the wall and RSS numbers from `e2e-testing/results/lazyframe_workers/116/report.md` (chr22 and chr1 runs) into the `docs/performance.md` table from Task 4; no placeholder cell may remain.

```bash
cd ~/research/git/vepyr
uv run ruff check . && uv run ruff format .
git add e2e-testing/scripts/lazyframe_workers_parity.py docs/performance.md
git commit -m "test(e2e): LazyFrame workers parity gate and sweep; document the numbers"
```

`e2e-testing/results/` is not committed (check `.gitignore`; if it is untracked, leave it).

---

### Task 6: Pull request

**Files:**
- Add: `docs/superpowers/plans/2026-09-06-lazyframe-workers-engine.md`, `docs/superpowers/plans/2026-09-06-lazyframe-workers-vepyr.md` (commit them on this branch; the spec is already committed)

- [ ] **Step 1: Commit the plans, run everything once more**

```bash
git add docs/superpowers/plans/2026-09-06-lazyframe-workers-engine.md docs/superpowers/plans/2026-09-06-lazyframe-workers-vepyr.md
git commit -m "docs: LazyFrame workers implementation plans"
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest -q 2>&1 | tail -3
uv run ruff check . && cargo clippy 2>&1 | tail -1
```

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/lazyframe-parallel-workers
gh pr create --title "feat(annotate): workers>1 on the LazyFrame path" --body-file - <<'EOF'
## Summary
- `annotate(..., workers=N).collect()` now runs N annotation pipelines per contig (engine streaming run pool, biodatageeks/datafusion-bio-functions#<PR>); output equals `workers=1` row for row and in order, on Ensembl, Merged and RefSeq
- composes with pushed-down region predicates, `head()`/LIMIT and column selection
- `workers>1` requires a tabix/CSI index on both output paths, checked before the engine is called
- engine pin bumped to <sha>; docs no longer call the LazyFrame path serial; timing table added

Design: `docs/superpowers/specs/2026-09-06-lazyframe-parallel-workers-design.md`

## Test plan
- [x] `tests/test_annotate.py` index checks on both paths
- [x] `tests/test_lazyframe_workers.py` golden-fixture parity (Ensembl + merged, `buffer_size=7`, one-buffer runs): plain, CSQ on, predicate, `head()`, `select()`, re-run
- [x] `e2e-testing/scripts/lazyframe_workers_parity.py --release 116` on HG002 chr22 and chr1 at workers 1/2/4/8, CSQ vs `output_vcf` at workers=8:

<paste the report.md tables>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

Then read the PR body back with `gh api repos/{owner}/{repo}/pulls/<n> --jq .body | head` (`gh pr edit` has silently failed on this repo before). If the engine PR was pinned at its head rather than its merge SHA, re-pin after the engine merge (Task 2), rerun Task 3 and the chr22 gate, and push again before merging.

---

## Self-review notes

- Spec coverage: index check on both paths (T1), pin (T2), fixture parity incl. `skip_csq` both ways, predicate, `head()`, both caches, default and one-buffer run lengths (T3), docstring and the three docs pages plus the bridge comment (T4), md5-equivalent LazyFrame-versus-VCF CSQ gate on chr22 and chr1 at `workers>1` with a 1/2/4/8 sweep and RSS (T5), delivery (T6). The measurement sweep over the engine knobs is in the spec and referenced from T5 Step 3; it changes engine defaults, not vepyr code.
- Names used across tasks: `_require_index_for_workers`, fixtures `ensembl_cache_dir`/`merged_cache_dir`/`cache_dir`, `_lazy`, `_assert_same`, runner `run_profile`/`write_report`/`vcf_csq_by_row`/`peak_rss_gb`, `--sweep`.
- Order dependency: T1 needs no engine change; T3, T5 need the pinned engine (T2); T4's table is filled by T5.
