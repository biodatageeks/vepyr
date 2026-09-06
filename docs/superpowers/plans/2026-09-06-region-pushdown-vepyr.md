# Region Pushdown: vepyr Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `vepyr.annotate(...).filter(<chrom/start/end predicate>)` push the genomic restriction into the engine as `regions`, warn when the input has no index, and prove exactness on fixtures and on a real chr22 slice.

**Architecture:** A new pure module `src/vepyr/_regions.py` turns the Polars predicate the IO plugin receives into a region list (chrom conjuncts are *evaluated* against the header contigs, range conjuncts are *interpreted* from the serialized tree, everything else fails open). `_batch_source` in `src/vepyr/__init__.py` adds that list to the engine options next to the existing flag inference. One new `_core` function returns the VCF header contigs. Tests cover extraction, wiring, the warning, fixture parity and a real-data gate.

**Tech Stack:** Python 3.10+, Polars 1.39 (`Expr.meta.serialize(format="json")`, `Expr.deserialize(..., format="json")`), PyO3 0.28 + maturin, `datafusion-bio-format-vcf` (`VcfTableProvider`), pytest.

**Spec:** `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md`. Engine prerequisite: `docs/superpowers/plans/2026-09-06-region-pushdown-engine.md` merged upstream.

## Global Constraints

- Contract: `annotate(...).filter(p).collect()` equals `annotate(...).collect().filter(p)` including row order, for every predicate. Extraction may only ever narrow the input to a superset of the accepted rows; unrecognised shapes contribute nothing.
- Coordinates are the frame's `start`/`end` columns, 1-based closed. No conversion.
- `regions` in the options is a list of `{"chrom": str, "start": int | None, "end": int | None}`; `[]` from extraction means "provably empty" and never reaches the engine.
- Build with `CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr` (both env vars are set in this shell and make maturin refuse; the flags match the user's canonical build).
- Run tests with `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest <path> -v`. `uv run ruff check .` and `uv run ruff format .` must be clean before each commit; `cargo clippy` and `cargo fmt` for the Rust bridge.
- Work on branch `feat/region-predicate-pushdown` created from `master`. Commit messages follow the repo's conventional style (`feat(annotate): ...`, `test: ...`, `docs: ...`) and end with the session trailer from the harness instructions.
- `CLAUDE.md` asks for a GSD entry point before edits; `.planning/ROADMAP.md` is absent, so `/gsd:quick` fails. Confirm with the user once that this plan runs outside GSD before the first edit.

---

## File structure

| File | Responsibility |
|---|---|
| `src/vepyr/_regions.py` (new) | Pure extraction: `GENOMIC_COLUMNS`, `extract_regions(predicate, contigs)`. No engine access. |
| `src/lib.rs` | `vcf_contigs(path) -> list[str]` PyO3 function. |
| `src/vepyr/_core.pyi` | Stub for `vcf_contigs`. |
| `src/vepyr/__init__.py` | Import `_vcf_contigs`; in `annotate()` memoise contigs, warn once when unindexed, add `regions` to `engine_opts` in `_batch_source`; docstring. |
| `tests/test_regions.py` (new) | Extraction table. |
| `tests/test_region_pushdown.py` (new) | Wiring with a fake annotator, warning behaviour, `vcf_contigs`, fixture parity on Ensembl and merged caches. |
| `docs/quickstart.md`, `docs/performance.md` | "Region filters" section, tuning table note, `workers` correction. |
| `e2e-testing/scripts/region_pushdown_parity.py` (new) | Real-data gate on a chr22 slice of HG002. |
| `Cargo.toml`, `Cargo.lock` | Engine pin bump. |

Reference points in `src/vepyr/__init__.py` at `be76af2`:

- imports from `vepyr._core` at lines 14-20; `import warnings` at line 7.
- `annotate()` starts at 1077; the `workers` docstring entry at ~1233; the probe annotator at ~1652 (`probe = _create_annotator(`), closure captures at ~1700 (`_vcf, _cache_dir, _opts, _engine_skip = (`), `_batch_source` at ~1710, `engine_opts = _flags_for_projection(...)` at ~1733, `annotator = _create_annotator(` at ~1745.

---

### Task 1: `_regions.py` extraction module

**Files:**
- Create: `src/vepyr/_regions.py`
- Test: `tests/test_regions.py`

**Interfaces:**
- Produces:
  ```python
  GENOMIC_COLUMNS: frozenset[str]  # {"chrom", "start", "end"}
  def extract_regions(predicate: pl.Expr, contigs: list[str]) -> list[dict] | None
  ```
  Returns `None` (no pushdown), `[]` (provably empty) or `[{"chrom", "start", "end"}, ...]` with `None` for open sides, chroms in `contigs` order within each group.

- [ ] **Step 1: Write the failing tests**

`tests/test_regions.py`:

```python
"""Extraction of genomic regions from a Polars predicate (pure, no engine)."""

from __future__ import annotations

import polars as pl
import pytest

from vepyr._regions import GENOMIC_COLUMNS, extract_regions

CONTIGS = ["chr1", "chr2", "chr3", "chrX", "chrM"]


def region(chrom, start=None, end=None):
    return {"chrom": chrom, "start": start, "end": end}


def test_genomic_columns():
    assert GENOMIC_COLUMNS == frozenset({"chrom", "start", "end"})


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (pl.col("chrom") == "chr2", [region("chr2")]),
        (pl.col("chrom") != "chr2", [region(c) for c in CONTIGS if c != "chr2"]),
        (pl.col("chrom").is_in(["chr3", "chr1"]), [region("chr1"), region("chr3")]),
        (pl.col("chrom").str.starts_with("chrX"), [region("chrX")]),
        (~(pl.col("chrom") == "chr1"), [region(c) for c in CONTIGS if c != "chr1"]),
        ((pl.col("chrom") == "chr1") | (pl.col("chrom") == "chr3"), [region("chr1"), region("chr3")]),
        ((pl.col("chrom") == "chr1") & (pl.col("chrom") == "chr3"), []),
        (pl.col("chrom") == "1", []),
    ],
)
def test_chrom_conjuncts_are_evaluated_against_contigs(predicate, expected):
    assert extract_regions(predicate, CONTIGS) == expected


@pytest.mark.parametrize(
    "predicate, start, end",
    [
        (pl.col("start") >= 100, 100, None),
        (pl.col("start") > 100, 101, None),
        (pl.col("start") <= 200, None, 200),
        (pl.col("start") < 200, None, 199),
        (pl.col("start") == 150, 150, 150),
        (pl.lit(100) <= pl.col("start"), 100, None),
        (pl.lit(200) > pl.col("start"), None, 199),
        (pl.col("start").is_between(100, 200), 100, 200),
        (pl.col("start").is_between(100, 200, closed="left"), 100, 199),
        (pl.col("start").is_between(100, 200, closed="right"), 101, 200),
        (pl.col("start").is_between(100, 200, closed="none"), 101, 199),
        (pl.col("end") <= 200, None, 200),
        (pl.col("end") < 200, None, 199),
        (pl.col("end") == 200, None, 200),
        (pl.col("end") >= 100, None, None),
        (pl.col("end").is_between(100, 200), None, 200),
        (pl.col("start") >= pl.lit(100, dtype=pl.Int64), 100, None),
        (pl.col("start") >= pl.lit(100, dtype=pl.UInt32), 100, None),
    ],
)
def test_range_conjuncts_bound_start(predicate, start, end):
    got = extract_regions((pl.col("chrom") == "chr1") & predicate, CONTIGS)
    assert got == [region("chr1", start, end)]


def test_range_without_chrom_applies_to_every_contig():
    got = extract_regions(pl.col("start").is_between(5, 9), CONTIGS)
    assert got == [region(c, 5, 9) for c in CONTIGS]


def test_conjuncts_intersect():
    p = (pl.col("chrom") == "chr1") & (pl.col("start") >= 100) & (pl.col("start") >= 150) & (pl.col("end") <= 900) & (pl.col("start") <= 500)
    assert extract_regions(p, CONTIGS) == [region("chr1", 150, 500)]


def test_unsatisfiable_range_is_empty():
    p = (pl.col("chrom") == "chr1") & (pl.col("start") >= 500) & (pl.col("start") <= 100)
    assert extract_regions(p, CONTIGS) == []


def test_non_genomic_conjuncts_are_residual():
    p = (pl.col("chrom") == "chr1") & (pl.col("AF") > 0.5) & (pl.col("SYMBOL").is_not_null())
    assert extract_regions(p, CONTIGS) == [region("chr1")]


def test_top_level_or_yields_one_group_per_disjunct():
    p = ((pl.col("chrom") == "chr1") & pl.col("start").is_between(10, 20)) | (
        (pl.col("chrom") == "chr1") & pl.col("start").is_between(100, 200)
    ) | (pl.col("chrom") == "chr3")
    assert extract_regions(p, CONTIGS) == [
        region("chr1", 10, 20),
        region("chr1", 100, 200),
        region("chr3"),
    ]


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("start") > 5.5,
        (pl.col("chrom") == "chr1") | (pl.col("start") > 5),
        (pl.col("chrom") == "chr1") & (pl.col("start") > pl.col("end")),
        (pl.col("chrom") == "chr1") & ((pl.col("start") > 5) | (pl.col("start") < 3)),
        (pl.col("chrom") == "chr1") | (pl.col("AF") > 0.5),
        pl.col("chrom").rank() == 1,
        (pl.col("chrom") == "chr1") & (pl.col("start").cast(pl.Int64) > 5),
        pl.col("start") != 5,
    ],
)
def test_unrecognised_shapes_fail_open(predicate):
    assert extract_regions(predicate, CONTIGS) is None


def test_empty_contig_list_means_nothing_matches():
    assert extract_regions(pl.col("chrom") == "chr1", []) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_regions.py -q 2>&1 | tail -3`
Expected: `ModuleNotFoundError: No module named 'vepyr._regions'`.

- [ ] **Step 3: Implement `src/vepyr/_regions.py`**

```python
"""Turn a Polars predicate into genomic regions for engine pushdown.

The IO plugin behind ``annotate()`` receives the predicate Polars pushed down.
This module extracts the restriction it places on ``chrom``/``start``/``end``
so the engine can skip contigs and seek by position *before* annotating.

Contract: the result is always a superset of the rows the predicate accepts;
Polars still evaluates the full predicate afterwards. Anything this module
does not recognise makes it fail open (``None``, no pushdown).

Walks ``Expr.meta.serialize(format="json")``. That format is documented as
unstable, so every shape read here is pinned by ``tests/test_regions.py``
and any structural surprise is caught and reported as ``None``.
"""

from __future__ import annotations

import io
import json
from typing import Any

import polars as pl

GENOMIC_COLUMNS = frozenset({"chrom", "start", "end"})
_RANGE_COLUMNS = frozenset({"start", "end"})
_INT_SCALAR_KEYS = frozenset(
    {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
)
_FLIP = {"Eq": "Eq", "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq"}
_CHROM_FUNCTION_FAMILIES = ("Boolean", "StringExpr")


class _Unrecognised(Exception):
    """A genomic conjunct this module cannot bound safely."""


def extract_regions(predicate: pl.Expr, contigs: list[str]) -> list[dict] | None:
    """Regions the predicate restricts the input to, or ``None`` for no pushdown.

    ``[]`` means the predicate can accept no row at all.
    """
    try:
        tree = json.loads(predicate.meta.serialize(format="json"))
        regions: list[dict] = []
        for group in _split(tree, "Or"):
            chroms, lo, hi = _analyse_group(group, contigs)
            if lo is not None and hi is not None and lo > hi:
                continue
            regions.extend({"chrom": c, "start": lo, "end": hi} for c in chroms)
        return regions
    except (_Unrecognised, KeyError, TypeError, ValueError, IndexError):
        return None
    except Exception:  # polars raised while evaluating a chrom conjunct
        return None


def _split(node: dict, op: str) -> list[dict]:
    """Flatten a left-deep chain of ``op`` (``And``/``Or``) into its operands."""
    binary = node.get("BinaryExpr") if isinstance(node, dict) else None
    if binary is not None and binary.get("op") == op:
        return _split(binary["left"], op) + _split(binary["right"], op)
    return [node]


def _analyse_group(
    group: dict, contigs: list[str]
) -> tuple[list[str], int | None, int | None]:
    """One conjunction: (chroms in contig order, lower bound, upper bound).

    Raises ``_Unrecognised`` when the group has no recognised genomic conjunct
    or holds one it cannot bound, because an ``Or`` over such a group could
    accept any row.
    """
    chrom_set: set[str] | None = None
    lo: int | None = None
    hi: int | None = None
    recognised = False
    for conjunct in _split(group, "And"):
        names = _column_names(conjunct)
        if names == {"chrom"}:
            _gate_chrom_shape(conjunct)
            frame = pl.DataFrame({"chrom": contigs}, schema={"chrom": pl.String})
            matched = set(frame.filter(_deserialize(conjunct))["chrom"].to_list())
            chrom_set = matched if chrom_set is None else chrom_set & matched
            recognised = True
        elif names and names <= _RANGE_COLUMNS:
            c_lo, c_hi = _range_bounds(conjunct)
            lo = c_lo if lo is None else (c_lo if c_lo is not None and c_lo > lo else lo)
            hi = c_hi if hi is None else (c_hi if c_hi is not None and c_hi < hi else hi)
            recognised = True
        elif names & GENOMIC_COLUMNS:
            raise _Unrecognised(f"mixed conjunct {names}")
        # any other conjunct is a residual Polars applies after annotation
    if not recognised:
        raise _Unrecognised("no genomic conjunct in group")
    if chrom_set is None:
        return list(contigs), lo, hi
    return [c for c in contigs if c in chrom_set], lo, hi


def _column_names(node: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "Column" and isinstance(value, str):
                names.add(value)
            else:
                names |= _column_names(value)
    elif isinstance(node, list):
        for item in node:
            names |= _column_names(item)
    return names


def _gate_chrom_shape(node: Any) -> None:
    """Allow only elementwise boolean shapes so evaluation on a contig table
    is faithful: columns, literals, binary operators, and ``Boolean``/
    ``StringExpr`` functions. Aggregations, windows, casts and the rest raise.
    """
    if not isinstance(node, dict) or len(node) != 1:
        raise _Unrecognised("node shape")
    (kind, payload), = node.items()
    if kind == "Column":
        if payload != "chrom":
            raise _Unrecognised("column")
    elif kind == "Literal":
        return
    elif kind == "BinaryExpr":
        _gate_chrom_shape(payload["left"])
        _gate_chrom_shape(payload["right"])
    elif kind == "Function":
        function = payload["function"]
        family = next(iter(function)) if isinstance(function, dict) else function
        if family not in _CHROM_FUNCTION_FAMILIES:
            raise _Unrecognised(f"function {family}")
        for item in payload["input"]:
            _gate_chrom_shape(item)
    else:
        raise _Unrecognised(kind)


def _deserialize(node: dict) -> pl.Expr:
    return pl.Expr.deserialize(io.BytesIO(json.dumps(node).encode()), format="json")


def _literal_int(node: Any) -> int:
    literal = node["Literal"]
    if "Dyn" in literal and "Int" in literal["Dyn"]:
        return int(literal["Dyn"]["Int"])
    scalar = literal.get("Scalar")
    if isinstance(scalar, dict) and len(scalar) == 1:
        (dtype, value), = scalar.items()
        if dtype in _INT_SCALAR_KEYS:
            return int(value)
    raise _Unrecognised("literal")


def _range_bounds(node: dict) -> tuple[int | None, int | None]:
    """Bounds on ``start`` implied by one ``start``/``end`` conjunct."""
    if "BinaryExpr" in node:
        binary = node["BinaryExpr"]
        op = binary["op"]
        left, right = binary["left"], binary["right"]
        if "Column" in left and "Literal" in right:
            column, value = left["Column"], _literal_int(right)
        elif "Literal" in left and "Column" in right:
            column, value = right["Column"], _literal_int(left)
            op = _FLIP.get(op, op)
        else:
            raise _Unrecognised("binary operands")
        if op not in _FLIP:
            raise _Unrecognised(f"operator {op}")
        if column == "start":
            return {
                "Eq": (value, value),
                "Gt": (value + 1, None),
                "GtEq": (value, None),
                "Lt": (None, value - 1),
                "LtEq": (None, value),
            }[op]
        # column == "end": only an upper bound carries over to start
        return {
            "Eq": (None, value),
            "Lt": (None, value - 1),
            "LtEq": (None, value),
            "Gt": (None, None),
            "GtEq": (None, None),
        }[op]
    if "Function" in node:
        function = node["Function"]
        between = function["function"].get("Boolean", {}).get("IsBetween")
        inputs = function["input"]
        if between is None or len(inputs) != 3 or "Column" not in inputs[0]:
            raise _Unrecognised("function")
        low, high = _literal_int(inputs[1]), _literal_int(inputs[2])
        closed = between["closed"]
        if closed in ("Left", "None"):
            high -= 1
        if closed in ("Right", "None"):
            low += 1
        if inputs[0]["Column"] == "start":
            return low, high
        return None, high
    raise _Unrecognised("range node")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_regions.py -q 2>&1 | tail -3`
Expected: all pass. If `pl.col("start") != 5` does not return `None`, check that `NotEq` is absent from `_FLIP` (it must be).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git switch -c feat/region-predicate-pushdown master
git add src/vepyr/_regions.py tests/test_regions.py
git commit -m "feat(annotate): extract genomic regions from a pushed Polars predicate"
```

---

### Task 2: `vcf_contigs` bridge function

**Files:**
- Modify: `src/lib.rs` (new `#[pyfunction]` + registration in `_core`)
- Modify: `src/vepyr/_core.pyi`
- Test: `tests/test_region_pushdown.py` (new file, first test)

**Interfaces:**
- Produces: `vepyr._core.vcf_contigs(vcf_path: str) -> list[str]` — contig ids from the VCF header in header order, `[]` when the header declares none.

- [ ] **Step 1: Write the failing test**

`tests/test_region_pushdown.py`:

```python
"""Region predicate pushdown: wiring, warning, and fixture parity."""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
PLAIN_INPUT_VCF = str(GOLDEN_DIR / "input.vcf")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


def test_vcf_contigs_reads_the_header_for_plain_and_bgzip_inputs():
    from vepyr._core import vcf_contigs

    gz = vcf_contigs(INPUT_VCF)
    assert "chr1" in gz
    assert vcf_contigs(PLAIN_INPUT_VCF) == gz


def test_vcf_contigs_missing_file_raises():
    from vepyr._core import vcf_contigs

    with pytest.raises(RuntimeError, match="Failed to open VCF"):
        vcf_contigs("/nonexistent/input.vcf")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown.py -q 2>&1 | tail -3`
Expected: `ImportError: cannot import name 'vcf_contigs'`.

- [ ] **Step 3: Implement**

In `src/lib.rs`, next to `create_annotator`:

```rust
/// Contig ids declared in the VCF header, in header order.
#[pyfunction]
fn vcf_contigs(vcf_path: &str) -> PyResult<Vec<String>> {
    use datafusion::datasource::TableProvider;

    let provider = datafusion_bio_format_vcf::table_provider::VcfTableProvider::new(
        vcf_path.to_string(),
        Some(vec![]),
        Some(vec![]),
        None,
        false,
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open VCF: {e}")))?;
    let schema = provider.schema();
    let Some(raw) = schema.metadata().get("bio.vcf.contigs") else {
        return Ok(Vec::new());
    };
    let entries: Vec<serde_json::Value> = serde_json::from_str(raw).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Invalid VCF contig metadata: {e}"))
    })?;
    Ok(entries
        .iter()
        .filter_map(|entry| entry.get("id").and_then(|id| id.as_str()).map(str::to_string))
        .collect())
}
```

Register it in `_core`:

```rust
    m.add_function(wrap_pyfunction!(vcf_contigs, m)?)?;
```

In `src/vepyr/_core.pyi`, after `create_annotator`'s stub (or at the end):

```python
def vcf_contigs(vcf_path: str) -> list[str]:
    """Contig ids declared in the VCF header, in header order."""
    ...
```

Rebuild: `CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr 2>&1 | tail -2`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown.py -q 2>&1 | tail -3`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy 2>&1 | tail -2
git add src/lib.rs src/vepyr/_core.pyi tests/test_region_pushdown.py
git commit -m "feat(core): expose VCF header contigs"
```

---

### Task 3: Wire extraction, the warning and the `regions` option into `annotate()`

**Files:**
- Modify: `src/vepyr/__init__.py`
- Test: `tests/test_region_pushdown.py`

**Interfaces:**
- Consumes: `extract_regions`, `GENOMIC_COLUMNS` (Task 1), `_core.vcf_contigs` (Task 2).
- Produces: `engine_opts["regions"]` in `_batch_source`; a `RuntimeWarning` containing `"tabix/CSI index"` at most once per `annotate()` call; `vepyr._vcf_contigs` as the monkeypatchable alias.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_region_pushdown.py`)

```python
class _FakeAnnotator:
    schema = pa.schema(
        [
            pa.field("chrom", pa.string()),
            pa.field("start", pa.uint32()),
            pa.field("end", pa.uint32()),
            pa.field("most_severe_consequence", pa.string()),
        ]
    )

    def __iter__(self):
        return iter(())


@pytest.fixture
def fake_engine(monkeypatch):
    """Capture the options every annotator creation receives."""
    import vepyr

    seen: list[dict] = []

    def fake_create(vcf_path, cache_dir, options_json, skip_csq=True, limit=None):
        seen.append(json.loads(options_json))
        return _FakeAnnotator()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create)
    monkeypatch.setattr(vepyr, "_vcf_contigs", lambda path: ["chr1", "chr2"])
    return seen


def _collect_opts(seen):
    # seen[0] is the schema probe at annotate() time; the rest are collects.
    return seen[1:]


def test_genomic_predicate_becomes_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter((pl.col("chrom") == "chr1") & (pl.col("start") >= 100)).collect()
    (opts,) = _collect_opts(fake_engine)
    assert opts["regions"] == [{"chrom": "chr1", "start": 100, "end": None}]


def test_non_genomic_predicate_sends_no_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter(pl.col("most_severe_consequence") == "missense_variant").collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_unrecognised_genomic_predicate_sends_no_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter((pl.col("chrom") == "chr1") | (pl.col("start") > 5)).collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_empty_regions_short_circuit_without_an_annotator(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    df = lf.filter(pl.col("chrom") == "chr9").collect()
    assert df.height == 0
    assert _collect_opts(fake_engine) == []


def test_no_predicate_sends_no_regions_and_reads_no_contigs(monkeypatch, fake_engine):
    import vepyr

    def boom(path):
        raise AssertionError("contigs must not be read without a genomic predicate")

    monkeypatch.setattr(vepyr, "_vcf_contigs", boom)
    vepyr.annotate(INPUT_VCF, CACHE_DIR).collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_unindexed_input_warns_once_per_annotate_call(fake_engine):
    import vepyr

    lf = vepyr.annotate(PLAIN_INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        lf.filter(pl.col("chrom") == "chr1").collect()
        lf.filter(pl.col("chrom") == "chr1").collect()
    hits = [w for w in caught if "tabix/CSI index" in str(w.message)]
    assert len(hits) == 1
    assert issubclass(hits[0].category, RuntimeWarning)


def test_indexed_input_does_not_warn(fake_engine):
    import vepyr

    assert os.path.exists(INPUT_VCF + ".tbi")
    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf.filter(pl.col("chrom") == "chr1").collect()


def test_non_genomic_predicate_never_warns_even_without_index(fake_engine):
    import vepyr

    lf = vepyr.annotate(PLAIN_INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf.filter(pl.col("most_severe_consequence") == "x").collect()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown.py -q 2>&1 | tail -5`
Expected: `AttributeError: module 'vepyr' has no attribute '_vcf_contigs'` from the fixture.

- [ ] **Step 3: Implement**

1. Imports at the top of `src/vepyr/__init__.py`: add `import os` to the stdlib block, and

```python
from vepyr._core import vcf_contigs as _vcf_contigs
from vepyr._regions import GENOMIC_COLUMNS, extract_regions
```

2. In `annotate()`, right after the closure captures (`_vcf, _cache_dir, _opts, _engine_skip = (...)`) and before `def _batch_source`, add:

```python
    # Region pushdown state, per annotate() call: header contigs are read
    # lazily on the first collect that carries a genomic predicate, and the
    # missing-index warning is raised at most once.
    _region_state: dict = {"contigs": None, "warned": False}

    def _header_contigs() -> list[str]:
        if _region_state["contigs"] is None:
            _region_state["contigs"] = list(_vcf_contigs(_vcf))
        return _region_state["contigs"]

    def _warn_if_unindexed() -> None:
        if _region_state["warned"]:
            return
        _region_state["warned"] = True
        if os.path.exists(_vcf + ".tbi") or os.path.exists(_vcf + ".csi"):
            return
        warnings.warn(
            f"region filter on {_vcf!r} without a tabix/CSI index ({_vcf}.tbi or "
            ".csi): the whole file is parsed and filtered before annotation. "
            "Compress with bgzip and index with tabix for seek-based reads.",
            RuntimeWarning,
            stacklevel=2,
        )
```

3. In `_batch_source`, immediately after `engine_opts = _flags_for_projection(_opts, needed, set(polars_schema), required)`:

```python
        # Predicate pushdown on genomic coordinates: chrom/start/end conjuncts
        # become engine `regions`, so unselected contigs are never prepared and
        # indexed inputs are read by seek. Polars still applies the full
        # predicate on every batch below, so this can only narrow the input.
        if predicate is not None and GENOMIC_COLUMNS & set(predicate.meta.root_names()):
            regions = extract_regions(predicate, _header_contigs())
            if regions == []:
                return
            if regions is not None:
                _warn_if_unindexed()
                engine_opts["regions"] = regions
```

`return` inside the generator ends the source: Polars sees an empty frame with the declared schema.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown.py tests/test_annotate.py -q 2>&1 | tail -5`
Expected: all pass. If `test_genomic_predicate_becomes_regions` sees no `regions`, print `predicate` inside `_batch_source`: Polars may have pushed the predicate as `None` when the frame is not "pushable"; the fake schema must include `chrom`, `start`, `end` with the declared types, which the fixture above does.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add src/vepyr/__init__.py tests/test_region_pushdown.py
git commit -m "feat(annotate): push genomic range predicates down as engine regions"
```

---

### Task 4: Pin the engine revision

**Files:**
- Modify: `Cargo.toml` (line 78, `datafusion-bio-function-vep = { git = ..., rev = "<sha>" }` and the comment above it), `Cargo.lock`

**Interfaces:**
- Consumes: the merge SHA of the engine PR from `docs/superpowers/plans/2026-09-06-region-pushdown-engine.md` Task 9.

- [ ] **Step 1: Bump the rev**

```bash
SHA=<merge sha of the engine PR>
sed -i '' "s|datafusion-bio-function-vep = { git = \"https://github.com/biodatageeks/datafusion-bio-functions.git\", rev = \"[0-9a-f]*\"|datafusion-bio-function-vep = { git = \"https://github.com/biodatageeks/datafusion-bio-functions.git\", rev = \"$SHA\"|" Cargo.toml
grep -n "bio-function-vep" Cargo.toml
cargo update -p datafusion-bio-function-vep 2>&1 | tail -2
git diff --stat Cargo.lock
```

Update the comment line above the pin to name the feature (`# bio-functions <sha7>: regions option (#<PR>)`). Never use a local `[patch]` in a committed state.

- [ ] **Step 2: Rebuild and run the whole suite**

```bash
CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr 2>&1 | tail -2
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest -q 2>&1 | tail -5
```

Expected: the full suite passes (golden suites included).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: bump bio-functions to the regions option"
```

---

### Task 5: Fixture parity tests on Ensembl and merged caches

**Files:**
- Test: `tests/test_region_pushdown.py`

**Interfaces:**
- Consumes: `tests.cache_metadata.copy_cache_with_source_metadata(src, target, source_type, release)`; the `buffer_size` argument of `annotate()`.

- [ ] **Step 1: Write the failing tests** (append)

```python
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


def _lazy(cache_dir):
    import vepyr

    # buffer_size=7 turns the 100-variant fixture into ~15 input buffers so
    # ranges start and end mid-buffer and cross several seams.
    return vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        everything=True,
        reference_fasta=REFERENCE_FASTA,
        buffer_size=7,
    )


def _predicates(full: pl.DataFrame) -> dict[str, pl.Expr]:
    s = full["start"].to_list()
    return {
        "chrom": pl.col("chrom") == "chr1",
        "chrom_is_in": pl.col("chrom").is_in(["chr1", "chr9"]),
        "range_mid_buffer": (pl.col("chrom") == "chr1") & pl.col("start").is_between(s[10], s[40]),
        "range_open_end": (pl.col("chrom") == "chr1") & (pl.col("start") >= s[73]),
        "end_upper": pl.col("end") <= s[23],
        "single_position": (pl.col("chrom") == "chr1") & (pl.col("start") == s[33]),
        "two_ranges_same_contig": ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[5], s[12]))
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[60], s[71])),
        "adjacent_ranges": ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[20], s[27]))
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[28], s[35])),
        "with_residual": (pl.col("chrom") == "chr1")
        & (pl.col("start") >= s[15])
        & (pl.col("most_severe_consequence") != "intron_variant"),
        "unknown_contig": pl.col("chrom") == "chr2",
        "unsatisfiable": (pl.col("chrom") == "chr1") & (pl.col("start") > s[-1]),
    }


def _assert_parity(cache_dir):
    lf = _lazy(cache_dir)
    full = lf.collect()
    assert full.height == 100
    for name, predicate in _predicates(full).items():
        pushed = lf.filter(predicate).collect()
        reference = full.filter(predicate)
        assert pushed.equals(reference), f"{name}: pushed {pushed.height} rows, reference {reference.height}"
    # a non-trivial case must actually select a strict subset
    assert 0 < lf.filter(_predicates(full)["range_mid_buffer"]).collect().height < 100


def test_pushdown_parity_ensembl(ensembl_cache_dir):
    _assert_parity(ensembl_cache_dir)


def test_pushdown_parity_merged(merged_cache_dir):
    _assert_parity(merged_cache_dir)


def test_pushdown_parity_with_limit(ensembl_cache_dir):
    lf = _lazy(ensembl_cache_dir)
    full = lf.collect()
    s = full["start"].to_list()
    predicate = (pl.col("chrom") == "chr1") & (pl.col("start") >= s[30])
    assert lf.filter(predicate).head(5).collect().equals(full.filter(predicate).head(5))


def test_pushdown_parity_with_projection(merged_cache_dir):
    lf = _lazy(merged_cache_dir)
    full = lf.collect()
    s = full["start"].to_list()
    predicate = (pl.col("chrom") == "chr1") & pl.col("start").is_between(s[10], s[40])
    cols = ["chrom", "start", "ref", "alt", "SYMBOL", "HGNC_ID", "most_severe_consequence"]
    assert lf.filter(predicate).select(cols).collect().equals(full.filter(predicate).select(cols))
```

- [ ] **Step 2: Run the tests**

Run: `CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown.py -q -k parity 2>&1 | tail -8`
Expected with the pinned engine: all pass. A merged mismatch that only touches `SYMBOL`/`HGNC_ID` on rows just after a range start means the engine's warm-up did not reach the donor buffer: rerun that case with `VEP_PIPELINE_TRACE=1` and read the `regions runs` and `grid_plans` lines; do not paper over it in Python.

- [ ] **Step 3: Commit**

```bash
uv run ruff check . && uv run ruff format .
git add tests/test_region_pushdown.py
git commit -m "test: region pushdown parity on the Ensembl and merged golden fixtures"
```

---

### Task 6: Documentation

**Files:**
- Modify: `docs/quickstart.md` (after the `workers` paragraph, ~line 200), `docs/performance.md` (tuning table, ~line 80), `src/vepyr/__init__.py` (`annotate()` docstring: `workers` entry ~1233 and a new note before `Returns`)

- [ ] **Step 1: Add the "Region filters" section to `docs/quickstart.md`**

````markdown
### Region filters

Filtering the LazyFrame on `chrom`, `start` or `end` is pushed into the
engine before annotation: contigs outside the filter are never prepared, and
an indexed input (bgzip + `.tbi`/`.csi`) is read by seek.

```python
lf = vepyr.annotate("input.vcf.gz", cache_dir, everything=True, reference_fasta="GRCh38.fa")
df = lf.filter(
    (pl.col("chrom") == "chr22") & pl.col("start").is_between(20_000_000, 25_000_000)
).collect()
```

The result is always identical to filtering after `collect()`; only the work
changes. Coordinates are the frame's own `start`/`end` columns (1-based,
closed). Recognised shapes:

- `chrom` conjuncts: `==`, `!=`, `is_in`, `str.starts_with` and boolean
  combinations of them.
- `start`/`end` conjuncts: comparisons with an integer literal and
  `is_between`. `end <= b` bounds the range; `end >= a` does not.
- Several regions: write them as an `|` of `(chrom & range)` groups.

Anything else (a float literal, `chrom` and `start` mixed inside one `|`,
casts) is not pushed down and is applied by Polars after annotation, which is
still correct, just not faster.

Without a tabix/CSI index next to the input a `RuntimeWarning` is emitted:
the whole file is parsed and filtered before annotation, and only the
selected rows are annotated. On Merged and RefSeq caches a range costs one
extra positional pass over each selected contig, which keeps the result
byte-identical to a whole-file run.
````

- [ ] **Step 2: Correct `workers` in `docs/quickstart.md`, `docs/performance.md` and the docstring**

Replace the `workers` sentence in `docs/quickstart.md` and the table row in `docs/performance.md` with: "`workers` controls how many within-contig annotation pipelines run concurrently when writing with `output_vcf`; it requires a tabix-indexed (bgzip + `.tbi`) input. The LazyFrame path is serial (`workers=1`) in this release." Update the `workers` docstring entry in `annotate()` the same way, and add after the `on_batch_written` entry:

```
    Notes
    -----
    Filtering the returned ``LazyFrame`` on ``chrom``, ``start`` or ``end``
    restricts the *input* before annotation (region pushdown): unselected
    contigs are skipped and indexed inputs are read by seek. Results are
    identical to filtering after ``collect()``. A ``RuntimeWarning`` is
    raised when the input has no ``.tbi``/``.csi`` index. See the quickstart
    "Region filters" section for the recognised predicate shapes.
```

- [ ] **Step 3: Build the docs if mkdocs is installed, then commit**

```bash
CONDA_PREFIX= VIRTUAL_ENV= uv run --extra docs mkdocs build -q 2>&1 | tail -2 || true
git add docs/quickstart.md docs/performance.md src/vepyr/__init__.py
git commit -m "docs: region filters on the LazyFrame path and the workers scope"
```

---

### Task 7: Real-data parity gate on a chr22 slice

**Files:**
- Create: `e2e-testing/scripts/region_pushdown_parity.py`

**Interfaces:**
- Consumes: `comparison.vcfio.slice_contig(vcf_gz, chrom, out_dir, force=False) -> path`, `comparison.profiles.default_input(name)`, `comparison.profiles.cache_dir_for(profile, release)`, `comparison.profiles.PROFILES`.
- Produces: `e2e-testing/results/region_pushdown/<release>/report.json` and `report.md`; exit status 1 on any mismatch.

- [ ] **Step 1: Write the runner**

```python
#!/usr/bin/env python3
"""Region pushdown parity gate on real data.

For a contig slice of the HG002 benchmark VCF and each cache profile, the
reference is `annotate(slice).collect()` filtered in Polars; the candidate is
`annotate(slice).filter(p).collect()` with the predicate pushed down. Frames
must be identical (row order included). One region is repeated on an
unindexed copy to check the warning and equality on the sequential path.

Examples:
    region_pushdown_parity.py --release 116
    region_pushdown_parity.py --release 116 --profiles ensembl merged --chrom 22
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import polars as pl  # noqa: E402

from comparison import profiles, vcfio  # noqa: E402

INPUT_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
DEFAULT_REGIONS = [
    "chr22:20000000-25000000",
    "chr22:30000000-30100000",
    "chr22:17000000-17500000,chr22:40000000-40200000",
    "chr22:45000000-",
]


def parse_region_list(text):
    """'chr22:a-b,chr22:c-' -> Polars predicate as an OR of (chrom & range)."""
    predicate = None
    for item in text.split(","):
        chrom, _, span = item.partition(":")
        clause = pl.col("chrom") == chrom
        if span:
            lo, _, hi = span.partition("-")
            if lo:
                clause = clause & (pl.col("start") >= int(lo))
            if hi:
                clause = clause & (pl.col("start") <= int(hi))
        predicate = clause if predicate is None else (predicate | clause)
    return predicate


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t


def plain_copy(slice_gz, out_dir):
    plain = os.path.join(out_dir, os.path.basename(slice_gz)[: -len(".gz")])
    if not os.path.exists(plain):
        with open(plain, "wb") as f:
            subprocess.run(["bgzip", "-dc", slice_gz], stdout=f, check=True)
    for idx in (".tbi", ".csi"):
        if os.path.exists(plain + idx):
            os.remove(plain + idx)
    return plain


def run_profile(vepyr, profile, release, slice_gz, plain_vcf, fasta, region_texts):
    cache_dir = profiles.cache_dir_for(profile, release)
    lf = vepyr.annotate(slice_gz, cache_dir, everything=True, reference_fasta=fasta)
    full, full_s = timed(lf.collect)
    rows = []
    ok = True
    for text in region_texts:
        predicate = parse_region_list(text)
        reference = full.filter(predicate)
        pushed, pushed_s = timed(lambda: lf.filter(predicate).collect())
        equal = pushed.equals(reference)
        ok &= equal
        rows.append(
            {
                "profile": profile,
                "region": text,
                "indexed": True,
                "rows": pushed.height,
                "reference_rows": reference.height,
                "full_s": round(full_s, 2),
                "pushed_s": round(pushed_s, 2),
                "equal": equal,
            }
        )
    # Unindexed leg: same result, plus the warning.
    predicate = parse_region_list(region_texts[0])
    reference = full.filter(predicate)
    lf_plain = vepyr.annotate(plain_vcf, cache_dir, everything=True, reference_fasta=fasta)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pushed, pushed_s = timed(lambda: lf_plain.filter(predicate).collect())
    warned = any("tabix/CSI index" in str(w.message) for w in caught)
    equal = pushed.equals(reference)
    ok &= equal and warned
    rows.append(
        {
            "profile": profile,
            "region": region_texts[0],
            "indexed": False,
            "rows": pushed.height,
            "reference_rows": reference.height,
            "full_s": round(full_s, 2),
            "pushed_s": round(pushed_s, 2),
            "equal": equal,
            "warned": warned,
        }
    )
    return ok, rows


def write_report(out_dir, rows):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "report.json"), "w") as f:
        json.dump(rows, f, indent=2)
    lines = [
        "| profile | region | indexed | rows | full s | pushed s | equal | warned |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['profile']} | {r['region']} | {r['indexed']} | {r['rows']} | "
            f"{r['full_s']} | {r['pushed_s']} | {r['equal']} | {r.get('warned', '')} |"
        )
    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--release", required=True, choices=profiles.RELEASES)
    p.add_argument("--profiles", nargs="+", default=sorted(profiles.PROFILES), choices=sorted(profiles.PROFILES))
    p.add_argument("--chrom", default="22")
    p.add_argument("--regions", nargs="+", default=DEFAULT_REGIONS, help="'chrom:lo-hi' items; comma-join several into one predicate")
    p.add_argument("--input", default=None, help="Indexed benchmark VCF (default: $DATA/input/%s)" % INPUT_NAME)
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
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results", "region_pushdown", args.release
    )
    slice_dir = os.path.join(out_dir, "input")
    slice_gz = vcfio.slice_contig(vcf, vcfio.canonical_contig(args.chrom), slice_dir)
    plain_vcf = plain_copy(slice_gz, slice_dir)

    all_ok = True
    rows = []
    for profile in args.profiles:
        ok, profile_rows = run_profile(vepyr, profile, args.release, slice_gz, plain_vcf, fasta, args.regions)
        all_ok &= ok
        rows.extend(profile_rows)
    write_report(out_dir, rows)
    print("PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

The region strings use `chr22`; `parse_region_list` compares against the slice's own spelling, so pass `--regions 22:...` for a bare-numbered file. `slice_contig` writes `input_22.vcf.gz` next to its `.tbi`.

- [ ] **Step 2: Run the gate**

```bash
uptime   # the host is shared; note the load average with the numbers
CONDA_PREFIX= VIRTUAL_ENV= uv run python e2e-testing/scripts/region_pushdown_parity.py --release 116 2>&1 | tail -25
```

Expected: every row `equal: True`, the unindexed rows `warned: True`, and `PASS`. `pushed_s` should be well below `full_s` for the indexed rows. Copy the printed table into the PR description. The first run of a session reads slow on this host; the parity verdict is what matters, so do not rerun for timing unless the numbers go into a benchmark.

- [ ] **Step 3: Commit**

```bash
uv run ruff check e2e-testing/scripts/region_pushdown_parity.py && uv run ruff format e2e-testing/scripts/region_pushdown_parity.py
git add e2e-testing/scripts/region_pushdown_parity.py
git commit -m "test(e2e): region pushdown parity gate on a chr22 slice"
```

---

### Task 8: Pull request

**Files:**
- Add: `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md`, `docs/superpowers/plans/2026-09-06-region-pushdown-engine.md`, `docs/superpowers/plans/2026-09-06-region-pushdown-vepyr.md` (currently untracked; commit them on this branch)

- [ ] **Step 1: Commit the design documents and run the whole suite once more**

```bash
git add docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md docs/superpowers/plans/2026-09-06-region-pushdown-engine.md docs/superpowers/plans/2026-09-06-region-pushdown-vepyr.md
git commit -m "docs: region predicate pushdown design and plans"
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest -q 2>&1 | tail -3
uv run ruff check . && cargo clippy 2>&1 | tail -1
```

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/region-predicate-pushdown
gh pr create --title "feat(annotate): push genomic range predicates down to the engine" --body-file - <<'EOF'
## Summary
- `annotate(...).filter(<chrom/start/end predicate>)` now restricts the engine input (`regions`): unselected contigs are skipped, indexed inputs are read by seek, Merged/RefSeq ranges stay byte-identical through grid-aligned warm-up in the engine
- chrom conjuncts are evaluated against the header contigs (`==`, `!=`, `is_in`, `str.starts_with`, boolean combinations); `start`/`end` comparisons and `is_between` are interpreted; anything else fails open
- `RuntimeWarning` once per `annotate()` call when the input has no `.tbi`/`.csi`
- new `_core.vcf_contigs()`; engine pin bumped to <sha>
- docs: "Region filters" section; `workers` scoped to the VCF-output path (the LazyFrame path is serial on this engine)

Design: `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md`

## Test plan
- [x] `tests/test_regions.py` extraction table
- [x] `tests/test_region_pushdown.py` wiring, warning, fixture parity (Ensembl + merged, `buffer_size=7`)
- [x] `e2e-testing/scripts/region_pushdown_parity.py --release 116` on the HG002 chr22 slice:

<paste the report.md table>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

Then read the PR body back with `gh api repos/{owner}/{repo}/pulls/<n> --jq .body | head` (`gh pr edit` has silently failed on this repo before).

---

## Self-review notes

- Spec coverage: extraction rules (T1), header contigs (T2), warning (T3), wiring (T3), pin bump (T4), fixture parity incl. limit and projection (T5), docs incl. the `workers` correction (T6), real-data gate incl. the unindexed leg (T7), delivery (T8). The deferred `regions=` argument is intentionally absent.
- Names used across tasks: `GENOMIC_COLUMNS`, `extract_regions`, `_vcf_contigs`, `_header_contigs`, `_warn_if_unindexed`, `_region_state`, `engine_opts["regions"]`, fixtures `ensembl_cache_dir`/`merged_cache_dir`, `_lazy`, `_predicates`, `_assert_parity`, `parse_region_list`, `run_profile`, `write_report`.
- Order dependency: Task 5 and Task 7 need the pinned engine (Task 4); Tasks 1-3 run against the fake annotator and do not.
