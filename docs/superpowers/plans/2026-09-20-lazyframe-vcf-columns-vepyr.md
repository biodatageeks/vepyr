# LazyFrame Carries INFO/FORMAT + `sink_vcf` Metadata (vepyr) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `vepyr.annotate()` returns a LazyFrame that carries the input's INFO and
FORMAT columns by default and the polars-bio metadata `pb.sink_vcf` needs, so
`annotate → filter → pb.sink_vcf` writes a VCF equivalent to `filter_vep`'s.

**Architecture:** The engine already carries every input column through
`annotate_vep()`; vepyr stops opening the VCF with empty field lists. Field selection
travels to Rust inside the options JSON (the repo's "opaque options payload" pattern),
which keeps every existing test fake working. Each `collect()` builds a fresh annotator,
so it passes only the carried fields the query reads. Frame metadata is built by
polars-bio's own extractor behind an optional extra.

**Tech Stack:** Rust 2021 + PyO3 0.28, DataFusion 53, Python 3.10+, Polars ≥ 1.37.1,
pytest, optional `polars-bio`.

**Spec:** `docs/superpowers/specs/2026-09-20-lazyframe-vcf-columns-and-sink-vcf-design.md`

## Global Constraints

- **Prerequisite, not part of this plan:** the collision fix
  (`docs/superpowers/plans/2026-09-20-input-info-annotation-name-collision.md`) is merged
  and pinned. It gives (a) `INFO_<id>` / `fmt_<id>` renames for colliding input columns,
  annotation columns keeping bare names, and (b) the input's schema-level VCF metadata on
  the annotator's schema. Task 1 asserts both; stop there if either is missing.
- `info_fields` / `format_fields`: `None` = **all**, a list selects, `[]` = none — the
  meaning `pb.scan_vcf` gives them. `output_vcf=` ignores both.
- `skip_csq` stays `True` by default. `sink_vcf` examples show `skip_csq=False`.
- Version **0.8.0**; `info_fields=[]`, `format_fields=[]` is the documented way back to
  the 0.7 frame.
- `polars-bio` is optional (`vepyr[polars-bio]`). Absent → no metadata, no error.
- Follow the runbook's GSD bypass already in use for this work; lint only through
  pre-commit (`uv run pre-commit run ruff --all-files`), build with
  `env -u CONDA_PREFIX uv run maturin develop`.
- Tests that need the golden cache use the `metadata_cache_dir` fixture
  (`tests/test_annotate.py:34`); wiring tests use the `fake_engine` pattern
  (`tests/test_region_pushdown.py:55`).

## File Structure

| File | Responsibility |
|---|---|
| `src/annotate.rs` | `vcf_header_fields()`; read `vcf_info_fields` / `vcf_format_fields` from the options JSON and open the VCF with them |
| `src/lib.rs`, `src/vepyr/_core.pyi` | `vcf_fields` binding + stub |
| `src/vepyr/_vcf_columns.py` (new) | pure functions: validate a selection, map carried columns to VCF ids, choose the fields a query reads |
| `src/vepyr/_vcf_metadata.py` (new) | build the polars-bio header dict and attach it; the only module that imports `polars_bio` |
| `src/vepyr/__init__.py` | `annotate()` arguments, options keys, schema, pruning call, metadata call |
| `tests/test_vcf_columns.py` (new) | everything above, wiring + fixture parity |

---

### Task 1: Native — list header fields, and open the VCF with a selection

**Files:**
- Modify: `src/annotate.rs` (new fn after `vcf_header_contigs` ~`:472`; `create_streaming_annotator` `:475-513`), `src/lib.rs:728-762`, `src/vepyr/_core.pyi`
- Test: `tests/test_vcf_columns.py` (new)

**Interfaces:**
- Produces: `vepyr._core.vcf_fields(vcf_path: str) -> tuple[list[str], list[str]]` — INFO ids, FORMAT ids, header order.
- Produces: options JSON keys `vcf_info_fields` / `vcf_format_fields` (`list[str]`). Key absent → all fields. Both are removed before the JSON reaches the engine.

- [ ] **Step 1: Branch**

```bash
git switch -c feat/lazyframe-vcf-columns
```

- [ ] **Step 2: Failing tests** — create `tests/test_vcf_columns.py`

```python
"""INFO/FORMAT columns on the LazyFrame, their pruning, and sink_vcf metadata."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

import vepyr

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")

GIAB_INFO = [
    "DPSum", "platforms", "platformnames", "platformbias", "datasets", "datasetnames",
    "datasetsmissingcall", "callsets", "callsetnames", "varType", "filt", "callable",
    "difficultregion", "arbitrated", "callsetwiththisuniqgenopassing",
    "callsetwithotheruniqgenopassing",
]
GIAB_FORMAT = ["DP", "GQ", "ADALL", "AD", "GT", "PS"]


def test_vcf_fields_lists_header_ids_in_header_order():
    from vepyr._core import vcf_fields

    info, fmt = vcf_fields(INPUT_VCF)
    assert info == GIAB_INFO
    assert fmt == GIAB_FORMAT


def test_vcf_fields_missing_file_raises():
    from vepyr._core import vcf_fields

    with pytest.raises(RuntimeError):
        vcf_fields("/nonexistent/input.vcf.gz")
```

- [ ] **Step 3: Run, expect failure**

Run: `uv run pytest tests/test_vcf_columns.py -v`
Expected: FAIL — `ImportError: cannot import name 'vcf_fields'`.

- [ ] **Step 4: `vcf_header_fields`** — add to `src/annotate.rs` after `vcf_header_contigs`

```rust
/// INFO and FORMAT ids declared in the VCF header, each in header order.
///
/// Read from the field metadata of a provider opened with every field, so a
/// FORMAT id the reader renamed to avoid an INFO id (`fmt_DP`) is reported by
/// its id, not its column name.
pub fn vcf_header_fields(vcf_path: &str) -> PyResult<(Vec<String>, Vec<String>)> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::datasource::TableProvider;

    let provider = datafusion_bio_format_vcf::table_provider::VcfTableProvider::new(
        vcf_path.to_string(),
        None,
        None,
        None,
        false,
    )
    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open VCF: {e}")))?;

    let mut info = Vec::new();
    let mut format = Vec::new();
    let schema = provider.schema();
    for field in schema.fields() {
        // Multi-sample inputs nest FORMAT fields under one `genotypes` struct.
        if field.name() == "genotypes" {
            if let DataType::Struct(children) = field.data_type() {
                format.extend(children.iter().map(|child| child.name().clone()));
            }
            continue;
        }
        match field.metadata().get("bio.vcf.field.field_type").map(String::as_str) {
            Some("INFO") => info.push(field.name().clone()),
            Some("FORMAT") => format.push(
                field
                    .metadata()
                    .get("bio.vcf.field.format_id")
                    .cloned()
                    .unwrap_or_else(|| field.name().clone()),
            ),
            _ => {}
        }
    }
    Ok((info, format))
}
```

- [ ] **Step 5: Binding + stub** — `src/lib.rs`, after `vcf_contigs`:

```rust
/// INFO and FORMAT ids declared in the VCF header, in header order.
#[pyfunction]
fn vcf_fields(vcf_path: &str) -> PyResult<(Vec<String>, Vec<String>)> {
    annotate::vcf_header_fields(vcf_path)
}
```

register it beside `vcf_contigs`: `m.add_function(wrap_pyfunction!(vcf_fields, m)?)?;`
and in `src/vepyr/_core.pyi`, after `vcf_contigs`:

```python
def vcf_fields(vcf_path: str) -> tuple[list[str], list[str]]:
    """INFO and FORMAT ids declared in the VCF header, in header order."""
    ...
```

- [ ] **Step 6: Field selection from the options JSON** — in `create_streaming_annotator`,
replace the block from `let (options_json, _cache_format) = normalize_options(options_json)?;`
through the `let opts: Value = ...?;` statement with

```rust
    let (options_json, _cache_format) = normalize_options(options_json)?;
    let mut opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    // Which input INFO/FORMAT fields the frame carries. vepyr-only keys: the
    // engine takes its input columns from the registered table, so they are
    // removed before the JSON reaches it. Absent means every field.
    let mut take_fields = |key: &str| -> PyResult<Option<Vec<String>>> {
        match opts.as_object_mut().and_then(|object| object.remove(key)) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => serde_json::from_value(value).map(Some).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("{key} must be a list of strings: {e}"))
            }),
        }
    };
    let info_fields = take_fields("vcf_info_fields")?;
    let format_fields = take_fields("vcf_format_fields")?;
    let options_json = serde_json::to_string(&opts).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
```

and the provider construction's two `Some(vec![]),` lines with `info_fields,` and
`format_fields,`. Leave `vcf_header_contigs` (`:397-403`) on `Some(vec![])`: it only
needs `chrom`.

- [ ] **Step 7: Add the schema tests** (append to `tests/test_vcf_columns.py`)

```python
@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    import os

    from tests.data.golden._cache_prep import copy_cache_with_source_metadata

    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("cache")
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl", "115"))


def _probe(cache_dir, **opts):
    from vepyr._core import create_annotator

    return create_annotator(INPUT_VCF, cache_dir, json.dumps(opts), True, 0).schema


def test_absent_keys_carry_every_field(cache_dir):
    names = _probe(cache_dir).names
    assert names[:8] == ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]
    assert names[8 : 8 + len(GIAB_INFO)] == GIAB_INFO
    assert names[8 + len(GIAB_INFO) : 8 + len(GIAB_INFO) + len(GIAB_FORMAT)] == GIAB_FORMAT


def test_empty_lists_carry_nothing(cache_dir):
    names = _probe(cache_dir, vcf_info_fields=[], vcf_format_fields=[]).names
    assert names[:9] == [
        "chrom", "start", "end", "id", "ref", "alt", "qual", "filter", "most_severe_consequence",
    ]


def test_engine_prerequisites_are_pinned(cache_dir):
    """The collision fix must be in the pinned engine; see Global Constraints."""
    schema = _probe(cache_dir)
    assert schema.metadata, "engine does not carry the input's schema-level VCF metadata"
    assert any(key.startswith(b"bio.vcf") for key in schema.metadata)
```

Match the import of `copy_cache_with_source_metadata` to how `tests/test_annotate.py`
imports it (top of that file) rather than the path guessed above.

- [ ] **Step 8: Build and run**

Run: `env -u CONDA_PREFIX uv run maturin develop && uv run pytest tests/test_vcf_columns.py -v`
Expected: PASS. If `test_engine_prerequisites_are_pinned` fails, **stop**: the collision
fix is not pinned.

- [ ] **Step 9: Rust tests, lint, commit**

```bash
cargo test --no-default-features --features mimalloc
cargo fmt && cargo clippy -- -D warnings
git add src/annotate.rs src/lib.rs src/vepyr/_core.pyi tests/test_vcf_columns.py
git commit -m "feat(native): list VCF header fields and open the input with a field selection"
```

---

### Task 2: `_vcf_columns.py` — validate, map columns to ids, pick what a query reads

**Files:**
- Create: `src/vepyr/_vcf_columns.py`
- Test: `tests/test_vcf_columns.py`

**Interfaces:**
- Produces:
  - `validate_selection(kind: str, requested: list[str] | None, available: list[str]) -> None` — `ValueError` naming unknown ids and listing the available ones.
  - `carried_columns(schema: pa.Schema) -> dict[str, tuple[str, str]]` — column name → `("INFO"|"FORMAT", vcf id)`; a multi-sample `genotypes` column maps to `("FORMAT", "*")`.
  - `fields_for_query(carried, needed: set[str] | None, info: list[str] | None, fmt: list[str] | None) -> tuple[list[str] | None, list[str] | None]` — what to send as `vcf_info_fields` / `vcf_format_fields`.

- [ ] **Step 1: Failing tests** (append)

```python
def _field(name, kind, fmt_id=None, dtype=pa.int32()):
    meta = {"bio.vcf.field.field_type": kind}
    if fmt_id:
        meta["bio.vcf.field.format_id"] = fmt_id
    return pa.field(name, dtype, metadata=meta)


CARRIED_SCHEMA = pa.schema(
    [
        pa.field("chrom", pa.string()),
        _field("DP", "INFO"),
        _field("INFO_AF", "INFO", dtype=pa.float32()),
        _field("GT", "FORMAT", "GT", pa.string()),
        _field("fmt_DP", "FORMAT", "DP"),
        pa.field("CSQ", pa.string()),
        pa.field("most_severe_consequence", pa.string()),
        pa.field("AF", pa.float32()),
    ]
)


def test_carried_columns_map_names_to_vcf_ids():
    from vepyr._vcf_columns import carried_columns

    assert carried_columns(CARRIED_SCHEMA) == {
        "DP": ("INFO", "DP"),
        "INFO_AF": ("INFO", "AF"),
        "GT": ("FORMAT", "GT"),
        "fmt_DP": ("FORMAT", "DP"),
    }


def test_a_query_reading_no_carried_column_reads_no_fields():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, {"chrom", "AF"}, None, None) == ([], [])


def test_a_query_reads_only_the_fields_it_names():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, {"INFO_AF", "fmt_DP"}, None, None) == (["AF"], ["DP"])


def test_no_projection_keeps_the_user_selection():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, None, ["DP"], None) == (["DP"], None)


def test_multi_sample_genotypes_keep_the_user_format_selection():
    from vepyr._vcf_columns import fields_for_query

    carried = {"genotypes": ("FORMAT", "*")}
    assert fields_for_query(carried, {"genotypes"}, None, ["GT"]) == ([], ["GT"])
    assert fields_for_query(carried, {"chrom"}, None, ["GT"]) == ([], [])


def test_unknown_id_is_a_value_error_listing_what_exists():
    from vepyr._vcf_columns import validate_selection

    with pytest.raises(ValueError, match=r"info_fields.*'NOPE'.*DP, AF"):
        validate_selection("info_fields", ["DP", "NOPE"], ["DP", "AF"])
    validate_selection("info_fields", None, ["DP"])
    validate_selection("info_fields", [], ["DP"])
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_vcf_columns.py -k "carried or query or unknown_id or projection or genotypes" -v`
Expected: FAIL — `ModuleNotFoundError: vepyr._vcf_columns`.

- [ ] **Step 3: Implement** — `src/vepyr/_vcf_columns.py`

```python
"""Input VCF columns on the annotation frame: selection, ids and pruning."""

from __future__ import annotations

import pyarrow as pa

_FIELD_TYPE = b"bio.vcf.field.field_type"
_FORMAT_ID = b"bio.vcf.field.format_id"
# The engine renames an input INFO column whose id matches an annotation column.
_INFO_PREFIX = "INFO_"


def validate_selection(
    kind: str, requested: list[str] | None, available: list[str]
) -> None:
    """Reject ids the header does not declare; the reader would panic on them."""
    if not requested:
        return
    unknown = [name for name in requested if name not in available]
    if unknown:
        raise ValueError(
            f"{kind} names fields the VCF header does not declare: "
            + ", ".join(repr(name) for name in unknown)
            + ". Available: "
            + ", ".join(available)
        )


def carried_columns(schema: pa.Schema) -> dict[str, tuple[str, str]]:
    """Map each carried input column to its kind and VCF id."""
    names = set(schema.names)
    carried: dict[str, tuple[str, str]] = {}
    for field in schema:
        if field.name == "genotypes":
            carried[field.name] = ("FORMAT", "*")
            continue
        metadata = field.metadata or {}
        kind = metadata.get(_FIELD_TYPE, b"").decode()
        if kind == "FORMAT":
            carried[field.name] = (kind, metadata.get(_FORMAT_ID, field.name.encode()).decode())
        elif kind == "INFO":
            vcf_id = field.name
            stripped = field.name.removeprefix(_INFO_PREFIX)
            # Renamed only when the bare id belongs to another column.
            if stripped != field.name and stripped in names:
                vcf_id = stripped
            carried[field.name] = (kind, vcf_id)
    return carried


def fields_for_query(
    carried: dict[str, tuple[str, str]],
    needed: set[str] | None,
    info: list[str] | None,
    fmt: list[str] | None,
) -> tuple[list[str] | None, list[str] | None]:
    """The INFO and FORMAT fields one collect has to read.

    Without a projection the user's selection stands. With one, only the carried
    columns the query names are read; nested genotypes cannot be split per field,
    so they are read as selected or not at all.
    """
    if needed is None:
        return info, fmt
    read_info = [vcf_id for name, (kind, vcf_id) in carried.items() if kind == "INFO" and name in needed]
    if "genotypes" in carried:
        read_fmt = fmt if "genotypes" in needed else []
    else:
        read_fmt = [vcf_id for name, (kind, vcf_id) in carried.items() if kind == "FORMAT" and name in needed]
    return read_info, read_fmt
```

- [ ] **Step 4: Run, lint, commit**

```bash
uv run pytest tests/test_vcf_columns.py -v
uv run pre-commit run ruff --all-files && uv run pre-commit run ruff-format --all-files
git add src/vepyr/_vcf_columns.py tests/test_vcf_columns.py
git commit -m "feat: map carried VCF columns to their ids and pick the fields a query reads"
```

---

### Task 3: `annotate(info_fields=, format_fields=)` and per-collect pruning

**Files:**
- Modify: `src/vepyr/__init__.py` — imports `:15-21`; signature `:1039-1098` and docstring; opts assembly before the probe `:1656`; `_batch_source` `:1742-1796`
- Test: `tests/test_vcf_columns.py`

**Interfaces:**
- Consumes: `_core.vcf_fields`, `_vcf_columns.*`, options keys from Task 1.
- Produces: `annotate(..., info_fields: list[str] | None = None, format_fields: list[str] | None = None)`.

- [ ] **Step 1: Failing wiring tests** (append; same fake shape as `tests/test_region_pushdown.py:40-67`)

```python
class _FakeAnnotator:
    schema = pa.schema(
        [
            pa.field("chrom", pa.string()),
            pa.field("start", pa.uint32()),
            pa.field("end", pa.uint32()),
            _field("DP", "INFO"),
            _field("GT", "FORMAT", "GT", pa.string()),
            pa.field("most_severe_consequence", pa.string()),
            pa.field("SYMBOL", pa.list_(pa.string())),
        ]
    )

    def __iter__(self):
        return iter(())


@pytest.fixture
def fake_engine(monkeypatch):
    seen: list[dict] = []

    def fake_create(vcf_path, cache_dir, options_json, skip_csq=True, limit=None):
        seen.append(json.loads(options_json))
        return _FakeAnnotator()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create)
    monkeypatch.setattr(vepyr, "_vcf_fields", lambda path: (["DP", "AF"], ["GT", "DP"]))
    monkeypatch.setattr(vepyr, "_vcf_contigs", lambda path: ["chr1"])
    return seen


def _annotate(**kw):
    return vepyr.annotate(INPUT_VCF, CACHE_DIR, show_progress=False, **kw)


def test_default_sends_no_selection_so_every_field_is_carried(fake_engine):
    _annotate().collect()
    assert all("vcf_info_fields" not in o and "vcf_format_fields" not in o for o in fake_engine)


def test_selection_reaches_the_probe_and_an_unprojected_collect(fake_engine):
    _annotate(info_fields=["DP"], format_fields=[]).collect()
    assert [(o["vcf_info_fields"], o["vcf_format_fields"]) for o in fake_engine] == [
        (["DP"], []),
        (["DP"], []),
    ]


def test_a_projection_without_input_columns_reads_none(fake_engine):
    _annotate().select("chrom", "SYMBOL").collect()
    collect = fake_engine[1]
    assert (collect["vcf_info_fields"], collect["vcf_format_fields"]) == ([], [])


def test_a_projection_reads_only_the_input_columns_it_names(fake_engine):
    _annotate().filter(pl.col("DP") > 10).select("chrom", "SYMBOL").collect()
    collect = fake_engine[1]
    assert (collect["vcf_info_fields"], collect["vcf_format_fields"]) == (["DP"], [])


def test_unknown_field_raises_before_any_annotator_is_created(fake_engine):
    with pytest.raises(ValueError, match="format_fields"):
        _annotate(format_fields=["NOPE"])
    assert fake_engine == []


def test_output_vcf_ignores_the_selection(monkeypatch, tmp_path):
    captured = {}

    def fake_annotate_vcf(vcf, cache_dir, output, options_json, *rest):
        captured.update(json.loads(options_json))
        return 0

    monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
    vepyr.annotate(
        INPUT_VCF, CACHE_DIR, output_vcf=str(tmp_path / "o.vcf"),
        info_fields=["DP"], show_progress=False,
    )
    assert "vcf_info_fields" not in captured and "vcf_format_fields" not in captured
```

If `annotate()` checks the cache directory's contents before the probe, reuse the
real-cache `cache_dir` fixture's value for `CACHE_DIR` in `_annotate`, exactly as
`tests/test_region_pushdown.py` does.

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/test_vcf_columns.py -k "fake or selection or projection or unknown_field or output_vcf" -v`
Expected: FAIL — `annotate() got an unexpected keyword argument 'info_fields'`.

- [ ] **Step 3: Imports** — after `:21`

```python
from vepyr._core import vcf_fields as _vcf_fields
from vepyr._vcf_columns import carried_columns, fields_for_query, validate_selection
```

- [ ] **Step 4: Signature and docstring** — add after `allow_non_variant: bool = False,`

```python
    # Input columns on the LazyFrame
    info_fields: list[str] | None = None,
    format_fields: list[str] | None = None,
```

and in the Parameters section of the docstring, before `output_vcf`:

```
    info_fields : list of str, optional
        Input INFO fields the ``LazyFrame`` carries: ``None`` (default) for all,
        a list to select, ``[]`` for none -- the meaning ``polars_bio.scan_vcf``
        gives the same argument. A field whose id is also an annotation column
        (``AF``) is carried as ``INFO_<id>``; the annotation column keeps the
        bare name. A query only reads the fields it names. Ignored with
        ``output_vcf``, which always keeps every field.
    format_fields : list of str, optional
        Input FORMAT fields, same convention. Single-sample inputs get one
        column per field; multi-sample inputs a nested ``genotypes`` struct.
```

- [ ] **Step 5: Validate and thread the selection** — immediately before the
`# Get schema from a probe annotator` comment (`:1656`). This is below the
`if output_vcf is not None:` return (`:1496-1549`), so the VCF path never sees the keys.

```python
    # Input columns. The reader panics on an id the header does not declare, so
    # the selection is checked against the header first.
    if info_fields is not None or format_fields is not None:
        header_info, header_format = _vcf_fields(vcf)
        validate_selection("info_fields", info_fields, header_info)
        validate_selection("format_fields", format_fields, header_format)
    if info_fields is not None:
        opts["vcf_info_fields"] = list(info_fields)
    if format_fields is not None:
        opts["vcf_format_fields"] = list(format_fields)
    options_json = json.dumps(opts)
```

Check how `options_json` is built above this point; if it is assembled once from `opts`
earlier, this re-dump replaces it, otherwise move the three `opts[...]` lines above the
existing dump and drop the last line.

After `pa_schema = probe.schema` add:

```python
    _carried = carried_columns(pa_schema)
```

- [ ] **Step 6: Prune per collect** — in `_batch_source`, directly after
`engine_opts = _flags_for_projection(...)` (`:1765`)

```python
        # Input columns the query does not read are not parsed at all.
        read_info, read_format = fields_for_query(
            _carried, needed, _opts.get("vcf_info_fields"), _opts.get("vcf_format_fields")
        )
        for key, value in (("vcf_info_fields", read_info), ("vcf_format_fields", read_format)):
            if value is None:
                engine_opts.pop(key, None)
            else:
                engine_opts[key] = value
```

`needed` already unions `with_columns` and the predicate's root names, so a filtered
column is read even when it is not selected. The batch then lacks the unread columns,
which is what `batch_df.select(with_columns)` expects; `selected_dataframe_columns`
(`fields=`) cannot combine with a projection (`:1748-1752` raises), so it always sees
every carried column.

- [ ] **Step 7: Run everything touching the frame**

Run: `uv run pytest tests/test_vcf_columns.py tests/test_region_pushdown.py tests/test_annotate.py -q`
Expected: PASS. A `test_annotate.py` assertion on the exact column list now sees the
carried columns: update the expectation there (this is the intended default change), do
not pass `info_fields=[]` to silence it.

- [ ] **Step 8: Fixture parity** (append)

```python
def test_carried_columns_equal_the_input(cache_dir):
    df = (
        vepyr.annotate(INPUT_VCF, cache_dir, reference_fasta=REFERENCE_FASTA, show_progress=False)
        .select("chrom", "start", "ref", "alt", "DPSum", "GT", "AD", "SYMBOL")
        .collect()
    )
    assert df.height == 100
    assert df["GT"].null_count() == 0
    assert df.schema["AD"] == pl.List(pl.Int32)


def test_pruned_and_unpruned_queries_agree(cache_dir):
    lf = vepyr.annotate(INPUT_VCF, cache_dir, reference_fasta=REFERENCE_FASTA, show_progress=False)
    narrow = lf.select("chrom", "start", "SYMBOL", "Consequence").collect()
    wide = lf.collect().select("chrom", "start", "SYMBOL", "Consequence")
    assert narrow.equals(wide)


def test_empty_selection_is_the_0_7_frame(cache_dir):
    lf = vepyr.annotate(
        INPUT_VCF, cache_dir, reference_fasta=REFERENCE_FASTA,
        info_fields=[], format_fields=[], show_progress=False,
    )
    names = list(lf.collect_schema())
    assert names[:9] == [
        "chrom", "start", "end", "id", "ref", "alt", "qual", "filter", "most_severe_consequence",
    ]
```

Run: `uv run pytest tests/test_vcf_columns.py -v` → PASS.

- [ ] **Step 9: Commit**

```bash
uv run pre-commit run ruff --all-files && uv run pre-commit run ruff-format --all-files
git add src/vepyr/__init__.py tests/
git commit -m "feat: carry the input's INFO and FORMAT columns on the LazyFrame, read only what a query names"
```

---

### Task 4: Plugin columns that share an input field's id

**Files:**
- Modify: `src/vepyr/__init__.py:1695-1703` and the batch loop `:1842-1864`
- Test: `tests/test_vcf_columns.py`

**Interfaces:**
- Produces: an input column named like a selected plugin's CSQ field is exposed as
  `INFO_<id>` (INFO) or `fmt_<id>` (FORMAT); the plugin column keeps the bare name.

- [ ] **Step 1: Failing test** (append)

```python
def test_input_field_named_like_a_plugin_column_is_renamed():
    from vepyr import _rename_shadowed_input_columns

    schema = {"chrom": pl.String, "CADD_PHRED": pl.Float32, "GT": pl.String, "SYMBOL": pl.String}
    carried = {"CADD_PHRED": ("INFO", "CADD_PHRED"), "GT": ("FORMAT", "GT")}
    renamed, mapping = _rename_shadowed_input_columns(schema, carried, ["CADD_PHRED"])
    assert list(renamed) == ["chrom", "INFO_CADD_PHRED", "GT", "SYMBOL"]
    assert mapping == {"CADD_PHRED": "INFO_CADD_PHRED"}


def test_annotation_column_named_like_a_plugin_field_still_raises():
    from vepyr import _rename_shadowed_input_columns

    with pytest.raises(ValueError, match="conflicts with an existing DataFrame column"):
        _rename_shadowed_input_columns({"SYMBOL": pl.String}, {}, ["SYMBOL"])
```

- [ ] **Step 2: Run, expect failure** — `ImportError: _rename_shadowed_input_columns`.

- [ ] **Step 3: Implement** — module-level in `src/vepyr/__init__.py`, near `_plugin_column`

```python
def _rename_shadowed_input_columns(
    schema: dict, carried: dict[str, tuple[str, str]], plugin_fields: list[str]
) -> tuple[dict, dict[str, str]]:
    """Give way to plugin columns the way the engine gives way to annotation columns.

    A plugin column keeps its CSQ field name; a carried input column with the
    same name becomes ``INFO_<id>`` / ``fmt_<id>``. A clash with anything else is
    still an error.
    """
    mapping: dict[str, str] = {}
    for name in plugin_fields:
        if name not in schema:
            continue
        if name not in carried:
            raise ValueError(
                f"plugin CSQ field {name!r} conflicts with an existing DataFrame column"
            )
        prefix = "INFO_" if carried[name][0] == "INFO" else "fmt_"
        mapping[name] = prefix + name
    return {mapping.get(name, name): dtype for name, dtype in schema.items()}, mapping
```

Then at `:1695`, before the `for name, dtype, per_variant in plugin_column_specs:` loop:

```python
        polars_schema, _shadowed = _rename_shadowed_input_columns(
            polars_schema, _carried, plugin_field_names
        )
        _carried = {_shadowed.get(name, name): value for name, value in _carried.items()}
```

(initialise `_shadowed: dict[str, str] = {}` next to `_carried` so the no-plugin path
has it), delete the now-unreachable `raise ValueError(... conflicts ...)` inside the loop,
and in the batch loop, right after `batch_df = pl.from_arrow(py_batch)`:

```python
            if _shadowed:
                batch_df = batch_df.rename(
                    {old: new for old, new in _shadowed.items() if old in batch_df.columns}
                )
```

- [ ] **Step 4: Run, commit**

```bash
uv run pytest tests/test_vcf_columns.py tests/test_annotate.py -q
git add src/vepyr/__init__.py tests/test_vcf_columns.py
git commit -m "feat: an input field named like a plugin column gives way to it"
```

---

### Task 5: Frame metadata for `pb.sink_vcf`

**Files:**
- Create: `src/vepyr/_vcf_metadata.py`
- Modify: `src/vepyr/__init__.py` (the `return register_io_source(...)` at `:1886`), `pyproject.toml:19-28`
- Test: `tests/test_vcf_columns.py`

**Interfaces:**
- Consumes: `carried_columns()` output; the probe's `pa.Schema`.
- Produces: `build_header(schema: pa.Schema, carried: dict, csq_fields: list[str] | None, extract) -> dict | None`
  and `attach(lf, vcf_path: str, schema: pa.Schema, carried: dict, csq_fields: list[str] | None) -> None`.

- [ ] **Step 1: Failing tests** (append) — `build_header` takes the extractor as an
argument so it is testable without polars-bio installed.

```python
def _fake_extract(schema):
    return {
        "format_specific": {
            "vcf": {
                "info_fields": {
                    "DP": {"number": "1", "type": "Integer", "description": "d"},
                    "INFO_AF": {"number": "A", "type": "Float", "description": "cohort"},
                    "CSQ": {"number": ".", "type": "String", "description": "old Format: X"},
                },
                "format_fields": {"GT": {"number": "1", "type": "String", "description": "g"}},
                "sample_names": ["S1"],
                "version": "VCFv4.2",
                "contigs": [{"id": "chr1", "length": 10}],
                "filters": [],
                "alt_definitions": [],
            }
        }
    }


def test_header_is_keyed_by_vcf_id_and_csq_is_replaced():
    from vepyr._vcf_metadata import build_header

    carried = {"DP": ("INFO", "DP"), "INFO_AF": ("INFO", "AF"), "GT": ("FORMAT", "GT")}
    header = build_header(CARRIED_SCHEMA, carried, ["Allele", "Consequence"], _fake_extract)
    assert set(header["info_fields"]) == {"DP", "AF", "CSQ"}
    assert header["info_fields"]["AF"]["description"] == "cohort"
    assert header["info_fields"]["CSQ"] == {
        "number": ".",
        "type": "String",
        "description": "Consequence annotations from Ensembl VEP. Format: Allele|Consequence",
    }
    assert header["sample_names"] == ["S1"]


def test_without_a_csq_column_no_csq_is_declared():
    from vepyr._vcf_metadata import build_header

    header = build_header(CARRIED_SCHEMA, {"DP": ("INFO", "DP")}, None, _fake_extract)
    assert "CSQ" not in header["info_fields"]


def test_a_schema_the_extractor_does_not_recognise_gives_no_header():
    from vepyr._vcf_metadata import build_header

    assert build_header(CARRIED_SCHEMA, {}, None, lambda schema: {"format_specific": {}}) is None
```

- [ ] **Step 2: Run, expect failure** — `ModuleNotFoundError: vepyr._vcf_metadata`.

- [ ] **Step 3: Implement** — `src/vepyr/_vcf_metadata.py`

```python
"""polars-bio frame metadata, so ``polars_bio.sink_vcf`` can write the frame.

The only module that imports polars-bio. Without it the frame is returned as is.
"""

from __future__ import annotations

import pyarrow as pa

_CSQ_DESCRIPTION = "Consequence annotations from Ensembl VEP. Format: "


def build_header(
    schema: pa.Schema,
    carried: dict[str, tuple[str, str]],
    csq_fields: list[str] | None,
    extract,
) -> dict | None:
    """VCF header metadata in polars-bio's shape, keyed by VCF id."""
    vcf = extract(schema).get("format_specific", {}).get("vcf")
    if not vcf:
        return None
    ids = {name: vcf_id for name, (_, vcf_id) in carried.items()}
    info = {
        ids.get(name, name): definition
        for name, definition in (vcf.get("info_fields") or {}).items()
        # The input's own CSQ is replaced by this run's, as Ensembl VEP does.
        if ids.get(name, name) != "CSQ"
    }
    if csq_fields is not None:
        info["CSQ"] = {
            "number": ".",
            "type": "String",
            "description": _CSQ_DESCRIPTION + "|".join(csq_fields),
        }
    return {
        "info_fields": info,
        "format_fields": {
            ids.get(name, name): definition
            for name, definition in (vcf.get("format_fields") or {}).items()
        },
        "sample_names": vcf.get("sample_names"),
        "version": vcf.get("version"),
        "contigs": vcf.get("contigs"),
        "filters": vcf.get("filters"),
        "alt_definitions": vcf.get("alt_definitions"),
    }


def attach(lf, vcf_path: str, schema: pa.Schema, carried, csq_fields) -> None:
    """Set the metadata ``polars_bio.sink_vcf`` reads; a no-op without polars-bio."""
    try:
        import polars_bio as pb
        from polars_bio._metadata import set_coordinate_system
        from polars_bio.metadata_extractors import extract_all_schema_metadata
    except ImportError:
        return
    header = build_header(schema, carried, csq_fields, extract_all_schema_metadata)
    if header is None:
        return
    pb.set_source_metadata(lf, format="vcf", path=vcf_path, header=header)
    set_coordinate_system(lf, zero_based=False)
```

- [ ] **Step 4: Call it** — replace the final `return register_io_source(...)` with

```python
    lf = register_io_source(
        io_source=_batch_source,
        schema=polars_schema,
    )
    # The CSQ sub-fields, in order: the typed columns the engine lists after
    # most_severe_consequence, then the plugin fields parsed from the string.
    csq_fields = None
    if "CSQ" in polars_schema:
        names = pa_schema.names
        csq_fields = names[names.index("most_severe_consequence") + 1 :] + plugin_field_names
    _attach_vcf_metadata(lf, vcf, pa_schema, _carried, csq_fields)
    return lf
```

with `from vepyr._vcf_metadata import attach as _attach_vcf_metadata` beside the other
imports, and the CSQ field list computed by this module-level helper instead of the
inline slice above (replace the three `csq_fields` lines with
`csq_fields = _csq_field_names(pa_schema.names, selected_fields, plugin_field_names) if "CSQ" in polars_schema else None`):

```python
# Typed columns the engine emits that have no CSQ sub-field (docs/dataframes.md).
_CACHE_ONLY_COLUMNS = frozenset(
    {
        "clin_sig_allele", "clinical_impact", "minor_allele", "minor_allele_freq",
        "clinvar_ids", "cosmic_ids", "dbsnp_ids",
    }
)


def _csq_field_names(
    schema_names: list[str], selected_fields: list[str] | None, plugin_fields: list[str]
) -> list[str]:
    """The `Format:` list of the CSQ string, in CSQ order."""
    if selected_fields is not None:  # annotate(fields=...) fixes the layout
        return list(selected_fields)
    start = schema_names.index("most_severe_consequence") + 1
    base = [name for name in schema_names[start:] if name not in _CACHE_ONLY_COLUMNS]
    return base + list(plugin_fields)
```

Test (append to `tests/test_vcf_columns.py`):

```python
def test_csq_field_names_skip_cache_only_columns_and_append_plugins():
    from vepyr import _csq_field_names

    names = ["chrom", "CSQ", "most_severe_consequence", "Allele", "Consequence", "dbsnp_ids"]
    assert _csq_field_names(names, None, ["CADD_PHRED"]) == ["Allele", "Consequence", "CADD_PHRED"]
    assert _csq_field_names(names, ["Consequence"], []) == ["Consequence"]
```

This assumes the CSQ layout equals the typed-column list for every flag combination.
Task 6 checks that against the `##INFO=<ID=CSQ` line the `output_vcf` path writes, with
and without `everything`. If the two differ for some flags, **stop and report**: the
engine then has to expose its `Format:` string on the `CSQ` field's metadata, which is an
engine change, not something to approximate here.

- [ ] **Step 5: The extra** — in `pyproject.toml` under `[project.optional-dependencies]`

```toml
polars-bio = ["polars-bio>=0.36"]
```

Use the first polars-bio release that contains the writer fixes
(`docs/superpowers/plans/2026-09-20-sink-vcf-writer-formats-polars-bio.md`); 0.35.1 lacks them.

- [ ] **Step 6: Run, commit**

```bash
uv run pytest tests/test_vcf_columns.py -q
git add src/vepyr/_vcf_metadata.py src/vepyr/__init__.py pyproject.toml tests/test_vcf_columns.py
git commit -m "feat: attach polars-bio VCF metadata to the annotation frame"
```

---

### Task 6: `sink_vcf` agrees with `output_vcf`

**Files:**
- Test: `tests/test_vcf_columns.py`
- Create: `e2e-testing/scripts/sink_vcf_parity.py`

**Interfaces:**
- Consumes: everything above plus a polars-bio with the writer fixes.

- [ ] **Step 1: Test** (append; skipped without polars-bio)

```python
def _records(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        if line.startswith("#"):
            continue
        f = line.split("\t")
        fmt = {k: v for k, v in zip(f[8].split(":"), f[9].split(":")) if v != "."}
        out[(f[0], f[1], f[3], f[4])] = (f[2], f[5], f[6], frozenset(f[7].split(";")), frozenset(fmt.items()))
    return out


def _header_line(path, prefix):
    return [l for l in Path(path).read_text().splitlines() if l.startswith(prefix)]


@pytest.mark.parametrize("flags", [{"everything": True}, {"check_existing": True, "hgvs": True}])
def test_sink_vcf_matches_output_vcf_field_for_field(cache_dir, tmp_path, flags):
    pb = pytest.importorskip("polars_bio")
    kwargs = dict(reference_fasta=REFERENCE_FASTA, show_progress=False, **flags)

    reference = tmp_path / "reference.vcf"
    vepyr.annotate(INPUT_VCF, cache_dir, output_vcf=str(reference), compression="plain", **kwargs)

    sunk = tmp_path / "sunk.vcf"
    lf = vepyr.annotate(INPUT_VCF, cache_dir, skip_csq=False, **kwargs)
    pb.sink_vcf(lf.filter(pl.col("IMPACT").list.contains("MODIFIER")), str(sunk))

    got, want = _records(sunk), _records(reference)
    assert got and set(got) <= set(want)
    assert all(got[key] == want[key] for key in got)
    # same CSQ layout declared, GT first, input header kept
    assert _header_line(sunk, "##INFO=<ID=CSQ") == _header_line(reference, "##INFO=<ID=CSQ")
    first = next(l for l in sunk.read_text().splitlines() if not l.startswith("#"))
    assert first.split("\t")[8].startswith("GT")
```

- [ ] **Step 2: Run**

Run: `uv pip install "polars-bio>=0.36" && uv run pytest tests/test_vcf_columns.py::test_sink_vcf_matches_output_vcf_field_for_field -v`
Expected: PASS. A `##INFO=<ID=CSQ` mismatch means Task 5 Step 4's slice is wrong: fix the
slice, not the test.

- [ ] **Step 3: chr22 parity script** — `e2e-testing/scripts/sink_vcf_parity.py`, modelled on
`e2e-testing/scripts/lazyframe_workers_parity.py` (arguments `--vcf --cache --fasta
--region --out`): runs the `output_vcf` path and the `filter → pb.sink_vcf` path on
HG002 chr22 for the three cache profiles, compares with `_records` above, prints
`records / identical / header CSQ equal` per profile, exits non-zero on any difference.
Copy `_records` into it verbatim; do not import from `tests/`.

Run: `uv run python e2e-testing/scripts/sink_vcf_parity.py --vcf tests/data/hg002_chr22/input_chr22.vcf.gz --cache $DATA_VEPYR_DIR/cache/116_GRCh38_ensembl --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa --region chr22:20000000-25000000 --out /tmp/sink-parity`
Expected: every profile `identical == records`.

- [ ] **Step 4: Commit**

```bash
git add tests/test_vcf_columns.py e2e-testing/scripts/sink_vcf_parity.py
git commit -m "test: sink_vcf output agrees with output_vcf field for field"
```

---

### Task 7: Docs, version, gates, PR

**Files:**
- Modify: `docs/dataframes.md` (schema listing `:35-45`, the sentence at `:140-142`, "What is pushed into the engine" `:187-216`, new section before "Agreement with the VCF output"), `docs/api.md`, `pyproject.toml:3`, `Cargo.toml:3`

- [ ] **Step 1: `docs/dataframes.md`**
  - Schema listing: after `'filter': String,` insert the 16 INFO and 6 FORMAT columns of
    the HG002 example with the comment `# the input's own INFO and FORMAT fields`.
  - Replace "The input's other `INFO` fields and its sample columns are not in the frame;
    use `output_vcf` for those." with: "The input's `INFO` fields and its sample columns
    are carried too, named by their VCF ids; pass `info_fields=[]`, `format_fields=[]` for
    the 0.7 frame. An id that is also an annotation column arrives as `INFO_<id>`
    (`fmt_<id>` for FORMAT): `AF` is always VEP's."
  - "What is pushed into the engine": make it four things; add "**The input columns a
    query names** are the only ones parsed: `select("chrom", "SYMBOL")` reads no INFO and
    no samples."
  - New section "Writing a filtered VCF" with the spec's §1 example, the
    `pip install "vepyr[polars-bio]"` line, the `skip_csq=False` requirement, and the §6
    limits verbatim in meaning: schema-order INFO, no provenance lines, use `output_vcf`
    for byte parity with VEP.

- [ ] **Step 2: `docs/api.md`** — document `info_fields`, `format_fields` with the
docstring text from Task 3 Step 4.

- [ ] **Step 3: Version** — `0.7.0` → `0.8.0` in `pyproject.toml:3` and `Cargo.toml:3`;
`cargo check` to refresh `Cargo.lock`; `uv lock`.

- [ ] **Step 4: Gates**

```bash
env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
uv run pytest -q
uv run python e2e-testing/scripts/lazyframe_workers_parity.py   # with its usual chr22 arguments
cd e2e-testing/scripts && uv run python run_comparison.py --release 116 --chroms chr22 chr1 \
  --comparison-mode md5 --md5-mode strict --bgzf
```

Expected: all green; the md5 digests cannot move (the `output_vcf` path is untouched).
Then re-measure the two `docs/dataframes.md` timing tables on the same host
(`uptime` first, discard the first run) and update the numbers — `collect()` now reads
INFO and samples; the `select(...)` rows must not move beyond noise.

- [ ] **Step 5: Commit and draft PR**

```bash
git add docs pyproject.toml Cargo.toml Cargo.lock uv.lock
git commit -m "docs: input columns on the LazyFrame and writing a filtered VCF; 0.8.0"
git push -u origin feat/lazyframe-vcf-columns
gh pr create --draft --title "feat: LazyFrame carries the input's INFO/FORMAT columns; pb.sink_vcf support" --body-file /tmp/pr-vepyr-vcf-columns.md
```

PR body: the default change and the way back, the pruning numbers, the parity script's
output, the polars-bio version floor, and the two prerequisite PRs.
