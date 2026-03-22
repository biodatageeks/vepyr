# Architecture

**Analysis Date:** 2026-03-22

## Pattern Overview

**Overall:** Python library facade over a Rust-powered genomics engine

**Key Characteristics:**
- Public API is Python-first, but heavy data processing lives in native Rust code
- Execution is file-oriented and batch/stream based rather than request/response
- DataFusion SQL is the core execution substrate for both cache conversion and annotation
- The package is state-light: inputs and outputs are mostly filesystem paths and streamed record batches

## Layers

**Python API Layer:**
- Purpose: expose ergonomic `build_cache()` and `annotate()` functions for users
- Contains: download/extraction orchestration, option validation, notebook-friendly status, Polars integration
- Location: `src/vepyr/__init__.py`
- Depends on: native `_core` module, Python stdlib, `polars`, `pyarrow`, `tqdm`
- Used by: package consumers and the Python test suite

**Rust FFI Layer:**
- Purpose: define the PyO3 module boundary and normalize errors to Python exceptions
- Contains: `_core` module registration, `convert_entity()`, `create_annotator()`, `StreamingAnnotator`
- Location: `src/lib.rs` and `src/annotate.rs`
- Depends on: PyO3, Arrow/PyArrow conversion, Tokio runtime
- Used by: Python API layer

**Execution Layer:**
- Purpose: perform conversion queries and live annotation streams against genomics datasets
- Contains: DataFusion session setup, SQL generation, provider registration, parquet writing
- Location: `src/convert.rs` and `src/annotate.rs`
- Depends on: DataFusion, Arrow, Parquet, `datafusion-bio-*` crates
- Used by: Rust FFI layer

## Data Flow

**Cache Build Flow:**

1. User calls `vepyr.build_cache()` in `src/vepyr/__init__.py`
2. Python layer validates options and optionally downloads/extracts an Ensembl cache tarball
3. Python loops through entity names and calls native `_convert_entity`
4. Rust `convert_entity()` in `src/lib.rs` forwards to conversion logic in `src/convert.rs`
5. DataFusion table providers read cache tables and emit sorted parquet partitions to disk
6. Python reports written files and row counts back to the caller

**Annotation Flow:**

1. User calls `vepyr.annotate()` in `src/vepyr/__init__.py`
2. Python validates feature flags and serializes engine options to JSON
3. Python probes schema through native `_create_annotator`
4. Rust creates a `SessionContext`, registers VCF and VEP functions, and executes SQL in `src/annotate.rs`
5. `StreamingAnnotator` yields PyArrow RecordBatches over the PyO3 boundary
6. Python converts batches to Polars frames and registers them as an IO source for a rerunnable `LazyFrame`

**State Management:**
- Persistent state lives on disk as downloaded cache data, generated parquet files, input VCFs, and reference FASTA files
- In-memory state is intentionally short-lived per conversion or annotation invocation

## Key Abstractions

**StreamingAnnotator:**
- Purpose: bridge an async `SendableRecordBatchStream` into Python iteration
- Examples: `StreamingAnnotator` in `src/annotate.rs`
- Pattern: PyO3 class wrapping a Tokio runtime plus mutex-protected optional stream

**Entity conversion pipeline:**
- Purpose: transform one Ensembl cache entity into sorted parquet outputs
- Examples: `convert_entity()`, `build_query()`, `writer_properties()` in `src/convert.rs`
- Pattern: per-entity strategy with hard-coded schema- and domain-specific behavior

**Python option passthrough:**
- Purpose: preserve a Python-friendly function signature while sending engine flags as JSON
- Examples: `opts` assembly in `annotate()` inside `src/vepyr/__init__.py`
- Pattern: argument normalization at the Python boundary, opaque options payload to Rust/SQL layer

## Entry Points

**Python package entry:**
- Location: `src/vepyr/__init__.py`
- Triggers: `import vepyr`, then calls to `build_cache()` or `annotate()`
- Responsibilities: validation, orchestration, UX, conversion to Polars

**Native module entry:**
- Location: `src/lib.rs`
- Triggers: Python importing `vepyr._core`
- Responsibilities: register exposed functions and classes

**Test entry points:**
- Location: `tests/test_import.py`, `tests/test_annotate.py`, `tests/test_golden.py`
- Triggers: `pytest`
- Responsibilities: verify importability, lazy annotation behavior, and golden-output agreement

## Error Handling

**Strategy:** validate early in Python for user-facing option errors, convert Rust/DataFusion failures into `PyRuntimeError`

**Patterns:**
- Python raises `ValueError` for invalid argument combinations in `annotate()`
- Rust wraps most failures with formatted `PyRuntimeError` messages in `src/lib.rs` and `src/annotate.rs`
- Some Rust control flow uses sentinel strings such as `"skipped"` in `src/lib.rs` to encode non-error states

## Cross-Cutting Concerns

**Logging:**
- Python layer logs downloads, extraction, and run progress via `logging`
- Rust conversion code writes progress summaries to stderr with `print_progress()`

**Validation:**
- Python layer validates method values, required FASTA combinations, and local cache existence
- Rust layer assumes most semantic correctness after the Python boundary

**Performance:**
- Conversion tunes row group size and sort keys per entity in `src/convert.rs`
- Annotation uses streaming batches and DataFusion pushdown via `n_rows` / SQL `LIMIT`

---

*Architecture analysis: 2026-03-22*
*Update when major patterns change*
