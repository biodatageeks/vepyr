# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**vepyr** (/ˈvaɪpər/) — VEP Yielding Performant Results — is a Python interface (via PyO3/maturin) wrapping two Rust crates from the biodatageeks ecosystem:

- **`datafusion-bio-function-vep`** — variant annotation engine: allele matching, transcript consequence prediction (SO terms, HGVS, protein impact), exposed as DataFusion UDFs/table functions (`annotate_vep()`, `lookup_variants()`)
- **`datafusion-bio-format-ensembl-cache`** — reads Ensembl VEP offline cache directories (Storable/Sereal `.gz` files) into DataFusion `TableProvider`s with Arrow schemas for variations, transcripts, exons, translations, regulatory/motif features

The Python package exposes:
1. Ensembl cache conversion to Parquet and fjall (embedded KV store) formats
2. Variant annotation against converted caches

## Build & Development

This is a PyO3 + maturin project managed with uv. The Rust code compiles to a native Python extension module.

```bash
# Install deps and build the extension into .venv
uv sync

# Rebuild after Rust changes (faster iteration)
uv run maturin develop

# Build release wheel
uv run maturin build --release

# Run Python tests
uv run pytest

# Run a single test
uv run pytest tests/test_foo.py::test_bar -v

# Run Rust tests
cargo test

# Lint
cargo clippy
uv run ruff check .

# Format
cargo fmt
uv run ruff format .
```

## Architecture

The crate boundary matters: **bio-format-ensembl-cache** is the data reader (Ensembl cache dir → Arrow batches), **bio-function-vep** is the compute layer (variant annotation logic). vepyr bridges both to Python.

Key upstream types to expose via PyO3:
- `EnsemblCacheOptions` / `EnsemblCacheTableProvider` — configure and register cache sources
- `EnsemblEntityKind` — enum: Variation, Transcript, RegulatoryFeature, MotifFeature, Exon, Translation
- `annotate_vep()` / `lookup_variants()` — the main annotation entry points
- `AnnotationConfig` — cache_size_mb, zstd_level, dict_size_kb

The annotation pipeline: VCF data + Ensembl cache → allele matching → transcript consequence engine (uses COITree interval trees) → SO terms + HGVS + protein impact.

Data flows through Arrow/DataFusion, so Python↔Rust data exchange should use PyArrow (`arrow-rs` ↔ `pyarrow` via `arrow` pyo3 bindings or `datafusion-python`).

## Upstream Repositories

- VEP functions: `github.com/biodatageeks/datafusion-bio-functions` (path: `datafusion/bio-function-vep`)
- Ensembl cache format: `github.com/biodatageeks/datafusion-bio-formats` (path: `datafusion/bio-format-ensembl-cache`)

<!-- GSD:project-start source:PROJECT.md -->
## Project

**vepyr**

vepyr is a Python-facing, Rust-powered reimplementation of Ensembl's Variant Effect Predictor for bioinformatics teams. It is designed to build and use Ensembl VEP cache data locally, annotate VCF inputs through a fast native engine, and return results in basic Polars workflows plus VCF-compatible output. The current brownfield codebase already exposes Python APIs for cache building and streaming annotation, and the project is now focused on closing the correctness and performance gap against Ensembl VEP itself.

**Core Value:** Produce Ensembl VEP `--everything` results with zero mismatches for the supported scope while being dramatically faster to run.

### Constraints

- **Product surface**: Python-first library API — that is the primary user interface for v1
- **Stack**: Existing Rust + PyO3 + DataFusion architecture — new work should build on the current engine rather than replacing it
- **Correctness**: Zero mismatches vs Ensembl VEP `--everything` for the supported scope — this is the short-term quality bar
- **Performance**: `50x+` speedup over Ensembl VEP — performance work must be measured against the reference tool
- **Scope**: `homo_sapiens`, `GRCh38`, one Ensembl release — limits the initial validation surface so parity can be achieved rigorously
- **Outputs**: Basic Polars plus VCF-compatible results — output work should serve those two paths first
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Rust 2021 edition - native annotation engine and cache conversion code in `src/lib.rs`, `src/annotate.rs`, and `src/convert.rs`
- Python 3.10+ - public package API and orchestration layer in `src/vepyr/__init__.py`
- Markdown - user-facing package docs in `README.md`
- TOML - package/build configuration in `Cargo.toml`, `pyproject.toml`, and `uv.lock`
## Runtime
- CPython 3.10+ - required by `pyproject.toml`, with `.python-version` pinned to `3.12` for local development
- Native Python extension module via PyO3 abi3 - built as `_core` from Rust with `crate-type = ["cdylib"]` in `Cargo.toml`
- Tokio multi-thread runtime - created inside `src/annotate.rs` to drive DataFusion streaming work
- `uv` - implied by committed `uv.lock` and README install command
- Rust/Cargo - used for native dependency resolution through `Cargo.toml`
- Lockfile: `uv.lock` present; Cargo lockfile is not committed in the current tree
## Frameworks
- PyO3 0.25 - Python bindings and module export surface in `src/lib.rs`
- DataFusion 50.3 - SQL/query execution for cache conversion and VCF annotation in `src/annotate.rs` and `src/convert.rs`
- Arrow 56 with `pyarrow` feature - Arrow/PyArrow interop for batch streaming
- `datafusion-bio-*` crates from pinned git revisions - domain-specific VCF and Ensembl cache table providers plus VEP functions
- Pytest 8+ - Python test runner declared in `pyproject.toml`
- Polars and PyArrow - exercised in integration tests under `tests/test_annotate.py` and `tests/test_golden.py`
- Maturin 1.x - build backend configured in `pyproject.toml`
- `uv sync --reinstall-package vepyr` - documented local install/bootstrap flow in `README.md`
## Key Dependencies
- `pyo3` 0.25 - exposes Rust functions/classes to Python
- `datafusion` 50.3 - powers SQL execution and streaming annotation
- `arrow` 56 - schema and RecordBatch transport to Python
- `datafusion-bio-function-vep` (git rev `5baf669...`) - registers annotation functions
- `datafusion-bio-format-ensembl-cache` and `datafusion-bio-format-vcf` (git rev `7cbc049...`) - provide table providers for cache and input VCFs
- `tokio` 1.x - runtime for async DataFusion execution
- `futures` 0.3 - stream iteration in Rust
- `pyarrow`, `tqdm`, `ipywidgets` - Python-side data transport and notebook/progress UX
## Configuration
- No `.env`-driven runtime configuration is present in the tracked files
- Runtime behavior is controlled through Python function arguments such as `cache_dir`, `reference_fasta`, `partitions`, `memory_limit_gb`, and annotation flags in `src/vepyr/__init__.py`
- Test fixture preparation relies on optional shell environment variables in `tests/data/golden/prepare.py`: `CACHE_SRC`, `VCF_SRC`, `GOLDEN_SRC`, `FASTA_SRC`
- `Cargo.toml` defines Rust crate metadata and pinned git dependencies
- `pyproject.toml` defines Python package metadata and maturin settings
- `.python-version` pins local interpreter expectations
## Platform Requirements
- Python 3.12 is the local default, but package metadata allows 3.10+
- Rust toolchain is required to build the extension module
- External genomics CLI tools are required only for fixture generation: `bcftools`, `samtools`, `bgzip`, `tabix` in `tests/data/golden/prepare.py`
- Distributed as a Python package with an embedded native extension
- Requires local filesystem access to large VCF/cache/reference datasets rather than a hosted deployment target
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- Rust modules use snake_case filenames in `src/` such as `annotate.rs` and `convert.rs`
- Python tests follow pytest’s `test_*.py` naming under `tests/`
- The Python package surface is concentrated in `src/vepyr/__init__.py` rather than split across many modules
- Rust functions use snake_case (`create_streaming_annotator`, `writer_properties`, `build_query_multi_chrom`)
- Python functions also use snake_case (`build_cache`, `_download_with_progress`, `annotate`)
- Helper/private functions are prefixed with `_` on the Python side
- Rust constants use `UPPER_SNAKE_CASE` (`MAIN_CHROMS`)
- Rust locals and Python variables use snake_case
- Python internal aliases for native imports use a leading underscore (`_convert_entity`, `_create_annotator`)
- Rust structs and enums use PascalCase (`StreamingAnnotator`, `EnsemblEntityKind`)
- Python typing uses builtin generics and quoted forward refs where needed
## Code Style
- Rust style matches `rustfmt` defaults: 4-space indentation, trailing commas, wrapped builder chains
- Python style is PEP 8-like with 4-space indentation and double-quoted docstring examples
- Long function signatures and conditionals are split across multiple lines rather than compressed
- No explicit lint config files (`ruff.toml`, `mypy.ini`, `clippy.toml`, etc.) were found in the repo root
- Style discipline appears to rely on standard formatter behavior and review rather than enforced lint configs
## Import Organization
- Both Python and Rust files use grouped imports with blank lines between major groups
- Rust commonly groups `std::` imports first, then crate/external imports
- None detected. Imports are package-relative or direct module paths.
## Error Handling
- Python validates user misuse with explicit `ValueError` or `FileNotFoundError`
- Rust converts lower-level errors to `PyRuntimeError` at the FFI boundary
- Rust uses `Result` propagation heavily internally and only formats strings when crossing into Python
- Expected invalid-user-input paths are handled in Python before entering the engine
- Execution/runtime failures are surfaced as runtime exceptions with context strings like `"Failed to open VCF: ..."` or `"Annotation stream error: ..."`
- One special non-error sentinel exists in `src/lib.rs`: the string `"skipped"` maps to `None` for Python callers
## Logging
- Python uses the stdlib `logging` module
- Rust uses direct stderr output for progress and does not yet integrate the `log` crate in the visible source files
- Logs are informational and operational rather than structured
- Progress feedback is user-oriented, especially in cache build loops and downloads
## Comments
- Comments explain domain-specific reasoning and edge cases rather than obvious control flow
- Examples: chromosome partitioning, Ensembl URL conventions, and why a `LazyFrame` is made rerunnable
- Python public functions have detailed docstrings with parameters, returns, and examples
- Rust exported/public items have short descriptive doc comments, especially around the PyO3 surface
## Function Design
- Small boundary functions in `src/lib.rs`
- Large orchestration functions are accepted when they model a complete workflow, especially `build_cache()` and `annotate()` in `src/vepyr/__init__.py`
- Python favors rich keyword-heavy signatures for user-facing APIs
- Rust helpers often accept explicit primitives and references rather than wrapper structs
- Python returns concrete user-facing objects (`list[tuple[str, int]]`, `pl.LazyFrame`)
- Rust returns `PyResult<T>` at the boundary and standard `Result` internally
## Module Design
- Public Python API is curated through `__all__ = ["build_cache", "annotate"]`
- Rust exports only the functions/classes registered in the `_core` module
- `src/vepyr/__init__.py` effectively acts as the barrel/module facade for the Python package
- No additional index/barrel pattern is present elsewhere
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- Public API is Python-first, but heavy data processing lives in native Rust code
- Execution is file-oriented and batch/stream based rather than request/response
- DataFusion SQL is the core execution substrate for both cache conversion and annotation
- The package is state-light: inputs and outputs are mostly filesystem paths and streamed record batches
## Layers
- Purpose: expose ergonomic `build_cache()` and `annotate()` functions for users
- Contains: download/extraction orchestration, option validation, notebook-friendly status, Polars integration
- Location: `src/vepyr/__init__.py`
- Depends on: native `_core` module, Python stdlib, `polars`, `pyarrow`, `tqdm`
- Used by: package consumers and the Python test suite
- Purpose: define the PyO3 module boundary and normalize errors to Python exceptions
- Contains: `_core` module registration, `convert_entity()`, `create_annotator()`, `StreamingAnnotator`
- Location: `src/lib.rs` and `src/annotate.rs`
- Depends on: PyO3, Arrow/PyArrow conversion, Tokio runtime
- Used by: Python API layer
- Purpose: perform conversion queries and live annotation streams against genomics datasets
- Contains: DataFusion session setup, SQL generation, provider registration, parquet writing
- Location: `src/convert.rs` and `src/annotate.rs`
- Depends on: DataFusion, Arrow, Parquet, `datafusion-bio-*` crates
- Used by: Rust FFI layer
## Data Flow
- Persistent state lives on disk as downloaded cache data, generated parquet files, input VCFs, and reference FASTA files
- In-memory state is intentionally short-lived per conversion or annotation invocation
## Key Abstractions
- Purpose: bridge an async `SendableRecordBatchStream` into Python iteration
- Examples: `StreamingAnnotator` in `src/annotate.rs`
- Pattern: PyO3 class wrapping a Tokio runtime plus mutex-protected optional stream
- Purpose: transform one Ensembl cache entity into sorted parquet outputs
- Examples: `convert_entity()`, `build_query()`, `writer_properties()` in `src/convert.rs`
- Pattern: per-entity strategy with hard-coded schema- and domain-specific behavior
- Purpose: preserve a Python-friendly function signature while sending engine flags as JSON
- Examples: `opts` assembly in `annotate()` inside `src/vepyr/__init__.py`
- Pattern: argument normalization at the Python boundary, opaque options payload to Rust/SQL layer
## Entry Points
- Location: `src/vepyr/__init__.py`
- Triggers: `import vepyr`, then calls to `build_cache()` or `annotate()`
- Responsibilities: validation, orchestration, UX, conversion to Polars
- Location: `src/lib.rs`
- Triggers: Python importing `vepyr._core`
- Responsibilities: register exposed functions and classes
- Location: `tests/test_import.py`, `tests/test_annotate.py`, `tests/test_golden.py`
- Triggers: `pytest`
- Responsibilities: verify importability, lazy annotation behavior, and golden-output agreement
## Error Handling
- Python raises `ValueError` for invalid argument combinations in `annotate()`
- Rust wraps most failures with formatted `PyRuntimeError` messages in `src/lib.rs` and `src/annotate.rs`
- Some Rust control flow uses sentinel strings such as `"skipped"` in `src/lib.rs` to encode non-error states
## Cross-Cutting Concerns
- Python layer logs downloads, extraction, and run progress via `logging`
- Rust conversion code writes progress summaries to stderr with `print_progress()`
- Python layer validates method values, required FASTA combinations, and local cache existence
- Rust layer assumes most semantic correctness after the Python boundary
- Conversion tunes row group size and sort keys per entity in `src/convert.rs`
- Annotation uses streaming batches and DataFusion pushdown via `n_rows` / SQL `LIMIT`
<!-- GSD:architecture-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd:quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd:debug` for investigation and bug fixing
- `/gsd:execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd:profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
