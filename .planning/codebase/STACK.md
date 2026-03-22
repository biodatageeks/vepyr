# Technology Stack

**Analysis Date:** 2026-03-22

## Languages

**Primary:**
- Rust 2021 edition - native annotation engine and cache conversion code in `src/lib.rs`, `src/annotate.rs`, and `src/convert.rs`
- Python 3.10+ - public package API and orchestration layer in `src/vepyr/__init__.py`

**Secondary:**
- Markdown - user-facing package docs in `README.md`
- TOML - package/build configuration in `Cargo.toml`, `pyproject.toml`, and `uv.lock`

## Runtime

**Environment:**
- CPython 3.10+ - required by `pyproject.toml`, with `.python-version` pinned to `3.12` for local development
- Native Python extension module via PyO3 abi3 - built as `_core` from Rust with `crate-type = ["cdylib"]` in `Cargo.toml`
- Tokio multi-thread runtime - created inside `src/annotate.rs` to drive DataFusion streaming work

**Package Manager:**
- `uv` - implied by committed `uv.lock` and README install command
- Rust/Cargo - used for native dependency resolution through `Cargo.toml`
- Lockfile: `uv.lock` present; Cargo lockfile is not committed in the current tree

## Frameworks

**Core:**
- PyO3 0.25 - Python bindings and module export surface in `src/lib.rs`
- DataFusion 50.3 - SQL/query execution for cache conversion and VCF annotation in `src/annotate.rs` and `src/convert.rs`
- Arrow 56 with `pyarrow` feature - Arrow/PyArrow interop for batch streaming
- `datafusion-bio-*` crates from pinned git revisions - domain-specific VCF and Ensembl cache table providers plus VEP functions

**Testing:**
- Pytest 8+ - Python test runner declared in `pyproject.toml`
- Polars and PyArrow - exercised in integration tests under `tests/test_annotate.py` and `tests/test_golden.py`

**Build/Dev:**
- Maturin 1.x - build backend configured in `pyproject.toml`
- `uv sync --reinstall-package vepyr` - documented local install/bootstrap flow in `README.md`

## Key Dependencies

**Critical:**
- `pyo3` 0.25 - exposes Rust functions/classes to Python
- `datafusion` 50.3 - powers SQL execution and streaming annotation
- `arrow` 56 - schema and RecordBatch transport to Python
- `datafusion-bio-function-vep` (git rev `5baf669...`) - registers annotation functions
- `datafusion-bio-format-ensembl-cache` and `datafusion-bio-format-vcf` (git rev `7cbc049...`) - provide table providers for cache and input VCFs

**Infrastructure:**
- `tokio` 1.x - runtime for async DataFusion execution
- `futures` 0.3 - stream iteration in Rust
- `pyarrow`, `tqdm`, `ipywidgets` - Python-side data transport and notebook/progress UX

## Configuration

**Environment:**
- No `.env`-driven runtime configuration is present in the tracked files
- Runtime behavior is controlled through Python function arguments such as `cache_dir`, `reference_fasta`, `partitions`, `memory_limit_gb`, and annotation flags in `src/vepyr/__init__.py`
- Test fixture preparation relies on optional shell environment variables in `tests/data/golden/prepare.py`: `CACHE_SRC`, `VCF_SRC`, `GOLDEN_SRC`, `FASTA_SRC`

**Build:**
- `Cargo.toml` defines Rust crate metadata and pinned git dependencies
- `pyproject.toml` defines Python package metadata and maturin settings
- `.python-version` pins local interpreter expectations

## Platform Requirements

**Development:**
- Python 3.12 is the local default, but package metadata allows 3.10+
- Rust toolchain is required to build the extension module
- External genomics CLI tools are required only for fixture generation: `bcftools`, `samtools`, `bgzip`, `tabix` in `tests/data/golden/prepare.py`

**Production:**
- Distributed as a Python package with an embedded native extension
- Requires local filesystem access to large VCF/cache/reference datasets rather than a hosted deployment target

---

*Stack analysis: 2026-03-22*
*Update after major dependency changes*
