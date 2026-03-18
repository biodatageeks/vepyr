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
