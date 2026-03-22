# Stack Research

**Domain:** offline variant annotation engine / Ensembl VEP-compatible Python library
**Researched:** 2026-03-22
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Rust | 2021 edition | Core annotation engine and cache conversion pipeline | Best fit for the project’s explicit speed target and already proven in the existing codebase |
| PyO3 abi3 | 0.25.x | Stable Python extension boundary | Keeps the user-facing API in Python while preserving native performance |
| Apache Arrow | 56.x | Columnar batch interchange | Aligns with Polars/PyArrow expectations and minimizes data-copy overhead |
| Apache DataFusion | 50.x | Query planning and execution for VCF/cache joins | Matches the current architecture and provides lazy query execution on Arrow-backed data |
| Polars LazyFrame | current stable | Primary Python consumption surface | Official docs position `LazyFrame` as the preferred high-performance mode, which fits the target user and current API shape |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `datafusion-bio-function-vep` | pinned git SHA | Consequence-calculation functions | Keep as the domain execution kernel until parity gaps force targeted upstream/fork changes |
| `datafusion-bio-format-vcf` | pinned git SHA | VCF ingestion | Use for the supported VCF-first annotation workflow |
| `datafusion-bio-format-ensembl-cache` | pinned git SHA | Ensembl cache ingestion | Use for release-specific local cache access in offline annotation mode |
| PyArrow | 18+ | Arrow bridge for Python users | Needed whenever batches cross from Rust/DataFusion into Python |
| `bgzip` / `tabix` / `samtools` / `bcftools` | current stable CLI tools | Fixture prep and ecosystem interoperability | Use in test-data prep and when reproducing VEP-style workflows externally |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `uv` | Python environment and lockfile management | Already present; keep for reproducible Python developer installs |
| `maturin` | Build the Python extension package | Standard fit for PyO3 projects shipping Python wheels |
| `pytest` | Python integration and golden testing | Use as the cross-language verification harness because the product surface is Python-first |
| `criterion` or equivalent Rust benchmarking tool | Native performance benchmarking | Add to make the `50x+` target measurable and repeatable |

## Installation

```bash
# Python + native extension dev environment
uv sync --reinstall-package vepyr

# Run tests
uv run pytest

# Build extension/wheel
uv run maturin develop
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Rust + PyO3 | Pure Python implementation | Only for thin orchestration/helper tooling, not the annotation hot path |
| DataFusion + Arrow | pandas-centric in-memory pipeline | Only for small-data convenience utilities where parity/performance are not critical |
| Polars LazyFrame | pandas DataFrame as primary surface | Offer later only if downstream users demand it; keep Polars first for performance and lazy pushdown |
| Local cache/offline-first execution | public database-backed annotation | Suitable only for small inputs or exploratory runs; official Ensembl docs recommend cache/offline for performance |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Database-backed VEP-equivalent execution as the main path | Official Ensembl docs note cache/offline mode is the most efficient path | Local indexed cache + offline/local FASTA workflow |
| Broad multi-species/multi-release support in v1 | Expands the parity surface too early and will dilute the zero-mismatch goal | Lock v1 to `homo_sapiens`, `GRCh38`, one release |
| Pandas-first output as the primary design center | Encourages eager materialization and weakens the project’s performance story | Polars LazyFrame + VCF-compatible emit path |
| Unpinned upstream schema assumptions | Ensembl cache structure changes by release | Release-pinned fixtures, golden data, and explicit version compatibility checks |

## Stack Patterns by Variant

**If the task is parity-critical annotation:**
- Use local cache + offline/reference FASTA flow
- Because official Ensembl docs recommend `--cache`/`--offline` for performance, privacy, and stable local execution

**If the task is Python analysis consumption:**
- Use Polars `LazyFrame` as the first-class result surface
- Because Polars’ lazy mode is documented as the preferred high-performance execution path

**If the task is benchmark or regression verification:**
- Use golden fixtures plus release-pinned VEP output
- Because parity claims against `--everything` must be reproducible, not anecdotal

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| Ensembl cache release N | VEP tool release N | Official Ensembl docs strongly recommend matching cache and VEP versions |
| `--hgvs` / `--everything` workflows | local FASTA in cache/offline mode | Official VEP docs require FASTA for HGVS generation when offline/cache-based |
| DataFusion 50.x | Arrow 56.x | Matches the current project dependencies |
| Polars lazy workflows | known schema up front | Polars docs emphasize that lazy execution depends on schema knowledge |

## Sources

- https://www.ensembl.org/info/docs/tools/vep/script/vep_cache.html — verified cache/offline guidance, FASTA requirements, release compatibility
- https://www.ensembl.org/info/docs/tools/vep/script/vep_options.html — verified `--everything` scope and performance guidance
- https://www.ensembl.org/info/docs/tools/vep/vep_formats.html — verified VCF/CSQ output expectations
- https://datafusion.apache.org/python/user-guide/dataframe/index.html — verified DataFusion’s lazy logical-plan execution model
- https://docs.pola.rs/api/python/stable/reference/lazyframe/ — verified LazyFrame as the core lazy query abstraction
- https://docs.pola.rs/user-guide/lazy/ — verified Polars lazy mode as the preferred high-performance path

---
*Stack research for: offline variant annotation engine / Ensembl VEP-compatible Python library*
*Researched: 2026-03-22*
