# Architecture

## Overview

vepyr is a Python library with a native Rust core, built on top of [Apache DataFusion](https://datafusion.apache.org/) and [Apache Arrow](https://arrow.apache.org/). It wraps two Rust crates from the [biodatageeks](https://github.com/biodatageeks) ecosystem.

![vepyr architecture overview](diagrams/architecture-overview-light.svg#only-light)
![vepyr architecture overview](diagrams/architecture-overview-dark.svg#only-dark)

## Layers

### Python API layer

**Location:** `src/vepyr/__init__.py`

The main public operations are:

- `build_cache()` — download, extract, and convert complete Ensembl VEP offline caches
- `build_cache_entity()` — release-aware targeted conversion of one raw cache entity
- `build_plugin_cache()` — build a per-chromosome plugin cache from a source manifest
- `annotate()` — annotate VCF files against converted caches

This layer handles validation, download orchestration, progress reporting, and conversion of Arrow batches to Polars LazyFrames. It also resolves plugin source manifests: `build_plugin_cache()` materializes `plugins/<name>/<name>.source.toml` from the public [vepyr-plugins](https://github.com/biodatageeks/vepyr-plugins) repository at a git tag via a throwaway `git worktree`, and records the immutable commit the tag resolved to.

### PyO3 FFI layer

**Location:** `src/lib.rs`, `src/annotate.rs`

Bridges Python and Rust via [PyO3](https://pyo3.rs/). Key exports:

- `convert_entity()` — convert a single cache entity to Parquet
- `create_annotator()` — create a `StreamingAnnotator` that yields PyArrow `RecordBatch`es
- `annotate_vcf()` — annotate and write directly to VCF
- `build_plugin_cache()` — drive a plugin cache build and install the result

Errors are normalized to `PyRuntimeError` at this boundary.

Installing a plugin cache is this layer's own responsibility rather than the
engine's. A build writes to a staging tree (`.overwrite-<plugin>.<unique>`) and
only swaps it into place under `plugin/<name>/` once every chromosome has
succeeded, setting the previous cache aside as `.previous-<plugin>.<unique>`. If
an interrupted overwrite leaves the live directory missing, the next build
recovers it from the single set-aside copy. Concurrent builds of the same plugin
therefore never delete or build into each other's tree.

### Rust engine

**Location:** `src/convert.rs`, `src/annotate.rs`

The heavy lifting happens here:

- **Cache conversion** (`convert.rs`): reads Ensembl's Storable/Sereal `.gz` files via `EnsemblCacheTableProvider`, runs DataFusion SQL queries, and writes sorted Parquet files with tuned row groups.
- **Annotation** (`annotate.rs`): registers VCF and cache table providers with DataFusion, builds SQL queries with `annotate_vep()` / `lookup_variants()` UDFs, and streams results as Arrow `RecordBatch`es. `plugin_cache_root` and `plugins` are passed through to the engine, which appends the selected plugins' CSQ fields to the output.
- **Plugin caches**: no vepyr-side module — the build and the lookup both live in `datafusion-bio-function-vep`'s `plugin_cache` module (`builder`, `source_manifest`, `source_verify`, `lookup`). `src/lib.rs` only drives it and installs the result.

### Upstream crates

| Crate | Purpose |
|---|---|
| `datafusion-bio-function-vep` | Annotation UDFs: allele matching, transcript consequence prediction (SO terms, HGVS, protein impact), exposed as DataFusion functions. Its `plugin_cache` module also builds and reads [plugin caches](plugins.md). |
| `datafusion-bio-format-ensembl-cache` | Reads Ensembl VEP offline cache directories into DataFusion `TableProvider`s with Arrow schemas |
| `datafusion-bio-format-vcf` | VCF file reader as DataFusion `TableProvider` |

## Data flow

### Cache building

![Ensembl cache building data flow](diagrams/cache-building-light.svg#only-light)
![Ensembl cache building data flow](diagrams/cache-building-dark.svg#only-dark)

Entity types processed: `Variation`, `Transcript`, `Exon`, `Translation`, `RegulatoryFeature`, `MotifFeature`.

### Plugin cache building

![Plugin cache building data flow](diagrams/plugin-cache-building-light.svg#only-light)
![Plugin cache building data flow](diagrams/plugin-cache-building-dark.svg#only-dark)

Two edges distinguish this from Ensembl cache building. The manifest is not part
of vepyr: it comes from an external repository, pinned to the exact commit its
tag resolved to, so a cache's provenance stays auditable even when the requested
ref was a branch. And the build **reads the variation cache** — plugin rows take
their warm/cold tier from the matching variation record rather than computing
one. A plugin cache therefore cannot be built standalone; the variation entity of
the corresponding Ensembl cache must exist first.

See [Plugins](plugins.md) for the manifest format, the shard schema, how tiering
is calculated, and the per-source table providers.

### Variant annotation

![Variant annotation data flow](diagrams/variant-annotation-light.svg#only-light)
![Variant annotation data flow](diagrams/variant-annotation-dark.svg#only-dark)

The plugin branch is optional: without `plugin_cache_root`, or with an empty
plugin selection, no lookup runs and the output is byte-identical to a run with
no plugin support at all. When plugins are selected, one page-scoped read per
plugin serves a whole annotation buffer — never the whole shard — and the per-transcript probes
(`allele_string` plus the match discriminator) happen inside the consequence
engine as it emits CSQ.

### Memory model

- **Streaming**: annotation results are streamed as Arrow `RecordBatch`es — full datasets are never materialized in memory
- **Cache**: the annotation engine maintains an LRU cache (`cache_size_mb`, default 1 GB) for transcript/variation data
- **Zero-copy**: Python receives PyArrow batches via zero-copy transfer from Rust

## Technology stack

| Component | Technology |
|---|---|
| Language (engine) | Rust 2021 edition |
| Language (API) | Python 3.10+ |
| Python bindings | PyO3 0.28, abi3 stable ABI |
| Query engine | Apache DataFusion 53.0 |
| Data format | Apache Arrow 58 |
| Async runtime | Tokio |
| Interval trees | COITree |
| Cache format | Partitioned, page-indexed Parquet |
| Plugin manifests | TOML, resolved from a git tag to an immutable commit |
| DataFrame | Polars |
| Build system | maturin + uv |
