# vepyr

## What This Is

vepyr is a Python-facing, Rust-powered reimplementation of Ensembl's Variant Effect Predictor for bioinformatics teams. It is designed to build and use Ensembl VEP cache data locally, annotate VCF inputs through a fast native engine, and return results in basic Polars workflows plus VCF-compatible output. The current brownfield codebase already exposes Python APIs for cache building and streaming annotation, and the project is now focused on closing the correctness and performance gap against Ensembl VEP itself.

## Core Value

Produce Ensembl VEP `--everything` results with zero mismatches for the supported scope while being dramatically faster to run.

## Requirements

### Validated

- ✓ Python package surface exists for annotation and cache building — existing
- ✓ Rust native extension powers the execution core through PyO3 — existing
- ✓ Annotation results can already be streamed into Python/Polars workflows — existing
- ✓ Cache conversion pipeline writes parquet-backed local cache data for the engine — existing

### Active

- [ ] Match Ensembl VEP `--everything` output with zero mismatches for `homo_sapiens`, `GRCh38`, and one Ensembl release
- [ ] Deliver `50x+` speedup over Ensembl VEP on the same supported workflow
- [ ] Provide a stable Python library API for bioinformatics teams to build caches and run annotations programmatically
- [ ] Support basic Polars-native consumption plus VCF-compatible output for annotated results

### Out of Scope

- CLI flag-for-flag compatibility with Ensembl VEP in v1 — Python/library parity matters first
- Multi-species, multi-assembly, or multi-release support from day one — the first milestone is intentionally narrowed to one human setup
- Broad UX surfaces beyond Python bioinformatics workflows — the target user is a Python bioinformatics team, not a general end-user product

## Context

This is a brownfield repository with an existing mixed Rust/Python implementation. The Rust core uses PyO3, Arrow, and DataFusion plus `datafusion-bio-*` crates, while the Python package in `src/vepyr/__init__.py` exposes `build_cache()` and `annotate()` entry points. Existing tests already verify importability, basic annotation behavior, and approximate golden-output agreement against Ensembl VEP, which gives the project a working foundation but also shows that correctness parity and performance claims still need to be tightened into explicit product goals.

The intended user is a Python bioinformatics team working locally or in data-processing environments with direct access to VCFs, Ensembl cache data, and reference FASTA files. The main success path is programmatic use in notebooks/scripts, with results flowing into Polars and VCF-centric downstream workflows.

## Constraints

- **Product surface**: Python-first library API — that is the primary user interface for v1
- **Stack**: Existing Rust + PyO3 + DataFusion architecture — new work should build on the current engine rather than replacing it
- **Correctness**: Zero mismatches vs Ensembl VEP `--everything` for the supported scope — this is the short-term quality bar
- **Performance**: `50x+` speedup over Ensembl VEP — performance work must be measured against the reference tool
- **Scope**: `homo_sapiens`, `GRCh38`, one Ensembl release — limits the initial validation surface so parity can be achieved rigorously
- **Outputs**: Basic Polars plus VCF-compatible results — output work should serve those two paths first

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Python library is the primary v1 surface | The target user is a Python bioinformatics team, and strict CLI compatibility is not the first priority | — Pending |
| Initial support is limited to human GRCh38 and one Ensembl release | Narrow scope makes zero-mismatch validation and performance claims achievable | — Pending |
| Project success is defined by both parity and speed | "Fast" alone is not enough; the tool must be trusted as a VEP replacement for the supported workflow | — Pending |
| Existing Rust/DataFusion architecture remains the foundation | The repository already has working cache-build and annotation paths worth evolving instead of rewriting | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-22 after initialization*
