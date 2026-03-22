# Project Research Summary

**Project:** vepyr
**Domain:** offline variant annotation engine / Ensembl VEP-compatible Python library
**Researched:** 2026-03-22
**Confidence:** HIGH

## Executive Summary

vepyr sits in a domain where the reference product is already clear: Ensembl VEP defines the behavioral contract, and its official documentation strongly recommends local cache or offline execution for performance-sensitive usage. That means the project does not need to invent a new workflow; it needs to reproduce the trusted VEP workflow for one tightly scoped human release while replacing the slow path with a faster Rust/DataFusion engine.

Research points to a disciplined product strategy: keep the Python API first, keep the local cache/offline workflow first, and treat VCF `CSQ` semantics plus `--everything` field coverage as part of correctness, not just formatting. The biggest risk is false confidence: it is easy to get “mostly right” consequence output or “fast on some path” benchmarks, but the project goal requires exact parity and apples-to-apples performance proof.

## Key Findings

### Recommended Stack

The existing stack is directionally correct and should be preserved: Rust for the hot path, PyO3 for the Python boundary, Arrow/DataFusion for local query execution, and Polars `LazyFrame` as the primary analysis-facing result surface. This is consistent with the project’s explicit `50x+` speed target and with official documentation around lazy/query-oriented execution on both the DataFusion and Polars sides.

**Core technologies:**
- Rust: native execution engine — best fit for the speed target
- PyO3: Python extension boundary — keeps the product Python-first without giving up native speed
- Arrow/DataFusion: local query execution and typed batch interchange — a strong fit for VCF/cache joins
- Polars LazyFrame: primary analysis surface — aligns with the target user and high-performance Python workflows

### Expected Features

The domain table stakes are clear: release-matched local cache support, offline VCF annotation, VCF-compatible `CSQ` output, and complete support for the supported scope of Ensembl VEP `--everything`. For this project specifically, the differentiators are benchmarked speed, Polars-native consumption, and a rigorous parity harness.

**Must have (table stakes):**
- Local cache + FASTA-backed offline annotation for one supported human workflow
- VCF-compatible `CSQ` output semantics
- `--everything`-equivalent field coverage for the supported release
- Stable Python API for bioinformatics users

**Should have (competitive):**
- `50x+` benchmarked speedup vs Ensembl VEP
- Polars-native lazy output
- Fast cache conversion into query-optimized local data

**Defer (v2+):**
- Multi-species / multi-release breadth
- Plugin/custom annotation parity
- Full CLI compatibility

### Architecture Approach

The recommended architecture is a thin Python facade over a release-pinned native annotation pipeline. Python should own validation, workflow ergonomics, and output adapters; Rust/DataFusion should own cache scanning, consequence computation, and streaming. Verification must be a first-class component of the architecture, not a trailing concern.

**Major components:**
1. Python API layer — user-facing `annotate` / cache workflows and error handling
2. Native engine layer — query planning, cache/VCF scanning, and consequence generation
3. Output contract layer — Polars-facing batches plus VCF/`CSQ` compatibility
4. Verification harness — golden parity and benchmark acceptance gates

### Critical Pitfalls

1. **Mixed-release validation** — pin one release and enforce release-matched cache/tool assumptions
2. **Underestimating `--everything`** — break it into explicit field groups and verify all of them
3. **Ignoring FASTA requirements** — make FASTA mandatory for the supported HGVS-heavy workflow
4. **Benchmarking the wrong workflow** — compare the same inputs, outputs, and flags against Ensembl VEP
5. **Deferring VCF/CSQ fidelity** — treat output contract fidelity as correctness work

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Supported Workflow Contract
**Rationale:** Everything else depends on freezing one exact comparison target.
**Delivers:** supported release/species/assembly definition, cache/FASTA contract, parity fixture baseline
**Addresses:** release-matched cache support, explicit offline workflow
**Avoids:** mixed-release and missing-FASTA pitfalls

### Phase 2: `--everything` Output Parity
**Rationale:** The product’s short-term success criteria are correctness-first.
**Delivers:** field-by-field parity for the supported workflow, VCF/`CSQ` contract fidelity
**Uses:** existing Rust/DataFusion engine plus explicit output verification
**Implements:** output contract and parity component

### Phase 3: Benchmarkable Performance
**Rationale:** Only meaningful once correctness is trusted.
**Delivers:** reproducible benchmark harness, bottleneck fixes, `50x+` acceptance reporting
**Addresses:** benchmark-mismatch and partial-workflow speed-claim pitfalls

### Phase 4: Python Product Hardening
**Rationale:** Once parity and performance are real, package ergonomics matter more.
**Delivers:** API polish, clearer errors, stable Polars/VCF flows, better cache lifecycle ergonomics

### Phase Ordering Rationale

- Parity claims require a fixed supported workflow before feature expansion.
- Output-contract work must precede performance marketing, because “fast wrong answers” are not product value.
- Benchmarking should follow correctness so optimization does not outrun the specification.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2:** field-level `--everything` parity details and edge-case output semantics
- **Phase 3:** benchmark methodology and representative workload selection

Phases with standard patterns (skip research-phase):
- **Phase 4:** Python package hardening and API ergonomics are standard compared with the parity-specific work

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Strong alignment between official docs, current codebase, and project goal |
| Features | HIGH | User goal and Ensembl docs make the table stakes explicit |
| Architecture | HIGH | Existing codebase already follows the right broad pattern |
| Pitfalls | HIGH | The main failure modes are directly implied by official VEP contracts and the replacement-tool goal |

**Overall confidence:** HIGH

### Gaps to Address

- Exact field-by-field parity status for each `--everything` output group: resolve during phase planning with a formal matrix
- Final benchmark dataset and command definition for the `50x+` claim: resolve during performance planning

## Sources

### Primary (HIGH confidence)
- https://www.ensembl.org/info/docs/tools/vep/script/vep_cache.html — cache/offline workflow, FASTA requirements, release matching
- https://www.ensembl.org/info/docs/tools/vep/script/vep_options.html — `--everything` scope and recommended performance mode
- https://www.ensembl.org/info/docs/tools/vep/vep_formats.html — VCF/`CSQ` output contract
- https://datafusion.apache.org/python/user-guide/dataframe/index.html — lazy logical-plan model for DataFusion
- https://docs.pola.rs/api/python/stable/reference/lazyframe/ — LazyFrame API reference
- https://docs.pola.rs/user-guide/lazy/ — Polars lazy execution guidance

### Secondary (MEDIUM confidence)
- Existing `vepyr` codebase and `.planning/codebase/` map — confirms feasibility and current shape

---
*Research completed: 2026-03-22*
*Ready for roadmap: yes*
