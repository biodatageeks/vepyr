# Roadmap: vepyr

## Overview

vepyr will move from an already-functional Rust/Python annotation prototype to a trusted Ensembl VEP replacement for one tightly defined workflow. The roadmap prioritizes explicit supported scope, deterministic cache and FASTA contracts, field-by-field `--everything` parity, VCF/Polars output stability, and only then benchmarked `50x+` performance claims against Ensembl VEP.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Supported Workflow Contract** - Freeze the exact release/species/assembly/FASTA contract and reject unsupported inputs cleanly
- [ ] **Phase 2: Cache Pipeline Hardening** - Make cache preparation and cache reuse deterministic for the supported workflow
- [ ] **Phase 3: `--everything` Annotation Parity** - Close core annotation-field gaps for the supported workflow
- [ ] **Phase 4: Python Output Surface** - Make Polars and VCF-compatible outputs stable and user-facing
- [ ] **Phase 5: Golden Verification and Performance** - Prove zero mismatches and `50x+` speed on the benchmark workflow

## Phase Details

### Phase 1: Supported Workflow Contract
**Goal**: Define and enforce the one supported v1 workflow so parity work happens against a stable target
**Depends on**: Nothing (first phase)
**Requirements**: [SCOPE-01, SCOPE-02, CACHE-03, API-02]
**UI hint**: no
**Success Criteria** (what must be TRUE):
  1. User can identify the supported species, assembly, and Ensembl release from the product contract and runtime metadata
  2. User gets a clear error when cache, FASTA, release, or workflow inputs fall outside the supported v1 scope
  3. Prepared cache artifacts expose enough metadata to validate release/species/assembly compatibility before annotation
  4. The supported comparison workflow is explicit enough that later golden and benchmark phases can reuse it without reinterpretation
**Plans**: 3 plans

Plans:
- [ ] 01-01: Create the executable supported-workflow contract and dedicated Phase 1 validation scaffold
- [ ] 01-02: Wire strict Python-side validation for unsupported workflow, manifest, and FASTA combinations
- [ ] 01-03: Persist workflow manifest metadata and expose the supported contract in runtime/docs artifacts

### Phase 2: Cache Pipeline Hardening
**Goal**: Make cache build and cache reuse reliable inputs to parity work
**Depends on**: Phase 1
**Requirements**: [CACHE-01, CACHE-02]
**UI hint**: no
**Success Criteria** (what must be TRUE):
  1. User can build the supported cache format from Python without manual internal steps
  2. User can reuse a previously prepared cache without unnecessary rebuilds
  3. Cache preparation behaves deterministically enough for parity and benchmark workflows to rely on it
**Plans**: 2 plans

Plans:
- [ ] 02-01: Harden cache-build flow and release-aware cache discovery
- [ ] 02-02: Verify reuse/idempotence behavior for prepared caches in the supported workflow

### Phase 3: `--everything` Annotation Parity
**Goal**: Match Ensembl VEP `--everything` field behavior for the supported workflow
**Depends on**: Phase 2
**Requirements**: [ANNO-01, ANNO-02, ANNO-04, ANNO-05, ANNO-06]
**UI hint**: no
**Success Criteria** (what must be TRUE):
  1. User can annotate a VCF through the Python API with the supported cache and FASTA-backed workflow
  2. User receives transcript, gene, regulatory, HGVS, and population/co-located fields required for the supported `--everything` workflow
  3. Annotation behavior is decomposed into field groups that can be verified independently against Ensembl VEP
  4. Remaining mismatches, if any, are narrow enough to be treated as final verification issues rather than unknown behavior gaps
**Plans**: 3 plans

Plans:
- [ ] 03-01: Build a field-group parity matrix for the supported `--everything` workflow
- [ ] 03-02: Close annotation gaps in transcript/gene/regulatory/HGVS behavior
- [ ] 03-03: Close annotation gaps in population and co-located variant fields

### Phase 4: Python Output Surface
**Goal**: Ship stable user-facing outputs for analysis and VCF-compatible downstream consumption
**Depends on**: Phase 3
**Requirements**: [OUT-01, OUT-02, OUT-03, API-01, API-03]
**UI hint**: no
**Success Criteria** (what must be TRUE):
  1. User can consume results from Python in a stable Polars-compatible lazy workflow
  2. User can emit VCF-compatible output with stable `CSQ` semantics and documented field ordering
  3. User can rerun the same annotation workflow programmatically without rebuilding process state manually
**Plans**: 3 plans

Plans:
- [ ] 04-01: Stabilize the Python-facing API and rerunnable annotation flow
- [ ] 04-02: Stabilize Polars-facing output semantics and schema expectations
- [ ] 04-03: Stabilize VCF-compatible `CSQ` header and payload behavior

### Phase 5: Golden Verification and Performance
**Goal**: Convert “looks right and fast” into verified product claims
**Depends on**: Phase 4
**Requirements**: [ANNO-03, PERF-01, PERF-02, PERF-03]
**UI hint**: no
**Success Criteria** (what must be TRUE):
  1. User can run automated golden verification that reports any mismatch against Ensembl VEP for the supported workflow
  2. User can run a benchmark harness that compares vepyr and Ensembl VEP on the same input and configuration
  3. The supported benchmark demonstrates at least `50x` speedup before the v1 claim is considered complete
  4. Zero-mismatch and performance claims are backed by repeatable project artifacts rather than ad hoc runs
**Plans**: 3 plans

Plans:
- [ ] 05-01: Finalize golden-comparison harness and mismatch reporting
- [ ] 05-02: Finalize benchmark harness and apples-to-apples VEP comparison
- [ ] 05-03: Close final mismatch/performance gaps and publish acceptance thresholds

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Supported Workflow Contract | 0/3 | Not started | - |
| 2. Cache Pipeline Hardening | 0/2 | Not started | - |
| 3. `--everything` Annotation Parity | 0/3 | Not started | - |
| 4. Python Output Surface | 0/3 | Not started | - |
| 5. Golden Verification and Performance | 0/3 | Not started | - |
