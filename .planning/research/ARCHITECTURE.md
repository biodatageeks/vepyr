# Architecture Research

**Domain:** offline variant annotation engine / Ensembl VEP-compatible Python library
**Researched:** 2026-03-22
**Confidence:** HIGH

## Standard Architecture

### System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                      Python Product Layer                  │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ annotate()   │  │ build_cache()│  │ VCF/output glue  │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                 │                    │            │
├─────────┴─────────────────┴────────────────────┴────────────┤
│                    Native Execution Boundary                │
├─────────────────────────────────────────────────────────────┤
│                 PyO3 extension / Arrow bridge              │
├─────────────────────────────────────────────────────────────┤
│                     Query + Annotation Engine              │
│   ┌────────────┐  ┌────────────┐  ┌────────────────────┐   │
│   │ VCF scan   │  │ Cache scan │  │ Consequence logic  │   │
│   └────────────┘  └────────────┘  └────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                       Local Data Layer                      │
│   ┌──────────┐  ┌──────────────┐  ┌────────────────────┐   │
│   │ VCF file │  │ FASTA files  │  │ Release cache data │   │
│   └──────────┘  └──────────────┘  └────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| Python API surface | User-facing ergonomics, validation, workflow orchestration | Thin Python package exposing stable functions/classes |
| Native execution boundary | Fast transfer between Python and query engine | PyO3 + Arrow/PyArrow-compatible batches |
| Annotation engine | Join/compute/filter logic for consequence generation | Rust execution engine backed by Arrow/DataFusion-style plans |
| Cache management | Convert/download/load release-matched cache data | Local filesystem layout with release/species/assembly awareness |
| Verification harness | Prove parity and performance | Golden-output tests plus benchmark datasets |

## Recommended Project Structure

```text
src/
├── vepyr/                 # Python-facing package surface
│   ├── __init__.py        # stable public API
│   ├── output.py          # VCF / Polars formatting helpers
│   └── cache.py           # cache install / validation orchestration
├── lib.rs                 # PyO3 module boundary
├── annotate.rs            # annotation query flow
├── convert.rs             # cache conversion flow
├── parity/                # release-specific parity helpers and field maps
└── benchmark/             # benchmark harness and fixtures
tests/
├── golden/                # exact/mismatch validation
├── benchmarks/            # perf acceptance tests
└── integration/           # Python API behavior
```

### Structure Rationale

- **Python package layer:** keeps the product surface stable even if native internals move.
- **Native engine layer:** isolates hot-path changes from Python ergonomics.
- **Parity/benchmark folders:** makes correctness and performance first-class architectural concerns instead of afterthoughts.

## Architectural Patterns

### Pattern 1: Release-pinned annotation pipeline

**What:** Treat supported release/species/assembly as explicit configuration, not ambient context.
**When to use:** Always for parity-critical VEP-compatible runs.
**Trade-offs:** Less flexible early on, but dramatically easier to verify.

### Pattern 2: Thin Python facade over native batches

**What:** Keep Python responsible for validation and user ergonomics, but not heavy annotation logic.
**When to use:** For the primary product API.
**Trade-offs:** Requires careful FFI/schema design, but preserves performance.

### Pattern 3: Golden-first verification architecture

**What:** Design outputs, field mappings, and benchmark flows around reproducible comparison to VEP.
**When to use:** From the start for any replacement-style product.
**Trade-offs:** Slower feature expansion, but much higher trust.

## Data Flow

### Request Flow

```text
Python user call
    ↓
argument validation
    ↓
release/cache/fasta resolution
    ↓
native query plan creation
    ↓
VCF + cache scan + consequence computation
    ↓
Arrow batches
    ↓
Polars / VCF-compatible output
```

### State Management

```text
Release config + local file paths
    ↓
execution options / field selection
    ↓
native runtime
    ↓
streamed record batches / emitted VCF
```

### Key Data Flows

1. **Cache-build flow:** raw Ensembl cache input → local optimized parquet/indexed representation.
2. **Annotation flow:** VCF + release-matched cache + optional FASTA → transcript/regulatory consequences.
3. **Verification flow:** vepyr output + VEP golden output + benchmark timings → parity/performance gate.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| single analyst / local jobs | current monolithic library architecture is fine |
| team-scale repeated runs | optimize cache reuse, benchmark regressions, and packaging/distribution |
| large cohort pipelines | focus on partitioning, streaming, and I/O layout before expanding product surface |

### Scaling Priorities

1. **First bottleneck:** cache/VCF I/O layout and query plan efficiency.
2. **Second bottleneck:** Python/native handoff and materialization of large result sets.

## Anti-Patterns

### Anti-Pattern 1: Treating output formatting as separate from parity work

**What people do:** Match “core” consequences first and leave VCF/CSQ semantics for later.
**Why it's wrong:** For VEP users, formatting and field semantics are part of correctness.
**Do this instead:** Make VCF/CSQ parity part of the primary architecture and tests.

### Anti-Pattern 2: Expanding species/features before locking one reference workflow

**What people do:** Add many scopes early to feel more complete.
**Why it's wrong:** It multiplies mismatch sources and makes debugging parity much harder.
**Do this instead:** Solve one supported release/workflow completely, then generalize.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Ensembl cache/FASTA distribution | local downloaded artifacts | release-matching is mandatory for parity |
| Ensembl VEP reference tool | benchmark/golden comparison target | treat as the truth source for supported scope |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Python API ↔ Rust engine | PyO3 + Arrow batches | keep payload typed and minimal |
| Cache conversion ↔ annotation engine | filesystem + release metadata | same release assumptions must be shared |
| Verification harness ↔ product output | VCF/Polars artifacts | parity/performance become acceptance gates |

## Sources

- Ensembl VEP cache/options/formats docs — official workflow and output contract
- DataFusion docs — lazy logical-plan execution model
- Polars docs — lazy output and schema expectations
- Existing vepyr brownfield architecture — confirms this pattern is already the right base

---
*Architecture research for: offline variant annotation engine / Ensembl VEP-compatible Python library*
*Researched: 2026-03-22*
