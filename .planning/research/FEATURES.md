# Feature Research

**Domain:** offline variant annotation engine / Ensembl VEP-compatible Python library
**Researched:** 2026-03-22
**Confidence:** HIGH

## Feature Landscape

### Table Stakes (Users Expect These)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Release-matched local cache support | Official VEP guidance centers cache-based annotation for serious use | HIGH | Must respect release-specific cache structure and compatibility |
| Offline annotation against VCF input | This is the standard high-performance/private workflow in VEP docs | HIGH | Needs local FASTA for HGVS-heavy modes |
| VCF-compatible output with CSQ semantics | VCF + `CSQ` is the standard interchange format for VEP users | HIGH | Field ordering and delimiter semantics must match VEP behavior closely |
| `--everything`-equivalent field coverage for supported scope | User explicitly wants zero mismatches vs this mode | HIGH | Includes HGVS, frequencies, symbols, transcript metadata, regulatory context, etc. |
| Programmatic Python API | Target user is a Python bioinformatics team | MEDIUM | Python API matters more than CLI parity in v1 |
| Deterministic release/species/assembly support | Annotation correctness depends on explicit dataset context | MEDIUM | v1 can narrow to `homo_sapiens` / `GRCh38` / one release |

### Differentiators (Competitive Advantage)

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| `50x+` speedup vs Ensembl VEP | Makes migration worthwhile instead of merely interesting | HIGH | Must be benchmarked on the same workflow and dataset |
| Polars-native lazy output | Better fit for high-throughput Python analysis than line-oriented CLI output alone | MEDIUM | Already aligned with current architecture |
| Fast local cache conversion into query-optimized parquet | Improves end-to-end throughput, not just annotation kernel speed | HIGH | Important because VEP cache preparation is part of the real workflow |
| Tight golden-testing against known VEP output | Builds trust for a replacement tool | MEDIUM | Becomes a product differentiator once automated and release-pinned |

### Anti-Features (Commonly Requested, Often Problematic)

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Full flag-for-flag CLI compatibility in v1 | Feels like the safest migration story | Bloats scope before parity is proven on the core workflow | Python-first API with VCF-compatible output, then selectively add CLI parity later |
| Multi-species / multi-release from day one | Broadens appeal | Multiplies parity matrix and slows down zero-mismatch work | Start with one human release and expand after parity is verified |
| “Fast enough” without benchmark discipline | Easy to claim during development | Won’t support the `50x+` promise or catch regressions | Add explicit benchmark fixtures and acceptance thresholds |
| Custom/plugin ecosystem parity immediately | Power users expect it eventually | Plugins/custom annotations create a huge compatibility surface | Defer until base `--everything` parity is reliable |

## Feature Dependencies

```text
Release-matched cache support
    └──requires──> cache conversion / cache loading
                           └──requires──> version-compatibility validation

VCF-compatible CSQ output
    └──requires──> field parity for --everything
                           └──requires──> FASTA-backed HGVS support

50x+ benchmark claim
    └──requires──> reproducible benchmark harness
                           └──requires──> stable supported workflow scope

Polars-native API ──enhances──> core annotation workflow

Early CLI parity ──conflicts──> ruthless v1 scope control
```

### Dependency Notes

- **Field parity requires release-matched cache support:** mismatches often come from release/schema drift rather than algorithm alone.
- **`--everything` parity requires FASTA-backed HGVS support:** official VEP docs require FASTA for HGVS generation in cache/offline mode.
- **Speed claims require a benchmark harness:** otherwise “fast” remains anecdotal and non-regression-proof.
- **CLI parity conflicts with scope control:** it expands UX and flag surface before correctness is locked down.

## MVP Definition

### Launch With (v1)

- [ ] Python API to annotate a VCF using local cache + reference FASTA for one supported human release
- [ ] VCF-compatible consequence output with `CSQ` semantics for the supported workflow
- [ ] Polars-oriented programmatic result access for downstream analysis
- [ ] Golden-test harness measuring mismatches against Ensembl VEP `--everything`
- [ ] Benchmark harness measuring runtime against Ensembl VEP on the same workflow

### Add After Validation (v1.x)

- [ ] Convenience output adapters beyond the basic Polars + VCF path — add when core users ask for them
- [ ] Better ergonomics around cache installation/conversion and release management — add after correctness/perf baseline is stable
- [ ] Selective CLI affordances for common workflows — add once the API and output model are stable

### Future Consideration (v2+)

- [ ] Additional species / assemblies / releases — defer until the first parity matrix is solved cleanly
- [ ] Plugin/custom-annotation compatibility layers — defer until base annotation parity is trusted
- [ ] Full CLI migration path — defer until the Python-first product is validated

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| `--everything` parity for one supported workflow | HIGH | HIGH | P1 |
| Local cache + FASTA annotation pipeline | HIGH | HIGH | P1 |
| VCF-compatible `CSQ` output | HIGH | HIGH | P1 |
| Python/Polars API | HIGH | MEDIUM | P1 |
| Benchmarking for `50x+` claim | HIGH | MEDIUM | P1 |
| CLI compatibility layer | MEDIUM | HIGH | P2 |
| Broader species/release coverage | MEDIUM | HIGH | P2 |
| Plugin/custom annotation parity | MEDIUM | HIGH | P3 |

**Priority key:**
- P1: Must have for launch
- P2: Should have, add when possible
- P3: Nice to have, future consideration

## Competitor Feature Analysis

| Feature | Ensembl VEP | Typical Python analysis stack | Our Approach |
|---------|--------------|------------------------------|--------------|
| Annotation correctness | Reference implementation for target parity | Usually consumes, not produces, VEP-style annotations | Match VEP output for the supported workflow |
| Output modes | Default/tab/VCF/JSON | Usually DataFrame-first | Keep VCF compatibility and expose Polars-native programmatic access |
| Execution model | Cache/offline strongly recommended for performance | Local table/dataframe workflows | Rebuild the same local workflow with a faster engine |
| Extensibility | Many flags, plugins, custom annotations | Flexible but fragmented | Delay ecosystem breadth until correctness and speed are established |

## Sources

- Ensembl VEP options and formats docs — official definition of expected workflow and `--everything` scope
- Ensembl VEP cache docs — official definition of local/offline cache usage
- Polars lazy docs — guidance for the Python-facing result surface
- Existing brownfield codebase and tests — evidence of current product surface and gaps

---
*Feature research for: offline variant annotation engine / Ensembl VEP-compatible Python library*
*Researched: 2026-03-22*
