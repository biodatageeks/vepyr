# Pitfalls Research

**Domain:** offline variant annotation engine / Ensembl VEP-compatible Python library
**Researched:** 2026-03-22
**Confidence:** HIGH

## Critical Pitfalls

### Pitfall 1: Declaring parity without release-matched validation

**What goes wrong:**
The tool appears close to VEP on one dataset, but mismatches persist because cache/tool/release assumptions differ.

**Why it happens:**
Ensembl’s own docs state that cache data format and compatibility can differ by release, yet teams often compare across mixed versions.

**How to avoid:**
Pin one supported Ensembl release, require matching cache data, and keep golden fixtures tied to that exact scope.

**Warning signs:**
Mismatch clusters appear in specific fields or transcripts after release changes; “works on one file” but not on another from the same release.

**Phase to address:**
Phase 1: Supported-scope and parity harness definition

---

### Pitfall 2: Underestimating `--everything`

**What goes wrong:**
Core consequence calls look right, but long-tail fields like HGVS, frequencies, transcript metadata, and regulatory context differ or are missing.

**Why it happens:**
`--everything` is a shortcut over many flags; teams often treat it as “default plus some extras” instead of a large compatibility surface.

**How to avoid:**
Break `--everything` into explicit sub-capabilities and verify each field group independently before claiming zero mismatches.

**Warning signs:**
Golden tests pass on consequence names but fail on CSQ column count, field order, HGVS values, or population-frequency fields.

**Phase to address:**
Phase 2: Field-by-field parity implementation

---

### Pitfall 3: Ignoring FASTA-dependent behavior in offline mode

**What goes wrong:**
HGVS-related outputs diverge or silently degrade in cache/offline runs.

**Why it happens:**
Official VEP docs require FASTA for HGVS generation in cache/offline mode, and this is easy to miss during engine-focused work.

**How to avoid:**
Make FASTA presence and supported behavior explicit in the API contract and parity tests for `--everything`.

**Warning signs:**
Mismatch concentration in `HGVSc`, `HGVSp`, or related fields; user confusion around “works without FASTA except for some fields.”

**Phase to address:**
Phase 1: supported workflow contract

---

### Pitfall 4: Measuring speed on a different workflow than VEP

**What goes wrong:**
The benchmark says `50x+`, but it compares a narrower or less costly path than Ensembl VEP `--everything`.

**Why it happens:**
Performance claims are easier to hit when field coverage, I/O mode, or preprocessing differ.

**How to avoid:**
Benchmark the exact supported workflow: same input, same release, same reference data, same requested outputs.

**Warning signs:**
Benchmarks omit VCF emit time, HGVS generation, or cache-preparation costs while marketing still compares against full VEP runs.

**Phase to address:**
Phase 3: benchmark harness and acceptance criteria

---

### Pitfall 5: Treating VCF/CSQ formatting as an afterthought

**What goes wrong:**
Users cannot slot the output into VEP-based downstream tools even when biological consequences are mostly correct.

**Why it happens:**
Teams focus on the engine and defer output contract fidelity.

**How to avoid:**
Make VCF `CSQ` field order, delimiters, replacement semantics, and header metadata part of the parity surface from the start.

**Warning signs:**
Downstream tools reject output, existing `CSQ` handling differs, or field-order assumptions break comparison scripts.

**Phase to address:**
Phase 2: output-contract parity

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| “Near-VEP” claims without field-by-field evidence | Faster iteration | Erodes trust and creates hard-to-debug drift | Never for the supported v1 workflow |
| Hardcoding one release path without explicit metadata | Simpler code path | Hidden assumptions block later expansion | Acceptable only if the release pin is made explicit in docs/tests |
| Benchmarking only the happy path | Easy performance wins | Regression risk and misleading claims | Never for user-facing speed promises |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Ensembl cache data | Mixing cache and tool releases | Enforce release-matched inputs and compatibility checks |
| FASTA-backed HGVS | Making FASTA optional in docs but required in behavior | Surface FASTA requirements explicitly for supported parity modes |
| VCF output | Emitting consequence data without exact `CSQ` semantics | Match header, field order, delimiters, and overwrite/keep semantics intentionally |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Eager materialization too early | Good engine time, poor end-to-end runtime | Keep output lazy/streamed until the final sink | Large VCFs and wide `--everything` outputs |
| Optimizing cache conversion but not annotation output | Benchmarks look great internally, user runtime still disappoints | Measure full workflow including output production | As soon as users export or compare full results |
| Query-plan drift from parity rules | Fast output with wrong answers | Treat correctness constraints as part of the plan design | Immediately once edge cases enter the dataset |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Downloading remote cache/archive data without verification | Supply-chain or corrupted-data risk | Prefer trusted Ensembl sources, add checksum/path validation |
| Treating sensitive input VCFs as safe for remote annotation | Data leakage | Keep the product centered on local cache/offline execution |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Ambiguous supported scope | Users assume any species/release works and get silent mismatches | State supported release/species/assembly explicitly in API/docs |
| “Polars-first” without VCF compatibility | Existing VEP users cannot migrate cleanly | Support both basic Polars use and VCF-compatible output in v1 |
| Error messages that expose engine internals instead of workflow fixes | Bioinformatics users cannot self-correct quickly | Map failures to concrete actions: cache version, FASTA missing, unsupported scope |

## "Looks Done But Isn't" Checklist

- [ ] **Parity:** Often missing field-order or delimiter matching — verify full `CSQ` header and payload equivalence
- [ ] **`--everything`:** Often missing one or more sub-flags — verify per-field-group coverage, not just consequence names
- [ ] **Performance:** Often missing apples-to-apples benchmarks — verify same workflow against Ensembl VEP
- [ ] **Scope contract:** Often missing explicit release/species/assembly guarantees — verify docs and runtime checks agree

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Mixed-release mismatches | MEDIUM | repin cache/tool versions, regenerate fixtures, rerun golden suite |
| `--everything` field drift | HIGH | isolate field group, add targeted fixture, patch field mapping/logic, rerun full comparison |
| Misleading speed claim | MEDIUM | rebuild benchmark harness with matched workflow and publish updated numbers |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Release mismatch | Phase 1 | golden suite is pinned to one supported release and rejects mixed inputs |
| `--everything` undercoverage | Phase 2 | per-field-group parity tests plus full VCF comparison |
| FASTA-dependent HGVS drift | Phase 1 | explicit runtime checks and HGVS fixture coverage |
| Benchmark mismatch | Phase 3 | benchmark spec names exact VEP command/config being compared |
| CSQ/output drift | Phase 2 | downstream-compatible VCF fixtures and header/payload equivalence checks |

## Sources

- https://www.ensembl.org/info/docs/tools/vep/script/vep_cache.html — release compatibility, offline mode, FASTA guidance
- https://www.ensembl.org/info/docs/tools/vep/script/vep_options.html — `--everything` definition and performance guidance
- https://www.ensembl.org/info/docs/tools/vep/vep_formats.html — VCF/CSQ output contract
- Existing project tests and codebase map — current implementation shape and likely failure surfaces

---
*Pitfalls research for: offline variant annotation engine / Ensembl VEP-compatible Python library*
*Researched: 2026-03-22*
