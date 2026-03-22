# Requirements: vepyr

**Defined:** 2026-03-22
**Core Value:** Produce Ensembl VEP `--everything` results with zero mismatches for the supported scope while being dramatically faster to run.

## v1 Requirements

### Supported Scope

- [ ] **SCOPE-01**: User can run vepyr against one explicitly documented supported workflow: `homo_sapiens`, `GRCh38`, and one pinned Ensembl release
- [ ] **SCOPE-02**: User gets a clear error when cache, assembly, FASTA, or release inputs do not match the supported workflow

### Cache

- [ ] **CACHE-01**: User can build or prepare the supported Ensembl cache into the local vepyr-optimized format from Python
- [ ] **CACHE-02**: User can reuse an already prepared local cache without rebuilding it on every run
- [ ] **CACHE-03**: User can tell which release/assembly/species a prepared cache belongs to before annotation starts

### Annotation Parity

- [ ] **ANNO-01**: User can annotate a VCF file with the supported local cache through the Python API
- [ ] **ANNO-02**: User can run the supported `--everything`-equivalent annotation workflow with required FASTA-backed fields enabled
- [ ] **ANNO-03**: User gets consequence annotations that match Ensembl VEP with zero mismatches on the supported golden comparison workflow
- [ ] **ANNO-04**: User gets transcript-, gene-, and regulatory-related fields needed for the supported `--everything` workflow
- [ ] **ANNO-05**: User gets HGVS-related fields that match Ensembl VEP for the supported workflow when reference FASTA is provided
- [ ] **ANNO-06**: User gets population/co-located variant fields needed for the supported `--everything` workflow

### Outputs

- [ ] **OUT-01**: User can consume annotation results as a basic Polars-compatible lazy workflow from Python
- [ ] **OUT-02**: User can emit VCF-compatible output that preserves Ensembl VEP-style `CSQ` semantics for the supported workflow
- [ ] **OUT-03**: User gets output headers and field ordering that are stable and documented for the supported workflow

### Python API

- [ ] **API-01**: User can run cache build and annotation through a stable Python package API without invoking the Ensembl VEP CLI
- [ ] **API-02**: User gets actionable Python-side validation errors for unsupported scope, missing FASTA, missing cache, or invalid workflow configuration
- [ ] **API-03**: User can rerun the same annotation workflow programmatically without rebuilding the Python process state manually

### Performance and Verification

- [ ] **PERF-01**: User can benchmark the supported vepyr workflow against Ensembl VEP on the same input data and configuration
- [ ] **PERF-02**: User can verify that the supported workflow achieves at least `50x` speedup over Ensembl VEP on the project benchmark
- [ ] **PERF-03**: User can run automated golden verification that reports any mismatch against Ensembl VEP for the supported workflow before release claims are made

## v2 Requirements

### Broader Compatibility

- **COMP-01**: User can run supported workflows for additional Ensembl releases
- **COMP-02**: User can run supported workflows for additional species and assemblies
- **COMP-03**: User can use a CLI surface for common VEP-style workflows

### Extended Ecosystem

- **EXT-01**: User can use plugin/custom-annotation compatibility paths where they are important for migration
- **EXT-02**: User can export to additional dataframe/output ecosystems beyond the basic Polars + VCF path

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full flag-for-flag Ensembl VEP CLI compatibility in v1 | Python API parity and correctness for one workflow matter more first |
| Multi-species / multi-release support in v1 | Would expand the parity matrix before the core workflow is solved |
| Plugin/custom annotation compatibility in v1 | Too much compatibility surface before base `--everything` parity is trusted |
| Non-Polars-first dataframe ergonomics as a primary goal | Basic Polars + VCF is enough for the first target users |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| SCOPE-01 | TBD | Pending |
| SCOPE-02 | TBD | Pending |
| CACHE-01 | TBD | Pending |
| CACHE-02 | TBD | Pending |
| CACHE-03 | TBD | Pending |
| ANNO-01 | TBD | Pending |
| ANNO-02 | TBD | Pending |
| ANNO-03 | TBD | Pending |
| ANNO-04 | TBD | Pending |
| ANNO-05 | TBD | Pending |
| ANNO-06 | TBD | Pending |
| OUT-01 | TBD | Pending |
| OUT-02 | TBD | Pending |
| OUT-03 | TBD | Pending |
| API-01 | TBD | Pending |
| API-02 | TBD | Pending |
| API-03 | TBD | Pending |
| PERF-01 | TBD | Pending |
| PERF-02 | TBD | Pending |
| PERF-03 | TBD | Pending |

**Coverage:**
- v1 requirements: 20 total
- Mapped to phases: 0
- Unmapped: 20 ⚠️

---
*Requirements defined: 2026-03-22*
*Last updated: 2026-03-22 after initialization*
