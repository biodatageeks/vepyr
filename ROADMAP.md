# vepyr Roadmap

> Last updated: 2025-03-24
> Baseline: vepyr with `datafusion-bio-function-vep` @ rev `2bb681d`, Ensembl VEP 115, GRCh38

## Current State

vepyr achieves **100% field-level correctness** against Ensembl VEP 115 `--everything` output across all 80 CSQ fields (verified on VCF output path). The annotation engine covers consequence prediction, HGVS notation, SIFT/PolyPhen, all population frequencies (1KG, gnomAD exome/genome), co-located variant lookup, and the full identifier set (SYMBOL, CANONICAL, MANE, TSL, APPRIS, UniProt, CCDS, DOMAINS, etc.).

**Architecture:** Python API → PyO3 → DataFusion SQL (`annotate_vep()` UDF) → Arrow streaming → Polars LazyFrame / VCF output.

### Known Issues

| Issue | Tracker | Impact |
|-------|---------|--------|
| DataFrame single-pick prefers transcript without gene symbol when overlapping genes at same locus | [biodatageeks/datafusion-bio-functions#54](https://github.com/biodatageeks/datafusion-bio-functions/issues/54) | Low — VCF output is correct; affects LazyFrame SYMBOL for ~6% of lncRNA-region variants |

---

## Feature Gap vs Ensembl VEP

### Implemented (confirmed working + tested)

| VEP Flag | vepyr Parameter | Notes |
|----------|----------------|-------|
| `--everything` | `everything=True` | All 80 CSQ fields, 100% match |
| `--hgvs` / `--hgvsc` / `--hgvsp` | `hgvs`, `hgvsc`, `hgvsp` | Full HGVS notation |
| `--shift_hgvs` | `shift_hgvs` | 3' shifting |
| `--no_escape` | `no_escape` | URI-escape control |
| `--hgvsp_use_prediction` | `hgvsp_use_prediction` | Predicted protein sequence |
| `--fasta` | `reference_fasta` | Required for HGVS |
| `--check_existing` | `check_existing` | rsIDs, CLIN_SIG, SOMATIC, PHENO |
| `--af` | `af` | Global 1KG AF |
| `--af_1kg` | `af_1kg` | Continental 1KG populations (AFR, AMR, EAS, EUR, SAS) |
| `--af_gnomad` / `--af_gnomade` | `af_gnomade` | gnomAD exome + 9 population AFs |
| `--af_gnomadg` | `af_gnomadg` | gnomAD genome + 11 population AFs |
| `--max_af` | `max_af` | MAX_AF + MAX_AF_POPS |
| `--pubmed` | `pubmed` | PubMed IDs for co-located variants |
| `--distance` | `distance` | Upstream/downstream bp; supports `int` or `(up, down)` tuple |
| `--merged` | `merged` | Merged Ensembl + RefSeq cache |
| `--failed` | `failed` | Failed variant inclusion threshold |
| `--cache` / `--offline` | implicit | Always offline — parquet cache by design |
| `--vcf` output | `output_vcf` | VCF with CSQ INFO field, bgzf/gzip/plain |
| `--symbol`, `--biotype`, `--canonical`, `--numbers`, `--protein`, `--sift`, `--polyphen`, `--regulatory`, `--ccds`, `--uniprot`, `--mane`, `--tsl`, `--appris`, `--variant_class`, `--gene_phenotype`, `--domains`, `--mirna` | via `everything=True` | All included in everything mode (not individually toggleable) |
| `remove_hgvsp_version` | `remove_hgvsp_version` | Version control on HGVSp transcript IDs |
| Streaming output | default return | Polars LazyFrame with projection/filter pushdown |
| Fjall KV backend | `use_fjall` | Embedded KV store alternative to parquet |
| Progress callbacks | `on_batch_written` | Notebook-friendly batch progress |
| Compression control | `compression` | bgzf, gzip, plain, auto-detect |

### Not Implemented

Organized by priority tier. Priority is based on how many real-world VEP workflows are blocked without the feature.

---

## P0 — Transcript Selection & Filtering

These are the **most impactful missing features**. Without pick/filter, users drown in multi-transcript output. Every clinical and research VEP workflow uses at least one of these.

### Pick logic

| VEP Flag | Description | Upstream change needed |
|----------|-------------|----------------------|
| `--pick` | One consequence line per variant, ranked by configurable criteria | Yes — ranking engine in `datafusion-bio-function-vep` |
| `--pick_allele` | One consequence per allele (for multi-allelic sites) | Yes |
| `--per_gene` | Most severe consequence per gene | Yes |
| `--pick_allele_gene` | One consequence per allele + gene combination | Yes |
| `--flag_pick` / `--flag_pick_allele` / `--flag_pick_allele_gene` | Add `PICK=1` flag instead of filtering rows | Yes |
| `--pick_order` | Customize ranking: `mane_select`, `canonical`, `appris`, `tsl`, `biotype`, `ccds`, `rank`, `length` | Yes |

**Default pick order** (VEP): MANE_SELECT > canonical > APPRIS > TSL > biotype > CCDS > consequence severity > transcript length.

### Filtering

| VEP Flag | Description | Upstream change needed |
|----------|-------------|----------------------|
| `--most_severe` | Only the single most severe consequence per variant (no transcript data) | Yes |
| `--coding_only` | Drop non-coding transcript consequences | Yes |
| `--no_intergenic` | Drop intergenic_variant lines | Yes |
| `--gencode_basic` | Restrict to GENCODE basic transcript set | Yes |
| `--gencode_primary` | Restrict to GENCODE primary set (GRCh38 only) | Yes |

**Implementation plan:** All pick/filter logic should live in `datafusion-bio-function-vep` as SQL-level options passed through the options JSON. vepyr exposes them as Python keyword arguments.

---

## P1 — Plugin Data Integration

This is the **killer differentiator** beyond performance. VEP's #1 user pain point is plugin data management — each plugin requires separate multi-GB downloads, manual bgzip/tabix indexing, and version matching. vepyr can solve this with a single `build_cache()` call.

### Target: bundled plugin data in parquet cache

```python
# Future API
vepyr.build_cache(
    release=115,
    cache_dir="/data/vep",
    plugins=["cadd", "spliceai", "alphamissense", "clinvar"]
)

# Annotation automatically includes plugin scores — zero additional config
lf = vepyr.annotate("input.vcf", "/data/vep/115_GRCh38_vep", everything=True, ...)
```

### Plugin priority

| Plugin | Output Fields | Data Size | User Demand |
|--------|-------------|-----------|-------------|
| **CADD** | `CADD_PHRED`, `CADD_RAW` | ~80 GB (SNVs + indels) | Very high — clinical standard |
| **SpliceAI** | `SpliceAI_pred_DS_{AG,AL,DG,DL}` + position offsets | ~30 GB | Very high — splicing analysis |
| **AlphaMissense** | `am_pathogenicity`, `am_class` | ~1 GB | High — DeepMind missense model |
| **LOFTEE** | `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info` | Algorithmic (no data file) | Very high — gnomAD standard |
| **dbNSFP** | 30+ scores (REVEL, MetaSVM, GERP++, phyloP, etc.) | ~30 GB | High — meta-database |
| **ClinVar** (direct) | `CLNSIG`, `CLNREVSTAT`, `CLNDN` | ~100 MB | High — clinical |
| `--custom` (tabix) | User-defined | Varies | High — custom annotation sources |

**Advantage over VEP:** Parquet format means annotation lookups use DataFusion pushdown instead of tabix random I/O — potentially 10-100x faster per-plugin.

---

## P1 — Individual Flag Toggles

Currently all annotation fields are bundled into `everything=True`. Users should be able to enable subsets independently.

| Flag Group | Parameters to Add |
|-----------|------------------|
| Identifiers | `symbol`, `canonical`, `biotype`, `protein`, `mane`, `tsl`, `appris`, `ccds`, `uniprot` |
| Annotation | `numbers` (EXON/INTRON), `domains`, `regulatory`, `variant_class`, `gene_phenotype`, `mirna` |

**Implementation:** These are already computed by the upstream engine when the relevant flags are in the options JSON. vepyr just needs to expose them as individual keyword arguments and pass them through.

---

## P1 — Output Formats

| Feature | Description | Effort |
|---------|-------------|--------|
| `--json` output | Nested JSON per variant (transcript_consequences array, etc.) | Medium — needs upstream JSON serializer or Python-side formatting |
| `--fields` | Custom CSQ field selection/ordering | Low — filter CSQ columns in SQL or post-processing |
| `--vcf_info_field` | Change `CSQ` to `ANN` for snpEff compatibility | Low — header string change |

---

## P1 — Additional Notations

| VEP Flag | Output Field | Description |
|----------|-------------|-------------|
| `--hgvsg` | `HGVSg` | Genomic HGVS (RefSeq contig accession + position) |

---

## P2 — Novel Capabilities (vepyr-only)

Features that Ensembl VEP cannot offer due to architectural limitations.

### SQL query interface

Since the backend is DataFusion, expose the SQL layer directly:

```python
# Future API
result = vepyr.query("""
    SELECT SYMBOL, Consequence, gnomADe_AF, SIFT, PolyPhen
    FROM annotate_vep('input.vcf', '/data/vep/cache')
    WHERE IMPACT IN ('HIGH', 'MODERATE')
      AND (gnomADe_AF IS NULL OR gnomADe_AF < 0.01)
      AND SIFT LIKE 'deleterious%'
""")
```

**Advantage:** Predicate pushdown into the annotation engine. No equivalent in VEP — users must annotate everything, then filter with separate tools.

### Real-time single-variant API

For clinical decision support systems that need sub-millisecond lookups:

```python
# Future API
annotator = vepyr.Annotator(cache_dir="/data/vep/cache")
result = annotator.lookup("chr17:41245466:G:A")  # BRCA1, <1ms with warm cache
```

**Advantage:** VEP has 10-30s startup overhead. A persistent `Annotator` with fjall backend could serve interactive/API use cases.

### Incremental annotation

Cache annotation results by variant hash. On re-run, only annotate new/changed variants and merge with previous results. For clinical labs with 99% variant overlap between daily runs, this turns 10-minute jobs into seconds.

### Cohort-aware annotation

Accept multi-sample VCF, annotate unique variants once, compute cohort-level allele frequencies alongside population AFs. Flag de novo candidates across trios/families.

### Cloud-native I/O

Read VCF from and write results to S3/GCS/Azure Blob. DataFusion already supports object stores via the `object_store` crate.

### Annotation provenance

Embed structured metadata in output:
- Exact cache version + build date
- All flags used
- Reference FASTA checksum
- Plugin data versions

---

## P2 — Lower-Priority VEP Parity

| VEP Flag | Description |
|----------|-------------|
| `--spdi` | SPDI notation |
| `--ga4gh_vrs` | GA4GH VRS notation |
| `--xref_refseq` | RefSeq cross-reference |
| `--transcript_version` / `--gene_version` | Version suffixes on Ensembl IDs |
| `--nearest` | Nearest gene/transcript for intergenic variants |
| `--check_ref` / `--lookup_ref` | Reference allele validation against FASTA |
| `--minimal` | Minimal allele representation |
| `--allele_number` | Track allele numbers in multi-allelic VCFs |
| `--chr` | Restrict annotation to specific chromosomes |
| `--shift_3prime` | 3' right-align for consequence calculation |
| `--no_check_alleles` | Compare co-located variants by coordinates only |
| `--clin_sig_allele` | Allele-specific clinical significance |
| `--var_synonyms` | Known variant synonyms |
| `--cell_type` | Filter regulatory regions by cell type |
| `--humdiv` | Use humDiv PolyPhen scores instead of humVar |

---

## Test Coverage Gaps

The current golden test set (100 variants, chr1:602K-850K) is predominantly non-coding lncRNA territory. The following features are **implemented but untested at the golden comparison level**:

| Feature | Why Untested | Action Needed |
|---------|-------------|---------------|
| HGVSp | No coding variants in test region | Add coding variants (missense, frameshift) to golden set |
| SIFT / PolyPhen | No missense variants | Same — needs missense variants |
| Amino_acids / Codons | No coding consequences | Same |
| CDS_position / Protein_position | No coding consequences | Same |
| ENSP / SWISSPROT / TREMBL / UNIPARC | No protein-coding transcripts | Add protein-coding region variants |
| MANE_SELECT / MANE_PLUS_CLINICAL | No MANE transcripts in region | Include a MANE transcript region |
| APPRIS / CCDS | No protein-coding transcripts | Same |
| DOMAINS | No domain-overlapping variants | Add exonic variants in known domain regions |
| SIFT / PolyPhen predictions | No missense variants | Add missense variants with known SIFT/PolyPhen scores |
| CLIN_SIG / SOMATIC | No ClinVar variants in region | Include known pathogenic variant positions |
| MOTIF_* / TRANSCRIPTION_FACTORS | No motif features in region | Include regulatory/motif region variants |
| FLAGS (cds_start_NF, cds_end_NF) | No incomplete CDS models | Include edge-case transcripts |

**Recommendation:** Expand the golden test set with ~50 coding variants from a gene-rich region (e.g., chr17 BRCA1/BRCA2 region or chr7 CFTR region) that exercises protein annotation, predictions, ClinVar, and MANE transcripts.

---

## Upstream Repository Changes Needed

All annotation logic changes go to [biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions).

| Change | Priority | Scope |
|--------|----------|-------|
| Pick/filter engine (P0) | P0 | New ranking + filtering logic in `annotate_vep()` |
| Fix DataFrame SYMBOL pick (#54) | P0 | Transcript ranking should prefer populated gene_symbol |
| Individual flag toggles (P1) | P1 | Expose existing fields as separate options |
| Plugin data table providers (P1) | P1 | New table providers for CADD, SpliceAI, etc. |
| HGVSg notation (P1) | P1 | Genomic HGVS output |
| JSON output serializer (P1) | P1 | Nested JSON per variant |

Cache format changes go to [biodatageeks/datafusion-bio-formats](https://github.com/biodatageeks/datafusion-bio-formats).

| Change | Priority | Scope |
|--------|----------|-------|
| Plugin data conversion (P1) | P1 | Convert CADD/SpliceAI/AlphaMissense to parquet alongside Ensembl cache |

---

## Adoption Strategy

Performance (50x+ over VEP) is the hook, but not sufficient alone to displace an entrenched tool. The adoption formula:

1. **Correctness parity** (done) — so people trust it
2. **Pick/filter logic** (P0) — so people can actually use it in real workflows
3. **Bundled plugin data** (P1) — so people love it ("annotate with CADD + SpliceAI + AlphaMissense in one command with zero setup")
4. **SQL interface** (P2) — so people can't go back ("filter during annotation, not after")

The story that drives word-of-mouth: *"pip install vepyr, build_cache(), annotate() — done. All scores included. 50x faster. No Perl."*
