# Fast Annotation Report: chr1_chr22 (parquet, release 115, profile ensembl)

**Date:** 2026-07-29 22:43
**Variants:** 4,096,123 (HG002 GRCh38, bcftools norm -m -both)
**Backend:** parquet only
**Total annotation time:** 188s (3.1 min)
**Aggregate rate:** 21,730 variants/s
**Build:** branch `release-testing` @ `01350d6c92d0`, bio-functions rev `0d02d711b352`
**Cargo.lock SHA-256:** `b521c417e242bf022898bc32534cc0e6cd27122437f500fb7d94cf873734f6c4`
**datafusion-bio-format-core:** `eee2d6926331fe5106cbbefbc1ca673e94357327`; effective source `git+https://github.com/biodatageeks/datafusion-bio-formats.git?rev=eee2d6926331fe5106cbbefbc1ca673e94357327#eee2d6926331fe5106cbbefbc1ca673e94357327`
**datafusion-bio-format-ensembl-cache:** `eee2d6926331fe5106cbbefbc1ca673e94357327`; effective source `git+https://github.com/biodatageeks/datafusion-bio-formats.git?rev=eee2d6926331fe5106cbbefbc1ca673e94357327#eee2d6926331fe5106cbbefbc1ca673e94357327`
**datafusion-bio-format-vcf:** `eee2d6926331fe5106cbbefbc1ca673e94357327`; effective source `git+https://github.com/biodatageeks/datafusion-bio-formats.git?rev=eee2d6926331fe5106cbbefbc1ca673e94357327#eee2d6926331fe5106cbbefbc1ca673e94357327`
**datafusion-bio-function-vep:** `0d02d711b352baf4087e2e9421e12716e10bb290`; effective source `git+https://github.com/biodatageeks/datafusion-bio-functions.git?rev=0d02d711b352baf4087e2e9421e12716e10bb290#0d02d711b352baf4087e2e9421e12716e10bb290`
**VEP reference:** VEP `115.2`, API `115`, cache `115`, Ensembl `115.266b84d`, variation `115.b7c2637`
**Validated cache:** release `115`, source `ensembl` (contig-local Parquet metadata)
**Native target:** VEP `115.2`, semantics `115`, cache `115`

## Headline

- **80 / 80 CSQ fields at 100% match** (0 mismatches)
- **0 fields** with mismatches, **0 total** across CSQ entries
- **49 fields FIXED** to 0 vs previous benchmark (was 2,864 mismatches)
- All mismatches traced to root cause classes with upstream issues filed

## Root Cause Classification & Issue Tracker

| # | Root Cause | Mismatches | Fields Affected | Upstream Issue | Status |
|---|-----------|-----------|-----------------|---------------|--------|
| 1 | `stop_retained_variant` false positive on inframe ops | 0 | Consequence | [#90](https://github.com/biodatageeks/datafusion-bio-functions/issues/90), [#117](https://github.com/biodatageeks/datafusion-bio-functions/issues/117), [#113](https://github.com/biodatageeks/datafusion-bio-functions/pull/113) | FIXED |
| 2 | `stop_gained` extra on frameshift | 0 | Consequence | [#114](https://github.com/biodatageeks/datafusion-bio-functions/issues/114) | FIXED |
| 3 | `stop_lost` missing on frameshift past stop codon | 0 | Consequence | [#115](https://github.com/biodatageeks/datafusion-bio-functions/issues/115) | FIXED |
| 4 | Inframe/frameshift disagree at CDS boundary | 0 | Consequence | [#117](https://github.com/biodatageeks/datafusion-bio-functions/issues/117) | FIXED |
| 5 | Incomplete terminal codon: IMPACT/HGVSp residual (Xaa vs Ter, missing p.Ter=) | ~0 IMPACT + 0 HGVSp | IMPACT, HGVSp | [#130](https://github.com/biodatageeks/datafusion-bio-functions/issues/130) | OPEN |
| 6 | `stop_gained` missing on frameshift/inframe_deletion | 0 | Consequence | [#116](https://github.com/biodatageeks/datafusion-bio-functions/issues/116) | FIXED |
| 7 | `incomplete_terminal_codon` companion terms | 0 | Consequence | [#101](https://github.com/biodatageeks/datafusion-bio-functions/issues/101) | FIXED |
| 8 | HGVSc/HGVS_OFFSET on non-coding + UTR indels | ~0 + 0 | HGVSc, HGVS_OFFSET | [#112](https://github.com/biodatageeks/datafusion-bio-functions/issues/112) | FIXED |
| 9 | HGNC_ID false-positive propagation | ~0 | HGNC_ID | [#108](https://github.com/biodatageeks/datafusion-bio-functions/issues/108) | FIXED |
| 10 | CDS/protein fields missing at CDS boundary | ~0 | CDS_position, Protein_position, Amino_acids, Codons, DOMAINS | [#118](https://github.com/biodatageeks/datafusion-bio-functions/issues/118) | FIXED |
| 11 | miRNA dedup (stem repeated in VEP) | 0 | miRNA | [#100](https://github.com/biodatageeks/datafusion-bio-functions/issues/100) | FIXED |
| 12 | `protein_altering_variant` not emitted for complex inframe changes | 0 | Consequence | [#124](https://github.com/biodatageeks/datafusion-bio-functions/issues/124) | FIXED |
| 13 | `start_retained_variant` missing alongside `start_lost` | 0 | Consequence | [#125](https://github.com/biodatageeks/datafusion-bio-functions/issues/125) | FIXED |

## Per-Chromosome Performance

| Chrom | Variants | Time (s) | Rate (v/s) |
|-------|----------|----------|------------|
| chr1 | 323,430 | 14.1 | 22,938 |
| chr2 | 331,324 | 14.1 | 23,498 |
| chr3 | 288,531 | 12.3 | 23,458 |
| chr4 | 307,295 | 12.2 | 25,188 |
| chr5 | 264,411 | 16.2 | 16,322 |
| chr6 | 271,966 | 10.9 | 24,951 |
| chr7 | 234,522 | 11.6 | 20,217 |
| chr8 | 225,240 | 9.3 | 24,219 |
| chr9 | 176,111 | 11.1 | 15,866 |
| chr10 | 213,466 | 8.6 | 24,822 |
| chr11 | 206,822 | 9.1 | 22,728 |
| chr12 | 197,815 | 8.5 | 23,272 |
| chr13 | 161,419 | 6.4 | 25,222 |
| chr14 | 133,199 | 5.9 | 22,576 |
| chr15 | 125,179 | 5.5 | 22,760 |
| chr16 | 123,358 | 5.6 | 22,028 |
| chr17 | 108,376 | 5.7 | 19,013 |
| chr18 | 119,383 | 4.9 | 24,364 |
| chr19 | 90,699 | 5.1 | 17,784 |
| chr20 | 86,904 | 6.2 | 14,017 |
| chr21 | 55,812 | 2.6 | 21,466 |
| chr22 | 50,861 | 2.6 | 19,562 |
| **TOTAL** | **4,096,123** | **188.5** | **21,730** |

## Variant Coverage

| Metric | Value |
|--------|-------|
| Variants compared | 4,096,123 |
| CSQ entry count match | 4,096,123 |
| CSQ entry count mismatch | 0 |
| Only in vepyr | 0 |
| Only in VEP | 0 |
| CSQ entries only in vepyr | 0 |
| CSQ entries only in VEP | 0 |
| Uncapped mismatch-ledger rows | 0 |

## Field Equality Shapes

| Shape | CSQ field comparisons |
|-------|----------------------:|
| `both_empty` | 1,620,215,488 |
| `both_nonempty_equal` | 1,355,126,192 |
| `vepyr_empty_only` | 0 |
| `vep_empty_only` | 0 |
| `both_nonempty_unequal` | 0 |

## Field-Level Mismatches: NEW vs OLD Benchmark

| Field | NEW (this run) | OLD (benchmark) | Delta | Status |
|-------|---------------|-----------------|-------|--------|
| HGVSc | 0 | 317 | -317 | FIXED |
| Feature | 0 | 260 | -260 | FIXED |
| ENSP | 0 | 242 | -242 | FIXED |
| HGVSp | 0 | 200 | -200 | FIXED |
| INTRON | 0 | 178 | -178 | FIXED |
| Consequence | 0 | 175 | -175 | FIXED |
| UNIPARC | 0 | 173 | -173 | FIXED |
| TREMBL | 0 | 117 | -117 | FIXED |
| cDNA_position | 0 | 91 | -91 | FIXED |
| APPRIS | 0 | 88 | -88 | FIXED |
| CCDS | 0 | 84 | -84 | FIXED |
| TSL | 0 | 83 | -83 | FIXED |
| IMPACT | 0 | 83 | -83 | FIXED |
| HGNC_ID | 0 | 78 | -78 | FIXED |
| EXON | 0 | 65 | -65 | FIXED |
| CDS_position | 0 | 64 | -64 | FIXED |
| Protein_position | 0 | 64 | -64 | FIXED |
| DOMAINS | 0 | 61 | -61 | FIXED |
| UNIPROT_ISOFORM | 0 | 51 | -51 | FIXED |
| FLAGS | 0 | 45 | -45 | FIXED |
| Codons | 0 | 44 | -44 | FIXED |
| BIOTYPE | 0 | 44 | -44 | FIXED |
| Amino_acids | 0 | 44 | -44 | FIXED |
| SWISSPROT | 0 | 43 | -43 | FIXED |
| CANONICAL | 0 | 30 | -30 | FIXED |
| MANE | 0 | 22 | -22 | FIXED |
| MANE_SELECT | 0 | 22 | -22 | FIXED |
| SYMBOL | 0 | 15 | -15 | FIXED |
| Gene | 0 | 15 | -15 | FIXED |
| STRAND | 0 | 14 | -14 | FIXED |
| HGVS_OFFSET | 0 | 11 | -11 | FIXED |
| SIFT | 0 | 8 | -8 | FIXED |
| PolyPhen | 0 | 8 | -8 | FIXED |
| DISTANCE | 0 | 7 | -7 | FIXED |
| SYMBOL_SOURCE | 0 | 4 | -4 | FIXED |
| gnomADg_FIN_AF | 0 | 1 | -1 | FIXED |
| gnomADg_AFR_AF | 0 | 1 | -1 | FIXED |
| gnomADg_NFE_AF | 0 | 1 | -1 | FIXED |
| gnomADg_EAS_AF | 0 | 1 | -1 | FIXED |
| miRNA | 0 | 1 | -1 | FIXED |
| gnomADg_SAS_AF | 0 | 1 | -1 | FIXED |
| gnomADg_AMI_AF | 0 | 1 | -1 | FIXED |
| gnomADg_MID_AF | 0 | 1 | -1 | FIXED |
| MAX_AF_POPS | 0 | 1 | -1 | FIXED |
| gnomADg_AF | 0 | 1 | -1 | FIXED |
| MAX_AF | 0 | 1 | -1 | FIXED |
| gnomADg_ASJ_AF | 0 | 1 | -1 | FIXED |
| gnomADg_REMAINING_AF | 0 | 1 | -1 | FIXED |
| gnomADg_AMR_AF | 0 | 1 | -1 | FIXED |

**Total mismatches: 0** (was 2,864, delta -2,864)

### Fields FIXED (previously had mismatches, now 0): 49 fields

**Consequence** (175), **IMPACT** (83), **SYMBOL** (15), **Gene** (15), **Feature** (260), **BIOTYPE** (44), **EXON** (65), **INTRON** (178), **HGVSc** (317), **HGVSp** (200), **cDNA_position** (91), **CDS_position** (64), **Protein_position** (64), **Amino_acids** (44), **Codons** (44), **DISTANCE** (7), **STRAND** (14), **FLAGS** (45), **SYMBOL_SOURCE** (4), **HGNC_ID** (78), **CANONICAL** (30), **MANE** (22), **MANE_SELECT** (22), **TSL** (83), **APPRIS** (88), **CCDS** (84), **ENSP** (242), **SWISSPROT** (43), **TREMBL** (117), **UNIPARC** (173), **UNIPROT_ISOFORM** (51), **SIFT** (8), **PolyPhen** (8), **DOMAINS** (61), **miRNA** (1), **HGVS_OFFSET** (11), **gnomADg_AF** (1), **gnomADg_AFR_AF** (1), **gnomADg_AMI_AF** (1), **gnomADg_AMR_AF** (1), **gnomADg_ASJ_AF** (1), **gnomADg_EAS_AF** (1), **gnomADg_FIN_AF** (1), **gnomADg_MID_AF** (1), **gnomADg_NFE_AF** (1), **gnomADg_REMAINING_AF** (1), **gnomADg_SAS_AF** (1), **MAX_AF** (1), **MAX_AF_POPS** (1)

## Remaining Mismatch Details

## Per-Chromosome Mismatch Breakdown

| Chrom | Variants | CSQ Match | Consequence | HGVSc | HGVSp | IMPACT | HGNC_ID | Other |
|-------|----------|-----------|-------------|-------|-------|--------|---------|-------|
| chr1 | 323,430 | 323,430 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr2 | 331,324 | 331,324 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr3 | 288,531 | 288,531 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr4 | 307,295 | 307,295 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr5 | 264,411 | 264,411 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr6 | 271,966 | 271,966 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr7 | 234,522 | 234,522 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr8 | 225,240 | 225,240 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr9 | 176,111 | 176,111 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr10 | 213,466 | 213,466 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr11 | 206,822 | 206,822 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr12 | 197,815 | 197,815 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr13 | 161,419 | 161,419 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr14 | 133,199 | 133,199 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr15 | 125,179 | 125,179 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr16 | 123,358 | 123,358 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr17 | 108,376 | 108,376 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr18 | 119,383 | 119,383 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr19 | 90,699 | 90,699 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr20 | 86,904 | 86,904 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr21 | 55,812 | 55,812 | 0 | 0 | 0 | 0 | 0 | 0 |
| chr22 | 50,861 | 50,861 | 0 | 0 | 0 | 0 | 0 | 0 |
