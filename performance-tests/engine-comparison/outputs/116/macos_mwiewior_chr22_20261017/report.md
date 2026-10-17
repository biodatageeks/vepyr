# vepyr, vep-rs and fastVEP on chr22

Reference: `##VEP="v116.0" API="v116" time="2026-07-28 04:41:51" cache="/opt/vep/.vep/homo_s…`

- Host: Apple M3 Max, 16 logical CPUs, 64 GiB, macOS-15.7.9-arm64-arm-64bit
- vepyr: vepyr 0.10.0
- vep-rs: `f64272326dee8f813a57f9fa832539ccd8067dab`
- fastVEP: `5d8004a72fb779b8f30ef6096c0a998cc4817138`
- Input: `HG002_chr22.vcf.gz`, 50,861 records, SHA-256 `cdde8185d793ed0e3c9c800e83574f944f6ef2260cd7420a3758832b0e59b5e5`

## CSQ columns with no mismatches

A column counts when every row the engine shares with Ensembl VEP holds the same value, and Ensembl has no value in it on rows the engine does not write.

| Cache | Ensembl columns | vepyr | vep-rs | fastVEP |
|---|---|---|---|---|
| ensembl | 80 | 80 | 30 | 19 |
| merged | 86 | 86 | 27 | 13 |

| Engine (scored mode) | Columns emitted | Of Ensembl's | Rows identical | Rows missing / extra |
|---|---|---|---|---|
| vepyr (default) | 80 | 80 | 100.00% | 0 / 0 |
| vep-rs (sift) | 82 | 80 | 0.13% | 4,231 / 0 |
| fastVEP (tabix) | 49 | 44 | 0.00% | 11,748 / 14,364 |

### Every column, ensembl cache

| Column | vepyr | vep-rs | fastVEP |
|---|---|---|---|
| `Allele` | exact | exact | exact |
| `Consequence` | exact | exact | 99.999% |
| `IMPACT` | exact | exact | exact |
| `SYMBOL` | exact | exact | exact |
| `Gene` | exact | exact | exact |
| `Feature_type` | exact | exact | exact |
| `Feature` | exact | exact | exact |
| `BIOTYPE` | exact | exact | exact |
| `EXON` | exact | exact | 100.000% |
| `INTRON` | exact | exact | exact |
| `HGVSc` | exact | exact | 99.975% |
| `HGVSp` | exact | exact | 99.916% |
| `cDNA_position` | exact | exact | 99.999% |
| `CDS_position` | exact | exact | exact |
| `Protein_position` | exact | exact | exact |
| `Amino_acids` | exact | exact | exact |
| `Codons` | exact | exact | exact |
| `Existing_variation` | exact | 72.461% | 78.228% |
| `DISTANCE` | exact | exact | exact |
| `STRAND` | exact | exact | 99.271% |
| `FLAGS` | exact | exact | 97.105% |
| `VARIANT_CLASS` | exact | exact | not emitted |
| `SYMBOL_SOURCE` | exact | exact | 6.461% |
| `HGNC_ID` | exact | exact | 6.511% |
| `CANONICAL` | exact | exact | exact |
| `MANE` | exact | 96.809% | exact |
| `MANE_SELECT` | exact | 96.809% | 96.809% |
| `MANE_PLUS_CLINICAL` | empty in both | empty in both | empty in both |
| `TSL` | exact | 93.847% | exact |
| `APPRIS` | exact | 74.856% | 74.831% |
| `CCDS` | exact | exact | exact |
| `ENSP` | exact | exact | exact |
| `SWISSPROT` | exact | exact | not emitted |
| `TREMBL` | exact | exact | not emitted |
| `UNIPARC` | exact | 59.715% | not emitted |
| `UNIPROT_ISOFORM` | exact | 92.742% | not emitted |
| `GENE_PHENO` | exact | 65.489% | not emitted |
| `SIFT` | exact | 100.000% | 99.651% |
| `PolyPhen` | exact | 100.000% | 99.840% |
| `DOMAINS` | exact | 99.369% | not emitted |
| `miRNA` | exact | 100.000% | not emitted |
| `HGVS_OFFSET` | exact | exact | 94.047% |
| `AF` | exact | 78.727% | 7.662% |
| `AFR_AF` | exact | 98.079% | not emitted |
| `AMR_AF` | exact | 98.079% | not emitted |
| `EAS_AF` | exact | 98.080% | not emitted |
| `EUR_AF` | exact | 98.079% | not emitted |
| `SAS_AF` | exact | 98.079% | not emitted |
| `gnomADe_AF` | exact | 98.117% | not emitted |
| `gnomADe_AFR_AF` | exact | 98.485% | not emitted |
| `gnomADe_AMR_AF` | exact | 98.526% | not emitted |
| `gnomADe_ASJ_AF` | exact | 98.603% | not emitted |
| `gnomADe_EAS_AF` | exact | 98.359% | not emitted |
| `gnomADe_FIN_AF` | exact | 98.480% | not emitted |
| `gnomADe_MID_AF` | exact | 98.640% | not emitted |
| `gnomADe_NFE_AF` | exact | 98.147% | not emitted |
| `gnomADe_REMAINING_AF` | exact | 98.419% | not emitted |
| `gnomADe_SAS_AF` | exact | 98.450% | not emitted |
| `gnomADg_AF` | exact | 87.458% | not emitted |
| `gnomADg_AFR_AF` | exact | 87.252% | not emitted |
| `gnomADg_AMI_AF` | exact | 1.223% | not emitted |
| `gnomADg_AMR_AF` | exact | 87.737% | not emitted |
| `gnomADg_ASJ_AF` | exact | 87.867% | not emitted |
| `gnomADg_EAS_AF` | exact | 87.957% | not emitted |
| `gnomADg_FIN_AF` | exact | 87.844% | not emitted |
| `gnomADg_MID_AF` | exact | 88.003% | not emitted |
| `gnomADg_NFE_AF` | exact | 87.408% | not emitted |
| `gnomADg_REMAINING_AF` | exact | 87.884% | not emitted |
| `gnomADg_SAS_AF` | exact | 87.872% | not emitted |
| `MAX_AF` | exact | 0.981% | not emitted |
| `MAX_AF_POPS` | exact | 80.039% | not emitted |
| `CLIN_SIG` | exact | 99.715% | 99.578% |
| `SOMATIC` | exact | 0.291% | 93.103% |
| `PHENO` | exact | 8.001% | 92.313% |
| `PUBMED` | exact | 99.959% | 99.391% |
| `MOTIF_NAME` | exact | never filled | never filled |
| `MOTIF_POS` | exact | never filled | never filled |
| `HIGH_INF_POS` | exact | never filled | never filled |
| `MOTIF_SCORE_CHANGE` | exact | never filled | never filled |
| `TRANSCRIPTION_FACTORS` | exact | never filled | never filled |

vep-rs also writes: `ALLELE_NUM`, `HGVSg`

fastVEP also writes: `REF_ALLELE`, `UPLOADED_ALLELE`, `SOURCE`, `ACMG`, `ACMG_CRITERIA`

### Every column, merged cache

| Column | vepyr | vep-rs | fastVEP |
|---|---|---|---|
| `Allele` | exact | exact | exact |
| `Consequence` | exact | exact | 99.999% |
| `IMPACT` | exact | exact | exact |
| `SYMBOL` | exact | exact | exact |
| `Gene` | exact | exact | 75.862% |
| `Feature_type` | exact | exact | exact |
| `Feature` | exact | exact | exact |
| `BIOTYPE` | exact | exact | 75.884% |
| `EXON` | exact | exact | 100.000% |
| `INTRON` | exact | exact | 100.000% |
| `HGVSc` | exact | 99.999% | 99.975% |
| `HGVSp` | exact | 100.000% | 99.907% |
| `cDNA_position` | exact | exact | 99.999% |
| `CDS_position` | exact | exact | 100.000% |
| `Protein_position` | exact | exact | 100.000% |
| `Amino_acids` | exact | exact | exact |
| `Codons` | exact | exact | exact |
| `Existing_variation` | exact | 72.267% | 77.891% |
| `DISTANCE` | exact | exact | exact |
| `STRAND` | exact | exact | 99.474% |
| `FLAGS` | exact | exact | 97.803% |
| `VARIANT_CLASS` | exact | exact | not emitted |
| `SYMBOL_SOURCE` | exact | exact | 4.875% |
| `HGNC_ID` | exact | 76.708% | 5.788% |
| `CANONICAL` | exact | exact | 96.375% |
| `MANE` | exact | 95.149% | 97.570% |
| `MANE_SELECT` | exact | 95.149% | 95.149% |
| `MANE_PLUS_CLINICAL` | empty in both | empty in both | empty in both |
| `TSL` | exact | 95.331% | exact |
| `APPRIS` | exact | 80.921% | 80.899% |
| `CCDS` | exact | exact | exact |
| `ENSP` | exact | exact | exact |
| `SWISSPROT` | exact | exact | not emitted |
| `TREMBL` | exact | exact | not emitted |
| `UNIPARC` | exact | 69.433% | not emitted |
| `UNIPROT_ISOFORM` | exact | 94.493% | not emitted |
| `REFSEQ_MATCH` | empty in both | not emitted | not emitted |
| `SOURCE` | exact | not emitted | exact |
| `REFSEQ_OFFSET` | empty in both | not emitted | not emitted |
| `GIVEN_REF` | exact | not emitted | not emitted |
| `USED_REF` | exact | not emitted | not emitted |
| `BAM_EDIT` | exact | not emitted | not emitted |
| `GENE_PHENO` | exact | 70.197% | not emitted |
| `SIFT` | exact | 100.000% | 99.660% |
| `PolyPhen` | exact | 100.000% | 99.817% |
| `DOMAINS` | exact | 99.521% | not emitted |
| `miRNA` | exact | 100.000% | not emitted |
| `HGVS_OFFSET` | exact | exact | 93.948% |
| `AF` | exact | 78.629% | 7.798% |
| `AFR_AF` | exact | 98.073% | not emitted |
| `AMR_AF` | exact | 98.073% | not emitted |
| `EAS_AF` | exact | 98.074% | not emitted |
| `EUR_AF` | exact | 98.073% | not emitted |
| `SAS_AF` | exact | 98.073% | not emitted |
| `gnomADe_AF` | exact | 98.186% | not emitted |
| `gnomADe_AFR_AF` | exact | 98.536% | not emitted |
| `gnomADe_AMR_AF` | exact | 98.576% | not emitted |
| `gnomADe_ASJ_AF` | exact | 98.641% | not emitted |
| `gnomADe_EAS_AF` | exact | 98.405% | not emitted |
| `gnomADe_FIN_AF` | exact | 98.528% | not emitted |
| `gnomADe_MID_AF` | exact | 98.677% | not emitted |
| `gnomADe_NFE_AF` | exact | 98.210% | not emitted |
| `gnomADe_REMAINING_AF` | exact | 98.470% | not emitted |
| `gnomADe_SAS_AF` | exact | 98.501% | not emitted |
| `gnomADg_AF` | exact | 87.271% | not emitted |
| `gnomADg_AFR_AF` | exact | 87.076% | not emitted |
| `gnomADg_AMI_AF` | exact | 1.202% | not emitted |
| `gnomADg_AMR_AF` | exact | 87.576% | not emitted |
| `gnomADg_ASJ_AF` | exact | 87.712% | not emitted |
| `gnomADg_EAS_AF` | exact | 87.809% | not emitted |
| `gnomADg_FIN_AF` | exact | 87.692% | not emitted |
| `gnomADg_MID_AF` | exact | 87.860% | not emitted |
| `gnomADg_NFE_AF` | exact | 87.236% | not emitted |
| `gnomADg_REMAINING_AF` | exact | 87.730% | not emitted |
| `gnomADg_SAS_AF` | exact | 87.712% | not emitted |
| `MAX_AF` | exact | 1.001% | not emitted |
| `MAX_AF_POPS` | exact | 79.685% | not emitted |
| `CLIN_SIG` | exact | 99.727% | 99.596% |
| `SOMATIC` | exact | 0.294% | 93.015% |
| `PHENO` | exact | 7.941% | 92.245% |
| `PUBMED` | exact | 99.959% | 99.424% |
| `MOTIF_NAME` | exact | never filled | never filled |
| `MOTIF_POS` | exact | never filled | never filled |
| `HIGH_INF_POS` | exact | never filled | never filled |
| `MOTIF_SCORE_CHANGE` | exact | never filled | never filled |
| `TRANSCRIPTION_FACTORS` | exact | never filled | never filled |

vep-rs also writes: `ALLELE_NUM`, `HGVSg`

fastVEP also writes: `REF_ALLELE`, `UPLOADED_ALLELE`, `ACMG`, `ACMG_CRITERIA`

## Cache size on disk, chr22 (MB)

| Engine and format | ensembl, with SIFT/PolyPhen | ensembl, without | merged, with | merged, without |
|---|---|---|---|---|
| Perl VEP Storable (source) | 381.1 | 324.0 | 426.4 | 331.7 |
| vepyr, Parquet | 472.8 | 380.2 | 529.1 | 383.0 |
| vep-rs, JSON | 4,189.9 | 4,073.5 | 4,262.2 | 4,075.4 |
| fastVEP, `--cache-dir` | n/a | 311.7 | n/a | 314.8 |
| fastVEP, `.osa2` | n/a | 455.9 | n/a | 458.9 |

## Run time and memory

Median of the timed runs after one warm-up (fastVEP `--cache-dir`: no warm-up). CPU-seconds are user plus system time.

| Engine | Mode | Cache | 1 thr wall (s) | 1 thr CPU-s | 1 thr RSS (GiB) | 2 thr wall (s) | 2 thr CPU-s | 2 thr RSS (GiB) |
|---|---|---|---|---|---|---|---|---|
| vepyr | default | ensembl | 2.95 | 4.29 | 1.06 | 2.14 | 4.6 | 1.14 |
| vepyr | default | merged | 3.98 | 5.31 | 1.19 | 3.09 | 5.58 | 1.23 |
| vep-rs | sift | ensembl | 35.62 | 34.76 | 21.92 | 24.31 | 35.69 | 22.19 |
| vep-rs | sift | merged | 38.52 | 37.82 | 21.94 | 25.89 | 38.79 | 23.77 |
| vep-rs | nosift | ensembl | 35.6 | 34.49 | 20.35 | 23.48 | 34.38 | 22.97 |
| vep-rs | nosift | merged | 38.84 | 37.96 | 21.05 | 25.45 | 37.77 | 23.23 |
| fastVEP | osa2 | ensembl | 12.63 | 12.61 | 2.05 | 11.53 | 12.87 | 2.89 |
| fastVEP | osa2 | merged | 14.64 | 14.59 | 3.31 | 14.2 | 15.75 | 2.96 |
| fastVEP | tabix | ensembl | 729.83 | 729.4 | 0.35 | 745.14 | 745.17 | 0.3 |
| fastVEP | tabix | merged | 731.31 | 731.04 | 0.45 | 741.83 | 742.16 | 0.34 |

## Same output at every thread count

| Engine | Mode | Cache | Identical |
|---|---|---|---|
| vepyr | default | ensembl | yes |
| vep-rs | sift | ensembl | no |
| vep-rs | nosift | ensembl | no |
| fastVEP | osa2 | ensembl | yes |
| fastVEP | tabix | ensembl | yes |
| vepyr | default | merged | yes |
| vep-rs | sift | merged | no |
| vep-rs | nosift | merged | no |
| fastVEP | osa2 | merged | yes |
| fastVEP | tabix | merged | yes |

fastVEP `.osa2`, ensembl: found a co-located ID for 50,421 of the 50,431 records Ensembl matches (99.98%); identical ID set on 48,040. These values go to the `FV_VEPCACHE` INFO field, not to CSQ.

fastVEP `.osa2`, merged: found a co-located ID for 50,421 of the 50,431 records Ensembl matches (99.98%); identical ID set on 48,040. These values go to the `FV_VEPCACHE` INFO field, not to CSQ.
