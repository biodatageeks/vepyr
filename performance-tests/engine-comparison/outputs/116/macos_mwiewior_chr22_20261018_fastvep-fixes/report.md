# vepyr, vep-rs and fastVEP on chr22

Reference: `##VEP="v116.0" API="v116" time="2026-07-28 04:41:51" cache="/opt/vep/.vep/homo_s…`

- Host: Apple M3 Max, 16 logical CPUs, 64 GiB, macOS-15.7.9-arm64-arm-64bit
- vepyr: vepyr 0.10.0
- vep-rs: `None`
- fastVEP: `78d58fcb156e59f0646def42a791f61f5f3e834f`
- Input: `HG002_chr22.vcf.gz`, 50,861 records, SHA-256 `cdde8185d793ed0e3c9c800e83574f944f6ef2260cd7420a3758832b0e59b5e5`

## CSQ columns with no mismatches

A column counts when every row the engine shares with Ensembl VEP holds the same value, and Ensembl has no value in it on rows the engine does not write.

| Cache | Ensembl columns | vepyr | fastVEP |
|---|---|---|---|
| ensembl | 80 | 80 | 21 |
| merged | 86 | 86 | 15 |

| Engine (scored mode) | Columns emitted | Of Ensembl's | Rows identical | Rows missing / extra |
|---|---|---|---|---|
| vepyr (default) | 80 | 80 | 100.00% | 0 / 0 |
| fastVEP (tabix) | 49 | 44 | 0.00% | 11,748 / 14,364 |

### Every column, ensembl cache

| Column | vepyr | fastVEP |
|---|---|---|
| `Allele` | exact | exact |
| `Consequence` | exact | 99.999% |
| `IMPACT` | exact | exact |
| `SYMBOL` | exact | exact |
| `Gene` | exact | exact |
| `Feature_type` | exact | exact |
| `Feature` | exact | exact |
| `BIOTYPE` | exact | exact |
| `EXON` | exact | 100.000% |
| `INTRON` | exact | exact |
| `HGVSc` | exact | 99.975% |
| `HGVSp` | exact | 99.916% |
| `cDNA_position` | exact | 99.999% |
| `CDS_position` | exact | exact |
| `Protein_position` | exact | exact |
| `Amino_acids` | exact | exact |
| `Codons` | exact | exact |
| `Existing_variation` | exact | 78.228% |
| `DISTANCE` | exact | exact |
| `STRAND` | exact | 99.271% |
| `FLAGS` | exact | 97.105% |
| `VARIANT_CLASS` | exact | not emitted |
| `SYMBOL_SOURCE` | exact | 6.461% |
| `HGNC_ID` | exact | 6.511% |
| `CANONICAL` | exact | exact |
| `MANE` | exact | exact |
| `MANE_SELECT` | exact | 96.809% |
| `MANE_PLUS_CLINICAL` | empty in both | empty in both |
| `TSL` | exact | exact |
| `APPRIS` | exact | 74.831% |
| `CCDS` | exact | exact |
| `ENSP` | exact | exact |
| `SWISSPROT` | exact | not emitted |
| `TREMBL` | exact | not emitted |
| `UNIPARC` | exact | not emitted |
| `UNIPROT_ISOFORM` | exact | not emitted |
| `GENE_PHENO` | exact | not emitted |
| `SIFT` | exact | exact |
| `PolyPhen` | exact | exact |
| `DOMAINS` | exact | not emitted |
| `miRNA` | exact | not emitted |
| `HGVS_OFFSET` | exact | 94.047% |
| `AF` | exact | 7.662% |
| `AFR_AF` | exact | not emitted |
| `AMR_AF` | exact | not emitted |
| `EAS_AF` | exact | not emitted |
| `EUR_AF` | exact | not emitted |
| `SAS_AF` | exact | not emitted |
| `gnomADe_AF` | exact | not emitted |
| `gnomADe_AFR_AF` | exact | not emitted |
| `gnomADe_AMR_AF` | exact | not emitted |
| `gnomADe_ASJ_AF` | exact | not emitted |
| `gnomADe_EAS_AF` | exact | not emitted |
| `gnomADe_FIN_AF` | exact | not emitted |
| `gnomADe_MID_AF` | exact | not emitted |
| `gnomADe_NFE_AF` | exact | not emitted |
| `gnomADe_REMAINING_AF` | exact | not emitted |
| `gnomADe_SAS_AF` | exact | not emitted |
| `gnomADg_AF` | exact | not emitted |
| `gnomADg_AFR_AF` | exact | not emitted |
| `gnomADg_AMI_AF` | exact | not emitted |
| `gnomADg_AMR_AF` | exact | not emitted |
| `gnomADg_ASJ_AF` | exact | not emitted |
| `gnomADg_EAS_AF` | exact | not emitted |
| `gnomADg_FIN_AF` | exact | not emitted |
| `gnomADg_MID_AF` | exact | not emitted |
| `gnomADg_NFE_AF` | exact | not emitted |
| `gnomADg_REMAINING_AF` | exact | not emitted |
| `gnomADg_SAS_AF` | exact | not emitted |
| `MAX_AF` | exact | not emitted |
| `MAX_AF_POPS` | exact | not emitted |
| `CLIN_SIG` | exact | 99.578% |
| `SOMATIC` | exact | 93.103% |
| `PHENO` | exact | 92.313% |
| `PUBMED` | exact | 99.391% |
| `MOTIF_NAME` | exact | never filled |
| `MOTIF_POS` | exact | never filled |
| `HIGH_INF_POS` | exact | never filled |
| `MOTIF_SCORE_CHANGE` | exact | never filled |
| `TRANSCRIPTION_FACTORS` | exact | never filled |

fastVEP also writes: `REF_ALLELE`, `UPLOADED_ALLELE`, `SOURCE`, `ACMG`, `ACMG_CRITERIA`

### Every column, merged cache

| Column | vepyr | fastVEP |
|---|---|---|
| `Allele` | exact | exact |
| `Consequence` | exact | 99.999% |
| `IMPACT` | exact | exact |
| `SYMBOL` | exact | exact |
| `Gene` | exact | 75.862% |
| `Feature_type` | exact | exact |
| `Feature` | exact | exact |
| `BIOTYPE` | exact | 75.884% |
| `EXON` | exact | 100.000% |
| `INTRON` | exact | 100.000% |
| `HGVSc` | exact | 99.975% |
| `HGVSp` | exact | 99.907% |
| `cDNA_position` | exact | 99.999% |
| `CDS_position` | exact | 100.000% |
| `Protein_position` | exact | 100.000% |
| `Amino_acids` | exact | exact |
| `Codons` | exact | exact |
| `Existing_variation` | exact | 77.891% |
| `DISTANCE` | exact | exact |
| `STRAND` | exact | 99.474% |
| `FLAGS` | exact | 97.803% |
| `VARIANT_CLASS` | exact | not emitted |
| `SYMBOL_SOURCE` | exact | 4.875% |
| `HGNC_ID` | exact | 5.788% |
| `CANONICAL` | exact | 96.375% |
| `MANE` | exact | 97.570% |
| `MANE_SELECT` | exact | 95.149% |
| `MANE_PLUS_CLINICAL` | empty in both | empty in both |
| `TSL` | exact | exact |
| `APPRIS` | exact | 80.899% |
| `CCDS` | exact | exact |
| `ENSP` | exact | exact |
| `SWISSPROT` | exact | not emitted |
| `TREMBL` | exact | not emitted |
| `UNIPARC` | exact | not emitted |
| `UNIPROT_ISOFORM` | exact | not emitted |
| `REFSEQ_MATCH` | empty in both | not emitted |
| `SOURCE` | exact | exact |
| `REFSEQ_OFFSET` | empty in both | not emitted |
| `GIVEN_REF` | exact | not emitted |
| `USED_REF` | exact | not emitted |
| `BAM_EDIT` | exact | not emitted |
| `GENE_PHENO` | exact | not emitted |
| `SIFT` | exact | exact |
| `PolyPhen` | exact | exact |
| `DOMAINS` | exact | not emitted |
| `miRNA` | exact | not emitted |
| `HGVS_OFFSET` | exact | 93.948% |
| `AF` | exact | 7.798% |
| `AFR_AF` | exact | not emitted |
| `AMR_AF` | exact | not emitted |
| `EAS_AF` | exact | not emitted |
| `EUR_AF` | exact | not emitted |
| `SAS_AF` | exact | not emitted |
| `gnomADe_AF` | exact | not emitted |
| `gnomADe_AFR_AF` | exact | not emitted |
| `gnomADe_AMR_AF` | exact | not emitted |
| `gnomADe_ASJ_AF` | exact | not emitted |
| `gnomADe_EAS_AF` | exact | not emitted |
| `gnomADe_FIN_AF` | exact | not emitted |
| `gnomADe_MID_AF` | exact | not emitted |
| `gnomADe_NFE_AF` | exact | not emitted |
| `gnomADe_REMAINING_AF` | exact | not emitted |
| `gnomADe_SAS_AF` | exact | not emitted |
| `gnomADg_AF` | exact | not emitted |
| `gnomADg_AFR_AF` | exact | not emitted |
| `gnomADg_AMI_AF` | exact | not emitted |
| `gnomADg_AMR_AF` | exact | not emitted |
| `gnomADg_ASJ_AF` | exact | not emitted |
| `gnomADg_EAS_AF` | exact | not emitted |
| `gnomADg_FIN_AF` | exact | not emitted |
| `gnomADg_MID_AF` | exact | not emitted |
| `gnomADg_NFE_AF` | exact | not emitted |
| `gnomADg_REMAINING_AF` | exact | not emitted |
| `gnomADg_SAS_AF` | exact | not emitted |
| `MAX_AF` | exact | not emitted |
| `MAX_AF_POPS` | exact | not emitted |
| `CLIN_SIG` | exact | 99.596% |
| `SOMATIC` | exact | 93.015% |
| `PHENO` | exact | 92.245% |
| `PUBMED` | exact | 99.424% |
| `MOTIF_NAME` | exact | never filled |
| `MOTIF_POS` | exact | never filled |
| `HIGH_INF_POS` | exact | never filled |
| `MOTIF_SCORE_CHANGE` | exact | never filled |
| `TRANSCRIPTION_FACTORS` | exact | never filled |

fastVEP also writes: `REF_ALLELE`, `UPLOADED_ALLELE`, `ACMG`, `ACMG_CRITERIA`

## Cache size on disk, chr22 (MB)

| Engine and format | ensembl, with SIFT/PolyPhen | ensembl, without | merged, with | merged, without |
|---|---|---|---|---|
| Perl VEP Storable (source) | 381.1 | 324.0 | 426.4 | 331.7 |
| vepyr, Parquet | 472.8 | 380.2 | 529.1 | 383.0 |
| fastVEP, `--cache-dir` | 368.2 | 311.7 | 376.5 | 314.8 |
| fastVEP, `.osa2` | 512.4 | 455.9 | 520.7 | 458.9 |

## Run time and memory

Median of the timed runs after one warm-up. CPU-seconds are user plus system time.

| Engine | Mode | Cache | 1 thr wall (s) | 1 thr CPU-s | 1 thr RSS (GiB) | 2 thr wall (s) | 2 thr CPU-s | 2 thr RSS (GiB) |
|---|---|---|---|---|---|---|---|---|
| vepyr | default | ensembl | 9.75 | 5.84 | 0.95 | 2.18 | 4.67 | 1.12 |
| vepyr | default | merged | 12.85 | 7.27 | 1.15 | 3.04 | 5.79 | 1.27 |
| fastVEP | osa2 | ensembl | 17.21 | 14.73 | 2.84 | 12.54 | 14.12 | 3.03 |
| fastVEP | osa2 | merged | 16.09 | 16.04 | 1.99 | 15.5 | 17.0 | 2.79 |
| fastVEP | tabix | ensembl | 9.55 | 9.32 | 0.41 | 8.27 | 9.08 | 0.36 |
| fastVEP | tabix | merged | 10.0 | 9.77 | 0.44 | 9.01 | 9.96 | 0.43 |

## Same output at every thread count

| Engine | Mode | Cache | Identical |
|---|---|---|---|
| vepyr | default | ensembl | yes |
| fastVEP | osa2 | ensembl | yes |
| fastVEP | tabix | ensembl | yes |
| vepyr | default | merged | yes |
| fastVEP | osa2 | merged | yes |
| fastVEP | tabix | merged | yes |

fastVEP `.osa2`, ensembl: found a co-located ID for 50,421 of the 50,431 records Ensembl matches (99.98%); identical ID set on 48,040. These values go to the `FV_VEPCACHE` INFO field, not to CSQ.

fastVEP `.osa2`, merged: found a co-located ID for 50,421 of the 50,431 records Ensembl matches (99.98%); identical ID set on 48,040. These values go to the `FV_VEPCACHE` INFO field, not to CSQ.
