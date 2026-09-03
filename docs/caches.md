# Ensembl VEP caches

vepyr annotates against an **Ensembl VEP cache** converted into a
point-lookup-optimized, per-chromosome Parquet layout by
[`vepyr.build_cache`](api.md). This page describes the cache types, the supported
Ensembl releases, the entities a converted cache contains, and their on-disk
sizes.

!!! tip "Don't want to build one?"
    Prebuilt release 116 caches are published for download — see
    [Download Ensembl VEP and plugin caches](downloads.md).

## Cache types

VEP ships three flavours of the GRCh38 cache, differing only in which transcript
set they carry:

| Type | Transcripts | `build_cache(cache_type=…)` |
|---|---|---|
| **ensembl** | Ensembl/GENCODE transcripts | `ensembl` |
| **refseq** | RefSeq transcripts | `refseq` |
| **merged** | Ensembl **and** RefSeq (both transcript sets) | `merged` |

The variation, regulatory, and motif data are the same across types; only the
transcript/exon/translation entities differ (merged is the union, so it is the
largest).

## Supported releases

- **Release 115** (GRCh38) — the golden reference VCFs in this project are
  produced with the `ensemblorg/ensembl-vep:release_115.2` docker image.
- **Release 116** (GRCh38) — exact VEP 116.0 semantics.

Both are built the same way; a converted cache directory is named
`<release>_<assembly>_<type>`, e.g. `115_GRCh38_merged`, `116_GRCh38_merged`.

### Strict Parquet identity

The current vepyr release supports cache 115 with VEP 115.2 semantics and cache
116 with VEP 116.0 semantics. `build_cache()` derives the raw cache release,
checks it against the requested release, and embeds `bio.vep.cache_version` in
the Arrow schema metadata of every generated Parquet shard. There is no
generated-cache version sidecar.

Before annotating a contig, vepyr checks only that contig's
manifest-referenced shards. Every participating entity must declare the same
supported release and source type. The first contig establishes the invocation
identity; a later contig must agree before any of its rows are annotated. A
chr1-only annotation therefore does not open chr2 Parquet footers.

Missing, malformed, mixed, and unsupported releases are errors. The generated
cache directory name is not a fallback, and `expected_cache_version` can only
assert the independently detected metadata—it cannot label an old
metadata-less cache. Such caches must be rebuilt.

## Layout & entities

`build_cache` downloads the Ensembl cache tarball and converts each **entity**
into a directory of per-chromosome Parquet shards:

```
<cache_dir>/<release>_<assembly>_<type>/
  variation/chr1.parquet … chrY.parquet   (+ non-standard contigs)
  transcript/chr1.parquet …
  exon/…  translation_core/…  translation_sift/…  regulatory/…  motif/…
```

The entities and what they feed in the CSQ output:

| Entity | Feeds | Notes |
|---|---|---|
| **variation** | `Existing_variation`, co-located allele frequencies (1000G, gnomAD exomes/genomes), `MAX_AF*`, `CLIN_SIG`, `SOMATIC`, `PHENO`, `PUBMED` | By far the largest entity; also carries the warm/cold `tier` that plugin caches inherit. |
| **transcript** | `Gene`, `Feature`, `SYMBOL`, `BIOTYPE`, `STRAND`, `CANONICAL`, `MANE*`, `TSL`, `APPRIS`, `CCDS`, source/xref fields | The transcript models; differs by cache type. |
| **exon** | `EXON`/`INTRON` numbering, `cDNA_position`, `CDS_position` | Exon/intron structure. |
| **translation_core** | `Protein_position`, `Amino_acids`, `Codons`, `HGVSp`, `ENSP` | Protein translations. |
| **translation_sift** | `SIFT`, `PolyPhen` | Precomputed missense predictions; the second-largest entity (per-protein blobs). |
| **regulatory** | regulatory-region consequences | Regulatory features. |
| **motif** | TF-binding / motif consequences (`MOTIF_NAME`, `MOTIF_POS`, `HIGH_INF_POS`, `MOTIF_SCORE_CHANGE`, `TRANSCRIPTION_FACTORS`) | Present from release 116; empty in the release-115 caches. |

(The Ensembl `translation` entity is split into two Parquet entities on
conversion: `translation_core` and `translation_sift`.)

## Entity schemas

Each entity directory holds per-chromosome Parquet shards with the column layout
below. These are the schemas **as built** from the release-115 and release-116
GRCh38 caches — the reader derives them from the raw cache's `info.txt` rather
than from a fixed literal, so a different source cache can declare different
optional columns.

Three rules cover every difference:

- **Cache type barely matters.** `transcript`, `exon`, `regulatory` and `motif`
  carry one extra column, `source_refseq` (`string`), in the **refseq** and
  **merged** caches; it is absent from **ensembl**. Nothing else differs, and
  `variation`, `translation_core` and `translation_sift` are byte-identical in
  layout across all three types.
- **Release matters once.** The 116 `variation` schema has one column the 115
  schema does not: `clin_sig_ref_allele` (`string`). 115 `variation` has 24
  columns, 116 has 25.
- **`motif` is 116-only.** In the 115 caches the `motif` directory holds only
  `chrom_manifest.json` — there are no shards, and therefore no 115 motif
  schema.

Column counts (merged cache, release 116):

| Entity | Columns | of which provenance | Cross-type delta |
|---|--:|--:|---|
| `variation` | 25 | 0 | — |
| `transcript` | 79 | 20 | `source_refseq` |
| `exon` | 36 | 20 | `source_refseq` |
| `translation_core` | 10 | 0 | — |
| `translation_sift` | 3 | 0 | — |
| `regulatory` | 32 | 20 | `source_refseq` |
| `motif` | 37 | 20 | `source_refseq` |

### The provenance block

`transcript`, `exon`, `regulatory` and `motif` each end with the same 20-column
provenance block, recording which raw cache the rows came from. It is listed
here once and omitted from the per-entity tables below:

`species`, `assembly`, `cache_version`, `serializer_type`, `source_cache_path`,
`source_file`, then one `string` column per source database declared by the raw
cache — `source_assembly`, `source_clinvar`, `source_cosmic`, `source_dbsnp`,
`source_gencode`, `source_genebuild`, `source_gnomade`, `source_gnomadg`,
`source_hgmd_public`, `source_polyphen`, `source_refseq`, `source_regbuild`,
`source_sift`, `source_src_1000genomes`.

All are `string`. `source_refseq` is the one that disappears in an **ensembl**
cache, leaving 19.

!!! note "`source_cache` is not provenance"
    `transcript` additionally carries a `source_cache` column. That is real
    data — which half of a merged cache a transcript came from — and it backs
    the `SOURCE` CSQ field.

### Per-entity columns

??? note "`variation` — 25 columns"

    The co-located known-variant table: identifiers, clinical significance, and the allele-frequency arrays. `tier` is the warm/cold flag that plugin caches inherit.

    | Column | Type |
    |---|---|
    | `chrom` | `string` |
    | `start` | `uint32` |
    | `end` | `uint32` |
    | `allele_string` | `string` |
    | `failed` | `bool` |
    | `variation_name` | `string` |
    | `clin_sig` | `string` |
    | `clin_sig_allele` | `string` |
    | `clin_sig_ref_allele` | `string` |
    | `clinical_impact` | `string` |
    | `phenotype_or_disease` | `bool` |
    | `pubmed` | `string` |
    | `somatic` | `bool` |
    | `minor_allele` | `string` |
    | `minor_allele_freq` | `double` |
    | `clinvar_ids` | `string` |
    | `cosmic_ids` | `string` |
    | `dbsnp_ids` | `string` |
    | `tier` | `int8` |
    | `af_global_alleles` | `list<item: string>` |
    | `af_global_freqs` | `list<item: float>` |
    | `af_gnomade_alleles` | `list<item: string>` |
    | `af_gnomade_freqs` | `list<item: float>` |
    | `af_gnomadg_alleles` | `list<item: string>` |
    | `af_gnomadg_freqs` | `list<item: float>` |

??? note "`transcript` — 57 columns (+ 20 provenance)"

    The transcript models, including sequences and the nested `exons` / `cdna_mapper_segments` / `refseq_edits` structures. No provenance block shown — see above.

    | Column | Type |
    |---|---|
    | `chrom` | `string` |
    | `start` | `int64` |
    | `end` | `int64` |
    | `strand` | `int8` |
    | `stable_id` | `string` |
    | `db_id` | `int64` |
    | `version` | `int32` |
    | `biotype` | `string` |
    | `source` | `string` |
    | `is_canonical` | `bool` |
    | `gene_stable_id` | `string` |
    | `gene_symbol` | `string` |
    | `gene_symbol_source` | `string` |
    | `gene_hgnc_id` | `string` |
    | `gene_hgnc_id_native` | `string` |
    | `refseq_id` | `string` |
    | `display_xref_id` | `string` |
    | `source_cache` | `string` |
    | `refseq_match` | `string` |
    | `refseq_edits` | `list<item: struct<start: int64 not null, end: int64 not null, repla…` |
    | `is_gencode_basic` | `bool` |
    | `is_gencode_primary` | `bool` |
    | `cds_start` | `int64` |
    | `cds_end` | `int64` |
    | `cdna_coding_start` | `int64` |
    | `cdna_coding_end` | `int64` |
    | `translation_stable_id` | `string` |
    | `translation_start` | `int64` |
    | `translation_end` | `int64` |
    | `exon_count` | `int32` |
    | `exons` | `list<item: struct<start: int64 not null, end: int64 not null, phase…` |
    | `cdna_seq` | `string` |
    | `peptide_seq` | `string` |
    | `codon_table` | `int32` |
    | `tsl` | `int32` |
    | `appris` | `string` |
    | `mane_select` | `string` |
    | `mane_plus_clinical` | `string` |
    | `gene_phenotype` | `bool` |
    | `ccds` | `string` |
    | `swissprot` | `string` |
    | `trembl` | `string` |
    | `uniparc` | `string` |
    | `uniprot_isoform` | `string` |
    | `cds_start_nf` | `bool` |
    | `cds_end_nf` | `bool` |
    | `mature_mirna_regions` | `list<item: struct<start: int64 not null, end: int64 not null>>` |
    | `ncrna_structure` | `string` |
    | `translateable_seq` | `string` |
    | `three_prime_utr_seq` | `string` |
    | `five_prime_utr_seq` | `string` |
    | `cdna_mapper_segments` | `list<item: struct<genomic_start: int64 not null, genomic_end: int64…` |
    | `bam_edit_status` | `string` |
    | `has_non_polya_rna_edit` | `bool` |
    | `spliced_seq` | `string` |
    | `flags_str` | `string` |
    | `transcript_uid` | `uint32` |

??? note "`exon` — 13 columns (+ 20 provenance)"

    Exon/intron structure, one row per exon of a transcript. No provenance block shown — see above.

    | Column | Type |
    |---|---|
    | `chrom` | `string` |
    | `start` | `int64` |
    | `end` | `int64` |
    | `strand` | `int8` |
    | `stable_id` | `string` |
    | `version` | `int32` |
    | `phase` | `int8` |
    | `end_phase` | `int8` |
    | `is_current` | `bool` |
    | `is_constitutive` | `bool` |
    | `transcript_id` | `string` |
    | `gene_stable_id` | `string` |
    | `exon_number` | `int32` |

??? note "`translation_core` — 10 columns"

    Protein translations and their CDS sequences, keyed by transcript.

    | Column | Type |
    |---|---|
    | `transcript_id` | `string` |
    | `stable_id` | `string` |
    | `version` | `int32` |
    | `cds_len` | `int64` |
    | `protein_len` | `int64` |
    | `translation_seq` | `string` |
    | `cds_sequence` | `string` |
    | `translation_seq_canonical` | `string` |
    | `cds_sequence_canonical` | `string` |
    | `protein_features` | `list<item: struct<analysis: string, hseqname: string, start: int64,…` |

??? note "`translation_sift` — 3 columns"

    Precomputed SIFT/PolyPhen matrices as opaque per-protein blobs, keyed by a 64-bit protein `key`.

    | Column | Type |
    |---|---|
    | `key` | `uint64` |
    | `sift` | `binary` |
    | `poly` | `binary` |

??? note "`regulatory` — 10 columns (+ 20 provenance)"

    Regulatory features. No provenance block shown — see above.

    | Column | Type |
    |---|---|
    | `chrom` | `string` |
    | `start` | `int64` |
    | `end` | `int64` |
    | `strand` | `int8` |
    | `stable_id` | `string` |
    | `db_id` | `int64` |
    | `feature_type` | `string` |
    | `epigenome_count` | `int32` |
    | `regulatory_build_id` | `int64` |
    | `cell_types` | `string` |

??? note "`motif` — 15 columns (+ 20 provenance)"

    TF-binding motif features (release 116 only). No provenance block shown — see above.

    | Column | Type |
    |---|---|
    | `chrom` | `string` |
    | `start` | `int64` |
    | `end` | `int64` |
    | `strand` | `int8` |
    | `motif_id` | `string` |
    | `db_id` | `int64` |
    | `score` | `double` |
    | `binding_matrix` | `string` |
    | `binding_matrix_length` | `int32` |
    | `binding_matrix_elements` | `string` |
    | `binding_matrix_unit` | `string` |
    | `motif_seq` | `string` |
    | `cell_types` | `string` |
    | `overlapping_regulatory_feature` | `string` |
    | `transcription_factors` | `string` |

## CSQ output fields

`annotate(output_vcf=…)` writes a `CSQ` INFO field whose `Format:` header lists
the per-transcript fields in output order. **Which fields appear, and in which
order, depends on the cache type and on `everything=`** — vepyr reproduces
Ensembl VEP's own flag-expansion order rather than a fixed list.

Measured against the release-116 GRCh38 caches:

| Cache type | `everything=True` | `everything=False` |
|---|--:|--:|
| `ensembl` | 80 | 74 |
| `refseq` | 85 | 78 |
| `merged` | 86 | 79 |

### The transcript-source block

The whole difference between cache types is one contiguous block of fields, plus
the fate of `SOURCE`:

| Cache type | Block inserted |
|---|---|
| `ensembl` | *(none)* |
| `refseq` | `REFSEQ_MATCH`, `REFSEQ_OFFSET`, `GIVEN_REF`, `USED_REF`, `BAM_EDIT` |
| `merged` | `REFSEQ_MATCH`, **`SOURCE`**, `REFSEQ_OFFSET`, `GIVEN_REF`, `USED_REF`, `BAM_EDIT` |

Where the block lands differs by mode:

- **`everything=True`** — inserted after `UNIPROT_ISOFORM`, before `GENE_PHENO`.
- **`everything=False`** — it *replaces* the `SOURCE` slot, which sits after
  `TRANSCRIPTION_FACTORS` and before `VARIANT_CLASS`.

!!! warning "`SOURCE` is not present in every combination"
    `SOURCE` names which transcript set a consequence came from, so it is only
    meaningful for a merged cache. Its presence is not uniform:

    | | `ensembl` | `refseq` | `merged` |
    |---|---|---|---|
    | `everything=True` | ✗ | ✗ | ✓ |
    | `everything=False` | ✓ | ✗ | ✓ |

    A `refseq` cache never emits `SOURCE`; an `ensembl` cache emits it only
    outside `--everything`. Do not write downstream code that assumes a fixed
    column index — parse the `Format:` header.

### Where the fields come from

For a merged cache with `everything=True` and `flag_pick_allele_gene`, the
87 fields break down as:

| Source | Added fields | Count |
|---|---|--:|
| VCF CSQ fixed base fields | `Allele`, `Consequence`, `IMPACT`, `SYMBOL`, `Gene`, … | 18 |
| `--everything` / `--hgvs` flag-derived, de-duplicated against the base | frequency, MANE, UniProt, HGVS offset, regulatory, … | 59 |
| VEP option-set implication: frequency/pubmed flags enable `check_existing` | `CLIN_SIG`, `SOMATIC`, `PHENO` | 3 |
| `--merged` | `REFSEQ_MATCH`, `SOURCE`, `REFSEQ_OFFSET` | 3 |
| `--flag_pick_allele_gene` | `PICK` | 1 |
| BAM-edited cache auto-enables `--use_transcript_ref` | `GIVEN_REF`, `USED_REF`, `BAM_EDIT` | 3 |
| **Total** | | **87** |

`PICK` is emitted as a standalone field immediately after `FLAGS`, not as a
token inside `FLAGS`.

### Field order

??? note "`ensembl` — `everything=True` (80 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `VARIANT_CLASS` |
    | 23 | `SYMBOL_SOURCE` |
    | 24 | `HGNC_ID` |
    | 25 | `CANONICAL` |
    | 26 | `MANE` |
    | 27 | `MANE_SELECT` |
    | 28 | `MANE_PLUS_CLINICAL` |
    | 29 | `TSL` |
    | 30 | `APPRIS` |
    | 31 | `CCDS` |
    | 32 | `ENSP` |
    | 33 | `SWISSPROT` |
    | 34 | `TREMBL` |
    | 35 | `UNIPARC` |
    | 36 | `UNIPROT_ISOFORM` |
    | 37 | `GENE_PHENO` |
    | 38 | `SIFT` |
    | 39 | `PolyPhen` |
    | 40 | `DOMAINS` |
    | 41 | `miRNA` |
    | 42 | `HGVS_OFFSET` |
    | 43 | `AF` |
    | 44 | `AFR_AF` |
    | 45 | `AMR_AF` |
    | 46 | `EAS_AF` |
    | 47 | `EUR_AF` |
    | 48 | `SAS_AF` |
    | 49 | `gnomADe_AF` |
    | 50 | `gnomADe_AFR_AF` |
    | 51 | `gnomADe_AMR_AF` |
    | 52 | `gnomADe_ASJ_AF` |
    | 53 | `gnomADe_EAS_AF` |
    | 54 | `gnomADe_FIN_AF` |
    | 55 | `gnomADe_MID_AF` |
    | 56 | `gnomADe_NFE_AF` |
    | 57 | `gnomADe_REMAINING_AF` |
    | 58 | `gnomADe_SAS_AF` |
    | 59 | `gnomADg_AF` |
    | 60 | `gnomADg_AFR_AF` |
    | 61 | `gnomADg_AMI_AF` |
    | 62 | `gnomADg_AMR_AF` |
    | 63 | `gnomADg_ASJ_AF` |
    | 64 | `gnomADg_EAS_AF` |
    | 65 | `gnomADg_FIN_AF` |
    | 66 | `gnomADg_MID_AF` |
    | 67 | `gnomADg_NFE_AF` |
    | 68 | `gnomADg_REMAINING_AF` |
    | 69 | `gnomADg_SAS_AF` |
    | 70 | `MAX_AF` |
    | 71 | `MAX_AF_POPS` |
    | 72 | `CLIN_SIG` |
    | 73 | `SOMATIC` |
    | 74 | `PHENO` |
    | 75 | `PUBMED` |
    | 76 | `MOTIF_NAME` |
    | 77 | `MOTIF_POS` |
    | 78 | `HIGH_INF_POS` |
    | 79 | `MOTIF_SCORE_CHANGE` |
    | 80 | `TRANSCRIPTION_FACTORS` |

??? note "`ensembl` — `everything=False` (74 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `SYMBOL_SOURCE` |
    | 23 | `HGNC_ID` |
    | 24 | `MOTIF_NAME` |
    | 25 | `MOTIF_POS` |
    | 26 | `HIGH_INF_POS` |
    | 27 | `MOTIF_SCORE_CHANGE` |
    | 28 | `TRANSCRIPTION_FACTORS` |
    | 29 | `SOURCE` |
    | 30 | `VARIANT_CLASS` |
    | 31 | `CANONICAL` |
    | 32 | `TSL` |
    | 33 | `MANE_SELECT` |
    | 34 | `MANE_PLUS_CLINICAL` |
    | 35 | `ENSP` |
    | 36 | `GENE_PHENO` |
    | 37 | `CCDS` |
    | 38 | `SWISSPROT` |
    | 39 | `TREMBL` |
    | 40 | `UNIPARC` |
    | 41 | `UNIPROT_ISOFORM` |
    | 42 | `AF` |
    | 43 | `AFR_AF` |
    | 44 | `AMR_AF` |
    | 45 | `EAS_AF` |
    | 46 | `EUR_AF` |
    | 47 | `SAS_AF` |
    | 48 | `gnomADe_AF` |
    | 49 | `gnomADe_AFR` |
    | 50 | `gnomADe_AMR` |
    | 51 | `gnomADe_ASJ` |
    | 52 | `gnomADe_EAS` |
    | 53 | `gnomADe_FIN` |
    | 54 | `gnomADe_MID` |
    | 55 | `gnomADe_NFE` |
    | 56 | `gnomADe_REMAINING` |
    | 57 | `gnomADe_SAS` |
    | 58 | `gnomADg_AF` |
    | 59 | `gnomADg_AFR` |
    | 60 | `gnomADg_AMI` |
    | 61 | `gnomADg_AMR` |
    | 62 | `gnomADg_ASJ` |
    | 63 | `gnomADg_EAS` |
    | 64 | `gnomADg_FIN` |
    | 65 | `gnomADg_MID` |
    | 66 | `gnomADg_NFE` |
    | 67 | `gnomADg_REMAINING` |
    | 68 | `gnomADg_SAS` |
    | 69 | `MAX_AF` |
    | 70 | `MAX_AF_POPS` |
    | 71 | `CLIN_SIG` |
    | 72 | `SOMATIC` |
    | 73 | `PHENO` |
    | 74 | `PUBMED` |

??? note "`refseq` — `everything=True` (85 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `VARIANT_CLASS` |
    | 23 | `SYMBOL_SOURCE` |
    | 24 | `HGNC_ID` |
    | 25 | `CANONICAL` |
    | 26 | `MANE` |
    | 27 | `MANE_SELECT` |
    | 28 | `MANE_PLUS_CLINICAL` |
    | 29 | `TSL` |
    | 30 | `APPRIS` |
    | 31 | `CCDS` |
    | 32 | `ENSP` |
    | 33 | `SWISSPROT` |
    | 34 | `TREMBL` |
    | 35 | `UNIPARC` |
    | 36 | `UNIPROT_ISOFORM` |
    | 37 | `REFSEQ_MATCH` |
    | 38 | `REFSEQ_OFFSET` |
    | 39 | `GIVEN_REF` |
    | 40 | `USED_REF` |
    | 41 | `BAM_EDIT` |
    | 42 | `GENE_PHENO` |
    | 43 | `SIFT` |
    | 44 | `PolyPhen` |
    | 45 | `DOMAINS` |
    | 46 | `miRNA` |
    | 47 | `HGVS_OFFSET` |
    | 48 | `AF` |
    | 49 | `AFR_AF` |
    | 50 | `AMR_AF` |
    | 51 | `EAS_AF` |
    | 52 | `EUR_AF` |
    | 53 | `SAS_AF` |
    | 54 | `gnomADe_AF` |
    | 55 | `gnomADe_AFR_AF` |
    | 56 | `gnomADe_AMR_AF` |
    | 57 | `gnomADe_ASJ_AF` |
    | 58 | `gnomADe_EAS_AF` |
    | 59 | `gnomADe_FIN_AF` |
    | 60 | `gnomADe_MID_AF` |
    | 61 | `gnomADe_NFE_AF` |
    | 62 | `gnomADe_REMAINING_AF` |
    | 63 | `gnomADe_SAS_AF` |
    | 64 | `gnomADg_AF` |
    | 65 | `gnomADg_AFR_AF` |
    | 66 | `gnomADg_AMI_AF` |
    | 67 | `gnomADg_AMR_AF` |
    | 68 | `gnomADg_ASJ_AF` |
    | 69 | `gnomADg_EAS_AF` |
    | 70 | `gnomADg_FIN_AF` |
    | 71 | `gnomADg_MID_AF` |
    | 72 | `gnomADg_NFE_AF` |
    | 73 | `gnomADg_REMAINING_AF` |
    | 74 | `gnomADg_SAS_AF` |
    | 75 | `MAX_AF` |
    | 76 | `MAX_AF_POPS` |
    | 77 | `CLIN_SIG` |
    | 78 | `SOMATIC` |
    | 79 | `PHENO` |
    | 80 | `PUBMED` |
    | 81 | `MOTIF_NAME` |
    | 82 | `MOTIF_POS` |
    | 83 | `HIGH_INF_POS` |
    | 84 | `MOTIF_SCORE_CHANGE` |
    | 85 | `TRANSCRIPTION_FACTORS` |

??? note "`refseq` — `everything=False` (78 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `SYMBOL_SOURCE` |
    | 23 | `HGNC_ID` |
    | 24 | `MOTIF_NAME` |
    | 25 | `MOTIF_POS` |
    | 26 | `HIGH_INF_POS` |
    | 27 | `MOTIF_SCORE_CHANGE` |
    | 28 | `TRANSCRIPTION_FACTORS` |
    | 29 | `REFSEQ_MATCH` |
    | 30 | `REFSEQ_OFFSET` |
    | 31 | `GIVEN_REF` |
    | 32 | `USED_REF` |
    | 33 | `BAM_EDIT` |
    | 34 | `VARIANT_CLASS` |
    | 35 | `CANONICAL` |
    | 36 | `TSL` |
    | 37 | `MANE_SELECT` |
    | 38 | `MANE_PLUS_CLINICAL` |
    | 39 | `ENSP` |
    | 40 | `GENE_PHENO` |
    | 41 | `CCDS` |
    | 42 | `SWISSPROT` |
    | 43 | `TREMBL` |
    | 44 | `UNIPARC` |
    | 45 | `UNIPROT_ISOFORM` |
    | 46 | `AF` |
    | 47 | `AFR_AF` |
    | 48 | `AMR_AF` |
    | 49 | `EAS_AF` |
    | 50 | `EUR_AF` |
    | 51 | `SAS_AF` |
    | 52 | `gnomADe_AF` |
    | 53 | `gnomADe_AFR` |
    | 54 | `gnomADe_AMR` |
    | 55 | `gnomADe_ASJ` |
    | 56 | `gnomADe_EAS` |
    | 57 | `gnomADe_FIN` |
    | 58 | `gnomADe_MID` |
    | 59 | `gnomADe_NFE` |
    | 60 | `gnomADe_REMAINING` |
    | 61 | `gnomADe_SAS` |
    | 62 | `gnomADg_AF` |
    | 63 | `gnomADg_AFR` |
    | 64 | `gnomADg_AMI` |
    | 65 | `gnomADg_AMR` |
    | 66 | `gnomADg_ASJ` |
    | 67 | `gnomADg_EAS` |
    | 68 | `gnomADg_FIN` |
    | 69 | `gnomADg_MID` |
    | 70 | `gnomADg_NFE` |
    | 71 | `gnomADg_REMAINING` |
    | 72 | `gnomADg_SAS` |
    | 73 | `MAX_AF` |
    | 74 | `MAX_AF_POPS` |
    | 75 | `CLIN_SIG` |
    | 76 | `SOMATIC` |
    | 77 | `PHENO` |
    | 78 | `PUBMED` |

??? note "`merged` — `everything=True` (86 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `VARIANT_CLASS` |
    | 23 | `SYMBOL_SOURCE` |
    | 24 | `HGNC_ID` |
    | 25 | `CANONICAL` |
    | 26 | `MANE` |
    | 27 | `MANE_SELECT` |
    | 28 | `MANE_PLUS_CLINICAL` |
    | 29 | `TSL` |
    | 30 | `APPRIS` |
    | 31 | `CCDS` |
    | 32 | `ENSP` |
    | 33 | `SWISSPROT` |
    | 34 | `TREMBL` |
    | 35 | `UNIPARC` |
    | 36 | `UNIPROT_ISOFORM` |
    | 37 | `REFSEQ_MATCH` |
    | 38 | `SOURCE` |
    | 39 | `REFSEQ_OFFSET` |
    | 40 | `GIVEN_REF` |
    | 41 | `USED_REF` |
    | 42 | `BAM_EDIT` |
    | 43 | `GENE_PHENO` |
    | 44 | `SIFT` |
    | 45 | `PolyPhen` |
    | 46 | `DOMAINS` |
    | 47 | `miRNA` |
    | 48 | `HGVS_OFFSET` |
    | 49 | `AF` |
    | 50 | `AFR_AF` |
    | 51 | `AMR_AF` |
    | 52 | `EAS_AF` |
    | 53 | `EUR_AF` |
    | 54 | `SAS_AF` |
    | 55 | `gnomADe_AF` |
    | 56 | `gnomADe_AFR_AF` |
    | 57 | `gnomADe_AMR_AF` |
    | 58 | `gnomADe_ASJ_AF` |
    | 59 | `gnomADe_EAS_AF` |
    | 60 | `gnomADe_FIN_AF` |
    | 61 | `gnomADe_MID_AF` |
    | 62 | `gnomADe_NFE_AF` |
    | 63 | `gnomADe_REMAINING_AF` |
    | 64 | `gnomADe_SAS_AF` |
    | 65 | `gnomADg_AF` |
    | 66 | `gnomADg_AFR_AF` |
    | 67 | `gnomADg_AMI_AF` |
    | 68 | `gnomADg_AMR_AF` |
    | 69 | `gnomADg_ASJ_AF` |
    | 70 | `gnomADg_EAS_AF` |
    | 71 | `gnomADg_FIN_AF` |
    | 72 | `gnomADg_MID_AF` |
    | 73 | `gnomADg_NFE_AF` |
    | 74 | `gnomADg_REMAINING_AF` |
    | 75 | `gnomADg_SAS_AF` |
    | 76 | `MAX_AF` |
    | 77 | `MAX_AF_POPS` |
    | 78 | `CLIN_SIG` |
    | 79 | `SOMATIC` |
    | 80 | `PHENO` |
    | 81 | `PUBMED` |
    | 82 | `MOTIF_NAME` |
    | 83 | `MOTIF_POS` |
    | 84 | `HIGH_INF_POS` |
    | 85 | `MOTIF_SCORE_CHANGE` |
    | 86 | `TRANSCRIPTION_FACTORS` |

??? note "`merged` — `everything=False` (79 fields)"

    | # | Field |
    |---:|---|
    | 1 | `Allele` |
    | 2 | `Consequence` |
    | 3 | `IMPACT` |
    | 4 | `SYMBOL` |
    | 5 | `Gene` |
    | 6 | `Feature_type` |
    | 7 | `Feature` |
    | 8 | `BIOTYPE` |
    | 9 | `EXON` |
    | 10 | `INTRON` |
    | 11 | `HGVSc` |
    | 12 | `HGVSp` |
    | 13 | `cDNA_position` |
    | 14 | `CDS_position` |
    | 15 | `Protein_position` |
    | 16 | `Amino_acids` |
    | 17 | `Codons` |
    | 18 | `Existing_variation` |
    | 19 | `DISTANCE` |
    | 20 | `STRAND` |
    | 21 | `FLAGS` |
    | 22 | `SYMBOL_SOURCE` |
    | 23 | `HGNC_ID` |
    | 24 | `MOTIF_NAME` |
    | 25 | `MOTIF_POS` |
    | 26 | `HIGH_INF_POS` |
    | 27 | `MOTIF_SCORE_CHANGE` |
    | 28 | `TRANSCRIPTION_FACTORS` |
    | 29 | `REFSEQ_MATCH` |
    | 30 | `SOURCE` |
    | 31 | `REFSEQ_OFFSET` |
    | 32 | `GIVEN_REF` |
    | 33 | `USED_REF` |
    | 34 | `BAM_EDIT` |
    | 35 | `VARIANT_CLASS` |
    | 36 | `CANONICAL` |
    | 37 | `TSL` |
    | 38 | `MANE_SELECT` |
    | 39 | `MANE_PLUS_CLINICAL` |
    | 40 | `ENSP` |
    | 41 | `GENE_PHENO` |
    | 42 | `CCDS` |
    | 43 | `SWISSPROT` |
    | 44 | `TREMBL` |
    | 45 | `UNIPARC` |
    | 46 | `UNIPROT_ISOFORM` |
    | 47 | `AF` |
    | 48 | `AFR_AF` |
    | 49 | `AMR_AF` |
    | 50 | `EAS_AF` |
    | 51 | `EUR_AF` |
    | 52 | `SAS_AF` |
    | 53 | `gnomADe_AF` |
    | 54 | `gnomADe_AFR` |
    | 55 | `gnomADe_AMR` |
    | 56 | `gnomADe_ASJ` |
    | 57 | `gnomADe_EAS` |
    | 58 | `gnomADe_FIN` |
    | 59 | `gnomADe_MID` |
    | 60 | `gnomADe_NFE` |
    | 61 | `gnomADe_REMAINING` |
    | 62 | `gnomADe_SAS` |
    | 63 | `gnomADg_AF` |
    | 64 | `gnomADg_AFR` |
    | 65 | `gnomADg_AMI` |
    | 66 | `gnomADg_AMR` |
    | 67 | `gnomADg_ASJ` |
    | 68 | `gnomADg_EAS` |
    | 69 | `gnomADg_FIN` |
    | 70 | `gnomADg_MID` |
    | 71 | `gnomADg_NFE` |
    | 72 | `gnomADg_REMAINING` |
    | 73 | `gnomADg_SAS` |
    | 74 | `MAX_AF` |
    | 75 | `MAX_AF_POPS` |
    | 76 | `CLIN_SIG` |
    | 77 | `SOMATIC` |
    | 78 | `PHENO` |
    | 79 | `PUBMED` |

## Cache format & lookup internals

--8<-- "includes/cache-internals.md"

## Sizing

Measured on-disk sizes of the converted Parquet caches in
`/Users/mwiewior/workspace/data_vepyr` (GRCh38):

| Cache | Total | variation | translation_sift | transcript | translation_core | exon | regulatory | motif |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `115_GRCh38_ensembl` | **29 G** | 26 G | 2.7 G | 533 M | 196 M | 178 M | 71 M | — |
| `115_GRCh38_refseq`  | **29 G** | 26 G | 2.8 G | 295 M | 156 M | 73 M  | 71 M | — |
| `115_GRCh38_merged`  | **32 G** | 26 G | 5.5 G | 720 M | 348 M | 240 M | 71 M | — |
| `116_GRCh38_ensembl` | **32 G** | 27 G | 4.1 G | 633 M | 263 M | 211 M | 71 M | 89 M |
| `116_GRCh38_refseq`  | **31 G** | 27 G | 2.9 G | 296 M | 156 M | 73 M | 71 M | 89 M |
| `116_GRCh38_merged`  | **36 G** | 27 G | 7.0 G | 818 M | 418 M | 272 M | 71 M | 89 M |

Observations:

- **`variation` dominates** every cache (~26–27 G, ~80–90% of the total) — it is
  the full set of known variants with population frequencies.
- **`translation_sift` is the clear #2** and scales with the transcript set:
  merged (both transcript sets) is ~2× the single-set caches, and 116 is larger
  than 115.
- **`merged` ≈ `ensembl` + `refseq`** for the transcript/exon/translation
  entities (it carries both), while sharing the same variation/regulatory data.
- **`motif`** is populated only from release **116** (empty in the 115 caches).

## Building a cache

```python
import vepyr

vepyr.build_cache(
    release=116,
    cache_dir="/Users/mwiewior/workspace/data_vepyr",
    cache_type="merged",          # "ensembl" | "refseq" | "merged"
    assembly="GRCh38",
    partitions=8,                 # conversion parallelism
)
# → /Users/mwiewior/workspace/data_vepyr/116_GRCh38_merged/<entity>/chr*.parquet
```

For a targeted rebuild, use the same release-aware public contract:

```python
vepyr.build_cache_entity(
    release=116,
    cache_dir="/Users/mwiewior/workspace/data_vepyr",
    entity="motif",
    cache_type="merged",
    local_cache="/data/ensembl-vep/homo_sapiens_merged/116_GRCh38",
    overwrite=True,
)
```

Valid raw entities are `variation`, `transcript`, `exon`, `translation`,
`regulatory`, and `motif`. `translation` produces both `translation_core` and
`translation_sift`. The targeted builder derives the expected Parquet cache
version from `release` and rejects a conflicting raw-cache release/source
before writing output, exactly like the full builder.

### Rebuilding a single contig

`chroms` restricts the rebuild to named contigs; omit it to rebuild every
contig, scaffolds included. One contig of one entity takes seconds rather than
the hours a full conversion needs, which is what makes a targeted cache fix
practical to iterate on:

```python
vepyr.build_cache_entity(
    release=116,
    cache_dir="/Users/mwiewior/workspace/data_vepyr",
    entity="translation",
    cache_type="merged",
    local_cache="/data/ensembl-vep/homo_sapiens_merged/116_GRCh38",
    overwrite=True,
    chroms=["chrX"],
)
```

Indicative timings for `translation` on the 116 merged cache: about 30 s for a
single contig, 926 s for chr1–22, and roughly 26 min for every contig including
scaffolds.

Two cautions when using this to patch a cache you intend to publish:

- **Restricting `chroms` leaves every other contig at its previous build.** The
  result is a cache of mixed generations. A published cache holds far more than
  the 24 main contigs — 1,744 `translation_core` files for 116 merged — so
  rebuild every contig before uploading, or the scaffolds silently stay stale.
- **The entity is the unit of rebuild, not the output.** Raw `translation`
  always writes both `translation_core` and `translation_sift`, but a given fix
  may only change one of them. Compare the rebuilt shards against the previous
  ones and upload only what differs; that is often the difference between
  shipping a few hundred MB and many GB.

See the [API reference](api.md#vepyr.build_cache) for the full signature. Plugin
caches (e.g. AlphaMissense) are built separately and layered on top — see
[Plugins](plugins.md).
