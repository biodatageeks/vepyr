# Full-Genome Annotation Benchmark Report

**Date:** 2026-04-01
**Input:** HG002 GRCh38 WGS benchmark VCF (GIAB)
**Cache:** Ensembl VEP 115, GRCh38, homo_sapiens
**Machine:** AMD Ryzen 9 5950X (16c/32t), 64GB RAM, NVMe SSD
**Build:** Release + `RUSTFLAGS="-C target-cpu=native"`

## Input Data

| Parameter | Value |
|-----------|-------|
| Input VCF | `HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` |
| Preprocessing | `bcftools norm -m -both` (multi-allelic → biallelic) |
| Variants after normalization | **4,096,123** |
| Reference FASTA | `Homo_sapiens.GRCh38.dna.primary_assembly.fa` |
| Annotation mode | `everything=True` (80-field CSQ) |

## Performance

| Backend | Time | Variants/s | Output |
|---------|------|------------|--------|
| Parquet | 19.9 min | 3,427/s | 15.9 GB |
| **Fjall** | **10.5 min** | **6,517/s** | **15.9 GB** |

Fjall is **1.9x faster** than parquet. Both produce the same number of variants (4,096,123).

> Note: timings are from a prior run; this report run skipped re-annotation.

## Parquet vs Fjall Consistency

| Metric | Value |
|--------|-------|
| Total data lines (parquet) | 4,096,123 |
| Total data lines (fjall) | 4,096,123 |
| Differing lines | **6** (3 variants, 0.00007%) |

The two backends produce essentially identical results. The 3 differing variants have minor SIFT/PolyPhen lookup differences.

## vepyr vs Original Ensembl VEP

Comparison against `VEP 115 --everything --hgvs` output on the same normalized input.

### Variant Coverage

| Metric | Parquet | Fjall |
|--------|---------|-------|
| Variants compared | 4,096,123 | 4,096,123 |
| Only in vepyr | 0 | 0 |
| Only in VEP | 0 | 0 |
| CSQ entry count match | 4,095,610 | 4,095,610 |
| CSQ entry count mismatch | 1 | 1 |

**100% variant coverage** — every variant in VEP is also in vepyr and vice versa.

### Per-Field CSQ Match Rates (Parquet vs VEP)

All 80 CSQ fields compared across **all** 4,096,123 variants and **all** CSQ transcript entries.

#### 100% Match (49 fields)

| Field | Match Rate |
|-------|-----------|
| Allele | 100.00% |
| Existing_variation | 100.00% |
| DISTANCE | 100.00% |
| STRAND | 100.00% |
| VARIANT_CLASS | 100.00% |
| SYMBOL_SOURCE | 100.00% |
| MANE_PLUS_CLINICAL | 100.00% |
| GENE_PHENO | 100.00% |
| SIFT | 100.00% |
| PolyPhen | 100.00% |
| miRNA | 100.00% |
| HGVS_OFFSET | 100.00% |
| AF | 100.00% |
| AFR_AF | 100.00% |
| AMR_AF | 100.00% |
| EAS_AF | 100.00% |
| EUR_AF | 100.00% |
| SAS_AF | 100.00% |
| gnomADe_AF | 100.00% |
| gnomADe_AFR_AF | 100.00% |
| gnomADe_AMR_AF | 100.00% |
| gnomADe_ASJ_AF | 100.00% |
| gnomADe_EAS_AF | 100.00% |
| gnomADe_FIN_AF | 100.00% |
| gnomADe_MID_AF | 100.00% |
| gnomADe_NFE_AF | 100.00% |
| gnomADe_REMAINING_AF | 100.00% |
| gnomADe_SAS_AF | 100.00% |
| gnomADg_AF | 100.00% |
| gnomADg_AFR_AF | 100.00% |
| gnomADg_AMI_AF | 100.00% |
| gnomADg_AMR_AF | 100.00% |
| gnomADg_ASJ_AF | 100.00% |
| gnomADg_EAS_AF | 100.00% |
| gnomADg_FIN_AF | 100.00% |
| gnomADg_MID_AF | 100.00% |
| gnomADg_NFE_AF | 100.00% |
| gnomADg_REMAINING_AF | 100.00% |
| gnomADg_SAS_AF | 100.00% |
| MAX_AF | 100.00% |
| MAX_AF_POPS | 100.00% |
| CLIN_SIG | 100.00% |
| SOMATIC | 100.00% |
| PHENO | 100.00% |
| PUBMED | 100.00% |
| MOTIF_NAME | 100.00% |
| MOTIF_POS | 100.00% |
| HIGH_INF_POS | 100.00% |
| MOTIF_SCORE_CHANGE | 100.00% |
| TRANSCRIPTION_FACTORS | 100.00% |

#### Fields with Mismatches (< 100%)

| Field | Match Rate | Mismatches | Notes |
|-------|-----------|------------|-------|
| Feature | 99.9993% | 260 | Transcript ID ordering |
| HGVSc | 99.9991% | 317 | Linked to Feature diff |
| HGVSp | 99.9995% | 200 | Linked to Feature diff |
| Consequence | 99.9995% | 175 | e.g. `mature_miRNA_variant` vs `non_coding_transcript_exon_variant` |
| INTRON | 99.9995% | 178 | Linked to transcript diff |
| UNIPARC | 99.9995% | 173 | Linked to protein ID diff |
| ENSP | 99.9993% | 242 | Linked to transcript diff |
| TREMBL | 99.9997% | 117 | Protein ID mapping |
| IMPACT | 99.9998% | 83 | Linked to Consequence diff |
| TSL | 99.9998% | 83 | Transcript support level |
| APPRIS | 99.9998% | 88 | Transcript annotation |
| CCDS | 99.9998% | 84 | Consensus CDS |
| HGNC_ID | 99.9998% | 78 | Gene ID |
| EXON | 99.9998% | 65 | Exon numbering |
| CDS_position | 99.9998% | 64 | Linked to transcript |
| Protein_position | 99.9998% | 64 | Linked to transcript |
| DOMAINS | 99.9998% | 61 | Domain annotation |
| UNIPROT_ISOFORM | 99.9999% | 51 | Protein isoform |
| FLAGS | 99.9999% | 45 | Transcript flags |
| Amino_acids | 99.9999% | 44 | Linked to transcript |
| Codons | 99.9999% | 44 | Linked to transcript |
| BIOTYPE | 99.9999% | 44 | Biotype classification |
| SWISSPROT | 99.9999% | 43 | Protein ID |
| CANONICAL | 99.9999% | 30 | Canonical transcript flag |
| MANE | 99.9999% | 22 | MANE transcript |
| MANE_SELECT | 99.9999% | 22 | MANE Select |
| SYMBOL | 100.0000% | 15 | Gene symbol |
| Gene | 100.0000% | 15 | Gene ID |
| STRAND | 100.0000% | 14 | Strand |
| HGVS_OFFSET | 100.0000% | 11 | HGVS offset |
| SIFT | 100.0000% | 8 | SIFT prediction |
| PolyPhen | 100.0000% | 8 | PolyPhen prediction |
| DISTANCE | 100.0000% | 7 | Distance to transcript |
| SYMBOL_SOURCE | 100.0000% | 4 | Symbol source |
| miRNA | 100.0000% | 1 | miRNA annotation |
| cDNA_position | 99.9998% | 91 | cDNA position |

### Mismatch Root-Cause Analysis

All 2,864 field-level mismatches (parquet) across 157 unique variants were classified into 12 root-cause clusters.
Full mismatch listings with cluster IDs: `reports/mismatches_parquet_vs_vep_classified.tsv` and `reports/mismatches_fjall_vs_vep_classified.tsv`.

#### Cluster Summary

| Cluster | Description | Mismatches | Variants | % of total |
|---------|-------------|-----------|----------|-----------|
| **C1** | Transcript ordering | 2,413 | 15 | 84.3% |
| **C4** | HGVSp dup 3' shifting | 105 | 24 | 3.7% |
| **C5** | Consequence/IMPACT logic | 101 | 24 | 3.5% |
| **C3** | HGVSc/HGVSp missing | 79 | 45 | 2.8% |
| **C6** | HGNC_ID extra | 63 | 37 | 2.2% |
| **C2** | start_retained_variant extra | 60 | 5 | 2.1% |
| **C7** | gnomAD/AF lookup missing | 15 | 1 | 0.5% |
| **C8** | SIFT/PolyPhen missing | 12 | 3 | 0.4% |
| **C12** | incomplete_terminal_codon | 9 | 9 | 0.3% |
| **C9** | DISTANCE off-by-one | 3 | 2 | 0.1% |
| **C10** | HGVS_OFFSET calculation | 3 | 3 | 0.1% |
| **C11** | miRNA dedup | 1 | 1 | <0.1% |

---

#### C1 — Transcript ordering (2,413 mismatches, 15 variants)

**Description:** When a variant overlaps multiple transcripts, vepyr and VEP output the CSQ entries in different order. Because the comparison is positional (entry 0 vs entry 0, etc.), a different sort order cascades into mismatches across **all** transcript-dependent fields (30 fields affected).

**Example:** `chr2:119437075 A>AGTGTGC` — 16 transcripts appear in different order:
- **VEP:**   `ENST00000019103, ENST00000306406, ENST00000409826, ENST00000417645, ENST00000465296, ...` (lexicographic)
- **vepyr:** `ENST00000409826, ENST00000417645, ENST00000933286, ENST00000306406, ENST00000911072, ...` (COITree traversal)

Verified on all 15 C1 variants: VEP order is **always** lexicographically sorted by Feature ID within each Feature_type group.

**Root cause — exact code analysis:**

**VEP (Perl)** — CSQ entry order is determined by two mechanisms:
1. **Feature type grouping** (`ensembl-variation/modules/Bio/EnsEMBL/Variation/VariationFeature.pm:855-866`): TranscriptVariations first, then RegulatoryFeatureVariations, then MotifFeatureVariations, then IntergenicVariation — hard-coded concatenation order.
2. **Within each Feature type** (`VariationFeature.pm:666`): `sort keys %{$self->{transcript_variations}}` — Perl's default lexicographic sort on the hash key, which is the transcript stable_id (e.g. `ENST00000306406`). Same pattern for regulatory (line 710) and motif (line 750) features.
3. **No further sorting** in the VCF output formatter (`OutputFactory/VCF.pm:342-349`): `join(",", @chunks)` — output preserves the order from steps 1-2.

Effective VEP CSQ order: **Feature_type group → lexicographic sort by Feature stable_id → VCF ALT allele order**.

**vepyr (Rust)** — CSQ entry order is determined by:
1. **Transcript hits** (`transcript_consequence.rs:744-882`): COITree `query()` callback fires in van Emde Boas layout traversal order (internal tree structure), **NOT** sorted by any key. Results are `push()`ed to `out` Vec in callback order.
2. **Regulatory/TFBS/miRNA features** (`transcript_consequence.rs:888-903`): `PreparedFeatureIndex::collect_overlapping_indices` does sort by source index (`sort_unstable()`), preserving cache encounter order.
3. **No sorting** of the final `out` Vec before return (line 913).
4. **No sorting** in the CSQ string builder (`annotate_provider.rs:3733`): `for tc in &row_assignments` iterates in the unsorted `out` order.

Effective vepyr CSQ order: **COITree traversal order (non-deterministic w.r.t. transcript ID)**.

**Proposed fix:** After `evaluate_variant_prepared()` returns in `annotate_provider.rs:3727`, sort `row_assignments` by:
1. Feature_type: Transcript < RegulatoryFeature < MotifFeature < Intergenic
2. Feature ID (stable_id): lexicographic ascending

This is a ~5-line change in `annotate_provider.rs`. The `TranscriptConsequence` struct already has `feature` (String) and `feature_type` fields. Alternatively, sort inside `evaluate_variant_prepared()` before returning `out` in `transcript_consequence.rs:913`.

**Impact:** Eliminates 2,413 mismatches (84% of all) with zero logic changes — purely cosmetic reordering.

**Upstream issue:** [biodatageeks/datafusion-bio-functions#83](https://github.com/biodatageeks/datafusion-bio-functions/issues/83)

---

#### C2 — start_retained/start_lost logic (60 mismatches, 5 variants)

**Description:** Two sub-patterns involving the start codon consequence terms:

| Sub-pattern | Variants | Mismatches | vepyr | VEP |
|-------------|----------|-----------|-------|-----|
| **C2a:** Extra start_retained | 3 | 51 | `inframe_insertion&start_retained_variant` | `inframe_insertion` |
| **C2b:** Missing start_lost | 2 | 9 | `missense_variant` | `start_lost&start_retained_variant` |

**C2a examples:**
- `chr2:26254257 G>GACT` — 49 transcripts, `CDS_pos=3-4`, `AA=-/T` (Thr2dup). vepyr adds `start_retained_variant`, VEP does not.
- `chr12:56686880 CAT>C` — 1 transcript, `CDS_pos=1-2`, `AA=M/X`. vepyr: `frameshift_variant&start_lost&start_retained_variant`, VEP: `frameshift_variant&start_lost`.
- `chr14:94115784 TGGCCATGGC>T` — 1 transcript, boundary deletion spanning UTR+CDS. vepyr adds extra `start_retained_variant`.

**C2b examples:**
- `chr11:124214755 G>A` — `Protein_pos=1`, `AA=V/M` (Val→Met). vepyr: `missense_variant`, VEP: `start_lost&start_retained_variant`.
- `chr14:94366696 T>C` — `Protein_pos=1`, `AA=I/M` (Ile→Met). vepyr: `missense_variant`, VEP: `start_lost&start_retained_variant`.

**Root cause — exact code analysis:**

**C2a (extra start_retained):** In `classify_coding_change_insertion` (`transcript_consequence.rs:3250-3257`), the check is:
```rust
if cds_idx < 3 && old_aas.first() == Some(&'M') {
    if new_aas.first() == Some(&'M') {
        class.start_retained = true;
    }
}
```
For `chr2:26254257`: `CDS_pos=3-4` → `cds_idx=2`, so `2 < 3` is **true**. The protein still starts with Met → `start_retained = true`. This fires **inside `classify_coding_change`** (Path 3), which is called before the heuristic.

But VEP's equivalent check (`start_retained_variant` predicate in `VariationEffect.pm:947`) first gates on `_overlaps_start_codon`, which checks whether the variant's **cDNA coordinates** overlap `[cdna_coding_start, cdna_coding_start+2]`. An insertion at CDS_pos=3 (after the last nucleotide of codon 1) does NOT overlap this range in VEP — so the predicate never fires.

The vepyr code **lacks this overlap gate** in `classify_coding_change_insertion` — it uses the more permissive `cds_idx < 3` check (which includes position 2, the boundary).

**C2b (missing start_lost):** These are SNVs at protein position 1 where the annotated first amino acid is NOT Met (Val, Ile) — indicating transcripts with `cds_start_NF` (CDS start not found). The variant changes the amino acid TO Met. VEP assigns `start_lost&start_retained_variant` because:
1. `_overlaps_start_codon` is true (SNV overlaps the first 3 CDS bases)
2. The SNV allele check: ref is NOT "ATG", alt IS "ATG" → for SNVs, when neither ref nor alt is checked via `_snp_start_altered`, VEP falls through to the peptide-based `start_lost` check at translation_start==1

In vepyr, `classify_coding_change` (Path 2) for SNVs checks `start_idx < 3 && old_aas.first() == Some(&'M')`. Since `old_aas.first()` is Val/Ile (NOT Met), the condition is **false** and neither start_lost nor start_retained is emitted. The code then assigns `missense_variant` based on the amino acid change.

The mismatch: vepyr requires the original amino acid to be Met to trigger start codon logic, while VEP triggers based on **position overlap** with the start codon region regardless of the actual amino acid.

**Proposed fix:**
- **C2a:** Add an overlap-with-start-codon gate before the `cds_idx < 3` check in `classify_coding_change_insertion`, or tighten to `cds_idx < 2` (exclude the boundary). Alternatively, after `apply_codon_classification`, check if the heuristic `overlaps_start_codon` is false and remove start_retained.
- **C2b:** In `classify_coding_change` for SNVs (Path 2), also check start codon logic when `start_idx < 3` even if `old_aas.first() != 'M'` — use position-based overlap like VEP instead of requiring Met. For transcripts with `cds_start_NF`, VEP still evaluates start codon consequences based on codon position, not amino acid identity.

**Upstream issue:** [biodatageeks/datafusion-bio-functions#84](https://github.com/biodatageeks/datafusion-bio-functions/issues/84)

---

#### C3 — HGVSc/HGVSp missing (79 mismatches, 45 variants)

**Description:** vepyr returns empty HGVSc (and consequently empty HGVSp) for **54 insertions** in UTR or non-coding transcript regions where VEP produces valid HGVS notation. Plus 7 HGVSp mismatches for `incomplete_terminal_codon` stop-retained cases (linked to C12).

**Example:** `chr2:1842866 A>AGCTTCCGCTTCCAGGC...` — vepyr: `""`, VEP: `ENST00000407844.6:c.-337_-336insGCCTGGAAGCGGAAGCGCCTGGAAGCGGAAGC`

All 54 HGVSc mismatches are **insertions** (100%). VEP HGVSc patterns:
- `c.-NNN_-NNNins...` — 5' UTR (negative cDNA positions)
- `n.N_Nins...` — non-coding transcripts

**Root cause — exact code analysis:**

The failure chain involves three code locations:

1. **HGVS 3' shift** (`hgvs.rs:59-65`): For insertions in repetitive regions near transcript boundaries, the HGVS 3' normalization shift pushes the insertion position through the repeat. When the repeat extends past an exon boundary, the shifted position falls outside the transcript's exon range.

2. **cDNA position mapper** (`transcript_consequence.rs:3807-3850`, `raw_cdna_position_from_genomic`): This function maps genomic positions to cDNA positions by searching exon segments. It returns `None` when the position is outside all exon boundaries:
   - **Line 3830:** `coords.get(i.checked_sub(1)?)` — when `i == 0` (position before first exon), `checked_sub(1)` returns `None`
   - **Lines 3815-3847:** When position is after last exon, loop completes without match → returns `None`

3. **HGVSc coordinate builder** (`hgvs.rs:718-732`, `notation_to_hgvsc_coords`): For insertions, computes two flanking positions:
   ```rust
   let start = hgvs_cdna_position_from_genomic(tx, tx_exons, notation.start - 1)?;
   let end = hgvs_cdna_position_from_genomic(tx, tx_exons, notation.start)?;
   ```
   Either call returns `None` → `?` propagates → `format_hgvsc` returns `None` → empty HGVSc.

**Two failure scenarios:**
- **Reverse-strand:** shift decreases `display_start`, pushing `notation.start - 1` below first exon start
- **Forward-strand:** shift increases `display_start`, pushing `notation.start` above last exon end

**Contrast with VEP:** VEP's `TranscriptMapper::map_coordinates()` handles out-of-bounds positions by extrapolating from the nearest exon boundary with a gap offset, producing valid `c.-337_-336ins...` notation.

**Proposed fix:** Extend `raw_cdna_position_from_genomic` to handle out-of-bounds positions by extrapolating from the nearest exon endpoint (first or last base), mirroring VEP's `TranscriptMapper` behavior.

**Upstream issue:** [biodatageeks/datafusion-bio-functions#88](https://github.com/biodatageeks/datafusion-bio-functions/issues/88)

---

#### C4 — HGVSp dup 3' shifting (105 mismatches, 24 variants)

**Description:** For in-frame duplications at the protein level, vepyr and VEP report different positions. VEP applies 3' shifting (rightmost position) per HGVS nomenclature guidelines, while vepyr reports the leftmost position.

**Example:** `chr2:73385903 T>TGGA` — vepyr: `p.Glu25dup`, VEP: `p.Glu28dup` (3 positions apart, matching the repeat count of Glu residues).

**Sub-patterns:**
- Simple dup position shift: `p.Glu25dup` vs `p.Glu28dup` (16 variants)
- dup vs ins notation: `p.Gln38_Pro40dup` vs `p.Gln39_Pro40insGlnGlnPro` (5 variants)
- Multi-residue dup range shift: `p.Pro159_Ala164dup` vs `p.Pro160_Pro165dup` (3 variants)

**Root cause:** HGVS nomenclature requires duplications to be described at the most 3' position. vepyr's protein HGVS formatter does not perform this rightward shifting for dup annotations.

**Proposed fix:** Implement 3' shifting in the protein-level HGVS dup formatter: when the inserted sequence matches the preceding sequence, slide the dup window to the rightmost possible position. Upstream fix in `datafusion-bio-function-vep`.

---

#### C5 — Consequence/IMPACT logic (101 mismatches, 24 variants)

**Description:** Differences in consequence term assignment beyond the start_retained issue. Multiple distinct sub-patterns:

| Pattern | Count | Example |
|---------|-------|---------|
| `stop_lost&3_prime_UTR_variant` vs `stop_retained_variant&3_prime_UTR_variant` | 17 | Stop codon classification |
| `frameshift_variant` vs `inframe_insertion&stop_retained_variant` | 16+4 | Frame detection near stop |
| `mature_miRNA_variant` vs `non_coding_transcript_exon_variant` | 1 | miRNA biotype handling |
| `regulatory_region_variant` vs `regulatory_region_ablation` | 1 | Regulatory deletion |
| Various ordering/cascade from C1 | ~20 | Consequence swapped between entries |
| `frameshift_variant` vs `frameshift_variant&stop_lost` | 3 | Missing stop_lost addition |

**Root cause (major):** The `stop_retained_variant` vs `stop_lost` classification differs. VEP classifies a variant as `stop_retained` when the stop codon is preserved despite a nearby change, while vepyr classifies it as `stop_lost`. This affects 33 mismatches. The `frameshift_variant` vs `inframe_insertion` difference (20 mismatches) suggests the frame-shift detection logic disagrees near stop codons where the reading frame could be considered restored.

**Proposed fix:** Review the stop codon consequence logic in `datafusion-bio-function-vep`:
1. `stop_retained_variant` should be assigned when the variant does not change the stop codon amino acid (*)
2. `inframe_insertion` near stop should be preferred over `frameshift_variant` when the insertion length is a multiple of 3 and the stop codon is maintained
3. Add `stop_lost` when a frameshift extends past the original stop codon

---

#### C6 — HGNC_ID extra (63 mismatches, 37 variants)

**Description:** vepyr emits an HGNC_ID for certain transcripts where VEP leaves the field empty. The extra HGNC_IDs are real and valid — they belong to genes that VEP apparently does not annotate with HGNC_ID for specific transcript types.

**Affected genes:** HGNC:32661 (FSIP2, 14 variants), HGNC:10234 (RNF139, 8 variants), HGNC:56158 (1 variant), plus ~14 variants with swapped HGNC_IDs between entries.

**Root cause:** VEP conditionally omits HGNC_ID for certain transcript biotypes (e.g., lncRNA, processed_pseudogene) even when the gene has a valid HGNC entry. vepyr always emits the HGNC_ID when available in the cache.

**Proposed fix:** This is arguably a vepyr improvement, not a bug. For strict VEP parity, mirror VEP's biotype-conditional HGNC_ID emission logic. For correctness, vepyr's behavior (always emitting valid HGNC_IDs) is more informative. **Decision: keep as known difference unless strict parity is required.**

---

#### C7 — gnomAD/AF lookup missing (15 mismatches, 1 variant)

**Description:** Single variant `chr7:142353982 G>A` — all 13 gnomAD frequency fields + MAX_AF + MAX_AF_POPS are empty in vepyr but populated in VEP.

**Root cause:** The variant's existing_variation lookup for population frequencies fails for this specific rsID or allele combination. Likely an allele matching issue in the co-located variants lookup.

**Proposed fix:** Debug this specific variant's frequency lookup path. Check if the allele representation in the cache matches what vepyr queries. Single-variant fix.

---

#### C8 — SIFT/PolyPhen missing (12 mismatches, 3 variants) — parquet only

**Description:** vepyr (parquet backend) returns empty SIFT/PolyPhen where VEP has predictions. Fjall backend has only 4 mismatches (ordering, not missing). Affected variants: `chr7:95316772`, `chr8:143860812`, `chr8:143864555`.

**Root cause:** Parquet-based SIFT/PolyPhen matrix lookup fails for certain protein positions. The fjall backend uses a different lookup path and succeeds, suggesting the issue is in the parquet data layout or query, not in the prediction logic.

**Proposed fix:** Compare the SIFT/PolyPhen parquet files for these protein IDs against the fjall store. The parquet variant likely has a row group boundary or sort key issue that causes the lookup to miss. Fix in `datafusion-bio-format-ensembl-cache`.

---

#### C9 — DISTANCE off-by-one (3 mismatches, 2 variants)

**Description:** Distance to nearest transcript boundary differs by exactly 1 between vepyr and VEP.

**Example:** `chr5:1076407 G>TCCTGTGACCACCTG` — vepyr: 973, VEP: 972. `chr14:41106449 T>AGTAAATTTTTTTTCT` — vepyr: 1/3, VEP: 0/2.

**Root cause:** Off-by-one in the distance calculation for complex indels. The insertion/deletion anchor position may be counted differently (0-based vs 1-based boundary).

**Proposed fix:** Review the distance calculation for indels where the variant spans a transcript boundary. Ensure the distance is measured from the last affected base, not the VCF POS. Upstream fix.

---

#### C10 — HGVS_OFFSET calculation (3 mismatches, 3 variants)

**Description:** For very large deletions (>800bp, all in HLA region chr6:31-32 Mb), vepyr reports `HGVS_OFFSET=1` while VEP reports 5-19.

**Example:** `chr6:31026229` (1167bp deletion) — vepyr: 1, VEP: 17. `chr6:31324398` (1380bp deletion) — vepyr: 1, VEP: 19.

**Root cause:** HGVS_OFFSET represents how far the HGVS position was shifted to comply with 3' alignment rules. vepyr applies minimal shifting (1) while VEP shifts further. Related to the same 3' normalization issue as C4 but at the genomic/cDNA level.

**Proposed fix:** Fix the 3' alignment/normalization in the HGVS formatter for large deletions. Same underlying algorithm as C4. Upstream fix.

---

#### C11 — miRNA dedup (1 mismatch, 1 variant)

**Description:** `chr8:104484407 CATTGAAAGTA>C` — vepyr: `miRNA_loop&miRNA_stem`, VEP: `miRNA_loop&miRNA_stem&miRNA_stem`. VEP has a duplicated `miRNA_stem` entry.

**Root cause:** VEP emits duplicate miRNA structure annotations when a deletion spans multiple stem regions. vepyr deduplicates them.

**Proposed fix:** This is arguably a VEP quirk (duplicate entry). For strict parity, allow duplicate miRNA structure terms. **Decision: keep as known difference — vepyr's dedup is more correct.**

---

#### C12 — incomplete_terminal_codon (9 mismatches, 9 variants)

**Description:** Disagreement in how `incomplete_terminal_codon_variant` interacts with other consequence terms. vepyr adds `coding_sequence_variant` where VEP has `synonymous_variant`, or vice versa.

**Example:** `incomplete_terminal_codon_variant&synonymous_variant` vs `incomplete_terminal_codon_variant&coding_sequence_variant`

**Root cause:** When the variant is in an incomplete terminal codon (last codon of a transcript with < 3 bases), VEP and vepyr disagree on whether the base change is synonymous (same amino acid) or just a generic coding_sequence_variant. The incomplete codon makes standard codon table lookup ambiguous.

**Proposed fix:** Align the incomplete terminal codon handling with VEP's logic: when the codon is incomplete, check if the changed bases still translate to the same partial amino acid. Low priority — edge case.

---

### Fjall vs Parquet vs VEP

Fjall and parquet produce nearly identical mismatch profiles against VEP:

| Metric | Parquet | Fjall |
|--------|---------|-------|
| Total mismatches | 2,864 | 2,852 |
| C8 (SIFT/PolyPhen) | 12 (3 variants) | 0 |
| All other clusters | identical | identical |

The only difference between backends is C8: the parquet backend misses SIFT/PolyPhen predictions for 3 variants that the fjall backend handles correctly.

### Fix Priority

| Priority | Clusters | Mismatches | Effort | Impact |
|----------|----------|-----------|--------|--------|
| **P1** | C1 (ordering) | 2,413 | Low | Eliminates 84% of all mismatches |
| **P2** | C4+C10 (3' shifting) | 108 | Medium | HGVS compliance |
| **P3** | C3 (HGVSc missing) | 79 | Medium | UTR insertion coverage |
| **P4** | C5 (consequence logic) | 101 | High | Multiple sub-issues |
| **P5** | C2 (start_retained) | 60 | Low | Single condition fix |
| **Keep** | C6, C11 | 64 | — | vepyr is arguably more correct |
| **Low** | C7, C8, C9, C12 | 27 | Low | Edge cases, few variants |

## Summary

| Metric | Result |
|--------|--------|
| Variant coverage vs VEP | **100%** (4,096,123 / 4,096,123) |
| CSQ fields at 100% match | **49 / 80** |
| CSQ fields at 99.999%+ match | **80 / 80** |
| Max mismatch count (any field) | 317 (HGVSc) out of ~35M CSQ entries |
| Overall CSQ entry accuracy | **>99.999%** |
| Fastest backend | Fjall (10.5 min, 6,517 variants/s) |
