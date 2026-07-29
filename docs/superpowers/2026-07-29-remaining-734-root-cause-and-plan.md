# The remaining 734: corrected root-cause analysis and dual-release plan

- **Date:** 2026-07-29
- **Target:** exact Ensembl VEP parity for cache releases 115 and 116
- **Profile:** `merged`, `--everything --hgvs`, HG002 GRCh38, chr1–22
- **Historical 116 baseline (`20260728_2105`):** 734 field mismatches over 4,096,123
  variants and 66,362,749 CSQ entries
- **Structure:** exact — 0 entry-count mismatches, 0 ordering mismatches, 0 one-sided variants
- **Field parity:** 76 of 86 CSQ fields at 100%

This document supersedes the first version of this analysis. It reconciles the aggregate reports,
the current vepyr dependency worktrees, the generated 115 and 116 Parquet caches, the raw Ensembl
caches, the exact VEP 115.2 and 116.0 source trees, and the detailed
[115 → 116 changelog](../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md).

It follows
[2026-07-28-chr1-22-parity-116.md](2026-07-28-chr1-22-parity-116.md), which reduced the mismatch
count from 31,699 to 734.

The executable implementation sequence is
[2026-07-29-vep-115-116-dual-release-parity.md](plans/2026-07-29-vep-115-116-dual-release-parity.md).

---

## 1. Executive conclusion

The main conclusion remains valid, with tighter boundaries:

- **612 of 734 fields are release-semantic differences** where the measured vepyr build
  implemented VEP 115 behavior and VEP 116 changed:
  - 388 partial-transcript HGVS fields;
  - 224 stop-codon consequence/IMPACT/HGVSp fields.
- **112 fields are caused by one VEP 116 cache/output feature**:
  `clin_sig_ref_allele`. The raw 116 cache contains it, but the generated vepyr variation cache
  used for the measured run dropped it.
- **10 fields are ordinary vepyr coordinate/state defects**, not release-policy differences:
  - 6 at one CDS/UTR boundary;
  - 2 protein-position fields at one exon boundary;
  - 2 RefSeq repeat-shift fields.

Only two behaviors need a release gate:

1. partial-overlap transcript HGVS;
2. the VEP 116 stop-predicate family.

The implementation activates ClinVar behavior from the presence of `clin_sig_ref_allele` and
applies the three residual fixes unconditionally. `IMPACT` and HGVSp do not have their own release
switches because they follow the consequence-term decision.

All six mismatch classes now have source-, cache-, and fixture-backed explanations. In
particular, the chr11 failed-BAM-edit state is closed: the 116 cache marks NM_002457.5 as
`bam_edit_status=failed`, VEP still performs the normal genomic 3′ shift, and the formatter can
receive the deletion reference rotated by that shift. The failed-edit predicate must accept both
the original normalized allele and the rotated `USED_REF` representation.

---

## 2. Evidence and provenance

### 2.1 Exact VEP releases

The reference images and headers identify these source/data combinations:

| Release | Image | Ensembl core | ensembl-variation | GENCODE | ClinVar |
|---|---|---|---|---|---|
| 115 | `ensemblorg/ensembl-vep:release_115.2` | `266b84d` | `b7c2637` | 49 | 202502 |
| 116 | `ensemblorg/ensembl-vep:release_116.0` | `c0cf13d` | `2fb834b` | 50 | 202509 |

The behavioral conclusions below were checked against the shipped source from these images, not
against `main`.

### 2.2 Historical benchmark versus the current local implementation

The `20260728_2105` benchmark summary records vepyr build `bb37297` and bio-functions revision
`e551ccc4e256`. Those revisions identify the historical 734-mismatch input to this analysis; they
do not identify the implementation used for the post-fix qualification runs.

The current local implementation is split into reviewable commits. The distinction between the
source tip and the installed native extension is explicit because a parity report is evidence for
the binary that produced it, not for a later unbuilt source commit:

| Repository | Current local revision | Qualification state |
|---|---|---|
| vepyr | `edd2995` | exact dependency-pin commit on `release-testing`; the preceding implementation commits are `6a2191b`, `72c6a8a`, `3f2f5eb`, and `54f97fa` |
| bio-functions PR [#203](https://github.com/biodatageeks/datafusion-bio-functions/pull/203) | `0d02d711b352baf4087e2e9421e12716e10bb290` | pushed dual-release/cache semantics and source-trace fixes, plus the exact bio-formats pin |
| bio-formats PR [#224](https://github.com/biodatageeks/datafusion-bio-formats/pull/224) | `eee2d6926331fe5106cbbefbc1ca673e94357327` | pushed Parquet cache identity persistence |

[Cargo.toml](../../Cargo.toml) now pins those exact pushed revisions. Commit `edd2995` removes all
absolute worktree patches, and `Cargo.lock` records only durable Git sources for the VEP
dependencies. The pre-pin reports remain valuable semantic qualification evidence, but the final
release gates must be regenerated from the clean native extension built from this pinned source.

### 2.3 Reports

The canonical mismatch totals come from the per-chromosome
`fast_chr*_merged_116_report.json` files. The combined
[116 summary](../../e2e-testing/reports/fast_chr1_chr22_merged_116_summary_20260728_2105.md)
correctly reports the 734 mismatch fields and structural parity.

The performance artifacts are not internally identical:

- the summary table says 251.8 seconds and 16,267 variants/s;
- the current 22 JSON `annotation.time_s` values sum to 250.5 seconds and 16,352 variants/s.

This does not affect correctness analysis, but the release run must regenerate one immutable
summary from one immutable set of JSON reports.

### 2.4 Limitation of the stored mismatch examples

The JSON reports contain exact aggregate counts but cap stored mismatch examples per field and
chromosome. The partition in §4 is the unique arithmetic reconciliation of:

- the ten exact field totals;
- the known chr11, chr16, and chr20 residuals;
- the VEP source deltas;
- the sampled mismatch values.

Before declaring zero, the comparator must emit an uncapped ledger keyed by variant, output
allele, Feature, field, vepyr value, and VEP value. That is a verification requirement, not an
unresolved alternative explanation for the current totals.

---

## 3. What the 115 baseline does and does not prove

The old statement “release 115 had zero mismatches on all chr1–22” was too strong.

The historical `fast_chr{1..22}_merged_report.json` files contain all 4,096,123 input variants,
but:

- chr2 has 331,324 annotated variants and `comparison: null`;
- chr4 has 307,295 annotated variants and `comparison: null`;
- the other 20 chromosomes contain 3,457,504 compared variants and zero field mismatches.

The most recent pre-contract, release-aware
[chr22 115 report](../../e2e-testing/reports/fast_chr22_merged_115_report.json) compares 50,861
variants with zero mismatches.

Therefore the accurate historical conclusion is:

> Historical data gives a strong 115-compatibility signal over 20 chromosomes, and a
> release-aware pre-contract build was exact on chr22; the historical artifacts alone did not
> prove a current-build chr1–22 VEP 115 comparison.

That missing comparison has now been performed. The pre-pin native-release
[chr1–22 VEP 115 merged report](../../e2e-testing/reports/fast_chr1_chr22_merged_115_summary_20260729_1928.md),
built with bio-functions through `daad832`, compares all 4,096,123 variants and reports zero
field, structure, ordering, or one-sided mismatches across all 86 fields; every per-contig ledger
is empty. The later `9a6f0fe` change has a paired V115 regression proving unchanged local
classification, but this full report still does not replace the final same-binary pinned gate or
either 115 single-source gate.

### Why a single-release parity suite missed these changes

1. **Both-silent equality.** VEP 115 and vepyr both omitted partial-boundary HGVSc. The comparator
   correctly called the strings equal but did not distinguish “equal and non-empty” from “both
   absent.” A both-silent count would have exposed this unexercised output surface.
2. **The port targeted 115.** The measured HGVS boundary bail and stop-codon logic were 115-style
   implementations. A suite against only 115 cannot detect a future semantic change.
3. **New cache content.** Some relevant 116 transcripts do not exist in 115, including the two
   chr16 transcripts localized in §8.2 and the chr20 transcript in §8.1.

---

## 4. Reconciled mismatch partition

The original classes overlapped: “HGVS boundary = 390” included the chr11 shift residual, and
“stop rework = 232” included all eight chr16/chr20 coding-field residuals. The non-overlapping
working partition is:

| # | Root cause | Fields | Count | Certainty |
|---|---|---|---:|---|
| 1 | VEP 116 partial-transcript HGVS | 291 HGVSc + 97 HGVS_OFFSET | **388** | source-proven; exact count reconciled from residuals |
| 2 | VEP 116 stop-predicate family | 112 Consequence + 99 IMPACT + 13 HGVSp | **224** | source-proven family; exact count reconciled from residuals |
| 3 | ClinVar reference-allele strand handling | CLIN_SIG | **112** | proven from source, raw cache, generated schema, and all three variants |
| 4 | RefSeq repeat shift at chr11 | HGVSc + HGVS_OFFSET | **2** | VEP source, 115/116 cache state, allele rotation, and exact fixture proven |
| 5 | CDS/3′ UTR insertion boundary at chr20 | Consequence, IMPACT, CDS_position, Protein_position, Amino_acids, Codons | **6** | code predicate and cache geometry proven |
| 6 | Exon-boundary insertion at chr16 | Protein_position | **2** | missing symmetric mapper branch proven |
|  | **Total** |  | **734** |  |

Arithmetic:

```text
388 + 224 + 112 + 2 + 6 + 2 = 734
```

The original 390 HGVS fields are `292 HGVSc + 98 HGVS_OFFSET`; subtracting the chr11 HGVSc and
HGVS_OFFSET leaves 291 + 97 = 388 partial-boundary fields.

The original 232 non-HGVS/non-ClinVar fields include the six chr20 fields and two chr16 fields;
subtracting those leaves 224 stop-family fields.

---

## 5. Class 1 — partial-transcript HGVS (388)

### 5.1 Root cause

VEP 115 `TranscriptVariationAllele::_var2transcript_slice_coords` rejects a variant if either
slice coordinate is outside the transcript. VEP 116 changed it to reject only a wholly outside
variant and clamp a partial overlap:

```perl
my $tr_length = $tr_end - $tr_start + 1;
return undef if (
  ($vf_start < 1 && $vf_end < 1) ||
  ($vf_start > $tr_length && $vf_end > $tr_length)
);
my $clamped_start = $vf_start < 1 ? 1 :
                    $vf_start > $tr_length ? $tr_length : $vf_start;
my $clamped_end   = $vf_end < 1 ? 1 :
                    $vf_end > $tr_length ? $tr_length : $vf_end;
```

See the exact source comparison in
[§3.1 of the changelog](../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md#31-hgvs-descriptions-are-now-produced-for-partial-transcript-overlaps).

`$vf_start` and `$vf_end` are transcript-slice coordinates, already oriented for the transcript
strand. VEP later rejects a shifted result that overruns the slice:

```perl
return undef
  if $slice_length < ($_slice_end + $offset_to_add);
```

The measured vepyr build returned `None` for any overhanging deletion in
`bio-function-vep/src/hgvs.rs`, and its fallback replaced the original coordinates with a
materialized genomic shift interval.

That representation mismatch explains the failed attempts:

- VEP clamps the **unshifted transcript-slice interval**, then applies a non-negative shift
  offset in slice orientation;
- vepyr materializes the shift as a genomic interval and substitutes it before formatting.

Those operations are not interchangeable at transcript boundaries.

### 5.2 Required semantics

For a deletion that intersects but overhangs a transcript:

1. Convert both genomic ends to transcript-slice coordinates:
   - forward: `g - transcript_start + 1`;
   - reverse: `transcript_end - g + 1`, with the interval ordered in transcript orientation.
2. Return no HGVSc and no HGVS_OFFSET if both ends lie below 1 or both lie above transcript length.
3. Clamp both ends to `[1, transcript_length]`.
4. Retain the 3′ shift as a slice-space offset; do not replace the interval with materialized
   genomic display coordinates.
5. Return no HGVSc and no HGVS_OFFSET if `clamped_end + offset > transcript_length`.
6. Otherwise format the shifted, clamped interval and apply VEP-equivalent allele clipping.

The implemented behavior is enabled only under `VepSemantics::V116`.
`VepSemantics::V115` preserves the measured build's early return.

### 5.3 Implemented shape

The implementation computes the following boundary-only values:

```text
unclamped_slice_start
unclamped_slice_end
clamped_slice_start
clamped_slice_end
shift_offset
transcript_length
strand
```

The non-boundary path remains unchanged. The implementation did not need to materialize VEP
transcript genomic slices or add sequence I/O: it performs the clamp and offset in transcript
slice space, maps the accepted interval back to genomic space, and crops the exact deletion
reference to the genomic overlap.

This avoids eager materialization of VEP’s transcript genomic slices; chr1 alone contains about
3.86 Gbp of aggregate transcript span.

Hydrated cDNA is not a substitute for VEP’s genomic transcript slice because cDNA omits introns.

The branch `wip/hgvs-transcript-clip-116` is not an implementation to merge as-is: tip `7fe0179`
reverts parent `88e586f`.

### 5.4 Expected yield

```text
291 HGVSc + 97 HGVS_OFFSET = 388
```

The chr11 repeat-shift pair in §8.3 is explicitly excluded.

---

## 6. Class 2 — VEP 116 stop-predicate family (224)

### 6.1 What changed

The shipped 115.2 → 116.0 diff contains **nine behavioral hunks**, not six. They are documented
in full in
[§2 of the changelog](../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md#2-consequence-calling--the-stop-codon-predicate-family):

| # | VEP 116 behavior |
|---|---|
| 1 | `inframe_insertion` is false when reference and alternate peptides are both `*` |
| 2 | `inframe_deletion` is false when the reference peptide is `*` |
| 3 | `stop_gained` is false when `stop_lost` is true |
| 4 | `stop_lost` is false for a partial terminal codon |
| 5 | `stop_lost` requires an alternate peptide without `X` |
| 6 | `stop_retained` requires an alternate peptide without `X` |
| 7 | the no-alt `stop_retained` branch uses genomic-coordinate `_cil` stop helpers |
| 8 | `ref_eq_alt_sequence` removes the old first-peptide clause and tightens final-stop matching |
| 9 | `frameshift` is false when the affected reference peptide starts with `*` |

The apparent `$consider_ins_len` change to the old helpers is dead code in shipped VEP 116. Its
only caller leaves the argument undefined, and the new behavior uses separate `_cil` helpers.
**Do not port that flag.**

`Utils/Constants.pm` is byte-identical between releases: consequence terms, ranks, and IMPACT
values did not change. All IMPACT changes are downstream of a different selected term.

### 6.2 Current Rust behavior

The Rust implementation is a consolidated classifier rather than a literal copy of the Perl
predicate layout. In the measured build, important 115-style behavior remained:

- insertion classification still implements the old `ref_eq_alt_sequence` first-peptide clause
  that 116 deleted;
- a `stop_retained` classification converts frameshift to inframe regardless of length;
- stop preservation is primarily evaluated in cDNA/CDS space rather than with the new genomic
  `_cil` overlap.

The implemented fix adds one semantics policy to the central classification layer and implements
the nine **net behaviors** there; it does not port dead `$consider_ins_len` plumbing.

### 6.3 Downstream cascade

The 224 fields partition as:

```text
112 Consequence + 99 IMPACT + 13 HGVSp = 224
```

HGVSp changes such as `p.Ter...fs...` → `p.Ter...delins...` are not a separate formatter release
rule. VEP’s protein-HGVS formatter asks the consequence predicate whether the allele is a
frameshift; changing the term changes the notation type.

Consequently:

- gate the consequence semantics once;
- derive IMPACT from the resulting term set;
- let HGVSp consume the same classification;
- do not add independent IMPACT or HGVSp release conditionals.

### 6.4 Required tests

Create paired 115/116 golden fixtures for all nine hunks, including:

- plus and minus strands;
- insertions and deletions;
- terminal `X`;
- partial terminal codons;
- `stop_gained`/`stop_lost` mutual exclusion;
- genomic `_cil` overlap across an intron;
- the deleted and surviving `ref_eq_alt_sequence` clauses.

The same fixture input must produce the 115 answer under `V115` and the 116 answer under `V116`.

### 6.5 Expected yield

```text
112 Consequence + 99 IMPACT + 13 HGVSp = 224
```

The chr20 Consequence/IMPACT pair and all chr16/chr20 coordinate fields are excluded.

---

## 7. Class 3 — CLIN_SIG (112): root cause closed

### 7.1 VEP 116 behavior

VEP 116 added `clin_sig_ref_allele` to the variation cache and changed
`OutputFactory::add_colocated_variant_info`:

1. read the live `VariationFeature` reference allele;
2. compare it with the cached ClinVar reference allele;
3. if they differ, reverse-complement each labelled ClinVar risk allele;
4. match the transformed risk allele to the output allele.

See
[§7.2](../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md#72-clinvar-significance-is-matched-against-a-strand-flipped-allele)
and
[§9.5](../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md#95-clinvar-reference-allele-plumbed-from-database-to-cache)
of the changelog.

The raw cache schemas prove the data delta:

- 115 `variation_cols` has `clin_sig_allele` but no `clin_sig_ref_allele`;
- 116 inserts `clin_sig_ref_allele` immediately after `clin_sig_allele`.

### 7.2 Exact reconciliation of all three variants

| Variant | Entries | Cached labelled allele | Cached ClinVar ref | Live ref | 116 transformation and result |
|---|---:|---|---|---|---|
| `chr3:42210085 C>CGGAGGA` | 41 | `GGAGGA:benign` | `AGG` | `C` | `GGAGGA → TCCTCC`; no output-allele match, CLIN_SIG blank |
| `chr15:89333596 T>TTGC` | 63 | `TGC:conflicting…` | `TGC` | `T` | `TGC → GCA`; no output-allele match, CLIN_SIG blank |
| `chr14:74506880 C>CGCGCGCAT` | 8 | `ATGCGCGC:benign` | `ATGCGCGC` | `C` | `ATGCGCGC → GCGCGCAT`; matches output allele, `benign` |

```text
41 + 63 + 8 = 112
```

This explains both over-call and under-call directions.

### 7.3 Where the measured build lost the field

The bio-formats variation reader appends unknown columns from `variation_cols`, so it could expose
`clin_sig_ref_allele` without a special hard-coded field.

The field is dropped later:

1. `bio-function-vep/src/cache/schema.rs::VARIATION_REQUIRED_COLUMNS` omits it;
2. generated 115 and 116 variation Parquet schemas therefore both omit it;
3. warm/cold lookup projections omit it;
4. `ColocatedCacheEntry` and `ColocatedEntry` omit it;
5. the clinical label matcher uses the original label without the VEP 116 reference comparison.

The earlier `matched_alleles.is_empty()` hypothesis is disproven as the cause of these 112 fields.
Do not tighten that behavior as part of this fix.

### 7.4 Implemented data path

1. `clin_sig_ref_allele` is in the generated variation cache projection/schema as an **optional**
   nullable UTF-8 field. It cannot join `VARIATION_REQUIRED_COLUMNS`, because the raw 115 schema
   legitimately lacks it.
2. Warm and cold variation lookup projections preserve it.
3. `ColocatedCacheEntry`, deduplication, and `ColocatedEntry` carry it.
4. Labelled clinical matching compares the cached value with the live VF reference.
5. It reverse-complements the label allele when they differ, then performs the existing output-allele
   lookup.
6. Exact tests cover the three variants above, including both over-call and under-call shapes.
7. The required full 116 cache rebuild will materialize the field in the variation data.
   The other 116 entities are rebuilt to satisfy the all-shard metadata contract, not because
   ClinVar needs them.

This is data-driven and requires no release switch:

- a 116 cache with the field activates the behavior;
- a 115 cache has null/absent `clin_sig_ref_allele` and preserves the old path.

---

## 8. Release-independent residuals (10)

### 8.1 chr20 CDS/3′ UTR boundary — 6 fields

Variant:

```text
chr20:45840343 A>AC
Feature: ENST00000984773.1
```

The 116 transcript cache gives:

```text
strand = +1
cds_end = 45840343
cdna_coding_end = 594
```

The normalized insertion lies immediately after the VCF padding base at the CDS end. VEP emits:

```text
3_prime_UTR_variant&NMD_transcript_variant
IMPACT=MODIFIER
CDS_position=
Protein_position=
Amino_acids=
Codons=
```

vepyr emits:

```text
inframe_insertion&stop_retained_variant&NMD_transcript_variant
IMPACT=MODERATE
CDS_position=462-463
Protein_position=154
Amino_acids=-/X
Codons=-/C
```

The measured `insertion_left_flank_in_cds` documented VEP’s inverted insertion condition
correctly:

```text
coding_start <= padding_position
coding_end >= padding_position + 1
```

but the plus-strand implementation accepts `left_flank == cds_end`. That routes this allele into
the coding branch and prevents the later 3′ UTR branch from firing.

The implementation excludes `left_flank >= cds_end`, routes the terminal insertion to the 3′ UTR,
and adds terminal and internal coding-exon fixtures. The transcript is new in 116, but the
coordinate rule is not release-specific.

### 8.2 chr16 exon-boundary protein span — 2 fields

Variant:

```text
chr16:5072071 G>GGTCT
```

The two mismatching Features are:

- `ENST00001096215.1`;
- `ENST00001096224.1`.

Both are present in 116 and absent in 115. Their first coding exons end at the insertion anchor.
Only the primary genomic flank maps into the CDS. VEP emits:

```text
CDS_position=222-223
Protein_position=74-75
```

vepyr computes the same CDS span but emits `Protein_position=74`.

`classify_insertion` has branches for:

- both flanks mapped;
- primary missing and alternate mapped;

but no symmetric branch for primary mapped and alternate missing. It therefore sets
`ins_at_boundary=false` and collapses the protein span.

The implementation makes the boundary test symmetric when exactly one anchor maps and includes
an exact fixture with `CDS_position=222-223` and `Protein_position=74-75`. This is
release-independent.

### 8.3 chr11 RefSeq repeat shift — 2 fields

Variant:

```text
chr11:1094638 ACCAACCACCACTCCCAGCCCT>A
Feature: NM_002457.5
```

VEP 115 and VEP 116 are identical:

```text
HGVSc=NM_002457.5:c.4443_4463del
HGVS_OFFSET=47
GIVEN_REF=CCAACCACCACTCCCAGCCCT
USED_REF=CACCACTCCCAGCCCTCCAAC
```

vepyr emits:

```text
HGVSc=NM_002457.5:c.4396_4416del
HGVS_OFFSET=
```

The 47-base coordinate difference exactly matches the missing HGVS_OFFSET. The cache and VEP
source inspection close the state transition:

- the generated 115 transcript row has no `bam_edit_status`;
- the generated 116 row has `bam_edit_status="failed"`, no retained `refseq_edits`, and
  `has_non_polya_rna_edit=false`;
- VEP `Transcript::apply_edits` removes failed `_rna_edit` attributes but does not disable the
  ordinary `_return_3prime()` genomic shift;
- the 21-base deleted sequence shifted by 47 bases is rotated left by `47 mod 21 = 5`, converting
  `CCAACCACCACTCCCAGCCCT` into VEP’s
  `CACCACTCCCAGCCCTCCAAC` `USED_REF`.

The pre-fix `hgvsc_uses_genomic_shift` failed-edit gate accepted only the original normalized
payload. The implementation accepts either the original normalized allele or the
shift-rotated allele, while continuing to reject unrelated formatter alleles. Exact
NM_002457.5 fixtures assert the normalized input state,
`HGVSc=NM_002457.5:c.4443_4463del`, `HGVS_OFFSET=47`, and the VEP
`GIVEN_REF`/`USED_REF` pair; the prior keep/suppress failed-edit fixtures remain passing.

The fix is unconditional because VEP 115 and 116 agree.

---

## 9. Cache comparison and implications

The 116 cache is not a small refresh. Counts below sum the canonical
chr1–22 + X + Y + MT Parquet shards:

| Entity | 115 rows | 116 rows | Change |
|---|---:|---:|---:|
| Exon | 6,145,408 | 7,561,968 | +23.05% |
| Regulatory | 380,956 | 380,956 | 0% |
| Transcript | 719,305 | 857,234 | +19.18% |
| Translation core | 399,450 | 537,034 | +34.44% |
| Translation SIFT | 142,824,504 | 230,074,931 | +61.09% |
| Variation | 1,112,354,246 | 1,429,070,556 | +28.47% |
| Motif | no Parquet data | 999,828 | new |

Key provenance changes:

- GENCODE 49 → 50;
- RefSeq `RS_2024_08` → `RS_2025_08`;
- ClinVar 202502 → 202509;
- COSMIC 101 → 102.

These data changes explain why new transcripts expose previously untested coordinate shapes.
They do not explain the source-proven HGVS and stop-policy changes.

### 9.1 Existing generated caches do not identify their release

Both raw `info.txt` files omit a `version`/`cache_version` line. The pre-fix bio-formats reader
parsed such a line if present but otherwise stored `"unknown"`. The generated transcript Parquet
files currently on disk have:

- a `cache_version` column whose value is `"unknown"`;
- schema metadata for `bio.vep.cache_source_type`;
- no `bio.vep.cache_version` metadata.

Therefore the earlier statement “release detection already works” was incorrect for the measured
code and the actual generated caches.

The implemented bio-formats change uses strict fallback from only the final canonical raw-cache
root name (`115_GRCh38...` or `116_GRCh38...`), rejects an explicit-info/basename conflict, and
adds `bio.vep.cache_version` to every raw provider schema. The generated-cache builder verifies
that detected identity before writing and preserves it in every output schema. It never infers an
annotation-cache release from a parent directory or generated-cache path.

---

## 10. Minimal dual-release semantics

### 10.1 One release value, two gated policies

The implementation introduces one enum:

```rust
enum VepSemantics {
    V115,
    V116,
}
```

It resolves this value from each requested contig's Parquet shard metadata, memoizes the
validated identity, and passes it into the transcript consequence engine. It does not inspect
unrequested contig shards or parse filenames in annotation hot paths.

Only these policy decisions consume it:

```text
partial_overlap_hgvs
stop_codon_predicates
```

Do not gate:

- ClinVar reference-allele handling — activate from field presence;
- chr11/chr16/chr20 fixes — general correctness;
- IMPACT — derive it from terms;
- HGVSp — derive it from the same coding classification.

### 10.2 Resolution precedence

Each vepyr package release must declare the exact reference targets it supports. For the current
implementation target:

| Cache | VEP | API | Ensembl core | Variation | Semantics |
|---|---|---|---|---|---|
| `115` | `115.2` | `115` | `266b84d` | `b7c2637` | `V115` |
| `116` | `116.0` | `116` | `c0cf13d` | `2fb834b` | `V116` |

Resolve annotation-cache identity only from `bio.vep.cache_version` schema metadata.
Generated-cache directory names are not an annotation fallback. Strict basename parsing is
permitted only at raw-cache conversion time, because the shipped raw 115 and 116 `info.txt` files
omit the release.

No generated-cache version sidecar or marker file is part of the contract. Existing
`chrom_manifest.json` files locate shards only. Annotation reads the version from each selected
Parquet footer's Arrow schema metadata; neither the directory name nor the existing provenance
column can supply or override it.

An optional API/CLI `expected_cache_version` is an assertion against that independently detected
identity. It is not an override and cannot make an unlabeled cache acceptable.

Rules:

- an expected version must agree with the detected version;
- conflicting values are an error;
- missing, malformed, or unknown identity is an error before annotation of the affected contig;
- a detected version outside the vepyr package's support matrix is an error;
- immediately before a contig is loaded, every present entity shard for that contig must carry the
  same supported `bio.vep.cache_version` and `bio.vep.cache_source_type`;
- runtime validation is lazy by contig: a chr1-only run must not open chr2 Parquet shards;
- the first requested contig establishes the invocation identity, and every later contig must
  agree before any of its rows are annotated;
- an unrecognized future release must not silently inherit 116 semantics; it requires a new
  explicit support record and policy decision in a subsequent vepyr release.

For newly generated caches, bio-formats now:

- derive the release from the raw cache directory when `info.txt` omits it;
- write `bio.vep.cache_version` into every entity’s schema metadata;
- keep the provenance column consistent with the metadata.

The bio-functions conversion layer preserves that metadata through its variation projection,
transcript-UID attachment, translation-core projection, compact translation-SIFT schema, and all
other output transformations that construct Arrow schemas.

The existing metadata-less generated 115 and 116 caches must be rebuilt before annotation. Their
generated directory names are not accepted as release identity, and annotation does not mutate or
silently migrate them.
The 116 rebuild also adds `clin_sig_ref_allele`; the 115 variation schema remains valid without it.

### 10.3 Current implementation and artifact status

The code implementation is complete, but release qualification is not. This distinction matters:
the strict runtime now correctly rejects the existing generated caches rather than treating their
directory names as identity.

| Area | Status on 2026-07-29 | Evidence |
|---|---|---|
| Root-cause partition | complete | all 734 fields reconcile into the six non-overlapping classes in §4 |
| Comparator/provenance | implemented | uncapped deterministic JSONL ledger, equality-shape counts, allele-aware duplicate pairing, exact reference-header validation, per-contig resolved build provenance |
| Cache support contract | implemented | exact 115/115.2 and 116/116.0 matrix; missing, malformed, unsupported, expected/detected conflict, mixed-source, and mixed-release errors |
| Lazy runtime validation | implemented | a test cache with a valid chr1 and broken chr2 footer accepts chr1 without opening chr2, then rejects chr2 before annotation |
| 734 fixes | implemented and unit-tested | two release gates, data-driven ClinVar, and unconditional chr11/chr16/chr20 fixes |
| Machine release gate | implemented | requires complete release-qualified reports, empty hash-verified ledgers, exact cache/reference/support identities, matching package/build provenance, clean durable dependencies, and zero structural/ordering/field mismatches |
| Legacy generated caches | **not release-ready** | the pre-rebuild artifacts are rejected at their first variation footer because `bio.vep.cache_version` is missing |
| Rebuilt cache artifacts | complete | all six native-release merged/Ensembl/RefSeq caches passed full footer/schema/row verification and are live under `cache/`; exact totals are below |
| Lazy live-cache identity | complete | chr1 resolves cache 115 → VEP 115.2 and cache 116 → VEP 116.0 from participating Parquet footers |
| Whole-genome merged parity | both pre-pin qualifications exact; pinned gates pending | [115 merged chr1–22](../../e2e-testing/reports/fast_chr1_chr22_merged_115_summary_20260729_1928.md) and [116 merged chr1–22](../../e2e-testing/reports/fast_chr1_chr22_merged_116_summary_20260729_2105.md) each compare all 4,096,123 variants with zero structural, ordering, one-sided, or field mismatches and empty per-contig ledgers; both full pinned runs remain mandatory |
| Whole-genome single-source parity | all four pre-pin gates exact | [115 RefSeq chr1–22](../../e2e-testing/reports/fast_chr1_chr22_refseq_115_summary_20260729_1959.md), [115 Ensembl chr1–22](../../e2e-testing/reports/fast_chr1_chr22_ensembl_115_summary_20260729_2021.md), [116 RefSeq chr1–22](../../e2e-testing/reports/fast_chr1_chr22_refseq_116_summary_20260729_2100.md), and [116 Ensembl chr1–22](../../e2e-testing/reports/fast_chr1_chr22_ensembl_116_summary_20260729_2147.md) are exact across all shared fields; each compares 4,096,123 variants with empty ledgers |
| Ensembl/RefSeq cache rebuilds | four of four complete | 115 RefSeq (1,251,658,968 verified rows), 115 Ensembl (1,251,754,252), 116 RefSeq (1,576,931,569), and 116 Ensembl (1,651,451,505) are live; the 116 Ensembl cache has 999,828 motif rows and every row has non-empty binding-matrix and transcription-factor values |
| Durable dependencies | pinned and pushed upstream | vepyr commit `edd2995` pins bio-formats `eee2d6926331fe5106cbbefbc1ca673e94357327` and bio-functions `0d02d711b352baf4087e2e9421e12716e10bb290`; both revisions are the remote heads of PR #224 and PR #203, and no absolute patch remains |

The requested VEP 115 chromosome-by-chromosome burn-down is therefore unambiguous. Each value
below is the sum of structural, ordering, one-sided, field, and ledger mismatches for that
chromosome in the current pre-pin native-release run:

| Chromosome | merged | Ensembl | RefSeq |
|---:|---:|---:|---:|
| 1 | 0 | 0 | 0 |
| 2 | 0 | 0 | 0 |
| 3 | 0 | 0 | 0 |
| 4 | 0 | 0 | 0 |
| 5 | 0 | 0 | 0 |
| 6 | 0 | 0 | 0 |
| 7 | 0 | 0 | 0 |
| 8 | 0 | 0 | 0 |
| 9 | 0 | 0 | 0 |
| 10 | 0 | 0 | 0 |
| 11 | 0 | 0 | 0 |
| 12 | 0 | 0 | 0 |
| 13 | 0 | 0 | 0 |
| 14 | 0 | 0 | 0 |
| 15 | 0 | 0 | 0 |
| 16 | 0 | 0 | 0 |
| 17 | 0 | 0 | 0 |
| 18 | 0 | 0 | 0 |
| 19 | 0 | 0 | 0 |
| 20 | 0 | 0 | 0 |
| 21 | 0 | 0 | 0 |
| 22 | 0 | 0 | 0 |

The safe rebuild command is
[`rebuild_release_cache.py`](../../e2e-testing/scripts/rebuild_release_cache.py). It is a dry run
unless `--run` is provided. A real run builds beside the target, validates every
manifest-referenced Parquet footer, checks exact release/source identity and manifest/footer row
counts, checks the 116 ClinVar and motif schemas/data, reconciles old/new per-entity row totals,
retains a timestamped backup, and rolls the target rename back if replacement fails. It creates
no version sidecar.

Both raw merged-cache roots pass preflight. Historical generated artifacts measured 32.4 GiB
(115) and 35.6 GiB (116), giving conservative fresh-staging estimates of 37.2 GiB and 40.9 GiB.
Cleaning 108 GiB of recoverable root Cargo build artifacts provided enough space for both
rebuilds. When the final runs started, neither preferred target under
`~/workspace/data_vepyr/cache/` existed, so promotion did not create a backup; a future
replacement of an existing target still retains the timestamped backup by construction.

The first parallel attempt exposed an important operational error: the extension had been built
by `maturin develop` without `--release`. After roughly 20 minutes both debug processes were
still writing chr1 variation. They were terminated before any swap and their isolated 112 MiB
(115) and 64 MiB (116) staging trees were removed. The package was then rebuilt with the
documented command:

```bash
env -u VIRTUAL_ENV -u CONDA_PREFIX \
  RUSTFLAGS="-C target-cpu=native" \
  uv sync --reinstall-package vepyr
```

The live compiler command confirmed `--profile release`, `-C opt-level=3`,
`-C strip=debuginfo`, and `-C target-cpu=native`. Under that build both chr1 variation shards
finished inside ten minutes. The complete guarded rebuilds then produced:

| Cache | Build/verify time | Size | Verified total rows | Entity shards and rows |
|---|---:|---:|---:|---|
| 115 merged | about 2 h 12 min | 33 GiB | 1,332,332,652 | variation 463 / 1,170,699,612; transcript 1,871 / 765,234; exon 1,871 / 6,501,829; translation core 1,745 / 426,495; translation SIFT 1,699 / 153,558,526; regulatory 24 / 380,956; motif 0 / 0 |
| 116 merged | about 3 h 17 min | 36 GiB | 1,739,586,368 | variation 463 / 1,487,415,922; transcript 1,869 / 903,421; exon 1,869 / 7,921,787; translation core 1,743 / 564,269; translation SIFT 1,696 / 241,400,185; regulatory 24 / 380,956; motif 24 / 999,828 |

All 999,828 VEP 116 motif rows have non-empty `binding_matrix` and
`transcription_factors`. The verifier accepted the 115 variation schema without
`clin_sig_ref_allele` and required the nullable UTF-8 field on 116.

Both long-running processes had loaded an early report-formatting bug and therefore raised after
successful verification but before target promotion. The failure was safe: staging remained
intact and neither target existed. `_print_report` was fixed to format
`EntityReport.entity`, a regression test was added (six rebuild-script tests now pass), and each
staging cache was fully verified a second time before the existing rollback-safe rename promoted
it. The six verified live targets are:

- `/Users/mwiewior/workspace/data_vepyr/cache/115_GRCh38_merged`
- `/Users/mwiewior/workspace/data_vepyr/cache/115_GRCh38_ensembl`
- `/Users/mwiewior/workspace/data_vepyr/cache/115_GRCh38_refseq`
- `/Users/mwiewior/workspace/data_vepyr/cache/116_GRCh38_merged`
- `/Users/mwiewior/workspace/data_vepyr/cache/116_GRCh38_ensembl`
- `/Users/mwiewior/workspace/data_vepyr/cache/116_GRCh38_refseq`

The native lazy validator then read only chr1-participating footers and returned the exact
support identities: cache 115 / VEP 115.2 / API 115 / semantics 115 and cache 116 / VEP 116.0 /
API 116 / semantics 116, both with source `merged`.

Implementation verification completed before the artifact rebuild:

- bio-formats: 461 passed, 1 ignored;
- bio-functions with all features after the durable bio-formats pin: 904 passed, 1 ignored;
- vepyr Python/integration: 990 passed, 2 skipped;
- rebuild verifier: 6 passed;
- machine parity gate: 9 passed;
- root `cargo check --locked`, all Rust formatting checks, and editable extension build passed.

### 10.4 Post-734 residuals and the source-exact VEP 116 stop replay

Eliminating the six classes in §4 did not immediately close the release-116 gate. The first
current-code chr1–22 sweep,
[`fast_chr1_chr22_merged_116_summary_20260729_1637.md`](../../e2e-testing/reports/fast_chr1_chr22_merged_116_summary_20260729_1637.md),
reduced the historical 734 fields to **54**:

| Field | Residual mismatches |
|---|---:|
| `Consequence` | 21 |
| `HGVSp` | 17 |
| `IMPACT` | 16 |
| all other fields and all structural/order buckets | 0 |

These were not assigned by pattern matching. Each distinct residual was traced through the exact
`ensemblorg/ensembl-vep:release_116.0` runtime and the `release/116`
`VariationEffect.pm` and `TranscriptVariationAllele.pm` code at variation revision `2fb834b`.
That trace exposed interactions which were not visible in the net 115→116 diff alone:

- frameshift HGVSp returns before ordinary `Ter`/`X` normalization;
- `_get_fs_peptides()` translates alternate CDS through available 3′ UTR sequence;
- `_ins_del_stop_altered_cil()` returns false when its required CDS/cDNA coordinate is undefined,
  and `stop_retained_variant` negates that result;
- a leading phase-derived `N` changes CDS-space indexing;
- the reference peptide used by the reference-equals-alternate decision excludes its terminal
  stop;
- release 116 retains ambiguous terminal peptide windows in the transcript-variation allele.

The investigation was kept as separate bio-functions commits so that each hypothesis and
its correction remain reviewable:

| Commit | Purpose |
|---|---|
| `5d25cde` | replay the VEP 116 ambiguous stop-loss fallback |
| `bcc97b5` | recompute the release-116 stop predicates |
| `004e758` | preserve ambiguous terminal protein HGVS |
| `6a2d4b0` | align the compound terminal-stop behavior with the exact release-116 execution trace |
| `daad832` | validate shifted terminal HGVS coordinates before retaining the VEP 116 ambiguous terminal fallback |
| `9a6f0fe` | use the trusted cached RefSeq peptide, rather than the raw genomic codon, as the VEP 116 stop-predicate input after a failed BAM edit |

After the first three commits, the ten-chromosome diagnostic set reported 80 fields
(69 `HGVSp`, 6 `Consequence`, 5 `IMPACT`). That was a deliberately broader diagnostic count, not
a new whole-genome baseline: it demonstrated that the partial replay was still wrong. The exact
Docker/source trace above led to `6a2d4b0`. Re-running the same ten affected chromosomes
(chr1, chr2, chr3, chr4, chr11, chr14, chr17, chr18, chr19, and chr22) on the rebuilt release
binary produced **zero** field, structural, ordering, or one-sided mismatches across all 86
fields. The independent final-binary chr2 rerun is recorded in
[`fast_chr2_merged_116_summary_20260729_1842.md`](../../e2e-testing/reports/fast_chr2_merged_116_summary_20260729_1842.md).

This is why the final implementation contains more detail than the initial “nine net behavioral
hunks” plan: the governing rule is parity with the exact VEP 116 execution path, not an
approximation inferred from output shape or from the source diff alone.

The continuation across chromosomes outside that first diagnostic set exposed two additional
boundary representations, totalling four fields in the binaries that preceded their respective
fixes:

1. `chr6:68956855 GA>G`, `ENST00001108876.1`, and
   `chr10:116861221 TAA>T`, `ENST00001130727.1`, each emitted `p.Ter...=` in vepyr while VEP
   emitted no HGVSp. In the exact release-116 source,
   `TranscriptVariationAllele::hgvs_protein()` performs `_return_3prime(1)`, clears the cached
   translation coordinates, and returns `undef` when the shifted mapper result is a `Gap`.
   For chr6 the deletion shifts nine bases across a nine-base poly-A 3′ UTR from peptide 524 to
   `c.*9`; the shifted CDS/peptide coordinates are therefore undefined. The V116 ambiguous
   terminal fallback had been returned before this already-correct mapper guard. `daad832`
   reorders the guard ahead of the fallback and includes an exact shift-nine terminal deletion
   fixture. Post-fix reports
   [`chr6`](../../e2e-testing/reports/fast_chr6_merged_116_summary_20260729_1909.md) and
   [`chr10`](../../e2e-testing/reports/fast_chr10_merged_116_summary_20260729_1912.md) are exact
   across all 86 fields with empty ledgers.
2. `chr12:40526308 T>C`, `NM_173600.2`, emitted
   `stop_lost&synonymous_variant`/`HIGH` instead of VEP's
   `synonymous_variant`/`LOW`. Both VEP 115.2 and 116.0 emit
   `NP_775871.2:p.Arg6985=`, `Amino_acids=R`, `Codons=Tga/Cga`, and `BAM_EDIT=FAILED`.
   The generated row has `bam_edit_status=failed`, no retained RefSeq edits, and a trusted cached
   protein with Arg at that position, while the genomic CDS contains the `TGA` codon. VEP 116's
   `stop_lost()` consumes the `peptide()` values returned by `_get_peptide_alleles()`; it does not
   re-derive that predicate from the displayed genomic codons. The Rust classifier already used
   the cached protein for the synonymous call, but then supplied the genomic `*/R` window to the
   V116 stop predicates. `9a6f0fe` supplies the trusted `R/R` peptide window to those predicates
   while preserving `Tga/Cga` for CSQ output. Its exact V115/V116 regression and the complete
   bio-functions VEP package suite pass. The native release rebuild and
   [`chr12`](../../e2e-testing/reports/fast_chr12_merged_116_summary_20260729_1934.md) rerun are
   exact across 197,815 variants and all 86 fields, with an empty ledger.

---

## 11. Implementation sequence and expected burn-down

The sequence below records the causal burn-down plan used for implementation. Its numeric steps
were diagnostic expectations, not observed intermediate whole-genome gates; the actual post-fix
residual sequence and its source-exact corrections are recorded in §10.4.

### Phase 0 — freeze evidence and improve the comparator

1. Emit an uncapped mismatch ledger.
2. Add per-field counts for:
   - both empty;
   - both non-empty and equal;
   - one empty;
   - both non-empty and unequal.
3. Record resolved vepyr, bio-functions, and bio-formats SHAs in reports.
4. Run a current-build 115 baseline once before semantic changes.

Expected mismatch count: **734**.

### Phase 1 — release context and cache metadata

1. Add `VepSemantics`.
2. Add the cache 115→VEP 115.2 and cache 116→VEP 116.0 support records.
3. Implement mandatory contig-lazy shard metadata validation, optional expected-version assertion,
   and conflict/unsupported-version checks.
4. Integrate the bio-formats release metadata work and preserve it through all bio-functions
   cache-builder schema transforms.

Expected mismatch count: **734**; this is plumbing only.

### Phase 2 — ClinVar reference allele

1. Preserve optional `clin_sig_ref_allele`.
2. Carry it through lookup and colocated aggregation.
3. Implement VEP’s conditional reverse complement.
4. Rebuild the complete 115 and 116 caches into staging so every shard has version metadata; the
   116 variation rebuild additionally supplies `clin_sig_ref_allele`.

Expected mismatch count:

```text
734 - 112 = 622
```

### Phase 3 — release-independent residuals

1. Fix the chr20 terminal plus-strand CDS predicate.
2. Fix the chr16 primary-only exon-boundary protein span.
3. Prove and fix the chr11 RefSeq failed-edit shift predicate.

Expected mismatch count:

```text
622 - 10 = 612
```

### Phase 4 — VEP 116 stop semantics

Implement all nine net behavioral hunks behind the single stop-policy gate. Do not port dead
`consider_ins_len` plumbing.

Expected mismatch count:

```text
612 - 224 = 388
```

### Phase 5 — VEP 116 partial-overlap HGVS

Implement slice-space clamp-then-shift semantics behind the single HGVS policy gate. Add lazy
sequence access only if the exact clipping fixtures require it.

Expected mismatch count:

```text
388 - 388 = 0
```

### Phase 6 — reproducible release candidate

1. Land the functions and formats changes on durable upstream revisions/releases.
2. Replace absolute Cargo path patches with pinned Git revisions or released versions.
3. Rebuild the final extension from those exact dependencies.
4. Regenerate the final 115 and 116 reports.

---

## 12. Verification strategy with minimal release cost

Do not run two whole-genome comparisons on every small commit.

### Per-change gates

- unit tests for release resolution and conflicts;
- paired V115/V116 golden fixtures for each semantic difference;
- exact ClinVar fixtures for all three variants;
- exact chr11/chr16/chr20 fixtures;
- comparator both-silent diagnostics;
- ordinary Rust/Python test suites.

### Targeted integration gates

- run the chromosome containing each changed residual;
- run chr22 under both releases after changes to gated semantics;
- run representative plus/minus-strand and Ensembl/RefSeq fixtures.

### Final release gates

Run current code over chr1–22 against each exact release/profile pair:

| VEP release | Cache/reference profiles |
|---|---|
| 115.2 | merged, Ensembl-only, RefSeq-only |
| 116.0 | merged, Ensembl-only, RefSeq-only |

For **each of all six gates** require:

```text
variants_only_in_vepyr      = 0
variants_only_in_vep        = 0
csq_entry_count_mismatch    = 0
csq_order_mismatch          = 0
field_mismatch_total        = 0
```

Also require:

- the report records the resolved release and dependency SHAs;
- no absolute local dependency patches remain;
- every shard used by each requested contig carries one supported version/source identity matching
  the expected cache/reference pair;
- the Ensembl reference `##VEP` header matches the declared VEP/API versions and core/variation
  revisions for that support record;
- variation cache schema behavior is tested with and without `clin_sig_ref_allele`;
- throughput and memory are compared with the frozen baseline, with any regression over 5%
  investigated before release.

The full 115 and 116 runs are release-candidate gates, not per-commit gates. That provides strong
dual-release protection without turning each implementation step into an hours-long validation
cycle.

---

## 13. Definition of done

Support for both releases is complete when:

1. Cache identity is mandatory and resolves through the vepyr release's declared support matrix:
   cache 115/VEP 115.2 or cache 116/VEP 116.0.
2. Only partial-overlap HGVS and the stop-predicate family are release-gated.
3. ClinVar behavior is data-driven by `clin_sig_ref_allele`.
4. The chr11, chr16, and chr20 fixes are release-independent.
5. The generated 116 variation cache preserves `clin_sig_ref_allele`, while the 115 schema remains
   valid without it.
6. Every manifest-referenced shard in the rebuilt 115 and 116 caches preserves
   `bio.vep.cache_version` through physical conversion; runtime verifies only the requested
   contig's shards immediately before that contig runs.
7. Current-build chr1–22 comparisons are structurally and field-exact for merged,
   Ensembl-only, and RefSeq-only caches at both 115 and 116 (six gates).
8. The uncapped per-contig ledgers account for every pre-fix field and all 132 ledgers
   (22 contigs × 6 release/profile gates) are empty post-fix.
9. Release artifacts use the pushed, pinned bio-formats and bio-functions Git revisions rather
   than local path overrides, and the final six gates identify that pinned build.
