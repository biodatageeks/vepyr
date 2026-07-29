# The remaining 734: root cause analysis and implementation plan

**Date:** 2026-07-29
**Baseline:** vepyr vs Ensembl VEP 116, `merged`, `--everything --hgvs`, HG002 GRCh38, chr1–22.
**Measured:** 734 field mismatches over 4,096,123 variants / 66,362,749 CSQ entries; 76 of 86 CSQ
fields at 100%; CSQ structure exact (0 entry-count, 0 ordering, 0 one-sided variants).

Follows [2026-07-28-chr1-22-parity-116.md](2026-07-28-chr1-22-parity-116.md), which took the
count from 31,699 to 734.

---

## 1. The finding that reframes everything

**At release 115, vepyr had zero field mismatches.**

The `merged` parity reports in `e2e-testing/reports/fast_chr*_merged_report.json` cover all 22
contigs, 3,457,504 variants, dated 2026-07-01 to 2026-07-06, and record **no field below 100% on
any contig**.

So the 734 are not accumulated technical debt. They are, almost entirely, the **delta of the
115 → 116 upgrade**. That changes what "fixing" means: for most of these, vepyr is not wrong in
any absolute sense — it faithfully implements Ensembl 115, and Ensembl 116 changed underneath it.

Caveat carried throughout: that baseline was produced by an older vepyr build (~3 weeks before
this analysis). Where the VEP source or reference output proves a 115→116 change, the conclusion
is certain. Where VEP is identical at both releases (classes 4 and 5 below), separating "vepyr
regressed since early July" from "the 116 cache data differs" requires re-running the 115 profile
with the current build — see §7.

### Why the parity suite could not have caught these earlier

Three distinct reasons, all worth designing against:

1. **Agreement on absence scores as a match.** Where VEP 115 emitted nothing and vepyr emitted
   nothing, the comparison recorded a pass. That is how the largest class (390) stayed invisible.
   "Both silent" is much weaker evidence than "both equal and non-empty", and the report does not
   distinguish them.
2. **vepyr was built against 115, so it matched 115.** The consequence predicates and the HGVS
   boundary bail are faithful ports of 115 behaviour — the bail even cites
   `release/115 TranscriptVariationAllele.pm#L1416` in its comment. A single-release suite cannot
   see a port that is correct for the release it targets.
3. **Some 116 transcripts do not exist at 115.** `ENST00001124442` and `ENST00001090237` are in
   the 116 transcript cache only, so at 115 there was no CSQ entry to compare at all.

---

## 2. Root cause taxonomy

| # | class | fields | count | root cause | certainty |
|---|---|---|---|---|---|
| 1 | HGVS transcript boundary | HGVSc, HGVS_OFFSET | 390 | Ensembl 116 clamps to the transcript where 115 declined | **proven from source + reference** |
| 2 | Stop-codon consequence rework | Consequence, IMPACT, HGVSp, protein coords | 232 | Ensembl 116 rewrote six predicates in `VariationEffect.pm` | **proven by diffing 115 vs 116 source** |
| 3 | CLIN_SIG | CLIN_SIG | 112 | 116 reference output changed for these variants; mechanism not yet isolated | evidence gathered, cause open |
| 4 | HGVSc 3'-shift in repeats | HGVSc | ~2 | shift coordinate disagreement | unanalysed |
| 5 | Protein position of insertions | Protein_position | 2 | VEP identical at both releases → vepyr-side or cache-driven | unresolved |

Classes 1 and 2 are 622 of 734 (85%) and both are fully specified below.

---

## 3. Class 1 — HGVS transcript boundary (390)

### Root cause

`TranscriptVariationAllele::_var2transcript_slice_coords` (116, line 2683) **clamps** the variant
to the transcript and only declines when it lies entirely outside:

```perl
my $tr_length = $tr_end - $tr_start + 1;
return undef if (($vf_start < 1 && $vf_end < 1) || ($vf_start > $tr_length && $vf_end > $tr_length));
my $clamped_start = ($vf_start < 1 ? 1 : $vf_start > $tr_length ? $tr_length : $vf_start);
my $clamped_end   = ($vf_end   < 1 ? 1 : $vf_end   > $tr_length ? $tr_length : $vf_end);
```

`$vf_start`/`$vf_end` are transcript-slice coordinates, strand-flipped for reverse-strand
transcripts. A second guard in `hgvs_transcript` (line 1472) discards the result when 3'-shifting
pushes the end past the slice:

```perl
return undef if (($slice->end - $slice->start + 1) < ($_slice_end + $offset_to_add));
```

vepyr instead returns `None` whenever either end is outside. Verified: the transcribed rule
reproduces all four measured chr22 cases, including the two where VEP emits nothing.

### Why 115 did not catch it

VEP 115 also emitted nothing for these variants — confirmed on six variants, same input, both
references. Both sides silent, so the comparison passed. Two of the six had no 115 CSQ entry at
all because the transcript is new in 116.

### Specification

For a deletion where `variant_start < tx.start || variant_end > tx.end`:

1. Map to transcript-slice coordinates: forward `g - tr_start + 1`; reverse `tr_end - g + 1` with
   start/end swapped.
2. If entirely outside (both ends `< 1`, or both `> tr_length`), return nothing.
3. Clamp both ends to `[1, tr_length]`.
4. Compute the 3' shift offset in slice orientation (always non-negative).
5. If `tr_length < clamped_end + offset`, return nothing — **and emit no `HGVS_OFFSET` either**.
6. Otherwise build the notation from `clamped_start + offset .. clamped_end + offset`, taking the
   reference from the transcript slice sequence, then `_clip_alleles`.

### Why it is not yet implemented

Two attempts were built and measured; both reverted. Ensembl clamps the **unshifted** variant and
then applies the shift as an **offset in slice coordinates**; vepyr materialises the shift into a
genomic interval (`HgvsGenomicShift::display_start/display_end`) and substitutes it wholesale in
`format_hgvsc_fallback`. At a boundary these are not interchangeable — the clamp must bound the
result *after* the offset, while the offset is measured from the *clamped* origin.

- Clamping the unshifted span: chr1 60 → 55 (kills the over-emitted `HGVS_OFFSET`) but reports
  `c.-329_-315del` where VEP says `c.-315del`.
- Clamping the shifted span: loses the decline semantics entirely and regresses
  `test_format_hgvsc_uses_shifted_coordinates_for_exonic_deletions`.

### Implementation plan

1. **Give the formatter sequence access.** `hgvs_reader: Option<FastaReader>` already lives on
   `AnnotationWorkerState` — one per worker, owned, no sharing. Thread `Option<&mut FastaReader>`
   into `evaluate_variant_with_context` → `format_hgvsc`. Fetch **lazily**, only on the boundary
   branch.
2. **Rework the shift for that branch only**: carry the shift as a slice-space offset applied
   after clamping, instead of a pre-materialised genomic interval. Shared code path everywhere
   else, so 115 output cannot move.
3. Gate on cache release ≥ 116 (see §6).
4. Port steps 1–6 of the specification.

**Do not** port Ensembl's data structure. VEP holds `$self->{_slice}`, the materialised transcript
span sequence. chr1 alone has 83,024 transcripts totalling 3.86 Gbp of span (mean 46 kb, max
1.55 Mb); materialising that eagerly would be a catastrophic memory and I/O regression. Same
algorithm, lazy access.

### Performance

Expected flat. Current throughput is 16,352 variants/s (4,096,123 in 250.5 s). Workers already
read the FASTA for every indel (two 1 kb flank reads for the shift), so the boundary path reuses
an open reader and a warm page cache. The path fires on ~0.008% of variants (25 in chr1's
323,430) — a few hundred extra fetches genome-wide.

### Expected yield

~290 HGVSc + 98 HGVS_OFFSET = **~388**, leaving the ~2 repeat-shift cases (class 4). Treat as a
ceiling: hitting the shape is not the same as producing VEP's exact string, which is precisely
what failed twice.

---

## 4. Class 2 — stop-codon consequence rework (232)

### Root cause

Ensembl 116 rewrote stop-codon-adjacent consequence calling. Diffing `VariationEffect.pm` between
`release/115` and the 116 image shows **six** changed predicates:

| predicate | change in 116 |
|---|---|
| `frameshift` | added `return 0 if defined $ref_pep && $ref_pep =~ /^\*/;` — *"if the first base affected is the stop codon then it does not affect the reading frame"* |
| `inframe_insertion` | added `return 0 if $ref_pep eq "*" && $alt_pep eq "*";` (ref codon TAG, alt codon TAAG) |
| `inframe_deletion` | added `return 0 if $ref_pep eq "*";` (ref codon TAG, alt codon G) |
| `stop_retained` | added `$alt_pep !~ 'X'` guard; switched to new `_overlaps_stop_codon_cil` / `_ins_del_stop_altered_cil` helpers |
| `stop_lost` | added `return 0 if partial_codon(@_);` and an `$alt_pep !~ 'X'` guard |
| `_ins_del_stop_altered` / `_overlaps_stop_codon` | gained a `$consider_ins_len` parameter |

The `_cil` helpers are new: 9 references in 116, 0 in 115.

vepyr implements the **115** versions, which is why it over-calls `inframe_insertion` (27 of 60
sampled Consequence mismatches) and `frameshift_variant` (10 more).

### The cascade — one cause, four fields

`IMPACT` is derived from the term set, so it moves 1:1 (27 `MODERATE→LOW`, 10 `MODERATE→HIGH`).
`HGVSp` follows too: `_get_hgvs_protein_type` line 2047 is

```perl
if( frameshift($self) ){ $hgvs_notation->{type} = "fs"; }
```

and that sub is **identical** between 115 and 116. So `p.Ter1332TrpfsTer12` → `p.Ter1332delinsTrpTer`
is not an HGVS formatting change at all — it is the `frameshift` predicate change propagating into
unchanged notation code. Confirmed on the reference: VEP 115 emits the `fs` form, 116 the `delins`
form, for the same variant.

That corrects an earlier reading in which the terminator notation looked like an independent
HGVS-p rule. It is not; it is downstream of the same six predicates.

### Why 115 did not catch it

vepyr's predicates match 115 exactly. Confirmed end to end on `chr11:7991931 CATAA>C`:

| | Consequence |
|---|---|
| VEP 115 | `frameshift_variant&stop_lost&NMD_transcript_variant` |
| VEP 116 | `inframe_deletion&stop_retained_variant&NMD_transcript_variant` |
| vepyr today | `frameshift_variant&stop_lost&NMD_transcript_variant` (i.e. the 115 answer) |

### Specification

Port the six predicate changes. The first three are small, self-contained guards on the
peptide alleles and should be done together:

1. `frameshift`: return false when the reference peptide starts with `*`.
2. `inframe_insertion`: return false when ref and alt peptides are both `*`.
3. `inframe_deletion`: return false when the reference peptide is `*`.
4. `stop_retained`: add the `alt_pep !~ 'X'` guard, and port `_overlaps_stop_codon_cil` /
   `_ins_del_stop_altered_cil`.
5. `stop_lost`: add the `partial_codon` early return and the `alt_pep !~ 'X'` guard.
6. Thread `consider_ins_len` through `_ins_del_stop_altered` / `_overlaps_stop_codon`.

### Implementation plan

Two stages, because 1–3 are cheap and cover the dominant shapes:

- **Stage A** (guards 1–3): expected to clear the 27 `inframe_insertion` and 10
  `frameshift_variant` over-calls plus their IMPACT and HGVSp followers.
- **Stage B** (guards 4–6): the `_cil` helpers must be read in full first — they are new code, not
  a tweak, and this analysis has only established that they exist and are referenced 9 times.

Release-gate both stages: at 115 these guards must not apply, or the 115 baseline regresses from
zero.

### Expected yield

Up to **232** across four fields. Stage A alone should account for the majority.

---

## 5. Class 3 — CLIN_SIG (112, three variants)

### Evidence

All three are insertions, and the reference itself changed for two of them:

| variant | VEP 115 | VEP 116 | vepyr today |
|---|---|---|---|
| `chr15:89333596 T>TTGC` | `conflicting_classifications_of_pathogenicity` | *(none)* | emits the 115 value |
| `chr3:42210085 C>CGGAGGA` | `benign` | *(none)* | emits the 115 value |
| `chr14:74506880 C>CGCGCGCAT` | `benign` | `benign` | *(none)* |

So two are over-calls where 116 stopped reporting, and one is an under-call where 116 still
reports. Both directions.

### What has been ruled out

vepyr's `allele.rs::get_matched_variant_alleles` is a **faithful port** of
`Utils/Sequence.pm::get_matched_variant_alleles` — same `"$ref_$alt_$pos"` key, same
both-direction `trim_sequences`. Worked by hand on the chr15 case, neither implementation matches:

| | minimised key |
|---|---|
| input `T>TTGC` | `-_TGC_89333597` |
| cache repeat record, alt `TGC` (left-trim) | `TGCTGC…(30)_-_89333600` |
| cache repeat record, alt `TGC` (right-trim) | `TGCTGC…(30)_-_89333597` |

### Open question

The 116 cache carries a refreshed ClinVar (`source_ClinVar 202509` in `info.txt`), so the record
set itself differs from 115. The candidate mechanism on the vepyr side is
`ColocatedCacheEntry::matches_output_allele`:

```rust
self.matched_alleles.is_empty() || self.matching_allele(output_allele, ...).is_some()
```

`is_empty()` exists to admit unknown-allele records matched on coordinates alone, but it would
also admit a known-allele entry that reached the sink with an empty match list — and the emitted
term is exactly what the repeat record carries under the `TGC:` label.

**This is unverified.** The next step is one check — instrument whether that entry arrives with
`matched_alleles` empty — not a code change.

### Implementation plan

1. Verify the `is_empty()` hypothesis on `chr15:89333596`.
2. If confirmed, tighten to admit empty match lists only for unknown-allele records.
3. Separately diagnose the `chr14` under-call, which is the opposite direction and may be a
   different defect.

Cheapest item on the board: 112 mismatches, three variants, no upstream dependency.

---

## 6. Cross-cutting requirements

### Release gating is mandatory

Classes 1 and 2 are both "116 changed, vepyr implements 115". Applying either fix unconditionally
would take the 115 baseline from **zero** to roughly the same magnitude of mismatches, reversed.
Every fix in classes 1 and 2 must be conditional on the cache release.

Detection already works without a cache rebuild: prefer `bio.vep.cache_version` on the transcript
shard, fall back to the cache directory name (`116_GRCh38_merged`). Release-116 caches ship an
`info.txt` with **no version line at all**, so the directory name is the only signal older caches
carry. Unknown release must mean "keep 115 behaviour". The plumbing is parked on
`wip/hgvs-transcript-clip-116` (bio-functions) and `wip/cache-version-metadata` (bio-formats).

### The 115 regression run has never been done

It has been flagged twice in this work and never executed. Given that the 115 baseline is the only
evidence that these fixes do not regress the previous release — and that the baseline is
zero, so any regression is immediately visible — it should be a gate on every change here, not an
afterthought.

### Add a both-silent diagnostic

The comparison scores "both empty" as a match, which is how 390 mismatches hid for a whole
release cycle. Reporting fields where both sides are silent as a separate count would turn that
from an invisible pass into a visible "unverified", and would have surfaced class 1 at 115.

---

## 7. Unresolved

| class | what is known | what is needed |
|---|---|---|
| 4 — HGVSc 3'-shift | `chr11:1094638` `c.4396_4416del` vs `c.4443_4463del`, same length, 47 bases apart; the only HGVSc mismatch not at a boundary | analysis not started |
| 5 — Protein_position | `chr16:5072071 G>GGTCT`, vepyr `74` vs VEP `74-75`; VEP **identical** at 115 and 116, and its Consequence agrees | run the 115 profile with the current build to separate a vepyr regression from a cache-data effect |

---

## 8. Sequencing

| order | work | yield | depends on |
|---|---|---|---|
| 1 | CLIN_SIG verification + fix | 112 | nothing |
| 2 | Class 2 Stage A (three peptide guards) | majority of 232 | release gating |
| 3 | Class 1 (shift rework + boundary clamp) | ~388 | release gating, sequence access |
| 4 | Class 2 Stage B (`_cil` helpers) | remainder of 232 | reading the new 116 helpers |
| 5 | Classes 4 and 5 | ~4 | the 115 re-run |

Every step gated on: the 116 chr1–22 run **and** the 115 regression run staying at zero.
