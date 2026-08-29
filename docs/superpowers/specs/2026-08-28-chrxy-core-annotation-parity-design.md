# chrX/chrY core-annotation parity: transcript ablation, feature ranges, tiny introns

**Date:** 2026-08-28
**Status:** Approved (design)
**Implementation repo:** `datafusion-bio-function-vep` (github.com/biodatageeks/datafusion-bio-functions)
**Investigation:** `/Users/mwiewior/workspace/data_vepyr/debug/chrXY-core-annotation-2026-08-28/`
(`HANDOVER.md`, `KICKOFF.md`)

## Problem

vepyr reproduces Ensembl VEP 116 byte-identically on all 22 autosomes — 4,096,123 records,
69,299,753 CSQ entries, 22/22 `body=ok`. Extending the same gate to chrX/chrY leaves **364
mismatched CSQ field values** across 221,426 compared records. Zero plugin fields differ; every
mismatch is a core annotation field produced by the consequence engine.

| | chrX | chrY |
|---|---:|---:|
| variants compared | 157,690 | 63,736 |
| variants on one side only | 0 | 0 |
| CSQ entry count / order mismatches | 0 / 0 | 0 / 0 |
| plugin-field mismatches | 0 | 0 |
| core-field mismatches | 148 | 216 |
| strict body MD5 | DIFF | DIFF |

The 364 rows decompose into exactly four defects, with no residue:

| defect | rows | field(s) |
|---|---:|---|
| **A** EXON/INTRON emitted as a single number where VEP emits a range | **324** | `EXON`, `INTRON` |
| **B** `transcript_ablation` never emitted for sequence variants | **37** | `Consequence` |
| **C** `intron_variant` missing at a 2 bp intron boundary | **2** | `Consequence` |
| **D** `HGVSp` missing for an insertion inside a 2 bp intron | **1** | `HGVSp` |

324 + 37 + 2 + 1 = 364.

### Defect A — EXON/INTRON single value vs range (324 rows)

`which_exon_str` (`src/transcript_consequence.rs:8090`) and `which_intron_str` (`:8111`) both
`return` from inside their scan loop on the first overlapping feature. Ensembl collects every
overlapping feature, sorts the numbers, and emits `min-max`:

```perl
# BaseTranscriptVariation::exon_number, ensembl-variation @ 23c76f60b
my @numbers = map {...} grep {overlap($vf_start, $vf_end, $_->{start}, $_->{end})}
              @{$self->_overlapped_exons};
if(scalar @numbers > 1) {
  @numbers = sort {$a <=> $b} @numbers;
  $number_string = $numbers[0].'-'.$numbers[-1];
}
$number_string .= '/'.$total;
```

`intron_number` is the same code over `_overlapped_introns`.

All 324 rows have the shape *VEP range vs vepyr single endpoint, identical total* — verified
mechanically over both mismatch ledgers: 113 rows on chrX and 211 on chrY, with
`total_same=True` and `vepyr_is_endpoint=True` for every single one. No other shape occurs.

**This defect is independent of B.** At `chrX:3886710`, feature `ENST00000648217` is only
partially covered by the deletion: its `Consequence` already matches VEP exactly, yet `EXON` is
`4/13` against VEP's `1-4/13` and `INTRON` is `4/12` against `1-4/12`. The handover's framing of
A and B as "two symptoms, one cause" is incorrect — A fires on partial overlaps that will never
be ablations.

### Defect B — `transcript_ablation` never emitted (37 rows)

32 rows on chrX, 5 on chrY, every one of the form:

```
VEP:    transcript_ablation
vepyr:  splice_acceptor_variant&splice_donor_variant&splice_donor_5th_base_variant
        &non_coding_transcript_exon_variant&intron_variant
```

Ensembl's rule is a tier-1 consequence:

```perl
# VariationEffect.pm
sub feature_ablation {
    return (complete_overlap_feature($bvfoa, $feat, $bvfo, $bvf) and deletion(@_));
}
sub complete_overlap_feature {
    return (($bvf->{start} <= $feat->{start}) and ($bvf->{end} >= $feat->{end}));
}
```

`transcript_ablation` and `transcript_amplification` are the only tier-1 transcript
consequences; `mature_miRNA_variant` is the only tier-2; the remaining 28 transcript-scoped
consequences are tier 3. Evaluation is strictly tier-ascending
(`@SORTED_OVERLAP_CONSEQUENCES = sort {$a->tier <=> $b->tier}`,
`BaseVariationFeatureOverlapAllele.pm:69`) and stops once a low tier has been assigned:

```perl
OC: for my $oc (@oc_list) {
  last if $assigned_tier && $oc->{tier} > $assigned_tier;
  if($oc->predicate->($self, $feat, $bvfo, $bvf)) {
    push @$cons, $oc;
    my $tier = $oc->{tier};
    if($tier <= 2) { $assigned_tier = $tier; }
  }
}
```

So an ablated transcript emits `transcript_ablation` alone — every tier-3 term is suppressed —
while `EXON`/`INTRON` are still populated, because those come from `OutputFactory`, not from the
consequence list. The reference output confirms this exactly: at `chrX:3886710`,
`ENST00000424415` carries `Consequence=transcript_ablation`, `EXON=1-4/4`, `INTRON=1-3/3`.

The engine does construct `SoTerm::TranscriptAblation`, but only on the structural-variant path
(`:2526`, `:2572`), driven by `SvFeatureKind::Transcript` + `SvEventKind::Ablation` rows from the
`sv_features` cache artifact, and it emits a standalone CSQ entry with no `transcript_id`. There
is no sequence-variant predicate, so a plain deletion spanning a transcript never reaches it.

### Defect C — `intron_variant` missing at a 2 bp intron (2 rows)

Two variants, both insertions abutting a 2 bp intron. Exon coordinates read from the
116 GRCh38 merged cache:

| variant | transcript | flanking exons | intron | VF (start, end) |
|---|---|---|---|---|
| `chrX:10015674 G>GC` | `ENST00000380861` (+) | 1: …–10015674, 2: 10015677–… | `[10015675, 10015676]` | (10015675, 10015674) |
| `chrX:119605952 C>CG` | `NM_001417890.1` (−) | 2: …–119605952, 1: 119605955–… | `[119605953, 119605954]` | (119605953, 119605952) |

Ensembl's `intronic` flag comes from `_intron_effects`
(`BaseTranscriptVariationAllele.pm:99-149`):

```perl
if($intron->{_frameshift} && overlap($r_start, $r_end, $intron_start, $intron_end)) {
  $intron_effects->{within_frameshift_intron} = 1;
  next;
}
if ( overlap($r_start, $r_end, $intron_start+2, $intron_end-2)
  || ($insertion && ($r_start == $intron_start+2 || $r_end == $intron_end-2))
  || overlap($r_start_unshifted, $r_end_unshifted, $intron_start+2, $intron_end-2)
  || ($insertion && ($r_start_unshifted == $intron_start+2 || $r_end_unshifted == $intron_end-2))
) { $intron_effects->{intronic} = 1; }
```

For the first case: the frameshift-intron `next` is **not** taken, because
`overlap(10015675, 10015674, 10015675, 10015676)` is false — an insertion's `end` is `start-1`,
so it does not overlap the intron it abuts. Evaluation falls through, and
`$r_end == $intron_end - 2` → `10015674 == 10015674` fires. `intronic = 1`. The second case is
identical: `119605952 == 119605954 - 2`.

`variant_hits_intron_body` (`:9679`) diverges twice:

1. It returns `false` for *any* intron with `(intron_end - intron_start).abs() <= 12`,
   unconditionally. Ensembl skips a frameshift intron only when the variant actually overlaps it.
2. It collapses Ensembl's four OR'd clauses into the single range `[inner_start, inner_end+1]`.
   That algebra is sound while `inner_start <= inner_end`, but for a 2 bp intron
   `inner_start = 10015677 > inner_end = 10015674` and the function bails at
   `if inner_start > inner_end { return false; }` — discarding the standalone
   `$r_end == $intron_end-2` clause, which is the entire defect.

Both tools agree on `EXON` and `INTRON` here (both empty), which independently confirms the
geometry: an insertion exactly at `exon_end+1` hits neither the exon (`start <= exon.end` fails)
nor the intron (`start > intron_start` fails).

### Defect D — `HGVSp` missing inside a 2 bp intron (1 row)

`chrX:10015674 G>GC` against `NM_015691.5`, whose exon 1 ends at 10015673 and exon 2 starts at
10015676 — intron 1 is `[10015674, 10015675]`, again 2 bp. Here the insertion **does** overlap
the intron (`overlap(10015675, 10015674, 10015674, 10015675)` is true), so Ensembl sets
`within_frameshift_intron` and routes the variant through the coding path:

```
VEP:    Consequence=coding_sequence_variant  HGVSc=NM_015691.5:c.96-1dup
        HGVSp=NP_056506.3:p.Pro33AlafsTer47  cDNA/CDS/Protein_position all empty
vepyr:  identical except HGVSp is empty
```

The engine has frameshift-intron handling (`:1800-1890`) and reaches the same
`coding_sequence_variant`, the same `HGVSc`, and the same empty position fields — it just does
not produce protein HGVS on that path. This is the one defect whose engine-side mechanism is not
yet located; see Stage 4.

### Why chr1–22 never showed any of this

The autosome benchmark (HG002 v4.2.1) contains no deletion large enough to span a transcript —
the chrXY input reaches 38,429 bp — and the four tiny-intron transcripts involved in C and D are
not exercised there. The autosome result stands; this is new coverage, not a regression.

## Design

### Defect A — collect, then range

Change `which_exon_str` and `which_intron_str` to accumulate every hit instead of returning on
the first. Emit `format!("{n}/{total}")` when one feature matches and
`format!("{min}-{max}/{total}")` when more than one does.

The hit predicates stay exactly as they are — insertions use `start > f.start && start <= f.end`,
everything else uses `overlaps(...)` — because those already reproduce Ensembl's `overlap()` on
`(P, P-1)` insertion coordinates and are what makes the autosomes pass today. Only the
accumulation changes.

For introns, the number is computed per index with the existing strand mapping
(`i + 1` on `+`, `total_introns - i` on `−`) and then min/max'd over the collected set, matching
Ensembl's numeric sort of transcript-order numbers. On the `−` strand the collected numbers are
descending in genomic order, so min/max is required — taking first/last would invert the range.

### Defect B — a generic tier gate, not a special case

Two additions, both mirroring Ensembl directly.

**1. `SoTerm::tier()`** in `so_terms.rs`, a match transcribed from `Config.pm`:

| tier | terms |
|---:|---|
| 1 | `transcript_ablation`, `transcript_amplification` |
| 2 | `mature_miRNA_variant`, `TFBS_ablation`, `TFBS_amplification`, `TF_binding_site_variant`, `regulatory_region_ablation`, `regulatory_region_amplification`, `regulatory_region_variant` |
| 4 | `intergenic_variant`, `sequence_variant` |
| 3 | everything else |

**2. `apply_tier_gate(&mut terms)`** at the emit boundary. Because the OC list is sorted by tier
alone and `assigned_tier` is only ever set for `tier <= 2`, Ensembl's `last if` reduces exactly to:

> Let `T` be the minimum tier among matched terms that is `<= 2`. If `T` exists, retain only terms
> with `tier <= T`. Otherwise retain everything.

Note the asymmetry this preserves: a tier-3 match never sets `assigned_tier`, so tier-3 terms do
**not** gate tier-4 terms. The reduction reproduces that, because the gate only engages when some
matched term has tier ≤ 2.

**3. The ablation predicate.** A named function with a traceability block:

```rust
fn transcript_is_ablated(variant: &VariantInput, tx: &TranscriptFeature) -> bool {
    is_deletion(variant) && variant.start <= tx.start && variant.end >= tx.end
}
```

inserting `SoTerm::TranscriptAblation` into the term set, after which the generic gate does the
suppression.

**Term production is not restructured.** `evaluate_transcript_overlap_inner` keeps returning the
full term set *and* `coding_class` unchanged, and the gate runs on the output. This is load-bearing,
not incidental: the pre-gate terms feed `original_terms_allow_protein_hgvs`,
`used_ref_for_transcript_variant`, and the coding block at `:1352-1400`. An early return of
`[TranscriptAblation]` would blank `coding_class` and silently change `HGVSc` and the position
fields — which currently match VEP on all 4,096,123 autosome records, and which the reference
shows VEP itself still populates on ablated transcripts.

Two ordering risks were checked and retired: CSQ blocks sort by feature type then feature id
(`:920`, `:1024`), not by term rank, so no block can reorder; and `TranscriptAblation` is already
rank 1 with impact `HIGH` (`so_terms.rs:199`, `:245`), the same bucket as `SpliceAcceptorVariant`,
so `IMPACT` is unchanged.

#### Known consequence: the gate is not a no-op on miRNA transcripts

`mature_miRNA_variant` is tier 2, so a generic gate suppresses tier-3 terms that the engine
currently hand-codes around — the `in_mature_mirna` flag skipping
`NonCodingTranscriptExonVariant` (`:1820-1832`), and the `!terms.contains(&MatureMirnaVariant)`
guard on `NonCodingTranscriptVariant` (`:1965-1971`) — and it will additionally strip splice terms
that `add_intron_splice_terms` adds unconditionally. That is more faithful to Ensembl, but it
changes behaviour outside the four defects, and chr1–22 contains many miRNA transcripts.

This is the single largest regression risk in this design and is accepted deliberately: the
autosome gate is the instrument that adjudicates it. If the gate fails on miRNA rows, the fix is
to remove the now-redundant hand-coded suppressions, not to narrow the tier gate.

### Defect C — restore the two discarded clauses

In `variant_hits_intron_body`:

- Replace the unconditional `abs(intron_end - intron_start) <= 12` bail with Ensembl's guard:
  skip the intron **only when the variant overlaps it**
  (`overlap(r_start, r_end, intron_start, intron_end)`). Non-overlapping variants fall through
  to the boundary clauses, as in `_intron_effects`.
- Remove the `inner_start > inner_end` early return and evaluate the clauses as a disjunction
  rather than a collapsed range, so `r_start == inner_start` and `r_end == inner_end` still
  apply when the inner range is inverted.

The collapsed range stays valid for well-formed introns; this only restores behaviour in the
degenerate case. The existing 402 unit tests in the file are the primary guard that the
non-degenerate path is untouched.

### Defect D — trace first, then fix

The mechanism is understood on the Ensembl side (`within_frameshift_intron` routes the variant
into the coding path, producing protein HGVS without CDS/protein positions) but not yet located
on the engine side. Stage 4 begins with a trace of `protein_hgvs_for_output_with_semantics` and
the `in_frameshift_intron && coding_class.is_none()` block at `:1878-1890`, which strips
`FrameshiftVariant` and its peers, to establish where the protein HGVS is dropped. Only then is
the fix designed.

Scoping this as investigation-then-fix is deliberate: it is 1 row of 364, and guessing at it
would risk the other 363.

## Staging

Four stages, each independently verifiable, each starting from a failing test.

| stage | defect | rows | risk |
|---|---|---:|---|
| 1 | A — EXON/INTRON ranges | 324 | low; additive, hit predicates unchanged |
| 2 | B — tier gate + ablation predicate | 37 | medium; miRNA interaction |
| 3 | C — frameshift-intron fall-through | 2 | medium; widens a hot predicate for every short intron |
| 4 | D — frameshift-intron HGVSp | 1 | unknown; trace first |

A and B run first because they are mechanical and carry 99% of the rows. C and D run last, where
a surprise cannot contaminate the bulk result.

## Verification

**Inner loop.** A 3-variant VCF built from
`input/HG002_chrXY_norm_acgt.vcf.gz` — `chrX:3886710`, `chrY:6246324`, `chrX:104069779` — plus a
2-variant VCF for C (`chrX:10015674`, `chrX:119605952`). Both tools' full outputs are already in
the investigation directory, so per-transcript diffs need no VEP re-run.

**Stage gate.** chrX and chrY to 0 semantic mismatches and strict body MD5, per §8 of `HANDOVER.md`:

```bash
uv run python scripts/run_comparison.py \
  --release 116 --profile merged_plugins --chroms X \
  --vcf   $DATA/input/HG002_chrXY_norm_acgt.vcf.gz \
  --vep   $DATA/output/116/plugins/HG002_chrX_5plugins_vep116_caddfix.vcf.gz \
  --plugin-cache $DATA/plugin_cache --workers 4 --bgzf --force

uv run python scripts/md5_concordance.py \
  --pair <vep slice>.vcf <vepyr>.vcf.gz --mode strict --explain --explain-limit 0
```

`--vcf` and `--vep` are both required; the profile otherwise resolves the autosome benchmark, and
plugin references are per-contig.

**Regression gate — non-negotiable.** `VEP_COMPARISON_WORKERS=4 ./scripts/run_all_plugin_comparisons.sh`
must stay at 22/22 `body=ok`, 4,096,123 records, 69,299,753 CSQ entries, zero mismatches. Stages 2
and 3 both touch logic shared with the autosomes; this is the only instrument that can detect the
miRNA interaction or a short-intron regression.

Also run `cargo test` in the engine crate — 402 unit tests in `transcript_consequence.rs` assert
exact term sets and are the fastest signal for stages 2 and 3.

## Provenance and repository state

Engine work branches off `datafusion-bio-functions` **`origin/master` = v0.19.1 `c3d4a5d`**, not
the reference's v0.19.0 `01fa21f`. The two intervening commits (`3b32256`, `8339fa3`) are
coordinate fixes in the `ranges` module, untouched by consequence logic, so they cannot confound
the result — and branching from master avoids a rebase before merge.

Preconditions before any of this reproduces:

- The local vepyr checkout is at `7fcaf7a` and pins engine v0.18.0 (`e937e49`). It must be pulled
  to `origin/master` = `a6ba994`, which pins v0.19.0 (`01fa21f`), before the comparison scripts
  from PRs #48 and #49 exist.
- GSD is not initialized in this repo (`.planning/ROADMAP.md` absent), so `/gsd:*` entry points
  error out. This spec follows the `docs/superpowers/` convention already in use here.

Reference provenance: Ensembl VEP `release_116.0`, merged cache `116_GRCh38_merged`, plugin
manifests vepyr-plugins `8af8353`, input GIAB HG002 chrXY smallvar v1.0 GRCh38, Ensembl Perl read
from `ensembl-variation` `23c76f60b`.

## Scope and non-goals

**Coverage caveat.** 221,426 of 255,212 chrXY records (86.8%) are compared. 24,327 are excluded
for non-ACGT alleles — VEP detects input format from the first record, which is `chrX 222582 . Y C`,
and aborts the whole file with `Can't detect input format` — and 9,459 for `*` spanning-deletion
ALTs. Both tools see the identical filtered input, so the comparison is sound over what it covers,
but chrX/Y parity must not be quoted as whole-benchmark coverage. The excluded records are
unexamined and need their own decision.

**Non-goal: porting Ensembl's full predicate table.** Ensembl evaluates 31 independent
transcript-scoped `OverlapConsequence` predicates; the engine uses one 263-line mutually-exclusive
branch tree plus ~476 lines of helpers, with 10 `terms.insert` / 4 `terms.remove` sites and a
60-line `strip_parent_terms` doing the conflict resolution that tier gating and `include` gates do
upstream. A faithful port means rewriting ~740 lines into 31 predicates plus
`_pre_consequence_predicates` / `_get_oc_list` / include-gate machinery, against ~2,441 lines of
Perl, underneath 402 term-set assertions and a byte-identical 4M-record gate.

It was evaluated and rejected for this work on three grounds: it would fix B and C but nothing of
A (324 of 364 rows) or D, both of which are `OutputFactory`/HGVS concerns; the branch tree is a
densely instrumented hot path whose short-circuiting looks load-bearing against a 50x-speedup
constraint; and `STRICT_VEP_PARITY_PLAN.md:517` already records the decision that *"every planned
rewrite starts from a reviewed Rust baseline rather than direct replacement."*

The generic tier gate is the part of that architecture worth having now, at ~40 lines instead of
~2–4k. Recorded here as a future direction, not a rejection on merit.
