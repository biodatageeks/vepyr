# chrX/chrY Core-Annotation Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 364 remaining CSQ field mismatches between vepyr and Ensembl VEP 116 on chrX/chrY, without disturbing the byte-identical result on chr1–22.

**Architecture:** Four independent defects in the consequence engine. Two are output-formatting (`EXON`/`INTRON` must be `min-max` ranges, not single numbers). One is a missing consequence predicate, fixed by adding Ensembl's *generic* tier gate rather than an ablation special case. One is a degenerate-geometry bug in intron-body detection at 2 bp introns. A fourth (missing `HGVSp`) is diagnosed before it is fixed. Term production is **not** restructured — the tier gate runs on the finished term set at the emit boundary, so every field derived from the pre-gate terms is untouched.

**Tech Stack:** Rust 2021, `cargo test`; verification via the vepyr Python e2e harness (`uv run python scripts/…`), `bcftools`, `tabix`.

**Spec:** `docs/superpowers/specs/2026-08-28-chrxy-core-annotation-parity-design.md` (in the vepyr repo, commit `f131554`)

## Global Constraints

- **All code changes land in `datafusion-bio-function-vep`**, i.e. the repo at `/Users/mwiewior/research/git/datafusion-bio-functions`, path `datafusion/bio-function-vep`. No consequence logic changes in vepyr.
- **Branch off `origin/master` = v0.19.1 `c3d4a5d`.** Not the reference build's v0.19.0 `01fa21f`. The two intervening commits (`3b32256`, `8339fa3`) touch only the `ranges` module.
- **Do not change the hit predicates in `which_exon_str` / `which_intron_str`.** Insertions use `start > f.start && start <= f.end`; everything else uses `overlaps(...)`. These reproduce Ensembl's `overlap()` on `(P, P-1)` insertion coordinates and are why chr1–22 passes. Only accumulation changes.
- **Do not restructure term production.** `evaluate_transcript_overlap_inner` must keep returning the full pre-gate term set and `coding_class` unchanged. `original_terms_allow_protein_hgvs(&terms)` at `transcript_consequence.rs:1332` must continue to see pre-gate terms.
- **Every new or modified parity-sensitive function carries a traceability doc comment** with an Ensembl GitHub permalink and line range, per `STRICT_VEP_PARITY_PLAN.md`.
- **Regression gate, non-negotiable:** chr1–22 must stay at 22/22 `body=ok`, **4,096,123** records, **69,299,753** CSQ entries, zero mismatches in every category.
- **Target:** chrX and chrY reach 0 semantic mismatches and pass strict body MD5.
- Reference tier data is `Config.pm` `@OVERLAP_CONSEQUENCES`: 41 consequences total, 31 transcript-scoped (2 tier-1, 1 tier-2, 28 tier-3).

---

## Preconditions

Do these once, before Task 1. They are environment setup, not a reviewable deliverable.

- [ ] **Pull vepyr to `origin/master`.** The local checkout is at `7fcaf7a` and pins engine v0.18.0; the comparison scripts used for verification arrive with PRs #48 and #49.

```bash
cd /Users/mwiewior/research/git/vepyr
git checkout master && git pull    # expect HEAD == a6ba994
grep datafusion-bio-function-vep Cargo.toml   # expect rev 01fa21f8533de17aeffa5ee9042381c4171ef02b
```

- [ ] **Create the engine branch.**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
git fetch origin
git checkout -b fix/chrxy-core-annotation-parity origin/master
git log --oneline -1    # expect c3d4a5d chore: release v0.19.1
```

- [ ] **Confirm the baseline test suite is green before changing anything.**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
cargo test -p datafusion-bio-function-vep --lib 2>&1 | tail -5
```

Expected: `test result: ok.` with 402 tests in `transcript_consequence.rs` among them. If this is already red, stop and report — do not start implementing on a broken baseline.

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `datafusion/bio-function-vep/src/so_terms.rs` | `SoTerm` enum, `rank()`, `impact()`. Gains `tier()` — pure data transcribed from `Config.pm`. | 3 |
| `datafusion/bio-function-vep/src/transcript_consequence.rs` | Consequence engine. Gains range accumulation in two output helpers, `transcript_is_ablated`, `apply_tier_gate`, `overlap_perl`, and a corrected `variant_hits_intron_body`. | 1, 2, 4, 5, 6 |
| `docs/superpowers/plans/2026-08-28-chrxy-core-annotation-parity.md` (vepyr) | This plan. Task 6 appends its diagnosis here. | 6 |

Both source files already exist and are large (23,805 and 345 lines). The codebase pattern is one big module per concern with an inline `#[cfg(test)] mod tests`; follow it. Do not split these files.

---

### Task 1: EXON emits a range when the variant spans multiple exons

**Files:**
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs:8090-8106` (`which_exon_str`)
- Test: same file, inline `#[cfg(test)] mod tests` — add next to the existing `which_exon_str_*` tests at `:13254`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `fn which_exon_str(variant: &VariantInput, tx_exons: &[&ExonFeature]) -> Option<String>` — signature unchanged; return value gains the `"{min}-{max}/{total}"` form.

Test helpers already in the test module: `exon(tx_id: &str, num: i32, start: i64, end: i64) -> ExonFeature` and `var(chrom: &str, start: i64, end: i64, r: &str, a: &str) -> VariantInput`.

- [ ] **Step 1: Write the failing tests**

Add to the test module, immediately after `which_exon_str_no_overlap`:

```rust
    /// A deletion spanning every exon reports the full range, not one exon.
    /// Reproduces chrX:3886710 / ENST00000424415, where VEP emits `1-4/4`.
    ///
    /// Traceability:
    /// - Ensembl Variation `BaseTranscriptVariation::exon_number()`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/BaseTranscriptVariation.pm#L679-L713>
    #[test]
    fn which_exon_str_spanning_deletion_reports_a_range() {
        let exons = vec![
            exon("tx1", 1, 100, 200),
            exon("tx1", 2, 300, 400),
            exon("tx1", 3, 500, 600),
            exon("tx1", 4, 700, 800),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        let v = var("22", 50, 900, "ACGT", "-");
        assert_eq!(which_exon_str(&v, &refs), Some("1-4/4".to_string()));
    }

    /// A deletion covering a contiguous subset reports just that subset.
    /// Reproduces chrX:3886710 / ENST00000648217, where VEP emits `1-4/13`.
    #[test]
    fn which_exon_str_partial_span_reports_the_covered_subset() {
        let exons = vec![
            exon("tx1", 1, 100, 200),
            exon("tx1", 2, 300, 400),
            exon("tx1", 3, 500, 600),
            exon("tx1", 4, 700, 800),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        let v = var("22", 150, 550, "ACGT", "-");
        assert_eq!(which_exon_str(&v, &refs), Some("1-3/4".to_string()));
    }

    /// Exon numbers, not slice indices: on a minus-strand transcript the
    /// cache stores descending exon_number against ascending genomic start,
    /// and Ensembl sorts the collected numbers numerically.
    #[test]
    fn which_exon_str_range_uses_min_max_of_exon_numbers() {
        let exons = vec![
            exon("tx1", 3, 100, 200),
            exon("tx1", 2, 300, 400),
            exon("tx1", 1, 500, 600),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        let v = var("22", 150, 550, "ACGT", "-");
        assert_eq!(which_exon_str(&v, &refs), Some("1-3/3".to_string()));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
cargo test -p datafusion-bio-function-vep --lib which_exon_str -- --nocapture
```

Expected: the three new tests FAIL with `assertion `left == right` failed` showing `Some("1/4")` (or `Some("3/3")` for the third) against the expected range. The three pre-existing `which_exon_str_*` tests PASS.

- [ ] **Step 3: Replace the function body with range accumulation**

Replace `which_exon_str` at `:8090-8106` entirely. Keep the doc comment above it as-is:

```rust
fn which_exon_str(variant: &VariantInput, tx_exons: &[&ExonFeature]) -> Option<String> {
    if tx_exons.is_empty() {
        return None;
    }
    let is_ins = variant.ref_allele == "-";
    let total = tx_exons.len();
    // Ensembl collects every overlapping exon, sorts the numbers, and emits
    // `numbers[0]-numbers[-1]`; min/max is the same result without the sort.
    let mut lo: Option<i32> = None;
    let mut hi: Option<i32> = None;
    for exon in tx_exons {
        let hit = if is_ins {
            variant.start > exon.start && variant.start <= exon.end
        } else {
            overlaps(variant.start, variant.end, exon.start, exon.end)
        };
        if hit {
            lo = Some(lo.map_or(exon.exon_number, |n| n.min(exon.exon_number)));
            hi = Some(hi.map_or(exon.exon_number, |n| n.max(exon.exon_number)));
        }
    }
    match (lo, hi) {
        (Some(lo), Some(hi)) if lo == hi => Some(format!("{lo}/{total}")),
        (Some(lo), Some(hi)) => Some(format!("{lo}-{hi}/{total}")),
        _ => None,
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p datafusion-bio-function-vep --lib which_exon_str -- --nocapture
```

Expected: PASS, 6 tests (3 new + 3 pre-existing).

- [ ] **Step 5: Run the whole crate suite for regressions**

```bash
cargo test -p datafusion-bio-function-vep 2>&1 | tail -20
```

Expected: `test result: ok.` If any pre-existing test now fails, it is asserting the single-value form on a multi-exon overlap. Read the failing test: if it is a genuine spanning case, the *test* is wrong and should be updated with a comment citing `BaseTranscriptVariation.pm#L679-L713`. If it is a single-exon case, the implementation is wrong — do not edit the test.

- [ ] **Step 6: Commit**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
git add datafusion/bio-function-vep/src/transcript_consequence.rs
git commit -m "fix(vep): EXON reports a range when a variant spans several exons

Ensembl's BaseTranscriptVariation::exon_number collects every overlapping
exon and emits numbers[0]-numbers[-1]; which_exon_str returned on the first
hit. Accounts for part of the 324 EXON/INTRON range mismatches on chrX/chrY.

Traceability: ensembl-variation BaseTranscriptVariation.pm#L679-L713"
```

---

### Task 2: INTRON emits a range when the variant spans multiple introns

**Files:**
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs:8111-8158` (`which_intron_str`)
- Test: same file, next to the existing `which_intron_str_*` tests at `:13283`

**Interfaces:**
- Consumes: nothing from Task 1 (independent function, same file).
- Produces: `fn which_intron_str(variant: &VariantInput, tx_exons: &[&ExonFeature], strand: i8) -> Option<String>` — signature unchanged; return value gains the `"{min}-{max}/{total}"` form.

The intron number is derived per window index with the existing strand mapping (`i + 1` on `+`, `total_introns - i` on `−`). On the minus strand the numbers descend as `i` ascends, so **min/max over the collected numbers is required** — taking the first and last hit would invert the range.

- [ ] **Step 1: Write the failing tests**

Add after `which_intron_str_minus_strand_reverses_numbering`:

```rust
    /// A deletion spanning every intron reports the full range.
    /// Reproduces chrX:3886710 / ENST00000424415, where VEP emits `1-3/3`.
    ///
    /// Traceability:
    /// - Ensembl Variation `BaseTranscriptVariation::intron_number()`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/BaseTranscriptVariation.pm#L727-L760>
    #[test]
    fn which_intron_str_spanning_deletion_reports_a_range() {
        let exons = vec![
            exon("tx1", 1, 100, 200),
            exon("tx1", 2, 300, 400),
            exon("tx1", 3, 500, 600),
            exon("tx1", 4, 700, 800),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        let v = var("22", 50, 900, "ACGT", "-");
        assert_eq!(which_intron_str(&v, &refs, 1), Some("1-3/3".to_string()));
    }

    /// On the minus strand the numbering runs opposite to genomic order, so
    /// the range must be min-max of the numbers, not first-to-last hit.
    #[test]
    fn which_intron_str_minus_strand_range_is_min_max_not_first_last() {
        let exons = vec![
            exon("tx1", 4, 100, 200),
            exon("tx1", 3, 300, 400),
            exon("tx1", 2, 500, 600),
            exon("tx1", 1, 700, 800),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        // Spans genomic introns 0 and 1, which are introns 3 and 2 on the
        // minus strand. First hit is 3, last hit is 2 — "3-2/3" would be wrong.
        let v = var("22", 250, 550, "ACGT", "-");
        assert_eq!(which_intron_str(&v, &refs, -1), Some("2-3/3".to_string()));
    }

    /// A variant inside a single intron keeps the plain "N/total" form.
    #[test]
    fn which_intron_str_single_intron_has_no_range() {
        let exons = vec![
            exon("tx1", 1, 100, 200),
            exon("tx1", 2, 300, 400),
            exon("tx1", 3, 500, 600),
        ];
        let refs: Vec<&ExonFeature> = exons.iter().collect();
        let v = var("22", 250, 260, "ACGTACGTAGC", "-");
        assert_eq!(which_intron_str(&v, &refs, 1), Some("1/2".to_string()));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p datafusion-bio-function-vep --lib which_intron_str -- --nocapture
```

Expected: the first two new tests FAIL (`Some("1/3")` and `Some("3/3")` respectively); `which_intron_str_single_intron_has_no_range` PASSES already; the four pre-existing `which_intron_str_*` tests PASS.

- [ ] **Step 3: Replace the accumulation in the loop**

In `which_intron_str`, replace the loop and its trailing `None` (currently `:8123-8157`, the block from `for (i, pair) in sorted.windows(2).enumerate() {` through the closing `None`). Leave the `if tx_exons.len() < 2` guard, the `start_sorted` call, `total_introns`, and the whole traceability comment block inside the loop untouched:

```rust
    // Ensembl collects every overlapping intron, sorts the numbers, and emits
    // `numbers[0]-numbers[-1]`. On the minus strand the numbers descend as the
    // window index ascends, so min/max is required rather than first/last.
    let mut lo: Option<usize> = None;
    let mut hi: Option<usize> = None;

    for (i, pair) in sorted.windows(2).enumerate() {
        let intron_start = pair[0].end + 1;
        let intron_end = pair[1].start - 1;
        // <-- the existing traceability comment block stays here, unchanged -->
        let hit = if variant.ref_allele == "-" {
            variant.start > intron_start && variant.start <= intron_end
        } else {
            overlaps(variant.start, variant.end, intron_start, intron_end)
        };
        if hit {
            let intron_num = if strand >= 0 {
                i + 1
            } else {
                total_introns - i
            };
            lo = Some(lo.map_or(intron_num, |n| n.min(intron_num)));
            hi = Some(hi.map_or(intron_num, |n| n.max(intron_num)));
        }
    }

    match (lo, hi) {
        (Some(lo), Some(hi)) if lo == hi => Some(format!("{lo}/{total_introns}")),
        (Some(lo), Some(hi)) => Some(format!("{lo}-{hi}/{total_introns}")),
        _ => None,
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p datafusion-bio-function-vep --lib which_intron_str -- --nocapture
```

Expected: PASS, 7 tests.

- [ ] **Step 5: Run the whole crate suite**

```bash
cargo test -p datafusion-bio-function-vep 2>&1 | tail -20
```

Expected: `test result: ok.` Same rule as Task 1 step 5 for any pre-existing failure.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-function-vep/src/transcript_consequence.rs
git commit -m "fix(vep): INTRON reports a range when a variant spans several introns

Same defect as EXON: which_intron_str returned on the first overlapping
intron where Ensembl's intron_number collects all of them. Min/max over the
collected numbers, not first/last, because minus-strand numbering runs
opposite to genomic window order.

Traceability: ensembl-variation BaseTranscriptVariation.pm#L727-L760"
```

---

### Task 3: `SoTerm::tier()`

**Files:**
- Modify: `datafusion/bio-function-vep/src/so_terms.rs` — add `tier()` inside `impl SoTerm`, immediately after `rank()` (which ends at `:241`) and before `impact()` (`:243`)
- Test: same file, in the existing `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: `pub fn tier(self) -> u8` on `SoTerm`. Task 4 depends on this exact name and return type. Values: 1, 2, 3, or 4.

This is transcribed data, so the whole 41-variant enum must be covered explicitly — no `_ =>` catch-all arm, so that adding an `SoTerm` variant later fails to compile rather than silently defaulting.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `so_terms.rs`:

```rust
    /// Tiers transcribed from Ensembl's @OVERLAP_CONSEQUENCES. Evaluation is
    /// tier-ascending and a tier <= 2 match suppresses every higher tier, so
    /// these values decide which consequences can co-occur.
    ///
    /// Traceability:
    /// - Ensembl Variation `Config.pm` `@OVERLAP_CONSEQUENCES`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/Config.pm#L347>
    #[test]
    fn tier_matches_ensembl_config() {
        assert_eq!(SoTerm::TranscriptAblation.tier(), 1);
        assert_eq!(SoTerm::TranscriptAmplification.tier(), 1);

        assert_eq!(SoTerm::MatureMirnaVariant.tier(), 2);
        assert_eq!(SoTerm::TfbsAblation.tier(), 2);
        assert_eq!(SoTerm::TfbsAmplification.tier(), 2);
        assert_eq!(SoTerm::TfBindingSiteVariant.tier(), 2);
        assert_eq!(SoTerm::RegulatoryRegionAblation.tier(), 2);
        assert_eq!(SoTerm::RegulatoryRegionAmplification.tier(), 2);
        assert_eq!(SoTerm::RegulatoryRegionVariant.tier(), 2);

        assert_eq!(SoTerm::IntergenicVariant.tier(), 4);
        assert_eq!(SoTerm::SequenceVariant.tier(), 4);

        // Spot-check the tier-3 bulk, including the ones a spanning deletion
        // produces today and that transcript_ablation must suppress.
        assert_eq!(SoTerm::SpliceAcceptorVariant.tier(), 3);
        assert_eq!(SoTerm::SpliceDonorVariant.tier(), 3);
        assert_eq!(SoTerm::SpliceDonor5thBaseVariant.tier(), 3);
        assert_eq!(SoTerm::NonCodingTranscriptExonVariant.tier(), 3);
        assert_eq!(SoTerm::IntronVariant.tier(), 3);
        assert_eq!(SoTerm::NmdTranscriptVariant.tier(), 3);
        assert_eq!(SoTerm::FeatureElongation.tier(), 3);
        assert_eq!(SoTerm::FeatureTruncation.tier(), 3);
    }

    /// Exactly 2 tier-1, 7 tier-2, 30 tier-3 and 2 tier-4 across all 41 terms.
    #[test]
    fn tier_histogram_is_complete() {
        let mut counts = [0usize; 5];
        for term in ALL_SO_TERMS {
            counts[term.tier() as usize] += 1;
        }
        assert_eq!(counts[1], 2, "tier 1");
        assert_eq!(counts[2], 7, "tier 2");
        assert_eq!(counts[3], 30, "tier 3");
        assert_eq!(counts[4], 2, "tier 4");
        assert_eq!(counts[1] + counts[2] + counts[3] + counts[4], 41);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p datafusion-bio-function-vep --lib tier_ -- --nocapture
```

Expected: FAIL to compile — `no method named `tier` found for enum `SoTerm``.

- [ ] **Step 3: Add `tier()`**

Insert into `impl SoTerm` in `so_terms.rs`, between `rank()` and `impact()`:

```rust
    /// Ensembl consequence tier, transcribed from `Config.pm`'s
    /// `@OVERLAP_CONSEQUENCES`.
    ///
    /// Consequences are evaluated in tier-ascending order
    /// (`@SORTED_OVERLAP_CONSEQUENCES = sort {$a->tier <=> $b->tier}`), and a
    /// match at tier 1 or 2 suppresses every higher tier. A tier-3 match sets
    /// no assigned tier, so tier 3 does not gate tier 4. See
    /// `transcript_consequence::apply_tier_gate`.
    ///
    /// Deliberately exhaustive: a new `SoTerm` variant must fail to compile
    /// here rather than silently take a default tier.
    ///
    /// Traceability:
    /// - Ensembl Variation `Config.pm` `@OVERLAP_CONSEQUENCES`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/Config.pm#L347>
    /// - Ensembl Variation `BaseVariationFeatureOverlapAllele.pm` tier sort
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/BaseVariationFeatureOverlapAllele.pm#L69>
    pub fn tier(self) -> u8 {
        match self {
            Self::TranscriptAblation | Self::TranscriptAmplification => 1,

            Self::MatureMirnaVariant
            | Self::TfbsAblation
            | Self::TfbsAmplification
            | Self::TfBindingSiteVariant
            | Self::RegulatoryRegionAblation
            | Self::RegulatoryRegionAmplification
            | Self::RegulatoryRegionVariant => 2,

            Self::IntergenicVariant | Self::SequenceVariant => 4,

            Self::SpliceAcceptorVariant
            | Self::SpliceDonorVariant
            | Self::StopGained
            | Self::FrameshiftVariant
            | Self::StopLost
            | Self::StartLost
            | Self::FeatureElongation
            | Self::FeatureTruncation
            | Self::InframeInsertion
            | Self::InframeDeletion
            | Self::MissenseVariant
            | Self::ProteinAlteringVariant
            | Self::SpliceDonor5thBaseVariant
            | Self::SpliceRegionVariant
            | Self::SpliceDonorRegionVariant
            | Self::SplicePolypyrimidineTractVariant
            | Self::IncompleteTerminalCodonVariant
            | Self::StartRetainedVariant
            | Self::StopRetainedVariant
            | Self::SynonymousVariant
            | Self::CodingSequenceVariant
            | Self::FivePrimeUtrVariant
            | Self::ThreePrimeUtrVariant
            | Self::NonCodingTranscriptExonVariant
            | Self::IntronVariant
            | Self::NmdTranscriptVariant
            | Self::NonCodingTranscriptVariant
            | Self::CodingTranscriptVariant
            | Self::UpstreamGeneVariant
            | Self::DownstreamGeneVariant => 3,
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p datafusion-bio-function-vep --lib tier_ -- --nocapture
```

Expected: PASS, 2 tests. If `tier_histogram_is_complete` fails, a variant landed in the wrong arm — the histogram is the guard against a transcription slip.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/so_terms.rs
git commit -m "feat(vep): add SoTerm::tier() from Ensembl's @OVERLAP_CONSEQUENCES

Pure data, no behaviour change yet. Exhaustive match so a new SoTerm
variant fails to compile rather than defaulting to a tier.

Traceability: ensembl-variation Config.pm#L347,
BaseVariationFeatureOverlapAllele.pm#L69"
```

---

### Task 4: `transcript_ablation` via a generic tier gate

**Files:**
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`
  - `:1296` — change the binding `let (terms, coding_class) = …` to `let (mut terms, coding_class) = …`
  - `:1536` — insert the ablation term and the gate immediately before `let push_started = profiling.then(Instant::now);`
  - add two free functions near `strip_parent_terms`
- Test: same file, inline test module

**Interfaces:**
- Consumes: `SoTerm::tier() -> u8` from Task 3.
- Produces:
  - `fn transcript_is_ablated(variant: &VariantInput, tx: &TranscriptFeature) -> bool`
  - `fn apply_tier_gate(terms: &mut Vec<SoTerm>)`

`terms` is read in exactly three places between its binding and the push: the `if !terms.is_empty()` guard at `:1313`, `original_terms_allow_protein_hgvs(&terms)` at `:1332`, and the move into `TranscriptConsequence` at `:1541`. Inserting at `:1536` therefore leaves every derived field — `hgvsc`, `hgvsp`, `cdna_position`, `cds_position`, `protein_position`, `amino_acids`, `codons`, `exon_str`, `intron_str` — computed from the pre-gate terms, which is what Ensembl does and what keeps chr1–22 byte-identical.

**Scoping note:** the gate sits inside the `if !terms.is_empty()` block, so a transcript that is ablated but produces no other term emits no CSQ entry. That case does not occur — a deletion covering a whole transcript always overlaps its exons — and moving the check outside would risk changing CSQ entry counts, which currently mismatch zero times. Leave it inside.

- [ ] **Step 1: Write the failing tests**

Add to the test module, next to `deletion_spanning_a_motif_is_a_tfbs_ablation` (`:23620`):

```rust
    /// A deletion whose span covers an entire transcript is a
    /// transcript_ablation, and being tier 1 it suppresses every tier-3 term
    /// the endpoints would otherwise produce.
    /// Reproduces chrX:3886710 / ENST00000424415.
    ///
    /// Traceability:
    /// - Ensembl Variation `VariationEffect::feature_ablation()`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L323-L328>
    #[test]
    fn deletion_spanning_a_transcript_is_a_transcript_ablation() {
        let engine = TranscriptConsequenceEngine::default();
        let transcripts = vec![tx("tx1", "22", 1_000, 2_000, 1, "lncRNA", None, None)];
        let exons = vec![
            exon("tx1", 1, 1_000, 1_200),
            exon("tx1", 2, 1_800, 2_000),
        ];
        let out = engine.evaluate_variant_with_context(
            &var("22", 900, 2_100, "ACGT", "-"),
            &transcripts,
            &exons,
            &[],
            &[],
            &[],
            &[],
            &[],
        );
        let c = out
            .iter()
            .find(|c| c.transcript_id.as_deref() == Some("tx1"))
            .expect("transcript consequence");
        assert_eq!(c.terms, vec![SoTerm::TranscriptAblation]);
    }

    /// A deletion that only clips the transcript is not an ablation, and the
    /// ordinary tier-3 terms survive.
    #[test]
    fn partial_deletion_is_not_a_transcript_ablation() {
        let engine = TranscriptConsequenceEngine::default();
        let transcripts = vec![tx("tx1", "22", 1_000, 2_000, 1, "lncRNA", None, None)];
        let exons = vec![
            exon("tx1", 1, 1_000, 1_200),
            exon("tx1", 2, 1_800, 2_000),
        ];
        let out = engine.evaluate_variant_with_context(
            &var("22", 1_100, 1_150, "ACGT", "-"),
            &transcripts,
            &exons,
            &[],
            &[],
            &[],
            &[],
            &[],
        );
        let c = out
            .iter()
            .find(|c| c.transcript_id.as_deref() == Some("tx1"))
            .expect("transcript consequence");
        assert!(!c.terms.contains(&SoTerm::TranscriptAblation));
        assert!(!c.terms.is_empty());
    }

    /// An insertion spanning the same coordinates is not a deletion, so no
    /// ablation.
    #[test]
    fn insertion_over_a_transcript_is_not_a_transcript_ablation() {
        let engine = TranscriptConsequenceEngine::default();
        let transcripts = vec![tx("tx1", "22", 1_000, 2_000, 1, "lncRNA", None, None)];
        let exons = vec![
            exon("tx1", 1, 1_000, 1_200),
            exon("tx1", 2, 1_800, 2_000),
        ];
        let out = engine.evaluate_variant_with_context(
            &var("22", 1_100, 1_099, "-", "ACGT"),
            &transcripts,
            &exons,
            &[],
            &[],
            &[],
            &[],
            &[],
        );
        if let Some(c) = out.iter().find(|c| c.transcript_id.as_deref() == Some("tx1")) {
            assert!(!c.terms.contains(&SoTerm::TranscriptAblation));
        }
    }

    /// The gate keeps every term at the assigned tier, drops only higher
    /// tiers, and a tier-3 match does not gate tier 4.
    #[test]
    fn apply_tier_gate_keeps_the_assigned_tier_only() {
        // tier 1 suppresses tier 3
        let mut t = vec![SoTerm::TranscriptAblation, SoTerm::SpliceAcceptorVariant];
        apply_tier_gate(&mut t);
        assert_eq!(t, vec![SoTerm::TranscriptAblation]);

        // tier 2 peers co-occur, and suppress tier 3
        let mut t = vec![
            SoTerm::TfbsAblation,
            SoTerm::TfBindingSiteVariant,
            SoTerm::IntronVariant,
        ];
        apply_tier_gate(&mut t);
        assert_eq!(t, vec![SoTerm::TfbsAblation, SoTerm::TfBindingSiteVariant]);

        // no tier <= 2 present: nothing is gated, tier 3 does not gate tier 4
        let mut t = vec![SoTerm::IntronVariant, SoTerm::IntergenicVariant];
        apply_tier_gate(&mut t);
        assert_eq!(t, vec![SoTerm::IntronVariant, SoTerm::IntergenicVariant]);

        // empty stays empty
        let mut t: Vec<SoTerm> = vec![];
        apply_tier_gate(&mut t);
        assert!(t.is_empty());
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p datafusion-bio-function-vep --lib \
  -- transcript_ablation apply_tier_gate --nocapture
```

Expected: FAIL to compile — `cannot find function `apply_tier_gate``. After Step 3 adds the functions but before wiring, `deletion_spanning_a_transcript_is_a_transcript_ablation` fails on the assertion with the splice/non-coding term list.

- [ ] **Step 3: Add the two functions**

Insert immediately before `fn strip_parent_terms(` in `transcript_consequence.rs`:

```rust
/// VEP `feature_ablation` specialised to transcripts: the variant is a
/// deletion whose span completely covers the transcript.
///
/// Traceability:
/// - Ensembl Variation `VariationEffect::feature_ablation()`
///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L323-L328>
/// - Ensembl Variation `VariationEffect::complete_overlap_feature()`
///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L169-L177>
fn transcript_is_ablated(variant: &VariantInput, tx: &TranscriptFeature) -> bool {
    let is_deletion = variant.ref_allele != "-"
        && !variant.ref_allele.is_empty()
        && (variant.alt_allele == "-" || variant.alt_allele.len() < variant.ref_allele.len());
    is_deletion && variant.start <= tx.start && variant.end >= tx.end
}

/// Apply Ensembl's consequence tier gate to a finished term set.
///
/// Ensembl evaluates consequences in tier-ascending order and breaks out with
/// `last if $assigned_tier && $oc->{tier} > $assigned_tier`, where
/// `$assigned_tier` is only ever set by a match at tier 1 or 2. That reduces
/// exactly to: let `T` be the smallest matched tier that is `<= 2`; if one
/// exists, retain only terms with `tier <= T`. A tier-3 match never assigns a
/// tier, so tier 3 does not gate tier 4.
///
/// Traceability:
/// - Ensembl Variation `BaseVariationFeatureOverlapAllele::get_all_OverlapConsequences()`
///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/BaseVariationFeatureOverlapAllele.pm#L243-L288>
fn apply_tier_gate(terms: &mut Vec<SoTerm>) {
    let Some(assigned) = terms.iter().map(|t| t.tier()).filter(|&t| t <= 2).min() else {
        return;
    };
    terms.retain(|t| t.tier() <= assigned);
}
```

- [ ] **Step 4: Wire it at the emit boundary**

Two edits in `evaluate_variant_prepared_profiled`:

1. At `:1296`, make the binding mutable:

```rust
                    let (mut terms, coding_class) = if let Some(profile) = profile.as_deref_mut() {
```

2. At `:1536`, immediately before `let push_started = profiling.then(Instant::now);`, insert:

```rust
                        // VEP's tier-1 feature_ablation. Applied here, after
                        // every field above has been derived from the
                        // pre-gate term set, because Ensembl's tier gate
                        // filters the consequence list only — EXON/INTRON,
                        // HGVSc and the position fields are produced by
                        // OutputFactory and stay populated on an ablated
                        // transcript.
                        if transcript_is_ablated(variant, tx) {
                            terms.push(SoTerm::TranscriptAblation);
                            terms.sort_by_key(|t| t.rank());
                        }
                        apply_tier_gate(&mut terms);
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cargo test -p datafusion-bio-function-vep --lib \
  -- transcript_ablation apply_tier_gate --nocapture
```

Expected: PASS, 4 tests.

- [ ] **Step 6: Run the whole crate suite — this is the miRNA check**

```bash
cargo test -p datafusion-bio-function-vep 2>&1 | tail -30
```

Expected: `test result: ok.`

**If tests involving `MatureMirnaVariant` now fail, that is the predicted tier-2 interaction, not a surprise.** `mature_miRNA_variant` is tier 2, so the gate now suppresses tier-3 terms that the engine previously hand-suppressed at `:1820-1832` and `:1965-1971`, plus splice terms `add_intron_splice_terms` adds unconditionally. The spec pre-commits the resolution: **remove the now-redundant hand-coded suppressions, do not narrow the gate.** Record which tests changed and why in the commit message. Do not proceed to Task 5 until the suite is green.

- [ ] **Step 7: Commit**

```bash
git add datafusion/bio-function-vep/src/transcript_consequence.rs
git commit -m "fix(vep): emit transcript_ablation via Ensembl's tier gate

A deletion spanning a whole transcript is VEP's tier-1 feature_ablation,
which suppresses every tier-3 term the deletion endpoints produce. vepyr
emitted the endpoint splice terms instead — 37 of the 364 chrX/chrY
mismatches.

Implemented as the general tier rule rather than an ablation special case,
so every tier-1/tier-2 term is gated consistently. The gate runs at the emit
boundary, after HGVSc, EXON/INTRON and the position fields have been derived
from the pre-gate term set — Ensembl's gate filters the consequence list
only, and VEP still populates those fields on an ablated transcript.

Traceability: ensembl-variation VariationEffect.pm#L323-L328,
BaseVariationFeatureOverlapAllele.pm#L243-L288"
```

---

### Task 5: intron-body detection at a 2 bp intron

**Files:**
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs:9679-9699` (`variant_hits_intron_body`), and add `overlap_perl` next to `overlaps` at `:3807`
- Test: same file, inline test module

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `fn overlap_perl(a_start: i64, a_end: i64, b_start: i64, b_end: i64) -> bool`. `variant_hits_intron_body`'s signature is unchanged.

`overlaps` at `:3807` **normalises inverted ranges** by swapping them, so it cannot express Ensembl's `overlap()` on insertion coordinates `(P, P-1)`. That is why the current code special-cases insertions with a derived range. The fix needs the literal form, so add it rather than reusing `overlaps`.

Two divergences to correct, both established from the exon coordinates in the 116 GRCh38 merged cache:

1. The unconditional `abs(intron_end - intron_start) <= 12` bail. Ensembl skips a frameshift intron **only when the variant overlaps it**. For `chrX:10015674 G>GC` against `ENST00000380861`, intron 1 is `[10015675, 10015676]` and the insertion's `(r_start, r_end) = (10015675, 10015674)` does **not** overlap it, so Ensembl falls through.
2. Collapsing Ensembl's clauses into the contiguous range `[inner_start, inner_end + 1]`. Valid while `inner_start <= inner_end`; for a 2 bp intron `inner_start = 10015677 > inner_end = 10015674`, and the standalone clause `r_end == inner_end` (`10015674 == 10015674`) still fires.

**Known limitation, deliberately out of scope:** Ensembl evaluates its clauses against both the shifted and unshifted coordinates. `variant.start`/`variant.end` here are unshifted, so this implements Ensembl's unshifted pair of clauses — which is what both defect-C cases fire on. The shifted pair stays unimplemented; note it in the doc comment.

- [ ] **Step 1: Write the failing tests**

Add to the test module, next to the existing `variant_hits_intron_body` / intron-boundary tests around `:16074`:

```rust
    /// An insertion at the first base of a 2 bp intron is intronic in VEP:
    /// the frameshift-intron skip does not apply because the insertion's
    /// (P, P-1) coordinates do not overlap the intron, and the standalone
    /// `r_end == intron_end - 2` clause then matches.
    /// Reproduces chrX:10015674 G>GC / ENST00000380861, exon 1 ending
    /// 10015674 and exon 2 starting 10015677.
    ///
    /// Traceability:
    /// - Ensembl Variation `BaseTranscriptVariationAllele::_intron_effects()`
    ///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/BaseTranscriptVariationAllele.pm#L99-L149>
    #[test]
    fn insertion_at_a_two_base_intron_boundary_is_intronic() {
        let v = var("X", 10_015_675, 10_015_674, "-", "C");
        assert!(variant_hits_intron_body(&v, 10_015_675, 10_015_676));
    }

    /// The minus-strand case: chrX:119605952 C>CG / NM_001417890.1, exon 2
    /// ending 119605952 and exon 1 starting 119605955.
    #[test]
    fn insertion_at_a_two_base_intron_boundary_is_intronic_minus_strand() {
        let v = var("X", 119_605_953, 119_605_952, "-", "G");
        assert!(variant_hits_intron_body(&v, 119_605_953, 119_605_954));
    }

    /// An insertion that genuinely falls inside a frameshift intron is
    /// within_frameshift_intron, not intronic — Ensembl takes the `next`.
    /// Reproduces chrX:10015674 G>GC / NM_015691.5, whose intron 1 is
    /// [10015674, 10015675] and does contain the insertion.
    #[test]
    fn insertion_inside_a_frameshift_intron_is_not_intron_body() {
        let v = var("X", 10_015_675, 10_015_674, "-", "C");
        assert!(!variant_hits_intron_body(&v, 10_015_674, 10_015_675));
    }

    /// A substitution overlapping a short intron still takes the
    /// frameshift-intron skip.
    #[test]
    fn substitution_inside_a_frameshift_intron_is_not_intron_body() {
        let v = var("X", 10_015_675, 10_015_675, "A", "G");
        assert!(!variant_hits_intron_body(&v, 10_015_674, 10_015_675));
    }

    /// A normal intron is unaffected: interior yes, splice-site bases no.
    #[test]
    fn normal_intron_body_bounds_are_unchanged() {
        let v = var("22", 1_050, 1_050, "A", "G");
        assert!(variant_hits_intron_body(&v, 1_000, 1_100));
        let at_donor = var("22", 1_001, 1_001, "A", "G");
        assert!(!variant_hits_intron_body(&at_donor, 1_000, 1_100));
        let at_acceptor = var("22", 1_099, 1_099, "A", "G");
        assert!(!variant_hits_intron_body(&at_acceptor, 1_000, 1_100));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p datafusion-bio-function-vep --lib \
  -- intron_body two_base_intron frameshift_intron --nocapture
```

Expected: `insertion_at_a_two_base_intron_boundary_is_intronic` and its minus-strand twin FAIL (`assertion failed`). The three negative tests PASS already.

- [ ] **Step 3: Add `overlap_perl`**

Insert immediately after `fn overlaps(...)` ends at `:3818` in `transcript_consequence.rs`:

```rust
/// Ensembl's `overlap()` verbatim, without the range normalisation `overlaps`
/// applies. Insertions carry inverted coordinates `(P, P-1)` and Ensembl
/// relies on that inversion, so normalising would change the result.
///
/// Traceability:
/// - Ensembl Variation `VariationEffect::overlap()`
///   <https://github.com/Ensembl/ensembl-variation/blob/release/116/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L81-L85>
fn overlap_perl(a_start: i64, a_end: i64, b_start: i64, b_end: i64) -> bool {
    a_start <= b_end && a_end >= b_start
}
```

- [ ] **Step 4: Rewrite `variant_hits_intron_body`**

Replace the body at `:9679-9699`, keeping the existing doc comment above it and appending the limitation note to it:

```rust
fn variant_hits_intron_body(variant: &VariantInput, intron_start: i64, intron_end: i64) -> bool {
    if intron_start > intron_end {
        return false;
    }

    // Ensembl's (r_start, r_end): an insertion at P is (P, P-1).
    let is_ins = variant.ref_allele == "-";
    let (r_start, r_end) = if is_ins {
        (variant.start, variant.start - 1)
    } else {
        (variant.start, variant.end)
    };
    let insertion = r_start == r_end + 1;

    // A frameshift intron is skipped only when the variant actually overlaps
    // it — that sets within_frameshift_intron instead. A variant that merely
    // abuts one falls through to the boundary clauses below.
    if (intron_end - intron_start).abs() <= 12
        && overlap_perl(r_start, r_end, intron_start, intron_end)
    {
        return false;
    }

    let inner_start = intron_start + 2;
    let inner_end = intron_end - 2;

    // Three independent clauses. They collapse to the contiguous range
    // [inner_start, inner_end + 1] only while inner_start <= inner_end; for a
    // 2 bp intron the inner range inverts and the equality clauses still
    // apply, which is the whole defect this restores.
    overlap_perl(r_start, r_end, inner_start, inner_end)
        || (insertion && (r_start == inner_start || r_end == inner_end))
}
```

Append to the function's existing doc comment:

```rust
/// Ensembl also evaluates these clauses against the HGVS-shifted coordinates
/// as well as the unshifted ones. `variant.start`/`variant.end` here are
/// unshifted, so this implements the unshifted pair only.
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cargo test -p datafusion-bio-function-vep --lib \
  -- intron_body two_base_intron frameshift_intron --nocapture
```

Expected: PASS, 5 tests.

- [ ] **Step 6: Run the whole crate suite**

```bash
cargo test -p datafusion-bio-function-vep 2>&1 | tail -30
```

Expected: `test result: ok.` This widens a predicate used by every transcript with a short intron, so a failure here is a real signal. If a pre-existing intron test fails, work out from `_intron_effects` which clause Ensembl would take for that geometry before touching either side.

- [ ] **Step 7: Commit**

```bash
git add datafusion/bio-function-vep/src/transcript_consequence.rs
git commit -m "fix(vep): intron_variant at a 2 bp intron boundary

variant_hits_intron_body diverged from _intron_effects twice: it bailed on
any intron of length <= 13 unconditionally, where Ensembl skips a frameshift
intron only when the variant overlaps it; and it collapsed Ensembl's three
OR'd clauses into a contiguous range, which discards the standalone
r_end == intron_end-2 clause once the inner range inverts.

Both chrX cases (10015674 / ENST00000380861 and 119605952 / NM_001417890.1)
abut a 2 bp intron without overlapping it, so both clauses were needed.

Adds overlap_perl because overlaps() normalises inverted ranges and so
cannot express Ensembl's overlap() on insertion coordinates.

Traceability: ensembl-variation BaseTranscriptVariationAllele.pm#L99-L149,
VariationEffect.pm#L81-L85"
```

---

### Task 6: Diagnose the missing `HGVSp` (defect D) — **CHECKPOINT, do not fix yet**

**Files:**
- Test: `datafusion/bio-function-vep/src/transcript_consequence.rs` — one new ignored test pinning the expected output
- Modify: `docs/superpowers/plans/2026-08-28-chrxy-core-annotation-parity.md` (vepyr) — append the diagnosis under "Task 6 findings"

**Interfaces:**
- Consumes: nothing.
- Produces: a written diagnosis plus a `#[ignore]`d failing test. **No fix.**

The deliverable of this task is a diagnosis, because the engine-side mechanism is not yet located and guessing at 1 row of 364 risks the other 363. The Ensembl side is understood: `NM_015691.5` has exon 1 ending 10015673 and exon 2 starting 10015676, so intron 1 is `[10015674, 10015675]` — 2 bp. The insertion at 10015675 **does** overlap it, so Ensembl sets `within_frameshift_intron` and routes the variant through the coding path, producing `HGVSp=NP_056506.3:p.Pro33AlafsTer47` with `Consequence=coding_sequence_variant`, `HGVSc=NM_015691.5:c.96-1dup`, and empty `cDNA_position` / `CDS_position` / `Protein_position`. vepyr reproduces all of that except `HGVSp`, which is empty.

- [ ] **Step 1: Pin the expected behaviour in an ignored test**

```rust
    /// Defect D: an insertion inside a 2 bp frameshift intron. VEP produces
    /// HGVSp from the coding path even though CDS and protein positions are
    /// empty; vepyr leaves HGVSp blank.
    /// Reproduces chrX:10015674 G>GC / NM_015691.5, expecting
    /// NP_056506.3:p.Pro33AlafsTer47.
    ///
    /// Ignored until Task 7 — see "Task 6 findings" in
    /// docs/superpowers/plans/2026-08-28-chrxy-core-annotation-parity.md
    #[test]
    #[ignore = "defect D: diagnosis pending, see plan Task 6"]
    fn insertion_in_a_frameshift_intron_still_yields_protein_hgvs() {
        unimplemented!("filled in by Task 7 once the trace identifies the drop site");
    }
```

- [ ] **Step 2: Confirm it compiles and is skipped**

```bash
cargo test -p datafusion-bio-function-vep --lib -- frameshift_intron_still_yields 2>&1 | tail -5
```

Expected: `1 ignored`.

- [ ] **Step 3: Trace where the protein HGVS is dropped**

Read these four sites in `transcript_consequence.rs` and record what each does for a variant whose `in_frameshift_intron` is true and `coding_class` is `None`:

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions/datafusion/bio-function-vep
sed -n '1838,1892p' src/transcript_consequence.rs   # coding branch + the frameshift-intron term strip at :1878-1890
sed -n '1402,1417p' src/transcript_consequence.rs   # the coding_class.is_none() else-arm that still calls protein_hgvs_for_output_with_semantics
grep -n "fn protein_hgvs_for_output_with_semantics" -A 60 src/transcript_consequence.rs | head -70
grep -n "fn original_terms_allow_protein_hgvs" -A 25 src/transcript_consequence.rs
```

The `else` arm at `:1402-1417` already calls `protein_hgvs_for_output_with_semantics` with `None` positions specifically to cover "VEP can still emit HGVSp for HGVS-shifted indels whose original consequence stayed `coding_sequence_variant`" — which is exactly this shape. So the likely drop is one of: (a) the variant never enters the coding branch at all, so `coding_class` is `None` *and* the else-arm is not reached; (b) `original_terms_allow_protein_hgvs` returns false for this term set; or (c) `protein_hgvs_for_output_with_semantics` returns `None` for a frameshift-intron insertion.

- [ ] **Step 4: Determine which of (a)/(b)/(c) it is, empirically**

Write a throwaway test reproducing the exact geometry, then run it under `cargo test -- --nocapture` with `dbg!` on `in_frameshift_intron`, `coding_class.is_some()`, `original_allows_protein_hgvs`, and the `protein_hgvs` result. Delete the throwaway test afterwards; it is a probe, not a deliverable.

The transcript and exon bounds are already read from the 116 GRCh38 merged cache — use them directly rather than re-querying:

```rust
        let transcripts = vec![tx(
            "NM_015691.5",
            "X",
            10_015_254,
            10_144_474,
            1,
            "protein_coding",
            Some(10_015_579),
            Some(10_141_501),
        )];
        let exons = vec![
            exon("NM_015691.5", 1, 10_015_254, 10_015_673),
            exon("NM_015691.5", 2, 10_015_676, 10_015_858),
            exon("NM_015691.5", 3, 10_063_445, 10_063_554),
        ];
        let v = var("X", 10_015_675, 10_015_674, "-", "C");
```

Note `tx()` takes no translation; a `TranslationFeature` is needed for `HGVSp` to be produced at all, so build one for `NM_015691.5` (protein `NP_056506.3`) and pass it in the `translations` slice. Check how the existing coding tests in the module construct one before writing your own.

To re-read any other transcript field from the cache, note the column is `stable_id`, **not** `transcript_id` (that name only exists in the `exon` table):

```bash
cd /Users/mwiewior/research/git/vepyr
uv run python -c "
import polars as pl
df=pl.read_parquet('/Users/mwiewior/workspace/data_vepyr/cache/116_GRCh38_merged/transcript/chrX.parquet')
print(df.filter(pl.col('stable_id')=='NM_015691.5').select('start','end','strand','cds_start','cds_end','biotype'))
"
```

- [ ] **Step 5: Write the diagnosis into this plan file**

Append a `## Task 6 findings` section to `docs/superpowers/plans/2026-08-28-chrxy-core-annotation-parity.md` recording: which of (a)/(b)/(c) holds, the exact function and line where the protein HGVS is dropped, the Ensembl code path that produces it instead, and a proposed one-paragraph fix. Commit the plan update.

```bash
cd /Users/mwiewior/research/git/vepyr
git add docs/superpowers/plans/2026-08-28-chrxy-core-annotation-parity.md
git commit -m "docs: diagnosis for defect D (HGVSp in a frameshift intron)"
```

- [ ] **Step 6: STOP and report**

Do not write Task 7. Return the diagnosis to the planner so the fix can be planned against evidence rather than guessed. Tasks 1–5 cover 363 of 364 rows and can be verified independently — run Task 8 first if useful.

---

### Task 7: Fix defect D

**Planned after Task 6 reports.** Its steps are written from the diagnosis, not before it. Attempting this task without Task 6's findings means guessing at shared HGVS code that 4,096,123 autosome records depend on.

---

### Task 8: Full verification

**Files:**
- Modify: `Cargo.toml` (vepyr) — the `datafusion-bio-function-vep` `rev`
- No engine source changes.

**Interfaces:**
- Consumes: the merged engine branch from Tasks 1–5 (and 7 if complete).
- Produces: the parity evidence that closes the work.

- [ ] **Step 1: Push the engine branch and record the commit**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -20   # expect no warnings
cargo fmt --check
git push -u origin fix/chrxy-core-annotation-parity
git rev-parse HEAD    # record this full SHA
```

- [ ] **Step 2: Point vepyr at it**

In `/Users/mwiewior/research/git/vepyr/Cargo.toml`, replace the `rev` on the `datafusion-bio-function-vep` line with the SHA from Step 1, then rebuild:

```bash
cd /Users/mwiewior/research/git/vepyr
uv run maturin develop 2>&1 | tail -5
```

Expected: build succeeds. Do not pipe the build through `tail` if it fails — rerun unfiltered; truncating build logs hid the real diagnostics twice during the investigation.

- [ ] **Step 3: Verify the three big deletions before running anything full-scale**

```bash
G=/Users/mwiewior/workspace/data_vepyr/debug/chrXY-core-annotation-2026-08-28
bcftools view -H $G/reference/HG002_chrX_5plugins_vep116_caddfix.vcf.gz \
  | awk -F'\t' '$2==3886710' | head -1 | cut -c1-120
```

Build a 3-variant VCF from `$G/input/HG002_chrXY_norm_acgt.vcf.gz` containing `chrX:3886710`, `chrY:6246324`, `chrX:104069779`, annotate it, and diff the CSQ blocks per transcript against `$G/reference/`. Expected: `Consequence=transcript_ablation` on the fully covered transcripts, `EXON`/`INTRON` as ranges (`1-2/2`, `1-3/3`, `1-4/4`, `1-4/13`, `1-4/12`), and `IMPACT=HIGH` unchanged.

- [ ] **Step 4: chrX and chrY semantic comparison**

```bash
cd /Users/mwiewior/research/git/vepyr
DATA=/Users/mwiewior/workspace/data_vepyr
for C in X Y; do
  uv run python scripts/run_comparison.py \
    --release 116 --profile merged_plugins --chroms $C \
    --vcf   $DATA/input/HG002_chrXY_norm_acgt.vcf.gz \
    --vep   $DATA/output/116/plugins/HG002_chr${C}_5plugins_vep116_caddfix.vcf.gz \
    --plugin-cache $DATA/plugin_cache --workers 4 --bgzf --force
done
```

Expected: 0 core-field mismatches and 0 plugin-field mismatches on both, i.e. 124/124 fields at 100%. Both `--vcf` and `--vep` are required — the profile otherwise resolves the autosome benchmark, and plugin references are per-contig.

If Task 7 is not yet done, expect exactly **1** residual mismatch (`HGVSp` at `chrX:10015674` / `NM_015691.5`) and nothing else. Any other residue means a fix in Tasks 1–5 is incomplete.

- [ ] **Step 5: chrX and chrY strict body MD5**

```bash
uv run python scripts/md5_concordance.py \
  --pair <vep slice>.vcf <vepyr>.vcf.gz --mode strict --explain --explain-limit 0
```

Expected: `body=ok` for both. This cannot pass until Task 7 lands.

- [ ] **Step 6: The chr1–22 regression gate**

```bash
cd /Users/mwiewior/research/git/vepyr
VEP_COMPARISON_WORKERS=4 ./scripts/run_all_plugin_comparisons.sh
```

Expected, unchanged from the pre-existing baseline: **22/22 `body=ok`, 4,096,123 records, 69,299,753 CSQ entries, zero mismatches in every category.**

This run is what adjudicates the tier gate's miRNA interaction. If it regresses on rows involving `mature_miRNA_variant`, the resolution is fixed in advance: remove the now-redundant hand-coded suppressions at `transcript_consequence.rs:1820-1832` and `:1965-1971`, not narrow the gate. Any other regression class needs its own diagnosis before proceeding.

This host is shared — check `uptime` before trusting any timing, though this gate is judged on correctness, not wall time.

- [ ] **Step 7: Commit the pin bump**

```bash
cd /Users/mwiewior/research/git/vepyr
git add Cargo.toml Cargo.lock
git commit -m "chore: pin bio-function-vep with the chrX/chrY parity fixes

chrX and chrY reach 0 semantic mismatches and pass strict body MD5.
chr1-22 unchanged: 22/22 body=ok, 4,096,123 records, 69,299,753 CSQ entries."
```

---

## Out of scope

Recorded so they are not silently dropped:

- **The 33,786 excluded chrXY records** (13.2%): 24,327 with non-ACGT alleles — VEP aborts the whole file with `Can't detect input format` because the first record is `chrX 222582 . Y C` — and 9,459 with `*` spanning-deletion ALTs. Both tools see the identical filtered input, so the comparison is sound over what it covers, but chrX/Y parity must not be quoted as whole-benchmark coverage.
- **Ensembl's shifted-coordinate clauses in `_intron_effects`.** Task 5 implements the unshifted pair only; see its limitation note.
- **An ablated transcript that produces no other term** emits no CSQ entry, because the gate sits inside the `if !terms.is_empty()` guard. The case does not arise for a deletion covering a whole transcript.
- **Porting Ensembl's full 31-predicate table.** Analysed and rejected in the spec's "Scope and non-goals"; recorded there as a future direction.

---

## Task 6 findings (2026-08-28)

**Candidate (b) is eliminated.** `original_terms_allow_protein_hgvs` includes
`SoTerm::CodingSequenceVariant`, which is exactly the term this transcript carries, so the
`!original_allows_protein_hgvs` early return is not the drop site.

**Candidate (a) is eliminated.** vepyr already reproduces VEP's `Consequence`
(`coding_sequence_variant`), `HGVSc` (`NM_015691.5:c.96-1dup`) and the empty
`cDNA_position`/`CDS_position`/`Protein_position` for this entry. It therefore does enter the
coding branch; `classify_coding_change` returns `None` (mapper gap through the frameshift
intron), so `coding_class` is `None` and control reaches the `else` arm at
`transcript_consequence.rs:1402-1417`, which calls
`protein_hgvs_for_output_with_semantics(..., original_allows_protein_hgvs, None, None, ...)`.

**The drop is candidate (c), inside `protein_hgvs_for_output_with_semantics`, with `fallback = None`.**
That function is reached, and it does *not* require a fallback to synthesise protein HGVS —
`shifted_tva_protein_hgvs_data` builds peptides from the shifted mapper coordinates and the
translation alone. Walking the guards with `fallback = None`:

| guard | outcome for this variant |
|---|---|
| `!original_allows_protein_hgvs` | passes (`CodingSequenceVariant` allows) |
| `!shift_hgvs` | passes (shifting is on) |
| `let Some(shift) = shift else { return fallback.cloned() }` | **needs checking** — returns `None` if no shift |
| `if shift.shift_length == 0 { return fallback.map(...) }` | **needs checking** — returns `None` if zero |
| `if ref_norm.len() == alt_norm.len()` | passes (0 vs 1, an insertion) |
| `shifted_tva_protein_hgvs_data(...)` | can return `Some`; returns `None` if the mapper fails |
| `shifted_tva_coords_from_mapper(...)?` under `shift.shift_length > 0` | **`?` propagates `None` out of the whole function** |

The reference FASTA settles the shift: genomic 10015674=G, 10015675=C, 10015676=G. Inserting `C`
at 10015675 duplicates the base already there, so it 3'-shifts one position to 10015676 and stops
(10015676 is `G`). **`shift` is therefore `Some` with `shift_length == 1`**, which rules out both
early returns.

That leaves two candidate lines, both of which resolve genomic coordinates through a mapper whose
CDS view has to span the 2 bp frameshift intron:

1. `shifted_tva_coords_from_mapper(tx, tx_exons, translation, &shifted_variant)?` — the guard
   under `if shift.shift_length > 0`. Note it maps `variant.parser_start`/`parser_end`, not
   `variant.start`/`end`.
2. `shifted_tva_protein_hgvs_data(...)` returning `None` from its own
   `shifted_tva_coords_from_mapper(...)?`.

The shifted position 10015676 is the first base of exon 2 (`c.97`) and is inside the CDS, so a
correct mapper should resolve it. The likely cause is the mapper's treatment of the frameshift
intron, or the `parser_start` vs `start` coordinate basis.

**Proposed next step, and why it is not a synthetic unit test.** A synthetic fixture cannot
faithfully exercise this path — it needs the real `cdna_seq`, `translateable_seq` and
`cdna_mapper_segments` for `NM_015691.5`, and a hand-built approximation risks a false negative
that would send Task 7 after the wrong mapper. Task 8 runs the real chrX comparison against the
real cache anyway, so the cheap decisive move is to instrument these two call sites during that
run and observe which returns `None` for `chrX:10015674 / NM_015691.5`. **Task 7 should be planned
from that observation.**

No `#[ignore]`d stub test was added: with the drop site still ambiguous between two lines, the
only test that could be written would assert `hgvsp.is_some()` against a fixture whose fidelity is
the very thing in question.

---

## Task 8 partial results — chrX/chrY verification (2026-08-28)

Run against the worktree build (engine `bd04392`, Tasks 1–5) via a temporary path dependency in
`Cargo.toml`; nothing pushed.

| | chrX | chrY |
|---|---:|---:|
| variants compared | 157,690 | 63,736 |
| variants on one side only | 0 | 0 |
| CSQ entry count / order mismatches | 0 / 0 | 0 / 0 |
| **value mismatches** (`field_mismatch_counts`) | **1** (was 148) | **0** (was 216) |
| order mismatches (`field_order_mismatch_counts`) | 4 (`DOMAINS`) | 0 |
| fields at 100% | 123/124 | **124/124** |
| strict body MD5 | FAIL — 5 of 157,690 records differ | **PASS — `4ed4691a20af77c3e0bb938762237491`** |

Tasks 1–5 removed exactly the predicted 147 value mismatches on chrX (34 `Consequence` + 59 `EXON`
+ 54 `INTRON`) and all 216 on chrY. **chrY is byte-identical to Ensembl VEP 116.**

### Defect E (new): `DOMAINS` ordering — 4 records, pre-existing

The 5 differing chrX records are **not** all defect D:

- 1 record — `chrX:10015674 / NM_015691.5`, the known defect D `HGVSp`.
- 4 records — `chrX:7077274`, `7077376`, `7077397`, `7105637`, all on `ENST00000381077`, all a
  `DOMAINS` **ordering** difference with an identical member set. VEP emits the two `SFLD:` entries
  first; vepyr emits them between `PANTHER:` and `Superfamily:`. Everything else is in the same
  order.

**This is pre-existing, not a regression.** The pre-change report in the investigation directory
carries the identical `field_order_mismatch_counts: {'DOMAINS': 4}`.

**Why the handover's 364-row inventory missed it.** The comparator sorts multi-valued fields before
comparing, so an order-only difference lands in `field_order_mismatch_counts`, not
`field_mismatch_counts`. The console summary and the "Total mismatches" line count only the latter
— so the run prints "Fields at 100%: 123/124, Total mismatches: 1" while five records differ on
disk. Reading only the summary hides this class entirely.

**Consequence for the definition of done:** chrX byte-identity needs defect E fixed as well as
defect D. Neither the spec nor Tasks 1–7 cover it. It needs its own diagnosis of how the engine
orders `DOMAINS` sources against VEP's `ProteinFunctionPredictions`/domain assembly order.

---

## Defect E diagnosis (2026-08-28): vepyr is correct; the stored chrX reference is the outlier

**Conclusion: this is not a vepyr defect, and it should not be "fixed" in the engine.**

### What the code does

Nothing on either side sorts `DOMAINS`:

- VEP `OutputFactory.pm:1449-1466` iterates `$tv->get_overlapping_ProteinFeatures` and pushes
  `analysis->display_label:hseqname` in encounter order.
- `BaseTranscriptVariation::get_overlapping_ProteinFeatures` (`:618-648`) filters
  `$tr->{_variation_effect_feature_cache}->{protein_features}` by overlap, preserving order.
- vepyr `annotate_provider.rs:7784` `lookup_domains` iterates `tl.protein_features` and joins with
  `&`, preserving order.
- The cache readers (`bio-format-ensembl-cache/src/translation.rs:605`, `:641`) read
  `vef_cache["protein_features"]` as an ordered array. No sort in the write path.

### What the data says

The Ensembl cache's own serialized array, read back from `raw_object_json` in
`cache/116_GRCh38_merged/transcript/chrX.parquet` for `ENST00000381077`:

```
 0 Gene3D 1.10.150.240      4 PDB-ENSP mappings 9m7m.A    8 SFLD SFLDG01135
 1 Gene3D 3.40.50.1000      5 AFDB-ENSP mappings AF-...   9 SFLD SFLDS00003
 2 PDB-ENSP mappings 3l5k.A 6 Pfam PF13419              10 Superfamily SSF56784
 3 PDB-ENSP mappings 9m7l.A 7 PANTHER PTHR18901         11 NCBIFAM TIGR01509
                                                        12 CDD cd07529
```

**SFLD sits at 8-9 — exactly what vepyr emits.** The stored reference emits it at 0-1.

### Five fresh VEP 116.0 runs all agree with vepyr

| run | invocation | SFLD position |
|---|---|---|
| 1, 2, 3 | `--cache --merged --everything`, 4 variants, separate processes | 8-9 |
| 4 | identical to the reference build: same image, `--dir_plugins`, `--custom` ClinVar, SpliceAI + CADD + AlphaMissense + dbNSFP | 8-9 |
| 5 | 423 real input variants spanning `chrX:7,000,000-8,000,000` | 8-9 |
| — | **stored `HG002_chrX_5plugins_vep116_caddfix.vcf.gz`** | **0-1** |

So vepyr agrees with the cache *and* with reproducible VEP 116.0 behaviour on the same image with
the same flags. The three plain runs also rule out Perl hash-order nondeterminism as an active
factor here: the order was stable across separate processes.

### What is not established

Why the stored reference differs. The only VEP code path that produces exactly this signature —
one whole analysis group relocated, every other group in identical relative order — is
`AnnotationType/Transcript.pm:583-593`, which on a BAM-edited transcript deletes
`_variation_effect_feature_cache->{protein_features}`, forcing regeneration through
`Translation::get_all_ProteinFeatures` (`ensembl/modules/Bio/EnsEMBL/Translation.pm:816-820`),
whose `foreach my $type (keys %{$self->{'protein_features'}})` flattens a hash of analysis groups
in Perl key order. Both runs log `BAM-edited cache detected`, and `ENST00000381077` has
`bam_edit_status = None` in the cache, so the trigger was not reproduced at these input sizes.

### Recommended action

1. **Do not change the engine.** Any change would move vepyr away from both the cache and
   reproducible VEP output.
2. Re-generate the chrX reference and re-compare. If the regenerated reference puts SFLD at 8-9,
   chrX reaches 0 order mismatches and the only remaining gap is defect D.
3. If a regenerated reference still differs, the DOMAINS order for such transcripts is not
   reproducible from the cache and should be recorded as an accepted difference, not chased.
4. Independently: the comparison summary should surface `field_order_mismatch_counts`. A run that
   prints "Fields at 100%: 123/124, Total mismatches: 1" while five records differ on disk is
   actively misleading, and is why this class went unnoticed in the original handover.

---

## Defect E — CORRECTION (2026-08-28)

**The previous section's conclusion was wrong.** The chrX reference was regenerated with the same
script, image and flags, and is **byte-identical** to the original:

```
original     body MD5  2ec98ed8efc86e3f42a996117f7009cc
regenerated  body MD5  2ec98ed8efc86e3f42a996117f7009cc
```

`ENST00000381077` still carries `SFLD:SFLDG01135&SFLD:SFLDS00003&Gene3D:...` — SFLD first.

So VEP is **deterministic at full-chromosome scale**, and the stored reference is not stale, not
anomalous, and not an artifact. The recommendation to regenerate it was based on a bad inference.

### What was actually right and wrong

Still correct:

- Neither VEP nor vepyr sorts `DOMAINS`; both preserve the order of the list they are given.
- The Ensembl cache's serialized `protein_features` array has SFLD at 8-9.
- vepyr faithfully reproduces that cache order.
- Five small-scale VEP runs (4 variants; 423 variants over 1 Mb; with and without the plugin set)
  all emit SFLD at 8-9.

Wrong:

- Concluding from those small runs that the reference deviated from "real" VEP behaviour. The
  discriminating variable is **input scale**, not the reference artifact. Small-scale runs are
  simply not representative for this field.

### Where that leaves defect E

It is a **genuine vepyr-side parity gap**, not a reference problem. At production scale VEP
reproducibly emits the analysis-grouped, SFLD-first order, and vepyr emits cache order.

The mechanism remains the `AnnotationType/Transcript.pm:583-593` deletion of
`_variation_effect_feature_cache->{protein_features}`, forcing regeneration through
`Translation::get_all_ProteinFeatures`, whose `foreach my $type (keys %hash)` flattens analysis
groups in Perl hash-key order. Both full-scale runs producing identical output means that hash
order is stable in the container, not randomized.

**This is expensive to match.** Reproducing it means emulating the group order Perl's hash
iteration yields for a given key set. Before any engine change, establish:

1. What minimal input reproduces the flip (bisect between 423 variants and 157,690). Without this,
   any fix is unverifiable at reasonable cost.
2. Whether the group order is a stable function of the analysis key set across transcripts, or
   per-transcript incidental. Sample every transcript in the chrX reference whose `DOMAINS` spans
   two or more analysis sources and compare group order against the cache array order.

Only 4 of 157,690 chrX records are affected, and 0 of 4,096,123 autosome records. Weigh the cost
of emulating Perl hash ordering against recording this as an accepted difference.

---

## Defect D — FIXED (2026-08-28), engine `f81f442`

**Root cause, traced not guessed.** Instrumenting `protein_hgvs_for_output_with_semantics` on the
real variant produced:

```
tx=NM_015691.5 allows=true shift_hgvs=true fallback=false bounds=None
existing=Some(1) refseq=None
shift_length=1
shifted_tva_protein_hgvs_data -> false
mapper guard (10015676,10015675) -> false
```

Both exits are the same call. `shifted_tva_coords_from_mapper` maps an insertion's two flanks and
required **both** via `?`. For `NM_015691.5` (exon 1 ends 10015673, exon 2 starts 10015676, so
intron 1 is 2 bp) the HGVS-shifted insertion sits at 10015676 — the first base of exon 2 — with its
upstream flank at 10015675, inside the intron. The intronic flank returned `None` and aborted the
whole lookup.

Ensembl does not do this. `Mapper::map_insert` maps the swapped 2 bp interval and then keeps only
the flanks that resolve to a `Coordinate`, silently dropping `Gap` flanks:

```perl
if(ref($m1) eq 'Bio::EnsEMBL::Mapper::Coordinate') { ... push @coords, $m1 }
if(ref($m2) eq 'Bio::EnsEMBL::Mapper::Coordinate') { ... push @coords, $m2 }
```

**Fix:** tolerate a dropped flank in both the cDNA and peptide windows of
`shifted_tva_coords_from_mapper`, following `map_insert`'s adjustments.

**Verified end-to-end:** vepyr now emits `NP_056506.3:p.Pro33AlafsTer47` with an empty
`Protein_position`, matching VEP. `ENST00000380861` at the same position correctly still has no
HGVSp — its shifted position stays intronic so neither flank maps, which is also what VEP does.
978 unit tests pass; the new test guards its own premise (asserts the upstream flank is genuinely
unmappable, so it cannot pass under both-flanks-required logic).

## Defect E — NOT FIXED, and should not be

Measured across the whole chrX reference, 2,411 distinct (transcript, DOMAINS) pairs:

| | count |
|---|---:|
| DOMAINS order matches cache order exactly | 1,015 |
| **pure reordering (same set, different order)** | **2, both `ENST00000381077`** |
| set differs (protein-position filtering, not ordering) | 1,394 |

**There is no systematic ordering rule.** 1,015 multi-source transcripts follow the cache's
`protein_features` order exactly; exactly one transcript does not. The earlier note suggesting a
fix would mean emulating Perl hash iteration order is superseded: no global hash-order effect
exists, or those 1,015 would not agree.

Nor is VEP's order for this transcript derivable. It is the cache order with the SFLD group hoisted
to the front, and is not sorted by start, end, analysis name, or hseqname.

**Recommendation: do not implement a fix.** Any change able to reproduce this one transcript would
be a rule invented to fit a single observation, with no upstream basis, and it would put the 1,015
currently-correct transcripts at risk. The residue is 4 of 157,690 chrX records and 0 of 4,096,123
autosome records.

If it must be closed, the only sound route is to determine why VEP regenerates this transcript's
`protein_features` at full-chromosome scale but not at 423-variant scale — the input-scale
threshold identified above — and reproduce that mechanism rather than its output.

---

## FINAL RESULTS (2026-08-28) — engine `f81f442`

### chr1-22 regression gate, re-run after the defect D fix

| metric | required | actual |
|---|---:|---:|
| chromosomes `body=ok` | 22/22 | **22/22** |
| records | 4,096,123 | **4,096,123** |
| CSQ entries | 69,299,753 | **69,299,753** |
| mismatches, every category | 0 | **0** |

Zero value mismatches, zero order mismatches, zero CSQ count/order mismatches, zero
variants-on-one-side-only, on every chromosome.

### chrX / chrY

| | before | after |
|---|---|---|
| chrX | 148 value + 4 order, strict DIFF | **0 value**, 4 order (defect E), strict FAIL on 4 records |
| chrY | 216 value, strict DIFF | **0 value, 0 order, strict PASS — byte-identical** |

**All 368 field-value mismatches across chrX and chrY are closed.** The only residue is 4 records
of `DOMAINS` ordering on a single transcript (defect E), deliberately not fixed — see above.

### Engine commits, all pushed to `fix/chrxy-core-annotation-parity`

| commit | defect | rows |
|---|---|---:|
| `76a6008` | A — EXON range | 324 combined |
| `25313d8` | A — INTRON range | |
| `40e662c` | `SoTerm::tier()` | — |
| `cc92486` | B — transcript_ablation via the generic tier gate | 37 |
| `bd04392` | C — intron_variant at a 2 bp intron | 2 |
| `f81f442` | D — HGVSp across a dropped insertion flank | 1 |

978 unit tests, clippy clean, `cargo fmt` clean. vepyr pins `f81f442`; the two later engine
commits only add and remove a stray doc and leave the code identical.

---

## Defect E — three hypotheses falsified, cause still unknown (2026-08-29)

Investigation carved out to `data_vepyr/debug/domains-ordering-2026-08-29/HANDOVER.md`, which is
the live document. Summary of what changed:

**New hard facts.** VEP's own cache blob was dumped directly
(`homo_sapiens_merged/116_GRCh38/X/7000001-8000000.gz`):
`_variation_effect_feature_cache->{protein_features}` is an `ARRAY` of 13 with SFLD at indices 8-9
— cache order, exactly what vepyr emits — and there is exactly one copy of the transcript in that
region file.

**The hash-flatten hypothesis is dead.** The translation object's `protein_features` is `UNDEF` in
the blob, so offline `get_all_ProteinFeatures` would hit
`return [] if (!$adaptor || !$dbID)` and return **empty**. Deleting the cached array would make
DOMAINS vanish, not reorder. It was also never consistent with only one analysis group moving.

**Input format is not the variable.** A real `bcftools` slice — 242 variants with the original
header and sample column — still yields cache order. This also exposed a flaw in the earlier
probes: all of them used a hand-written minimal VCF, conflating "small input" with "reformatted
input".

**Falsified so far:** stale reference; hash-flatten regeneration; input format; Perl hash-order
randomisation; plugins/`--custom`.

**Remaining contradiction:** the blob says 8-9, nothing in offline VEP writes to that array, small
runs emit 8-9, full-chromosome runs reproducibly emit SFLD first.

**In flight:** full chrX, real input, no plugins — splits "scale alone" from "scale + plugins".
It only needs to write past `chrX:7,105,637` to answer; see §7 of the handover.

The recommendation is unchanged: do not implement a fix. 1,015 multi-source transcripts follow
cache order and one does not, so there is still no rule to implement.

---

## Defect E — ROOT CAUSE FOUND (2026-08-29). Supersedes the "do not fix" recommendation.

`ENST00000381077` is stored in **two** Ensembl VEP cache region files with **different
`protein_features` array orders**:

| region file | order |
|---|---|
| `X/6000001-7000000.gz` | **SFLD first** |
| `X/7000001-8000000.gz` | SFLD at 8-9 |

VEP de-duplicates transcripts by stable_id across regions and keeps the **first copy loaded**. Any
input spanning 6-7 Mb gives VEP the SFLD-first copy, which it then uses for the variants at
7.07-7.11 Mb. vepyr's cache keeps the 7-8 Mb copy, so it emits SFLD at 8-9.

Bisect:

| input | variants | regions | SFLD first? |
|---|---:|---:|---|
| `chrX:7000001-8000000` | 423 | 1 | no |
| **`chrX:6000001-8000000`** | **1,162** | **2** | **YES** |
| `chrX:1-8000000` | 15,768 | 8 | yes |
| full chrX, no plugins | 157,690 | ~156 | yes |

This explains everything previously unexplained: the scale dependence (large inputs span 6-7 Mb),
why plugins and `--buffer_size` were irrelevant, and why exactly one transcript is affected.

**The fix belongs in the cache builder, not the consequence engine:** when a transcript appears in
several region files, keep the copy from the lowest-coordinate region, matching VEP's
first-seen-wins de-duplication.

**This supersedes the earlier "no rule exists, do not fix" recommendation** — that conclusion was
drawn before the mechanism was known. Scope must be measured first: count transcripts appearing in
multiple region files with differing `protein_features` order genome-wide, and keep chr1-22 at
22/22 `body=ok`.

Live document: `data_vepyr/debug/domains-ordering-2026-08-29/HANDOVER.md`.
Cheap reproducer: `chrX:6000001-8000000`, 1,162 variants, no plugins, ~1 minute.

---

## Defect E — regression risk of the fix, measured (2026-08-29)

| | chr21 | chrX |
|---|---:|---:|
| transcripts in >1 region file | 997 | 2,973 |
| copies differing at all | 13 | 46 |
| differ **only** in `protein_features`/`translation` | 1 | 1 |
| differ in coordinates/`dbID`/exons/introns/mapper | 12 | 45 |

Two distinct kinds hide behind "duplicate":

1. **stable_id collisions** — all `compmerge.*.chrNN`, the same id at different loci with different
   `dbID` and coordinates, no protein features. chr1-22 is byte-identical today, so vepyr already
   picks the copy VEP picks. **A blanket de-dup change could break these 12.**
2. **same transcript, different `protein_features` order** — identical `dbID` and coordinates.
   One per chromosome: `ENST00000381077` (chrX), `ENST00000348990` (chr21). This is defect E.

The autosomes are clean because the chr21 case is unexercised: `ENST00000348990` yields 605 CSQ
entries in the reference and **0** with a non-empty `DOMAINS`.

**So a rule scoped to same-identity duplicates only is low risk** — it cannot touch the collisions,
leaves chr21's output unchanged, and should make chrX byte-identical.

**The real cost is the cache rebuild.** The fix changes cache content, so every cache must be
rebuilt, including those published on Hugging Face; an old cache with a new engine still produces
the old ordering. That is a data-versioning decision.

Prerequisites before implementing: read what vepyr's builder does today (its "keeps the later copy"
behaviour is inferred, not read); scan the remaining chromosomes; then scope the rule and re-run
chrX + chr1-22. Detail and scripts in `data_vepyr/debug/domains-ordering-2026-08-29/`.

---

## Defect E — mechanism confirmed in the VEP source (2026-08-29)

Verified against `ensembl-vep` @ `2beada0d`, with two corrections to the earlier write-up.

**The chain.** `AnnotationSource.pm:109-145` builds the feature list as *already-cached regions
first (in region order), then newly loaded regions*, and hands it to `merge_features`. Region
indices are walked ascending (`:193-200`) over the buffer's variants in input order (`:243-246`).
`AnnotationType/Transcript.pm:264-277` then keeps the **first** occurrence:

```perl
my $dbID = $tr->dbID;
if($seen_trs{$dbID}) { next; }
$seen_trs{$dbID} = 1;
push @return, $tr;
```

**Correction 1 — the key is `dbID`, not `stable_id`.** `ENST00000381077`'s two copies share a
`dbID` (only `translation` and `protein_features` differ), so they are merged, first-wins, and
region order decides. Mechanism unchanged; the key was misnamed.

**Correction 2 — merged/refseq caches run a second stage that is region-order independent**
(`:293-322`): per `stable_id`, prefer `source eq 'ensembl'`, else the **lowest `dbID`**.

The second correction **relaxes the risk assessment**. The `compmerge.*` stable_id collisions have
differing `dbID`s, so stage 1 never merges them and stage 2 resolves them deterministically by
source/lowest-dbID — independent of region order and input scale. A fix scoped to same-`dbID`
duplicates therefore **cannot** affect those 12/45 cases, so the earlier "a blanket change could
break all 12" warning was too pessimistic.

**The contract vepyr's builder should implement**, now quoted rather than inferred:

1. duplicates sharing a `dbID` -> keep the copy from the earliest region;
2. stable_id collisions with differing `dbID`s -> prefer `source == 'ensembl'`, else lowest `dbID`.

The cache-rebuild cost from the previous section still stands unchanged.
