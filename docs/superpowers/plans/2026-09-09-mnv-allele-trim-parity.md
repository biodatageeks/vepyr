# MNV allele trimming parity (vepyr#95)

**Status:** awaiting human go-ahead at runbook step 1d. No file in any of the three
checkouts has been edited. This artifact is the gate.

**Goal:** make vepyr's CSQ `Allele` and internal variant coordinates match Ensembl VEP
116.0 for same-length multi-base substitutions (MNVs), without disturbing the indel path.

**Architecture:** the whole fix lands in `datafusion-bio-functions`. `datafusion-bio-formats`
is untouched; vepyr takes a pin bump plus a regression test.

**Spec:** `biodatageeks/vepyr#95`, corrected by the analysis below — the issue's proposed
rule is wrong for indels and must not be implemented as written.

---

## 1. The defect

For a REF/ALT pair of equal length longer than one base, vepyr strips the shared leading
bases and advances the variant start by that many positions. Ensembl VEP leaves such a pair
completely untouched. `GAC>GTC` at POS 100 is reported by VEP as `Allele=GTC` spanning
100-102 and by vepyr as `Allele=TC` spanning 101-102. Because the shift is internal, the
printed VCF `POS` is unaffected — the divergence surfaces in `Codons`, `cDNA_position`,
`CDS_position`, `HGVSc`, `DISTANCE` and, where the span change flips a term,
`most_severe_consequence`.

**Triggering record shape:** `len(REF) == len(ALT) > 1` **and** REF and ALT share at least
one leading base. A same-length pair sharing no leading base (`GCCTT>TCCTT`) is already
correct, which is why the whole-genome gate is green.

### Measured, not assumed

Real Ensembl VEP 116.0 (docker `ensemblorg/ensembl-vep:release_116.0`, `--everything --hgvs`,
offline cache) against vepyr at the current master pin, same input, REF bases taken from the
real GRCh38 FASTA:

| case | shape | VEP 116.0 `Allele` | vepyr `Allele` | verdict |
|---|---|---|---|---|
| `GCC>GTC` | MNV, prefix 1 | `GTC` | `TC` | **diverges** |
| `CCTGG>CTTGG` | MNV, prefix 1, len 5 | `CTTGG` | `TTGG` | **diverges** |
| `TCCC>TC` | indel, prefix 2, suffix 1 | `-` | `-` | match |
| `TTCA>TA` | indel, prefix 1, suffix 1 | `-` | `-` | match |
| `A>G` | SNV control | `G` | `G` | match |

Blast radius measured on a coding MNV, `chr22:20086483 ATA>AGA`, 25 CSQ entries, against an
SNV control at the same locus that matched perfectly:

| field | VEP | vepyr |
|---|---|---|
| `Allele` | `AGA` | `GA` |
| `Codons` | `ATA/AGA` | `aTA/aGA` |
| `CDS_position` | `520-522` | `521-522` |
| `cDNA_position` | `1156-1158` | `1157-1158` |
| `HGVSc` | `c.521T>G` | `c.521-522T>G` |
| `DISTANCE` | `341` / `650` / `4899` | `342` / `651` / `4900` |

`Protein_position`, `Amino_acids`, `VARIANT_CLASS`, `Consequence` and the CSQ entry count were
identical. The `DISTANCE` off-by-one on the non-coding probes is independent confirmation that
the coordinate shift is real and not only an `Allele` string difference.

## 2. The issue's proposed fix is wrong for indels — do not implement it as written

Issue #95 models VEP's default rule from `Parser/VCF.pm:295-336` alone: "one leading base,
only for indels, never a suffix". That is only half of VEP's default path. At **`release/116.0`**,
`Parser.pm:844` `post_process_vfs()` runs immediately after, and line `:881`

```perl
$vf = ${$self->minimise_alleles([$vf])}[0] if $is_non_minimised_indel;
```

is **not** gated on `--minimal` (that gate is the line above it, `:852`).
`$is_non_minimised_indel` is initialised at `:863` and set at `:871-880` by a length test on the
original untrimmed VCF REF/ALT (`nontrimmed_allele_string`, `Parser/VCF.pm:343`). So VEP's
composed default rule is:

- **same length** → no trim at all, no coordinate shift;
- **different length** → anchor chop, then a full `trim_sequences` (full prefix **and** full
  suffix, `ensembl-variation` `release/116` `Utils/Sequence.pm:965`).

vepyr already implements the second line correctly. The VEP run above confirms it from the
outside: `TTCA>TA` yields `Allele=-` in VEP, not the `TTG/G`-shaped one-base chop the issue
predicts. Two consequences for the plan:

1. **Issue item 3 is answered: there is no shared-suffix defect.** The suffix loop stays.
2. **Issue items 1 and 3, implemented verbatim, would regress every shared-suffix indel** and
   re-open what engine PR #110 (`7d4ee58`) fixed against a real chr14 `DISTANCE` mismatch. The
   issue's own proposed assertion `vcf_to_vep_allele("AT","GTT") == ("AT","GTT")` is wrong;
   VEP gives `A/GT`, which is what vepyr produces today.

The fix is therefore **one condition — skip the trim when `ref.len() == alt.len()`** — applied
at the sites that must not drift apart, not the rewrite the issue describes.

## 3. Ownership

| repo | verdict | evidence |
|---|---|---|
| `datafusion-bio-formats` | **untouched** | no trimming, minimisation or coordinate logic anywhere in `bio-format-vcf/src/`; `physical_exec.rs:989-990` writes REF verbatim and ALT pipe-joined; `start` is the raw 1-based POS given vepyr's `zero_based=false` (`vepyr/src/annotate.rs:489-494`); `end` from `get_variant_end` (`:815-836`) already equals VEP's `Parser/VCF.pm:254`. Verified byte-identical between pinned rev `419be98` and the drifted `feat/cooler` branch for every file quoted. |
| `datafusion-bio-functions` | **owner** | every trim, every `is_indel` predicate and all three coordinate shifts live here |
| `vepyr` | **carrier** | no allele or coordinate code at all; stake is the pin at `Cargo.toml:88` and two blind gates |

A decisive argument against ever fixing this in formats: `bio-format-vcf/src/serializer.rs:607-671`
reconstructs the output VCF's POS/REF/ALT from those same columns, so trimming in the reader
would rewrite the user's own records on the `output_vcf=` path.

## 4. The change, per repo, in dependency order

### 4a. `datafusion-bio-formats` — no change

No pin bump on its own account. vepyr keeps both `datafusion-bio-format-*` lines at
`tag = "v1.12.1"`; the comment at `vepyr/Cargo.toml:84-87` requires the reference form stay a
tag or the formats crates compile twice (E0308).

### 4b. `datafusion-bio-functions` — the whole fix, four sites

Pinned rev `8b1abd0` (= tag `v0.20.1`, = `origin/master`).

| # | site | change |
|---|---|---|
| 1 | `allele.rs:296-342` `vcf_to_vep_allele` | return the pair untouched when `ref_allele.len() == alt_allele.len()`. Leave the indel suffix loop at `:315-325` exactly as it is. |
| 2 | `allele.rs:764-793` `vep_prefix_suffix_len` | the same guard, so `vep_norm_start` (`:812-815`) and `vep_norm_end` (`:826-829`) cannot drift from site 1 |
| 3 | `transcript_consequence.rs:57-58` | widen the bail-out from `(prefix_len == 0 && same length)` to `same length`; `:93`'s `new_start = pos + prefix_len` then never fires for a same-length pair |
| 4 | `allele.rs:353-382` `vcf_to_vep_input_allele` | `is_indel` at `:358` is `ref.len() != 1 \|\| alt.len() != 1` ("not an SNV") where VEP's is `length($alt) != length($ref)`. **See the open question in §7 — this site has an out-of-repo cost and may be split into its own PR.** |

Sites 1-3 must land in one commit: `Allele` comes from site 1, the colocated-match coordinates
from site 2 and the consequence engine's span from site 3. Fixing any subset silently
desynchronises the CSQ value from the coordinates it was computed at.

Falls out for free at site 1: a two-base MNV such as `GA>GT` currently trims to `A/T` and
classifies as `VARIANT_CLASS=SNV` (`annotate_provider.rs:7854-7862`); VEP calls it
`substitution`. The same guard fixes it.

### 4c. `vepyr` — pin bump plus a regression test

- `Cargo.toml:88` — `datafusion-bio-function-vep` tag → the functions PR head commit, then the
  merge commit after merge.
- One integration test in `tests/test_annotate.py` asserting `Allele` on an MNV, with an SNV
  positive control. **Constraint:** `tests/data/golden/cache/` is trimmed to chr1
  `start <= 852_922` and `tests/data/golden/reference.fa` holds 860,893 bases, so any new
  record must sit inside that window with a REF matching the FASTA.
- `docs/plugins.md:391-410` presents the trim as unconditional; add one sentence saying the
  parser rule is same-length-conditional.

## 5. The failing test, and why no existing gate can see this

**No existing fixture can see the defect.** Measured, not quoted from the issue:

- `tests/data/golden/input.vcf` — 100 records: 91 SNVs, 9 indels, **zero MNVs**, zero
  multi-allelic, zero `ALT=.`, zero shared-suffix indels. Every one of the 9 indels has shared
  prefix exactly 1 and shared suffix exactly 0, so both the current and the fixed rule produce
  identical output. All six golden suites share this one input.
- The golden gate is blinder still: `tests/_golden_suite.py:173-174` **excludes `Allele`** from
  the VCF field comparison, and it is absent from `DEFAULT_DF_COMPARISON_FIELDS` (`:126-152`).
  An MNV in the fixture would fail via `DISTANCE`/`Codons`/`cDNA_position`/`CDS_position`/`HGVSc`,
  not via `Allele`.
- The 22-autosome strict-md5 gate is **structurally** blind, not stale: `HG002_normalized.vcf.gz`
  contains 511 MNVs, but they come from multi-allelic splitting and every one shares no leading
  base, so the full-prefix trim is already a no-op on them. Its 29,404 shared-suffix indels are
  on the indel branch, which this fix does not touch.

Failing tests to write, in the owning repo:

- **Engine, `allele.rs`:** MNV untouched (`GAC>GTC`, `ATCG>AGCG`, and the 25-mer pair from the
  issue); indel path frozen — `AG>ATCG → -/TC`, `ATTG>AG → TT/-`, `T>AGTAAATTTTTTTTCT`,
  `AT>GTT → A/GT`. The frozen set is the guard against the issue's over-broad rule.
- **Engine, `transcript_consequence.rs`:** `from_vcf` for `ATCG/AGCG` must become
  `start 100, ATCG/AGCG` (today: `start 101, TCG/GCG`).
- **Engine, consistency:** site 1 and site 3 must agree for a fixed table of pairs.
- **vepyr:** the integration test in §4c.

Existing tests that **must be updated** because they pin the defect:
`allele.rs:1043-1049` (`test_complex_indel_common_prefix`, `ATCG/ATTT → CG/TT`),
`allele.rs:1163-1170` (`test_vcf_to_vep_allele_mnv_no_suffix_trim`, `ATCG/AGCG → TCG/GCG`),
`transcript_consequence.rs:13798-13807` (`from_vcf_mnv_prefix_only_no_suffix_trim`).

Existing tests that **must stay green untouched** — these are the regression guard:
`allele.rs:1196-1203` (`test_vcf_to_vep_allele_prefix_and_suffix_insertion`, the exact
`ATTG>AG` shape), `transcript_consequence.rs:13809`, `:13817`, `:13826`.

## 6. Pin cascade

| PR | pins |
|---|---|
| `datafusion-bio-formats` | **none — no PR** |
| `datafusion-bio-functions` | nothing (formats stays at `v1.12.1`) |
| `vepyr` | the functions PR head, re-pinned to the merge commit after merge |

Two PRs, not three. Branch name in both: `fix/mnv-allele-trim-parity`.

## 7. Decisions (confirmed by the human, 2026-09-09)

| # | decision |
|---|---|
| Q1 | **Isolate the plugin key.** Fix site 4's `is_indel` for VEP semantics, and route the plugin probe through a dedicated key function preserving today's anchor-shift, so no built plugin cache is invalidated and neither `cadd.source.toml` nor `clinvar.source.toml` changes. A test must prove the probe key for an MNV is byte-unchanged. |
| Q2 | **Standalone test *and* extend the golden** with an MNV plus a shared-suffix indel, via a scoped docker VEP 116.0 run. |
| Q3 | **Scope out the `minimal=` kwarg**, and **post the corrected rule on issue #95** so the wrong rule is not implemented later from the issue text alone. |
| Q4 | **Not in this PR.** The `Allele` exclusion at `tests/_golden_suite.py:173-174` is filed separately — changing it may turn other golden suites red for reasons unrelated to this fix. |

### The questions as originally posed

**Q1 — scope of site 4 (`vcf_to_vep_input_allele`).** Correcting its `is_indel` is required for
MNV `HGVSp` parity (the `parser_*` fields derive from it), but it changes the **runtime plugin
probe key** for MNVs while every already-built plugin shard still stores them anchor-shifted.
The published manifests hard-code today's predicate into their build SQL —
`vepyr-plugins/plugins/cadd/cadd.source.toml:47-48` and
`plugins/clinvar/clinvar.source.toml:109-110` — and ClinVar genuinely contains same-length
delins records, so this is not hypothetical. The existing fallback does not rescue it.
Three options: **(a)** land sites 1-3 now and file site 4 separately; **(b)** fix site 4 but
route the plugin probe through a dedicated key function preserving today's semantics, so no
cache is invalidated — *my recommendation*; **(c)** fix site 4, update both manifests and
rebuild/republish the CADD and ClinVar caches.

**Q2 — fixture route.** **(a)** a standalone fixture plus a vepyr integration test, cheap and
sufficient; **(b)** additionally splice an MNV and a shared-suffix indel into
`tests/data/golden/`, which needs a ~1-minute docker VEP 116.0 run (`prepare.py` cannot do it —
it extracts golden lines by exact `(CHROM,POS,REF,ALT)` key from a pre-existing WGS run, and
HG002 contains no prefix-sharing MNV). *Recommendation:* (b), so the gate stops being blind
permanently.

**Q3 — issue item 4, the `minimal=` kwarg.** Not needed for default-VEP parity: the run above
shows vepyr's indel output already equals VEP's non-minimal form. *Recommendation:* scope out
and say so on the issue.

**Q4 — `Allele` excluded from the golden comparison** (`tests/_golden_suite.py:173-174`). Fix
in this PR, or file separately? It is the reason a fixture alone would not have caught this.

## 8. Expected gate movement

**Quality: nothing moves.** All 9 golden indels and all 91 SNVs are unaffected by the guard;
all 511 corpus MNVs are prefix-0; the 29,404 shared-suffix indels stay on the unchanged branch.
Prediction: six golden suites green, `verify_parity_gate.py` exits 0, 22/22 strict-md5 body
digests still match. **This is also why a new fixture is mandatory — otherwise the fix ships
unverified.**

**Performance: no measurable change.** The edit adds one integer length comparison per row on a
path that already computes both lengths. Expect every phase, `annotation_seconds` and
`max_rss_kb` inside noise at both 1 and 8 workers; `compare_runs.py` must return VERDICT pass
with no phase moving more than a couple of percent. Any real movement means something else
changed and should be investigated rather than accepted.

## 9. Version provenance of the Perl analysis

The first pass read the Perl at `release/115.2`. Re-verified against the pinned target,
`ensembl-vep release/116.0`:

- `modules/Bio/EnsEMBL/VEP/Parser/VCF.pm` — **content-identical** to 115.2; the entire diff
  between the two tags is the copyright year. `:295-297` (`is_indel` on a length difference),
  `:325-336` (the one-base anchor chop) and `:343` (`nontrimmed_allele_string`) all hold.
- `modules/Bio/EnsEMBL/VEP/Parser.pm` — 48 lines differ between 115.2 and 116.0, but **none of
  them touch this logic**: a diff filtered to `minimis|is_non_minimised|nontrimmed` is empty.
  Only the line numbers moved, and the anchors above are the 116.0 ones.
- `ensembl-variation` `Utils/Sequence.pm` — `trim_sequences` is **unchanged** between
  `release/115` and `release/116`. The only functional difference in that file is in
  `get_3prime_seq_offset` (`check_length` no longer subtracts the deletion length), which
  affects HGVS 3'-shifting, not allele trimming, and is out of scope here — though it is worth
  remembering when working vepyr#32 or #50.

The remaining Perl-semantics assumption — that assigning to the `foreach` variable at `:881`
aliases the array element — is standard, and is **corroborated from the outside** by the
VEP 116.0 docker run in §1, which is the stronger evidence. Both agree, so the plan proceeds on
the measurement rather than on the Perl.
