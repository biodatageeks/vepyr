# ClinVar chrMT mismatches: root-cause analysis and debugging plan

Status: analysis for review. No engine, reader or cache change has been made.

Scope: the 2,327 of 3,134 chrMT records that differ between Ensembl VEP 116.0 and
vepyr 0.10.0 in the ClinVar 20261011 parity run
([`e2e-testing/golden/116/clinvar/`](../../../e2e-testing/golden/116/clinvar/README.md)),
using the fixtures in
[`mismatches/chrMT/`](../../../e2e-testing/golden/116/clinvar/mismatches/chrMT/README.md).
The representative fixtures, TRNT experiments, and all 3,134 chrMT records
have been checked against the exact **Ensembl VEP 116.0 and 116.2 images**. Both
images produce the same fixture bodies and the same 2,327-record mismatch key
set against vepyr. Their full chrMT outputs differ only at MT:9204, which
remains a mismatch in both.

Evidence for this document is in
[`2026-10-18-clinvar-chrmt-mismatches/`](2026-10-18-clinvar-chrmt-mismatches/):
the VEP 116.2 outputs of the three fixtures and of the TRNT experiment, the TRNT
input, `reproduce.sh` for fixtures, and
[`full-chrmt-1162-verification.md`](2026-10-18-clinvar-chrmt-mismatches/full-chrmt-1162-verification.md)
with full-run hashes, results, and the full-run reproducer. The exact 116.0
image rerun, fixture checks, full chrMT comparison, and corrected-flank
diagnostic are recorded in
[`vep1160-cross-check.md`](2026-10-18-clinvar-chrmt-mismatches/vep1160-cross-check.md).

Repository revisions read: vepyr `1931257` (this branch's base,
`e2e/clinvar-parity`), datafusion-bio-functions `46d8c20`,
datafusion-bio-formats `dbc62fb`. The converted cache inspected is the local
`116_GRCh38_merged`. The raw cache is `homo_sapiens_merged/116_GRCh38`.
The pinned default image for reproducing these experiments is
`ensemblorg/ensembl-vep:release_116.2@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da`;
`reproduce.sh` also checks the VEP, core, and variation identity in every
output header.

## Summary

| # | Class | Records | Root cause | Status | 116.0 vs 116.2 | Repos | Issue |
|---|---|---:|---|---|---|---|---|
| 1 | `csq_order` | 2,030 | VEP sorts on `stable_id_dbID`; the engine sorts on `stable_id` | **confirmed** | same fix | functions | #199 |
| 2 | `codon_table` | 294 | the reader drops `codon_table` (wrong key path); the engine only knows table 1 | **confirmed** | same fix | formats, functions, caches | #200 |
| 3 | `hgvs_shift` (a) | 1 | VEP's MT-end flank truncation suppresses a valid 2-base genomic shift before partial-overlap HGVS formatting | **confirmed** (VEP bug) | same fix | functions, only if emulating VEP | #201 |
| 3 | `hgvs_shift` (b) | 3 | The same MT-end flank truncation makes VEP shift TRNT indels by 0/1/1 instead of 3/5/4 | **confirmed** (VEP bug) | same fix | functions, only if emulating VEP | #201 |
| — | CYTB `start_lost` | 1 (MT:6467) | vepyr `start_lost&5_prime_UTR_variant`, VEP `coding_sequence_variant&5_prime_UTR_variant`; not caused by the flank bug | **open** | same in both | functions | #202 |
| 4 | stop-codon frameshift HGVSp | 1 (MT:9204) | Ensembl variation PR #1217: 116.1/116.2 write `extTer` (count − 1) where 116.0 wrote `fsTer` | **confirmed** | **116.2 only** | functions, runner | #203 (#50, #51) |

Causes 1–3 and the CYTB item behave identically in VEP 116.0 and 116.2, so
their fixes are shared. Cause 4 exists only from 116.1 on. MT:9204 is already
counted in `codon_table`, so the mismatch key set (2,327 records) is the same
for both versions. See [section 4](#4-fixes-for-1160-vs-1162).

## 0. Cross-version VEP check

The stored references were made with `release_116.0`. Fresh runs against both
exact image digests reproduce those fixture bodies: the 116.0 outputs are
body-identical to the stored VEP references, and the 116.2 outputs are also
body-identical. Both runs use the golden flags (`--cache --offline --assembly
GRCh38 --merged --vcf --everything --hgvs --fasta ...`). The 116.0 image is
`release_116.0@sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11`
with core revision `c0cf13d` and variation revision `2fb834b`.

| Fixture | 116.2 vs 116.0 (body lines) | 116.2 vs vepyr 0.10.0 (body lines) |
|---|---:|---:|
| `codon_table` | 0 | 24 |
| `csq_order` | 0 | 6 |
| `hgvs_shift` | 0 | 8 |

The 116.2 headers identify core revision `0d85231` and variation revision
`db7394a`. Fresh full chrMT runs each emitted 3,134 records and both differ
from vepyr at the same 2,327 record keys. VEP 116.0 and 116.2 differ from each
other only at MT:9204; that record is a mismatch in both. Details are in the
[`full-run verification`](2026-10-18-clinvar-chrmt-mismatches/full-chrmt-1162-verification.md).
The existing parity runner checks exact reference identity, so add an explicit
116.2 target rather than weakening the 116.0 check.

## 1. `csq_order` — confirmed

**Symptom.** Identical CSQ entries; VEP writes RefSeq `ND4L` before `ND4`, vepyr
`ND4` first.

**VEP 116.2.** Transcript variations are returned by
`VariationFeature::get_all_TranscriptVariations` as
`map { $self->{transcript_variations}->{$_} } sort keys %{...}`, and the key is
`BaseVariationFeature::_get_transcript_key` (`BaseVariationFeature.pm:310`):

```perl
my $key = $tr->stable_id;
if (my $dbID = $tr->dbID) { $key .= '_'.$dbID; }
```

The raw cache gives:

| stable_id | dbID | key |
|---|---:|---|
| `ND4L` | 8378488 | `ND4L_8378488` |
| `ND4` | 8361687 | `ND4_8361687` |

`L` (0x4C) sorts before `_` (0x5F), so ND4L comes first. The Ensembl pair
(`ENST00000361335`, `ENST00000361381`) has fixed-length IDs, so it is unaffected.

**Engine.** `transcript_consequence.rs:1024` sorts transcripts by
`transcript_id` alone ("so that source-array index order equals lexicographic
feature-ID order"), and the CSQ sort compares those indices.

**Fix direction.** Sort on `stable_id + "_" + db_id` compared as bytes, and
compare the dbID as text, not as a number. `db_id` is already in the converted
transcript cache (ND4L 8378488, ND4 8361687), so no cache change is needed.

**Blast radius to check.** The order only changes where one stable ID is a
prefix of another overlapping one, or where two transcripts share a stable ID.
On MT the only prefix pair is ND4/ND4L. Scan every contig's transcript cache for
such pairs before changing the engine; HG002 chr1–22 md5 parity must hold.

## 2. `codon_table` — confirmed, two layers

**Symptom.** MT transcripts are translated with the standard code: ATA→Ile
instead of Met, AGA/AGG→Arg instead of stop, TGA→stop instead of Trp.
`Amino_acids`, `HGVSp`, `Consequence`, `IMPACT`, `SIFT` and `PolyPhen` follow.
SIFT and PolyPhen change because their scores are looked up by amino-acid change.

**Raw cache (read with the 116.2 image).** Every protein-coding MT transcript,
Ensembl and RefSeq, has `codon_table = 2` under
`$tr->{_variation_effect_feature_cache}{codon_table}`. The keys of that hash
are `codon_table, five_prime_utr, introns, mapper, sorted_exons,
three_prime_utr, translateable_seq` (plus `peptide`, `protein_features`,
`protein_function_predictions` and `seq_edits` on coding transcripts). The slice
carries no attributes in the offline cache. So in offline mode VEP gets the
codon table from VEFC; without it VEP would fall back to the slice attribute,
then to 1.

**Layer 1, reader (datafusion-bio-formats).**
`bio-format-ensembl-cache/src/transcript.rs` reads the scalar from the top level
of the transcript object:

- JSON path, line 1596: `json_i32(object.get("codon_table"))`
- Storable path, line 2153: `sv_i64(object.get("codon_table"))`

There is no top-level `codon_table`, so the column is null. In the converted
`116_GRCh38_merged/transcript/chrMT.parquet` all 74 transcripts have
`codon_table = null`. Every published cache is affected.

**Layer 2, engine (datafusion-bio-functions).** `translate_codon` and
`translate_codon_bytes` in `transcript_consequence.rs` (lines ~10053 and ~9718)
hard-code table 1. There are 24 call sites, and none reads a per-transcript
table.

**Fix direction.**

1. Reader: read `_variation_effect_feature_cache.codon_table`, falling back to
   the top level. Rebuild the MT cache and check the column says 2.
2. Engine: carry the codon table on `TranscriptFeature` and translate with it
   at every call site.
3. Read 116.2's start/stop logic before coding (`VariationEffect.pm`,
   `TranscriptVariationAllele.pm`, `Bio::Tools::CodonTable`). Table 2 has
   alternative start codons (ATT, ATC, ATA, ATG, GTG), and MT transcripts
   complete their stop codons with RNA edits; CYTB carries
   `_rna_edit = 1141 1141 TAA`.
4. Decide how already-published caches behave (question 1 below).

## 3. `hgvs_shift` — two different problems

### 3a. The 9,119 bp deletion at MT:6467

The record's REF is 9,120 bases with ALT `C`, so 9,119 bases are deleted
(MT:6468–15586). The run README and the fixture README call it a 9,120 bp
deletion.

VEP 116.0 and 116.2 against vepyr:

| Feature | VEP 116.0 / 116.2 | vepyr 0.10.0 |
|---|---|---|
| ATP6 (inside the deletion) | `transcript_ablation`, `c.1_681del` | `transcript_ablation`, HGVSc empty |
| CYTB (deletion covers its 5' end) | `coding_sequence_variant&5_prime_UTR_variant`, `c.1_840del`, no offset, cDNA `?-840` | `start_lost&5_prime_UTR_variant`, `c.3_842del`, `HGVS_OFFSET=2`, cDNA `?-840` |

**Confirmed source cause (VEP 116.0 and 116.2).** The VEP variation interval is
MT:6468–15586. Its MT slice is 1–16569. The 116.0 source at variation revision
`2fb834b` and the 116.2 source at `db7394a` both use the same expansion,
sequence-region constraint, and last-1,000-base `post_seq` selection.
`TranscriptVariationAllele::_genomic_shift` expands that whole slice, constrains
it to MT, then takes the last 1,000 bases as `post_seq`. The resulting slice is
5468–16569 (11,102 bases), so that suffix starts at 15570: 17 bases before the
deletion ends. The actual post-deletion sequence begins at 15587. VEP therefore
checks the wrong flank and, in its default run, reports no shift for CYTB and
`c.1_840del`.

vepyr reads the actual genomic flank, finds a 2-base shift, then clamps the
deletion to CYTB's transcript slice and applies the offset, giving `c.3_842del`.
For ATP6, applying that offset moves the clamped end beyond the transcript; the
HGVS formatter returns no HGVSc. A diagnostic VEP run correcting only the
post-flank selection also produced CYTB `c.3_842del` with `HGVS_OFFSET=2` and
empty ATP6 HGVSc in both images. Both HGVS differences therefore follow from
VEP's truncated MT flank, rather than a missing vepyr partial-overlap
implementation.

The 116.2 source path is in
[VEP 116.2 `TranscriptVariationAllele.pm`](https://github.com/Ensembl/ensembl-variation/blob/db7394a/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm):
`_genomic_shift` constrains the expanded slice and then uses its final 1,000
bases. The engine's partial-overlap formatter clamps coordinates and rejects a
shift that runs past the transcript in
[`vep116_partial_overlap_deletion`](https://github.com/biodatageeks/datafusion-bio-functions/blob/b960112/datafusion/bio-function-vep/src/hgvs.rs#L543)
(`hgvs.rs`, functions `master` at `b960112`).

The CYTB `start_lost` against VEP's `coding_sequence_variant` is a separate
consequence difference: it persists in both default images and is unchanged
by the corrected-flank diagnostic. Its root cause remains open.

### 3b. TRNT 1 bp indels: VEP does not shift fully

Reference, MT:15925–15946: `CCGGAGAT G AAAA CC TTTTT CC` (A run at
15934–15937, T run at 15940–15944). TRNT and ENST00000387460 span
15888–15953 on the + strand, single exon, so n.N = 15887 + N.

| Record | VEP 116.0 / 116.2 | vepyr | Full 3' shift |
|---|---|---|---|
| 15933 GA>G | `n.47del`, offset empty | `n.50del`, offset 3 | n.50 (15937) |
| 15939 C>CT | `n.54dup`, offset 1 | `n.57dup`, offset 5 | n.57 (15944) |
| 15939 CT>C | `n.54del`, offset 1 | `n.57del`, offset 4 | n.57 (15944) |

The branch README says vepyr places these 3 bases too far 3'. The reverse is
true: vepyr's output is the HGVS-correct 3' shift and matches ClinVar's own
`CLNHGVS=NC_012920.1:m.15937del` / `m.15944dup` / `m.15944del`. VEP shifts by
0, 1 and 1 bases.

What is ruled out so far:

- **Not a VEP version change.** Fresh 116.0 and 116.2 runs give the same
  outputs for these three records.
- **Not MT-wide.** In the full 116.0 chrMT output VEP shifts 12 other tRNA
  indel entries (e.g. TRNF `n.19dup` offset 2, TRNV `n.55del` offset 1) and 5
  rRNA ones, and vepyr agrees on all of them.
- **Not the neighbouring CYTB.** CYTB ends at 15887, carries a +2 bp RNA edit
  and is `downstream_gene_variant` here. Re-running with
  `--transcript_filter "stable_id in TRNT,ENST00000387460"` and with
  `--transcript_filter "not stable_id in CYTB,ENST00000361789"` gives the same
  `n.47del` / `n.54dup` / `n.54del` (`vep116_2/hgvs_shift_trnt_*.vcf`).
- **Not a transcript sequence edit.** TRNT and ENST00000387460 have no
  `_rna_edit` or `seq_edits` in the raw cache, and their exons span the genome
  sequence above.

**Confirmed source cause (VEP 116.0 and 116.2).** Both exact image revisions
use the same `_genomic_shift` code for these variants. For a deletion at
MT:15934,
the source slice is 1–16569; after expansion and sequence-region constraint,
the slice is 14934–16569 (1,636 bases). Taking its last 1,000 bases makes
`post_seq` start at 15570, 364 bases before the variant. The true post-variant
sequence begins at 15935 and contains the remaining A run. VEP compares against
the wrong bases, so the A deletion does not shift. For the T insertion/deletion,
the wrong flank starts with one matching T followed by A, so VEP stops after one
shift.

As a causal check, I ran both exact VEP images and the release 116 cache with a
temporary module patch that starts `post_seq` immediately after the variant
interval, using available sequence up to the MT boundary. The three original
inputs then produce shifts 3/5/4 and `n.50del`, `n.57dup`, `n.57del`, matching
vepyr 0.10.0 and ClinVar. No Ensembl or engine source was modified; the
patched module was bind-mounted read-only into the container. The patches,
traces and corrected-flank outputs are in
[`flank_diagnostic/`](2026-10-18-clinvar-chrmt-mismatches/flank_diagnostic/):

- `vep1160_post_seq.diff`, `vep1162_post_seq.diff`: the one-line `post_seq`
  change against each image's own module. The 116.2 diff also carries a
  `warn` line gated to MT:6400–6600 for the deletion.
- `vep1162_trnt_trace_instrumentation.diff`: the trace-only `warn` lines used
  for the TRNT probes.
- `vep1162_trnt_shift_trace_default.tsv`, `..._corrected.tsv`: per-probe
  traces. In the default run `post_seq` starts with `TATTCGCCTACA` (MT:15570)
  for every probe position; with the change it starts right after the variant.
- `vep116{0,2}_hgvs_shift_corrected_flank.vcf`: the four `hgvs_shift` records
  annotated with the change.

Run details are recorded in
[`hgvs-shift-1162-confirmation.md`](2026-10-18-clinvar-chrmt-mismatches/hgvs-shift-1162-confirmation.md)
and [`vep1160-cross-check.md`](2026-10-18-clinvar-chrmt-mismatches/vep1160-cross-check.md).

This confirms the remaining HGVS parity mismatches as VEP 116.0 and 116.2's
MT-end flank-selection behavior. It does not establish that vepyr should
reproduce that behavior; a parity-only change would need a separate decision.

**What emulation would have to copy.** VEP's flank window is
`[max(1, var_start − 1000), min(contig_length, var_end + 1000)]`; `pre_seq`
is its first 1,000 bases and `post_seq` its last 1,000. That equals the true
flanks only when neither end is clipped. So:

- the downstream flank is wrong when `var_end > contig_length − 1000`
  (MT:15570 and later); this is what the four records exercise;
- by the same code the upstream flank is wrong when `var_start ≤ 1000`, which
  matters for minus-strand 3' shifting. No ClinVar MT record exercises it, so
  a synthetic minus-strand probe near MT:1 has to confirm it before it is
  copied.

On nuclear contigs both conditions fall in telomeric `N` sequence, so in
practice only MT is affected.

## 4. Fixes for 116.0 vs 116.2

**What changed between the releases** (Ensembl GitHub compares):

| Component | 116.0 | 116.1 | 116.2 | Change |
|---|---|---|---|---|
| ensembl-variation | `2fb834b` | `db7394a` | `db7394a` | PR #1217 "Fix hgvsp notation for extTer" (`7bd0526`), one hunk in `TranscriptVariationAllele::_get_hgvs_protein_format` |
| ensembl (core) | `c0cf13d` | `0d85231` | `0d85231` | "Removing MySQL hint" (database mode only) |
| ensembl-vep | `release/116.0` | `release/116.1` | `release/116.2` | 35 commits 116.0→116.2, see below |

The annotation code that decides consequences, HGVS and sorting
(`ensembl-variation`) is identical in 116.1 and 116.2.

**Shared fixes (one implementation serves 116.0 and 116.2):**

1. `csq_order`: the sort code (`_get_transcript_key`, `get_all_TranscriptVariations`)
   is unchanged.
2. `codon_table`: `BaseTranscriptVariation::_codon_table` reads VEFC, then the
   slice attribute, then 1, in both; the fixtures are byte-identical.
3. `hgvs_shift`: `_genomic_shift` lines 427–437 are identical in both images;
   the flank bug is present in 116.2.
4. CYTB `start_lost` (open): same VEP output in both.

**116.2-only fix:**

5. Stop-codon frameshift HGVSp. When the first changed residue is `Ter`, 116.1
   and 116.2 write `p.Ter<n><aa>extTer<m−1>` where 116.0 wrote
   `p.Ter<n><aa>fsTer<m>`; `?` stays `?`. On chrMT this is MT:9204
   (`ATP6.1:c.679dup`): 116.2 `p.Ter227LeuextTer?`, 116.0 and vepyr
   `p.Ter227LeufsTer?`. It is the first observed `extTer?` case; the HG002
   116.1 run in issue #50 had only numeric counts. This is genome-wide, not
   MT-specific: #50 measured 22 HG002 CSQ cells at 9 positions. The engine has
   to switch on the resolved VEP target, which needs a 116.x target in the
   support matrix and runner (#51).

**ensembl-vep changes judged not to affect cache-based `--everything` runs:**

- `--regulatory_gff`, bigWig summary stats, GFF protein versions,
  `--compress_output` failure detection: opt-in or error paths.
- `OutputFactory` (116.1→116.2) now writes `HIGH_INF_POS` and
  `MOTIF_SCORE_CHANGE` only when the motif matrix has `elements`. Checked: 8,000
  HG002 chr22 records with 46 motif entries are byte-identical under 116.1 and
  116.2.
- `InputBuffer` / `AnnotationType::Transcript` (116.0→116.1) now match
  transcripts to variants by chromosome as well as position (breakend fix).
  Expected to be a no-op for sorted SNV/indel input; #50's HG002 116.0 vs 116.1
  diff found only the HGVSp cells, which is consistent with that.

## 5. Plan

The three root causes have been reproduced against both exact images. Keep the
116.0 golden target strict and use the separately identified 116.2 target for
version-specific comparisons.

### Phase 0 — references and baselines

1. Slice MT from `clinvar_20261011.vcf.gz` and preprocess it as in the run
   README (`-e 'ALT="."'`, `bcftools norm -m -both`). **Done:** 3,134 records;
   the input source and body checksums are recorded in the full-run evidence.
2. Produce the full chrMT VEP 116.2 reference with the image pinned by digest.
   **Done:** all 3,134 records have VEP 116.2 headers and the expected core and
   variation revisions. Compared with VEP 116.0, one record (MT:9204) changed
   in terminal frameshift HGVSp notation; the change is documented in the
   full-run evidence.
3. Compare all chrMT records from vepyr against the 116.2 output. **Done:**
   3,134 records on each side, no missing records, 2,327 mismatches; their
   position/ClinVar-ID keys and class totals match the original 116.0 ledger.
   A fresh full 116.0 run produced the same mismatch key set. The original
   ledger remains unchanged as provenance for the 116.0 run.
4. Add a separately named 116.2 native support target for
   `run_comparison.py` / `validate_vep_reference_identity()`, with the 116.2
   codebase identity `116.2`, API/cache release `116`, assembly GRCh38, core
   revision `0d85231`, and variation revision `db7394a`. Keep the existing
   116.0 target strict. **Open** (#51 asks the same for 116.1).
5. Run the fixture reproducer and confirm vepyr 0.10.0 still reproduces the
   stored `vepyr.vcf` bodies. **Done:** all fixture outputs matched their
   expected 116.2-versus-116.0 and 116.2-versus-vepyr body-diff counts; the
   TRNT-only and no-CYTB probes both returned `n.47del`, `n.54dup`, and
   `n.54del` from VEP 116.2.

### Phase 1 — `csq_order`

1. Failing engine test with two overlapping transcripts `ND4` / `ND4L` and
   their dbIDs.
2. Sort key `stable_id + "_" + db_id`, compared as bytes.
3. Prefix/duplicate stable-ID scan over all transcript caches.
4. Gates: `csq_order` fixture diff empty against 116.2; HG002 chr1–22 md5
   unchanged.

### Phase 2 — `codon_table`

1. Reader fix and test against a Storable and a Sereal/JSON MT transcript.
2. Engine: per-transcript codon table at all 24 translation sites, after reading
   116.2's start/stop codon handling.
3. Tests from the 12 fixture records (one per distinct amino-acid change).
4. Gates: `codon_table` fixture diff empty against 116.2; HG002 unchanged.

### Phase 3 — `hgvs_shift`

1. 3a and 3b source mechanisms are confirmed against the exact VEP 116.0 and
   116.2 images.
2. Decide whether the parity target should preserve the current HGVS-correct
   vepyr results or emulate VEP's MT-end flank behavior. Do not change engine
   behavior until that compatibility choice is made.
3. If emulating: copy the window rule above, confirm the upstream-flank case
   with a synthetic minus-strand probe near MT:1 first.
4. CYTB `start_lost`: read VEP's `start_lost` predicate for deletions with an
   unknown cDNA start (`?-840`) and re-check after the codon-table fix.

### Phase 4 — 116.2-only (stop-codon frameshift HGVSp)

1. Add a 116.2 target to the support matrix and the runner, selected by the
   reference's declared VEP version rather than by cache version alone (#51,
   option b).
2. In the engine's `"fs"` HGVSp arm, write `extTer<m−1>` (or `extTer?`) when
   the reference residue is `Ter` and the resolved target is 116.1 or later;
   keep `fsTer<m>` for 116.0 (#50).
3. Gates: MT:9204 matches 116.2; #50's 22 HG002 cells match 116.1/116.2; the
   116.0 golden runs are unchanged.

### Phase 5 — verification

1. All three fixture diffs empty.
2. Full chrMT md5, strict and canonical, against VEP 116.2, with the 116.0
   golden run retained as the strict baseline.
3. chrY still matches; HG002 chr1–22 md5 gate unchanged.

### Delivery

Through the `vepyr-fix` workflow, as separate pieces: `csq_order` (engine
only, small), `codon_table` (reader + engine + cache rebuild), the MT-end
flank emulation if chosen, CYTB `start_lost` once its cause is known, and the
116.2 target with the `extTer` rule.

## 6. Questions for review

1. **Codon table on published caches.** Rebuild and republish the converted
   caches (116 merged/ensembl/refseq, 115), or also let the engine default to
   table 2 on MT when the column is null, so existing caches work?
   Recommendation: both.
2. **MT-end flank.** Emulate VEP's flank bug for both the TRNT indels and the
   9 kb deletion, even though vepyr's output is the HGVS-correct one and
   matches ClinVar? The zero-mismatch goal says yes; the alternative is to
   record these 4 records as known VEP errors.
3. **Reference version.** Keep the existing 116.0 identity check strict and
   add a separately identified 116.2 target, so vepyr supports both and
   switches the `extTer` rule by target? Or move to 116.2 only and regenerate
   the 116.0 golden references?
