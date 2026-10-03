# Issue 149: shifted negative-strand insertion HGVSp

## Objective and authorization

The user authorized iterating the remaining data issues, with separate PRs,
performance and quality evaluation, corresponding ported-test PASS evidence,
and reviewer iteration until green. This plan is the next issue in that queue.
Merging and post-merge pins remain human-directed.

Vepyr branch `fix/issue-149-frameshift-hgvsp` starts at freshly fetched remote
master `e906fbb38fdc422dbfe86aae2c390d258e1ce500`, whose engine pin is
`34369217daf373eb5b2d41605ba6cfaa3656494f`. Functions starts at remote master
`789aa6f481a9681893dc41e8bd637ac8155bfaa8`, the merged #148 fix. The relevant
translation functions are identical at these two engine revisions. Formats
remains v1.13.0 (`00487b9edf53cbf36918d7d22a03a79fd0acf863`). Ordinary working
trees and downloaded caches remain untouched.

## Defect, witnesses, and ownership

For `21:25592985 A>ATAAA`, ENST00001110241 receives
`ENSP00000780046.1:p.Leu262TyrfsTer8` instead of the VEP 116.2 value
`ENSP00000780046.1:p.Leu262PhefsTer81`. Two original ported fixtures share
normalized input SHA256
`65b85070cb3c7080d181c2c74eead898b81f0f6844959364a10d82b03c4bb6c3`:

- `report_the_intronic_consequence_for_the_taaa_insertion_fe6e88`
- `report_the_unanchored_taaa_insertion_allele_in_csq_f43eb8`

Their expected strict body MD5 is `46364ad56f27f3b15bb0a484c3e0f63e`.
All other CSQ fields already match, including HGVSc `c.780_783dup`, HGVS_OFFSET
-7, CDS position776-777, cDNA792-793, protein259, amino acidsY/YLX and
codons `tat/taTTTAt`. Preserve those fields and the original primary assertions.

Three independent read-only analyses cover formats, functions and vepyr.
Functions owns alternate CDS construction and HGVSp. Formats carries native
reference CDS, UTR and mapper data; no demonstrated formats defect exists.
Vepyr transports flags/results and requires only a pin plus regression coverage.

Pinned formats extraction: `transcript.rs:2106` and `translation.rs:491` copy
reference CDS/peptide; `transcript.rs:1049` decodes mapper segments and `:2139`
carries 3-prime UTR. Existing sequence equality checks do not prove every runtime
mapping choice correct. Vepyr delegates to the engine at `src/annotate.rs:193`
and `:704`; it must not rewrite HGVSp strings afterward.

## Source rule and targeted investigation

At engine789aa6, `transcript_consequence.rs:5650`
(`shifted_tva_coords_from_mapper`) and `:6511`
(`rotate_hgvs_protein_allele`) diverge from the negative-strand shifted replay.
Ensembl variation source is pinned to
`db7394a02523a6e519cb0d89f84b2889bac12737`, paired with VEP116.2 source
`2cb0bbe216bb31c75de8f8000e2da7ff4fb7b451`.
`TranscriptVariationAllele.pm:1337-1343` assigns
`$shift_length = $seq_length - $shift_length` on the negative strand, then
rotates only while the loop counter is below that length. In particular a
4-base allele shifted7 bases does not perform a modulo rotation.

A read-only replay using the actual cached CDS+UTR reproduced all four outcomes:
existing mapping and rotation => TyrfsTer8; corrected rotation only =>
PhefsTer8; corrected mapping only => IlefsTer81; both => PhefsTer81.
The mapping must preserve insertion start>end after transforming negative-strand
genomic endpoints to transcript coordinates. Confirm exact Ensembl mapper
source boundaries and 116.0/116.2 source equivalence before implementing.
Actual VEP116.2 Mapper helper probes confirm shifted cDNA800..799 and
CDS784..783. Core `Mapper.pm:485-502` swaps mapped bounds for a single
contiguous coordinate; split coordinates and gaps follow separate rules.
`TranscriptMapper.pm:528-529` converts mapped bounds into peptide coordinates.
Cover same-segment, split/edited and missing-flank cases rather than sorting all
mapped coordinates indiscriminately. Governing core Mapper, TranscriptMapper
and BaseTranscriptVariation files are byte-identical in Docker116.0 and116.2;
the relevant variation shift/CDS/stop-distance routines are unchanged.
Do not special-case the transcript, genomic position, or expected HGVS string.

## Implementation and regression plan

1. Capture untouched native-build timing/peakRSS/phase baselines at1 and8
   workers after discarded warm-up, and all22 strict body-MD5 autosomes.
2. Normalize each original input with `bcftools norm -m -both --no-version`,
   run fresh VEP116.2 Docker and baseline vepyr on identical bytes with matching
   merged116 caches/FASTA, and retain commands, exits and body digests.
3. Add failing engine regressions for shifted negative-strand coordinate mapping,
   allele rotation below/equal/above allele length, and exact alternate peptide.
   Existing unshifted tests at18987/19049 miss this path; the chr2 shifted fixture
   at19947 is development-only and cannot be the only guard.
4. Correct those source-derived rules in functions, keeping ordinary unshifted
   and positive-strand paths stable. Validate controls and inspect any moved
   WGS digest before changing scope.
5. Add focused Python integration coverage reusing immutable checked-in
   `tests/data/ref_genome_mismatch/cache` and reference.fa.gz. They already
   reproduce the exact wrong feature tuple. This Ensembl slice returns33 CSQ
   entries versus38 in the full merged oracle, so assert the exact target feature
   and preserve matching fields; reserve whole-body assertions for full-cache
   ported replays. Reach API/CLI, indexed workers and LazyFrame projections.
6. Commit/open separate engine and vepyr draft PRs, pin the actual engine head,
   request Codex and Claude, answer every finding, and refresh downstream pins
   after upstream changes. Formats needs no PR unless new evidence changes owner.
7. Repeat native performance and strict WGS gates on the final current-head
   cascade. Replay both original cases plus the full campaign to catch regressions.
   Hand off through the runbook only when CI/reviews/gates are green; never merge.

## Expected gates and evidence

Both original FAIL cases must become PASS with strict body MD5
`46364ad56f27f3b15bb0a484c3e0f63e`. Do not bless changed expected values.
Existing #147 and #148 fixes remain passing when included in the new engine pin;
use the matching native synonym metadata overlay for accession tests.
The full WGS reference remains VEP116.0, separate from fresh issue-specific
116.2 oracles: all22 autosomes /4,096,123 records must match strictly.
Performance limits are5% per measured phase and10% for wall/peakRSS, at1/8 workers.
Any RSS delta is reported in absolute MiB as well as percentage.

Local evidence is retained in the original workspace under
`e2e-testing/results/fix-issue149-20261003/`; it is not checked into the PR.

## Validation-discovered RefSeq control

The corrected peptide boundary exposed an existing compensating approximation
for `2:73385903 T>TGGA`, `NM_015120.4`. A fresh VEP116.2 run confirms
`NP_055935.4:p.GluGlu25=`. Instrumenting the actual oracle gives genomic shift39,
CDS75..77 and peptide25..26: the shifted insertion spans the transcript-only
three-base RNA edit. The old no-mapper unit omitted the real genomic shift and
obtained the correct string through a wrongly widened window.

Preserve the real genomic shift via the source `_return_3prime` reuse guard,
reconstruct edited scalar mapping from exon geometry and SeqEdit deltas when
cached mapper segments are absent, and preserve actual gaps when they exist.
A nonempty edited insertion gap must keep its edited CDS coordinates and edited
sequence together, including the reference RNA-edit patch. An already equal
shifted peptide pair retains its mapper window before legacy reclassification.
The unrelated fallback shifting path without a precomputed genomic shift is
unchanged; this is not a claim of full parity for that helper.

Source receipts and fresh full-cache VCF control are retained under
`source/` and `refseq-equality-control/` in the evidence directory. No expected
HGVS string was changed. The one-flank coordinate unit now expects peptide end140
for CDS421..420, confirmed independently by actual VEP116.2 mapper helpers.
The corrected real-shift RefSeq regression retains its original expected HGVS.

## Candidate checkpoint

Engine PR267 pins `55f92666a26bb0abc7820b57abcbab9a89cfb328` and keeps
formats v1.13.0. All1,181 engine unit tests pass (3ignored), full pre-commit
and workspace Clippy pass, and20 focused Python tests pass. The complete189-case
replay improves180PASS/9FAIL to182PASS/7FAIL/0ERROR, with exactly the two original
#149 cases changing and no regressions. Both fresh116.2 oracle bodies match
`46364ad56f27f3b15bb0a484c3e0f63e`; the additional edited RefSeq control also
matches its entire body (`f75a66fdc644abcf4ccb4130c96baf18`). The final pinned
native performance/WGS gates and both external review loops remain pending.

Review follow-up: preserve legacy split mapper + RNA-edit flag state when the
parsed edit list is absent; restrict direct equality to actual edited gaps;
cover edit encounter order and zero-length windows on both strands. The
Python projection test now checks per-feature list alignment explicitly.
