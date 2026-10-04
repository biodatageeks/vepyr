# Issue #150: select transcript and genomic reference independently

The normalized miRNA SNV at 21:25573985 C>T must retain the supplied VCF REF
while matching VEP 116.2 HGVSc and USED_REF on all transcripts. This is the next
issue after #149. The three repository analyses are complete in analysis.md.

## Source rule and ownership

Native VEP enables use_transcript_ref from the cache info.txt BAM declaration.
For every transcript, USED_REF comes from fully mapped spliced transcript
sequence, with supplied REF as the fallback for intronic/incomplete mappings.
HGVSc ordinarily reads the genomic feature slice independently; genuine RNA
edits restore the transcript feature sequence (use native has_rna_edit evidence,
including poly-A, independently of alignment-inferred edits). The original witness distinguishes
these choices: miRNA uses A/n.6A>T, the negative intronic transcript uses supplied
C but c.970-626T>A, and the downstream transcript retains supplied C with no HGVS.

- Formats: expose explicit native BAM policy in provider schema metadata. Native
  absence means known false; converted metadata absence remains unknown.
- Functions: preserve validated metadata on conversion/resume; propagate policy;
  separate USED_REF from genomic HGVS reference; reuse the genomic substring
  only for completely mapped ordinary unedited spans. Keep edited RefSeq handling.
- Vepyr: pin cascade and retain FASTA for USED_REF-only projections. Add actual
  merged-cache witnesses and API/CLI/LazyFrame regressions.

## Execution

1. Fetch remote masters and create isolated branches. Record exact identities and
   confirm prior analyses against those sources. Capture native performance and
   strict MD5 baselines before edits. Prepare a metadata overlay of the downloaded
   merged cache; keep every original cache file untouched.
2. Confirm original and reference-consistent control using actual Docker VEP
   116.2 after bcftools norm -m -both --no-version. Run the same input bytes through
   the baseline engine. Capture full-body digests and exact differing CSQ fields.
3. Add known BAM policy to CacheInfo and all native provider schemas without
   changing biological row schemas. Test actual nonempty, absent/empty BAM input,
   projection metadata preservation and all entity schemas.
4. Preserve a versioned root reference-policy sidecar in prepare_build_metadata,
   after native source validation and before existing-entity resume skips. Validate
   sidecar release/source against participating shard identities at annotation.
   Known false must disable GIVEN_REF/USED_REF header selection and population;
   unknown legacy policy preserves the existing behavior explicitly.
   Use atomic writes. Unknown legacy metadata retains the documented compatibility
   behavior; never infer policy from a directory name or merged identity. Test
   fresh build, completed-entity resume, standalone chromosome builds, false and
   unknown policy, mismatched identity and malformed policy. Existing build entry
   points provide metadata refresh; add no unrelated annotation flag.
5. Resolve policy from the first existing variation footer plus the validated
   root sidecar before building schemas or headers. Freeze it for serial, worker
   and SQL paths; reject conflicting later shard evidence. Split GIVEN_REF and
   USED_REF from the unrelated RefSeq field gate in every layout/value path.
   Pass policy through contig configuration to the consequence engine. Prefer
   complete native transcript sequence; a synthesized CDS-only string does not
   establish full transcript coverage. For an ordinary unedited span, require
   every genomic base to map monotonically and consecutively without gaps or
   duplicate positions before using the genomic substring as transcript REF.
   This follows core116 Transcript::spliced_seq plus Exon::seq and avoids broad
   whole-transcript hydration. Preserve edited and shifted-deletion paths.
   Provide genomic HGVS REF separately from original input and USED_REF. Keep
   original CSQ coordinates and allele spelling. Measure broader FASTA reads.
6. Prepare a genuine merged chr21 fixture containing the miRNA, intronic and
   downstream features. Preserve source identity and native policy metadata;
   do not relabel the old Ensembl fixture. Test full VCF/API, CLI, indexed workers
   1/2, full LazyFrame and narrow HGVSc/USED_REF projections. Include ordinary,
   edited, negative-strand and incomplete-mapping controls.
7. Open separate draft formats/functions/vepyr PRs with exact head pins, run
   pre-commit and relevant units/integration tests, request Codex and Claude,
   answer each finding, and re-pin after upstream changes.
8. Recheck the original port and control with fresh VEP 116.2, replay the whole
   campaign, and run native worker 1/8 performance plus all-autosome strict MD5.
   Leave draft while any gate fails. Hand off only when reviewers/CI/gates are
   green; never merge. Inspect incidental effects on #151–#155 with their own
   witnesses and retain separate PR/test accounting.

## Source anchors and expected gates

- VEP 116.2 `2cb0bbe`, CacheDir.pm:412–415 enables use_transcript_ref from
  info.bam; AnnotationType/Transcript.pm:155–162 applies it to every transcript.
- variation116 `db7394a`, TranscriptVariation.pm:448–467 requires a complete
  mapped spliced reference; TranscriptVariationAllele.pm:1474–1478 and
  2689–2735 reads genomic sequence, with actual RNA edits restoring feature
  sequence at 1508–1514.
- core116 `0d852313c420a1d5128b6774635125ebbac03350`, Transcript.pm:823–878
  concatenates Exon::seq then applies RNA edits; Exon.pm:1491–1530 obtains
  ordinary sequence from the strand-oriented genomic slice. Native dumps remove
  exon sequence caches (VEP Pipeline/DumpVEP/Dumper/Core.pm:210–222).
- VEP Constants.pm:99 and OutputFactory.pm:1774–1778 gate GIVEN_REF/USED_REF
  independently on use_transcript_ref.

Expected: original port full body FAIL→PASS; control remains PASS; full-genome
strict MD5 stays PASS. No performance improvement is claimed. Both worker counts
must pass the runbook phase/wall/RSS gate. Baseline source remains untouched
until both native measurement and strict-MD5 baselines finish.

## Review and performance follow-up

Known BAM policy also controls BAM_EDIT, as required by VEP Constants.pm:100.
A Docker control removes only native BAM metadata and verifies the complete
VCF, including RNA-edited NR_001458.3:n.291C>T. Disabled policy must reach the
HGVS fallback so it cannot independently re-read edited transcript sequence.

Two initial native performance attempts missed the context-loading phase at
eight workers (5.7% and 7.0%); the first also missed writer1 by 6.5%. Both attempts
are retained. The first strict 22-autosome run passed; the second quality run
was stopped after its performance failure before changing the installed binary.
A bounded 64 KiB per-batch FASTA window now reuses nearby genomic reference reads.
An instrumented indexed-reader test verifies the seek reduction, exact sequence,
window boundaries, contig changes/ends, long-variant bypass and reader errors.
The unchanged input/oracles, full campaign and all native gates must be rerun
on this candidate before hand-off; no failed threshold is waived.

The bounded-reader attempt also failed performance (four phase bars), while
all 22 strict body digests passed. This remains a failed attempt, not evidence
of a performance improvement. A diagnostic rebuild of untouched master/pins
will check host drift without replacing the original pre-edit baseline.

Further review found that conversion could overwrite a conflicting root policy
or skip shard validation when the root matched. Conversion now rejects a
conflicting root without modifying files and checks existing shards on every
resume. Regression tests cover both BAM directions, source/version conflicts
and byte preservation. Projection-aware genomic reads now require an emitted
HGVSc or USED_REF consumer. A real indexed-reader test uses a missing contig to
prove that consequence-only and unrelated-field projections skip the query,
while reference-consuming projections still query and report the error.
