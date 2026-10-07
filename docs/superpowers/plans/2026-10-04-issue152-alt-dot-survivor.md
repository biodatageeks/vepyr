# Issue 152: reference selection after a leading ALT-dot row

The normalized original input contains 21:25587759 G>. followed by
21:25587762 C>A. The first row is already correctly skipped. The surviving
SNV previously differed from VEP 116.2 in HGVSc and USED_REF.

## Ownership and source rule

All three repository analyses were completed before this plan. Formats owns
native BAM policy metadata, functions owns reference selection and HGVSc, and
vepyr carries dependency pins and the USED_REF projection dependency. The common
fix is formats#259, functions#268 and vepyr#160. The full campaign replay now
passes this original test, so this separate PR adds its regression witness.

VEP 116.2 2cb0bbe Parser/VCF.pm:259–266 skips ALT-dot rows and advances;
CacheDir.pm:412–415 enables transcript reference from native info.bam;
AnnotationType/Transcript.pm:155–162 passes that policy with no_ref_check.
Variation 116 db7394a TranscriptVariation.pm:448–467 requires complete spliced
mapping. TranscriptVariationAllele.pm:1474–1478 and 1508–1514 selects genomic
HGVS reference with the actual RNA-edit exception. Intronic USED_REF can remain
the given C while ordinary HGVSc uses the genomic T. No parser change or REF
repair is needed.

## Execution and acceptance

Start at verified remote master e906fbb and retain the shared fix's exact pins
and Python projection change. The untouched shared baseline captured before any
production edit uses this same master with functions3436921/formats00487b9;
the original body fails there. Use its immutable performance and strict-MD5
baseline and the recorded original input SHA-256.

Copy the genuine merged 116 chr21 cache slice covering all 39 annotations,
without changing biological data or relabelling an Ensembl-only cache. Include
the unchanged normalized original and separate T>. / T>A control, each with
the actual Docker VEP 116.2 golden and command/checksum provenance.

Check that only position 25587762 is emitted, raw REF remains C (T in control),
all 39 ordered CSQs match, ENST00000307301 has c.997A>T and USED_REF=T, and the
intronic ENST00000352957 retains the appropriate given reference. Exercise
plain/indexed VCF workers 1/2, CLI and full/narrow LazyFrame projections.

Build this branch natively. Reuse #150's final workers 1/8 performance and
strict 22-autosome gate only with an explicit identical-production receipt;
do not describe shared measurements as new per-issue runs. A production change
would require a new measurement. Request Codex and Claude, answer feedback,
wait for CI and complete the hand-off. Leave all merging to the user.
