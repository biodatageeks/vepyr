# Issue 153: transcript reference for an anchored deletion

Original normalized 21:25587759 AC>A has correct deletion geometry and HGVS,
but five Ensembl USED_REF values previously differed from VEP 116.2. Native
genomic bases are TA. Preserve the original AC>A and a separate TA>T control.

## Ownership and source rule

Three completed read-only analyses locate reference selection in functions,
native BAM policy preservation in formats, and dependency/projection transport
in vepyr. The common production fix is formats#259 / functions#268 / vepyr#160.
The complete original body passes the post-fix campaign. This issue therefore
needs its own fixture and regression PR, without another annotation algorithm.

VEP 116.2 2cb0bbe Parser/VCF.pm:327–335 removes the shared anchor before
annotation. CacheDir.pm:412–415 and AnnotationType/Transcript.pm:155–162 enable
transcript reference from native BAM policy. Variation 116 db7394a
TranscriptVariation.pm:448–467 returns the complete mapped deleted interval in
variant orientation; OutputFactory.pm:1774–1778 preserves the shift policy.
The annotation REF is C and Allele is -, while raw output REF remains AC.
Complete exonic mappings use A, not anchored TA or transcript-oriented T.
Intronic mappings retain the supplied C. HGVSc/HGVSp must remain unchanged.

## Execution and acceptance

Branch from verified remote master e906fbb, carry the common exact dependency
pins and Python USED_REF projection fix. The untouched shared baseline taken
before any production edit uses this master with functions3436921 and
formats00487b9; this original body fails there. Retain its input/checksum and
native performance/strict-MD5 evidence rather than recapturing a changed baseline.

Use the genuine merged 116 chr21 slice shared with issue #151, preserving all
39 annotations and biological bytes. Include the unchanged normalized original
and independent control with fresh actual Docker VEP 116.2 goldens/provenance.
Assert all five affected Ensembl features select USED_REF=A, their exact coding
and UTR HGVS values, the two previously correct RefSeq values, intronic fallback,
GIVEN_REF, unanchored CSQ allele, original raw alleles and complete ordered body.
Exercise plain/indexed workers 1/2, CLI and full/narrow LazyFrame projections.

Build this branch natively. Reuse #150's final performance workers 1/8 and
strict 22-autosome results only with an identical-production receipt and clear
attribution; a new production change requires new measurements. Request both
reviewers on the final head, answer findings, complete CI and hand-off without
merging. Human merges must precede downstream merge-commit re-pins.
