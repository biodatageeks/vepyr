# Issue 155: genomic reference for an MNV crossing a cache boundary

Original normalized 1:1000000 AC>GT retains all 21 annotations but previously
gave an inversion HGVS and USED_REF=AC on nine Ensembl features. The actual
genomic reference is GG. Preserve the original and separate GG>AT control.

## Ownership and source rule

All three read-only analyses are complete. Functions owns reference selection
before HGVS minimization; formats carries native BAM policy and raw alleles;
vepyr carries exact engine pins and the USED_REF projection dependency. The
common formats#259 / functions#268 / vepyr#160 implementation now passes this
original full body, so this PR supplies the distinct MNV witness.

VEP 116.2 2cb0bbe CacheDir.pm:412–415 and AnnotationType/Transcript.pm:155–162
activate transcript reference from native BAM policy. Variation 116 db7394a
TranscriptVariation.pm:448–467 selects complete mapped reference. In
TranscriptVariationAllele.pm:1474–1492 genomic reference precedes clipping;
2188–2248 clips common ends and 2260–2266 identifies a remaining substitution.
Utils/Sequence.pm:525–526 and 549–562 use that reference for inversion detection.
The actual RNA-edit exception is TVA:1510–1514.

Thus GG>GT minimizes only the HGVS view to a one-base substitution, yielding
minus-strand ENST00000304952.11:c.-28C>A. The original raw AC>GT, GIVEN_REF=AC,
full USED_REF=GG and cDNA span 97–98 remain intact. Existing inversion logic is
not itself defective, and changing cache-boundary feature selection is unwarranted.

## Execution and acceptance

Start at verified remote master e906fbb and carry the common exact production
pins/projection fix. The unchanged pre-edit shared baseline at that master,
functions3436921/formats00487b9, fails this original body. Keep its native
performance/strict-MD5 evidence and original input checksum.

Create a genuine merged 116 chr1 slice covering all 21 original feature entries
and complete transcript sequence context around the 1 Mb boundary. Existing
release-115 goldens and their short FASTA are insufficient. Preserve native BAM
policy and all biological rows; use coordinate-preserving FASTA. Copy actual
Docker VEP 116.2 original/control oracles and normalization/checksum receipts.

Assert raw alleles, full GIVEN_REF/USED_REF, the minimized UTR and retained-intron
HGVS values, untrimmed cDNA spans, non-overlap fallback and all 21 ordered entries.
Exercise plain/indexed VCF workers 1/2, CLI and full/narrow LazyFrame projections.
Build this branch natively and replay both cases against the full cache.

Only reuse #150's final workers 1/8 performance and strict 22-autosome evidence
with an identical-production receipt and explicit attribution. Any production
change needs new gates. Iterate both reviewers and CI to green, complete the
hand-off and leave merging/post-merge dependency re-pins to the user.
