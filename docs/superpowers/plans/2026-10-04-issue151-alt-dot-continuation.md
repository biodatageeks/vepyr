# Issue151: independent coverage for SNVs around consecutive ALT-dot rows

Both original test identities use the same normalized four-row input: two
mismatched-reference SNVs surrounding two ALT-dot rows. Parsing already skips
the nonvariants and retains both SNVs; the missing behavior was transcript
reference policy and independent genomic HGVSc selection.

## Ownership and rule

Three read-only repository analyses were completed before this plan. Formats
owns preservation of native BAM policy; functions owns reference selection;
vepyr carries the pins and the USED_REF projection dependency. These production
changes are supplied by formats#259, functions#268 and vepyr#160 (#150).
The complete ported campaign replay proved that both issue #151 cases now match
fresh VEP 116.2, so this PR adds independent witnesses rather than another
annotation algorithm.

VEP 116.2 2cb0bbe Parser/VCF.pm:259–266 skips ALT-dot rows and continues reading.
CacheDir.pm:412–415 enables transcript reference only from native info.bam.
AnnotationType/Transcript.pm:155–162 passes it with no_ref_check to each TV.
Variation116 db7394a TranscriptVariation.pm:448–467 requires complete spliced
mapping; TranscriptVariationAllele.pm:1474–1478 and 1508–1514 selects genomic
HGVS reference with the actual-RNA-edit exception.

## Execution and evidence

The branch starts at remote master e906fbb. The untouched baseline captured
before any shared production edit uses that same source, functions3436921,
formats00487b9 and the same merged 116 data overlay: both original issue #151
bodies FAIL. Preserve those input hashes and baseline/candidate digests.
Carry the exact reviewed common dependency pins and Python projection fix.
Use a genuine merged 116 chr21 slice containing all 39 feature entries per SNV;
include both ported identities, their unaltered inputs, fresh Docker116.2
oracles and the separate reference-consistent control. Normalize before both
engines without repairing REF.

Test plain/indexed VCF workers 1/2, CLI, full and narrow LazyFrame projections,
ordered positions 25587759 and 25587762, absence of both nonvariants, all 78 CSQs,
exonic USED_REF=T versus intronic fallback C, and full unsorted body MD5.
If production source matches #150 exactly, explicitly reference its native
workers 1/8 and strict 22-autosome gates; do not present reused measurements as
new runs. Any additional production change requires new gates. Request both
reviewers on this PR's final head, answer findings and complete CI/handoff.
Leave merging and post-merge re-pins to the user.
