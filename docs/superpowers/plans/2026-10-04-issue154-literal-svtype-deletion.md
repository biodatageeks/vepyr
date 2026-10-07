# Issue 154: a literal deletion carrying SVTYPE metadata

Normalized 21:25587759 AC>A;SVTYPE=DEL already follows the literal-deletion path,
but five Ensembl USED_REF values previously disagreed with VEP 116.2. Preserve
the original alleles and INFO; a separate TA>T;SVTYPE=DEL control matches GRCh38.

## Ownership and source rule

Three completed read-only analyses identify the common reference-policy fix in
formats#259 / functions#268 / vepyr#160. Formats transports the native BAM policy
and raw VCF fields; functions selects reference alleles; vepyr transports the
pins and USED_REF projection dependency. The original now passes full campaign
replay, so this separate issue needs an independent fixture/regression PR.

VEP 116.2 2cb0bbe Parser/VCF.pm:237–244 only considers structural dispatch when
the joined alleles are not all ACGT. Thus SVTYPE alone does not reinterpret AC>A.
Lines 327–335 strip the shared anchor. CacheDir.pm:412–415 and
AnnotationType/Transcript.pm:155–162 carry native BAM transcript-reference policy.
Variation 116 db7394a TranscriptVariation.pm:448–467 selects the fully mapped
deleted interval and preserves input-strand orientation. OutputFactory.pm:
1774–1778 retains deletion shift semantics. No new SV classification branch is
needed. GIVEN_REF=C, Allele=-, mapped USED_REF=A and intronic fallback C must
coexist with unchanged literal VCF AC>A and SVTYPE=DEL.

## Execution and acceptance

Start at verified remote master e906fbb with the common exact pins/projection
fix. The untouched pre-edit baseline at this master (functions3436921 and
formats00487b9) fails the original body. Retain that baseline and input checksum.

Use the genuine merged 116 chr21 biological slice and all 39 ordered annotations;
include this issue's original and independent control, fresh actual Docker VEP
116.2 goldens and command/checksum provenance. Normalize before either engine
without repairing REF. Test raw INFO/header preservation, the five Ensembl and
two RefSeq mapped references, intronic fallback, unchanged HGVS/consequences,
unanchored CSQ allele and complete ordered VCF body. Cover plain/indexed workers
1/2, CLI and full/narrow LazyFrame projections including input SVTYPE.

Build this branch natively and replay original/control against the full cache.
Explicitly attribute shared #150 workers 1/8 performance and strict 22-autosome
evidence only if an identical-production receipt proves reuse is valid. New
production edits require new gates. Request Codex and Claude on the final head,
resolve findings, complete CI and hand-off. Leave merging and subsequent
merge-commit dependency re-pins to the user.
