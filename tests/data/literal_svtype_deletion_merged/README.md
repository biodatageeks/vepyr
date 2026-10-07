# Literal deletion with SVTYPE metadata (issue #154)

DT-cce7b4c742 retains normalized 21:25587759 AC>A;SVTYPE=DEL. The literal alleles
remain a sequence deletion: CSQ Allele=- and GIVEN_REF=C. Five Ensembl and two
RefSeq features use reference A; 32 annotations retain C. The TA>T;SVTYPE=DEL
control is independent and reference-consistent. Raw alleles and INFO survive.

The genuine merged release-116 chr21 slice is byte-identical to issue #151's
biological data: 41 transcripts, 382 exons, 39 translations, 3,498 variation
rows, and 10,354 SIFT rows. Native info.txt separately verifies BAM policy.
The FASTA preserves coordinates and real bases 25567000–25613000; other bases
are padded with N.

`provenance.json` records actual Docker VEP 116.2 commands, normalization without
REF repair, input SHA-256, body MD5 and native metadata SHA-256. Original body
MD5 is `22cc6f5b7b2a791ef079b4772e03dc59`; control is
`10b6ad54c8da371600f6d67bbb36f161`. Full bodies retain record/CSQ order.

Tests cover raw SVTYPE/header preservation, exact reference/HGVS fields and all
39 annotations in plain/indexed API, workers 1/2, CLI and full/narrow LazyFrame
projections. `prepare.py` regenerates biological data/indexes from DATA_VEPYR_DIR
with its listed dependencies; regenerate goldens with the recorded Docker command.
