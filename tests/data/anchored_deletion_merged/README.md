# Anchored deletion reference selection (issue #153)

DT-2e50dd94b9 preserves normalized 21:25587759 AC>A. The annotation removes the
anchor (GIVEN_REF=C, Allele=-), but raw output remains AC>A. Five Ensembl and two
RefSeq mappings use deleted reference A; 32 remaining annotations retain C.
The independent TA>T control is reference-consistent. Both retain all 39 CSQs.

This is the genuine merged 116 chr21 biological slice used for issue #151:
41 transcripts, 382 exons, 39 translations, 3,498 variation rows and 10,354 SIFT
rows. Native info.txt separately proves BAM policy. The coordinate-preserving
FASTA retains real bases 25567000–25613000 and pads other positions with N.

Actual pinned Docker VEP 116.2 commands and normalization/input/body digests are
in `provenance.json`. Original body MD5 is `112babcbb30898bed613cdf4fa20049c`;
control is `f782a096f680fc27e9174b5d184ed4f6`. No REF repair or output sorting
is applied. Tests cover ordered full bodies, exact feature HGVS/reference fields,
API plain/indexed workers 1/2, CLI and narrow/full LazyFrame projections.

`prepare.py` regenerates the biological slice/indexes from DATA_VEPYR_DIR with
the dependencies named in its docstring; goldens require the recorded Docker run.
