# ALT-dot survivor reference selection (issue #152)

DT-61ac08b70b preserves the original normalized leading G>. row and following
C>A SNV at 21:25587762. Only the SNV is emitted, with 39 ordered merged-cache
annotations. The independent control uses T>. / T>A. Neither input is repaired.

The cache is a genuine release-116 merged chr21 slice: 41 transcripts, 382 exons,
39 translations, 3,498 variation rows and 10,354 SIFT rows. Its biological bytes
match the issue #151 fixture. Native BAM policy is separately verified from
info.txt; absent legacy Parquet metadata is not inferred from the source name.
The coordinate-preserving FASTA retains real bases 25567000–25613000 and pads
other positions with N.

`provenance.json` records actual pinned Docker VEP 116.2 commands, normalization,
input SHA-256, oracle body MD5 and native info.txt SHA-256. The original body is
`d86f96669daebe8c90a89021f44d0eab`; control is
`9e7cfb43d430613cb27c72dbe1a9fef2`. Digests remove only header lines.

The tests cover plain/indexed API output, workers 1/2, CLI and narrow/full
LazyFrame projections. Structured reference assertions precede complete body
comparison. Run `prepare.py` with DATA_VEPYR_DIR and the dependencies listed in
its docstring to regenerate the biological slice and indexed inputs; it does
not regenerate VEP goldens.
