# MNV reference across a cache-region boundary (issue #155)

DT-dff61aedd9 preserves normalized 1:1000000 AC>GT spanning the 1 Mb boundary.
Genomic GG produces a one-base HGVSc substitution on the minus strand, while
GIVEN_REF=AC, USED_REF=GG, raw AC>GT and the full cDNA spans remain unchanged.
The GG>AT input is a separate reference-consistent control, not a repair.

The fixture is sliced from the genuine merged release-116 chr1 cache, with all
21 expected feature entries, their complete transcript context and supporting
entities. Native info.txt separately verifies BAM policy. `fasta-region.json`
records the actual genomic interval retained, spanning every selected transcript
plus 2 kb flanks. Other bases are N padding; whole-chromosome coordinates remain.

`provenance.json` contains actual pinned Docker VEP 116.2 commands, normalization
without REF repair, input SHA-256, oracle body MD5 and native metadata SHA-256.
Original body MD5 is `1c36ddf3b1f811ba6d52745bb7d7bf36`; control is
`8008f5c89ebb6f4f1b5e9dc003bda5b4`. No body sorting or field filtering is applied.

Tests assert UTR/retained-intron HGVS minimization, full reference alleles,
untrimmed cDNA spans, non-overlap fallback, regulatory output, all 21 ordered
CSQs and exact body bytes. They cover API plain/indexed workers 1/2, CLI and
full/narrow LazyFrame projections. `prepare.py` regenerates biological data and
indexes from DATA_VEPYR_DIR with its listed dependencies; goldens require Docker.
