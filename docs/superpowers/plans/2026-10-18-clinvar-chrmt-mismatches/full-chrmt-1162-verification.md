# Full chrMT verification against Ensembl VEP 116.2

Run on 2026-10-18 from the `e2e/clinvar-parity` baseline, using the full
preprocessed ClinVar 20261011 MT slice. Reproduction script:
[`reproduce_full_chrmt.sh`](reproduce_full_chrmt.sh).

## Inputs and tool identities

- ClinVar source: `clinvar_20261011.vcf.gz`, MD5
  `0f3a1a5e84252386033d5fcab1bf1f6a`, SHA-256
  `05d9481dd7217cc4df15803362d4a4c9eae5875b27b1750867d6bfd2643b3d10`.
- Preprocessing: `bcftools view -e 'ALT="."' -r MT`, then
  `bcftools norm -m -both`; 3,134 records, body MD5
  `387468a844114c3f32b36a925c94d9ad`.
- FASTA SHA-256:
  `1e74081a49ceb9739cc14c812fbb8b3db978eb80ba8e5350beb80d8ad8dfef3b`.
- VEP image:
  `ensemblorg/ensembl-vep:release_116.2@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da`.
- VEP header: `v116.2`, API/cache 116, GRCh38.p14, core `0d85231`, variation
  `db7394a`.
- Raw cache: release 116 merged; published archive BSD sum `26554 29618744`.
- vepyr: PyPI `0.10.0`, merged cache revision
  `5b83dd8d249106c6cc3f1c04c522b4bec716cc97`.

Generated VCFs are in
`$DATA_VEPYR_DIR/output/116/clinvar_1162_chrMT_reproduced/` on the run
machine; they are intentionally not checked into Git.

## Results

Both annotators emitted all 3,134 input records. No records were missing on
either side. The VEP 116.2 output body MD5 is
`d41e25a239331e375129dde11f0f641e` (strict) and
`0f1a9a2f84ca71e78ac9d3e2a41711c7` (canonical). The vepyr 0.10.0 body MD5 is
`c50250977ffd4a597fac7f26d1f7b88c` (strict) and
`4495590e95702c0403ca9d126aee49ba` (canonical).

The VEP 116.2 versus vepyr comparison has 2,327 differing records:

| Result | Records |
|---|---:|
| Identical | 807 |
| Same CSQ entries, order differs | 2,030 |
| CSQ content differs | 297 |

The 297 content differences correspond to 294 codon-table records and four
HGVS records, with the large deletion shared by both classes. The mismatch key
set (position and ClinVar ID) is identical to the VEP 116.0 ledger in
`e2e-testing/golden/116/clinvar/mismatches/chrMT/records.tsv`.

VEP 116.2 and VEP 116.0 are not fully byte-identical across chrMT: only the
record at MT:9204 differs. VEP 116.2 uses `p.Ter227LeuextTer?` where 116.0 uses
`p.Ter227LeufsTer?` for both the RefSeq ATP6 and Ensembl MT-ATP6 consequences.
That record still differs from vepyr 0.10.0 and remains in the mismatch set;
the 116.2 comparison also reports `HGVSp` as differing there. The old ledger is
kept as evidence of the original 116.0 run and is not overwritten.

The fixture VEP 116.2 outputs remain body-identical to their stored VEP 116.0
fixture outputs. Thus the full-run version change is localized to the MT:9204
record, outside the sampled fixtures, and does not change the mismatch key set
or the three root-cause totals.
