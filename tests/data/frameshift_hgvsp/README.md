# Shifted insertion HGVSp regression (#149)

The unchanged input comes from the two VEP 116.2 ported TAAA insertion cases:
`report_the_intronic_consequence_for_the_taaa_insertion_fe6e88` and
`report_the_unanchored_taaa_insertion_allele_in_csq_f43eb8`.
It was normalized with `bcftools norm -m -both --no-version` (bcftools 1.23).
Input SHA256: `65b85070cb3c7080d181c2c74eead898b81f0f6844959364a10d82b03c4bb6c3`.

Fresh Ensembl VEP 116.2 (`--everything --merged`, release-116 GRCh38 merged
cache and primary-assembly FASTA) gives
`ENSP00000780046.1:p.Leu262PhefsTer81` for `ENST00001110241`.
The whole merged-cache output body MD5 is `46364ad56f27f3b15bb0a484c3e0f63e`.
Docker image: `ensemblorg/ensembl-vep@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da`.

The Python regression reuses the existing `ref_genome_mismatch` cache and FASTA.
That Ensembl slice includes the real affected transcript and returns 33 entries,
whereas the full merged oracle has 38. It therefore asserts the exact affected
feature and the fields that already matched, rather than claiming whole-body
parity for a different cache. Full-cache replays of both original ports remain
part of the fix verification.
