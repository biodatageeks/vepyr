# chrX mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chrX: 248 of 198,574 records differ between Ensembl VEP and
vepyr. Both files hold the same records in the same order, and no record
differs only in the order of its CSQ entries.

The layout, the class rules and the reproducing steps are those of
[`../chr15/`](../chr15/README.md), which also describes each class in detail.
The VEP reference is the full chrX run of this check on Linux (6,420 s for one
process). This page lists what chrX adds.

| Class | Records on chrX | In the fixture | What differs |
|---|---:|---:|---|
| `allele_with_n` | 22 | 12 | `HGVSc`, `HGVSp`, `Amino_acids`, `HGVS_OFFSET`, `Consequence` |
| `large_deletion` | 47 | 12 | `Consequence`, `HGVSc`, `Codons`, `Amino_acids`, `HGVSp` |
| `splice` | 16 | 7 | `Consequence` |
| `cds_utr_boundary` | 43 | 12 | `Consequence`, `IMPACT` |
| `start_codon` | 8 | 8 | `Consequence`, `HGVSp` |
| `stop_codon` | 9 | 3 | `Consequence` |
| `incomplete_terminal_codon` | 10 | 7 | `Consequence`, `HGVSp` |
| `inframe_indel` | 68 | 4 | `Consequence`, `IMPACT` |
| `hgvs` | 25 | 12 | `HGVSp`, `HGVSc`, `Amino_acids`, positions |

There is no `info_percent_encoding` and no `used_ref` record on chrX. The
`large_deletion` files are in Git LFS. All nine fixtures were checked:
`vepyr annotate` on each `input.vcf` writes the records of `vepyr.vcf` byte for
byte.

## What chrX adds

**large_deletion.** chrX has 47 deletions of 556 to 6,415 bp, most of them
across exons. The most common difference, in 19 records, is
`stop_retained_variant&3_prime_UTR_variant` in VEP against
`stop_lost&3_prime_UTR_variant` in vepyr, e.g. X:101350768 (6,066 bp, ClinVar
836009, ENST00000470329). In 7 records VEP has `start_lost` and vepyr
`coding_sequence_variant`, e.g. X:154030586 (3,900 bp, ClinVar 940739,
XM_047442121.1); in 4 the other way round. In 6 VEP has `stop_lost` and in 4
`inframe_deletion` where vepyr has `coding_sequence_variant`. Five records
differ in `HGVSc` only: VEP writes a `del` or `delins` over the deleted
region, vepyr leaves it empty.

**inframe_indel.** 61 of the 68 records are `protein_altering_variant` in VEP
and `inframe_deletion` in vepyr. X:153868677, a 181 bp deletion across both
splice sites of an exon (ClinVar 4823493), is `frameshift_variant` in VEP and
`coding_sequence_variant` in vepyr.

**cds_utr_boundary.** As on chr15: 14 records go from `coding_sequence_variant`
to `stop_lost`, 13 to `protein_altering_variant`, 3 to `start_lost`, and 5 from
`stop_retained_variant` to `stop_lost`. In 4 more, VEP has
`coding_sequence_variant&incomplete_terminal_codon_variant` and vepyr
`stop_lost`.

**stop_codon.** In 5 records vepyr adds `stop_lost`, in 3 it turns
`coding_sequence_variant` into `stop_retained_variant`. X:101348537 (ClinVar
11326) is `stop_lost&splice_region_variant` in VEP and
`protein_altering_variant&splice_region_variant` in vepyr.

**start_codon.** In 6 records vepyr adds `start_lost`, in 2 it drops it.

**hgvs.** Besides the patterns of chr15:

- one-base insertions at the stop codon: VEP writes `HGVSp` `p.Ter104%3D`
  (`=` percent-encoded), vepyr leaves it empty, e.g. X:23392681 T>TA (ClinVar
  3376263, ENST00000456522) and X:49075651 C>CT (ClinVar 2626856).
- X:22226503 T>A (ClinVar 1037395), ENST00000682888: VEP `p.Ter172Ile` and
  `Amino_acids` `X/I`, vepyr `p.Ter172%3D` and `X`, with the same
  `Consequence`.
