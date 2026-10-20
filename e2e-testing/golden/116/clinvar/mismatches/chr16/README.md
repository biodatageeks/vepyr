# chr16 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr16: 356 of 237,223 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 64 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr16 run of this check on Linux (9,341 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr16 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 114 | 1 |
| `allele_with_n` | 28 | 12 |
| `large_deletion` | 11 | 10 |
| `splice` | 13 | 7 |
| `cds_utr_boundary` | 72 | 12 |
| `start_codon` | 8 | 8 |
| `stop_codon` | 8 | 2 |
| `incomplete_terminal_codon` | 10 | 7 |
| `inframe_indel` | 53 | 9 |
| `hgvs` | 39 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (114 records)

- 114: vepyr decodes a percent-encoded INFO value; e.g. 16:1706402 CG>C (ClinVar 632561, -)

**allele_with_n** (28 records)

- 9: vepyr adds `stop_gained`; e.g. 16:2054316 C>CAGAGC...(119 bp) (ClinVar 1385761, ENST00000219476)
- 6: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 16:3601089 A>ACTGGT...(117 bp) (ClinVar 4729462, ENST00000294008)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 16:1583389 G>GAAAAG...(112 bp) (ClinVar 4718103, ENST00000397417)

**large_deletion** (11 records)

- 3: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 16:89544771 AGGTCA...(5839 bp)>A (ClinVar 3242465, ENST00000268704)
- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 16:1772492 CGCGCC...(863 bp)>C (ClinVar 2837647, ENST00000177742)
- 2: vepyr drops `start_lost`; e.g. 16:1772492 CGCGCC...(863 bp)>C (ClinVar 2837647, ENST00000177742)

**splice** (13 records)

- 7: vepyr adds `splice_region_variant`; e.g. 16:172929 CC>GCCT (ClinVar 4818634, ENST00000723699)
- 1: VEP has `splice_region_variant`, vepyr `splice_acceptor_variant`; e.g. 16:2055399 CAGCTG>TAGCTA (ClinVar 3578781, ENST00000219476)
- 1: VEP has `intron_variant&splice_donor_region_variant&splice_donor_variant`, vepyr `splice_region_variant`; e.g. 16:5072070 GG>AGGTCT (ClinVar 515090, ENST00001096215)

**cds_utr_boundary** (72 records)

- 36: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 16:578403 CTG>C (ClinVar 520669, ENST00000420990)
- 23: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 16:173428 GGCGGC...(92 bp)>TACACCGCAGG (ClinVar 4818661, ENST00000484216)
- 3: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 16:1351957 TGCGGCGCGATG>T (ClinVar 1043186, ENST00000529110)

**start_codon** (8 records)

- 6: vepyr adds `start_lost`; e.g. 16:1351967 T>TGGCG (ClinVar 2703201, ENST00000204679)
- 1: vepyr adds `start_retained_variant`; e.g. 16:1351967 T>TGGCG (ClinVar 2703201, ENST00000204679)
- 1: vepyr drops `start_lost`; e.g. 16:1351967 TGGC>T (ClinVar 555097, ENST00000204679)

**stop_codon** (8 records)

- 7: vepyr adds `stop_lost`; e.g. 16:5084878 A>AT (ClinVar 2429274, ENST00000262374)
- 1: VEP has `inframe_insertion&stop_retained_variant`, vepyr `stop_lost`; e.g. 16:16157716 C>CAGGTC...(14 bp) (ClinVar 433493, ENST00001008033)

**incomplete_terminal_codon** (10 records)

- 3: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 16:2097727 CCT>C (ClinVar 4755552, ENST00000567946)
- 3: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 16:2097728 CT>C (ClinVar 1177416, ENST00000567946)
- 2: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 16:15724247 G>GC (ClinVar 3728366, ENST00000572756)

**inframe_indel** (53 records)

- 48: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 16:680677 GCGCGAT>CAGC (ClinVar 4875530, ENST00000219548)
- 2: VEP has `frameshift_variant`, vepyr `coding_sequence_variant`; e.g. 16:176985 ACGGCT...(249 bp)>A (ClinVar 4814378, ENST00001097581)
- 1: vepyr drops `missense_variant`; e.g. 16:2047742 GC>AT (ClinVar 1776749, ENST00000561841)

**hgvs** (39 records)

- 6: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 16:173117 CCCCGC...(22 bp)>GG (ClinVar 4818694, ENST00000723702)
- 5: `CDS_position`: VEP `#-?`, vepyr `empty`; e.g. 16:15724284 AG>CA (ClinVar 928284, ENST00001128233)
- 5: `Protein_position`: VEP `#-?`, vepyr `empty`; e.g. 16:15724284 AG>CA (ClinVar 928284, ENST00001128233)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
