# chr17 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr17: 1,167 of 277,114 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 132 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr17 run of this check on Linux (13,349 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr17 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 848 | 3 |
| `allele_with_n` | 72 | 12 |
| `large_deletion` | 31 | 12 |
| `splice` | 22 | 7 |
| `cds_utr_boundary` | 59 | 12 |
| `start_codon` | 5 | 5 |
| `stop_codon` | 6 | 4 |
| `incomplete_terminal_codon` | 4 | 4 |
| `inframe_indel` | 75 | 7 |
| `hgvs` | 45 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (848 records)

- 848: vepyr decodes a percent-encoded INFO value; e.g. 17:41583413 A>C (ClinVar 4057201, -)

**allele_with_n** (72 records)

- 21: vepyr adds `stop_gained`; e.g. 17:31156107 T>TAAAGA...(113 bp) (ClinVar 2809169, ENST00000356175)
- 16: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 17:7673733 T>TGAGGC...(131 bp) (ClinVar 2808558, ENST00000269305)
- 13: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 17:31181750 C>CTATAA...(154 bp) (ClinVar 4734749, ENST00000356175)

**large_deletion** (31 records)

- 4: `HGVSc`: VEP `n.#_#-#delinsS`, vepyr `empty`; e.g. 17:31224599 AGGAAT...(3782 bp)>TAGC (ClinVar 217068, ENST00000493220)
- 3: VEP has `start_lost`, vepyr `coding_sequence_variant`; e.g. 17:8002614 TGTACC...(4915 bp)>T (ClinVar 4538535, ENST00000254854)
- 3: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 17:16216572 GGATCA...(805 bp)>G (ClinVar 2093342, ENST00000225609)

**splice** (22 records)

- 7: vepyr adds `splice_region_variant`; e.g. 17:8016001 CGAGGCCG>AAGGTGAGGTAC (ClinVar 2069382, ENST00000763246)
- 5: VEP has `intron_variant&splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 17:43063946 CAGC>GCAGAATCAA (ClinVar 267576, ENST00000352993)
- 4: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 17:7673526 ACTTAG>GGTGAAA (ClinVar 3232070, ENST00000269305)

**cds_utr_boundary** (59 records)

- 18: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 17:7220786 CAGTTTCT>C (ClinVar 557072, ENST00000581378)
- 16: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 17:1770026 ACGGCCCTCT>A (ClinVar 4689724, ENST00000571149)
- 6: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 17:15260675 AGCACC...(213 bp)>A (ClinVar 4750163, NM_153322.3)

**start_codon** (5 records)

- 3: vepyr adds `start_lost`; e.g. 17:1772035 T>TG (ClinVar 2832239, ENST00000573763)
- 2: vepyr adds `start_retained_variant`; e.g. 17:11598500 TGCGGC...(29 bp)>T (ClinVar 2957745, ENST00000262442)
- 1: vepyr drops `start_lost`; e.g. 17:82032736 ATGG>A (ClinVar 1687595, ENST00000580965)

**stop_codon** (6 records)

- 2: vepyr adds `stop_lost`; e.g. 17:7220608 C>CT (ClinVar 203588, ENST00000988291)
- 2: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 17:7224063 CT>C (ClinVar 2635422, ENST00001020570)
- 1: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 17:41818389 A>AAGGGCGGC (ClinVar 2858998, ENST00000489591)

**incomplete_terminal_codon** (4 records)

- 2: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 17:7221019 GC>G (ClinVar 1210348, ENST00000584103)
- 1: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 17:7910893 C>CCGAA (ClinVar 549749, ENST00000573936)
- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 17:78997394 A>AG (ClinVar 31014, ENST00000588075)

**inframe_indel** (75 records)

- 55: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 17:1651696 GTGAGT...(17 bp)>CATGGT...(14 bp) (ClinVar 3249953, ENST00000304992)
- 13: vepyr drops `protein_altering_variant`; e.g. 17:2676561 TATTA>ATCTTAAT (ClinVar 419614, ENST00000397195)
- 4: vepyr adds `inframe_deletion`; e.g. 17:6694191 GACA>T (ClinVar 4086263, ENST00000293800)

**hgvs** (45 records)

- 5: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 17:7675108 GTGCTG...(26 bp)>G (ClinVar 1397281, ENST00000610623)
- 5: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 17:17221584 TCCAGG>CGGTCCGAGC (ClinVar 3381155, ENST00000480316)
- 4: `HGVSc`: VEP `n.#delinsS`, vepyr `empty`; e.g. 17:7675119 GC>AA (ClinVar 2687643, ENST00000574684)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
