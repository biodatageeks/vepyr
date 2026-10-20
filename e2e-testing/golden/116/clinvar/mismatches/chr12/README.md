# chr12 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr12: 161 of 211,578 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 43 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr12 run of this check on Linux (7,541 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr12 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 21 | 12 |
| `large_deletion` | 9 | 9 |
| `splice` | 12 | 8 |
| `cds_utr_boundary` | 32 | 12 |
| `start_codon` | 8 | 8 |
| `stop_codon` | 9 | 5 |
| `incomplete_terminal_codon` | 4 | 4 |
| `inframe_indel` | 37 | 6 |
| `hgvs` | 29 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (21 records)

- 5: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 12:8836307 G>GCAGGA...(71 bp) (ClinVar 4735134, ENST00000299698)
- 4: vepyr adds `stop_gained`; e.g. 12:21842398 T>TAAGAA...(125 bp) (ClinVar 2128352, ENST00000261200)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 12:21818255 T>TGAAAA...(117 bp) (ClinVar 4734525, ENST00000261200)

**large_deletion** (9 records)

- 2: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 12:102854488 CCTATT...(802 bp)>C (ClinVar 458079, ENST00000984282)
- 1: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 12:9093579 TCTATG...(1394 bp)>T (ClinVar 18173, ENST00000982748)
- 1: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 12:12717200 TCCCGG...(978 bp)>T (ClinVar 4713076, ENST00000228872)

**splice** (12 records)

- 4: vepyr adds `splice_region_variant`; e.g. 12:6110590 GCAAAGG>AGACTC...(13 bp) (ClinVar 1304220, ENST00000261405)
- 2: vepyr drops `splice_donor_variant`; e.g. 12:47993987 TAC>GAAGTA...(21 bp) (ClinVar 2446374, ENST00000337299)
- 2: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 12:49042734 GTATTA...(37 bp)>TGAAGA (ClinVar 1701198, ENST00000301067)

**cds_utr_boundary** (32 records)

- 12: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 12:6600533 CCTTT>C (ClinVar 3354519, ENST00000545942)
- 8: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 12:1854035 TCCTCGTCA>T (ClinVar 1003595, ENST00000537784)
- 7: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 12:1918461 AGCCAC...(20 bp)>A (ClinVar 957610, ENST00000586184)

**start_codon** (8 records)

- 7: vepyr adds `start_lost`; e.g. 12:6944123 TG>T (ClinVar 2859820, ENST00000229281)
- 1: vepyr drops `start_lost`; e.g. 12:49962019 TGAA>T (ClinVar 1645185, ENST00000293599)

**stop_codon** (9 records)

- 6: vepyr adds `stop_lost`; e.g. 12:49955605 C>CT (ClinVar 3574929, ENST00000199280)
- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant&stop_lost`; e.g. 12:53307490 T>TA (ClinVar 2581229, ENST00000548931)
- 1: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 12:70353912 C>CT (ClinVar 1064799, ENST00000229195)

**incomplete_terminal_codon** (4 records)

- 2: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 12:48132978 TG>T (ClinVar 3574637, ENST00000549003)
- 1: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 12:48132978 T>TG (ClinVar 4916497, ENST00000549003)
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 12:57750668 ATC>A (ClinVar 4824893, ENST00000551800)

**inframe_indel** (37 records)

- 28: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 12:4912246 ATCCT>TC (ClinVar 463866, ENST00000382545)
- 3: vepyr drops `protein_altering_variant`; e.g. 12:6075541 T>TGGGCA...(70 bp) (ClinVar 4915277, ENST00000261405)
- 3: vepyr adds `inframe_deletion`; e.g. 12:47974785 TCTCGC...(67 bp)>ACTA (ClinVar 2735205, ENST00000337299)

**hgvs** (29 records)

- 3: `HGVSc`: VEP `n.#delinsS`, vepyr `empty`; e.g. 12:23575657 TTAC>ATAT (ClinVar 3900637, ENST00000542241)
- 3: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 12:76348353 TAACA>T (ClinVar 2158383, ENST00000650064)
- 2: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 12:51913988 T>TA (ClinVar 464763, ENST00001027687)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
