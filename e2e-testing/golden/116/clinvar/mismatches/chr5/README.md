# chr5 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr5: 177 of 222,343 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 58 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr5 run of this check on Linux (8,597 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr5 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 41 | 12 |
| `large_deletion` | 8 | 8 |
| `splice` | 15 | 8 |
| `cds_utr_boundary` | 30 | 12 |
| `start_codon` | 7 | 7 |
| `stop_codon` | 11 | 4 |
| `incomplete_terminal_codon` | 2 | 2 |
| `inframe_indel` | 44 | 5 |
| `hgvs` | 19 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (41 records)

- 19: vepyr adds `stop_gained`; e.g. 5:13716603 T>TCAACA...(120 bp) (ClinVar 4720387, ENST00000265104)
- 5: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 5:6632602 T>TTGTAA...(128 bp) (ClinVar 4712852, ENST00000264670)
- 4: VEP has `coding_sequence_variant`, vepyr `inframe_insertion`; e.g. 5:13721066 A>ACGGGT...(118 bp) (ClinVar 2808650, ENST00000265104)

**large_deletion** (8 records)

- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 5:140113537 CGCATG...(908 bp)>GAGGTGGG (ClinVar 2861228, ENST00000331327)
- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 5:140855984 ATGTCA...(2655 bp)>A (ClinVar 975862, ENST00000562220)
- 1: VEP has `frameshift_variant`, vepyr `coding_sequence_variant`; e.g. 5:233472 TGCAGG...(1864 bp)>T (ClinVar 545722, ENST00000952712)

**splice** (15 records)

- 4: VEP has `intron_variant&splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 5:13729556 ACC>CTTATACACA (ClinVar 5010921, ENST00000265104)
- 4: vepyr adds `splice_region_variant`; e.g. 5:74759431 TGAAAA>CGAAAG (ClinVar 421611, ENST00000296805)
- 3: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 5:80792836 GATTTA...(22 bp)>TATTTA...(14 bp) (ClinVar 3398683, ENST00000265081)

**cds_utr_boundary** (30 records)

- 13: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 5:44388457 CTCCTT...(34 bp)>C (ClinVar 3369362, ENST00000513107)
- 8: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 5:35873527 CCAACCGGCAG>C (ClinVar 2734715, ENST00000505093)
- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 5:228330 CAGGGT...(80 bp)>C (ClinVar 2067609, ENST00001127770)

**start_codon** (7 records)

- 5: vepyr adds `start_lost`; e.g. 5:112754892 T>TG (ClinVar 1801580, ENST00000257430)
- 2: vepyr drops `start_lost`; e.g. 5:7870796 TGAG>T (ClinVar 4815063, ENST00000440940)
- 1: vepyr adds `start_retained_variant`; e.g. 5:112754892 T>TG (ClinVar 1801580, ENST00000257430)

**stop_codon** (11 records)

- 4: vepyr adds `stop_lost`; e.g. 5:112801298 A>ATG (ClinVar 827089, ENST00001081087)
- 3: VEP has `frameshift_variant&stop_gained`, vepyr `inframe_insertion&stop_retained_variant`; e.g. 5:35875588 G>GTGAGTGTT (ClinVar 1581844, ENST00000506850)
- 3: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 5:132394188 G>GAAACACAGAA (ClinVar 203937, ENST00000415928)

**incomplete_terminal_codon** (2 records)

- 1: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 5:90708908 AG>A (ClinVar 4532007, ENST00000640403)
- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 5:134606988 C>CGTAA (ClinVar 2926, ENST00000509730)

**inframe_indel** (44 records)

- 30: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 5:36976194 TCCAGC...(18 bp)>GTTCTG (ClinVar 4884136, ENST00000282516)
- 11: vepyr drops `protein_altering_variant`; e.g. 5:13916398 TACTATA>AGTATC...(46 bp) (ClinVar 2760877, ENST00000265104)
- 1: vepyr drops `missense_variant`; e.g. 5:35860925 GC>TT (ClinVar 640639, ENST00000515665)

**hgvs** (19 records)

- 3: `HGVSc`: VEP `n.#delinsS`, vepyr `empty`; e.g. 5:7897100 TA>AAGATC (ClinVar 2676860, ENST00000508354)
- 2: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 5:224379 T>TA (ClinVar 545901, ENST00000509632)
- 2: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 5:44388652 AGGCAC...(31 bp)>CTGAAA...(18 bp) (ClinVar 3254622, ENST00000264664)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
