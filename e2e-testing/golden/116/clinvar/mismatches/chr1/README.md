# chr1 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr1: 383 of 414,715 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 68 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr1 run of this check on Linux (15,028 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr1 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 131 | 1 |
| `allele_with_n` | 34 | 12 |
| `used_ref` | 2 | 2 |
| `large_deletion` | 16 | 12 |
| `splice` | 20 | 8 |
| `cds_utr_boundary` | 46 | 12 |
| `start_codon` | 13 | 12 |
| `stop_codon` | 16 | 9 |
| `incomplete_terminal_codon` | 4 | 3 |
| `inframe_indel` | 69 | 5 |
| `hgvs` | 32 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (131 records)

- 131: vepyr decodes a percent-encoded INFO value; e.g. 1:11989273 CAAG>C (ClinVar 30734, -)

**allele_with_n** (34 records)

- 11: vepyr adds `stop_gained`; e.g. 1:19239959 T>TCCACT...(152 bp) (ClinVar 1429859, ENST00000375199)
- 9: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 1:74336096 A>AAGGAT...(123 bp) (ClinVar 2115549, ENST00000326637)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 1:12011474 C>CAAAAG...(114 bp) (ClinVar 4723528, ENST00000235329)

**used_ref** (2 records)

- 1: `Codons`: VEP `tcC/tcA`, vepyr `tcG/tcA`; e.g. 1:151767299 G>A (ClinVar 4517684, NM_001134939.1)
- 1: `USED_REF`: VEP `C`, vepyr `G`; e.g. 1:151767299 G>A (ClinVar 4517684, NM_001134939.1)
- 1: `HGVSc`: VEP `c.#C>A`, vepyr `c.#G>A`; e.g. 1:151767299 G>A (ClinVar 4517684, NM_001134939.1)

**large_deletion** (16 records)

- 2: `HGVSc` differs in its numbers only, e.g. VEP `c.2447-4_2667del`, vepyr `c.2447-4_2671del`; e.g. 1:16567346 CTGAGC...(1895 bp)>C (ClinVar 4289650, NM_001405672.2)
- 2: VEP has `inframe_deletion`, vepyr `coding_sequence_variant`; e.g. 1:21852127 TGTGAC...(832 bp)>T (ClinVar 4726287, ENST00000374695)
- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 1:33008227 AGCACC...(5039 bp)>A (ClinVar 18250, ENST00000354858)

**splice** (20 records)

- 8: vepyr adds `splice_region_variant`; e.g. 1:1516013 TTATGCC>CCATGTCA (ClinVar 3235910, ENST00000339113)
- 2: VEP has `splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 1:21573653 CGTCCT>GGTCCC...(13 bp) (ClinVar 3580099, ENST00000374832)
- 2: VEP has `splice_donor_region_variant&splice_donor_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 1:33013198 AGTCTT>TGAGTT...(26 bp) (ClinVar 3279821, ENST00000373449)

**cds_utr_boundary** (46 records)

- 15: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 1:1804521 TCCCAG...(30 bp)>T (ClinVar 2742218, ENST00000437146)
- 11: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 1:1232198 GTGCGC...(88 bp)>G (ClinVar 3760089, ENST00000379198)
- 10: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 1:6449046 GCTGGC...(22 bp)>T (ClinVar 1331614, ENST00000461727)

**start_codon** (13 records)

- 13: vepyr adds `start_lost`; e.g. 1:17053947 CCTGCA...(73 bp)>C (ClinVar 438410, ENST00000485515)
- 5: vepyr adds `start_retained_variant`; e.g. 1:21560681 T>TG (ClinVar 4086843, ENST00000539907)

**stop_codon** (16 records)

- 5: vepyr adds `stop_lost`; e.g. 1:62603986 C>CT (ClinVar 2525900, ENST00000987786)
- 4: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 1:2306763 T>TAGATTCCGTGC (ClinVar 1044180, ENST00000378536)
- 3: VEP has `missense_variant`, vepyr `stop_lost`; e.g. 1:25812789 T>C (ClinVar 3629721, ENST00000354177)

**incomplete_terminal_codon** (4 records)

- 2: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 1:153818824 CTG>C (ClinVar 817498, ENST00000634401)
- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 1:7938193 C>CT (ClinVar 2988687, ENST00000674210)
- 1: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 1:11992592 A>AG (ClinVar 2853203, ENST00000675781)

**inframe_indel** (69 records)

- 51: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 1:1054900 CGTG>T (ClinVar 3338062, ENST00000379370)
- 8: vepyr drops `protein_altering_variant`; e.g. 1:26773661 GGG>CTAGGA (ClinVar 451207, ENST00000324856)
- 7: vepyr adds `inframe_deletion`; e.g. 1:26779203 CTAGGT...(19 bp)>T (ClinVar 4530202, ENST00000324856)

**hgvs** (32 records)

- 4: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 1:17018881 T>TGC (ClinVar 3904598, ENST00000375499)
- 2: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 1:42766958 GCGCGT...(14 bp)>G (ClinVar 1526177, ENST00000236040)
- 1: `HGVSp`: VEP `p.MetAlaProAlaAlaAla#_?#`, vepyr `p.Met#?`; e.g. 1:2412483 CTGGCG...(19 bp)>C (ClinVar 4895095, ENST00000288774)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
