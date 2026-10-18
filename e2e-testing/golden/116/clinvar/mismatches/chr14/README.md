# chr14 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr14: 75 of 145,471 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 21 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr14 run of this check on Linux (5,409 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr14 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 14 | 12 |
| `large_deletion` | 4 | 3 |
| `splice` | 10 | 6 |
| `cds_utr_boundary` | 15 | 6 |
| `start_codon` | 3 | 3 |
| `stop_codon` | 1 | 1 |
| `incomplete_terminal_codon` | 2 | 2 |
| `inframe_indel` | 17 | 3 |
| `hgvs` | 9 | 9 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (14 records)

- 6: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 14:21400395 T>TAAAGA...(114 bp) (ClinVar 2014575, ENST00000430710)
- 3: vepyr adds `stop_gained`; e.g. 14:21307767 T>TAAGAA...(111 bp) (ClinVar 2954456, ENST00000400017)
- 2: VEP has `coding_sequence_variant`, vepyr `inframe_insertion`; e.g. 14:35011659 A>AGTAAG...(91 bp) (ClinVar 1993779, ENST00000216774)

**large_deletion** (4 records)

- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 14:90033188 TGCCTC...(670 bp)>T (ClinVar 586806, ENST00000556063)
- 1: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 14:45178953 CTTTGG...(2555 bp)>C (ClinVar 1599, ENST00000555484)
- 1: `HGVSc`: VEP `n.#-#_#delinsG`, vepyr `empty`; e.g. 14:95101215 GCTGCA...(2918 bp)>C (ClinVar 933114, ENST00000696926)

**splice** (10 records)

- 5: vepyr adds `splice_region_variant`; e.g. 14:21361079 TCCTT>ACCTC (ClinVar 3359561, ENST00000216297)
- 2: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 14:23415461 ACAGGTCAGCA>TCAGATCCGAC (ClinVar 3876498, ENST00000557368)
- 1: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant&splice_donor_variant`; e.g. 14:21427863 ACTCACTTG>CCTCACTTC (ClinVar 1800933, ENST00000430710)

**cds_utr_boundary** (15 records)

- 7: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 14:22773628 CCGTTGCTTGGG>C (ClinVar 834451, ENST00000556350)
- 4: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 14:21321371 TTCTGC...(15 bp)>T (ClinVar 3576490, ENST00000554303)
- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 14:104729867 GTCGTG...(27 bp)>G (ClinVar 2101148, ENST00000553540)

**start_codon** (3 records)

- 2: vepyr adds `start_lost`; e.g. 14:96833473 T>TG (ClinVar 3576914, ENST00000216639)
- 1: vepyr drops `start_lost`; e.g. 14:23321471 TGGC>T (ClinVar 2434548, ENST00000216727)

**stop_codon** (1 records)

- 1 of the 1 records differ on one transcript only, `ENST00001034067`.
- 1: vepyr adds `stop_lost`; e.g. 14:103574141 C>CT (ClinVar 1967635, ENST00001034067)

**incomplete_terminal_codon** (2 records)

- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 14:22813049 AAAGC>A (ClinVar 2678986, ENST00000557629)
- 1: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 14:45176975 AG>A (ClinVar 1324388, ENST00000696641)

**inframe_indel** (17 records)

- 14: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 14:23390070 ATCTGCT>GAGG (ClinVar 3222334, ENST00000405093)
- 2: vepyr drops `protein_altering_variant`; e.g. 14:36663511 ATC>TACCGA...(24 bp) (ClinVar 13778, ENST00000361487)
- 1: vepyr drops `intron_variant`; e.g. 14:22775936 TCT>AATGG (ClinVar 646821, ENST00000285850)

**hgvs** (9 records)

- 1: `HGVSp`: VEP `p.Gly#GlyfsTer?`, vepyr `p.Val#del`; e.g. 14:22813269 ACACG>A (ClinVar 1376330, ENST00000554741)
- 1: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 14:24258356 TTC>T (ClinVar 1725646, ENST00000544573)
- 1: `HGVSp`: VEP `p.Leu#LeufsTer?`, vepyr `p.Ter#del`; e.g. 14:73171047 TG>T (ClinVar 98013, ENST00000560005)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
