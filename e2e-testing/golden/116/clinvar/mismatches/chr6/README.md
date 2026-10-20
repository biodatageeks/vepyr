# chr6 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr6: 134 of 206,599 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 48 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr6 run of this check on Linux (7,287 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr6 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 1 | 1 |
| `allele_with_n` | 32 | 12 |
| `large_deletion` | 11 | 9 |
| `splice` | 13 | 8 |
| `cds_utr_boundary` | 17 | 7 |
| `start_codon` | 4 | 4 |
| `stop_codon` | 4 | 3 |
| `incomplete_terminal_codon` | 3 | 3 |
| `inframe_indel` | 35 | 5 |
| `hgvs` | 14 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (1 records)

- 1: vepyr decodes a percent-encoded INFO value; e.g. 6:142773072 ATAGT>A (ClinVar 3899257, -)

**allele_with_n** (32 records)

- 13: vepyr adds `stop_gained`; e.g. 6:7579754 T>TGAAAA...(116 bp) (ClinVar 1457582, ENST00000379802)
- 8: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 6:7580578 T>TAAGAT...(119 bp) (ClinVar 4784291, ENST00000379802)
- 5: `Amino_acids`: VEP `empty`, vepyr `-/P`; e.g. 6:7583322 A>AGCATC...(108 bp) (ClinVar 2922201, ENST00000379802)

**large_deletion** (11 records)

- 4: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 6:1610149 CGCCTG...(2304 bp)>C (ClinVar 3236692, ENST00000645831)
- 1: VEP has `inframe_deletion`, vepyr `coding_sequence_variant`; e.g. 6:35238017 AGGTAA...(1857 bp)>A (ClinVar 981657, ENST00000274938)
- 1: `HGVSc`: VEP `n.#_#+#delinsS`, vepyr `empty`; e.g. 6:35455802 CTGATG...(707 bp)>GGACTC...(14 bp) (ClinVar 1514119, ENST00000696267)

**splice** (13 records)

- 4: vepyr adds `splice_region_variant`; e.g. 6:73636623 AAAAAG>CAAAA (ClinVar 5014631, ENST00000355773)
- 2: vepyr drops `intron_variant&splice_donor_5th_base_variant`; e.g. 6:31963733 GGT>AGAGAGA (ClinVar 2871929, ENST00000375394)
- 2: VEP has `splice_donor_5th_base_variant&splice_donor_variant`, vepyr `splice_region_variant`; e.g. 6:75175029 GG>TTTCAGAAAA (ClinVar 1099073, ENST00000322507)

**cds_utr_boundary** (17 records)

- 5: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 6:5368705 AGCTCC...(24 bp)>A (ClinVar 4973563, ENST00000602691)
- 5: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 6:73462004 GGCAGC...(23 bp)>G (ClinVar 931370, ENST00000680563)
- 4: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 6:35452542 CGGCAT...(16 bp)>C (ClinVar 1387558, ENST00000696264)

**start_codon** (4 records)

- 4: vepyr adds `start_lost`; e.g. 6:26090867 ATGGG>A (ClinVar 3593440, ENST00000714170)
- 2: vepyr adds `start_retained_variant`; e.g. 6:35452547 TGGCGACACCG>T (ClinVar 2798486, ENST00000229769)

**stop_codon** (4 records)

- 2: vepyr adds `stop_lost`; e.g. 6:26204953 C>CT (ClinVar 3367431, ENST00000615164)
- 1: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 6:36225363 TG>T (ClinVar 4289861, ENST00000449261)
- 1: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 6:131583376 A>AAGTT (ClinVar 4759926, ENST00000986422)

**incomplete_terminal_codon** (3 records)

- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 6:13306445 C>CT (ClinVar 488617, ENST00000422136)
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 6:33173352 TCTC>T (ClinVar 3677122, ENST00000361917)
- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 6:123530498 AT>A (ClinVar 512259, ENST00000714040)

**inframe_indel** (35 records)

- 28: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 6:7583705 CCTT>G (ClinVar 3894321, ENST00000379802)
- 5: vepyr drops `protein_altering_variant`; e.g. 6:63721689 CTTCC>TATAGT...(59 bp) (ClinVar 1453873, ENST00000370621)
- 1: vepyr adds `inframe_deletion`; e.g. 6:64590525 ACTGAG...(26 bp)>TA (ClinVar 438198, ENST00000370621)

**hgvs** (14 records)

- 2: `HIGH_INF_POS`: VEP `Y`, vepyr `N`; e.g. 6:26156815 C>AG (ClinVar 635259, ENSM00000782923)
- 1: `HGVSp`: VEP `p.Ter#delextTer?`, vepyr `p.Ter#?extTer?`; e.g. 6:26157045 AAGT>A (ClinVar 3364416, ENST00000304218)
- 1: `HGVSc`: VEP `c.-#delinsS`, vepyr `empty`; e.g. 6:32853681 CC>AT (ClinVar 937287, ENST00000698420)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
