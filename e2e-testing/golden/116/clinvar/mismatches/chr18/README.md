# chr18 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr18: 51 of 76,630 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 20 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr18 run of this check on Linux (2,627 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr18 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 13 | 12 |
| `large_deletion` | 3 | 3 |
| `splice` | 7 | 6 |
| `cds_utr_boundary` | 5 | 3 |
| `stop_codon` | 1 | 1 |
| `incomplete_terminal_codon` | 5 | 4 |
| `inframe_indel` | 13 | 2 |
| `hgvs` | 4 | 4 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (13 records)

- 6: vepyr adds `stop_gained`; e.g. 18:3164438 T>TCCTAG...(119 bp) (ClinVar 3648745, ENST00000261606)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 18:6992550 T>TAGAAA...(124 bp) (ClinVar 1512105, ENST00000389658)
- 3: `Amino_acids`: VEP `empty`, vepyr `-/P`; e.g. 18:31400962 T>TAAGAA...(116 bp) (ClinVar 2813708, ENST00000308128)

**large_deletion** (3 records)

- 1: `HGVSc`: VEP `c.*#+#_*#delinsS`, vepyr `empty`; e.g. 18:33740509 TTCAAG...(3337 bp)>AGAAGC...(16 bp) (ClinVar 3061746, ENST00000592288)
- 1: `HGVSc`: VEP `n.#_#-#delinsS`, vepyr `empty`; e.g. 18:51048861 GTAAGT...(5919 bp)>ATCTCT...(29 bp) (ClinVar 4285687, ENST00000585448)
- 1: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 18:62154348 TAGAAA...(3359 bp)>T (ClinVar 264638, ENST00000585458)

**splice** (7 records)

- 2: vepyr adds `splice_region_variant`; e.g. 18:23548008 TTTTTCCCA>AT (ClinVar 1705753, ENST00000269228)
- 1: VEP has `intron_variant&splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 18:46489138 TATC>CTGTCTCTTATA (ClinVar 5011039, ENST00000300591)
- 1: vepyr drops `splice_polypyrimidine_tract_variant`; e.g. 18:51046934 ATGTCTAA>CTAAAG...(387 bp) (ClinVar 4734459, ENST00000590061)

**cds_utr_boundary** (5 records)

- 2: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 18:23533436 AGAATATC>A (ClinVar 558037, ENST00000593280)
- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 18:46656907 TGGCCT...(459 bp)>T (ClinVar 1356361, ENST00000441551)
- 1: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 18:58918506 TTCCGA...(14 bp)>C (ClinVar 4939210, ENST00000588543)

**stop_codon** (1 records)

- 1: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 18:24160067 CTG>C (ClinVar 4285642, ENST00000327201)

**incomplete_terminal_codon** (5 records)

- 3: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 18:23194538 CA>C (ClinVar 4285630, ENST00000580153)
- 1: vepyr drops `incomplete_terminal_codon_variant`; e.g. 18:47868326 G>GAAT (ClinVar 3907994, ENST00000587269)
- 1: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 18:55260006 T>TA (ClinVar 4077684, ENST00000628078)

**inframe_indel** (13 records)

- 10: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 18:6950965 CGAGAGC>A (ClinVar 983534, ENST00000389658)
- 3: vepyr drops `protein_altering_variant`; e.g. 18:31524786 TAATTG...(30 bp)>AATTGG...(84 bp) (ClinVar 2093339, ENST00000261590)

**hgvs** (4 records)

- 1: `HGVSc`: VEP `n.#delinsS`, vepyr `empty`; e.g. 18:2722603 AG>CC (ClinVar 4734033, ENST00000690757)
- 1: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#AsnfsTer#`; e.g. 18:34978515 GGAAT>G (ClinVar 2672722, ENST00000436190)
- 1: `HGVSp`: VEP `p.Thr#ThrfsTer?`, vepyr `p.Ter#del`; e.g. 18:55269873 AG>A (ClinVar 1809777, ENST00000562607)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
