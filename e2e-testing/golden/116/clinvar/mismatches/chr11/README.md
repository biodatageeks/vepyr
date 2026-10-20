# chr11 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr11: 293 of 267,817 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 77 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr11 run of this check on Linux (10,753 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr11 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 2 | 1 |
| `allele_with_n` | 56 | 12 |
| `large_deletion` | 20 | 12 |
| `splice` | 23 | 11 |
| `cds_utr_boundary` | 39 | 12 |
| `start_codon` | 4 | 4 |
| `stop_codon` | 14 | 6 |
| `incomplete_terminal_codon` | 10 | 7 |
| `inframe_indel` | 60 | 8 |
| `hgvs` | 65 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (2 records)

- 2: vepyr decodes a percent-encoded INFO value; e.g. 11:32417592 C>T (ClinVar 620630, -)

**allele_with_n** (56 records)

- 11: vepyr adds `stop_gained`; e.g. 11:821659 T>TAAAGA...(117 bp) (ClinVar 2735395, ENST00000336615)
- 9: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 11:63633087 T>TCAGCA...(117 bp) (ClinVar 1393807, ENST00000398868)
- 7: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 11:36576074 T>TCTTTT...(112 bp) (ClinVar 3752354, ENST00000299440)

**large_deletion** (20 records)

- 6: VEP has `start_lost`, vepyr `coding_sequence_variant`; e.g. 11:5226640 GCCCTT...(910 bp)>G (ClinVar 869251, ENST00000380315)
- 6: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 11:5226947 ACCAAC...(539 bp)>A (ClinVar 869252, ENST00000335295)
- 1: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 11:1017591 CGGGAT...(677 bp)>GGGGA (ClinVar 4283425, ENST00000421673)

**splice** (23 records)

- 8: vepyr adds `splice_region_variant`; e.g. 11:5225733 GAGGAA>TAGGAG (ClinVar 4848400, ENST00000335295)
- 3: vepyr drops `splice_polypyrimidine_tract_variant`; e.g. 11:2570608 GT>TCAAGG (ClinVar 421568, ENST00000155840)
- 2: VEP has `intron_variant&splice_donor_region_variant&splice_donor_variant`, vepyr `splice_region_variant`; e.g. 11:2662080 CA>TGTGC (ClinVar 4910322, ENST00000155840)

**cds_utr_boundary** (39 records)

- 19: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 11:792546 CTCCTA...(27 bp)>C (ClinVar 1524607, ENST00000532484)
- 8: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 11:6558052 AGAG>A (ClinVar 3390742, ENST00000524401)
- 6: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant`, vepyr `stop_lost`; e.g. 11:31789935 TACTGTAA>T (ClinVar 638055, ENST00000423822)

**start_codon** (4 records)

- 2: vepyr drops `start_lost`; e.g. 11:1436130 TGAA>T (ClinVar 1801019, ENST00000524702)
- 1: vepyr adds `start_retained_variant`; e.g. 11:57598272 TGGCCTCCA>T (ClinVar 3336827, ENST00000531133)
- 1: vepyr adds `start_lost`; e.g. 11:112230645 TG>T (ClinVar 1725108, ENST00000524931)

**stop_codon** (14 records)

- 7: vepyr adds `stop_lost`; e.g. 11:61368635 G>GT (ClinVar 3599808, ENST00001123469)
- 3: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 11:20654867 A>AGTCCAGTG (ClinVar 1511947, ENST00000525748)
- 1: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 11:72814562 CT>C (ClinVar 4283792, ENST00000977703)

**incomplete_terminal_codon** (10 records)

- 4: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 11:31789937 CT>C (ClinVar 3776221, ENST00000640613)
- 3: vepyr adds `incomplete_terminal_codon_variant`; e.g. 11:5226620 TCA>T (ClinVar 869308, ENST00000380315)
- 3: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 11:31789935 TA>T (ClinVar 802665, ENST00000640287)

**inframe_indel** (60 records)

- 46: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 11:679792 ATCTGTC>CCCA (ClinVar 2581977, ENST00000382409)
- 9: vepyr drops `protein_altering_variant`; e.g. 11:2884087 CGCC>TCAGGCGCAG (ClinVar 1708131, ENST00000380725)
- 2: vepyr drops `intron_variant`; e.g. 11:17510520 ACCT>CCAGGG...(21 bp) (ClinVar 3906797, ENST00000005226)

**hgvs** (65 records)

- 27: `HGNC_ID`: VEP `empty`, vepyr `HS:#`; e.g. 11:22827233 C>T (ClinVar 3964266, NR_187205.1)
- 6: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 11:17428575 TACTTG...(19 bp)>AA (ClinVar 1725424, ENST00000529967)
- 6: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 11:17476771 GGGCAT>G (ClinVar 2105693, ENST00000302539)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
