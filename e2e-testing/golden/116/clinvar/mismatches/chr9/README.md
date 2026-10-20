# chr9 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr9: 155 of 203,226 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 38 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr9 run of this check on Linux (8,928 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr9 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 27 | 12 |
| `large_deletion` | 6 | 6 |
| `splice` | 7 | 5 |
| `cds_utr_boundary` | 26 | 9 |
| `start_codon` | 6 | 6 |
| `stop_codon` | 3 | 3 |
| `incomplete_terminal_codon` | 3 | 1 |
| `inframe_indel` | 44 | 10 |
| `hgvs` | 33 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (27 records)

- 11: vepyr adds `stop_gained`; e.g. 9:6644616 A>ACAGGG...(116 bp) (ClinVar 2695608, ENST00000321612)
- 4: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 9:36223504 T>TAAAAG...(235 bp) (ClinVar 1014000, ENST00000396594)
- 4: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 9:128522778 T>TAAAGA...(153 bp) (ClinVar 1456784, ENST00000309971)

**large_deletion** (6 records)

- 1: VEP has `frameshift_variant`, vepyr `coding_sequence_variant`; e.g. 9:441285 GGGAGG...(2231 bp)>G (ClinVar 4884343, ENST00000382329)
- 1: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 9:21971106 CGTCGT...(964 bp)>C (ClinVar 1456941, ENST00000578845)
- 1: `HGVSc`: VEP `c.#+#_*#delinsS`, vepyr `empty`; e.g. 9:34648944 TCCCAG...(2295 bp)>GAATAGACCCCA (ClinVar 1064541, ENST00000378842)

**splice** (7 records)

- 3: vepyr adds `splice_region_variant`; e.g. 9:34514641 ACCCCG>TGATAG...(263 bp) (ClinVar 2126586, ENST00000242317)
- 1: VEP has `splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 9:27209127 CCT>ATCTTTGGAGGC (ClinVar 4902820, ENST00000856720)
- 1: VEP has `intron_variant&splice_donor_5th_base_variant&splice_donor_variant`, vepyr `splice_region_variant`; e.g. 9:77339902 TAACAATA>ATGTGT...(15 bp) (ClinVar 5011078, ENST00000360280)

**cds_utr_boundary** (26 records)

- 11: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 9:2186200 GGAAGAT>G (ClinVar 1701562, ENST00000634706)
- 9: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 9:2047295 GGCCCT...(85 bp)>G (ClinVar 2048149, ENST00000635226)
- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 9:97697266 CGGCAA...(31 bp)>C (ClinVar 556808, ENST00000462523)

**start_codon** (6 records)

- 3: vepyr drops `start_lost`; e.g. 9:84670750 TGTC>T (ClinVar 2121500, ENST00000277120)
- 2: vepyr adds `start_lost`; e.g. 9:69174374 T>TGCCGGTGC (ClinVar 2888023, ENST00000348208)
- 1: vepyr adds `start_retained_variant`; e.g. 9:137716635 T>TAGC (ClinVar 2742745, ENST00000492232)

**stop_codon** (3 records)

- 1: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 9:12709180 T>TAACA (ClinVar 1025066, ENST00000381136)
- 1: vepyr drops `stop_lost`; e.g. 9:32491294 CAGGTACCTGA>C (ClinVar 4725789, ENST00000999580)
- 1: vepyr adds `stop_lost`; e.g. 9:37432055 G>GT (ClinVar 551167, ENST00000973533)

**incomplete_terminal_codon** (3 records)

- 3: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 9:13143484 TAG>T (ClinVar 1374316, ENST00000542239)

**inframe_indel** (44 records)

- 32: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 9:3828310 AGCGGTCCAC>G (ClinVar 2505757, ENST00000324333)
- 5: vepyr drops `protein_altering_variant`; e.g. 9:77370460 AGGGGG>CCCTTA...(69 bp) (ClinVar 4816431, ENST00000360280)
- 2: vepyr adds `inframe_deletion`; e.g. 9:95459618 GCATGTAGTC>ACTA (ClinVar 419615, ENST00000331920)

**hgvs** (33 records)

- 5: `HGVSc` differs in its numbers only, e.g. VEP `c.150_152dup`, vepyr `c.153_155dup`; e.g. 9:92474742 C>CTCA (ClinVar 2527, NM_001193335.3)
- 5: `REFSEQ_OFFSET`: VEP `-#`, vepyr `empty`; e.g. 9:92474742 C>CTCA (ClinVar 2527, NM_001193335.3)
- 4: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 9:21974813 CGCCGC...(15 bp)>C (ClinVar 656963, ENST00000304494)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
