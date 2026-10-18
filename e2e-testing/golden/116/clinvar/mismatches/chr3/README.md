# chr3 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr3: 198 of 251,389 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 39 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr3 run of this check on Linux (10,486 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr3 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 2 | 1 |
| `allele_with_n` | 26 | 12 |
| `large_deletion` | 7 | 7 |
| `splice` | 14 | 8 |
| `cds_utr_boundary` | 38 | 11 |
| `start_codon` | 11 | 10 |
| `stop_codon` | 10 | 6 |
| `incomplete_terminal_codon` | 4 | 3 |
| `inframe_indel` | 50 | 8 |
| `hgvs` | 36 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (2 records)

- 2: vepyr decodes a percent-encoded INFO value; e.g. 3:100302223 GC>G (ClinVar 4057202, -)

**allele_with_n** (26 records)

- 8: vepyr adds `stop_gained`; e.g. 3:14129450 T>TAAAAC...(117 bp) (ClinVar 1379055, ENST00000306077)
- 6: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 3:37014514 A>ATTTTT...(146 bp) (ClinVar 1070285, ENST00000231790)
- 5: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 3:25737388 T>TAAAAC...(115 bp) (ClinVar 1431487, ENST00000280699)

**large_deletion** (7 records)

- 2: VEP has `frameshift_variant`, vepyr `coding_sequence_variant`; e.g. 3:37048989 CTGAGG...(1564 bp)>C (ClinVar 90026, ENST00000231790)
- 1: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 3:10133798 CATGTG...(8098 bp)>C (ClinVar 374036, ENST00000256474)
- 1: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 3:37048989 CTGAGG...(1564 bp)>C (ClinVar 90026, ENST00000231790)

**splice** (14 records)

- 6: vepyr adds `splice_region_variant`; e.g. 3:14148739 GGGTG>CGGTT (ClinVar 1810492, ENST00000285021)
- 2: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 3:33051866 AGGCAA...(30 bp)>GGCAAG...(19 bp) (ClinVar 420844, ENST00000307363)
- 2: VEP has `splice_region_variant`, vepyr `splice_acceptor_variant`; e.g. 3:37048901 CAGG>AAGT (ClinVar 3295048, ENST00000231790)

**cds_utr_boundary** (38 records)

- 12: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 3:9442163 ATGGTG...(14 bp)>A (ClinVar 3068280, ENST00000399686)
- 10: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 3:4467241 GCCATG...(22 bp)>G (ClinVar 2743228, ENST00000448413)
- 9: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 3:32140358 CCACCA...(14 bp)>C (ClinVar 3643177, ENST00000429432)

**start_codon** (11 records)

- 8: vepyr adds `start_lost`; e.g. 3:3129041 ATGCTGAGG>A (ClinVar 3017738, ENST00000251607)
- 3: vepyr drops `start_lost`; e.g. 3:36993549 TGTC>T (ClinVar 928016, ENST00000231790)

**stop_codon** (10 records)

- 5: vepyr adds `stop_lost`; e.g. 3:10143001 G>GT (ClinVar 2109925, ENST00000696142)
- 2: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 3:36993662 TG>T (ClinVar 2673709, ENST00000454028)
- 1: vepyr drops `stop_lost`; e.g. 3:36993547 AATGTC...(117 bp)>A (ClinVar 4533193, ENST00000454028)

**incomplete_terminal_codon** (4 records)

- 2: vepyr adds `incomplete_terminal_codon_variant`; e.g. 3:43598564 GGGTAACCA>G (ClinVar 2867864, ENST00000444344)
- 2: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 3:127575938 AC>A (ClinVar 4287889, ENST00000490290)

**inframe_indel** (50 records)

- 44: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 3:8733975 CGAG>T (ClinVar 577351, ENST00000343849)
- 3: vepyr drops `protein_altering_variant`; e.g. 3:38846924 GAT>TTCATA...(21 bp) (ClinVar 2614204, ENST00000302328)
- 1: vepyr adds `frameshift_variant`; e.g. 3:10141833 TGGATC...(19 bp)>CGGCCC...(23 bp) (ClinVar 1311090, ENST00000256474)

**hgvs** (36 records)

- 3: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 3:37047556 T>TA (ClinVar 2674465, ENST00000441265)
- 2: `CDS_position`: VEP `#-?`, vepyr `empty`; e.g. 3:25598292 GTGCTC...(32 bp)>TTGCAA...(32 bp) (ClinVar 2701357, ENST00000264331)
- 2: `Protein_position`: VEP `#-?`, vepyr `empty`; e.g. 3:25598292 GTGCTC...(32 bp)>TTGCAA...(32 bp) (ClinVar 2701357, ENST00000264331)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
