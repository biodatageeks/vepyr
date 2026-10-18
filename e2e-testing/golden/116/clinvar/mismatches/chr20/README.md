# chr20 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr20: 54 of 99,377 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 12 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr20 run of this check on Linux (3,503 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr20 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 6 | 6 |
| `large_deletion` | 1 | 1 |
| `splice` | 1 | 1 |
| `cds_utr_boundary` | 15 | 5 |
| `start_codon` | 7 | 6 |
| `incomplete_terminal_codon` | 1 | 1 |
| `inframe_indel` | 13 | 5 |
| `hgvs` | 10 | 9 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (6 records)

- 2: vepyr adds `stop_gained`; e.g. 20:10641118 C>CCACAT...(117 bp) (ClinVar 3726266, ENST00000254958)
- 1: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 20:10413142 T>TCAAAA...(118 bp) (ClinVar 2954280, ENST00000347364)
- 1: VEP has `coding_sequence_variant`, vepyr `inframe_insertion`; e.g. 20:45897751 A>ACATGG...(118 bp) (ClinVar 2827278, ENST00000191018)

**large_deletion** (1 records)

- 1: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 20:50899224 CTTGCT...(9549 bp)>AGACAG...(9549 bp) (ClinVar 1879783, ENST00000642364)

**splice** (1 records)

- 1: vepyr adds `splice_region_variant`; e.g. 20:3229017 GCAGACGGGCA>CCGGCCGGCC (ClinVar 1310, ENST00000380056)

**cds_utr_boundary** (15 records)

- 7: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 20:3889668 TAC>T (ClinVar 803593, ENST00000646394)
- 3: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 20:58891722 CCGCCAT>C (ClinVar 426629, ENST00000338783)
- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 20:35511609 CAAGCC...(100 bp)>C (ClinVar 1449233, ENST00000422671)

**start_codon** (7 records)

- 4: vepyr adds `start_lost`; e.g. 20:25418182 TG>T (ClinVar 3652707, ENST00000696807)
- 3: vepyr drops `start_lost`; e.g. 20:32052425 C>T (ClinVar 3251251, ENST00000262651)
- 2: VEP has `start_lost`, vepyr `synonymous_variant`; e.g. 20:32052425 C>T (ClinVar 3251251, ENST00000262651)

**incomplete_terminal_codon** (1 records)

- 1 of the 1 records differ on one transcript only, `ENST00000446834`.
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 20:59877463 GTTTTT>G (ClinVar 633614, ENST00000446834)

**inframe_indel** (13 records)

- 10: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 20:3230597 CGTA>T (ClinVar 1313, ENST00000380056)
- 2: vepyr adds `inframe_deletion`; e.g. 20:3209552 ATGGC>TG (ClinVar 4277410, ENST00000380113)
- 1: vepyr drops `inframe_insertion`; e.g. 20:36927190 T>TCACCT...(71 bp) (ClinVar 4944963, ENST00000683720)

**hgvs** (10 records)

- 2: `HGVSp`: VEP `p.Pro#ProfsTer?`, vepyr `p.Ter#del`; e.g. 20:63408476 CG>C (ClinVar 1328847, ENST00000636614)
- 1: `HGVSp`: VEP `p.Met#?`, vepyr `p.Leu#?`; e.g. 20:3889432 T>A (ClinVar 193506, ENST00000610179)
- 1: `Amino_acids`: VEP `M/Q`, vepyr `L/Q`; e.g. 20:3889432 T>A (ClinVar 193506, ENST00000610179)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.

## A result that depends on the rest of the input

20:63674046 (ClinVar 651186, a 91 bp deletion, class `cds_utr_boundary`) is
not in the `cds_utr_boundary` fixture, although it is in `records.tsv`. In the
full chr20 run vepyr writes `HGNC_ID` `HGNC:44095` for the RefSeq transcript
NR_037882.1, as VEP does. When the record is annotated on its own, or together
with the other fixture records, vepyr leaves `HGNC_ID` empty for that
transcript. The rest of the record is the same in both runs. The vepyr output
for this record therefore depends on the other records in the input, and a
fixture cannot reproduce the full-run line.
