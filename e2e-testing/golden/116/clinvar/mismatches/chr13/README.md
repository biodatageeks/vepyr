# chr13 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr13: 875 of 90,238 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 52 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr13 run of this check on Linux (2,882 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr13 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 770 | 3 |
| `allele_with_n` | 36 | 12 |
| `large_deletion` | 7 | 6 |
| `splice` | 6 | 4 |
| `cds_utr_boundary` | 11 | 6 |
| `start_codon` | 4 | 4 |
| `stop_codon` | 2 | 2 |
| `incomplete_terminal_codon` | 1 | 1 |
| `inframe_indel` | 33 | 4 |
| `hgvs` | 5 | 5 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (770 records)

- 770: vepyr decodes a percent-encoded INFO value; e.g. 13:32316406 G>A (ClinVar 125955, -)
- 1: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 13:32338392 CTGTTTG>T (ClinVar 126035, ENST00000380152)

**allele_with_n** (36 records)

- 14: vepyr adds `stop_gained`; e.g. 13:23335430 T>TAAGAT...(111 bp) (ClinVar 3647081, ENST00000382292)
- 5: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 13:23203853 T>TACAAT...(142 bp) (ClinVar 1366500, ENST00000218867)
- 5: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 13:32336830 T>TGAAAA...(135 bp) (ClinVar 1447722, ENST00000380152)

**large_deletion** (7 records)

- 2: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 13:23234651 GCTTGG...(1227 bp)>G (ClinVar 664767, ENST00001113091)
- 1: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 13:32316584 AATGCT...(2865 bp)>CAC (ClinVar 2692390, ENST00000700199)
- 1: `HGVSp`: VEP `p.Pro#GlnfsTer#`, vepyr `empty`; e.g. 13:32326097 TTAGTC...(514 bp)>T (ClinVar 3780835, ENST00000380152)

**splice** (6 records)

- 2: vepyr adds `splice_region_variant`; e.g. 13:32326244 GTA>CTT (ClinVar 630437, ENST00000380152)
- 2: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 13:48380084 GGTAAATTTT>CTTTTA...(77 bp) (ClinVar 2012455, ENST00000267163)
- 1: vepyr adds `splice_acceptor_variant`; e.g. 13:32326097 TTAGTCC>GTAGAACA (ClinVar 4867850, ENST00000380152)

**cds_utr_boundary** (11 records)

- 4: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 13:48001234 GGCCACTA>G (ClinVar 4850066, ENST00000434484)
- 3: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 13:32316453 AGGTAA...(17 bp)>A (ClinVar 430603, ENST00000614259)
- 1: vepyr drops `incomplete_terminal_codon_variant`; e.g. 13:23368417 TCC>ACT (ClinVar 1806972, ENST00000682547)

**start_codon** (4 records)

- 4: vepyr adds `start_lost`; e.g. 13:48303914 T>TGCCGC...(35 bp) (ClinVar 428711, ENST00000267163)

**stop_codon** (2 records)

- 1: vepyr adds `stop_lost`; e.g. 13:32398767 C>CT (ClinVar 1628176, ENST00000380152)
- 1: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 13:99985680 T>TGACGG (ClinVar 2990499, ENST00000376335)

**incomplete_terminal_codon** (1 records)

- 1 of the 1 records differ on one transcript only, `ENST00000465820`.
- 1: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 13:26401597 TGG>T (ClinVar 4284302, ENST00000465820)

**inframe_indel** (33 records)

- 22: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 13:23331845 ACTGTTCAGA>GTTCATT (ClinVar 3242492, ENST00000382292)
- 7: vepyr drops `protein_altering_variant`; e.g. 13:32337719 GGAAGTC>ATTCTT...(37 bp) (ClinVar 545851, ENST00000380152)
- 3: vepyr adds `inframe_deletion`; e.g. 13:32338607 ATAA>T (ClinVar 254533, ENST00000380152)

**hgvs** (5 records)

- 1: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#SerfsTer#`; e.g. 13:32325127 AAATGG>A (ClinVar 266766, ENST00000530893)
- 1: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 13:48303715 GGAAGTGACG>AA (ClinVar 3237119, ENST00000433480)
- 1: `HGVSp`: VEP `p.Lys#_Ter#del`, vepyr `p.Lys#del`; e.g. 13:50956450 TAAAA>T (ClinVar 5004234, ENST00000643405)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
