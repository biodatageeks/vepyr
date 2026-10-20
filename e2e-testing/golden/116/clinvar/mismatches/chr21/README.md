# chr21 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr21: 50 of 51,667 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 10 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr21 run of this check on Linux (1,567 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr21 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 2 | 2 |
| `used_ref` | 1 | 1 |
| `large_deletion` | 3 | 3 |
| `splice` | 2 | 2 |
| `cds_utr_boundary` | 4 | 2 |
| `start_codon` | 1 | 1 |
| `stop_codon` | 3 | 2 |
| `incomplete_terminal_codon` | 1 | 1 |
| `inframe_indel` | 21 | 6 |
| `hgvs` | 12 | 9 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (2 records)

- 1: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 21:32650172 T>TAAGAA...(115 bp) (ClinVar 1475797, ENST00000357345)
- 1: VEP has `coding_sequence_variant`, vepyr `inframe_insertion`; e.g. 21:46444722 G>GAAAGA...(118 bp) (ClinVar 2743356, ENST00000359568)

**used_ref** (1 records)

- 1: VEP has `inframe_deletion`, vepyr `intergenic_variant`; e.g. 21:45504520 AGGCCC...(19 bp)>A (ClinVar 1338712, NM_030582.4)

**large_deletion** (3 records)

- 1: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 21:33355545 TATGAC...(1826 bp)>T (ClinVar 3064420, ENST00000873009)
- 1: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 21:33550229 CAATTG...(4016 bp)>C (ClinVar 617862, ENST00000436227)
- 1: `HGVSc` differs in its numbers only, e.g. VEP `c.3737_3756+693del`, vepyr `c.3737_3756+702del`; e.g. 21:45505952 TTCCGG...(714 bp)>T (ClinVar 1348869, NM_030582.4)

**splice** (2 records)

- 1: vepyr adds `splice_donor_5th_base_variant`; e.g. 21:42863432 CCACCTA>ACACCTG (ClinVar 3336333, ENST00000330317)
- 1: vepyr adds `splice_region_variant`; e.g. 21:46363480 GTAA>TTCAAC...(29 bp) (ClinVar 3587731, ENST00000359568)

**cds_utr_boundary** (4 records)

- 3: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 21:33602313 ATGTGC...(23 bp)>A (ClinVar 4286168, ENST00000417979)
- 1: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 21:43058951 ACACGG...(15 bp)>A (ClinVar 2435631, ENST00000458223)

**start_codon** (1 records)

- 1: vepyr adds `start_retained_variant`; e.g. 21:46111477 A>ATGCTC...(15 bp) (ClinVar 2439448, ENST00000300527)
- 1: vepyr adds `start_lost`; e.g. 21:46111477 A>ATGCTC...(15 bp) (ClinVar 2439448, ENST00000300527)

**stop_codon** (3 records)

- 2: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 21:46132080 T>TGACGC...(17 bp) (ClinVar 215295, ENST00001119206)
- 1: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 21:33826861 CT>C (ClinVar 4992305, XM_047440949.1)

**incomplete_terminal_codon** (1 records)

- 1 of the 1 records differ on one transcript only, `ENST00000458138`.
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 21:32603225 GTTC>G (ClinVar 1410302, ENST00000458138)

**inframe_indel** (21 records)

- 11: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 21:25997363 TAACAGGATC>G (ClinVar 662051, ENST00000346798)
- 7: VEP has `coding_sequence_variant`, vepyr `intergenic_variant`; e.g. 21:45504511 CGGCCC...(19 bp)>C (ClinVar 803638, NM_030582.4)
- 2: vepyr drops `protein_altering_variant`; e.g. 21:33553688 GAA>ATTAAA...(18 bp) (ClinVar 3959662, ENST00000300278)

**hgvs** (12 records)

- 4: `REFSEQ_OFFSET`: VEP `-#`, vepyr `empty`; e.g. 21:45504534 CACGTGGCTA>C (ClinVar 3060537, NM_030582.4)
- 2: `Protein_position` differs in its numbers only, e.g. VEP `?-1260`, vepyr `?-1263`; e.g. 21:45507465 CAGGTG...(128 bp)>ACCCTG...(21 bp) (ClinVar 2043948, NM_030582.4)
- 2: `CDS_position` differs in its numbers only, e.g. VEP `?-3779`, vepyr `?-3788`; e.g. 21:45507465 CAGGTG...(128 bp)>ACCCTG...(21 bp) (ClinVar 2043948, NM_030582.4)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
