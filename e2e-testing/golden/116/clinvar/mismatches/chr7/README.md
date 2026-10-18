# chr7 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr7: 233 of 223,219 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 65 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr7 run of this check on Linux (8,068 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr7 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 29 | 12 |
| `large_deletion` | 24 | 11 |
| `splice` | 11 | 7 |
| `cds_utr_boundary` | 45 | 12 |
| `start_codon` | 5 | 5 |
| `stop_codon` | 8 | 5 |
| `incomplete_terminal_codon` | 3 | 3 |
| `inframe_indel` | 80 | 8 |
| `hgvs` | 28 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (29 records)

- 12: vepyr adds `stop_gained`; e.g. 7:5986901 T>TAGAAA...(123 bp) (ClinVar 4734945, ENST00000265849)
- 4: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 7:37885125 T>TTTGTT...(113 bp) (ClinVar 3655744, ENST00000199447)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 7:2947555 C>CAAAGA...(117 bp) (ClinVar 2094425, ENST00000396946)

**large_deletion** (24 records)

- 5: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 7:92517344 GTCCAT...(1450 bp)>G (ClinVar 863102, ENST00000422866)
- 3: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 7:117530898 GGAAGT...(3468 bp)>G (ClinVar 634919, ENST00000003084)
- 2: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 7:5973398 GTCAGT...(9594 bp)>G (ClinVar 3780911, ENST00000265849)

**splice** (11 records)

- 3: vepyr adds `splice_region_variant`; e.g. 7:5982998 GTAGA>ATAGG (ClinVar 2762821, ENST00000265849)
- 2: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 7:5987198 AGCTGGCCGCAT>TGCTGGCCACAC (ClinVar 418418, ENST00000699764)
- 2: VEP has `splice_donor_5th_base_variant`, vepyr `splice_donor_region_variant`; e.g. 7:117536674 GTAA>ACATTATT (ClinVar 4001814, ENST00000003084)

**cds_utr_boundary** (45 records)

- 14: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 7:2578347 CCCAGCCGCG>C (ClinVar 871385, ENST00000422276)
- 11: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 7:5528089 GACAGC...(19 bp)>G (ClinVar 4001521, ENST00000464611)
- 5: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 7:16421312 CCGGCC...(25 bp)>C (ClinVar 2022451, ENST00000399310)

**start_codon** (5 records)

- 4: vepyr adds `start_lost`; e.g. 7:76425119 T>TG (ClinVar 1333775, ENST00000336517)
- 1: vepyr drops `start_lost`; e.g. 7:70762874 G>A (ClinVar 2142053, ENST00000611706)

**stop_codon** (8 records)

- 3: VEP has `stop_lost`, vepyr `protein_altering_variant`; e.g. 7:5997326 T>TA (ClinVar 91369, ENST00000699765)
- 2: vepyr adds `stop_lost`; e.g. 7:75982323 G>GT (ClinVar 360704, ENST00001076920)
- 1: vepyr drops `stop_lost`; e.g. 7:5997326 TAA>T (ClinVar 1761644, ENST00000699765)

**incomplete_terminal_codon** (3 records)

- 1: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 7:21744917 CTT>C (ClinVar 3642058, ENST00000605912)
- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 7:75980438 TA>T (ClinVar 2763217, ENST00000426184)
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 7:157005841 TTTCA>T (ClinVar 2132397, ENST00000428439)

**inframe_indel** (80 records)

- 66: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 7:1745111 ACAGCG...(14 bp)>TG (ClinVar 4870372, ENST00000424383)
- 7: vepyr drops `protein_altering_variant`; e.g. 7:1470586 GGCCTCCATAT>AGGCTC...(14 bp) (ClinVar 2277337, ENST00000404767)
- 2: vepyr adds `inframe_deletion`; e.g. 7:124870949 AAATTA...(20 bp)>GAAGCCCT (ClinVar 4725632, ENST00000357628)

**hgvs** (28 records)

- 4: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 7:6005948 CTTAGA...(96 bp)>ATTT (ClinVar 91341, ENST00000699831)
- 3: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 7:5992002 AATGGA...(24 bp)>A (ClinVar 1766682, NM_001322011.2)
- 3: `HGVSp` differs in its numbers only, e.g. VEP `p.Trp288del`, vepyr `p.Trp289del`; e.g. 7:66639222 CATG>C (ClinVar 1505073, ENST00000639828)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.

## A result that depends on the rest of the input

7:117544096 (ClinVar 1705265, a 6,795 bp deletion, class `large_deletion`) is
not in the `large_deletion` fixture, although it is in `records.tsv`. In the
full chr7 run vepyr writes `HGNC_ID` `HGNC:40144` for the RefSeq transcript
NR_149084.1, as VEP does. When the record is annotated together with the other
fixture records, vepyr leaves `HGNC_ID` empty for that transcript; the rest of
the record is the same. This is the same behaviour as at 20:63674046 (see
[`../chr20/`](../chr20/README.md)).
