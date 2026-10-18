# chr4 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr4: 118 of 163,605 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 23 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr4 run of this check on Linux (5,886 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr4 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 4 | 1 |
| `allele_with_n` | 18 | 12 |
| `used_ref` | 2 | 2 |
| `large_deletion` | 5 | 4 |
| `splice` | 7 | 5 |
| `cds_utr_boundary` | 17 | 11 |
| `start_codon` | 3 | 3 |
| `stop_codon` | 3 | 1 |
| `incomplete_terminal_codon` | 4 | 3 |
| `inframe_indel` | 21 | 4 |
| `hgvs` | 34 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (4 records)

- 4: vepyr decodes a percent-encoded INFO value; e.g. 4:54727438 G>A (ClinVar 458885, -)

**allele_with_n** (18 records)

- 4: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 4:15515878 T>TAAAAA...(111 bp) (ClinVar 2947454, ENST00000389652)
- 3: vepyr adds `stop_gained`; e.g. 4:5741717 T>TGTTTA...(113 bp) (ClinVar 4786583, ENST00000264956)
- 2: `Amino_acids`: VEP `empty`, vepyr `-/P`; e.g. 4:15515878 T>TAAAAA...(111 bp) (ClinVar 2947454, ENST00000389652)

**used_ref** (2 records)

- 2 of the 2 records differ on one transcript only, `NM_002111.8`.
- 2: `USED_REF` differs in its numbers only, e.g. VEP `GCAGCAGCAGCAGCAGCAGCAGCAACAG`, vepyr `GCAGCAGCAGCAGCAGCAACAG`; e.g. 4:3074917 AGCAGC...(23 bp)>A (ClinVar 4966119, NM_002111.8)
- 2: `Protein_position`: VEP `#-#`, vepyr `empty`; e.g. 4:3074917 AGCAGC...(23 bp)>A (ClinVar 4966119, NM_002111.8)
- 2: `CDS_position`: VEP `#-#`, vepyr `empty`; e.g. 4:3074917 AGCAGC...(23 bp)>A (ClinVar 4966119, NM_002111.8)

**large_deletion** (5 records)

- 2: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 4:15984279 TAGCTA...(1858 bp)>T (ClinVar 3249956, ENST00000503884)
- 1: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 4:1002731 GATGAG...(1776 bp)>G (ClinVar 4976804, ENST00000247933)
- 1: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 4:52031952 TGCCAG...(2082 bp)>CACAGT...(2082 bp) (ClinVar 4077503, ENST00000381431)

**splice** (7 records)

- 3: vepyr adds `splice_region_variant`; e.g. 4:663772 CCTGAA...(47 bp)>TCTGGG (ClinVar 224742, ENST00000255622)
- 1: vepyr adds `splice_donor_variant`; e.g. 4:1799810 CAGGTA>TAGGTG (ClinVar 2500431, ENST00000260795)
- 1: VEP has `splice_donor_5th_base_variant`, vepyr `splice_donor_region_variant`; e.g. 4:3489798 TAG>CCGGGC...(15 bp) (ClinVar 3763069, ENST00000340083)

**cds_utr_boundary** (17 records)

- 6: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 4:656913 GTGCTG...(38 bp)>G (ClinVar 971646, ENST00000487902)
- 4: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 4:987056 AGCCCC...(42 bp)>A (ClinVar 2866981, ENST00000504568)
- 2: VEP has `start_lost`, vepyr `coding_sequence_variant`; e.g. 4:521860 GA>TT (ClinVar 1416100, NM_001345990.2)

**start_codon** (3 records)

- 3: vepyr adds `start_lost`; e.g. 4:36282013 ATG>A (ClinVar 4966805, ENST00000357504)

**stop_codon** (3 records)

- 3: vepyr adds `stop_lost`; e.g. 4:39460842 A>AT (ClinVar 817587, ENST00000640489)

**incomplete_terminal_codon** (4 records)

- 2: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 4:113353763 AG>A (ClinVar 4992569, ENST00000504454)
- 1: VEP has `coding_sequence_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 4:654082 A>AT (ClinVar 3382926, ENST00000488061)
- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 4:20707869 AG>A (ClinVar 4288619, ENST00000504630)

**inframe_indel** (21 records)

- 18: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 4:990155 ACACGT...(14 bp)>GGTGA (ClinVar 3591515, ENST00000361661)
- 2: vepyr drops `protein_altering_variant`; e.g. 4:22435429 C>CAGTGT...(19 bp) (ClinVar 1345372, ENST00000334304)
- 1: vepyr adds `frameshift_variant`; e.g. 4:102634854 GCTC>CTGAA (ClinVar 3589712, ENST00000978476)

**hgvs** (34 records)

- 25 of the 34 records differ on one transcript only, `NM_002111.8`.
- 23: `HGVSp` differs in its numbers only, e.g. VEP `p.Pro39del`, vepyr `p.Pro49del`; e.g. 4:3074876 C>CCAG (ClinVar 1301634, NM_002111.8)
- 2: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 4:533870 T>TA (ClinVar 546954, ENST00000986517)
- 1: `HGVSp`: VEP `p.MetGly#_?#`, vepyr `p.Met#?`; e.g. 4:1000892 GATGGGC>G (ClinVar 863830, NM_001363576.1)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
