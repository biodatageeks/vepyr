# chr2 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr2: 1,057 of 401,230 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 148 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr2 run of this check on Linux (20,027 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr2 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 2 | 1 |
| `allele_with_n` | 85 | 12 |
| `large_deletion` | 24 | 12 |
| `splice` | 27 | 9 |
| `cds_utr_boundary` | 38 | 12 |
| `start_codon` | 22 | 12 |
| `stop_codon` | 16 | 5 |
| `incomplete_terminal_codon` | 2 | 2 |
| `inframe_indel` | 111 | 12 |
| `hgvs` | 730 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (2 records)

- 2: vepyr decodes a percent-encoded INFO value; e.g. 2:9440490 G>T (ClinVar 3899256, -)

**allele_with_n** (85 records)

- 25: vepyr adds `stop_gained`; e.g. 2:15489010 T>TAAAAA...(113 bp) (ClinVar 4728148, ENST00000281513)
- 19: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 2:32154362 T>TTAAAA...(117 bp) (ClinVar 2833014, ENST00000315285)
- 15: VEP has `coding_sequence_variant`, vepyr `inframe_insertion`; e.g. 2:11767838 C>CAAGAA...(97 bp) (ClinVar 4736138, ENST00000256720)

**large_deletion** (24 records)

- 3: `HGVSc` differs in its numbers only, e.g. VEP `c.9782-1271_9845del`, vepyr `c.9782-1271_9848del`; e.g. 2:73533550 CTTGCA...(1336 bp)>C (ClinVar 1339655, NM_015120.4)
- 2: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 2:47401746 GCTGTG...(1540 bp)>G (ClinVar 2613699, ENST00000233146)
- 2: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 2:47778331 CCTTTC...(4340 bp)>AA (ClinVar 3572004, ENST00000636749)

**splice** (27 records)

- 9: VEP has `splice_donor_region_variant`, vepyr `splice_donor_5th_base_variant`; e.g. 2:29223336 GCT>ACC (ClinVar 1730573, ENST00000389048)
- 8: vepyr adds `splice_region_variant`; e.g. 2:47805039 GTTTCC...(40 bp)>ATCACA...(40 bp) (ClinVar 3647617, ENST00000936508)
- 3: VEP has `splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 2:27240254 TATCCCT>GGAGGTTGAGG (ClinVar 2525971, ENST00000264705)

**cds_utr_boundary** (38 records)

- 14: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 2:15424393 CCTGAA...(21 bp)>C (ClinVar 4953814, ENST00000442506)
- 6: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 2:45843021 AGAGTG...(18 bp)>A (ClinVar 2819482, ENST00000421201)
- 5: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 2:47783229 TCGGTA...(48 bp)>T (ClinVar 1736919, ENST00000700003)

**start_codon** (22 records)

- 19: vepyr adds `start_lost`; e.g. 2:32165902 TG>T (ClinVar 4287544, ENST00000282587)
- 2: vepyr adds `start_retained_variant`; e.g. 2:26254256 T>TTAC (ClinVar 4776046, ENST00000317799)
- 2: vepyr drops `start_lost`; e.g. 2:47403391 TGGGGCC>T (ClinVar 631150, ENST00000543555)

**stop_codon** (16 records)

- 7: vepyr adds `stop_lost`; e.g. 2:47480755 G>GT (ClinVar 2773620, ENST00001084073)
- 7: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 2:47480757 A>AATAG (ClinVar 4726636, ENST00001084073)
- 1: VEP has `inframe_insertion&stop_retained_variant`, vepyr `stop_lost`; e.g. 2:168936256 T>TATCTC (ClinVar 6594, XM_011512080.3)

**incomplete_terminal_codon** (2 records)

- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 2:224514636 T>TC (ClinVar 1451655, ENST00000436172)
- 1: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 2:241756013 TG>T (ClinVar 1480964, ENST00000432449)

**inframe_indel** (111 records)

- 76: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 2:1477311 CTGCTCCGCG>T (ClinVar 2633991, ENST00000329066)
- 21: vepyr drops `protein_altering_variant`; e.g. 2:25161269 CCAGGTCGGCCT>ACAGGT...(18 bp) (ClinVar 4999320, ENST00000264708)
- 5: vepyr adds `inframe_deletion`; e.g. 2:47798911 CTTA>T (ClinVar 1766427, ENST00000234420)

**hgvs** (730 records)

- 690 of the 730 records differ on one transcript only, `NM_015120.4`.
- 300: `HGVSp` differs in its numbers only, e.g. VEP `p.Pro7HisfsTer35`, vepyr `p.Pro7HisfsTer34`; e.g. 2:73385886 GC>G (ClinVar 558032, NM_015120.4)
- 18: `HGVSc` differs in its numbers only, e.g. VEP `c.62_85del`, vepyr `c.62_88del`; e.g. 2:73385919 GGAGGA...(25 bp)>G (ClinVar 3355830, NM_015120.4)
- 6: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 2:24872661 TAGTAGGACA>CCCGCA...(14 bp) (ClinVar 4998582, NM_001377131.1)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
