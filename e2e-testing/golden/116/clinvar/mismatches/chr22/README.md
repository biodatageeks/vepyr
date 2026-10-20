# chr22 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr22: 100 of 99,409 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 27 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr22 run of this check on Linux (3,894 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr22 | In the fixture |
|---|---:|---:|
| `allele_with_n` | 10 | 10 |
| `large_deletion` | 10 | 6 |
| `splice` | 4 | 3 |
| `cds_utr_boundary` | 30 | 12 |
| `start_codon` | 3 | 3 |
| `stop_codon` | 5 | 5 |
| `incomplete_terminal_codon` | 2 | 2 |
| `inframe_indel` | 18 | 4 |
| `hgvs` | 18 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**allele_with_n** (10 records)

- 3: vepyr adds `stop_gained`; e.g. 22:20983054 T>TAAAGA...(113 bp) (ClinVar 1412195, ENST00000414985)
- 2: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 22:28695690 G>GCAATG...(121 bp) (ClinVar 2109318, ENST00000348295)
- 2: `Amino_acids`: VEP `empty`, vepyr `-/P`; e.g. 22:28719414 T>TGATGT...(129 bp) (ClinVar 1071123, ENST00000348295)

**large_deletion** (10 records)

- 4: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 22:28696899 ACCTTT...(3038 bp)>A (ClinVar 3780843, ENST00001131070)
- 4: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 22:28710005 CATGAT...(2013 bp)>C (ClinVar 3780844, ENST00000348295)
- 4: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 22:28710005 CATGAT...(2013 bp)>C (ClinVar 3780844, ENST00000348295)

**splice** (4 records)

- 2: vepyr adds `splice_region_variant`; e.g. 22:28695711 AGATAAAAA>CGATAAAAGAT (ClinVar 2094521, ENST00000348295)
- 1: VEP has `splice_donor_variant`, vepyr `splice_region_variant`; e.g. 22:20996964 AA>TTGG (ClinVar 817939, ENST00000415817)
- 1: VEP has `intron_variant&splice_acceptor_variant`, vepyr `splice_region_variant`; e.g. 22:50080394 TG>ACTGTC...(13 bp) (ClinVar 5011524, ENST00000311597)

**cds_utr_boundary** (30 records)

- 9: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 22:18936131 CGGCGG...(28 bp)>C (ClinVar 547891, ENST00000438924)
- 7: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 22:28711893 TTTTAT...(24 bp)>T (ClinVar 4634930, ENST00000439200)
- 6: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 22:21867429 CGCCGC...(24 bp)>C (ClinVar 4947107, ENST00000544786)

**start_codon** (3 records)

- 3: vepyr adds `start_lost`; e.g. 22:19759645 T>TGCACT...(26 bp) (ClinVar 4075816, ENST00000329705)

**stop_codon** (5 records)

- 3: VEP has `missense_variant`, vepyr `stop_lost`; e.g. 22:50217359 G>C (ClinVar 3439343, ENST00000380903)
- 1: vepyr adds `stop_lost`; e.g. 22:31837104 A>ATG (ClinVar 1076718, XM_017029113.2)
- 1: VEP has `inframe_insertion&stop_retained_variant`, vepyr `frameshift_variant&stop_gained`; e.g. 22:33381906 G>GTGTTC...(30 bp) (ClinVar 1501224, ENST00001105941)

**incomplete_terminal_codon** (2 records)

- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 22:31804217 GT>G (ClinVar 1076836, ENST00000473802)
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 22:38751007 TCTC>T (ClinVar 851745, ENST00000417332)

**inframe_indel** (18 records)

- 15: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 22:17181507 GAACTTATCCCA>AAGTTC (ClinVar 4291854, ENST00000262607)
- 2: vepyr drops `protein_altering_variant`; e.g. 22:20992298 A>ACTT (ClinVar 3869307, ENST00000646124)
- 1: vepyr adds `inframe_deletion`; e.g. 22:28689172 TCCTC>CA (ClinVar 4228003, ENST00000348295)

**hgvs** (18 records)

- 3: `HGVSp`: VEP `p.Ter#%#D`, vepyr `empty`; e.g. 22:28695237 C>CT (ClinVar 1072632, ENST00000711048)
- 2: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 22:28719401 AGAGTT...(14 bp)>A (ClinVar 1070237, ENST00000425190)
- 1: `HGVSp`: VEP `p.Ser#_Arg#del`, vepyr `p.Ser#ArgfsTer?`; e.g. 22:18921380 TCCTG>T (ClinVar 1998253, ENST00000438924)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
