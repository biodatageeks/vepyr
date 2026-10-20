# chr19 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr19: 209 of 261,537 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 43 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr19 run of this check on Linux (8,666 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr19 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 38 | 1 |
| `allele_with_n` | 13 | 12 |
| `used_ref` | 1 | 1 |
| `large_deletion` | 19 | 12 |
| `splice` | 11 | 5 |
| `cds_utr_boundary` | 38 | 12 |
| `start_codon` | 7 | 7 |
| `stop_codon` | 5 | 4 |
| `incomplete_terminal_codon` | 6 | 4 |
| `inframe_indel` | 41 | 5 |
| `hgvs` | 30 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (38 records)

- 38: vepyr decodes a percent-encoded INFO value; e.g. 19:39414994 C>T (ClinVar 1034015, -)

**allele_with_n** (13 records)

- 5: vepyr adds `stop_gained`; e.g. 19:11129576 T>TCAACA...(138 bp) (ClinVar 1069978, ENST00000252444)
- 3: VEP has `coding_sequence_variant`, vepyr `inframe_insertion&stop_gained`; e.g. 19:1207144 C>CAAGAT...(118 bp) (ClinVar 4730241, ENST00000326873)
- 2: `HGVSc`: VEP `empty`, vepyr `c.#_#insS`; e.g. 19:1220364 T>TGCCCG...(126 bp) (ClinVar 2033707, ENST00000326873)

**used_ref** (1 records)

- 1: `USED_REF`: VEP `G`, vepyr `A`; e.g. 19:2273083 A>T (ClinVar 2648969, NM_001301020.1)
- 1: `Codons`: VEP `gGg/gTg`, vepyr `gAg/gTg`; e.g. 19:2273083 A>T (ClinVar 2648969, NM_001301020.1)
- 1: `Amino_acids`: VEP `G/V`, vepyr `E/V`; e.g. 19:2273083 A>T (ClinVar 2648969, NM_001301020.1)

**large_deletion** (19 records)

- 5: VEP has `stop_lost`, vepyr `coding_sequence_variant`; e.g. 19:1223158 ACTTCA...(1245 bp)>A (ClinVar 527833, ENST00000585465)
- 5: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 19:11105597 TGCGGT...(967 bp)>T (ClinVar 251401, ENST00001109384)
- 3: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 19:1205255 AAGAGT...(1699 bp)>A (ClinVar 4865208, ENST00000326873)

**splice** (11 records)

- 7: vepyr adds `splice_region_variant`; e.g. 19:1222013 GGC>CGT (ClinVar 3904058, ENST00000326873)
- 1: vepyr adds `splice_donor_variant`; e.g. 19:11107506 AAGAGTGC>GAGAGTGG (ClinVar 453194, ENST00001047212)
- 1: VEP has `splice_donor_variant`, vepyr `splice_region_variant`; e.g. 19:11111634 CTGTG>TGTGCAA (ClinVar 4076984, ENST00000252444)

**cds_utr_boundary** (38 records)

- 14: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 19:861440 AGCCCC...(363 bp)>CATTGA (ClinVar 2077091, ENST00000695946)
- 12: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 19:19197598 ACCGTT...(59 bp)>A (ClinVar 643059, ENST00000593273)
- 5: VEP has `stop_retained_variant`, vepyr `stop_lost`; e.g. 19:1106565 ATTTCT...(226 bp)>A (ClinVar 3726905, ENST00000354171)

**start_codon** (7 records)

- 6: vepyr adds `start_lost`; e.g. 19:11089550 TG>T (ClinVar 226303, ENST00000455727)
- 1: vepyr drops `start_lost`; e.g. 19:35718020 TGGC>T (ClinVar 4941851, ENST00000420124)

**stop_codon** (5 records)

- 2: vepyr adds `stop_lost`; e.g. 19:11123218 C>CT (ClinVar 252250, ENST00001047213)
- 1: VEP has `stop_gained`, vepyr `stop_retained_variant`; e.g. 19:1106429 G>A (ClinVar 1610786, ENST00000589115)
- 1: vepyr drops `stop_lost`; e.g. 19:4095387 CCATT>C (ClinVar 2123037, ENST00001054217)

**incomplete_terminal_codon** (6 records)

- 3: VEP has `protein_altering_variant`, vepyr `frameshift_variant`; e.g. 19:29702788 AG>A (ClinVar 3640706, ENST00000614091)
- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 19:11202082 T>TA (ClinVar 3649519, ENST00000588666)
- 1: vepyr adds `incomplete_terminal_codon_variant`; e.g. 19:48234471 CTCA>C (ClinVar 1462831, ENST00000522889)

**inframe_indel** (41 records)

- 37: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 19:7132171 GTAGAA...(17 bp)>AC (ClinVar 4853995, ENST00000302850)
- 1: VEP has `frameshift_variant`, vepyr `coding_sequence_variant`; e.g. 19:13920646 CCCCAA...(123 bp)>C (ClinVar 2835658, ENST00000318003)
- 1: vepyr drops `intron_variant`; e.g. 19:29702912 TC>ACTGTTCCTCCA (ClinVar 210552, ENST00001033515)

**hgvs** (30 records)

- 4: `HGVSp`: VEP `p.Met#_?#`, vepyr `p.Met#?`; e.g. 19:4817725 AGGAAGGGCAT>A (ClinVar 2068181, NM_001385680.1)
- 3: `HGVSc`: VEP `n.#_#delinsS`, vepyr `empty`; e.g. 19:11110705 CTTAAG...(16 bp)>GA (ClinVar 4533277, ENST00000560173)
- 2: `HGVSp`: VEP `p.Ter#delextTer#`, vepyr `p.Ter#?extTer#`; e.g. 19:1221955 TTGA>T (ClinVar 422324, ENST00000714318)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.
