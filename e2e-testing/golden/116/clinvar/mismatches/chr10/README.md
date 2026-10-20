# chr10 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr10: 517 of 171,717 records differ between Ensembl VEP and vepyr. Both files hold the same records in the same order; no record differs in the order of its CSQ entries. 43 of the differing records have an allele of 50 bp or more.

The layout, the class rules and the reproducing steps are those of [`../chr15/`](../chr15/README.md), which also describes each class. The VEP reference is the full chr10 run of this check on Linux (6,991 s for one process). Every fixture was checked: `vepyr annotate` on its `input.vcf` writes the records of its `vepyr.vcf` byte for byte. The `large_deletion` files are in Git LFS.

| Class | Records on chr10 | In the fixture |
|---|---:|---:|
| `info_percent_encoding` | 1 | 1 |
| `allele_with_n` | 21 | 12 |
| `large_deletion` | 5 | 5 |
| `splice` | 9 | 7 |
| `cds_utr_boundary` | 46 | 12 |
| `start_codon` | 4 | 4 |
| `stop_codon` | 4 | 4 |
| `incomplete_terminal_codon` | 3 | 3 |
| `inframe_indel` | 29 | 6 |
| `hgvs` | 395 | 12 |

## Most common differences

For each class, the three most common changes between VEP and vepyr, with the number of records and one example. `NMD_transcript_variant` is left out of the terms; it is the same on both sides.

**info_percent_encoding** (1 records)

- 1: vepyr decodes a percent-encoded INFO value; e.g. 10:86923640 A>G (ClinVar 239857, -)

**allele_with_n** (21 records)

- 6: vepyr adds `stop_gained`; e.g. 10:53840321 T>TAAAAA...(101 bp) (ClinVar 1422787, ENST00000320301)
- 4: `HGVSc`: VEP `empty`, vepyr `c.#+#_#+#insS`; e.g. 10:18498485 G>GAATAA...(195 bp) (ClinVar 1060428, ENST00000282343)
- 3: `HGVSc`: VEP `empty`, vepyr `c.#-#_#-#insS`; e.g. 10:27147672 T>TAGGAA...(114 bp) (ClinVar 2115096, ENST00000326799)

**large_deletion** (5 records)

- 1: `HGVSc`: VEP `n.#+#_#delinsS`, vepyr `empty`; e.g. 10:68286875 CCCAGC...(8812 bp)>GGTGGCTC (ClinVar 4283348, ENST00000277795)
- 1: VEP has `coding_sequence_variant`, vepyr `start_lost`; e.g. 10:87863214 GCTGGG...(890 bp)>G (ClinVar 1771406, ENST00000693560)
- 1: `HGVSc`: VEP `n.#_#-#delinsS`, vepyr `empty`; e.g. 10:87954244 AAATTA...(5832 bp)>CTTTTA...(5832 bp) (ClinVar 689436, ENST00000700026)

**splice** (9 records)

- 2: vepyr adds `splice_region_variant`; e.g. 10:13112454 ACCCCA>CTTCC (ClinVar 3348994, ENST00000263036)
- 2: VEP has `intron_variant&splice_donor_5th_base_variant&splice_donor_variant`, vepyr `splice_region_variant`; e.g. 10:71732373 AC>TATCTTGAA (ClinVar 3596862, ENST00000224721)
- 1: vepyr drops `intron_variant&splice_donor_region_variant`; e.g. 10:87894099 GATGTAGTAAGG>AATATT...(14 bp) (ClinVar 2674295, ENST00000371953)

**cds_utr_boundary** (46 records)

- 18: VEP has `coding_sequence_variant`, vepyr `protein_altering_variant`; e.g. 10:8058391 TCCCCC...(47 bp)>CCC (ClinVar 4906506, ENST00000461472)
- 10: VEP has `coding_sequence_variant`, vepyr `stop_lost`; e.g. 10:75022090 CGAGGA...(13 bp)>C (ClinVar 2180301, ENST00000647666)
- 3: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant`, vepyr `stop_lost`; e.g. 10:75022132 AGAG>A (ClinVar 3051984, ENST00000647637)

**start_codon** (4 records)

- 3: vepyr adds `start_lost`; e.g. 10:28535620 T>TG (ClinVar 4906951, ENST00000375646)
- 1: vepyr adds `start_retained_variant`; e.g. 10:89014160 A>AGTCG (ClinVar 1703254, ENST00000460510)

**stop_codon** (4 records)

- 1: VEP has `coding_sequence_variant`, vepyr `stop_retained_variant`; e.g. 10:18539723 G>GAGTTT...(63 bp) (ClinVar 1522823, ENST00000282343)
- 1: VEP has `stop_lost`, vepyr `stop_retained_variant`; e.g. 10:54079377 T>TCAGTG...(17 bp) (ClinVar 1458978, ENST00000373956)
- 1: vepyr adds `stop_lost`; e.g. 10:87957876 C>CTA (ClinVar 1451626, ENST00000700022)

**incomplete_terminal_codon** (3 records)

- 2: VEP has `coding_sequence_variant&incomplete_terminal_codon_variant&inframe_insertion`, vepyr `frameshift_variant`; e.g. 10:49530725 T>TA (ClinVar 1068519, ENST00000462247)
- 1: VEP has `coding_sequence_variant`, vepyr `frameshift_variant`; e.g. 10:8058493 GG>T (ClinVar 3061035, ENST00000481743)

**inframe_indel** (29 records)

- 21: VEP has `protein_altering_variant`, vepyr `inframe_deletion`; e.g. 10:27035384 TTGATC...(27 bp)>AGA (ClinVar 3369983, ENST00000376087)
- 4: vepyr drops `protein_altering_variant`; e.g. 10:70598951 C>CCCT (ClinVar 3632385, ENST00000697571)
- 1: VEP has `protein_altering_variant`, vepyr `splice_region_variant`; e.g. 10:78035352 AAC>TACGGATAG (ClinVar 7247, ENST00000360830)

**hgvs** (395 records)

- 371 of the 395 records differ on one transcript only, `NM_001304718.2`.
- 43: `HGVSc` differs in its numbers only, e.g. VEP `c.-1072_-1071insGG`, vepyr `c.-1071_-1070insGG`; e.g. 10:87864103 C>CGG (ClinVar 2570771, NM_001304718.2)
- 16: `CDS_position` differs in its numbers only, e.g. VEP `43-?`, vepyr `44-?`; e.g. 10:87952258 CAG>C (ClinVar 1752960, NM_001304718.2)
- 11: `HGVSp` differs in its numbers only, e.g. VEP `p.Glu795dup`, vepyr `p.Glu796dup`; e.g. 10:75022111 G>GGAA (ClinVar 2431885, ENST00000648159)

In the `hgvs` lines, `#` stands for a number and `S` for a sequence.

## NM_001304718.2

371 of the 395 `hgvs` records, all in PTEN, differ on the RefSeq transcript
NM_001304718.2 only. On this transcript the coordinates of vepyr are one
position further than those of VEP: `CDS_position` `43-?` > `44-?`,
`Protein_position` `?-16` > `?-17`, `HGVSc` `c.-625-23_-620del` >
`c.-625-23_-621del`. In most of these records `HGVSp` differs as a
consequence, with other residues as well as other positions, e.g. 10:87957854
TC>T `p.Pro16LeufsTer8` > `p.Gln17SerfsTer7`. For some insertions vepyr writes
`REFSEQ_OFFSET` `-1` where VEP writes none.
