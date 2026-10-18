# chr8 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr8: 112 of 155,536 records differ between Ensembl VEP and
vepyr. Both files hold the same records in the same order, and no record
differs only in the order of its CSQ entries.

The layout, the class rules and the reproducing steps are those of
[`../chr15/`](../chr15/README.md), which also describes each class in detail.
The VEP reference is the full chr8 run of this check on Linux (5,744 s for one
process). This page lists what chr8 adds.

| Class | Records on chr8 | In the fixture | What differs |
|---|---:|---:|---|
| `info_percent_encoding` | 1 | 1 | `CLNDN` (outside CSQ) |
| `allele_with_n` | 27 | 12 | `HGVSc`, `HGVSp`, `Amino_acids`, `HGVS_OFFSET`, `Consequence` |
| `large_deletion` | 5 | 5 | `Consequence`, `HGVSc`, `Codons`, `Amino_acids`, `HGVSp` |
| `splice` | 12 | 9 | `Consequence` |
| `cds_utr_boundary` | 17 | 6 | `Consequence`, `IMPACT` |
| `start_codon` | 2 | 2 | `Consequence`, `HGVSp` |
| `stop_codon` | 6 | 3 | `Consequence` |
| `incomplete_terminal_codon` | 1 | 1 | `Consequence`, `HGVSp` |
| `inframe_indel` | 28 | 5 | `Consequence`, `IMPACT` |
| `hgvs` | 13 | 10 | `HGVSp`, `HGVSc`, positions |

There is no `used_ref` record on chr8. The `large_deletion` files are in Git
LFS. All ten fixtures were checked: `vepyr annotate` on each `input.vcf`
writes the records of `vepyr.vcf` byte for byte.

## What chr8 adds

**info_percent_encoding.** 8:144514982 CA>C (ClinVar 6066) has `%3B` three
times in one disease name, `t(12%3B21)(p13%3Bq22)%3B_TEL-AML1`, which vepyr
decodes to `;`.

**allele_with_n.** Besides the long insertions with a run of `N`, chr8 has two
SNVs whose ALT is `N`: 8:96145055 C>N (ClinVar 60533) and 8:102232169 A>N
(ClinVar 2687854). VEP leaves `HGVSc`, `HGVSp` and `Amino_acids` empty; vepyr
writes `c.876G>N`, `Amino_acids` `E/X` and `HGVSp` `p.Glu292Ter`. One
insertion goes from `stop_retained_variant` in VEP to `frameshift_variant` in
vepyr (8:18064504, ClinVar 1451339).

**large_deletion.** Two records at 8:100713180 (2,281 bp, ClinVar 4290618 and
4286651) are `stop_retained_variant&3_prime_UTR_variant` in VEP and
`stop_lost&3_prime_UTR_variant` in vepyr. For two deletions in non-coding
transcripts VEP writes the `HGVSc` and vepyr leaves it empty, e.g. 8:1957015
`ENST00000521927.1:n.624_814delinsTCC`.

**splice.** More combinations than on chr15, in both directions, e.g.
8:63066097 GCTA>ACT: VEP `splice_region_variant`, vepyr
`splice_acceptor_variant`; 8:74351328 CT>TTA: VEP `intron_variant`, vepyr
`splice_region_variant&intron_variant`; 8:99121157: vepyr drops
`splice_polypyrimidine_tract_variant`.

**cds_utr_boundary.** Two short insertions just after the stop codon:
8:41978637 C>CT (ClinVar 3684523) is `3_prime_UTR_variant` in VEP and
`stop_retained_variant` in vepyr; 8:18069865 A>ACTATAAC is
`3_prime_UTR_variant` in VEP and `inframe_insertion&stop_retained_variant` in
vepyr.

**start_codon.** 8:80486812 TGTC>T (ClinVar 1245018) goes the other way: VEP
`start_lost&inframe_deletion&start_retained_variant` and `p.Ser2?`, vepyr
without `start_lost` and `p.Ser2del`.

**inframe_indel.** 22 of the 28 records are `protein_altering_variant` in VEP
and `inframe_deletion` in vepyr. 8:144511959, a 194 bp deletion across both
splice sites of an exon (ClinVar 2001638), is `inframe_deletion` with
`p.Phe159_Glu197del` in VEP and `coding_sequence_variant` with empty `HGVSp`,
`Codons` and `Amino_acids` in vepyr, like the `large_deletion` records. At
8:38145258 CT>AAG vepyr adds `frameshift_variant` to
`stop_lost&3_prime_UTR_variant` and leaves `CDS_position` (`522-?` in VEP)
empty.

**hgvs.** Besides the patterns of chr15:

- the HGVSp position is one residue further in vepyr: 8:89935588 TCTC>T
  `p.Arg753del` > `p.Arg754del`; 8:133238971 (60 bp deletion) and 8:133239010
  (60 bp insertion) `p.Gly349_Glu368del` > `p.Thr350_Gly369del` and
  `p.Gly349_Glu368dup` > `p.Thr350_Gly369dup`.
- 8:37744362 GTGA>G: VEP `p.Met1?`, vepyr `p.Met1del`.
- 8:81660514 AC>A: VEP `p.Gly265del`, vepyr `p.Gly265ValfsTer?`.
- 8:143973463: VEP `p.MetAlaGly1_?3`, vepyr `p.Met1?`.
- 8:143817402 AG>GGGGCTGG...: VEP `p.His356_Ter357delinsHisTer`, vepyr empty.
- 8:38414789 TTTT>ATAA: VEP `CDS_position` `1977-?`, vepyr empty.
