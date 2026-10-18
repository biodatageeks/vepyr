# chr15 mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chr15: 199 of 159,327 records differ between Ensembl VEP and
vepyr. Both files hold the same records in the same order, and no record
differs only in the order of its CSQ entries. This directory lists all of them
and keeps a small fixture for each kind of difference, so that a fix can be
checked without the whole run.

The VEP reference is the full chr15 run of this check on Linux
(`clinvar_20261011_chr15_vep116_merged.vcf.gz`, 5,271 s for one process);
the vepyr side is the unchanged `run_comparison.py` with vepyr 0.10.0 and
the cache revision it pins (`5b83dd8`).

- `records.tsv`: every differing record, with its position, ClinVar ID, alleles
  (long ones shortened), class, whether the CSQ order differs, the differing
  fields, the transcripts concerned and the `Amino_acids` values (VEP>vepyr).
- `<class>/input.vcf`: records from the preprocessed ClinVar input.
- `<class>/vep.vcf`: the same records copied from the VEP 116.0 chr15 output.
- `<class>/vepyr.vcf`: the same records copied from the vepyr 0.10.0 output.

Each file keeps the full header of the file it comes from. A fixture holds up
to 12 records, one for each distinct pattern of difference in its class. The
`large_deletion` files are stored in Git LFS: VEP repeats the deleted sequence
in several CSQ fields of every transcript, so one record takes up to 1.9 MB.

Each record is in one class only, the first rule below that matches it:

| Class | Rule | Records on chr15 | In the fixture | What differs |
|---|---|---:|---:|---|
| `info_percent_encoding` | an input INFO value differs | 52 | 1 | `CLNDN` (outside CSQ) |
| `allele_with_n` | REF or ALT contains `N` | 34 | 12 | `HGVSc`, `HGVSp`, `Amino_acids`, `HGVS_OFFSET`, `Consequence` |
| `used_ref` | `USED_REF` differs | 2 | 2 | `USED_REF`, `Codons`, `Amino_acids`, `HGVSc`, `HGVSp` |
| `large_deletion` | REF of 500 bp or more | 5 | 5 | `Consequence`, `Codons`, `Amino_acids`, `HGVSp`, `HGVSc` |
| `splice` | only splice terms differ, with `intron_variant` or `coding_sequence_variant` | 7 | 4 | `Consequence` |
| `cds_utr_boundary` | a UTR term is present and a start, stop or coding term differs | 31 | 11 | `Consequence`, `IMPACT`, positions, `HGVSp` |
| `start_codon` | `start_lost` or `start_retained_variant` differs | 5 | 5 | `Consequence`, `HGVSp` |
| `stop_codon` | `stop_lost` or `stop_retained_variant` differs | 4 | 3 | `Consequence` |
| `incomplete_terminal_codon` | `incomplete_terminal_codon_variant` is present | 7 | 4 | `Consequence`, `HGVSp` |
| `inframe_indel` | any other `Consequence` difference | 36 | 4 | `Consequence`, `IMPACT` |
| `hgvs` | `Consequence` is the same | 16 | 12 | `HGVSp`, `HGVSc`, positions |

`IMPACT` follows `Consequence` wherever the most severe term changes.
`NMD_transcript_variant` is ignored when the terms are compared; it is the same
on both sides.

## info_percent_encoding

ClinVar writes a `;` inside an INFO value as `%3B` (and `=` as `%3D`). VEP
copies the input INFO unchanged. vepyr decodes the value and writes a plain
`;`, which ends the INFO field early: the rest of the value is read as a key of
its own, and the line is no longer valid VCF. The CSQ of these records is
identical. All 52 chr15 records with a percent-encoded INFO value differ, and
nothing else on chr15 has one. The whole release has 1,966 such records.

15:29268829 C>T (ClinVar 1004005), input and VEP:
`CLNDN=Lung_disease,_immunodeficiency,_and_chromosome_breakage_syndrome%3B|not_provided`,
vepyr: `CLNDN=Lung_disease,_immunodeficiency,_and_chromosome_breakage_syndrome;|not_provided`.

## allele_with_n

ClinVar has long insertions (most of 100 bp or more, 32 of the 34 records,
plus two delins) whose inserted sequence contains a run of unknown bases
(`NNNNNNNNNN`), and one `T>TNN`. All 34 chr15 records with an `N` in REF or ALT
differ; the 5 chr15 insertions of 100 bp or more without an `N` match. VEP leaves `HGVSc`, `HGVSp`, `Amino_acids` and `HGVS_OFFSET` empty.
vepyr fills them, translating `N` codons as `X`, and adds terms from the
translated sequence: `frameshift_variant` becomes `stop_gained&frameshift_variant`,
`coding_sequence_variant` becomes `inframe_insertion` or
`stop_gained&inframe_insertion`.

- 15:31026229 A>A+118 bp (ClinVar 1452847), ENST00000256552: VEP `frameshift_variant`
  with empty HGVS; vepyr `stop_gained&frameshift_variant`,
  `c.3538_3539insGAGCC...`, `p.Phe1180Ter`.
- 15:50949868 T>TNN (ClinVar 30661), ENST00000261842: VEP empty; vepyr
  `c.1359_1360insNN`, `p.Val454TerfsTer5`, `Amino_acids` `-/X`.
- 15:34791099 A>A+141 bp (ClinVar 1374609), intronic: only `HGVSc` and
  `HGVS_OFFSET`, e.g. vepyr `c.990+14_990+15ins...`.

The whole release has 668 records with an `N` in REF or ALT.

## used_ref

Two SNVs on the RefSeq transcripts NM_002537.3 and NM_001301302.1, where the
transcript sequence differs from the genome. VEP uses the transcript base, vepyr
the genome base:

| Position | ClinVar ID | Change | `USED_REF` | `HGVSc` (NM_002537.3) | `HGVSp` |
|---|---|---|---|---|---|
| 64689084 | 4404590 | C>A | `T` > `C` | `c.439A>T` > `c.439G>T` | `p.Thr147Ser` > `p.Ala147Ser` |
| 64691478 | 4404594 | G>A | `C` > `G` | `c.133G>T` > `c.133C>T` | `p.Gly45Ter` > `p.Arg45Ter` |

## large_deletion

Deletions of 500 bp or more that cross exon boundaries. Usually VEP gives a
coding term with `Codons`, `Amino_acids` and `HGVSp`, and vepyr gives
`coding_sequence_variant` with those fields empty:

- 15:43599671, 9,606 bp (ClinVar 1185095), ENST00000450892: VEP
  `...&frameshift_variant&stop_lost&...`, `p.Gln1186ArgfsTer6`; vepyr
  `...&coding_sequence_variant&...`, empty `Codons`, `Amino_acids`, `HGVSp`.
- 15:84859308, 536 bp (ClinVar 4727555), ENST00000258888: VEP `inframe_deletion`,
  `p.Gln1297_Cys1346del`; vepyr `coding_sequence_variant`.
- 15:42410445, 1,548 bp (ClinVar 560538): VEP has `stop_lost`, vepyr
  `coding_sequence_variant`.
- 15:72370592, 7,945 bp (ClinVar 3891): same terms; VEP `HGVSc`
  `ENST00000268097.10:c.-42_253+5128delinsG`, vepyr empty.
- 15:89316396, 736 bp (ClinVar 929719), ENST00000561894, the other way round:
  VEP `coding_sequence_variant&3_prime_UTR_variant`, vepyr
  `stop_lost&3_prime_UTR_variant`.

## splice

Delins, MNVs and insertions at exon-intron boundaries get different splice
terms:

- 15:34237421 TCTC>ACT (ClinVar 1726327): VEP `frameshift_variant`, vepyr
  `frameshift_variant&splice_region_variant`.
- 15:42753248 CT>G+26 bp (ClinVar 2574735): VEP
  `splice_acceptor_variant&splice_polypyrimidine_tract_variant&intron_variant`,
  vepyr `splice_acceptor_variant`.
- 15:48288516 TG>CGTT (ClinVar 3577326): VEP
  `splice_donor_variant&splice_donor_region_variant&coding_sequence_variant&intron_variant`,
  vepyr `splice_donor_variant&coding_sequence_variant`.
- 15:48505145 TG>ACCTTC (ClinVar 519559): VEP
  `splice_acceptor_variant&frameshift_variant&intron_variant`, vepyr
  `frameshift_variant&splice_region_variant`.

## cds_utr_boundary

Deletions (27 of the 31 records, plus two delins, one insertion and one MNV)
that reach from the coding sequence into the 5' or the 3' UTR. Mostly vepyr gives a more severe term:

| VEP | vepyr | Records |
|---|---|---:|
| `coding_sequence_variant&3_prime_UTR_variant` | `stop_lost&3_prime_UTR_variant` | 9 |
| `coding_sequence_variant&5_prime_UTR_variant` | `protein_altering_variant&5_prime_UTR_variant` | 9 |
| `coding_sequence_variant&5_prime_UTR_variant` | `start_lost&5_prime_UTR_variant` | 5 |
| `stop_retained_variant&3_prime_UTR_variant` | `stop_lost&3_prime_UTR_variant` | 4 |

Examples: 15:22866845 TAAGA>T (ClinVar 1227869, ENST00000359727,
`stop_retained_variant` > `stop_lost`), 15:29822026 CATCTT>C (ClinVar
4923786, ENST00000545208, `coding_sequence_variant` > `start_lost`). Two go the
other way: 15:90233885 TC>AA (ClinVar 1948973) is `start_lost&5_prime_UTR_variant`
with `CDS_position` and `Protein_position` `?-1` in VEP, and
`coding_sequence_variant&5_prime_UTR_variant` with both positions empty in
vepyr; 15:90088729 ATCT>A (ClinVar 4886331) loses `start_lost` in vepyr. At
15:72737545 A>ATCAAG (ClinVar 4925771, ENST00001112280) VEP writes
`3_prime_UTR_variant` and vepyr `stop_lost` with `CDS_position` `738-739`.

## start_codon

One-base indels in the codon after the start codon. VEP writes
`frameshift_variant&start_retained_variant` and a frameshift `HGVSp`; vepyr adds
`start_lost` and writes an unknown consequence:

- 15:42409791 T>TG (ClinVar 288777), ENST00000337571: `p.Glu2GlyfsTer6` > `p.Glu2?`.
- 15:72686229 TG>T (ClinVar 3577623), ENST00000268057: `p.Ala2LeufsTer37` > `p.Ala2?`.
- 15:66703260 T>TG (ClinVar 1174547), ENST00000288840: `p.Phe2ValfsTer43` > `p.Xaa2?`.

## stop_codon

Indels at the stop codon that stay inside the coding sequence:

- 15:66490612 C>CT and C>CTA (ClinVar 950974, 3343301): VEP
  `frameshift_variant`, vepyr `frameshift_variant&stop_lost`.
- 15:40418197 A>A+23 bp (ClinVar 2850099), ENST00001119005: VEP
  `inframe_insertion&coding_sequence_variant`, vepyr
  `inframe_insertion&stop_retained_variant`.
- 15:72346235 CAG>C (ClinVar 961970): VEP has `stop_lost`, vepyr does not.

## incomplete_terminal_codon

Indels in transcripts whose last codon is incomplete:

- 15:42410909 TG>T (ClinVar 497201), ENST00000565559: VEP
  `incomplete_terminal_codon_variant&coding_sequence_variant`, `p.Ter158%3D`;
  vepyr `frameshift_variant&incomplete_terminal_codon_variant`, `p.Ter158del`.
- 15:45606484 G>GT (ClinVar 1038957): VEP
  `inframe_insertion&incomplete_terminal_codon_variant&coding_sequence_variant`,
  vepyr `frameshift_variant&incomplete_terminal_codon_variant`.
- 15:55223915 TTCC>T (ClinVar 1390483): vepyr adds
  `incomplete_terminal_codon_variant` to `inframe_deletion`.

## inframe_indel

In-frame changes inside the coding sequence that replace bases as well as
delete or insert them:

| VEP | vepyr | Example |
|---|---|---|
| `protein_altering_variant` | `inframe_deletion` | 15:23644907 AACCACT>CCAC (ClinVar 4076818) |
| `stop_gained&protein_altering_variant` | `stop_gained` | 15:44564604 T>TAAG (ClinVar 1075394) |
| `stop_gained` | `stop_gained&inframe_deletion` | 15:74897108 TACCCCTA>GG (ClinVar 4815046) |
| `stop_gained&splice_region_variant` | `stop_gained&inframe_deletion&splice_region_variant` | 15:30930667 TTAAG>AA (ClinVar 3577061) |

The first row covers 30 of the 36 records, 34 of which are delins.

## hgvs

`Consequence` is the same, `HGVSp` or `HGVSc` differs:

- delins and MNVs in non-coding exons and UTRs: VEP writes the `HGVSc`, vepyr
  leaves it empty, e.g. 15:31028431 GGAAGC>TACAGG (ClinVar 4923840)
  `ENST00000557948.1:n.564delinsCCTGTA` > empty, 15:44615487 GTA>ATC
  `XM_047433146.1:c.-104_-103delinsGAT` > empty.
- deletions that remove the start codon: VEP `p.Met1_?4`, vepyr `p.Met1?`
  (15:42405919, 15:68229573, 15:100580594).
- frameshifts where VEP finds no new stop and writes `fsTer?`: vepyr writes a
  stop position or a deletion, e.g. 15:66711675 `p.Pro1HisfsTer?` >
  `p.Pro1HisfsTer47`, 15:44563124 `p.Leu339LeufsTer?` > `p.Ala339del`,
  15:64980969 `p.Arg41ArgfsTer?` > `p.Ter41del`.
- 15:82676442 GTCA>G (ClinVar 2035244): `p.Ter529delextTer19` >
  `p.Ter529?extTer19`.

## Reproducing

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache DIR --chrom 15
vepyr annotate -i hgvs/input.vcf -o vepyr_out.vcf \
  --dir_cache DIR/116_GRCh38_merged \
  --fasta Homo_sapiens.GRCh38.dna.primary_assembly.fa
diff <(grep -v '^#' vepyr_out.vcf) <(grep -v '^#' hgvs/vep.vcf)
```

With vepyr 0.10.0 and the cache revision it pins (`5b83dd8`), `vepyr_out.vcf`
has the same records as `hgvs/vepyr.vcf`, byte for byte; this was checked for
all eleven classes. The `vep.vcf` records are those of the full chr15 VEP run
described in [`../../README.md`](../../README.md); a fix is complete when the
`diff` above prints nothing.
