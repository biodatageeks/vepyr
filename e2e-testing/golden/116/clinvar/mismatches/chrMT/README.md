# chrMT mismatches: VEP 116.0 vs vepyr 0.10.0, merged cache

ClinVar 20261011, chrMT: 2,327 of 3,134 records differ between Ensembl VEP and
vepyr. This directory lists all of them and keeps a small fixture for each kind
of difference, so that a fix can be checked without the whole run.

- `records.tsv`: every differing record, with its position, ClinVar ID, alleles
  (long ones shortened), class, whether the CSQ order differs, the differing CSQ
  fields, the transcripts concerned and the `Amino_acids` values (VEP>vepyr).
- `<class>/input.vcf`: records from the preprocessed ClinVar input.
- `<class>/vep.vcf`: the same records copied from the VEP 116.0 chrMT output.
- `<class>/vepyr.vcf`: the same records copied from the vepyr 0.10.0 output.

Each file keeps the full header of the file it comes from.

| Class | Records on chrMT | In the fixture | What differs |
|---|---:|---:|---|
| `csq_order` | 2,030 | 3 | Order of the CSQ entries only |
| `codon_table` | 294 | 12 | `Amino_acids`, `HGVSp`, `Consequence`, `IMPACT`, `SIFT`, `PolyPhen` |
| `hgvs_shift` | 4 | 4 | `HGVSc`, `HGVS_OFFSET` |

One record, the 9,120 bp deletion at MT:6467, is in both `codon_table` and
`hgvs_shift`.

## csq_order

The content of the CSQ entries is identical; only their order differs, and
always in the same way: VEP writes the RefSeq transcript `ND4L` before `ND4`,
vepyr writes `ND4` first (plain alphabetical order). The fixture holds MT:5772,
5773 and 5774.

## codon_table

vepyr translates mitochondrial transcripts with the standard genetic code. VEP
uses the vertebrate mitochondrial code, where ATA is Met instead of Ile, AGA
and AGG are stop codons instead of Arg, and TGA is Trp instead of a stop. The
fixture has one record per distinct amino-acid change, all in ND1
(ENST00000361390 and the RefSeq `ND1`):

| Position | ClinVar ID | Change | `Amino_acids` VEP > vepyr |
|---|---|---|---|
| 3307 | 692328 | A>T | `M/L` > `I/L` |
| 3308 | 692329 | T>AC | `M/NX` > `I/NX` |
| 3308 | 9728 | T>C | `M/T` > `I/T` |
| 3308 | 9729 | T>G | `M/*` > `I/R` |
| 3337 | 692337 | G>A | `V/M` > `V/I` |
| 3388 | 29998 | C>A | `L/M` > `L/I` |
| 3397 | 9726 | A>G | `M/V` > `I/V` |
| 3399 | 692352 | A>T | `M/I` > `I` |
| 3578 | 4850158 | T>G | `M/*` > `I/R` |
| 3669 | 1256442 | A>G | `W` > `*/W` |
| 3764 | 692398 | C>T | `T/M` > `T/I` |
| 4029 | 692415 | C>A | `I/M` > `I` |

## hgvs_shift

Four indels get a different `HGVSc` and `HGVS_OFFSET`:

- **MT:6467, a 9,120 bp deletion.** For each transcript that lies entirely
  inside the deletion VEP writes a deletion of the whole transcript, e.g.
  `ATP6.1:c.1_681del` or `TRNK.1:n.1_70del`; vepyr leaves `HGVSc` empty. For
  CYTB VEP writes `c.1_840del` and vepyr `c.3_842del` with `HGVS_OFFSET` 2.
- **MT:15933 and MT:15939 (two records), 1 bp indels in MT-TT** (`TRNT`,
  ENST00000387460). vepyr places the variant 3 bases further towards the 3'
  end than VEP: `n.50del` against `n.47del`, `n.57dup` against `n.54dup`,
  `n.57del` against `n.54del`.

## Reproducing

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache DIR --chrom MT
vepyr annotate -i codon_table/input.vcf -o vepyr_out.vcf \
  --dir_cache DIR/116_GRCh38_merged \
  --fasta Homo_sapiens.GRCh38.dna.primary_assembly.fa
diff <(grep -v '^#' vepyr_out.vcf) <(grep -v '^#' codon_table/vep.vcf)
```

With vepyr 0.10.0 and the cache revision it pins (`5b83dd8`), `vepyr_out.vcf`
has the same records as `codon_table/vepyr.vcf`, byte for byte; this was checked
for all three classes. The `vep.vcf` records are those of the full chrMT VEP run
described in [`../../README.md`](../../README.md); a fix is complete when the
`diff` above prints nothing.
