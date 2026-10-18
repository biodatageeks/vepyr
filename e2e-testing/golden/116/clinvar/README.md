# ClinVar parity check: VEP 116.0 vs vepyr, merged cache

All GRCh38 records of the ClinVar release of 2026-10-11 are annotated by
Ensembl VEP 116.0 and by vepyr with the merged cache. The two outputs are
compared per contig by body md5, the same check the HG002 golden suite runs
(`run_comparison.py --comparison-mode md5 --md5-mode both`).

This directory keeps the description of the run, the checksums of every input
(`manifest.json`) and the comparison results. The VCFs are too large for Git and
stay in the data directory (`$DATA_VEPYR_DIR`, default `~/workspace/data_vepyr`):

| File | Location |
|------|----------|
| ClinVar release, index, md5 | `input/clinvar/clinvar_20261011.vcf.gz{,.tbi,.md5}` |
| Preprocessed input | `input/clinvar/clinvar_20261011.norm.vcf.gz{,.tbi}` |
| VEP reference (all contigs) | `output/116/clinvar/clinvar_20261011_vep116_merged.vcf.gz{,.tbi}` |
| VEP reference per contig, logs, timings | `output/116/clinvar/{contigs,logs,timings.tsv}` |
| vepyr outputs and reports | `clinvar_e2e/{results,reports}/` |

## 1. Input

Source: <https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/>, files
`clinvar_20261011.vcf.gz` (md5 `0f3a1a5e84252386033d5fcab1bf1f6a`, as published
in `clinvar_20261011.vcf.gz.md5`) and its tabix index.

What the release contains:

| | Records |
|---|---:|
| All records | 4,594,764 |
| Chromosomes 1-22, X, Y, MT | 4,594,713 |
| RefSeq scaffolds (`NT_*`, `NW_*`, 7 contigs) | 51 |
| No alternate allele (`ALT="."`) | 1,341 |
| Multiallelic records | 0 |
| Symbolic (`<DEL>`, `<DUP>`, ...) or breakend alleles | 0 |
| An allele of 50 bp or more (longest 9,983 bp) | 6,798 |

## 2. Preprocessing

```bash
bcftools view -e 'ALT="."' \
    -t 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT \
    clinvar_20261011.vcf.gz -Ou \
  | bcftools norm -m -both -Oz -o clinvar_20261011.norm.vcf.gz
tabix -p vcf clinvar_20261011.norm.vcf.gz
```

Run with bcftools 1.23.1 (htslib 1.24). The three steps:

1. **`-e 'ALT="."'` removes the 1,341 records with no alternate allele.** In
   `--vcf` mode VEP drops such lines from its output unless
   `--allow_non_variant` is given, and the golden runs do not give it. There is
   nothing to annotate in them, and they would only leave the two outputs with
   different record sets.
2. **`-t 1,...,22,X,Y,MT` removes the 51 records on RefSeq scaffold accessions.**
   The comparison covers the 25 chromosomes, as the HG002 suite covers
   chromosomes 1-22.
3. **`bcftools norm -m -both` splits multiallelic records**, the step applied to
   the HG002 input. ClinVar has one ALT per record, so nothing is split here
   (`total/split = 4593372/0`). The step is kept so that both inputs go through
   the same procedure. As for HG002, there is no left-alignment (`-f`) and no
   REF check.

Nothing else is removed. The file has no structural variants in the VEP sense
(symbolic or breakend alleles). The 6,795 remaining records with an allele of
50 bp or more carry their full sequence, and both VEP and vepyr annotate them as
ordinary sequence variants, so they stay in the input and in the comparison.

The result has **4,593,372 records**. bcftools writes the run date into the
header, so the file checksum changes between runs; the md5 of the records
(`bcftools view -H clinvar_20261011.norm.vcf.gz | md5sum`) is
`547285cb0e2b3f97c7222156a748c371`.

## 3. VEP reference

```bash
VEP_REFERENCE_JOBS=8 e2e-testing/scripts/generate_vep_clinvar_reference.sh
```

The script downloads the release if it is missing, preprocesses it as above
and runs `ensemblorg/ensembl-vep:release_116.0` with the flags of the HG002
golden runs ([`vep-docker.md`](../../../vep-docker.md)):

```
vep --dir /opt/vep/.vep --cache --offline --assembly GRCh38 --merged \
    --vcf --force_overwrite --no_stats --everything --hgvs \
    --fasta Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

VEP runs without `--fork`. Each contig is a separate VEP process on its own
slice of the preprocessed input, and `VEP_REFERENCE_JOBS` contigs run at the
same time. The script warns when a contig output holds fewer records than its
input. The contig outputs are then joined in input order by copying their records as text:
`bcftools concat` would reprint ClinVar's floating-point INFO values and change
the bytes the md5 check compares.

The raw VEP cache must be extracted first. The archive holds
`homo_sapiens_merged/116_GRCh38/`, and Ensembl publishes its BSD `sum` in
`CHECKSUMS` (`26554 29618744`):

```bash
curl -O https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz
sum homo_sapiens_merged_vep_116_GRCh38.tar.gz   # 26554 29618744
tar -xzf homo_sapiens_merged_vep_116_GRCh38.tar.gz -C "$DATA_VEPYR_DIR"
```

## 4. vepyr and the md5 comparison

The vepyr side is the existing runner, unchanged. `--no-normalize` because the
input is already preprocessed:

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache "$DATA_VEPYR_DIR/cache"

python e2e-testing/scripts/run_comparison.py \
  --release 116 --profile merged --chroms all \
  --vcf "$DATA_VEPYR_DIR/input/clinvar/clinvar_20261011.norm.vcf.gz" \
  --vep "$DATA_VEPYR_DIR/output/116/clinvar/clinvar_20261011_vep116_merged.vcf.gz" \
  --cache-dir "$DATA_VEPYR_DIR/cache/116_GRCh38_merged" \
  --output-dir "$DATA_VEPYR_DIR/clinvar_e2e" \
  --comparison-mode md5 --md5-mode both --bgzf --no-normalize --workers 8
```

## 5. Results

vepyr 0.10.0 from PyPI, whose engine is the one on `master` (no engine change
since the 0.10.0 tag), against the cache revision pinned by that release
(`biodatageeks/vepyr_116_GRCh38_merged@5b83dd8`). The VEP references of the
contigs below are stored here (`merged_chr<contig>.vcf.gz`, Git LFS), with the
runner's reports in `reports/` and all checksums in `manifest.json`.

| Contig | Records | md5 strict | md5 canonical | VEP (1 process) | vepyr (2 workers) |
|---|---:|---|---|---:|---:|
| chrY | 697 | match | match | 13 s | 8.5 s |
| chrMT | 3,134 | **differs** | **differs** | 26 s | 4.7 s |

The remaining contigs are running.

### chrMT

2,327 of the 3,134 records differ, for three independent reasons:

1. **CSQ entry order, 2,030 records with identical content.** All of them
   differ by the same swap: VEP writes the RefSeq transcript `ND4L` before
   `ND4`, vepyr writes `ND4` first (plain alphabetical order). The rest of the
   order agrees.
2. **Mitochondrial codon table, 294 records.** vepyr translates MT transcripts
   with the standard genetic code instead of the vertebrate mitochondrial one.
   For example MT:3307 A>T in ENST00000361390 (ND1): ATA is Met in mitochondria,
   so VEP writes `M/L` and `p.Met1?`, vepyr `I/L` and `p.Ile1?`.
   `Amino_acids` (543 entries), `HGVSp` (541), `Consequence` (281, e.g. VEP
   `stop_gained&start_lost` against vepyr `start_lost`), `IMPACT` (273), `SIFT`
   (239) and `PolyPhen` (249) follow from it. Most cases are in CYTB, ATP6, ND5
   and COX1.
3. **HGVSc and HGVS_OFFSET of four indels, 4 records.** For the 9,120 bp
   deletion at MT:6467 VEP writes a deletion of each transcript that lies
   inside it, e.g. `ATP6.1:c.1_681del`; vepyr leaves `HGVSc` empty. For three
   1 bp indels in MT-TT (MT:15933, 15939) vepyr places the variant 3 bases
   further towards the 3' end than VEP (`n.50del` against `n.47del`).

chrMT is outside the HG002 validation, which covers chromosomes 1-22, so these
are new findings rather than regressions. All differing records, and a small
input/VEP/vepyr fixture for each class, are in
[`mismatches/chrMT/`](mismatches/chrMT/README.md).
