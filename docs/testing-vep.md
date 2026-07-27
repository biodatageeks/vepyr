# Testing against Ensembl VEP

vepyr's correctness bar is **zero mismatches against Ensembl VEP `--everything --hgvs`**
for the supported scope (`homo_sapiens`, GRCh38, Ensembl release 116). Reaching that bar
means running both tools over the same input and diffing the resulting CSQ fields
variant by variant.

This page covers the input preparation, the reference (golden truth) VEP run, and the
comparison harness under `e2e-testing/`.

## Test dataset

The parity suite runs on the GIAB **HG002 / NA24385** benchmark callset (Ashkenazim
son, NIST v4.2.1, GRCh38, chr1-22) — roughly 4M variants, large enough to exercise
every consequence type and small enough to re-run after every dependency bump.

| Item | Value |
|---|---|
| Source | GIAB AshkenazimTrio, `NISTv4.2.1/GRCh38` |
| Download size | ~156 MB (`.vcf.gz`) |
| Input records | 4,048,342 |
| Records after normalization | 4,096,123 |

## Preprocessing

All commands below assume a working directory that will hold the test data. The
e2e scripts default to `~/workspace/data_vepyr` and honour `DATA_VEPYR_DIR`.

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr
mkdir -p "$DATA_VEPYR_DIR"
cd "$DATA_VEPYR_DIR"
```

You need `bcftools`, `bgzip`, `tabix`, and `samtools` on `PATH`. The reference numbers
on this page were produced with **bcftools 1.21 / htslib 1.21**.

### 1. Download the benchmark VCF

```bash
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz

wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi
```

The `-c` flag resumes a partial download, which matters on this file size. Confirm you
got the same release used for the numbers on this page:

```bash
md5sum HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz
# dc750b3807d4af1f7ffec852e9c2f771
```

### 2. Normalize to biallelic records

VEP consequence calls are per-allele, so multiallelic sites must be decomposed into one
record per ALT allele before either tool sees them. Without this step the two tools
disagree on allele ordering and the comparison is meaningless.

```bash
bcftools norm -m -both \
  -o HG002_normalized.vcf \
  HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz
```

Expected output:

```
Lines   total/split/joined/realigned/removed/skipped:	4048342/47781/0/0/0/0
```

47,781 multiallelic sites split, giving 4,096,123 biallelic records in a ~2.9 GB plain
VCF. Note this is `-m -both` only — no `-f`/left-alignment against the reference, so the
step is deterministic and needs no FASTA.

### 3. Compress and index

```bash
bgzip -f HG002_normalized.vcf
tabix -p vcf HG002_normalized.vcf.gz
```

`bgzip` replaces the 2.9 GB plain VCF with a ~151 MB `.vcf.gz`, and `tabix` writes the
`.tbi` beside it. Both tools consume this pair directly, so there is no reason to keep
the uncompressed copy:

- **vepyr** needs the index for within-contig parallelism (`workers` > 1) and for
  per-chromosome extraction in the e2e scripts.
- **Ensembl VEP** accepts a block-gzipped `--input_file` and decompresses it in a little
  under a second — negligible against a multi-hour run.

### 4. Reference FASTA

Needed by both tools for HGVS notation (`--hgvs` / `reference_fasta=`):

```bash
wget -c https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
samtools faidx Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

### 5. Ensembl VEP caches

VEP ships three GRCh38 caches for release 116, differing only in which transcript set
they contain. Download whichever ones you intend to compare against — the parity suite
uses all three:

| Cache | Transcripts | Download |
|---|---|---|
| Ensembl | Ensembl/GENCODE only | [`homo_sapiens_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_vep_116_GRCh38.tar.gz) |
| RefSeq | RefSeq only | [`homo_sapiens_refseq_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_refseq_vep_116_GRCh38.tar.gz) |
| Merged | Ensembl + RefSeq | [`homo_sapiens_merged_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz) |

These are the *indexed* caches, which is what vepyr's converter and VEP's `--offline`
mode both expect. The full FTP directory is
[`release-116/variation/indexed_vep_cache/`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/).

```bash
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_vep_116_GRCh38.tar.gz
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_refseq_vep_116_GRCh38.tar.gz
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz

for t in homo_sapiens homo_sapiens_refseq homo_sapiens_merged; do
  tar xzf "${t}_vep_116_GRCh38.tar.gz"
done
```

Each archive unpacks to a `<species_dir>/116_GRCh38/` tree — the directory name encodes
the transcript set, which is why the three can coexist under one root:

| Cache | Extracted path | vepyr Parquet cache |
|---|---|---|
| Ensembl | `homo_sapiens/116_GRCh38` | `116_GRCh38_ensembl` |
| RefSeq | `homo_sapiens_refseq/116_GRCh38` | `116_GRCh38_refseq` |
| Merged | `homo_sapiens_merged/116_GRCh38` | `116_GRCh38_merged` |

Convert each extracted cache to vepyr's Parquet format with
[`build_cache()`](caches.md) before annotating.

!!! note "Reproducibility of the normalized VCF"
    `bcftools norm` writes a `##bcftools_normCommand=` header line containing the output
    path and the wall-clock date of the run, so a whole-file checksum differs between two
    runs of the same command on the same input. Compare header and records separately:

    ```bash
    # Header: expect a single differing ##bcftools_normCommand= line
    diff <(bcftools view -h A.vcf.gz) <(bcftools view -h B.vcf.gz)

    # Records: expect no output and exit status 0
    cmp <(bcftools view -H A.vcf.gz) <(bcftools view -H B.vcf.gz)
    ```

    Re-running steps 1-3 on the md5 above reproduces the records byte for byte
    (md5 `f47259aebaa00b4eb6840c636ea14783` over all 4,096,123 records).

## Generating the reference VEP output

The golden truth is Ensembl VEP 116 run offline in Docker over the same normalized VCF.
This is the slow half of the exercise — a full chr1-22 `--everything --hgvs` run takes
**4-7 hours per cache**, so generate each once and keep it.

The three commands below differ only in the cache mounted, the transcript-set flag
(none / `--refseq` / `--merged`), and the output filename. All of them run
`--offline --cache --everything --hgvs` against the same
`HG002_normalized.vcf.gz` from the preprocessing steps.

=== "Ensembl"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens/116_GRCh38:/opt/vep/.vep/homo_sapiens/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR:/work" \
      -v "$DATA_VEPYR_DIR:/fasta:ro" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --offline \
      --assembly GRCh38 \
      --input_file /work/HG002_normalized.vcf.gz \
      --output_file /work/HG002_annotated_wgs_everything_hgvs_vep.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

=== "RefSeq"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens_refseq/116_GRCh38:/opt/vep/.vep/homo_sapiens_refseq/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR:/work" \
      -v "$DATA_VEPYR_DIR:/fasta:ro" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --refseq \
      --offline \
      --assembly GRCh38 \
      --input_file /work/HG002_normalized.vcf.gz \
      --output_file /work/HG002_annotated_wgs_everything_hgvs_refseq.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

=== "Merged"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens_merged/116_GRCh38:/opt/vep/.vep/homo_sapiens_merged/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR:/work" \
      -v "$DATA_VEPYR_DIR:/fasta:ro" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --merged \
      --offline \
      --assembly GRCh38 \
      --input_file /work/HG002_normalized.vcf.gz \
      --output_file /work/HG002_annotated_wgs_everything_hgvs_merged.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

!!! warning "BAM-edited cache"
    The RefSeq and merged caches are BAM-edited, so VEP logs
    `BAM-edited cache detected, enabling --use_transcript_ref` and annotates against the
    transcript reference rather than the REF allele you supplied. Pass `--use_given_ref`
    to override. Both caches also emit `WARNING: Transcript-assembly mismatch` lines for
    a few dozen variants; these are expected and are captured in the
    `*_warnings.txt` files next to the output.

The pick-mode reference commands (`--pick`, `--pick_allele`, `--per_gene`,
`--flag_pick`, ...) are recorded verbatim in
[`e2e-testing/vep-docker.md`](https://github.com/biodatageeks/vepyr/blob/master/e2e-testing/vep-docker.md),
which still documents the release 115 runs the current reports were generated from.
All pick modes use the ranking order
`biotype,rank,mane_select,tsl,canonical,appris,ccds,length`.

## Running the comparison

With the normalized input, the converted Parquet cache, and the reference VEP output in
place, the harness under `e2e-testing/scripts/` annotates with vepyr and diffs against
the reference field by field.

```bash
cd e2e-testing/scripts

# Single chromosome
uv run python run_annotation_fast.py chr1

# Full chr1-22 run with a timestamped Markdown report
uv run python run_annotation_fast_all.py

# A specific pick-mode profile
uv run python run_annotation_fast_all.py --profile merged_pick_allele_gene
```

Outputs land in `e2e-testing/reports/`: per-chromosome JSON
(`fast_chr{N}_report.json`) and an aggregate summary
(`fast_chr1_22_summary_YYYYMMDD_HHMM.md`) with per-chromosome timings, mismatch counts
classified by root cause, and per-field mismatch examples.

The scripts normalize the input themselves if a normalized copy is not already present,
using exactly the `bcftools norm -m -both` → `bgzip` → `tabix` sequence above — so
running the preprocessing by hand and pointing the scripts at the result gives identical
inputs.

See [`e2e-testing/README.md`](https://github.com/biodatageeks/vepyr/blob/master/e2e-testing/README.md)
for the full flag reference, the profile-to-reference-file mapping, and the
dependency-bump workflow.
