# VEP scalability benchmark

This directory contains reproducible scripts and captured results for running
Ensembl VEP on a normalized HG002 GRCh38 WGS benchmark VCF with the merged cache.

The benchmark varies VEP's `--fork` parameter while keeping the input, cache,
FASTA, Docker image, and `--buffer_size 20000` fixed.

## Layout

- `scripts/normalize_vcf.sh` - decompose and normalize the benchmark VCF with `bcftools norm`.
- `scripts/run_vep_scalability_benchmark.sh` - run VEP for one or more fork settings and append `summary.tsv`.
- `scripts/plot_vep_scalability.py` - regenerate the timing plot from a summary table.
- `results/wgs/` - full WGS raw logs and summary, excluding the still-running `fork=1` run.
- `results/100k/` - 100k-variant compressed-input raw logs and summary.
- `results/100k-uncompressed/` - 100k-variant uncompressed-input comparison logs.
- `results/figures/` - generated PNG plots.

Large VEP output VCF files are intentionally not versioned.

## Data Assumptions

The scripts default to the local benchmark data layout used for these results:

```text
/home/tgambin/workspace/vep_data2/
  HG002_GRCh38_1_22_v4.2.1_benchmark.normalized.vcf.gz
  HG002_GRCh38_1_22_v4.2.1_benchmark.normalized.vcf.gz.tbi
  Homo_sapiens.GRCh38.dna.primary_assembly.fa
  homo_sapiens_merged/115_GRCh38/
```

The VEP Docker image is:

```text
ensemblorg/ensembl-vep:release_115.1
```

## Re-run

Run the full WGS benchmark with the default fork set:

```bash
performance-tests/vep/scripts/run_vep_scalability_benchmark.sh
```

Run selected fork values only:

```bash
performance-tests/vep/scripts/run_vep_scalability_benchmark.sh none 2 4 8 16
```

Regenerate the WGS plot:

```bash
python performance-tests/vep/scripts/plot_vep_scalability.py \
  performance-tests/vep/results/wgs/summary.tsv \
  performance-tests/vep/results/figures/vep_merged_fork_benchmark_wgs.png \
  --title "VEP merged cache WGS benchmark, buffer_size=20000"
```

## Current WGS Results

These full WGS results use `4,096,123` normalized input records. For every
completed fork setting, `input_records == output_records`.

| fork | elapsed | speedup vs none |
|---:|---:|---:|
| none | 9:19:55 | 1.00x |
| 2 | 3:11:38 | 2.92x |
| 4 | 2:03:40 | 4.53x |
| 8 | 1:22:13 | 6.81x |
| 16 | 1:01:21 | 9.13x |

Note: `fork=1` was still running when this preliminary branch was prepared, so
it is intentionally absent from the committed WGS summary and plot.
