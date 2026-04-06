# E2E Testing

End-to-end annotation benchmarks comparing vepyr against Ensembl VEP 115 on the full HG002 GRCh38 WGS dataset (4M+ variants, chr1-22).

## Prerequisites

### 1. Build vepyr

Follow the main [README.md](../README.md) to set up the project:

```bash
cd ..
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

### 2. External data

The scripts expect data files under `~/workspace/data_vepyr/`. Override paths with CLI flags if your layout differs.

| File | Description | Default path |
|------|-------------|-------------|
| VCF input | HG002 GRCh38 benchmark VCF (GIAB) | `~/workspace/data_vepyr/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` |
| VEP reference | VEP 115 `--everything --hgvs` output | `~/workspace/data_vepyr/HG002_annotated_wgs_everything_hgvs.vcf` |
| Cache dir | Converted Ensembl 115 cache (parquet + fjall) | `~/workspace/data_vepyr/115_GRCh38_vep` |
| Reference FASTA | GRCh38 primary assembly | `~/workspace/data_vepyr/Homo_sapiens.GRCh38.dna.primary_assembly.fa` |

### 3. System tools

- `bcftools`, `bgzip`, `tabix` (for VCF normalization and chromosome extraction)

## Bumping upstream dependencies

When a fix lands in `datafusion-bio-functions` or `datafusion-bio-formats`, bump the pinned git revision in `Cargo.toml` and rebuild:

```bash
# 1. Get the commit hash you want to pin to
#    (e.g. from a merged PR in biodatageeks/datafusion-bio-functions)

# 2. Update the rev in Cargo.toml
#    Edit the datafusion-bio-function-vep line:
#      rev = "<new-commit-hash>"
#    And/or the datafusion-bio-format-vcf line if formats changed.

# 3. Rebuild
cd /path/to/vepyr
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 4. Verify unit tests still pass
uv run pytest
```

The relevant lines in `Cargo.toml`:

```toml
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", rev = "..." }
datafusion-bio-format-vcf   = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", rev = "..." }
```

## Scripts

### `run_annotation_fast.py` -- single chromosome

Annotate one chromosome with the fjall backend and compare against VEP.

```bash
cd scripts

# Single chromosome
uv run python run_annotation_fast.py chr1

# With custom paths
uv run python run_annotation_fast.py chr22 \
    --vcf /path/to/input.vcf.gz \
    --vep /path/to/vep_output.vcf \
    --cache-dir /path/to/cache \
    --fasta /path/to/reference.fa

# Force re-annotation (ignore existing output)
uv run python run_annotation_fast.py chr1 --force

# Skip comparison, only annotate
uv run python run_annotation_fast.py chr1 --skip-compare
```

**Output:**
- `results/fast_chr{N}/` -- intermediate VCF files
- `reports/fast_chr{N}_report.json` -- per-field match rates, mismatch examples

### `run_annotation_fast_all.py` -- full chr1-22 report

Run all 22 chromosomes and generate a timestamped Markdown summary with root cause classification and upstream issue links.

```bash
cd scripts

# Full run -- always re-annotates by default (~6-7 min on a 16-core machine)
uv run python run_annotation_fast_all.py

# Reuse existing annotation output (only re-run comparison + report)
uv run python run_annotation_fast_all.py --no-force

# Only specific chromosomes
uv run python run_annotation_fast_all.py --chroms 1 6 22

# Regenerate report from existing per-chromosome JSONs (instant)
uv run python run_annotation_fast_all.py --skip-annotate
```

**Output:**
- `reports/fast_chr1_22_summary_YYYYMMDD.md` -- full report with:
  - Per-chromosome performance table
  - Root cause classification with GitHub issue links
  - Field-level delta vs previous benchmark
  - Mismatch examples per field

### `run_annotation.py` -- full genome benchmark (both backends)

Run the complete benchmark with both parquet and fjall backends, including backend-vs-backend and vepyr-vs-VEP comparisons. Takes ~20-30 min.

```bash
cd scripts
uv run python run_annotation.py
```

**Output:**
- `results/vepyr_parquet.vcf`, `results/vepyr_fjall.vcf`
- `reports/benchmark_report.json`, `reports/benchmark_report.md`

### `extract_all_mismatches.py` -- detailed mismatch TSV

Extract every field-level mismatch across all variants into TSV files for analysis.

```bash
cd scripts
uv run python extract_all_mismatches.py
```

**Output:**
- `reports/mismatches_parquet_vs_vep.tsv`
- `reports/mismatches_fjall_vs_vep.tsv`

### `classify_mismatches.py` -- root cause clustering

Classify extracted mismatches into root cause clusters (C1-C12).

```bash
cd scripts
uv run python classify_mismatches.py
```

**Output:**
- `reports/mismatches_*_classified.tsv` (with `cluster_id` column)

## Typical workflow after a dependency bump

```bash
# 1. Bump rev in Cargo.toml and rebuild
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 2. Run unit tests
uv run pytest

# 3. Run full e2e benchmark
cd e2e-testing/scripts
uv run python run_annotation_fast_all.py --force

# 4. Compare the new report against the previous one
#    Reports are timestamped so you can diff them:
diff reports/fast_chr1_22_summary_YYYYMMDD.md reports/fast_chr1_22_summary_YYYYMMDD.md
```

## Directory layout

```
e2e-testing/
  scripts/
    run_annotation_fast.py       # single-chromosome annotation + comparison
    run_annotation_fast_all.py   # chr1-22 orchestrator + report generator
    run_annotation.py            # full genome benchmark (both backends)
    extract_all_mismatches.py    # TSV mismatch extraction
    classify_mismatches.py       # root cause clustering
  reports/
    fast_chr{N}_report.json      # per-chromosome comparison results
    fast_chr1_22_summary_*.md    # timestamped aggregate reports
    benchmark_report.json        # full genome benchmark (JSON)
    benchmark_report.md          # full genome benchmark (Markdown)
  results/
    fast_chr{N}/                 # per-chromosome intermediate files
    normalized.vcf.gz            # normalized input VCF (shared)
    vepyr_parquet.vcf            # full genome parquet output
    vepyr_fjall.vcf              # full genome fjall output
```
