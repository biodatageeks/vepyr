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

The scripts expect data files under `~/workspace/data_vepyr/`. Set `DATA_VEPYR_DIR` or use CLI flags if your layout differs.

| File | Description | Default path |
|------|-------------|-------------|
| VCF input | HG002 GRCh38 benchmark VCF (GIAB) | `~/workspace/data_vepyr/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` |
| VEP reference | Golden Ensembl VEP 115 `--everything --hgvs` output | `~/workspace/data_vepyr/HG002_annotated_wgs_everything_hgvs_vep.vcf` |
| Cache dir | Converted Ensembl 115 partitioned Parquet cache | `~/workspace/data_vepyr/115_GRCh38_ensembl` |
| Reference FASTA | GRCh38 primary assembly | `~/workspace/data_vepyr/Homo_sapiens.GRCh38.dna.primary_assembly.fa` |

### 3. System tools

- `bcftools`, `bgzip`, `tabix` (for VCF normalization and chromosome extraction)

## Bumping upstream dependencies

When a fix lands in `datafusion-bio-functions` or `datafusion-bio-formats`, bump the pinned git revision in `Cargo.toml` and rebuild:

```bash
# 1. Get the released tag you want to pin to
#    (e.g. from a release in biodatageeks/datafusion-bio-functions)

# 2. Update the tag in Cargo.toml
#    Edit the datafusion-bio-function-vep line:
#      tag = "<new-tag>"
#    And/or the datafusion-bio-format-vcf line if formats changed.

# 3. Rebuild
cd /path/to/vepyr
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 4. Verify unit tests still pass
uv run pytest
```

The relevant lines in `Cargo.toml`:

```toml
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", tag = "..." }
datafusion-bio-format-vcf   = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", tag = "..." }
```

## Scripts

### `run_annotation_fast.py` -- single chromosome

Annotate one chromosome against the partitioned Parquet cache and compare
against VEP. Pass `--bgzf` to write (and validate) block-gzipped `.vcf.gz`
output instead of plain `.vcf`.

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
uv run python run_annotation_fast.py chr1 --skip-comparison

# Emit + validate block-gzipped (.vcf.gz) annotated output
uv run python run_annotation_fast.py chr1 --bgzf

# Use annotation workers for within-contig parallelism (requires tabix index)
uv run python run_annotation_fast.py chr22 --workers 4 --force

# Same option works with cache profiles
uv run python run_annotation_fast.py chr22 --cache merged_pick_filter \
    --workers 4 \
    --force

# Compare against a merged-cache VEP pick-mode reference
uv run python run_annotation_fast.py chr22 --cache merged_pick_filter
uv run python run_annotation_fast.py chr22 --cache merged_flag_pick_allele_gene
```

**Output:**
- `results/fast_chr{N}/` -- intermediate VCF files (`.vcf`, or `.vcf.gz` with `--bgzf`)
- `reports/fast_chr{N}{cache_suffix}_report.json` -- comparison report

Supported `--cache` profiles and golden truth samples:

Golden truth VCF paths are resolved under `DATA_VEPYR_DIR` (default:
`~/workspace/data_vepyr`) unless `--vep` is passed explicitly.

| Test scenario (`--cache`) | VEP reference flags | Golden truth sample |
|---------------------------|---------------------|---------------------|
| `ensembl` | Ensembl cache baseline | `HG002_annotated_wgs_everything_hgvs_vep.vcf` |
| `merged` | `--merged` baseline | `HG002_annotated_wgs_everything_hgvs_merged.vcf` |
| `merged_pick_filter` | `--merged --pick` | `HG002_annotated_wgs_everything_hgvs_merged_pick_filter.vcf` |
| `merged_pick_allele` | `--merged --pick_allele` | `HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf` |
| `merged_per_gene` | `--merged --per_gene` | `HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf` |
| `merged_pick_allele_gene` | `--merged --pick_allele_gene` | `HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene.vcf` |
| `merged_flag_pick` | `--merged --flag_pick` | `HG002_annotated_wgs_everything_hgvs_merged_flag_pick.vcf` |
| `merged_flag_pick_allele` | `--merged --flag_pick_allele` | `HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele.vcf` |
| `merged_flag_pick_allele_gene` | `--merged --flag_pick_allele_gene` | `HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele_gene.vcf` |
| `refseq` | `--refseq` baseline | `HG002_annotated_wgs_everything_hgvs_refseq.vcf` |

### `run_annotation_fast_all.py` -- full chr1-22 report

Run all 22 chromosomes and generate a timestamped Markdown summary with root
cause classification and upstream issue links. Annotates against the
partitioned Parquet cache; pass `--bgzf` to emit + validate block-gzipped
output.

```bash
cd scripts

# Full run -- always re-annotates by default (~6-7 min on a 16-core machine)
uv run python run_annotation_fast_all.py

# Reuse existing annotation output (only re-run comparison + report)
uv run python run_annotation_fast_all.py --no-force

# Only specific chromosomes
uv run python run_annotation_fast_all.py --chroms 1 6 22

# Run a pick-mode profile across chr1-22
uv run python run_annotation_fast_all.py --cache merged_pick_allele_gene

# Emit + validate block-gzipped output across chr1-22
uv run python run_annotation_fast_all.py --bgzf

# Run across chr1-22 with within-contig annotation workers
uv run python run_annotation_fast_all.py --workers 4

# Annotate all selected chromosomes without VEP comparison or aggregate report
uv run python run_annotation_fast_all.py --skip-comparison

# Regenerate report from existing per-chromosome JSONs (instant)
uv run python run_annotation_fast_all.py --skip-annotate
```

**Output:**
- `reports/fast_chr1_22{cache_suffix}_summary_YYYYMMDD_HHMM.md` -- aggregate report
  - Per-chromosome performance table
  - Root cause classification with GitHub issue links
  - Field-level delta vs previous benchmark
  - Mismatch examples per field

### Annotation workers

Use `--workers N` to choose how many within-contig annotation pipelines run
concurrently. `--workers 1` is the serial path; `--workers > 1` requires a
tabix-indexed (bgzip + `.tbi`) input VCF:

```bash
cd scripts

# One chromosome, re-annotating even if a previous VCF exists
uv run python run_annotation_fast.py chr22 \
    --workers 4 \
    --force

# All autosomes
uv run python run_annotation_fast_all.py \
    --workers 4

# All autosomes, annotation timing only
uv run python run_annotation_fast_all.py \
    --workers 4 \
    --skip-comparison

# A pick-mode cache profile with within-contig workers
uv run python run_annotation_fast_all.py \
    --cache merged_pick_allele_gene \
    --workers 4
```

`--workers` defaults to `1`. If
output files already exist, pass `--force` to `run_annotation_fast.py` or omit
`--no-force` from `run_annotation_fast_all.py` so timing reflects the new
worker setting. Pass `--skip-comparison` when you only need annotation
timing; the all-chromosome wrapper forwards skip mode to each chromosome run
and does not create an aggregate comparison report.

## Typical workflow after a dependency bump

```bash
# 1. Bump rev in Cargo.toml and rebuild
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 2. Run unit tests
uv run pytest

# 3. Run full e2e benchmark
cd e2e-testing/scripts
uv run python run_annotation_fast_all.py

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
  reports/
    fast_chr{N}_report.json      # per-chromosome comparison results
    fast_chr1_22_summary_*.md    # timestamped aggregate reports
  results/
    fast_chr{N}/                 # per-chromosome intermediate files
    normalized.vcf.gz            # normalized input VCF (shared)
```
