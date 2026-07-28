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

### `run_comparison.py` -- vepyr vs Ensembl VEP parity

Annotates HG002 against a Parquet cache and compares the result field-by-field
against an Ensembl VEP reference. Handles one contig or all of them, and accepts
plain or block-gzipped VCFs on both sides.

`--release` is required: it selects both the Parquet cache and the VEP reference,
so the two can never be silently mismatched.

```bash
cd scripts

# All contigs detected from the reference index
uv run python run_comparison.py --release 115

# One contig
uv run python run_comparison.py --release 115 --chroms 22

# Several contigs, a different scenario and release
uv run python run_comparison.py --release 116 --profile merged --chroms 1 2 22

# Re-annotate instead of reusing existing output
uv run python run_comparison.py --release 115 --chroms 22 --force

# Block-gzipped output, validated as BGZF
uv run python run_comparison.py --release 115 --chroms 22 --bgzf

# Within-contig parallel pipelines (requires a tabix-indexed input)
uv run python run_comparison.py --release 115 --chroms 22 --workers 4

# One subprocess per contig, so a native crash loses only that contig
uv run python run_comparison.py --release 115 --isolate

# Annotate only, no comparison -- useful for annotation timing
uv run python run_comparison.py --release 115 --chroms 22 --skip-compare

# Regenerate the summary from existing per-contig JSONs (instant)
uv run python run_comparison.py --release 115 --skip-annotate

# Explicit paths override everything derived from profile x release
uv run python run_comparison.py --release 115 --chroms 22 \
    --vcf /path/to/input.vcf.gz \
    --vep /path/to/vep_output.vcf.gz \
    --cache-dir /path/to/cache \
    --fasta /path/to/reference.fa
```

**Defaults:** `--profile merged`, reuse existing output (`--force` to
re-annotate), plain output (`--bgzf` for block-gzipped), `--workers 1`,
normalization on (`--no-normalize` to skip), contigs detected from the
reference index.

**Contig detection** reads the tabix index, not the `##contig` headers: the
real VEP references list 195 contigs in their headers while only 22 carry
records. The detected set is the reference index intersected with the input
index, in coordinate order.

**Output:**

```
results/{release}/_shared/normalized.vcf.gz     # + .tbi, + normalized.source.json
results/{release}/fast_{chrom}/                 # per-contig slices and annotated output
reports/fast_{chrom}_{profile}_{release}_report.json
reports/fast_{span}_{profile}_{release}_summary_{timestamp}.md
```

Every intermediate lives under `results/{release}/`, so nothing a run reads can
come from a different release. Normalization is shared by every contig of a
release and re-runs automatically if `--vcf` changes.

The aggregate summary contains a per-contig performance table, root cause
classification with GitHub issue links, field-level delta vs the previous
benchmark, and mismatch examples per field.

**Data layout** under `$DATA_VEPYR_DIR` (default `~/workspace/data_vepyr`):

```
input/                                     # benchmark VCF, reference FASTA
cache/{release}_GRCh38_{flavour}/          # vepyr Parquet caches
output/{115.2,116}/                        # Ensembl VEP reference VCFs
```

Both `input/` and `cache/` fall back to the directory root with a warning, so
the runner works before and after those files are reorganised.

**Supported `--profile` values:**

| Test scenario (`--profile`) | VEP reference flags | Golden truth sample |
|---------------------------|---------------------|---------------------|
| `ensembl` | Ensembl cache baseline | `HG002_annotated_wgs_everything_hgvs_vep.vcf.gz` |
| `merged` | `--merged` baseline | `HG002_annotated_wgs_everything_hgvs_merged.vcf.gz` |
| `merged_pick_filter` | `--merged --pick` | `HG002_annotated_wgs_everything_hgvs_merged_pick_filter.vcf.gz` |
| `merged_pick_allele` | `--merged --pick_allele` | `HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf.gz` |
| `merged_per_gene` | `--merged --per_gene` | `HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf.gz` |
| `merged_pick_allele_gene` | `--merged --pick_allele_gene` | `HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene.vcf.gz` |
| `merged_flag_pick` | `--merged --flag_pick` | `HG002_annotated_wgs_everything_hgvs_merged_flag_pick.vcf.gz` |
| `merged_flag_pick_allele` | `--merged --flag_pick_allele` | `HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele.vcf.gz` |
| `merged_flag_pick_allele_gene` | `--merged --flag_pick_allele_gene` | `HG002_annotated_wgs_everything_hgvs_merged_pick.vcf.gz` |
| `refseq` | `--refseq` baseline | `HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz` |

Not every profile is available at every release. Pass an unavailable pair to
print the availability matrix -- the run fails immediately rather than after a
normalization pass:

```bash
uv run python run_comparison.py --release 116 --profile refseq
# Error: Profile 'refseq' at release 116: no Parquet cache at .../cache/116_GRCh38_refseq
#
# Available combinations:
# profile                                   115          116
# ensembl                                    ok            -
# merged                                     ok           ok
# refseq                                     ok     no cache
# ...
```

## Typical workflow after a dependency bump

```bash
# 1. Bump rev in Cargo.toml and rebuild
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 2. Run unit tests
uv run pytest

# 3. Run full e2e benchmark
cd e2e-testing/scripts
uv run python run_comparison.py --release 115 --force

# 4. Compare the new report against the previous one
#    Reports are timestamped so you can diff them:
diff reports/fast_chr1_chr22_merged_115_summary_YYYYMMDD_HHMM.md \
     reports/fast_chr1_chr22_merged_115_summary_YYYYMMDD_HHMM.md
```

## Directory layout

```
e2e-testing/
  scripts/
    run_comparison.py                  # entry point
    comparison/
      profiles.py                      # profile x release matrix, path derivation
      vcfio.py                         # compression, indexing, contig detection, slicing
      compare.py                       # CSQ comparison (pure)
      annotate.py                      # the only vepyr importer
      report.py                        # aggregation, classification, Markdown
      cli.py                           # argparse and orchestration
  reports/
    fast_{chrom}_{profile}_{release}_report.json      # per-contig results
    fast_{span}_{profile}_{release}_summary_*.md      # timestamped aggregates
  results/
    {release}/
      _shared/normalized.vcf.gz        # normalized input, shared by every contig
      fast_{chrom}/                    # per-contig intermediate files
```

Reports predating the release axis (`fast_{chrom}_{profile}_report.json`) are
still loaded by `--skip-annotate`, with a notice, so historical runs remain
readable.
