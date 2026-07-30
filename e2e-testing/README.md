# E2E Testing

Release-qualified end-to-end annotation benchmarks comparing vepyr with the
exact Ensembl VEP 115.2 and 116.0 codebases on the full HG002 GRCh38 WGS
dataset (4,096,123 variants across chr1–22).

## Prerequisites

### 1. Build vepyr

Follow the main [README.md](../README.md) to set up the project:

```bash
cd ..
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

### 2. External data

The scripts expect data files under `~/workspace/data_vepyr/`. Set `DATA_VEPYR_DIR` or use CLI flags if your layout differs.

| Data | Description | Default path |
|------|-------------|-------------|
| VCF input | HG002 GRCh38 benchmark VCF (GIAB) | `~/workspace/data_vepyr/input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` |
| VEP references | Exact 115.2/116.0 golden VCFs | `~/workspace/data_vepyr/output/{115.2,116}/` |
| Converted caches | 115/116 × Ensembl/merged/RefSeq Parquet caches | `~/workspace/data_vepyr/cache/<release>_GRCh38_<type>/` |
| Raw caches | Extracted Ensembl VEP caches containing `info.txt` | `~/workspace/data_vepyr/homo_sapiens{,_ensembl,_merged,_refseq}/<release>_GRCh38/` |
| Reference FASTA | GRCh38 primary assembly | `~/workspace/data_vepyr/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa` |

### 3. System tools

- `bcftools`, `bgzip`, `tabix` (for VCF normalization and chromosome extraction)

## Bumping upstream dependencies

When a fix lands in `datafusion-bio-functions` or `datafusion-bio-formats`,
update the exact Git revision in `Cargo.toml`, refresh `Cargo.lock`, and rebuild:

```bash
# 1. Get the exact commit SHA that passed the upstream PR checks.

# 2. Update every dependency from that repository to the same exact rev.
#    Never qualify a release with a local path patch.

# 3. Refresh only the changed pinned packages and rebuild the native package.
cargo update -p datafusion-bio-function-vep
cargo update -p datafusion-bio-format-ensembl-cache
cargo update -p datafusion-bio-format-vcf
cd /path/to/vepyr
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 4. Verify the complete suite.
uv run pytest
```

The relevant lines in `Cargo.toml`:

```toml
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", rev = "<40-character-sha>", ... }
datafusion-bio-format-ensembl-cache = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", rev = "<40-character-sha>" }
datafusion-bio-format-vcf = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", rev = "<same-40-character-sha>" }
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

### `verify_parity_gate.py` -- machine-enforced release gate

The comparison runner produces evidence; the parity gate decides whether that
evidence is complete and releasable. It requires every requested contig report,
the exact compiled dependency provenance, matching VEP/cache identity, an empty
uncapped mismatch ledger, and zero structural, ordering, one-sided, or field
mismatches.

```bash
uv run python verify_parity_gate.py \
    --release 115 --profile ensembl --chroms 1-22
uv run python verify_parity_gate.py \
    --release 116 --profile refseq --chroms 1-22
```

### `rebuild_release_cache.py` -- authoritative complete-cache rebuild

This is the only full-cache rebuild command. It is a dry run by default. A real
run builds beside the live cache, verifies every manifest-referenced Parquet
footer, schema, release/source metadata value, and row total, then swaps with
rollback while retaining the previous cache as a timestamped backup.

```bash
# Preflight only
uv run python rebuild_release_cache.py \
    --release 116 --cache-type merged

# Build, verify, and swap
uv run python rebuild_release_cache.py \
    --release 116 --cache-type merged --run

# Verify an existing cache without rebuilding
uv run python rebuild_release_cache.py \
    --release 116 --cache-type merged \
    --verify-only ~/workspace/data_vepyr/cache/116_GRCh38_merged
```

### `rebuild_cache_entity.py` -- targeted transactional entity rebuild

Use this when a change is isolated to one raw entity: `variation`,
`transcript`, `exon`, `translation`, `regulatory`, or `motif`. It invokes the
public release-aware `vepyr.build_cache_entity()` API and validates every
manifest shard, footer row count, schema, and Parquet identity value before
swapping. Entity-specific checks enforce the release-116 variation and motif
contracts. The raw `translation` entity produces `translation_core` and
`translation_sift`; both are verified and swapped as one transaction. Previous
generated directories are retained as hidden, timestamped sibling backups.

```bash
# Preflight and verify the current translation outputs
uv run python rebuild_cache_entity.py \
    --release 116 --cache-type merged --entity translation

# Build, fully verify, and swap only variation
uv run python rebuild_cache_entity.py \
    --release 116 --cache-type merged --entity variation --run
```

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

The baseline Ensembl, merged, and RefSeq profiles are qualified and available
for both supported releases:

| Release | Ensembl | merged | RefSeq |
|---|---|---|---|
| 115 / VEP 115.2 | qualified, zero mismatches | qualified, zero mismatches | qualified, zero mismatches |
| 116 / VEP 116.0 | qualified, zero mismatches | qualified, zero mismatches | qualified, zero mismatches |

Optional selection profiles depend on their corresponding reference VCFs.
Passing an unavailable combination prints the live availability matrix and
fails before normalization.

## Typical workflow after a dependency bump

```bash
# 1. Bump rev in Cargo.toml and rebuild
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr

# 2. Run unit tests
uv run pytest

# 3. Run and gate all six release baselines
cd e2e-testing/scripts
for release in 115 116; do
  for profile in ensembl merged refseq; do
    uv run python run_comparison.py \
      --release "$release" --profile "$profile" --force
    uv run python verify_parity_gate.py \
      --release "$release" --profile "$profile" --chroms 1-22
  done
done

# 4. Compare a new report against the previous one
#    Reports are timestamped so you can diff them:
diff reports/fast_chr1_chr22_merged_115_summary_YYYYMMDD_HHMM.md \
     reports/fast_chr1_chr22_merged_115_summary_YYYYMMDD_HHMM.md
```

## Directory layout

```
e2e-testing/
  scripts/
    run_comparison.py                  # release-qualified E2E entry point
    verify_parity_gate.py              # machine zero-mismatch gate
    rebuild_release_cache.py           # complete transactional rebuild/verifier
    rebuild_cache_entity.py            # targeted transactional entity rebuild
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
