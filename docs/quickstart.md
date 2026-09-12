# Quick start

## Installation

### From PyPI

```bash
pip install vepyr
```

This installs the Python package and a `vepyr` executable — see
[Command line](cli.md).

### From bioconda

[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/vepyr)
[![Bioconda](https://img.shields.io/conda/vn/bioconda/vepyr?label=bioconda)](https://anaconda.org/bioconda/vepyr)
[![Bioconda - Platforms](https://img.shields.io/conda/pn/bioconda/vepyr?label=bioconda%20platforms)](https://anaconda.org/bioconda/vepyr)

```bash
conda install -c conda-forge -c bioconda vepyr
```

The package is built for linux-64, osx-64 and osx-arm64 and ships the same
`vepyr` executable. A new release reaches bioconda after its recipe update is
merged, so it can trail PyPI by a few days — check the badge above for the
current version.

### From source (for development)

vepyr requires a Rust toolchain and Python 3.10+.

1. Install [uv](https://docs.astral.sh/uv/) and [Rust](https://rustup.rs/):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. Clone and build:

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

3. Verify:

```bash
uv run python -c "import vepyr; print('build_cache' in vepyr.__all__)"
# True
uv run vepyr --version
```

## Getting a cache

Annotation needs an Ensembl VEP cache in vepyr's optimized Parquet format. You
have two options: **download a prebuilt one** (minutes, recommended) or **build
your own** from an Ensembl VEP offline cache (hours of CPU).

### Option A — download a prebuilt cache (recommended)

Release-116 GRCh38 caches for all three transcript sets are published on
Hugging Face. Install the client once:

```bash
pip install -U "huggingface_hub[cli]"
```

Then pull the cache type you want:

=== "merged"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_merged \
      --repo-type dataset \
      --local-dir ~/vepyr_cache/116_GRCh38_merged
    ```

=== "ensembl"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_ensembl \
      --repo-type dataset \
      --local-dir ~/vepyr_cache/116_GRCh38_ensembl
    ```

=== "refseq"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_refseq \
      --repo-type dataset \
      --local-dir ~/vepyr_cache/116_GRCh38_refseq
    ```

The download is resumable and the client verifies integrity, so there is no
separate checksum step. Budget 31–36 G of disk per cache.

Confirm the cache reports the release you expect before annotating — this opens
only the named contig's shards, so it is fast:

```python
import os
import vepyr

cache = os.path.expanduser("~/vepyr_cache/116_GRCh38_merged")
print(vepyr.cache_contig_identity(cache, "chr22", expected_cache_version="116"))
```

For a second mirror, per-contig partial downloads, and the four prebuilt plugin
caches, see [Download Ensembl VEP and plugin
caches](downloads.md).

### Option B — build your own cache

Use this for release 115, for a cache type or assembly that is not mirrored, or
when you want to convert a VEP cache you already hold.

#### Download and convert automatically

```python
import vepyr

results = vepyr.build_cache(
    release=115,
    cache_dir="/data/vepyr_cache",
    cache_type="ensembl",
)
for path, rows in results:
    print(f"{path}: {rows:,} rows")
```

This downloads the Ensembl VEP 115 cache for `homo_sapiens` / `GRCh38` and converts it to a partitioned Parquet cache.

#### Convert a local cache

If you already have the Ensembl VEP cache unpacked locally:

```python
results = vepyr.build_cache(
    release=115,
    cache_dir="/data/vepyr_cache",
    cache_type="ensembl",
    local_cache="/data/ensembl_vep/homo_sapiens/115_GRCh38",
)
```

To rebuild a single raw entity without converting the full cache, pass
`entity` (and optionally `chroms`):

```python
results = vepyr.build_cache(
    release=116,
    cache_dir="/data/vepyr_cache",
    cache_type="merged",
    entity="motif",
    local_cache="/data/ensembl_vep/homo_sapiens_merged/116_GRCh38",
    overwrite=True,
)
```

A targeted build uses the same strict release/source validation as a full one
and writes into the same `<release>_<assembly>_<cache_type>` directory, leaving
the other entities untouched.

#### Options

| Parameter | Default | Description |
|---|---|---|
| `partitions` | `8` | DataFusion partitions for parallel conversion |
| `species` | `homo_sapiens` | Species name |
| `assembly` | `GRCh38` | Genome assembly |
| `cache_type` | required | Ensembl VEP cache type: `ensembl`, `merged`, or `refseq` |

## Annotating variants

### Writing annotated VCF output

Write results directly to a VCF file with a `CSQ` INFO field:

```python
import vepyr

out_path = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    everything=True,
    reference_fasta="GRCh38.fa",
    output_vcf="annotated.vcf.gz",  # .vcf.gz for bgzf, .vcf for plain
)
print(f"Wrote annotated VCF to {out_path}")
```

For the supported scope the records are **byte-identical to Ensembl VEP's own
`--everything --hgvs` output** — not merely equivalent — so the file is a drop-in
replacement wherever a VEP VCF is expected. Only the provenance header lines
(wall-clock time, cache paths, tool versions) differ, since they can never match.
The e2e suite verifies this by hashing the record bodies of both files; see
[Checking byte-level agreement](testing-vep.md#checking-byte-level-agreement).

### Annotating to a Polars LazyFrame

Omit `output_vcf` and `annotate()` returns a Polars LazyFrame instead:

```python
import vepyr

lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    check_existing=True,
    af=True,
    max_af=True,
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "most_severe_consequence").head())
```

### Full `--everything` mode

Enable all annotation features (80-field CSQ). Requires a reference FASTA:

```python
lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    everything=True,
    reference_fasta="GRCh38.fa",
)

df = lf.collect()
print(f"{df.height} variants x {df.width} columns")
```

`workers` controls how many within-contig annotation pipelines run
concurrently, on both the LazyFrame and the `output_vcf` path. It requires a
tabix-indexed (bgzip + `.tbi` or `.csi`) input VCF. Output is identical to
`workers=1`, row order included.

```python
df = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    workers=4,
).collect()
```

Filtering the LazyFrame on `chrom`, `start` or `end` is pushed into the
engine before annotation; see [Polars DataFrames](dataframes.md#region-filters).

### From the command line

The quickest path from a VCF to an annotated VCF needs no Python at all:

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_merged \
    --fasta GRCh38.fa \
    --everything \
    --fork 8
```

The flags follow Ensembl VEP's own spelling. The command covers the VCF-in /
VCF-out path only; the LazyFrame path above is Python-side. See
[Command line](cli.md) for the full flag set, the plugin flags and the exit
codes.
