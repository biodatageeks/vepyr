# Quick start

## Installation

### From PyPI

```bash
pip install vepyr
```

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
uv run python -c "import vepyr; print('build_cache_entity' in vepyr.__all__)"
# True
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

To rebuild a single raw entity without converting the full cache:

```python
results = vepyr.build_cache_entity(
    release=116,
    cache_dir="/data/vepyr_cache",
    entity="motif",
    cache_type="merged",
    local_cache="/data/ensembl_vep/homo_sapiens_merged/116_GRCh38",
    overwrite=True,
)
```

This uses the same strict release/source validation as `build_cache()`.

#### Options

| Parameter | Default | Description |
|---|---|---|
| `partitions` | `8` | DataFusion partitions for parallel conversion |
| `species` | `homo_sapiens` | Species name |
| `assembly` | `GRCh38` | Genome assembly |
| `cache_type` | required | Ensembl VEP cache type: `ensembl`, `merged`, or `refseq` |

## Annotating variants

### Basic annotation

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
concurrently when writing with `output_vcf`; it requires a tabix-indexed
(bgzip + `.tbi`) input VCF. The LazyFrame path is serial (`workers=1`) in
this release.

```python
df = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    workers=4,
).collect()
```

### Region filters

Filtering the LazyFrame on `chrom`, `start` or `end` is pushed into the
engine before annotation: contigs outside the filter are never prepared, and
an indexed input (bgzip + `.tbi`/`.csi`) is read by seek.

```python
lf = vepyr.annotate("input.vcf.gz", cache_dir, everything=True, reference_fasta="GRCh38.fa")
df = lf.filter(
    (pl.col("chrom") == "chr22") & pl.col("start").is_between(20_000_000, 25_000_000)
).collect()
```

The result is always identical to filtering after `collect()`; only the work
changes. Coordinates are the frame's own `start`/`end` columns (1-based,
closed). Recognised shapes:

- `chrom` conjuncts: `==`, `!=`, `is_in`, `str.starts_with` and boolean
  combinations of them.
- `start`/`end` conjuncts: comparisons with an integer literal and
  `is_between`. `end <= b` bounds the range; `end >= a` does not.
- Several regions: an `|` of `(chrom & range)` groups, one region per group.

Anything else (a float literal, a range compared to another column, a cast,
an `|` *inside* a range conjunct) is not pushed down and is applied by Polars
after annotation, which is still correct, just not faster.

Without a tabix/CSI index next to the input a `RuntimeWarning` is emitted:
the whole file is parsed and filtered before annotation, and only the
selected rows are annotated. On Merged and RefSeq caches a range costs one
extra positional pass over each selected contig, which keeps the result
byte-identical to a whole-file run.

To use vepyr as a lightweight plugin annotator, select VEP's core VCF fields.
Plugin outputs become named DataFrame columns, so keeping the raw `CSQ` string
is optional:

```python
df = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/parquet/116_GRCh38_ensembl",
    fields="core",
    plugin_cache_root="/data/plugin_cache",
    plugins=["cadd", "spliceai"],
).collect()
```

### Writing annotated VCF output

Write results directly to a VCF file instead of returning a LazyFrame:

```python
out_path = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    everything=True,
    reference_fasta="GRCh38.fa",
    output_vcf="annotated.vcf.gz",  # .vcf.gz for bgzf, .vcf for plain
)
print(f"Wrote annotated VCF to {out_path}")
```
