# Quick start

## Installation

### From source (recommended during development)

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
uv run python -c "import vepyr; print(vepyr.__all__)"
# ['build_cache', 'annotate']
```

## Building a cache

Before annotating variants you need to convert an Ensembl VEP offline cache to vepyr's optimized format.

### Download and convert automatically

```python
import vepyr

results = vepyr.build_cache(
    release=115,
    cache_dir="/data/vepyr_cache",
)
for path, rows in results:
    print(f"{path}: {rows:,} rows")
```

This downloads the Ensembl VEP 115 cache for `homo_sapiens` / `GRCh38`, converts it to Parquet, and builds fjall KV stores.

### Convert a local cache

If you already have the Ensembl VEP cache unpacked locally:

```python
results = vepyr.build_cache(
    release=115,
    cache_dir="/data/vepyr_cache",
    local_cache="/data/ensembl_vep/homo_sapiens/115_GRCh38",
)
```

### Options

| Parameter | Default | Description |
|---|---|---|
| `partitions` | `1` | DataFusion partitions for parallel conversion |
| `build_fjall` | `True` | Build fjall KV stores alongside Parquet |
| `fjall_zstd_level` | `3` | Zstd compression level (1-22) |
| `species` | `homo_sapiens` | Species name |
| `assembly` | `GRCh38` | Genome assembly |
| `cache_type` | `vep` | Cache type: `vep`, `merged`, or `refseq` |

## Annotating variants

### Basic annotation

```python
import vepyr

lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_vep",
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
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_vep",
    everything=True,
    reference_fasta="GRCh38.fa",
)

df = lf.collect()
print(f"{df.height} variants x {df.width} columns")
```

### Using the fjall backend

Pass `use_fjall=True` for faster co-located variant lookups on large caches:

```python
lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_vep",
    check_existing=True,
    af=True,
    max_af=True,
    use_fjall=True,
)
```

### Writing annotated VCF output

Write results directly to a VCF file instead of returning a LazyFrame:

```python
out_path = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_vep",
    everything=True,
    reference_fasta="GRCh38.fa",
    output_vcf="annotated.vcf.gz",  # .vcf.gz for bgzf, .vcf for plain
)
print(f"Wrote annotated VCF to {out_path}")
```
