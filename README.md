# vepyr
vepyr (/ˈvaɪpər/) — VEP Yielding Performant Results — a blazing-fast Rust reimplementation of Ensembl's Variant Effect Predictor.

![logo.png](docs/logo.png)

## Setup with uv

1. Install `uv`.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository and enter it.

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr
```

3. Sync dependencies and build the package in place.

```bash
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

4. Run Python commands inside the managed environment.

```bash
uv run python -c "import vepyr; print(vepyr.__all__)"
```

5. Run the test suite.

```bash
uv run pytest
```

## Quick start

The repository ships with small test fixtures so you can verify the full
pipeline — build, annotate (parquet), annotate (fjall) — without downloading
any external data.

### 1. Build a cache from a local Ensembl VEP cache directory

`tests/data/ensembl_cache` contains a tiny slice of the Ensembl VEP 115
offline cache (chr22). Convert it to Parquet **and** fjall formats:

```python
import vepyr

results = vepyr.build_cache(
    release=115,
    cache_dir="/tmp/vepyr_cache",
    local_cache="tests/data/ensembl_cache",  # skip download
    build_fjall=True,                         # parquet + fjall
)
for path, rows in results:
    print(f"{path}: {rows:,} rows")
```

Set `build_fjall=False` if you only need Parquet files.

### 2a. Annotate variants (Parquet backend)

A small 5-variant VCF for chr22 ships with the cache fixture:

```python
import vepyr

cache_dir = "/tmp/vepyr_cache/parquet/115_GRCh38_vep"

lf = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "most_severe_consequence").head())
```

### 2b. Annotate variants (fjall backend)

Pass `use_fjall=True` to use the embedded KV store instead of Parquet for
co-located variant lookups — same API, faster on large caches:

```python
lf = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
    use_fjall=True,  # <-- only difference
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "most_severe_consequence").head())
```

### 2c. Write annotated VCF output

Instead of a LazyFrame, write results directly to a VCF file with CSQ in the
INFO column — use `.vcf.gz` for bgzf compression or `.vcf` for plain text:

```python
out_path = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
    output_vcf="/tmp/annotated.vcf",  # or .vcf.gz for bgzf
)
print(f"Wrote annotated VCF to {out_path}")
```

### 3. Full `--everything` annotation (golden test data)

`tests/data/golden` has a pre-built chr1 cache, a 100-variant VCF, and a
matching reference FASTA. Run a full `--everything` annotation:

```python
import vepyr

lf = vepyr.annotate(
    vcf="tests/data/golden/input.vcf.gz",
    cache_dir="tests/data/golden/cache",
    everything=True,
    reference_fasta="tests/data/golden/reference.fa",
)

df = lf.collect()
print(f"{df.height} variants × {df.width} columns")
print(df.select("chrom", "start", "ref", "alt",
                "most_severe_consequence", "SYMBOL", "IMPACT").head(5))
```

## Documentation

Build and serve the docs locally:

```bash
uv sync --extra docs
uv run mkdocs serve
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000). Docs are auto-deployed to GitHub Pages on each tag push.

### One-liner smoke test

Exercises cache build, both annotation backends, and VCF output:

```bash
uv run python -c "
import vepyr, tempfile, os
with tempfile.TemporaryDirectory() as d:
    r = vepyr.build_cache(115, d, local_cache='tests/data/ensembl_cache', build_fjall=True, show_progress=False)
    cache = os.path.join(d, 'parquet', '115_GRCh38_vep')
    print(f'build_cache : {len(r)} parquet files, {sum(n for _,n in r):,} rows')
    vcf = 'tests/data/ensembl_cache/sample.vcf'
    df1 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True).collect()
    print(f'parquet     : {df1.height} variants × {df1.width} columns')
    df2 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True, use_fjall=True).collect()
    print(f'fjall       : {df2.height} variants × {df2.width} columns')
    assert df1.height == df2.height and df1.width == df2.width, 'backend mismatch'
    out = os.path.join(d, 'annotated.vcf')
    vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True, output_vcf=out, show_progress=False)
    print(f'vcf output  : {os.path.getsize(out):,} bytes')
    assert os.path.getsize(out) > 0, 'empty VCF'
lf = vepyr.annotate('tests/data/golden/input.vcf.gz', 'tests/data/golden/cache', everything=True, reference_fasta='tests/data/golden/reference.fa')
df = lf.collect()
print(f'everything  : {df.height} variants × {df.width} columns')
assert df.height > 0 and df.width > 80, 'smoke test failed'
print('smoke test passed')
"
```
