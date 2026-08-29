# Developers guide

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable)
- Python 3.10+ with [uv](https://docs.astral.sh/uv/)

## Building from source

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr

# Development build
uv sync

# Rebuild after Rust changes (faster iteration)
uv run maturin develop

# Release build with native CPU optimizations
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

## Running tests

```bash
# Full test suite
uv run pytest -v

# Single test
uv run pytest tests/test_annotate.py::test_annotate_parquet -v

# Rust tests
cargo test
```

## Linting and formatting

```bash
# Rust
cargo fmt
cargo clippy --all-targets -- -D warnings

# Python
uv run ruff format .
uv run ruff check .
```

## Building documentation locally

```bash
# Install docs dependencies
uv sync --extra docs

# Serve locally with hot reload
uv run mkdocs serve

# Build static site
uv run mkdocs build
```

## Release wheels

```bash
uv run maturin build --release
```

Wheels are produced for Linux (x86_64), macOS (x86_64, aarch64), and Windows (x64).

## Smoke test

Exercises cache build, indexed Parquet annotation, and VCF output against the
small fixtures that ship with the repository — no external data needed:

```bash
uv run python -c "
import vepyr, tempfile, os
with tempfile.TemporaryDirectory() as d:
    r = vepyr.build_cache(115, d, cache_type='ensembl', local_cache='tests/data/ensembl_cache', show_progress=False)
    cache = os.path.join(d, '115_GRCh38_ensembl')
    print(f'build_cache : {len(r)} parquet files, {sum(n for _,n in r):,} rows')
    vcf = 'tests/data/ensembl_cache/sample.vcf'
    df1 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True).collect()
    print(f'indexed     : {df1.height} variants × {df1.width} columns')
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
