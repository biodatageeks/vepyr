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

## Plugin sources

`vepyr` can now download raw source files and convert them into plugin parquet
plus `<plugin>.fjall` point-lookup stores via `build_plugin()`.

```python
import vepyr

source_path = vepyr.fetch_plugin_source("alphamissense", "/data/vep/cache")
vepyr.build_plugin("alphamissense", source_path, "/data/vep/cache")
```

You can also keep the workflow chromosome-scoped:

```python
source_path = vepyr.fetch_plugin_source(
    "clinvar",
    "/data/vep/cache",
    chromosomes=["1"],
)
vepyr.build_plugin(
    "clinvar",
    source_path,
    "/data/vep/cache",
    chromosomes=["1"],
)
```

Downloaded files are stored under:

```text
<cache_dir>/plugin_sources/<plugin>/<assembly>/<version>/<scope>/
```

Built plugin caches are stored under:

```text
<cache_dir>/<plugin>/chr*.parquet
<cache_dir>/<plugin>.fjall/
```

Supported automated sources in the current implementation:

- `alphamissense`
- `cadd` (SNV source file)
- `spliceai` (GRCh38 Ensembl plugin VCF)
- `dbnsfp` (GRCh38 merged source prepared from the vendor zip)
- `clinvar`

Current chromosome-aware source strategies:

- `clinvar`, `spliceai`: indexed VCF region slicing via `tabix` + `bgzip`
- `dbnsfp`: chromosome-aware assembly from per-chromosome files inside the
  vendor zip
- `alphamissense`, `cadd`: full source download followed by local
  chromosome filtering

When building from already-downloaded local files, CADD materializes one shared
cache. The issue-like default is to point at the SNV source file and keep the
official indel file next to it in the same directory:

- `vepyr.build_plugin("cadd", "/data/plugins/whole_genome_SNVs.tsv.gz", ...)` -> `<cache_dir>/cadd/` + `<cache_dir>/cadd.fjall/`

You can also build the core cache and selected plugin caches in one call via
`build_cache(..., plugins=...)`.

List mode auto-downloads supported sources:

```python
import vepyr

vepyr.build_cache(
    species="homo_sapiens",
    version=115,
    assembly="GRCh38",
    output_dir="/data/vep/cache",
    plugins=["clinvar", "spliceai", "cadd"],
)
```

Mapping mode uses explicit local paths instead of downloading. Logical `cadd`
accepts the SNV source path `whole_genome_SNVs.tsv.gz` and resolves the
official sibling indel file `gnomad.genomes.r4.0.indel.tsv.gz` automatically.
Dict and tuple forms remain accepted for compatibility.

```python
import vepyr

vepyr.build_cache(
    species="homo_sapiens",
    version=115,
    assembly="GRCh38",
    output_dir="/data/vep/cache",
    plugins={
        "clinvar": "/data/plugins/clinvar.vcf.gz",
        "alphamissense": "/data/plugins/AlphaMissense_hg38.tsv.gz",
        "cadd": "/data/plugins/whole_genome_SNVs.tsv.gz",
    },
)
```
