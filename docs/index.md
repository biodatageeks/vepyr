# vepyr

**VEP Yielding Performant Results** — a blazing-fast Rust reimplementation of Ensembl's [Variant Effect Predictor](https://www.ensembl.org/info/docs/tools/vep/index.html).

<p align="center">
  <img src="assets/logo.png" alt="vepyr logo" width="300">
</p>

---

## What is vepyr?

vepyr is a Python library backed by a native Rust engine that provides:

- **Cache conversion** — download and convert Ensembl VEP offline caches to optimized Parquet and [fjall](https://github.com/fjall-rs/fjall) (embedded KV store) formats
- **Variant annotation** — annotate VCF files with transcript consequences, HGVS notation, allele frequencies, and more
- **Full `--everything` parity** — aims for zero mismatches against Ensembl VEP for the supported scope
- **50x+ speedup** — dramatically faster than the reference Perl implementation

## Key features

- **Python-first API** — two functions: `build_cache()` and `annotate()`
- **Polars integration** — annotation results returned as `polars.LazyFrame` for efficient downstream analysis
- **VCF output** — write annotated VCFs with CSQ in the INFO column, compatible with downstream tools
- **Streaming engine** — built on Apache Arrow and DataFusion for memory-efficient processing of large datasets
- **Dual backend** — choose Parquet (default) or fjall for co-located variant lookups

## Supported scope

| Parameter | Value |
|---|---|
| Species | `homo_sapiens` |
| Assembly | `GRCh38` |
| Cache types | `vep`, `merged`, `refseq` |
| Ensembl releases | 115+ |
| Python | 3.10 — 3.14 |
| Platforms | Linux (x86_64), macOS (x86_64, aarch64) |

## Quick example

```python
import vepyr

# Build cache from a local Ensembl VEP offline cache
results = vepyr.build_cache(
    release=115,
    cache_dir="/data/vepyr_cache",
    local_cache="/data/ensembl_vep/homo_sapiens/115_GRCh38",
)

# Annotate variants
lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_vep",
    everything=True,
    reference_fasta="GRCh38.fa",
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "SYMBOL", "Consequence", "IMPACT"))
```
