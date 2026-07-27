# Performance

## Design goals

vepyr targets a **50x+ speedup** over the reference Ensembl VEP Perl implementation while maintaining **zero mismatches** for the supported annotation scope.

## Why it's fast

### Native Rust engine

The entire annotation pipeline — allele matching, interval tree lookups, consequence prediction, HGVS computation — runs in compiled Rust code. No interpreter overhead touches the hot path.

### Apache DataFusion

vepyr uses [DataFusion](https://datafusion.apache.org/) as its query execution substrate. This provides:

- Vectorized execution over Arrow columnar batches
- Predicate pushdown to minimize data scanned
- Parallel partition processing

### Streaming architecture

Results are streamed as Arrow `RecordBatch`es rather than materializing full datasets in memory. This keeps memory usage bounded regardless of input VCF size.

### Optimized cache format

The Ensembl VEP offline cache ships as Perl `Storable` / `Sereal` serialized files. vepyr converts these to a partitioned **Parquet** cache — columnar, compressed, with sorted row groups, per-chromosome shards, and a page/column index that powers fast point lookups for co-located variants.

### COITree interval matching

Transcript overlap queries use [COITree](https://github.com/dcjones/coitree) (cache-oblivious interval trees), which provide O(n + log(n)) query performance.

## Benchmarking

### Running a comparison

To benchmark vepyr against Ensembl VEP on the same input. For the full parity setup —
downloading the HG002 benchmark VCF, normalizing it, and running the comparison harness —
see [Testing against Ensembl VEP](testing-vep.md).

**Ensembl VEP (Docker):**

```bash
time docker run --rm \
  -v /data/vep/homo_sapiens/115_GRCh38:/opt/vep/.vep/homo_sapiens/115_GRCh38:ro \
  -v /work:/work \
  ensemblorg/ensembl-vep:release_115.2 \
  vep \
  --dir /opt/vep/.vep \
  --cache --offline --assembly GRCh38 \
  --input_file /work/input.vcf \
  --output_file /work/output.vcf \
  --vcf --force_overwrite --no_stats \
  --everything
```

**vepyr:**

```python
import vepyr
import time

start = time.time()
lf = vepyr.annotate(
    vcf="input.vcf",
    cache_dir="/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    everything=True,
    reference_fasta="GRCh38.fa",
)
df = lf.collect()
elapsed = time.time() - start
print(f"{df.height} variants in {elapsed:.1f}s")
```

### Tuning

| Parameter | Default | Effect |
|---|---|---|
| `cache_size_mb` | `1024` | LRU cache for annotation data — increase for large inputs |
| `workers` | `1` | Within-contig annotation pipelines. `1` is serial; values greater than 1 require a tabix-indexed (bgzip + `.tbi`) input VCF. |
| `partitions` | `1` | DataFusion partitions during cache build — increase for parallel conversion |

!!! tip "Compile-time optimization"
    For maximum throughput, build with native CPU instructions:

    ```bash
    RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
    ```
