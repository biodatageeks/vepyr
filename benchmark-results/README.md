# Performance baselines

This directory stores reproducible benchmark summaries. Large inputs, caches,
and generated annotated VCF files remain outside the repository.

Each result directory contains:

- `runs.jsonl`: one complete measurement per line,
- `runs.csv`: tabular raw measurements,
- `summary.json`: environment metadata and median timings.

Run the Indexed Parquet lookup benchmark with:

```bash
PYTHONPATH=src .venv/bin/python \
  e2e-testing/scripts/benchmark_parallel_lookup.py \
  --input-vcf /path/to/chr22.single_alt.vcf \
  --cache-dir /path/to/115_GRCh38_ensembl \
  --reference-fasta /path/to/Homo_sapiens.GRCh38.dna.toplevel.fa \
  --results-dir benchmark-results/chr22-indexed-parquet-YYYYMMDD \
  --repeats 3 \
  --workers 1,2,4,8
```

The runner executes each measurement in a fresh process, verifies the SHA-256
of the VCF body against the first serial run, and deletes generated VCF output
after recording its hash and resource usage.
