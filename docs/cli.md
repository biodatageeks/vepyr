# Command line

`vepyr annotate` is a VCF-in / VCF-out shell over [`annotate()`](api.md). It
covers the configuration validated against Ensembl VEP — `--everything` with a
reference FASTA — plus the options a workflow engine needs. Everything else,
including the Polars `LazyFrame` path, stays on the Python API.

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache /data/vep/116_GRCh38_ensembl \
    --fasta GRCh38.fa \
    --everything \
    --fork 8
```

## Options

| Flag | Description |
|---|---|
| `-i`, `--input_file FILE` | Input VCF (plain, gzip or bgzip). Required. |
| `-o`, `--output_file FILE` | Output VCF. A `.gz`/`.bgz` suffix selects bgzf. Required. |
| `--dir_cache DIR` | Parquet cache directory. Required. |
| `--fasta FILE` | Reference FASTA. Required by `--everything`. |
| `--everything` | Enable all annotation features (80-field CSQ). |
| `--fork N`, `--workers N` | Annotation pipelines to run. Default 1. |
| `--cache_version N` | Assert the cache version in the Parquet metadata. |
| `--plugin_cache_root DIR` | Root of a plugin cache tree. |
| `--plugin NAME` | Restrict to this plugin. Repeatable; order is CSQ block order. |
| `--no_progress` | Suppress the progress bar. |

Flag names follow Ensembl VEP's own spelling, so `ext.args` strings written for
`vep` carry over as the flag set grows.

## Notes

**Compression is inferred from the output suffix.** `-o out.vcf.gz` writes bgzf;
`-o out.vcf` writes plain text. There is no `--compress_output` flag.

**`--fork` above 1 needs an indexed input.** The input VCF must be bgzip-compressed
with a `.tbi` or `.csi` beside it, or the run fails. Results are identical to
`--fork 1`, row for row and in the same order.

**No index is written.** The output is bgzf but unindexed; run `tabix` afterwards
if you need one.

**Unknown flags are an error.** A VEP flag this interface does not implement — say
`--pick` — fails the run rather than being ignored, so a stale command line can
never silently produce differently-annotated output.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 2 | Bad arguments, or the annotation request was rejected (unusable cache, missing FASTA, unindexed input with `--fork` > 1). |
