# Command line

`vepyr annotate` is a VCF-in / VCF-out shell over [`annotate()`](api.md). It
covers the configuration validated against Ensembl VEP — `--everything` with a
reference FASTA — plus the options a workflow engine needs. Everything else,
including the Polars `LazyFrame` path, stays on the [Python API](api.md).

`pip install vepyr` installs a `vepyr` executable alongside the library; no
separate package is needed. `python -m vepyr` runs the same entry point when
the scripts directory is not on `PATH`.

```bash
vepyr --version          # vepyr 0.6.0
vepyr annotate --help
```

## Annotating a VCF

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_ensembl \
    --fasta GRCh38.fa \
    --everything \
    --fork 8
```

`--dir_cache` is a cache directory in vepyr's Parquet format — a prebuilt one
from [Downloads](downloads.md), or one you built with
[`build_cache()`](api.md#vepyr.build_cache). It is not an Ensembl VEP cache
directory.

## Options

| Flag | Description |
|---|---|
| `-i`, `--input_file FILE` | Input VCF (plain, gzip or bgzip). Required. |
| `-o`, `--output_file FILE` | Output VCF. A `.gz`/`.bgz` suffix selects bgzf. Required. |
| `--dir_cache DIR` | Parquet cache directory. Required. |
| `--fasta FILE` | Reference FASTA. Required by `--everything`. |
| `--everything` | Enable all annotation features (80-field CSQ). |
| `--hgvsc` | Add HGVS coding-sequence notation. Requires `--fasta`. |
| `--fork N`, `--workers N` | Annotation pipelines to run. Default 1. |
| `--allow_non_variant` | Keep `ALT=.` records instead of dropping them, as VEP does. |
| `--cache_version N` | Assert the cache version in the Parquet metadata. |
| `--plugin_cache_root DIR` | Root of a plugin cache tree. |
| `--plugin NAME` | Restrict to this plugin. Repeatable; order is CSQ block order. |
| `--no_progress` | Suppress the progress bar. |
| `-h`, `--help` / `--version` | Usage, or the installed version. |

Flag names follow Ensembl VEP's own spelling, so `ext.args` strings written for
`vep` carry over as the flag set grows.

VEP's `--hgvs` and `--hgvsp` are not implemented yet. `--hgvsc` gives the coding
notation on its own, without the cost of the full `--everything` layout; for the
protein notation, use `--everything`.

## Plugins

Point `--plugin_cache_root` at a [plugin cache](plugins.md) and the plugin CSQ
fields are appended to every record:

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_merged \
    --fasta GRCh38.fa \
    --everything \
    --plugin_cache_root ~/vepyr_plugin_cache \
    --plugin clinvar \
    --plugin cadd
```

Omit `--plugin` to apply every plugin under the root, in alphabetical order.
Each `--plugin` narrows the set to those named, and the order they are given in
is the order of their blocks in the `CSQ` header and in every `CSQ` value.

Building a plugin cache is a Python-API job — see
[`build_plugin_cache()`](api.md#vepyr.build_plugin_cache) — or download a
prebuilt one from [Downloads](downloads.md).

## Notes

**Compression is inferred from the output suffix.** `-o out.vcf.gz` writes bgzf;
`-o out.vcf` writes plain text. There is no `--compress_output` flag.

**`--fork` above 1 needs an indexed input.** The input VCF must be bgzip-compressed
with a `.tbi` or `.csi` beside it, or the run fails. Results are identical to
`--fork 1`, row for row and in the same order.

**No index is written.** The output is bgzf but unindexed; run `tabix` afterwards
if you need one.

**Records with no alternate allele are dropped.** A record whose first `ALT` is
`.` carries no alternate allele, and — as Ensembl VEP does — it is left out of
the output entirely, silently. `--allow_non_variant` writes it through instead,
with its original `ALT` and no `CSQ` key. Only the first `ALT` is tested, so
`ALT=.,C` is non-variant while `ALT=C,.` is an ordinary record.

**Unknown flags are an error.** A VEP flag this interface does not implement — say
`--pick` — fails the run rather than being ignored, so a stale command line can
never silently produce differently-annotated output.

**Output is always a named file.** There is no stdout streaming mode, so `-o -`
is not accepted.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 2 | Bad arguments, or the annotation request was rejected (unusable cache, missing FASTA, unindexed input with `--fork` > 1). |
