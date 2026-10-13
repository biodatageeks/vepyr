# Command line

`vepyr annotate` is a VCF-in / VCF-out shell over [`annotate()`](api.md). It
always runs the configuration validated against Ensembl VEP — `--everything`,
which needs a reference FASTA — and every `annotate()` parameter that applies
to VCF output has a flag. Only the Polars `LazyFrame` path (`skip_csq`,
`info_fields`, `format_fields`) and the `on_batch_written` callback stay on the
[Python API](api.md). For Nextflow, the [`vepyr/annotate` module](nextflow.md)
wraps this command.

`pip install vepyr` (or `conda install -c conda-forge -c bioconda vepyr`)
installs a `vepyr` executable alongside the library; no separate package is
needed. `python -m vepyr` runs the same entry point when
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
| `--fasta FILE` | Reference FASTA. Required: annotation always runs `--everything`. |
| `--everything` | Always on; accepted and ignored for Ensembl VEP compatibility. |
| `--hgvsc` | Implied by `--everything`; accepted and ignored for Ensembl VEP compatibility. |
| `--fork N`, `--workers N` | Annotation pipelines to run. Default 1. |
| `--buffer_size N`, `--buffer-size N` | Variants per annotation buffer. Positive integer; defaults to the Python API's 5000. |
| `--allow_non_variant` | Keep `ALT=.` records instead of dropping them, as VEP does. |
| `--cache_version N` | Assert the cache version in the Parquet metadata. |
| `--failed {0,1}` | Include co-located variants flagged as failed. Default 0. |
| `--distance N[,M]` | Up- and downstream distance for transcript overlap; `N,M` sets them separately. Default 5000. |
| `--fields LIST` | Comma-separated `CSQ` fields to output, in that order, or `core` alone for the eleven core fields. Names must not repeat. Plugin fields follow. |
| `--compress_output {bgzip,gzip}` | Compress the output, overriding the suffix of `-o`. |
| `--shift_hgvs {0,1}` | 3′ shift HGVS notation. Default 1. |
| `--no_escape` | Do not URI-escape HGVS strings. |
| `--remove_hgvsp_version` | Remove the version from the HGVSp transcript identifier. |
| `--hgvsp_use_prediction` | Use the predicted rather than the observed protein sequence. |
| `--gencode_basic`, `--gencode_primary` | Restrict to the GENCODE basic or primary transcript set. Mutually exclusive. |
| `--all_refseq` | Keep all RefSeq transcripts, including CCDS and EST-based ones. |
| `--exclude_predicted` | Exclude predicted RefSeq transcripts (`XM_`, `XR_`). |
| `--pick`, `--pick_allele`, `--per_gene`, `--pick_allele_gene` | Report one selected consequence per variant, allele, gene, or allele and gene. |
| `--flag_pick`, `--flag_pick_allele`, `--flag_pick_allele_gene` | Keep all consequences and mark the selected one with `PICK=1`. If several pick options are given, the first in this table's order applies, as in VEP. |
| `--pick_order LIST` | Ranking criteria for the pick options, e.g. `biotype,rank,mane_select,tsl,canonical,appris,ccds,length`. Criteria must not repeat; unknown names are rejected with exit code 2. |
| `--plugin_cache_root DIR` | Root of a plugin cache tree. |
| `--plugin NAME` | Restrict to this plugin. Repeatable; order is CSQ block order. |
| `--no_progress` | Suppress the progress bar. |
| `--no_preserve_record_layout` | Write `INFO` in schema order and drop `FORMAT` keys missing in every sample, instead of keeping each record's own layout. |
| `-h`, `--help` / `--version` | Usage, or the installed version. |

Flag names follow Ensembl VEP's own spelling, so `ext.args` strings written for
`vep` carry over. Each flag maps to the `annotate()` keyword of the same name;
`--fork` maps to `workers`, `--cache_version` to `expected_cache_version` and
`--compress_output` to `compression`.

Annotation always runs Ensembl VEP `--everything`, HGVS included, and cannot be
turned off; [Annotation mode](quickstart.md#annotation-mode-ensembl-vep-everything)
lists the flags it implies. `--everything` and `--hgvsc` are accepted only so VEP-style argument
strings keep parsing; they change nothing.

## Plugins

Point `--plugin_cache_root` at a [plugin cache](plugins.md) and the plugin CSQ
fields are appended to every record:

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_merged \
    --fasta GRCh38.fa \
    --plugin_cache_root ~/vepyr_plugin_cache \
    --plugin clinvar \
    --plugin cadd
```

Omit `--plugin` to apply every plugin under the root, in alphabetical order.
Each `--plugin` narrows the set to those named, and the order they are given in
is the order of their blocks in the `CSQ` header and in every `CSQ` value.

Build a plugin cache with [`vepyr build-plugin-cache`](#building-caches) or
[`build_plugin_cache()`](api.md#vepyr.build_plugin_cache), or download a
prebuilt one from [Downloads](downloads.md).

## Building caches

`vepyr build-cache` converts an Ensembl VEP cache to vepyr's Parquet format,
wrapping [`build_cache()`](api.md#vepyr.build_cache). It downloads the cache
from the Ensembl FTP site unless `--local_cache` points at an unpacked copy,
and writes the result to `<dir_cache>/<release>_<assembly>_<cache_type>`:

```bash
vepyr build-cache --release 116 --cache_type merged --dir_cache ~/vepyr_cache
```

| Flag | Description |
|---|---|
| `--release N` | Ensembl release. Required. |
| `--cache_type {ensembl,merged,refseq}` | Ensembl VEP cache type. Required. |
| `--dir_cache DIR` | Root directory for the downloaded and the converted cache. Required. |
| `--local_cache DIR` | Unpacked Ensembl VEP cache (the directory holding `info.txt`); skips the download. |
| `--entity NAME` | Build only one entity: `variation`, `transcript`, `exon`, `translation`, `regulatory` or `motif`. |
| `--chrom NAME` | Build only this contig. Repeatable. |
| `--species NAME`, `--assembly NAME` | Default `homo_sapiens`, `GRCh38`. |
| `--partitions N` | DataFusion partitions for the conversion; at least 1. Default 8. |
| `--download_retries N` | Resume-retries for the download; 0 or more. Default 10. |
| `--overwrite` | Rebuild existing outputs instead of skipping them. |
| `--no_progress` | Suppress progress bars. |

`vepyr build-plugin-cache` builds a [plugin cache](plugins.md) from a manifest in
[biodatageeks/vepyr-plugins](https://github.com/biodatageeks/vepyr-plugins),
wrapping [`build_plugin_cache()`](api.md#vepyr.build_plugin_cache). The cache is
written to `<plugin_cache_root>/plugin/<plugin>/`, and `--dir_cache` is the
vepyr cache whose variation data sets the frequency tiering:

```bash
vepyr build-plugin-cache --plugin cadd --version v0.2.0 \
    --source snv=whole_genome_SNVs.tsv.gz \
    --source indel=gnomad.genomes.r4.0.indel.tsv.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_merged \
    --plugin_cache_root ~/vepyr_plugin_cache
```

| Flag | Description |
|---|---|
| `--plugin NAME` | Plugin name. Required. |
| `--version TAG` | vepyr-plugins git tag of the manifest. Required. |
| `--source [PART=]PATH` | Source file: one bare path, or one `PART=PATH` for every source of a multi-source manifest (CADD: `snv`, `indel`). A single `--source` is always a path; with several, `PART` is letters, digits, `_` or `-`, so paths containing `=` stay whole. Required. |
| `--dir_cache DIR` | vepyr Parquet cache whose variation data sets the tiering. Required. |
| `--plugin_cache_root DIR` | Root of the plugin cache tree. Required. |
| `--chrom NAME` | Build only this contig. Repeatable. |
| `--plugins_repo PATH` | Local clone of vepyr-plugins, for offline builds. |
| `--verify_source {strict,warn,skip}` | Checksum check of the source files. Default `strict`. |
| `--overwrite` | Rebuild an existing plugin cache. |

Both commands finish with a summary of what was written and the `vepyr annotate`
flags that use it. When `build-cache` skips every existing output, the summary
says so; `--overwrite` rebuilds them.

**`--dir_cache` differs between the commands.** For `build-cache` it is the root
under which `<release>_<assembly>_<cache_type>` is created. For
`build-plugin-cache` and `annotate` it is that converted cache itself, e.g.
`~/vepyr_cache/116_GRCh38_merged`, the path `build-cache` prints as
`Cache ready:`.

An unknown `--version`, an invalid `--plugins_repo` or a failed clone of
vepyr-plugins ends `build-plugin-cache` with a one-line error and exit code 2.

## Notes

**Compression is inferred from the output suffix.** `-o out.vcf.gz` writes bgzf;
`-o out.vcf` writes plain text. `--compress_output bgzip` or `gzip` overrides
the suffix.

**`--fork` above 1 needs an indexed input.** The input VCF must be bgzip-compressed
with a `.tbi` or `.csi` beside it, or the run fails. Results are identical to
`--fork 1`, row for row and in the same order.

**No index is written.** The output is bgzf but unindexed; run `tabix` afterwards
if you need one.

**`--buffer-size` controls batching.** For example, `--buffer-size 1000` forwards
`buffer_size=1000` to `annotate()`. Omit it to use the API default of 5000.
Zero, negative values and non-integers are rejected with exit code 2.

**Records with no alternate allele are dropped.** A record whose first `ALT` is
`.` carries no alternate allele, and — as Ensembl VEP does — it is left out of
the output entirely, silently. `--allow_non_variant` writes it through instead,
with its original `ALT` and no `CSQ` key. Only the first `ALT` is tested, so
`ALT=.,C` is non-variant while `ALT=C,.` is an ordinary record.

**Unknown flags are an error.** A VEP flag this interface does not implement — say
`--tab` — fails the run rather than being ignored, so a stale command line can
never silently produce differently-annotated output. Flags are never
abbreviated, so VEP's `--hgvs` is rejected rather than read as `--hgvsc`.

**Output is always a named file.** There is no stdout streaming mode, so `-o -`
is not accepted.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 2 | Bad arguments, or the request was rejected (unusable cache, missing FASTA, unindexed input with `--fork` > 1, a missing local cache or source file, a failed checksum). |
