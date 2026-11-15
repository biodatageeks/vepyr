# Download Ensembl VEP and plugin caches

Building a cache from an Ensembl VEP tarball with
[`build_cache`](api.md#vepyr.build_cache) takes hours of CPU and needs the raw
Ensembl download on local disk. If you just want to annotate, download a
**prebuilt Parquet cache** instead — it is the exact output `build_cache` would
have produced, ready to pass to [`annotate`](api.md#vepyr.annotate).

See [Caches](caches.md) for what a cache contains, how the entities map to CSQ
fields, and how the three cache types differ.

!!! info "Currently available: release 116, GRCh38"
    All three cache types are published for **Ensembl release 116 / GRCh38**,
    along with five [plugin caches](#plugin-caches). Release 115 caches are not
    published yet — build those locally for now. The **dbNSFP** plugin cache is
    a different case: it is supported but cannot be published at all, see
    [dbNSFP](#dbnsfp-supported-but-not-published).

| Cache | Transcript set | Size |
|---|---|--:|
| `116_GRCh38_merged` | Ensembl **and** RefSeq | 36 G |
| `116_GRCh38_ensembl` | Ensembl/GENCODE | 32 G |
| `116_GRCh38_refseq` | RefSeq | 31 G |

!!! warning "Upgrading from vepyr 0.10.0 or earlier"
    vepyr 0.11.0 needs caches that carry the transcript columns `gene_start`
    and `gene_end`. A cache downloaded or built with an earlier vepyr does not,
    and annotating with it stops with an error. Download it again into an empty
    directory — see [Caches built before vepyr 0.11.0](caches.md#caches-built-before-vepyr-0110).

## Hugging Face

Each cache is a public dataset repository holding the Parquet shard tree
directly, so no extraction step is needed and the download lands in the layout
`annotate()` expects.

vepyr downloads them itself; the Hugging Face client is installed with it:

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache ~/vepyr_cache
```

or from Python:

```python
import vepyr

cache = vepyr.download_cache(116, "~/vepyr_cache", cache_type="merged")
```

`--cache_type` is `merged`, `ensembl` or `refseq`. The default revision is the
one pinned by your vepyr release, so a given vepyr version always fetches the
same cache; pass `--revision main` (or `revision="main"`) for the latest.

??? note "Manual alternative: the `hf` command"
    === "merged"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_merged \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_merged
        ```

    === "ensembl"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_ensembl \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_ensembl
        ```

    === "refseq"

        ```bash
        hf download biodatageeks/vepyr_116_GRCh38_refseq \
          --repo-type dataset \
          --local-dir ~/vepyr_cache/116_GRCh38_refseq
        ```

The download is resumable — rerun the same command after an interruption and it
continues where it stopped. Integrity is checked by the client, so there is no
separate checksum step.

### Downloading only part of a cache

For testing you can fetch a single chromosome instead of the whole 31–36 G
cache. Every entity directory carries a `chrom_manifest.json` that the engine
requires, so it has to be included alongside the shards, together with the two
root sidecars: `reference_policy.json` (vepyr refuses merged and RefSeq caches
without it) and `chr_synonyms.txt` (contig-name aliases):

```bash
vepyr download-cache --release 116 --cache_type merged \
  --dir_cache ~/vepyr_cache --chrom 22
```

`--chrom` adds the manifests and sidecar files itself, and writes to
`~/vepyr_cache/116_GRCh38_merged`. With `hf`:

```bash
hf download biodatageeks/vepyr_116_GRCh38_merged \
  --repo-type dataset \
  --include '*/chr22.parquet' \
  --include '*/chrom_manifest.json' \
  --include 'reference_policy.json' \
  --include 'chr_synonyms.txt' \
  --local-dir ~/vepyr_cache/116_GRCh38_merged_chr22
```

!!! warning "Partial caches annotate only what they cover"
    vepyr validates the shards for each contig it annotates. A cache pulled
    with `--include '*/chr22.parquet'` can annotate chr22 and nothing else.

!!! danger "Do not skip the `variation` entity"
    `variation` is the cache's root table — the engine discovers everything
    else relative to it. Excluding it (`--exclude 'variation/*'`) does not
    produce a smaller working cache, it produces a cache that fails to open
    with `no partitioned Parquet VEP cache found`, even for annotations that
    never touch co-located variant data.

## Confirming the cache before you annotate

Before annotating, check that the cache reports the release and assembly you
expect. This opens only the shards for the named contig, so it is fast:

```python
import os
import vepyr

cache = os.path.expanduser("~/vepyr_cache/116_GRCh38_merged")

print(vepyr.cache_contig_identity(cache, "chr22", expected_cache_version="116"))
```

(`cache_dir` is passed straight to the Rust engine, which does not expand `~` —
hence `os.path.expanduser`.)

A mismatched, missing, or mixed release is an error rather than a warning — see
[Strict Parquet identity](caches.md#strict-parquet-identity).

Then annotate as usual, pointing `cache_dir` at the cache directory itself:

```python
lf = vepyr.annotate(
    vcf="sample.vcf.gz",
    cache_dir=cache,
    reference_fasta="Homo_sapiens.GRCh38.dna.primary_assembly.fa",
)
print(lf.head().collect())
```

## Plugin caches

Five plugin caches are published on Hugging Face, built against the release-116
GRCh38 variation cache above. They are the exact output
[`build_plugin_cache`](api.md#vepyr.build_plugin_cache) would have produced —
see [Plugins](plugins.md) for what a plugin cache is and how the values reach
the CSQ output.

| Plugin | Dataset | Size | Source version |
|---|---|--:|---|
| CADD | [`vepyr_116_GRCh38_plugin_cadd`](https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_plugin_cadd) | 69 G | v1.7 SNVs + gnomAD r4.0 indels |
| SpliceAI | [`vepyr_116_GRCh38_plugin_spliceai`](https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_plugin_spliceai) | 24 G | Ensembl 110 masked SNV MANE (model v1.3.1) |
| AlphaMissense | [`vepyr_116_GRCh38_plugin_alphamissense`](https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_plugin_alphamissense) | 545 M | hg38 canonical, 2023 release |
| ClinVar | [`vepyr_116_GRCh38_plugin_clinvar`](https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_plugin_clinvar) | 77 M | GRCh38 weekly, `fileDate=2026-07-06` |
| PhenotypeOrthologous | [`vepyr_116_GRCh38_plugin_phenotypeorthologous`](https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous) | < 10 M | Ensembl release-116 GFF3 (`…_112_GRCh38.gff3.gz`) |

Each repository holds `chr1.parquet` … `chr22.parquet` plus the plugin
`manifest.json`. The four score caches are **autosomes only** — chrX, chrY and
chrM are not covered; the PhenotypeOrthologous cache also carries `chrX` and
`chrMT` (its source has no chrY rows, which the manifest lists with 0 rows).
Every dataset card documents its full source provenance, schema, CSQ field
mapping and licence.

!!! warning "Three of the five are non-commercial only"
    **CADD**, **SpliceAI** and **AlphaMissense** restrict use to academic /
    non-profit research; commercial use needs a licence from the respective
    provider. Only the ClinVar cache is unrestricted (NCBI public domain). Each
    dataset card carries the specific terms — check them before use.

### Downloading

`annotate()` takes a `plugin_cache_root`, and looks for each plugin under
`<root>/plugin/<name>/`. `vepyr download-plugin-cache` writes that layout:

```bash
vepyr download-plugin-cache --plugin clinvar --release 116 \
  --plugin_cache_root ~/vepyr_plugin_cache
```

`--chrom 22` limits it to one chromosome. With `hf`, download each cache
**into that layout** — the `plugin/` path component is required:

=== "clinvar"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_plugin_clinvar \
      --repo-type dataset \
      --local-dir ~/vepyr_plugin_cache/plugin/clinvar
    ```

=== "alphamissense"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_plugin_alphamissense \
      --repo-type dataset \
      --local-dir ~/vepyr_plugin_cache/plugin/alphamissense
    ```

=== "spliceai"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_plugin_spliceai \
      --repo-type dataset \
      --local-dir ~/vepyr_plugin_cache/plugin/spliceai
    ```

=== "cadd"

    ```bash
    hf download biodatageeks/vepyr_116_GRCh38_plugin_cadd \
      --repo-type dataset \
      --local-dir ~/vepyr_plugin_cache/plugin/cadd
    ```

Download several into the **same** root to combine them — this is the same rule
as building, where repeated `build_plugin_cache` calls share one
`plugin_cache_root`.

A single chromosome works here too, and unlike the variation cache there is no
root table to preserve — the plugin `manifest.json` is the only companion file:

```bash
hf download biodatageeks/vepyr_116_GRCh38_plugin_cadd \
  --repo-type dataset \
  --include 'chr22.parquet' \
  --include 'manifest.json' \
  --local-dir ~/vepyr_plugin_cache/plugin/cadd
```

!!! note "The manifest lists every chromosome, downloaded or not"
    `manifest.json` is published whole, so it advertises all 22 shards even in a
    partial download. Keep the plugin cache's contig coverage at least as wide
    as the contigs you annotate.

### dbNSFP: supported, but not published

vepyr supports **dbNSFP** as a plugin — the manifest lives at
[`plugins/dbnsfp`](https://github.com/biodatageeks/vepyr-plugins/blob/master/plugins/dbnsfp/dbnsfp.source.toml)
and was developed against dbNSFP 5.3.1a for GRCh38. Its cache is **not mirrored
here, and will not be**: unlike the four above, the upstream data cannot be
redistributed.

- The academic download is gated — it needs registration with an institutional
  email plus an access code.
- It is licensed **CC BY-NC-ND 4.0**. The *no-derivatives* term is the blocker:
  a converted Parquet cache is a derivative work, so even a registered academic
  user may not pass one on. Registration is not the only obstacle.
- Commercial use requires a paid licence.

So dbNSFP is the one plugin you have to build yourself. Fetch the source from
[dbnsfp.org/download](https://www.dbnsfp.org/download/) under whichever licence
applies to you, then build into the **same** `plugin_cache_root` as the
downloaded caches so they combine:

```python
import os
import vepyr

vepyr.build_plugin_cache(
    plugin="dbnsfp",
    version="v0.2.0",                       # vepyr-plugins tag for this manifest
    source_path="dbNSFP5.3.1a_grch38.gz",   # your registered download
    cache_dir=os.path.expanduser("~/vepyr_cache/116_GRCh38_merged"),
    plugin_cache_root=os.path.expanduser("~/vepyr_plugin_cache"),
)
```

That writes `~/vepyr_plugin_cache/plugin/dbnsfp/`, alongside the plugins you
downloaded. See [Plugins](plugins.md) for the build in full.

### Annotating with a downloaded plugin cache

```python
import os
import vepyr

lf = vepyr.annotate(
    vcf="sample.vcf.gz",
    cache_dir=os.path.expanduser("~/vepyr_cache/116_GRCh38_merged"),
    reference_fasta="Homo_sapiens.GRCh38.dna.primary_assembly.fa",
    plugin_cache_root=os.path.expanduser("~/vepyr_plugin_cache"),
)
```

Note `plugin_cache_root` points at the root that *contains* `plugin/`, not at
an individual plugin directory.

!!! tip "Tiering is inherited, so match the releases"
    A plugin cache's warm/cold `tier` is copied row-for-row from the variation
    cache it was built against (rows with no variation-cache match are cold).
    These four were built against the release-116 GRCh38 cache, so pair them
    with a 116 cache. Pairing with a different release still annotates
    correctly — `tier` only affects lookup locality, not values.
