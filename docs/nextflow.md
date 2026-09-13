# Nextflow

`vepyr/annotate` is an [nf-core](https://nf-co.re/)-style Nextflow module around
[`vepyr annotate`](cli.md): one VCF in, one bgzip VCF with `CSQ` plus its tabix
index out. It ships Docker and Singularity containers for `linux/amd64` and
`linux/arm64`, so it runs natively on x86_64 servers, ARM servers and Apple
Silicon.

!!! info "Not in nf-core/modules yet"
    The module is staged in this repository under
    [`nf-core-module/modules/nf-core/vepyr/annotate`](https://github.com/biodatageeks/vepyr/tree/master/nf-core-module/modules/nf-core/vepyr/annotate)
    until it is submitted to nf-core/modules. Until then, copy it into your
    pipeline as shown below; `nf-core modules install vepyr/annotate` will work
    once it is merged there.

## Adding the module to a pipeline

Copy the module directory to the path nf-core tooling would install it at, so a
later `nf-core modules install` replaces it in place:

```bash
git clone --depth 1 https://github.com/biodatageeks/vepyr.git
mkdir -p my-pipeline/modules/nf-core/vepyr
cp -R vepyr/nf-core-module/modules/nf-core/vepyr/annotate my-pipeline/modules/nf-core/vepyr/
```

## Example

A minimal pipeline that annotates one VCF with `--everything`:

```groovy title="main.nf"
include { VEPYR_ANNOTATE } from './modules/nf-core/vepyr/annotate/main'

workflow {
    vcf = channel.of([
        [ id: params.sample ],
        file(params.vcf, checkIfExists: true),
        file("${params.vcf}.tbi", checkIfExists: true)
    ])
    cache = channel.value([
        [ id: file(params.cache).name ],
        file(params.cache, checkIfExists: true, type: 'dir')
    ])
    fasta = channel.value([
        [ id: 'GRCh38' ],
        file(params.fasta, checkIfExists: true),
        // bgzip FASTA: pass [ fai, gzi ]; plain FASTA: just the .fai
        [ file("${params.fasta}.fai", checkIfExists: true), file("${params.fasta}.gzi", checkIfExists: true) ]
    ])

    VEPYR_ANNOTATE(vcf, cache, fasta, params.cache_version, [[], []])

    VEPYR_ANNOTATE.out.vcf.view { meta, annotated -> "${meta.id}: ${annotated}" }
}
```

```groovy title="nextflow.config"
params {
    sample        = 'sample'
    vcf           = null
    cache         = null
    fasta         = null
    cache_version = 116
    outdir        = 'results'
}

docker.enabled = true

process {
    withName: 'VEPYR_ANNOTATE' {
        ext.args   = '--everything'
        cpus       = 4
        publishDir = [ path: { "${params.outdir}/vepyr" }, mode: 'copy' ]
    }
}

profiles {
    // main.nf names the linux/amd64 image; on arm64 hosts (Apple Silicon,
    // Graviton) use the arm64 build from the module's meta.yml.
    arm64 {
        docker.runOptions = '--platform=linux/arm64'
        process {
            withName: 'VEPYR_ANNOTATE' {
                container = 'community.wave.seqera.io/library/htslib_pip_python_vepyr:d7cf9a888587f5b0'
            }
        }
    }
}
```

```bash
# x86_64 Linux
nextflow run main.nf \
    --sample HG002 \
    --vcf HG002.vcf.gz \
    --cache ~/vepyr_cache/116_GRCh38_ensembl \
    --fasta Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz

# Apple Silicon or another arm64 host: add -profile arm64
```

Results land in `results/vepyr/HG002.vcf.gz` and `results/vepyr/HG002.vcf.gz.tbi`.

This exact pipeline, run with `-profile arm64` on all 50,861 normalized HG002
chr22 records against a release-116 `ensembl` cache, reproduces the record-body
md5 of Ensembl VEP 116 `--everything` (see [Testing vs Ensembl VEP](testing-vep.md)).

## Inputs

| Channel | Shape | Notes |
|---|---|---|
| 1 | `[ meta, vcf, tbi ]` | Input VCF (plain, gzip or bgzip). `tbi` is optional — pass `[]` — but without it the task runs a single pipeline. |
| 2 | `[ meta2, cache ]` | vepyr Parquet cache **directory**, e.g. `116_GRCh38_ensembl`. Not an Ensembl VEP cache. |
| 3 | `[ meta3, fasta, fai ]` | Reference FASTA and its `.fai`. For a bgzip FASTA pass `[ fai, gzi ]` as the third element. Required by `--everything`; pass `[ meta3, [], [] ]` otherwise. |
| 4 | `cache_version` | Release the cache must carry in its metadata, e.g. `116`. Pass `[]` to skip the check. |
| 5 | `[ meta4, plugin_cache ]` | Root of a [plugin cache](plugins.md) tree, or `[ [], [] ]` for none. |

The module never builds indexes: vepyr opens the reference through its `.fai`
(and a bgzip reference through its `.gzi` as well), so a missing index fails the
task.

## Outputs

| Emit | Shape | Content |
|---|---|---|
| `vcf` | `[ meta, "${prefix}.vcf.gz" ]` | Annotated VCF, bgzip. |
| `tbi` | `[ meta, "${prefix}.vcf.gz.tbi" ]` | tabix index of it. |
| `versions_vepyr`, `versions_tabix` | `[ process, tool, version ]` | Also published on the `versions` topic. |

## Configuration

| Setting | Effect |
|---|---|
| `ext.args` | Extra `vepyr annotate` flags, e.g. `'--everything'` or `'--hgvsc'`. See [Command line](cli.md#options). |
| `ext.args2` | Extra `tabix` flags for indexing the output. |
| `ext.prefix` | Output file name stem. Default: `meta.id`. |
| `cpus` | Annotation pipelines (`--fork`). |

The module sets `-i`, `-o`, `--dir_cache`, `--fasta`, `--cache_version`,
`--plugin_cache_root`, `--fork` and `--no_progress` itself.

**`--fork` comes from `cpus`, not `ext.args`.** The module appends
`--fork ${task.cpus}` after `ext.args`, so a `--fork` or `--workers` in
`ext.args` is overridden. When the input has no index it passes `--fork 1`,
since more than one pipeline needs an indexed VCF.

**Unknown flags fail the task.** `vepyr annotate` rejects any VEP flag it does not
implement, so an `ext.args` copied from an `ensemblvep/vep` configuration fails
rather than silently annotating differently.

## Caches

Channel 2 stages a directory, so it must be something Nextflow can stage as one:
a local or shared-filesystem path, or an object-store prefix Nextflow can list.
A plain `https://` URL to a cache directory does not work, because web servers
serve files, not directory trees.

Get a prebuilt cache from [Downloads](downloads.md). For a quick test, a single
chromosome is enough — see
[Downloading only part of a cache](downloads.md#downloading-only-part-of-a-cache);
such a cache annotates only the contigs it holds.

Pair `cache_version` with the cache you pass: a `116_GRCh38_*` cache with
`cache_version = 116`.

## Plugins

Pass the plugin cache root as channel 5 and choose plugins in `ext.args`:

```groovy
VEPYR_ANNOTATE(vcf, cache, fasta, 116, [ [ id: 'plugins' ], file(params.plugin_cache, type: 'dir') ])
```

```groovy
process {
    withName: 'VEPYR_ANNOTATE' {
        ext.args = '--everything --plugin clinvar --plugin cadd'
    }
}
```

Without `--plugin`, every plugin under the root is applied, in alphabetical
order; each `--plugin` narrows the set and fixes its `CSQ` block order. The root
is the directory that *contains* `plugin/`. See [Plugins](plugins.md).

## Containers

| Engine | linux/amd64 | linux/arm64 |
|---|---|---|
| Docker | `community.wave.seqera.io/library/htslib_pip_python_vepyr:00a5ec7681bdfa20` | `community.wave.seqera.io/library/htslib_pip_python_vepyr:d7cf9a888587f5b0` |
| Singularity / Apptainer | `oras://community.wave.seqera.io/library/htslib_pip_python_vepyr:acf4dcc00f540d69` | `oras://community.wave.seqera.io/library/htslib_pip_python_vepyr:15625539a599a8eb` |

They are [Seqera Wave](https://seqera.io/containers/) builds of the module's
`environment.yml`: htslib (for `tabix`) and Python from conda, and vepyr from
its PyPI wheel until bioconda ships the same version for linux-64 and
linux-aarch64. The module's `meta.yml` lists them with conda lock files for both
platforms, and `-profile conda` uses `environment.yml` directly.

!!! warning "Use the image for your host's architecture"
    `main.nf` names the `linux/amd64` images, which is what Nextflow runs unless
    you override `container`. On an arm64 host, select the arm64 image as the
    `arm64` profile above does. Running the amd64 image under emulation on Apple
    Silicon fails with `SIGILL` (exit 132): Docker's emulated x86_64 guest has no
    AVX, which the native extension uses.
