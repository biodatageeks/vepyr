# Nextflow

`vepyr/annotate` is an
[nf-core Nextflow module](https://github.com/nf-core/modules/tree/master/modules/nf-core/vepyr/annotate) around
[`vepyr annotate`](cli.md): one VCF in, one bgzip VCF with `CSQ` plus its tabix
index out. It ships Docker and Singularity containers for `linux/amd64` and
`linux/arm64`, so it runs natively on x86_64 servers, ARM servers and Apple
Silicon.

![vcf_annotate_vepyr subworkflow](diagrams/nextflow-subworkflow-light.svg#only-light)
![vcf_annotate_vepyr subworkflow](diagrams/nextflow-subworkflow-dark.svg#only-dark)

The diagram shows the `vcf_annotate_vepyr` subworkflow described in
[Normalizing first](#normalizing-first); with `val_normalize` false, or when you
call `VEPYR_ANNOTATE` directly, only the dashed path runs. Names starting with
`ch_` are Nextflow channels, not chromosomes. Each VCF is one whole task per
process, with no per-chromosome scatter; vepyr parallelizes inside its task, up
to `cpus` pipelines.

## Adding the module to a pipeline

Install the module from nf-core/modules with [nf-core tools](https://nf-co.re/tools),
running the command from your pipeline directory:

```bash
cd my-pipeline
nf-core modules install vepyr/annotate
```

This installs the module under `modules/nf-core/vepyr/annotate/` and records its
revision in `modules.json`.

## Example

A minimal pipeline that annotates one VCF (vepyr always runs `--everything`):

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
    // A bgzip FASTA needs its .gzi as well as the .fai; a plain FASTA passes [] for it.
    fasta = channel.value([
        [ id: 'GRCh38' ],
        file(params.fasta, checkIfExists: true),
        file("${params.fasta}.fai", checkIfExists: true),
        params.fasta.endsWith('.gz') ? file("${params.fasta}.gzi", checkIfExists: true) : []
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
                container = 'community.wave.seqera.io/library/htslib_vepyr:ebb29e9a21ff05c9'
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

Results land in `results/vepyr/HG002_vepyr.vcf.gz` and `results/vepyr/HG002_vepyr.vcf.gz.tbi`.

This exact pipeline, run with `-profile arm64` on all 50,861 normalized HG002
chr22 records against a release-116 `ensembl` cache, reproduces the record-body
md5 of Ensembl VEP 116 `--everything` (see [Testing vs Ensembl VEP](testing-vep.md)).

## Normalizing first

vepyr annotates records as given. The parity inputs were normalized with
`bcftools norm -m -both` (multiallelic records split, indels not left-aligned).
To run that step in the same pipeline, use the `vcf_annotate_vepyr` subworkflow,
which runs nf-core's `bcftools/norm` module before `VEPYR_ANNOTATE`. It takes the
module's inputs plus two values: `val_normalize`, set to `true` to normalize, and
`val_hf_repo`, which can download the cache from Hugging Face instead of reading
it from channel 2 (see [Cache from Hugging Face](#cache-from-hugging-face)). Both
steps read the reference FASTA from channel 3.

The subworkflow is in review as
[nf-core/modules#13070](https://github.com/nf-core/modules/pull/13070) and is
not yet available from nf-core/modules. Next to `vepyr/annotate` (installed as
above), it needs nf-core's `bcftools/norm` and `huggingface/download` at the
paths nf-core tooling uses; both are included even when unused. From the parent
directory of `my-pipeline`, clone this repository and copy the subworkflow:

```bash
git clone --depth 1 https://github.com/biodatageeks/vepyr.git
mkdir -p my-pipeline/subworkflows/nf-core
cp -R vepyr/nf-core-module/subworkflows/nf-core/vcf_annotate_vepyr my-pipeline/subworkflows/nf-core/

# In an nf-core pipeline: nf-core modules install bcftools/norm huggingface/download
# Otherwise copy them at the commit the subworkflow is tested against:
ref=45778ac3f84844e33a7afd9282a3a26409c277b2
for module in bcftools/norm huggingface/download; do
    mkdir -p my-pipeline/modules/nf-core/$module
    for f in main.nf meta.yml environment.yml; do
        curl -fsSL -o my-pipeline/modules/nf-core/$module/$f \
            https://raw.githubusercontent.com/nf-core/modules/$ref/modules/nf-core/$module/$f
    done
done
```

In the example above, include the subworkflow instead of the module and call it
with the same channels plus `true` and `''` (no Hugging Face download):

```groovy title="main.nf"
include { VCF_ANNOTATE_VEPYR } from './subworkflows/nf-core/vcf_annotate_vepyr/main'

// ... vcf, cache and fasta channels as in the example above ...

    VCF_ANNOTATE_VEPYR(vcf, cache, fasta, params.cache_version, [[], []], true, '')

    VCF_ANNOTATE_VEPYR.out.vcf_tbi.view { meta, annotated, tbi -> "${meta.id}: ${annotated}" }
```

`cache` and `fasta` must be value channels (`channel.value(...)` or
`.collect()`), as in the example: with a queue channel only the first sample is
annotated.

Configure `BCFTOOLS_NORM` as validated. This is required, not optional:

```groovy
process {
    withName: 'BCFTOOLS_NORM' {
        ext.args = '--multiallelics -both --do-not-normalize --output-type z --write-index=tbi'
    }
}
```

Without this block, `bcftools/norm` falls back to its default `ext.args` of
`--output-type z`: it left-aligns indels against the reference and leaves
multiallelic records unsplit.

Without `--do-not-normalize`, bcftools also left-aligns indels against the
reference, and it fails when the FASTA and VCF name contigs differently. Keep
the output bgzip VCF (`--output-type z`): `VEPYR_ANNOTATE` runs a single
pipeline for BCF and plain VCF. `--write-index=tbi` is optional; without it,
`VEPYR_ANNOTATE` builds the index itself. On the raw HG002 chr22 benchmark
records this subworkflow reproduces the Ensembl VEP 116 `--everything`
record-body md5.

No `ext.prefix` is needed. `bcftools/norm` writes `${meta.id}_norm.vcf.gz` and
`VEPYR_ANNOTATE` writes `${meta.id}_vepyr.vcf.gz`, and both stop with an error
if a prefix you set would overwrite their staged input.

### Cache from Hugging Face

Instead of a local cache, pass `[[], []]` as channel 2 and a [prebuilt vepyr cache](quickstart.md#option-a-download-a-prebuilt-cache-recommended)
from Hugging Face as `val_hf_repo`. `HUGGINGFACE_DOWNLOAD` downloads
it once and every sample shares it. Setting both a cache and `val_hf_repo`, or
neither, is an error.

```groovy title="main.nf"
    VCF_ANNOTATE_VEPYR(vcf, [[], []], fasta, 116, [[], []], true, 'biodatageeks/vepyr_116_GRCh38_ensembl')
```

To pin the cache to a commit, or download only the contigs your VCFs carry, set
`HUGGINGFACE_DOWNLOAD`'s `ext.args`. A partial download must include every
entity's `chrom_manifest.json` and every contig present in the VCF:

```groovy
process {
    withName: 'HUGGINGFACE_DOWNLOAD' {
        ext.args = "--revision <commit> --include '*/chr22.parquet' --include '*/chrom_manifest.json'"
    }
}
```

## Inputs

| Channel | Shape | Notes |
|---|---|---|
| 1 | `[ meta, vcf, tbi ]` | Input VCF, plain or bgzip. The index, `.tbi` or `.csi`, is optional — pass `[]` — and the task then builds one with `tabix` for a `.gz` input, which must be bgzip; a plain `.vcf` runs a single pipeline. |
| 2 | `[ meta2, cache ]` | vepyr Parquet cache **directory**, e.g. `116_GRCh38_ensembl`. Not an Ensembl VEP cache. |
| 3 | `[ meta3, fasta, fai, gzi ]` | Reference FASTA, its `.fai` and, for a bgzip FASTA, its `.gzi` — pass `[]` for `gzi` with a plain FASTA. Required: vepyr always runs `--everything`, which needs it, and `bcftools/norm` reads it when `val_normalize` is `true`. |
| 4 | `cache_version` | Release the cache must carry in its metadata, e.g. `116`. Pass `[]` to skip the check. |
| 5 | `[ meta4, plugin_cache ]` | Root of a [plugin cache](plugins.md) tree, or `[ [], [] ]` for none. |

The module never builds the reference indexes: vepyr opens the reference through
its `.fai` (and a bgzip reference through its `.gzi` as well), so a missing
FASTA index fails the task. Only the input VCF's index is built when absent.

## Outputs

| Emit | Shape | Content |
|---|---|---|
| `vcf` | `[ meta, "${prefix}.vcf.gz" ]` | Annotated VCF, bgzip. |
| `tbi` | `[ meta, "${prefix}.vcf.gz.tbi" ]` | tabix index of it. |
| `versions_vepyr`, `versions_tabix` | `[ process, tool, version ]` | Also published on the `versions` topic. |

## Configuration

| Setting | Effect |
|---|---|
| `ext.args` | Extra `vepyr annotate` flags, e.g. `'--plugin clinvar'`. See [Command line](cli.md#options). |
| `ext.args2` | Extra `tabix` flags for indexing the output. |
| `ext.prefix` | Output file name stem. Default: `${meta.id}_vepyr`. It must differ from the input VCF's name. |
| `cpus` | Annotation pipelines (`--fork`). |

The module sets `-i`, `-o`, `--dir_cache`, `--fasta`, `--cache_version`,
`--plugin_cache_root`, `--fork` and `--no_progress` itself.

**Annotation always runs `--everything`.** The module pins
`bioconda::vepyr=0.9.0`, which always annotates as Ensembl VEP `--everything`
and accepts `--everything` and `--hgvsc` only as no-ops, so `ext.args` need not
carry them. The reference FASTA is therefore required: the module stops before
starting the task when channel 3 carries none.

**`--fork` comes from `cpus`, not `ext.args`.** The module appends
`--fork ${task.cpus}` after `ext.args`, so a `--fork` or `--workers` in
`ext.args` is overridden. More than one pipeline needs an indexed VCF, so a
plain `.vcf` input runs with `--fork 1`.

**Unknown flags fail the task.** `vepyr annotate` rejects any VEP flag it does not
implement, including one that is a prefix of a vepyr flag: `--hgvs` is not read
as `--hgvsc`, nor `--dir` as `--dir_cache`. An `ext.args` copied from an
`ensemblvep/vep` configuration therefore fails rather than silently annotating
differently.

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
        ext.args = '--plugin clinvar --plugin cadd'
    }
}
```

Without `--plugin`, every plugin under the root is applied, in alphabetical
order; each `--plugin` narrows the set and fixes its `CSQ` block order. The root
is the directory that *contains* `plugin/`. See [Plugins](plugins.md).

## Containers

| Engine | linux/amd64 | linux/arm64 |
|---|---|---|
| Docker | `community.wave.seqera.io/library/htslib_vepyr:408f2021357958aa` | `community.wave.seqera.io/library/htslib_vepyr:ebb29e9a21ff05c9` |
| Singularity / Apptainer | `oras://community.wave.seqera.io/library/htslib_vepyr:5f4d3201731219a4` | `oras://community.wave.seqera.io/library/htslib_vepyr:76a3378692bb013e` |

They are [Seqera Wave](https://seqera.io/containers/) builds of the module's
`environment.yml`: `bioconda::vepyr` and `bioconda::htslib` (for `tabix`). The
module's `meta.yml` lists them with conda lock files for both platforms, and
`-profile conda` uses `environment.yml` directly. For Singularity and Apptainer,
`main.nf` refers to the image by its https download URL, which `meta.yml` lists
next to the `oras://` URI above; the two name the same build, and either works as
a `container` override.

!!! warning "Use the image for your host's architecture"
    `main.nf` names the `linux/amd64` images, which is what Nextflow runs unless
    you override `container`. On an arm64 host, select the arm64 image as the
    `arm64` profile above does. Running the amd64 image under emulation on Apple
    Silicon fails with `SIGILL` (exit 132): Docker's emulated x86_64 guest has no
    AVX, which the native extension uses.

    The same applies to Singularity and Apptainer: `main.nf` names the amd64
    image for them too. On an arm64 host, override it with the arm64 image from
    the table above:

    ```groovy
    process {
        withName: 'VEPYR_ANNOTATE' {
            container = 'oras://community.wave.seqera.io/library/htslib_vepyr:76a3378692bb013e'
        }
    }
    ```

    nf-core modules name a single image per engine; pipelines choose the
    architecture in their own config, as the `arm64` profile above does for Docker.
