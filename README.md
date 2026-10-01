# vepyr

![PyPI - Version](https://img.shields.io/pypi/v/vepyr)
[![Python versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue?logo=python&logoColor=white)](https://pypi.org/project/vepyr/)
[![Platforms](https://img.shields.io/badge/platforms-linux%20%7C%20macOS%20%7C%20Windows-lightgrey)](https://pypi.org/project/vepyr/#files)
![GitHub License](https://img.shields.io/github/license/biodatageeks/vepyr)
![PyPI - Downloads](https://img.shields.io/pypi/dm/vepyr)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/vepyr)
[![Bioconda](https://img.shields.io/conda/vn/bioconda/vepyr?label=bioconda)](https://anaconda.org/bioconda/vepyr)
[![Bioconda - Downloads](https://img.shields.io/conda/dn/bioconda/vepyr?label=bioconda%20downloads)](https://anaconda.org/bioconda/vepyr)
[![Bioconda - Platforms](https://img.shields.io/conda/pn/bioconda/vepyr?label=bioconda%20platforms)](https://anaconda.org/bioconda/vepyr)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/biodatageeks/vepyr)

![CI](https://github.com/biodatageeks/vepyr/actions/workflows/ci.yml/badge.svg?branch=master)
![Docs](https://github.com/biodatageeks/vepyr/actions/workflows/publish_documentation.yml/badge.svg?branch=master)

<p align="center">
  <img src="docs/logo.png" alt="vepyr logo" width="320">
</p>

**vepyr** (/ˈvaɪpər/) — VEP Yielding Performant Results — is a blazing-fast Rust
reimplementation of Ensembl's [Variant Effect
Predictor](https://www.ensembl.org/info/docs/tools/vep/index.html), exposed as a
Python library and a `vepyr` command. It builds and uses Ensembl VEP caches
locally, annotates VCF input through a native DataFusion engine, and returns
results as a `polars.LazyFrame` or a VCF with `CSQ` in the `INFO` column.

## 📚 Documentation

**<https://biodatageeks.org/vepyr/>**

| | |
|---|---|
| [Quick start](https://biodatageeks.org/vepyr/quickstart/) | Install, get a cache, annotate |
| [Polars DataFrames](https://biodatageeks.org/vepyr/dataframes/) | Schema, region filters, `filter_vep` in Polars |
| [Command line](https://biodatageeks.org/vepyr/cli/) | `vepyr annotate`, VCF in, VCF out |
| [Nextflow](https://biodatageeks.org/vepyr/nextflow/) | The `vepyr/annotate` module, amd64 and arm64 containers |
| [Download Ensembl VEP and plugin caches](https://biodatageeks.org/vepyr/downloads/) | Prebuilt release-116 caches |
| [Caches](https://biodatageeks.org/vepyr/caches/) | Cache types, entity schemas, CSQ output fields |
| [Plugins](https://biodatageeks.org/vepyr/plugins/) | CADD, SpliceAI, AlphaMissense, ClinVar, dbNSFP, PhenotypeOrthologous |
| [API reference](https://biodatageeks.org/vepyr/api/) | `build_cache()`, `annotate()`, … |
| [Performance](https://biodatageeks.org/vepyr/performance/) | Benchmarks vs Ensembl VEP |

## Install

```bash
pip install vepyr
```

or from [bioconda](https://anaconda.org/bioconda/vepyr)
(linux-64, osx-64, osx-arm64):

```bash
conda install -c conda-forge -c bioconda vepyr
```

Either installs both the Python package and a `vepyr` executable:

```bash
vepyr annotate \
    -i input.vcf.gz \
    -o annotated.vcf.gz \
    --dir_cache ~/vepyr_cache/116_GRCh38_ensembl \
    --fasta GRCh38.fa \
    --fork 8
```

See [Developers](https://biodatageeks.org/vepyr/developers/) for building from
source and running the test suite.

## Reproduce the chr22 comparison

Paper reviewers can check **all 10 core comparison profiles** against Ensembl
VEP **116.0** on the 50,861 normalized HG002 GRCh38 chr22 records with Docker.
The image includes vepyr 0.9.0, the Hugging Face client, Git LFS and the VCF tools;
Ensembl VEP does not need to be installed.

From the repository root:

```bash
docker build -t vepyr-chr22-reviewer e2e-testing/reviewer
mkdir -p e2e-testing/results/reviewer-chr22
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/repo" \
  -v "$PWD/e2e-testing/results/reviewer-chr22:/work" \
  vepyr-chr22-reviewer
```

The script downloads only chr22 shards and manifests for the Ensembl, merged
and RefSeq caches from **Hugging Face**, at pinned revisions (~1.5 GB total).
The ten BGZF golden VCFs are stored in Git LFS (~178 MB); the script fetches
missing LFS objects automatically. Downloads and results persist between runs.
Allow about 5 GB of free disk space, including the Docker image.

A successful run prints **`10/10 profiles passed`** and exits zero. Both strict
and canonical record-body md5 digests appear in
`e2e-testing/results/reviewer-chr22/summary.tsv`; any failure gives a nonzero exit.
The two gene-selection profiles sort CSQ entries before hashing to account for
VEP's variable entry order.

For a shorter sanity check, append **`--profiles merged`** to the Docker run
command; this downloads only the merged cache and runs one profile. Append
`--offline` to reuse verified downloads without network access. See the
[reviewer workflow](e2e-testing/README.md#reviewer-chr22-sanity-check) for profile
names, output files, native execution and golden-data provenance.

## License

[Apache-2.0](LICENSE)
