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

## Quick sanity check with chr22

Check Ensembl VEP **116.0** parity on the 50,861 normalized HG002 GRCh38 chr22
records using the existing `run_comparison.py` CLI. Docker provides vepyr 0.9.0,
the Hugging Face client, Git LFS and the VCF tools.

From the repository root, download the merged chr22 cache and run its md5 check:

```bash
docker build -t vepyr-e2e e2e-testing/docker
mkdir -p e2e-testing/results/sanity-chr22
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/repo" \
  -v "$PWD/e2e-testing/results/sanity-chr22:/work" \
  vepyr-e2e bash -ec '
    python e2e-testing/scripts/download_chr22.py --work-dir /work --profiles merged
    DATA_VEPYR_DIR=/work python e2e-testing/scripts/run_comparison.py \
      --release 116 --profile merged --chroms 22 \
      --vcf /work/input/input_chr22.vcf.gz --fasta /work/input/chr22.fa.gz \
      --output-dir /work --comparison-mode md5 --md5-mode both --bgzf --no-normalize
  '
```

The download helper fetches only chr22 shards and manifests from **Hugging Face**
at pinned revisions, plus the BGZF golden VCF and FASTA through **Git LFS**.
The comparison CLI prints the strict and canonical record-body md5 verdicts and
exits nonzero on a mismatch. Downloads and results persist in the mounted folder.
The input is already normalized with `bcftools norm -m -both`.

See [Quick sanity check with chr22](e2e-testing/README.md#quick-sanity-check-with-chr22)
for the command that runs **all 10 profiles**, offline reuse and golden-data
provenance. The three caches total ~1.5 GB and the ten BGZF goldens total ~178 MB;
allow about 6 GB of disk space for the complete run and Docker image.

## License

[Apache-2.0](LICENSE)
