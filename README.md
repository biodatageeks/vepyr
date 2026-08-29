# vepyr

![PyPI - Version](https://img.shields.io/pypi/v/vepyr)
[![Python versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue?logo=python&logoColor=white)](https://pypi.org/project/vepyr/)
[![Platforms](https://img.shields.io/badge/platforms-linux%20%7C%20macOS-lightgrey)](https://pypi.org/project/vepyr/#files)
![GitHub License](https://img.shields.io/github/license/biodatageeks/vepyr)
![PyPI - Downloads](https://img.shields.io/pypi/dm/vepyr)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/biodatageeks/vepyr)

![CI](https://github.com/biodatageeks/vepyr/actions/workflows/ci.yml/badge.svg?branch=master)
![Docs](https://github.com/biodatageeks/vepyr/actions/workflows/publish_documentation.yml/badge.svg?branch=master)

<p align="center">
  <img src="docs/logo.png" alt="vepyr logo" width="320">
</p>

**vepyr** (/ˈvaɪpər/) — VEP Yielding Performant Results — is a blazing-fast Rust
reimplementation of Ensembl's [Variant Effect
Predictor](https://www.ensembl.org/info/docs/tools/vep/index.html), exposed as a
Python library. It builds and uses Ensembl VEP caches locally, annotates VCF
input through a native DataFusion engine, and returns results as a
`polars.LazyFrame` or a VCF with `CSQ` in the `INFO` column.

## 📚 Documentation

**<https://biodatageeks.org/vepyr/>**

| | |
|---|---|
| [Quick start](https://biodatageeks.org/vepyr/quickstart/) | Install, get a cache, annotate |
| [Download Ensembl VEP and plugin caches](https://biodatageeks.org/vepyr/downloads/) | Prebuilt release-116 caches |
| [Caches](https://biodatageeks.org/vepyr/caches/) | Cache types, entity schemas, CSQ output fields |
| [Plugins](https://biodatageeks.org/vepyr/plugins/) | CADD, SpliceAI, AlphaMissense, ClinVar, dbNSFP |
| [API reference](https://biodatageeks.org/vepyr/api/) | `build_cache()`, `annotate()`, … |
| [Performance](https://biodatageeks.org/vepyr/performance/) | Benchmarks vs Ensembl VEP |

## Install

```bash
pip install vepyr
```

See [Developers](https://biodatageeks.org/vepyr/developers/) for building from
source and running the test suite.

## License

[Apache-2.0](LICENSE)
