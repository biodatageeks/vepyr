# Bioconda recipe for vepyr 0.6.0

> **Status.** `meta.yaml` targets 0.6.0, the first release carrying the `vepyr`
> console script that the nf-core module wraps, with the released 0.6.0 sdist
> digest in place. Bioconda CI is green on the bumped
> [PR](https://github.com/bioconda/bioconda-recipes/pull/68869) (`ea0e8cb4`),
> which is awaiting merge.

This recipe follows the source-build approach used by
[polars-bio in bioconda-recipes#67602](https://github.com/bioconda/bioconda-recipes/pull/67602).
It targets Linux x86-64, macOS x86-64, and macOS arm64, matching the Unix wheel
platforms published for vepyr.

Submitted as [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869).

## Release source

The recipe builds from the [PyPI 0.6.0 source
distribution](https://pypi.org/project/vepyr/0.6.0/#files), with SHA-256
verified by hashing the downloaded archive rather than trusting the API
response:

```text
7a28e6bc6f25d550e46765df0a94a7ca27a4a3d3e77ca778da0940d0c7a7a7ad
```

The sdist was also checked to carry `src/vepyr/cli.py`, `src/vepyr/__main__.py`
and the `[project.scripts] vepyr = "vepyr.cli:main"` entry — without those the
built container has no `vepyr` executable and the nf-core module cannot run.

To re-derive the digest for a future bump:

```bash
curl -sLO https://files.pythonhosted.org/packages/source/v/vepyr/vepyr-<version>.tar.gz
shasum -a 256 vepyr-<version>.tar.gz
```

The Python requirement (`>=3.10`) and runtime dependency bounds come from the
archive's `PKG-INFO`: `pyarrow>=18.0`, `polars>=1.37.1`, and `tqdm>=4.60`.
DataFusion is a compiled Rust dependency, so the Python `datafusion` package
is not required.

The build uses conda's Rust compiler and maturin, keeps the released
`Cargo.lock` with `--locked`, and bundles third-party crate licenses in
`THIRDPARTY.yml`. Git is needed to fetch the pinned bio-functions, bio-formats,
noodles, and opendal revisions. The development toolchain pin is removed from
the extracted source; conda's compiler activation flags are preserved.

## Submit to Bioconda

Copy `meta.yaml` and `build.sh` into `recipes/vepyr/` in a
`bioconda-recipes` checkout. This README stays in the vepyr repository.

With `bioconda-utils` installed and its environment activated, run from the
`bioconda-recipes` checkout:

```bash
bioconda-utils lint recipes config.yml --packages vepyr
bioconda-utils build recipes config.yml --packages vepyr
```

Use the standard Bioconda CI to validate every target platform and run the
Linux mulled container tests before merging the recipe.

## Smoke tests

The recipe checks imports, installed dependency compatibility, the Python and
compiled Rust versions, and the compiled VEP 115/116 compatibility records.
It also runs `vepyr --version` and `vepyr annotate --help`, so a container
built without a working console script fails the recipe rather than shipping —
the module invokes `vepyr annotate` and would be useless without it. And it
creates a small VCF without contig headers, checking the native provider's
DataFusion scan returns both chromosomes. The functional tests use
runtime dependencies and temporary data, so they can run in the mulled container
without a downloaded VEP cache or external test files. Full annotation parity
remains covered by the upstream release tests.

## Validation

### 0.6.0

Bioconda CI passed on 2026-09-08 for commit `ea0e8cb4`:

- Lint, Linux Tests, OSX-64 Tests, and `build and test (ARM)` all passed;
  `Summary` green. (`Mergify Merge Queue` reports `neutral`, which is the queue
  check idling, not a failure.)
- The build matrix is unchanged from the 0.5.0 run — Python 3.10-3.13 across
  linux-64, osx-64 and osx-arm64 — since the bump touches only `version`,
  `sha256` and the two CLI test commands. The per-package listing and the Linux
  mulled container tests are **not** re-confirmed for this commit: Bioconda runs
  the whole matrix inside one job per platform, so that detail only appears in a
  `@BiocondaBot please fetch artifacts` comment, which has not been run on
  `ea0e8cb4`. Run it there if the per-variant record matters.

Local validation on 2026-09-08, against the published 0.6.0 wheel in an isolated
Python 3.12 environment — all five of the recipe's `test: commands:` entries:

- `pip check` — no broken requirements.
- `vepyr --version` → `vepyr 0.6.0`.
- `vepyr annotate --help` → the annotate usage block.
- The compiled-VEP-targets assertion → `compiled VEP targets passed`.
- The native VCF scan → `native VCF scan passed`.

Source checks:

- The sdist SHA-256 was verified by hashing the downloaded archive, and agrees
  with what the PyPI API reports.
- The sdist was confirmed to contain `src/vepyr/cli.py`, `src/vepyr/__main__.py`
  and the `[project.scripts] vepyr = "vepyr.cli:main"` entry — the whole point of
  the bump, since a 0.5.0 container has no `vepyr` executable for the nf-core
  module to call.

### 0.5.0 (superseded)

Kept for the per-variant record the 0.6.0 run has not reproduced.

Local validation on 2026-09-06:

- Bioconda lint: all checks OK.
- Released source checksum, dependency bounds, and Python selectors verified.
- Recipe smoke tests passed against the published 0.5.0 wheel in an isolated
  Python 3.12 environment.
- Native conda source build and all recipe tests passed on `osx-arm64` with
  Python 3.12, using conda-forge and Bioconda compiler configuration.

Bioconda CI passed on 2026-09-07 (Europe/Warsaw) for commit `67f7982`:

- Lint passed.
- Linux x86-64, macOS x86-64, and macOS arm64 each built and passed recipe tests
  on Python 3.10, 3.11, 3.12, and 3.13 (12 packages in total).
- Linux mulled container tests passed for all four Python variants.

See the [PR checks](https://github.com/bioconda/bioconda-recipes/pull/68869/checks).
