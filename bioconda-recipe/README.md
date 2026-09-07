# Bioconda recipe for vepyr 0.6.0

> **Status.** `meta.yaml` is bumped to 0.6.0, the first release carrying the
> `vepyr` console script that the nf-core module wraps. The `sha256` is still
> the 0.5.0 digest and **must be replaced** with the 0.6.0 sdist digest once
> that is on PyPI. The validation results below are the 0.5.0 run; 0.6.0 has
> not been validated yet.

This recipe follows the source-build approach used by
[polars-bio in bioconda-recipes#67602](https://github.com/bioconda/bioconda-recipes/pull/67602).
It targets Linux x86-64, macOS x86-64, and macOS arm64, matching the Unix wheel
platforms published for vepyr.

Submitted as [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869).

## Release source

The recipe builds from the PyPI source distribution. The digest currently in
`meta.yaml` is the **0.5.0** one, verified against that archive:

```text
1afe1824512e211f084e298fc86fa964e9516bcc87086ea02dde4a045237d538
```

Replace it with the 0.6.0 digest before submitting the bump:

```bash
curl -sL https://pypi.org/pypi/vepyr/0.6.0/json | \
    python -c "import json,sys; print(json.load(sys.stdin)['urls'][-1]['digests']['sha256'])"
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

## Validation (0.5.0)

These results are for 0.5.0. The 0.6.0 bump has not been validated.

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
