# nf-core module: `vepyr/annotate`

Staging area for the nf-core/modules submission. `modules/nf-core/vepyr/annotate/`
mirrors the upstream path, so it copies verbatim into a nf-core/modules fork:

```bash
cp -R modules/nf-core/vepyr /path/to/modules-fork/modules/nf-core/
```

`.nf-core.yml` and `tests/config/nf-test.config` exist only so `nf-core modules
lint` treats this directory as a modules repository. nf-core/modules has its own
copies — do not copy these upstream.

## Linting

```bash
uvx --from nf-core nf-core modules lint vepyr/annotate
```

Requires `nextflow` on `PATH`. Current result: **53 passed, 0 warnings, 2 failed**.
Both failures are upstream blockers rather than defects, and both clear on their
own once the steps below are done:

- `bioconda_version` — `bioconda::vepyr=0.6.0` cannot be resolved because vepyr is
  not on bioconda yet (step 2).
- `test_snapshot_exists` — `main.nf.test.snap` can only be produced by a real run
  (step 5).

Treat any *third* failure as a genuine regression.

## Remaining work, in order

1. **Release vepyr 0.6.0 to PyPI.** The CLI this module wraps first ships in 0.6.0.
2. **Update [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869) to 0.6.0.**
   Bump `version`, replace `sha256` with the 0.6.0 sdist digest, and keep the
   `vepyr --version` / `vepyr annotate --help` test commands added in
   `bioconda-recipe/meta.yaml`. Bumping the open PR rather than filing a follow-up
   avoids ever publishing a container without a `vepyr` executable.

   As of 2026-09-07 that PR is open with every check green (Lint, Linux, OSX-64,
   ARM) for py310-py313 on linux-64 / osx-64 / osx-arm64, so the bump is the only
   thing standing between it and a merge.

   Its `@BiocondaBot please fetch artifacts` run does publish Docker images, but
   they are **not usable here**: they are tarballs inside the linux-64 zip, loaded
   with `docker load`, not registry-hosted — and they are built from 0.5.0, which
   has no `vepyr` executable at all.
3. **Resolve the container URIs.** Replace `PLACEHOLDER_DOCKER_URI` and
   `PLACEHOLDER_SINGULARITY_URI` in `main.nf` with a Seqera Wave image built from
   `environment.yml` (https://seqera.io/containers/ emits both the Docker and the
   Singularity URI for a package list).

   Note this cannot be a plain `quay.io/biocontainers/vepyr:...` image. Bioconda
   publishes one container per package, and `environment.yml` needs two — vepyr for
   annotation and htslib for the `tabix` call that indexes the output. Wave builds
   the combined image, and it can only do so once vepyr is on bioconda, so step 2
   genuinely blocks this one.
4. **PR the test data.** Run `./stage-testdata.sh <dir>`, then copy the resulting
   `data/` tree into a clone of the `modules` branch of nf-core/test-datasets.
   About 6 MB: `cache.tar.gz` (the chr1 Parquet cache), an 875 KB reference FASTA
   with its `.fai`, and a 100-variant VCF with its `.tbi`. Unlike `ensemblvep/vep`
   — whose tests are effectively stubs because its cache is too large to host —
   this fixture is small enough that the module can assert on real annotation
   output.

   The cache is an archive rather than a directory because
   `modules_testdata_base_path` points at raw.githubusercontent.com, which serves
   blobs and not directory trees, so Nextflow cannot stage a directory URL.
   (`ensemblvep/vep` and `snpeff/snpeff` get away with directories only because
   they read from `s3://annotation-cache/`, where Nextflow can list.) The nf-test
   extracts it in a `setup` block with the `UNTAR` module, the same pattern
   `kraken2/kraken2` uses for its database.
5. **Generate the snapshot.** With 3 and 4 done:
   `nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test --update-snapshot`
6. **Open the nf-core/modules PR.**

## Scope

Eleven flags, covering the configuration validated against Ensembl VEP
(`--everything` with `--fasta`) plus what a workflow engine needs. The pick family,
HGVS sub-flags, AF sub-flags and `--fields` are all additive later and need no
engine work — see `docs/superpowers/specs/2026-09-07-nf-core-vepyr-module-design.md`.

Note that unknown flags are a hard error, so an `ext.args` string copied from an
`ensemblvep/vep` config will fail the task if it carries a flag outside that eleven.

`--fork` is the one flag `ext.args` cannot set. The module emits it after `${args}`
so its computed value always wins: `--fork`/`--workers` are a single argparse
option, the last occurrence wins, and a user-supplied one would otherwise defeat
the unindexed-input fallback in `main.nf` and fail the task. Use the `cpus`
directive to control parallelism.
