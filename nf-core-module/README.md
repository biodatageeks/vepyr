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
  not on bioconda yet — it clears when #68869 merges.
- `test_snapshot_exists` — `main.nf.test.snap` can only be produced by a real run
  (step 3).

Treat any *third* failure as a genuine regression.

## Remaining work, in order

**Done (2026-09-08):**

- ~~Release vepyr 0.6.0 to PyPI.~~ The CLI this module wraps first ships in
  0.6.0. sdist `7a28e6bc6f25d550e46765df0a94a7ca27a4a3d3e77ca778da0940d0c7a7a7ad`,
  verified to carry `src/vepyr/cli.py`, `src/vepyr/__main__.py` and the
  `[project.scripts] vepyr = "vepyr.cli:main"` entry.
- ~~Bump [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869)
  to 0.6.0.~~ Pushed as `ea0e8cb4`, PR retitled "Add vepyr 0.6.0". Bumping the
  open PR rather than filing a follow-up avoids ever publishing a container
  without a `vepyr` executable. **Not yet merged** — that merge is what unblocks
  step 1 below.

1. **Resolve the container URIs.** Replace `PLACEHOLDER_DOCKER_URI` and
   `PLACEHOLDER_SINGULARITY_URI` in `main.nf` with a Seqera Wave image built from
   `environment.yml` (https://seqera.io/containers/ emits both the Docker and the
   Singularity URI for a package list).

   This cannot be a plain `quay.io/biocontainers/vepyr:...` image. Bioconda
   publishes one container per package, and `environment.yml` needs two — vepyr for
   annotation and htslib for the `tabix` call that indexes the output. Wave builds
   the combined image, and it can only do so once vepyr is *on* bioconda, so the
   recipe merge above genuinely blocks this step.

   Note also that #68869's `@BiocondaBot please fetch artifacts` run publishes
   Docker images which look like they would serve: they are not registry-hosted,
   only tarballs inside the linux-64 zip loaded with `docker load`, and they are
   single-package vepyr images with no `tabix`.
2. **PR the test data.** Run `./stage-testdata.sh <dir>`, then copy the resulting
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

   This step does not depend on the recipe merge and can run in parallel with 1.
3. **Generate the snapshot.** With 1 and 2 done:
   `nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test --update-snapshot`

   This is the real gate, not a formality. Every bug review found in this module
   was in input staging — a missing `.fai`, an unstamped cache, a directory URL
   that cannot be fetched — and none were reachable from the Python test suite,
   which reads its fixtures in place. Nothing exercises Nextflow's staging until
   this runs.
4. **Open the nf-core/modules PR.**

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
