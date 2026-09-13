# nf-core module: `vepyr/annotate`

Staging area for the nf-core/modules submission. `modules/nf-core/vepyr/annotate/`
mirrors the upstream path, so it copies verbatim into a nf-core/modules fork:

```bash
cp -R modules/nf-core/vepyr /path/to/modules-fork/modules/nf-core/
```

`.nf-core.yml` and `tests/config/nf-test.config` exist only so `nf-core modules
lint` treats this directory as a modules repository. nf-core/modules has its own
copies — do not copy these upstream.

## Testing on Apple Silicon (linux/arm64)

bioconda has no linux-aarch64 vepyr yet ([bioconda-recipes#69191](https://github.com/bioconda/bioconda-recipes/pull/69191)),
so no arm64 Wave image can be built from `environment.yml`, and the amd64 image
dies with SIGILL under emulation (no AVX). Until then, `dev/` holds a stand-in:

```bash
./dev/nf-test-arm64.sh
```

### nf-test setup

nf-test has no Homebrew formula. Its installer writes an `nf-test` launcher into
the *current directory* (and the jar into `~/.nf-test/`), so run it from a
directory on your `PATH` — not from the repository root:

```bash
mkdir -p ~/.local/bin && cd ~/.local/bin
curl -fsSL https://get.nf-test.com | bash
cd - && nf-test version
```

Needs Java 11+ (the one Nextflow uses). If `~/.local/bin` is not on `PATH`, add
`export PATH="$HOME/.local/bin:$PATH"` to `~/.zshrc`. The runner uses `nf-test`
from `PATH`; to pick a different one, set `NF_TEST`:

```bash
cd nf-core-module
NF_TEST="$(which nf-test)" ./dev/nf-test-arm64.sh
```

It builds `dev/Dockerfile` (the `environment.yml` packages, with vepyr from its
PyPI aarch64 wheel), stages the fixture into `.testdata/`, fetches the UNTAR
module, and runs both tests natively with `dev/nf-test.config`. To test an
unreleased engine, put a linux aarch64 wheel in `dev/wheels/` and set
`VEPYR_SPEC=/wheels/<file>.whl`.

The runner also executes `dev/tests/hg002_chr22.nf.test`, an offline Ensembl VEP
parity check: all 50,861 normalized HG002 chr22 records annotated with
`--everything` must reproduce the record-body md5 of VEP 116
(`f0a0a7021c498d2b4e38c9caf5959f77`). It reads `tests/data/hg002_chr22/` (~30 MB,
Parquet and FASTA in LFS): a cache trimmed to the rows that run reads, plus a
bgzip FASTA passed with `[ fai, gzi ]` in the index slot. Rebuild it with
`uv run python tests/data/hg002_chr22/prepare.py`.

`dev/` is not part of the submission. Do not commit a snapshot produced by it,
and delete `dev/` once the Wave URIs land — moving the parity test and its
config somewhere that outlives `dev/` first.

## Linting

```bash
uvx --from nf-core nf-core modules lint vepyr/annotate
```

Requires `nextflow` on `PATH`. Current result: **54 passed, 0 warnings, 2 failed**.
Both failures are upstream blockers rather than defects, and both clear on their
own once the steps below are done:

- `bioconda_version` — `bioconda::vepyr=0.7.0` cannot be resolved until 0.7.0 is
  on bioconda (step 2).
- `test_snapshot_exists` — `main.nf.test.snap` can only be produced by a real run
  (step 5).

Treat any *third* failure as a genuine regression.

## Remaining work, in order

1. **~~Get vepyr onto bioconda.~~** Done: 0.6.0 merged in
   [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869)
   (linux-64, osx-64, osx-arm64 only).
2. **Merge [bioconda-recipes#69191](https://github.com/bioconda/bioconda-recipes/pull/69191).**
   It bumps the recipe to 0.7.0, the version this module pins, and adds
   `linux-aarch64`, which step 3 needs for an arm64 image. It supersedes the
   BiocondaBot autobump #69182, which does not add the platform.
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

Twelve flags, covering the configuration validated against Ensembl VEP
(`--everything` with `--fasta`) plus what a workflow engine needs. The pick family,
HGVS sub-flags, AF sub-flags and `--fields` are all additive later and need no
engine work — see `docs/superpowers/specs/2026-09-07-nf-core-vepyr-module-design.md`.

Note that unknown flags are a hard error, so an `ext.args` string copied from an
`ensemblvep/vep` config will fail the task if it carries a flag outside that twelve.

`--fork` is the one flag `ext.args` cannot set. The module emits it after `${args}`
so its computed value always wins: `--fork`/`--workers` are a single argparse
option, the last occurrence wins, and a user-supplied one would otherwise defeat
the unindexed-input fallback in `main.nf` and fail the task. Use the `cpus`
directive to control parallelism.
