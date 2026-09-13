# nf-core module: `vepyr/annotate`

Staging area for the nf-core/modules submission. `modules/nf-core/vepyr/annotate/`
mirrors the upstream path, so it copies verbatim into a nf-core/modules fork:

```bash
cp -R modules/nf-core/vepyr /path/to/modules-fork/modules/nf-core/
```

`.nf-core.yml` and `tests/config/nf-test.config` exist only so `nf-core modules
lint` treats this directory as a modules repository. nf-core/modules has its own
copies — do not copy these upstream.

## Testing

The module ships Seqera Wave containers for `linux/amd64` and `linux/arm64`,
built by `nf-core modules containers create vepyr/annotate` from
`environment.yml` and listed under `containers:` in `meta.yml`. vepyr comes from
its PyPI wheel inside them until bioconda ships 0.7.0 for linux-64 and
linux-aarch64 ([bioconda-recipes#69191](https://github.com/bioconda/bioconda-recipes/pull/69191));
then `environment.yml` switches to `bioconda::vepyr` and the containers are
rebuilt.

`main.nf` names only the amd64 image. `dev/` holds a runner that tests with the
image for the host's own platform, plus an offline VEP parity test; it is not
part of the nf-core submission.

### Shared setup

Needed on every host:

- **Docker** — Docker Desktop on macOS; Docker Engine on Linux, with your user
  allowed to run `docker` without `sudo`.
- **Nextflow** (>= 24.10.2) and **Java 11+**.
- **uv** and the project environment (`uv sync` at the repository root):
  `stage-testdata.sh` uses it to build and verify the test data (it also needs
  samtools, bgzip and tabix on `PATH`).
- **Git LFS fixtures** — `git lfs pull` at the repository root, so the Parquet
  caches and the chr22 FASTA are real files rather than pointers.
- **nf-test** — no Homebrew formula exists. The installer writes an `nf-test`
  launcher into the *current directory* (and the jar into `~/.nf-test/`), so run
  it from a directory on your `PATH`, not from the repository:

  ```bash
  mkdir -p ~/.local/bin && cd ~/.local/bin
  curl -fsSL https://get.nf-test.com | bash
  cd - && nf-test version
  ```

  If `~/.local/bin` is not on `PATH`, add `export PATH="$HOME/.local/bin:$PATH"`
  to `~/.zshrc` or `~/.bashrc`.

### Running the tests

Same command on every host:

```bash
cd nf-core-module
./dev/nf-test-local.sh
```

The runner picks the host's platform, reads that platform's Docker image from
`meta.yml`, fetches the UNTAR module the golden test needs, stages the golden
fixture into `.testdata/` and runs both test files with `dev/nf-test.config`.
Extra arguments go to `nf-test`.

| Host | Platform picked | Image (from `meta.yml`) |
|---|---|---|
| Apple Silicon (macOS) | `linux/arm64` | `containers.docker.linux/arm64` |
| Linux aarch64 | `linux/arm64` | `containers.docker.linux/arm64` |
| Linux x86_64 | `linux/amd64` | `containers.docker.linux/amd64` (the one `main.nf` names) |

Always test on the host's native platform. On Apple Silicon the amd64 image dies
with SIGILL under emulation — Docker's emulated guest has no AVX — which says
nothing about the module or the package.

Overrides:

| Variable | Default | Use |
|---|---|---|
| `NF_TEST` | `nf-test` on `PATH` | a different nf-test, e.g. `NF_TEST="$(which nf-test)"` |
| `VEPYR_DOCKER_PLATFORM` | from `uname -m` | force `linux/arm64` or `linux/amd64` |
| `VEPYR_CONTAINER` | the `meta.yml` image for the platform | test another image, e.g. one built from an unreleased engine |

### What runs

- `modules/nf-core/vepyr/annotate/tests/main.nf.test` — the submission tests:
  1,000 HG002 chr22 records against a release-116 cache with `--everything`,
  and a stub.
- `dev/tests/hg002_chr22.nf.test` — offline Ensembl VEP parity. All 50,861
  normalized HG002 chr22 records annotated with `--everything` must reproduce
  the record-body md5 of VEP 116 (`f0a0a7021c498d2b4e38c9caf5959f77`). It prints
  the task work dir, the input, cache, FASTA and output paths, and the exact
  `vepyr annotate` command. It reads `tests/data/hg002_chr22/` (~30 MB, Parquet
  and FASTA in LFS): a cache trimmed to the rows that run reads, plus a bgzip
  FASTA passed with `[ fai, gzi ]` in the index slot. Rebuild it from the
  repository root with `uv run python tests/data/hg002_chr22/prepare.py`.

The golden snapshot (`main.nf.test.snap`) waits for the fixture on
nf-core/test-datasets — see step 3 below — so do not commit one produced from
the local `.testdata/`.

## Linting

```bash
uvx --from nf-core nf-core modules lint vepyr/annotate
```

Requires `nextflow` on `PATH`. Current result: **72 passed, 0 warnings, 1 failed**.
The failure is an upstream blocker rather than a defect, and clears once the
steps below are done:

- `test_snapshot_exists` — `main.nf.test.snap` can only be produced by a real run
  against the published test data (step 4).

Treat any *second* failure as a genuine regression.

## Remaining work, in order

1. **~~Get vepyr onto bioconda.~~** Done: 0.6.0 merged in
   [bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869)
   (linux-64, osx-64, osx-arm64 only).
2. **~~Resolve the container URIs.~~** Done with vepyr from PyPI:
   `nf-core modules containers create vepyr/annotate` built Docker and
   Singularity images for linux/amd64 and linux/arm64, wrote them and the conda
   lock files into `meta.yml`, and set the amd64 URIs in `main.nf`.

   When [bioconda-recipes#69191](https://github.com/bioconda/bioconda-recipes/pull/69191)
   ships 0.7.0 for linux-64 and linux-aarch64, replace the `pip:` entry in
   `environment.yml` with `bioconda::vepyr=0.7.0` and rerun that command. (That PR
   supersedes the BiocondaBot autobump #69182, which does not add linux-aarch64;
   its ARM job passes build and tests but hits CircleCI's one-hour limit, because
   the recipe builds every Python version although vepyr is abi3.)
3. **PR the test data** ([nf-core/test-datasets#2270](https://github.com/nf-core/test-datasets/pull/2270)).
   `./stage-testdata.sh <dir>` builds it from the offline chr22 fixture in
   `tests/data/hg002_chr22` and verifies it against Ensembl VEP 116: 1,000
   normalized HG002 chr22 records from chr22:20572272-21735973 (chosen for
   coding, SIFT, motif and regulatory annotations) with their `.tbi`, a bgzip
   GRCh38 `22:1-21745973` FASTA with `.fai` and `.gzi`, and `cache.tar.gz`, a
   release-116 `ensembl` cache trimmed to the rows those records read. Unlike `ensemblvep/vep`
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
4. **Generate the snapshot.** With 2 and 3 done:
   `nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test --update-snapshot`
5. **Open the nf-core/modules PR.**

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
