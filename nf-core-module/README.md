# nf-core module `vepyr/annotate` and subworkflow `vcf_annotate_vepyr`

Staging area for the nf-core/modules submission. `modules/nf-core/vepyr/annotate/`
and `subworkflows/nf-core/vcf_annotate_vepyr/` mirror the upstream paths, so they
copy verbatim into a nf-core/modules fork:

```bash
cp -R modules/nf-core/vepyr /path/to/modules-fork/modules/nf-core/
cp -R subworkflows/nf-core/vcf_annotate_vepyr /path/to/modules-fork/subworkflows/nf-core/
```

The subworkflow runs nf-core's existing `bcftools/norm` module before
`VEPYR_ANNOTATE` when `val_normalize` is true (pipelines should default it to
true), so a raw VCF goes through end to end. The module itself stays a single
tool. `BCFTOOLS_NORM` is configured through its `ext.args`; the configuration
validated against Ensembl VEP splits multiallelic records without left-aligning:

```groovy
withName: 'BCFTOOLS_NORM' {
    ext.args   = '--multiallelics -both --do-not-normalize --output-type z --write-index=tbi'
    ext.prefix = { "${meta.id}.norm" }
}
```

`--do-not-normalize` matters: `bcftools/norm` always passes `--fasta-ref`, and
without the flag bcftools also left-aligns indels, which the parity inputs never
were. It also fails outright when the FASTA and VCF name contigs differently
(`22` vs `chr22`), a mismatch vepyr itself tolerates. Keep the `ext.prefix`
whenever `meta.id` can equal the input VCF's basename: `bcftools/norm` writes
`${meta.id}.vcf.gz` over its own staged input, a symlink, and so truncates the
original file. `VEPYR_ANNOTATE` stages its input under `input/`, so the
normalized file reaching it under its own output name is harmless; the
subworkflow tests leave the prefix unset to cover exactly that.

`.nf-core.yml` and `tests/config/nf-test.config` exist only so `nf-core modules
lint` treats this directory as a modules repository. nf-core/modules has its own
copies — do not copy these upstream.

## Testing

The module ships Seqera Wave containers for `linux/amd64` and `linux/arm64`,
built by `nf-core modules containers create vepyr/annotate` from
`environment.yml` (`bioconda::vepyr` and `bioconda::htslib`) and listed under
`containers:` in `meta.yml`.

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
`meta.yml`, fetches the pinned upstream UNTAR and `bcftools/norm` modules the
tests need, stages the test data into `.testdata/` with `stage-testdata.sh`, and
runs all four test files with `dev/nf-test.config`. `bcftools/norm`'s image is
linux/amd64 only; `dev/local.config` runs it under emulation, which works
because bcftools, unlike vepyr, needs no AVX.
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
- `subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test` — the subworkflow
  submission tests on the same data: with normalization, without it, and a stub.
  The published input has no multiallelic records, so both annotated outputs are
  identical; these tests prove the wiring, the parity test below the effect.
- `dev/tests/hg002_chr22_normalize.nf.test` — end-to-end parity through the
  subworkflow: the raw HG002 chr22 benchmark records (`raw_chr22.vcf.gz`, 50,284
  records, 577 multiallelic, chr22 of the v4.2.1 benchmark VCF, unmodified) go
  through `BCFTOOLS_NORM` and `VEPYR_ANNOTATE` and must reproduce the same VEP 116
  md5 on the resulting 50,861 records. The split-only normalization reproduces
  `input_chr22.vcf.gz` byte for byte in the record body.
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

### Running nf-test directly

`dev/nf-test-local.sh` sets everything up; to run `nf-test` yourself — say,
against published test data instead of `.testdata/` — set the variables
`dev/nf-test.config` reads and run from `nf-core-module/`:

```bash
cd nf-core-module

# Module tests, test data from the nf-core/test-datasets#2270 branch (Apple Silicon)
VEPYR_DOCKER_PLATFORM=linux/arm64 \
VEPYR_CONTAINER=community.wave.seqera.io/library/htslib_vepyr:806fe605983a885b \
VEPYR_NF_TESTDATA=https://raw.githubusercontent.com/mwiewior/test-datasets/vepyr-annotate/data/ \
nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test --config dev/nf-test.config

# Offline VEP parity test
VEPYR_DOCKER_PLATFORM=linux/arm64 \
VEPYR_CONTAINER=community.wave.seqera.io/library/htslib_vepyr:806fe605983a885b \
VEPYR_HG002_CHR22="$PWD/../tests/data/hg002_chr22" \
nf-test test dev/tests/hg002_chr22.nf.test --config dev/nf-test.config
```

| Variable | Read by | Value |
|---|---|---|
| `VEPYR_DOCKER_PLATFORM` | `dev/local.config` | `linux/arm64` (Apple Silicon, Linux aarch64) or `linux/amd64` (Linux x86_64) |
| `VEPYR_CONTAINER` | `dev/local.config` | required: the `containers.docker` image for that platform from `meta.yml` — `…:806fe605983a885b` (arm64) or `…:84d01ceaf76003ed` (amd64) |
| `VEPYR_NF_TESTDATA` | `dev/local.config` | base of the module test data, with a trailing slash. Unset: nf-core's `https://raw.githubusercontent.com/nf-core/test-datasets/modules/data/`. The runner points it at `.testdata/data/`; the example above at the #2270 branch. |
| `VEPYR_HG002_CHR22` | `dev/local.config` | absolute path to `tests/data/hg002_chr22`; the parity test only |

The test appends file paths to `VEPYR_NF_TESTDATA`, so the base URL itself is
never fetched — opening it in a browser gives 404, because
raw.githubusercontent.com serves files, not directories.

The module test needs `modules/nf-core/untar/`; `dev/nf-test-local.sh` fetches
it on first run.

**Snapshots.** Each run of `main.nf.test` compares against, or creates,
`modules/nf-core/vepyr/annotate/tests/main.nf.test.snap`. A leftover file from
an earlier run — for instance one where a task failed and nf-test recorded empty
outputs — fails the next run with `Different Snapshot`. Delete it, or pass
`--update-snapshot` to rewrite it. Commit one only when it comes from nf-core's
published URLs (`VEPYR_NF_TESTDATA` unset), after #2270 merges.

## Linting

```bash
uvx --from nf-core nf-core modules lint vepyr/annotate
uvx --from nf-core nf-core subworkflows lint vcf_annotate_vepyr
```

Requires `nextflow` on `PATH`. Current results: module **74 passed, 0 warnings,
1 failed**; subworkflow **20 passed, 0 warnings, 1 failed**. Both failures are the
same upstream blocker rather than a defect, and clear once the steps below are
done:

- `test_snapshot_exists` — `main.nf.test.snap` can only be produced by a real run
  against the published test data (step 4).

Treat any *second* failure as a genuine regression.

## Remaining work, in order

1. **~~Get vepyr onto bioconda.~~** Done: 0.7.0 merged in
   [bioconda-recipes#69191](https://github.com/bioconda/bioconda-recipes/pull/69191)
   for linux-64, linux-aarch64, osx-64 and osx-arm64, as a single abi3 build
   (`python >=3.10`).
2. **~~Resolve the container URIs.~~** Done: `nf-core modules containers create
   vepyr/annotate` built Docker and Singularity images of `bioconda::vepyr=0.7.0`
   for linux/amd64 and linux/arm64, wrote them and the conda lock files into
   `meta.yml`, and set the amd64 URIs in `main.nf`. Rerun it whenever
   `environment.yml` changes.
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
4. **Generate the snapshots.** With 2 and 3 done, against nf-core's published
   URLs (`VEPYR_NF_TESTDATA` unset):
   `nf-test test modules/nf-core/vepyr/annotate/tests/main.nf.test subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test --update-snapshot`
5. **Open the nf-core/modules PR for the module** (`vepyr/annotate`).
6. **Open a second PR for the subworkflow** (`vcf_annotate_vepyr`) once the module
   is merged: nf-core reviews subworkflows separately, and the subworkflow's
   `components` must already exist upstream (`bcftools/norm` does).

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
