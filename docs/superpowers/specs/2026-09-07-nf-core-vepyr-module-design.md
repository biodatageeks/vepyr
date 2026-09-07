# nf-core module for vepyr — design

**Date:** 2026-09-07
**Status:** approved, not yet implemented
**Goal:** ship `vepyr/annotate` to nf-core/modules, wrapping a new minimal `vepyr` CLI.

## Context

vepyr's public surface is `vepyr.annotate()` / `vepyr.build_cache()`. nf-core modules
invoke a shell command, and `pip install vepyr` puts no executable on `$PATH`. The
module therefore requires a CLI, which does not exist yet.

The bioconda recipe is in flight as
[bioconda-recipes#68869](https://github.com/bioconda/bioconda-recipes/pull/68869) at
version 0.5.0 — a version with no CLI, and so useless to the module.

Reference module: `modules/nf-core/ensemblvep/vep` in nf-core/modules.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| How the module invokes vepyr | A real `vepyr` console script | `template`/heredoc Python makes `ext.args` pass-through awkward and draws review pushback. A CLI also supplies `vepyr --version` for the versions topic. |
| CLI scope | `annotate` only | `build_cache` is an operator step, not a pipeline step. Leaves `vepyr/buildcache` as a later module. |
| Flag vocabulary | Ensembl VEP's spelling, underscores | Matches the project's drop-in-parity thesis; `ext.args` written for `ensemblvep/vep` transfers as the flag set grows. |
| Flag count | Minimal — 10 | Only flags that are already proven. See "Flag selection" below. |
| Plugins | In v1 | Zero engine work, best-tested option in the suite, and adding a module input later is a breaking change in nf-core. |
| Output | bgzf VCF + tabix index | Matches `ensemblvep/vep`'s contract. |
| Test fixtures | PR to nf-core/test-datasets | Convention; the existing golden fixture is already the right scale. |

### Flag selection

No flag needs engine work: every kwarg is folded into a single `opts` JSON dict
(`src/vepyr/__init__.py:1365-1470`) handed to the engine, and the `output_vcf` path
applies that same dict with no VCF-specific gating. The discriminator is evidence,
not feasibility.

- **Parity-validated against real Ensembl VEP:** `everything` + `reference_fasta`
  only. `tests/_golden_suite.py:274,288` runs exactly that configuration on both the
  DataFrame and VCF paths, and the `e2e-testing/` harness matches.
- **Unit-tested:** `plugin_cache_root`/`plugins`, `workers`, `fields`, `af`,
  `buffer_size`, `hgvs`, `expected_cache_version`.
- **One mapping test, no parity coverage:** the pick family, `shift_hgvs`,
  `pubmed`, `pick_order`.

v1 ships the parity-validated configuration plus the options the module itself
needs, and nothing else.

## 1. CLI

New `src/vepyr/cli.py` exposing `main()`, plus `src/vepyr/__main__.py` delegating to
it so `python -m vepyr` works. Wired as `[project.scripts] vepyr = "vepyr.cli:main"`.
Parsing uses stdlib `argparse` — no new runtime dependency, which keeps the bioconda
recipe's `run:` list unchanged.

The CLI is VCF-in / VCF-out only: a thin shell over `annotate(..., output_vcf=...)`.
The LazyFrame path stays Python-only, so `skip_csq`, `fields`-as-columns,
`on_batch_written` and region pushdown are deliberately unexposed.

```
vepyr annotate -i IN.vcf.gz -o OUT.vcf.gz --dir_cache CACHE
               [--fasta REF] [--everything]
               [--fork N] [--cache_version N]
               [--plugin_cache_root PATH] [--plugin NAME ...]
               [--no_progress]
vepyr --version
```

| Flag | `annotate()` kwarg | Notes |
|---|---|---|
| `-i`, `--input_file` | `vcf` | required |
| `-o`, `--output_file` | `output_vcf` | required |
| `--dir_cache` | `cache_dir` | required |
| `--fasta` | `reference_fasta` | required by `--everything` |
| `--everything` | `everything=True` | |
| `--fork N` (alias `--workers`) | `workers` | `>1` requires an indexed input |
| `--cache_version N` | `expected_cache_version` | int on the CLI, str to the API |
| `--plugin_cache_root PATH` | `plugin_cache_root` | |
| `--plugin NAME` | `plugins` | repeatable; order is CSQ block order |
| `--no_progress` | `show_progress=False` | keeps tqdm out of `.command.err` |

Deliberately omitted: `--compress_output` (compression is auto-detected from the
`.gz` extension, so the module gets bgzf for free).

**Unknown flags hard-error.** An `ext.args` string carrying `--pick` or `--no_stats`
copied from an `ensemblvep/vep` config will fail the task rather than be ignored;
silently dropping an unrecognised flag would yield quietly wrong annotations.

**Errors.** `ValueError` and `FileNotFoundError` from the API are caught in `main()`
and become exit code 2 with the message on stderr — no traceback.

**Version.** `vepyr --version` prints `vepyr X.Y.Z` (argparse convention), read from
`importlib.metadata`.

## 2. nf-core module

Path `modules/nf-core/vepyr/annotate/`, process `VEPYR_ANNOTATE`. The `annotate`
subdirectory (rather than a bare `vepyr/`) leaves room for `vepyr/buildcache`
without a rename.

```nextflow
process VEPYR_ANNOTATE {
    tag "${meta.id}"
    label 'process_medium'

    input:
    tuple val(meta),  path(vcf), path(tbi)   // tbi optional
    tuple val(meta2), path(cache)            // Parquet cache directory
    tuple val(meta3), path(fasta)            // optional; required by --everything
    val   cache_version                      // optional
    tuple val(meta4), path(plugin_cache)     // optional

    output:
    tuple val(meta), path("${prefix}.vcf.gz"),     emit: vcf
    tuple val(meta), path("${prefix}.vcf.gz.tbi"), emit: tbi
    tuple val("${task.process}"), val('vepyr'), eval("vepyr --version | cut -d' ' -f2"), topic: versions
    tuple val("${task.process}"), val('tabix'), eval("tabix -h 2>&1 | grep -oP 'Version:\s*\K[^\s]+'"), topic: versions
}
```

Two behaviours worth stating explicitly:

- **`--fork` is gated on the index.** `workers > 1` raises unless the input carries a
  `.tbi`/`.csi` (`_require_index_for_workers`, `src/vepyr/__init__.py:1026`). The
  script emits `--fork ${task.cpus}` only when `tbi` is present and `--fork 1`
  otherwise, so a config mistake costs speed rather than crashing the run.
- **`tabix` runs after annotation.** `annotate()` writes bgzf but no index.

`environment.yml` therefore carries two packages, as `ensemblvep/vep` does:

```yaml
channels: [conda-forge, bioconda]
dependencies:
  - bioconda::vepyr=0.6.0
  - bioconda::htslib=1.23.1
```

The `container` line stays a documented placeholder until the conda package exists;
the Seqera Wave URI is generated from `environment.yml` at that point.

A `stub:` block emits an empty `.vcf.gz` and `.tbi` so the module is runnable before
the container lands.

## 3. Test fixtures

`tests/data/golden/` already holds a self-contained fixture at nf-core scale: a
5.1 MB chr1 Parquet cache (all seven entities plus `chrom_manifest.json`), an 875 KB
reference FASTA, and a 100-variant VCF with a `.tbi`.

These go to nf-core/test-datasets (`modules` branch) under
`genomics/homo_sapiens/vepyr/`, and the tests address them through
`params.modules_testdata_base_path`. Unlike `ensemblvep/vep` — whose tests are
effectively stubs because its cache is too large to host — this module can assert on
real annotation output.

The plugin path has no fixture; the v1 nf-test passes an empty plugin-cache channel
and plugin coverage stays in pytest.

## 4. Testing

- `tests/test_cli.py`: flag→kwarg mapping against a monkeypatched `annotate`
  (covering every row of the table above, the `--fork`/`--workers` alias, repeated
  `--plugin`, and `--cache_version` int→str), plus unknown-flag and missing-required
  failures, exit codes, and `--version`.
- One end-to-end CLI run over `tests/data/golden/` asserting the output VCF's CSQ
  against the existing golden expectations — the same assertion the API-level golden
  suite makes, reached through `subprocess`.
- `main.nf.test` is written now with the stub test runnable immediately; the real
  test and its `.snap` wait on the container.

## 5. Delivery sequence

| # | Step | Blocked by |
|---|---|---|
| 1 | CLI + tests + docs, release 0.6.0 | — |
| 2 | Bump #68869 to 0.6.0, add a `vepyr --version` test command | 1 |
| 3 | Resolve the Wave/biocontainer URI | 2 |
| 4 | nf-core/test-datasets PR | — (can run parallel to 1) |
| 5 | `nf-test` run → `main.nf.test.snap` | 3, 4 |
| 6 | nf-core/modules PR | 5 |

Bumping #68869 rather than filing a follow-up recipe PR avoids ever publishing a
container without a `vepyr` executable.

Steps 2, 4 and 6 are outward-facing and are not taken without explicit approval.

## 6. Repository layout

```
src/vepyr/cli.py                 new
src/vepyr/__main__.py            new
pyproject.toml                   + [project.scripts]; version 0.6.0
tests/test_cli.py                new
docs/cli.md, mkdocs.yml          new page — meta.yml's documentation URL must resolve
bioconda-recipe/meta.yaml        bump to 0.6.0, add a CLI smoke test
nf-core-module/                  .nf-core.yml, README.md, stage-testdata.sh
  modules/nf-core/vepyr/annotate/ main.nf, meta.yml, environment.yml,
                                 tests/{main.nf.test,nextflow.config}
```

`nf-core-module/` is laid out as a miniature nf-core/modules repository: the full
upstream path means `modules/nf-core/vepyr/` copies verbatim into a fork, and the
`.nf-core.yml` marker (repo-local only, never copied) lets `nf-core modules lint`
run against it. The README records the copy target and the upstream blockers.

## 7. Out of scope

`build_cache` / `build_plugin_cache` on the CLI; the LazyFrame path; `--tab`/`--json`
output (vepyr has no such writers); an HTML stats report; the pick family, HGVS
sub-flags, AF sub-flags and `--fields`, all of which are additive later and cost no
engine work.
