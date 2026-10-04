# Polars integration benchmarks (paper supplement)

These are the benchmarks behind the vepyr paper's supplementary material on the Polars
integration. Each one pits vepyr's LazyFrame and output paths against Ensembl VEP plus
`filter_vep` for the same filtering queries, on HG002 chr22 (50,861 records), Ensembl
release 116. The design is in
`docs/superpowers/specs/2026-09-24-polars-supplement-benchmarks-design.md`.

| Experiment | Question | Script | Result dir |
|---|---|---|---|
| VEP matrix | How long does VEP 116 take to annotate chr22, at `--fork` 0/1/3/7, core and with plugins? | `scripts/run_vep.py` | `vep/` |
| A | Filtering only: `filter_vep` against Polars over the same VEP-annotated VCF | `scripts/run_filter_only.py` | `A/` |
| B | End to end: VEP `--fork N` + `filter_vep` against vepyr `workers=N+1` → filter → one output (collect, VCF, Parquet) | `scripts/run_e2e.py` | `B/` |
| B_sliced | Region queries done the way a VEP user would: slice the window with bcftools, annotate the slice, filter | `scripts/run_vep_sliced.py` | `B_sliced/` |
| C | What pushdown saves: region pushdown on vs off, and a full frame vs a narrow `select()` | `scripts/run_pushdown.py` | `C/` |
| Parity gate | Does every query return `filter_vep`'s records, on every path, before anything is timed? | `scripts/verify.py` | `verify.json` |

`scripts/make_supplement.py` turns a run directory into CSVs (`csv/`), LaTeX tables and PDF
figures. The query catalogue (18 queries: Q*, R1–R3 regions, P1–P5 plugins) is
`pibench/queries.py`. It mirrors `filter_vep` semantics, and `tests/` pins them.

## Layout

```
pibench/      paths.py (every path and constant, env-overridable), queries.py (the catalogue),
              measure.py (one subprocess per run, peak RSS, load guard), parity.py, csq.py
scripts/      one script per experiment, plus *_once.py / filter_vep_once.sh (one timed run each)
panels/       the ACMG SF v3.2 gene panel and its chr22 BED (the panel query)
tests/        unit tests for the harness: `pytest performance-tests/polars-integration/tests`
outputs/116/<host>_chr22_<date>[_suffix]/   one run: results, csv/, build.json, env.json
```

## Prerequisites

- A vepyr build as measured: from the repo root, `env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`. That is a release build with native CPU instructions; a `maturin develop` build is a different binary and not comparable.
- Docker with `ensemblorg/ensembl-vep:release_116.0`, which runs VEP and `filter_vep`.
- `bcftools`, `tabix` and `bgzip` on `PATH`.
- Under `$DATA_VEPYR_DIR` (default `~/workspace/data_vepyr`):
  - `input/HG002_normalized.vcf.gz` and `input/Homo_sapiens.GRCh38.dna.primary_assembly.fa` (+ `.fai`);
  - vepyr caches `cache/116_GRCh38_{ensembl,merged}`;
  - VEP caches `homo_sapiens_{ensembl,merged}/116_GRCh38`;
  - plugin caches `plugin_cache_116`, and the VEP plugin sources and code (`output/116/plugins/plugin_code`).

  The VEP container mounts only this tree, so every path passed to VEP or `filter_vep` must
  resolve under it; `filter_vep_once.sh` refuses anything else.

| Variable | Default | Meaning |
|---|---|---|
| `DATA_VEPYR_DIR` | `~/workspace/data_vepyr` | Root of all inputs, caches and the work dir |
| `PIBENCH_WORK` | `$DATA_VEPYR_DIR/polars_integration/chr22` | chr22 input, VEP outputs, plugin slices, scratch |
| `VEP_IMAGE` | `ensemblorg/ensembl-vep:release_116.0` | VEP / `filter_vep` container |
| `PIBENCH_MAX_LOAD` | `2.0` | Load guard: wait before each run until the 1-minute load average is at or below this. The published runs used **4**; see *Measurement* |

## Reproduction

Run from `performance-tests/polars-integration`. The `--run-dir` paths are relative to this
directory.

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr PIBENCH_MAX_LOAD=4
RUN=outputs/116/$(hostname -s)_chr22_$(date +%Y%m%d)
PY=../../.venv/bin/python        # the native release build above

bash scripts/prepare_chr22.sh                          # chr22 input under $PIBENCH_WORK (once)
$PY scripts/run_vep.py --run-dir $RUN                  # VEP matrix: core + plugins x fork 0/1/3/7 (hours)
$PY scripts/verify.py --run-dir $RUN                   # parity gate; stop here if any query fails
$PY scripts/run_filter_only.py --run-dir $RUN          # A
$PY scripts/run_pushdown.py --run-dir $RUN             # C  (--workers 1 8 by default)
$PY scripts/run_e2e.py --run-dir $RUN                  # B  (re-times filter_vep: ~6 h)
$PY scripts/run_vep_sliced.py --run-dir $RUN           # B_sliced
$PY scripts/make_supplement.py --run-dir $RUN --dest ~/research/git/papers/vepyr/supplementary
```

`verify.py` must pass before timing: B and C only time the paths a query passed on.

To re-time **only vepyr** after a vepyr or engine change, reuse the VEP side of an earlier
run on the same host. Copy `vep/`, `A/` and `B_sliced/` from that run, run `verify.py` and
`run_pushdown.py` again, and then:

```bash
$PY scripts/run_e2e.py --run-dir $RUN --reuse-filter-vep outputs/116/<earlier run>
```

This copies each query's VEP + `filter_vep` row instead of re-timing it (about 1 h instead
of about 6.5 h). It is only valid while the VEP outputs, `filter_vep` and the host are
unchanged, so say so in the run's `build.json`. `make_supplement.py`'s plugin-fix table also
needs `C_old_build_pre_fix/` and `verify_old_build.json` copied across.

## Measurement

- Every configuration runs once as a discarded warm-up, then three times. Tables report the median.
- Each run is its own subprocess, so peak RSS belongs to that run alone.
- VEP `--fork N` runs N+1 processes, so it is paired with vepyr `workers=N+1`: fork 0/1/3/7 against workers 1/2/4/8.
- Queries without plugins use the Ensembl cache. P1–P5 use the merged cache with SpliceAI, CADD, AlphaMissense, dbNSFP and ClinVar.
- **The host is shared.** Other projects' Docker containers start on their own and add load. `PIBENCH_MAX_LOAD=4` was the working compromise on a 16-core M3 Max, because the default 2.0 never clears with them running. Check `uptime` and `docker ps` before a run, and discard numbers taken under load rather than explaining them away.

## Parity (every published run)

All 18 queries match `filter_vep` over serial VEP. For each query:
- **A:** Polars returns the same record keys as `filter_vep`.
- **B_vcf:** vepyr's `pb.sink_vcf` output matches line for line at workers 1, 2, 4 and 8.
- **B_collect and B_parquet:** the same record keys at workers 1 and 8.
- **C:** turning pushdown off and using a narrow `select()` each give the same keys.

One difference is reported but not gated: polars-bio decodes the `%3D` in HGVSp to `=`, where
`filter_vep` keeps it percent-encoded.

## Deviations and caveats

- **Region queries have two VEP baselines.** In `B/` VEP annotates the whole chromosome and then filters; that is the naive workflow and gives about 1,100× for R1–R3 at one process. `B_sliced/` slices the window first, which is the fair comparison: 23–52× on R1, 1.8–5.8× on R2 and R3. The supplement labels both.
- **VEP `--fork 7` with plugins** drifts from serial VEP output on 93 lines. The other fork counts match serial VEP.
- **`C_old_build_pre_fix/` is partial.** The run before the plugin fixes was stopped mid-C to debug the plugin slowdown. It covers P1 and one P2 configuration; the plugin-fix table compares only those.

## Runs

| Run | vepyr / engine | Notes |
|---|---|---|
| `…_chr22_20260924` | vepyr #131 at `9501251`, engine #261 at `cd38a19` through a local Cargo `[patch]` | The first full run on a pre-release build with both plugin fixes. The plugin curve is flat from 4 to 8 workers (4.0 s at both). |
| `…_chr22_20260926_master` | vepyr master `9e37d43`, engine `v0.23.0` (#261 + #262) | Parity and C re-run; B re-timed vepyr only (`--reuse-filter-vep` from 20260924); A, B_sliced and `vep/` carried over. Plugin queries at 8 processes: 4.07 → 3.31 s median (63× → 76× vs VEP + `filter_vep`). The core groups are unchanged, as expected for the Ensembl cache. |

The engine-side cause of the old plugin plateau, and its fix, are in
`docs/superpowers/specs/2026-09-26-lf-run-pool-scaling-design.md`.
