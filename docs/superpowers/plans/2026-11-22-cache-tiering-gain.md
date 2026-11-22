# Warm/cold tiering gain (chr22 first) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure whether the warm/cold variation-cache layout speeds up vepyr annotation or reduces CPU time, against the same records sorted by position only, on HG002 chr22.

**Architecture:** A small Rust tool rewrites a variation shard in either layout with the engine's own `VariationParquetShardWriter`; a Python helper assembles a cache directory per layout (only `variation/` differs, everything else symlinked); a Python runner times vepyr 0.11.0 on both caches with the existing `run_vepyr_worker_scaling.py`, checks output identity and writes a CSV and a summary. Page/byte counts come from the existing copied-lookup tool run on the two rewritten files.

**Tech Stack:** Rust (datafusion-bio-function-vep tag v0.27.1, its parquet/arrow versions), Python 3.12 (vepyr 0.11.0 venv for runs; repo `.venv` for helpers and tests), pytest.

**Spec:** `docs/superpowers/specs/2026-11-22-cache-tiering-gain-design.md`

## Global Constraints

- vepyr 0.11.0 PyPI wheel: `~/workspace/venvs/vepyr-0.11.0/bin/python`; `VEPYR_EXPECTED_VERSION=0.11.0`.
- Engine crate: `datafusion-bio-function-vep`, git tag `v0.27.1`, feature `cache-builder` (same pin as vepyr `Cargo.toml:36`).
- Source cache: `~/workspace/data_vepyr/cache/116_GRCh38_merged` (never modified). Input `~/workspace/data_vepyr/input/HG002_normalized.vcf.gz`, chr22 = 50,861 records; reference `~/workspace/data_vepyr/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa`.
- Layout names: `tiered` (current order, rewritten unchanged) and `sorted` (by `start`, `tier` = 1 for every row).
- Both layouts are written with identical Parquet settings: `VariationParquetShardWriter` (i.e. `variation_writer_properties`: zstd(3), dictionary off, data page 4 KiB / 512 rows, page statistics, row-group cap `VARIATION_ROW_GROUP_ROWS`, sorting columns (tier, start)) and the same `--batch-rows`. `paramcheck` (Task 1) must pass before any timing. Page counts per column may differ because the 4 KiB limit depends on content; that is reported, not equalised.
- Output VCFs are not kept: per-contig MD5 of record lines, then delete. A run whose MD5 differs from the other layout's is invalid.
- Before every timed run: at least 20 GiB free memory (restore with the `ensure_free` method of `performance-tests/vepyr/outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261121/run.zsh`) and at least 40 GiB free disk. No other work on the host during timing.
- Everything new lives under `performance-tests/cache-tiering/`. Results committed: CSV, JSON, Markdown only.
- Commit messages end with the two attribution lines of this session (Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>, Claude-Session: https://claude.ai/code/session_01W815XL29mth9pwsSzTGFNU).

## Review Focus

- The `sorted` file must be read correctly by the real engine (PageDir with a single run): pinned by the output-MD5 identity check in Task 3 and the smoke run in Task 5.
- The rewrite must keep every row, column, type and the Parquet key-value/schema metadata: pinned by Task 1 tests (`same_rows_both_layouts`, `metadata_preserved`).
- vepyr must accept a cache directory whose entries are symlinks (and may read `reference_policy.json`, `chr_synonyms.txt`): pinned by Task 2 test plus the Task 5 smoke run.
- A run that fails or whose output differs must not enter the summary: pinned by Task 3 `test_invalid_runs_excluded`.
- Low free memory or disk must stop the runner before timing, not after: pinned by Task 3 `test_refuses_when_disk_low`.

---

### Task 1: `relayout` Rust tool

**Files:**
- Create: `performance-tests/cache-tiering/relayout/Cargo.toml` (own `[workspace]`, so the root workspace is untouched), `performance-tests/cache-tiering/relayout/src/main.rs`, `performance-tests/cache-tiering/relayout/src/lib.rs`
- Test: `performance-tests/cache-tiering/relayout/tests/relayout.rs`

**Interfaces:**
- Produces: binary `paramcheck <a.parquet> <b.parquet> [<original.parquet>]` → JSON with `identical_settings: bool`, the list of differing settings (empty when identical), and per file: row groups, rows per row group, pages per column (min/median/max rows and compressed bytes per page). Exit 1 if any setting differs (page counts excluded). Binary `relayout --input <parquet> --output <parquet> --layout tiered|sorted [--batch-rows 8192]`; prints one JSON line `{"rows":N,"runs":R,"start_pages":P,"bytes":B}`. Library fn `pub fn relayout(input: &Path, output: &Path, layout: Layout, batch_rows: usize) -> Result<Summary>`; `pub enum Layout { Tiered, Sorted }`; `pub struct Summary { rows: usize, runs: usize, start_pages: usize, bytes: u64 }` (runs = maximal non-decreasing `start` runs, from the page index).

- [ ] **Step 1: Write the failing tests** on a synthetic shard written with `VariationParquetShardWriter` (two tiers, starts warm `[5,9,30]`, cold `[1,5,7,40]`):
  - `tiered_keeps_row_order`: output rows equal input rows in order; `runs == 2`.
  - `sorted_is_monotonic_single_tier`: `start` non-decreasing; every `tier == 1`; `runs == 1`; stable for equal `start` (warm row with start 5 before cold row with start 5).
  - `same_rows_both_layouts`: multiset of rows (all columns, `tier` excluded) equal between tiered and sorted outputs and the input.
  - `metadata_preserved`: Arrow schema (fields, types, nullability, schema metadata) and Parquet key-value metadata of output equal the input's; declared sorting columns `(tier, start)`.
  - `writer_params_identical`: for the tiered and sorted outputs of the same input, the footers agree on: number of row groups and rows per row group, per column chunk codec and zstd level, encodings, dictionary page absent, statistics present, column index and offset index present, sorting columns, `created_by`, and Arrow/Parquet key-value metadata.
- [ ] **Step 2: Run** `cargo test --manifest-path performance-tests/cache-tiering/relayout/Cargo.toml` — expect FAIL (no `relayout`).
- [ ] **Step 3: Implement `relayout`**: read the whole shard with the parquet Arrow reader (chr22 fits in memory); `Sorted` = `lexsort_to_indices` on `start` with a stable index tiebreak, `take`, replace `tier` by `Int8` 1s; write with `VariationParquetShardWriter::create(output, schema)` in slices of `batch_rows`, copying the input's key-value metadata. `Summary.start_pages` and `runs` from the output's offset/column index.
- [ ] **Step 4: Run** the tests — expect PASS. Then on the real shard: `relayout --input <cache>/variation/chr22.parquet --output /tmp/x.parquet --layout tiered` prints `rows` 15,148,238 and `runs` 2.
- [ ] **Step 5: Commit** (`perf(cache-tiering): relayout tool for tiered and position-sorted variation shards`).

### Task 2: per-layout cache directory

**Files:**
- Create: `performance-tests/cache-tiering/make_layout_cache.py`
- Test: `performance-tests/cache-tiering/tests/test_make_layout_cache.py`

**Interfaces:**
- Consumes: `relayout` binary path.
- Produces: `make_layout_cache(source: Path, dest_root: Path, layout: str, contigs: list[str], relayout_bin: Path) -> Path` returning `dest_root/<layout>/<source.name>`; CLI with the same arguments. Every top-level entry except `variation/` is a symlink to the source; `variation/` is a real directory whose files are symlinks except `<contig>.parquet` for each requested contig, which is the `relayout` output. Writes `layout.json` (`layout`, `contigs`, relayout summaries, source path) beside the cache directory, not inside it.

- [ ] **Step 1: Write failing tests** with a fake cache tree and a stub `relayout` (a script that copies input to output and prints the JSON line): `test_only_requested_contigs_rewritten`, `test_other_entries_are_symlinks`, `test_refuses_existing_dest` (no overwrite without `--force`), `test_layout_json_written`.
- [ ] **Step 2: Run** `.venv/bin/python -m pytest performance-tests/cache-tiering/tests -q` — expect FAIL.
- [ ] **Step 3: Implement.** Use `os.symlink` with absolute targets; refuse if fewer than `2 × size(source shard) + 40 GiB` free on the destination volume.
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Commit.**

### Task 3: runner and summary

**Files:**
- Create: `performance-tests/cache-tiering/run_tiering_gain.py`, `performance-tests/cache-tiering/contig_md5.py` (copy of `performance-tests/paper-0.11.0/engines-two-modes-20261120/scripts/contig_md5.py` if present on the branch, else the same function: MD5 of record lines per contig)
- Test: `performance-tests/cache-tiering/tests/test_run_tiering_gain.py`

**Interfaces:**
- Consumes: the two cache directories from Task 2; `performance-tests/vepyr/scripts/run_vepyr_worker_scaling.py` (one call per run, `--workers <n> --compression plain --force`, reads its `*.metrics.json` and `*.time.txt`).
- Produces: `schedule(layouts: list[str], workers: list[int], repeats: int) -> list[tuple[str,int,str]]` (layout, workers, label: `warmup` or `r1..rN`; per worker count: one warm-up per layout, then repeats in ABBA order); `summarize(rows: list[dict]) -> list[dict]` (per layout × workers: n, median and min/max of wall, user, system, CPU = user+system, peak RSS; per workers count `ratio = median(sorted) / median(tiered)` for wall, CPU and RSS, > 1 means tiering helps); CLI writes `runs.csv` (one row per run: layout, workers, label, wall_s, user_s, sys_s, max_rss_kib, annotation_s, md5_ok, valid) and `summary.json`/`summary.md`. `--purge-run` adds one run per layout after `sudo purge` (skipped with a note if `purge` is unavailable).

- [ ] **Step 1: Write failing tests:** `test_schedule_abba` (workers [1] repeats 2 → `tiered warmup, sorted warmup, tiered r1, sorted r1, sorted r2, tiered r2`), `test_summary_medians_and_ratio`, `test_invalid_runs_excluded` (a row with `md5_ok=False` or nonzero exit is in `runs.csv` but not in the summary), `test_refuses_when_disk_low` (patched `shutil.disk_usage` → exits before any run).
- [ ] **Step 2: Run** the tests — expect FAIL.
- [ ] **Step 3: Implement.** Free memory via `vm_stat` (macOS), restore by allocating as in `ensure_free`; per run: call the scaling script with the vepyr 0.11.0 python, then `contig_md5.py`, compare with the first valid run's MD5 for that worker count (either layout), delete the VCF. Set `VEP_PROFILE=1` only in a separate optional `--profile` pass (it adds output; never in timed runs).
- [ ] **Step 4: Run** the tests — expect PASS.
- [ ] **Step 5: Commit.**

### Task 4: page and byte counts

**Files:**
- Create: `performance-tests/cache-tiering/pages/` (copy of `performance-tests/paper-0.11.0/cache-pages-20261121/tool` from PR #292's branch, unchanged), `performance-tests/cache-tiering/pages_summary.py`
- Test: `performance-tests/cache-tiering/tests/test_pages_summary.py`

**Interfaces:**
- Consumes: the two rewritten chr22 shards from Task 2.
- Produces: per-buffer text output of the tool for each layout (extended probes, as vepyr runs) and `pages_summary.py <tiered.txt> <sorted.txt>` → `pages.csv` (buffer, layout, phase-2 pages decoded, phase-2 pages fetched, phase-2 bytes and ops, pages with matches, phase-3 pages decoded, phase-3 bytes and ops) and the per-metric reduction range over full buffers.

- [ ] **Step 1: Write a failing test** for `pages_summary.py` on two fixture files cut from `chr22_ext.txt` / `sorted_ext.txt` of `cache-pages-20261121` (buffer 0: pages with matches 315 vs 907 → reduction 0.653).
- [ ] **Step 2–4:** implement the parser, run tests — PASS; run the tool on both rewritten shards.
- [ ] **Step 5: Commit.**

### Task 5: chr22 run and results

**Files:**
- Create: `performance-tests/cache-tiering/outputs/chr22_<date>/` (`README.md`, `runs.csv`, `summary.json`, `summary.md`, `pages.csv`, `layout-*.json`, `paramcheck.json`, `env.json`)

- [ ] **Step 1:** Build both caches (Task 2) under `~/workspace/data_vepyr/cache/tiering-gain/` for contig `chr22`; check both `layout.json`: equal `rows`, `runs` 2 vs 1; run `paramcheck tiered/…/chr22.parquet sorted/…/chr22.parquet <original chr22.parquet>` → `identical_settings: true` for the two rewritten files (save as `paramcheck.json` in the results; also record how the original differs, if at all). Stop if false.
- [ ] **Step 2: Smoke:** one 1-worker run per layout; expect MD5 equal to each other and to the vepyr 0.11.0 chr22 output that matches VEP (the parity reports in PR #292). Stop if not.
- [ ] **Step 3: Timed runs:** workers 1 and 8, 5 repeats, ABBA, plus `--purge-run`; nothing else running on the host.
- [ ] **Step 4:** Task 4 page counts on the rewritten shards; write `README.md` with environment, protocol, the summary table (wall, CPU, peak RSS per layout and workers, ratio), page metrics, and a one-paragraph verdict on whether tiering helps on chr22.
- [ ] **Step 5: Commit** results and push the branch; do not open a PR unless asked.
