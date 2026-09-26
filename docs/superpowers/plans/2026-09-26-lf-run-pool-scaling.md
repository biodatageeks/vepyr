# LazyFrame Run-Pool Scaling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `vepyr.annotate(...)` LazyFrame `collect()` scale with `workers` like `output_vcf`. The fix is in the engine's streaming run pool: the output budget scales with the worker count, and on small Merged/RefSeq contigs the minimum run length gives way so every worker gets a run.

**Architecture:** Two scheduling-only changes to `datafusion/bio-function-vep/src/annotate_provider.rs` in `biodatageeks/datafusion-bio-functions`, branched from engine master. vepyr gets a pin bump, one parity test that asserts the new run plan, and a committed benchmark script that measures the raw engine stream, LF `collect()` and `output_vcf` in one subprocess per run. Delivery follows `docs/runbooks/applying-a-vepyr-fix.md` (steps 2 and 4–9).

**Tech Stack:** Rust (tokio, DataFusion 53), PyO3/maturin, Python 3.12, Polars, pytest, uv, gh.

**Spec:** `docs/superpowers/specs/2026-09-26-lf-run-pool-scaling-design.md` (commit d049415). Read it before starting. The evidence table explains every number used below.

## Global Constraints

- Budget default: `max(1024, 512 × workers)` MiB; a positive `VEP_STREAM_BUFFER_MB` replaces it outright; zero or garbage is ignored.
- Stateful floor: `min(4, ceil(buffers / workers)).max(1)` on Merged/RefSeq; Ensembl keeps floor 1; `VEP_STREAM_RUN_BUFFERS` is floored at the effective floor.
- Scheduling only. Values and row order stay identical to workers=1. No Python API change.
- Engine PR from engine `origin/master` (0cefc99), **not** stacked on #261.
- Success: LF `collect()` at w8 within 20% of `output_vcf` at w8 on chr22 and chr1, core and five plugins, except chr22 with plugins. RSS is reported, not gated.
- Same branch name in both repos: `perf/lf-run-pool-scaling`.
- Never merge, never enable auto-merge. Draft PRs; the human merges.
- Measured builds: `env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`, identical for baseline and final.
- The temporary Cargo `[patch]` used for local iteration never reaches a commit.
- Never stage `notebooks/*.ipynb` in the main checkout (the user's uncommitted edits).
- A test or gate command whose output is piped for reading runs under `set -o pipefail` with `|| exit 1`, so a failure cannot hide behind `tail`. Commands in RED steps, which are expected to fail, are exempt.

## Review Focus

1. **Region query on a large stateful contig at workers>1.** The call site passes the whole contig's buffer count, so a small region on chr1 would keep 4-buffer runs and get few runs. Expected: the plan counts only the buffers inside the planned ranges. Pinned by `covered_buffers` unit tests (Task 4) and `test_merged_region_run_plan_fills_workers` (Task 6). This extends the spec's "buffers" to mean "buffers the runs cover", a scheduling-only refinement; say so in the PR body.
2. **One-run seams on Merged.** With the floor down to 1 or 2, a warm-up can be as long as the run it prepares. Expected: identical to workers=1. Pinned by the vepyr pytest at workers=8 on the merged golden (Task 6) and the chr22/chr1 parity gate on merged and refseq (Task 7).
3. **workers=0 or 1.** Expected: budget 1024 and floor unchanged (`ceil(buffers / 1)` ≥ 4 for any contig with 4+ buffers). Pinned in the Task 3 and Task 4 unit tests.
4. **An empty contig or region (0 buffers).** Expected: no panic, floor 1, no runs. Pinned by `(0, 8, true, None) -> 1` and `covered_buffers(&[]) == 0` (Task 4).
5. **User override below the new floor** (`VEP_STREAM_RUN_BUFFERS=1` on merged, as the existing vepyr tests set). Expected: lifted to the effective floor, never below it. Pinned by `(11, 8, true, Some("1")) -> 2` (Task 4).

## Ownership (runbook step 1)

The analysis for step 1 was done by measurement during brainstorming, not by three read-only agents. The spec's evidence is the analysis:
- **datafusion-bio-functions** owns both causes: `stream_buffer_mb` and `stream_run_buffers` in `annotate_provider.rs`.
- **vepyr** carries the pin and gains a test and a benchmark script. `_batch_source` is not changed.
- **datafusion-bio-formats** is untouched. The VCF reader is not on the path that moved.

Runbook step 1a-bis (quote the Ensembl VEP rule) does not apply: no VEP behaviour is being ported, and the output is unchanged by construction. Spec approval plus plan approval stand in for the step 1d go-ahead; the executor starts at Task 1.

## File Structure

| Repo | File | Change |
|---|---|---|
| functions | `datafusion/bio-function-vep/src/annotate_provider.rs` | `STREAM_BUFFER_MB_PER_WORKER`, `stream_buffer_mb(workers, env)`, stateful floor in `stream_run_buffers`, new `covered_buffers`, both call sites, unit tests in `mod tests` |
| vepyr | `performance-tests/vepyr/scripts/lf_run_pool_bench.py` | new: raw / lf / vcf timing and peak RSS, one subprocess per run |
| vepyr | `tests/test_lazyframe_workers.py` | two run-plan tests at workers=8 on merged, docstring update |
| vepyr | `Cargo.toml` | pin `datafusion-bio-function-vep` to the engine PR head, with a comment |
| vepyr | `docs/superpowers/{specs,plans}/…` | already on the branch |

Worktrees:
- vepyr: `~/research/git/_wt/vepyr-lf-run-pool` (exists; branch renamed in Task 1)
- functions: `~/research/git/_wt/functions-run-pool` (created in Task 1)

Run directory for all evidence: `$ROOT/e2e-testing/results/fix-lf-run-pool/{baseline,spike,final}`, where `ROOT=~/research/git/_wt/vepyr-lf-run-pool`.

---

### Task 1: Worktrees, branches, and the benchmark script

**Files:**
- Create: `performance-tests/vepyr/scripts/lf_run_pool_bench.py`

**Interfaces:**
- Produces: `lf_run_pool_bench.py` CLI, used by Tasks 2, 3 and 8:
  `--out-dir DIR --input NAME=PATH [...] --cache-dir DIR --fasta PATH --plugin-cache-root DIR --workers N [...] --modes {raw,lf,vcf} [...] --plugin-sets {none,all} [...] --repeats 3 --max-load 4 --env KEY=VAL [...]`.
  It writes `DIR/results.json` (one object per configuration: `input, plugins, mode, workers, env, median_wall_s, min_wall_s, max_wall_s, median_rss_gib, median_engine_wait_s, median_consumer_s, batches, runs`) and `DIR/summary.md` (the table, plus one `lf/vcf` ratio line per input and plugin set at the largest worker count).

- [ ] **Step 1: Rename the vepyr branch and create the engine worktree**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
git branch -m docs/lf-run-pool-scaling-spec perf/lf-run-pool-scaling
git -C ~/research/git/datafusion-bio-functions fetch -q origin
git -C ~/research/git/datafusion-bio-functions worktree add -b perf/lf-run-pool-scaling \
  ~/research/git/_wt/functions-run-pool origin/master
git -C ~/research/git/_wt/functions-run-pool log --oneline -1   # expect 0cefc99
grep -n 'datafusion-bio-function-vep' Cargo.toml                # expect rev = "0cefc99"
```

The engine master head and the vepyr pin must be the same commit, so the baseline build measures engine master.

- [ ] **Step 2: Link the gitignored results directory into the worktree**

The e2e harness reads references relative to the script (see memory `md5-harness-extracts-29gb-of-slices`):

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
[ -e e2e-testing/results ] || ln -s ~/research/git/vepyr/e2e-testing/results e2e-testing/results
ls e2e-testing/results/116 | head -3
```

- [ ] **Step 3: Write the benchmark script**

The script is committed at `performance-tests/vepyr/scripts/lf_run_pool_bench.py`, and that file is the source of truth. It was first written in 1040f13 and then hardened in review on #134: it validates row counts in every mode, aborts when the host never goes quiet, reports setup and a paired end-to-end median, and strips inherited `VEP_*` variables. Its behaviour is pinned by `tests/test_lf_run_pool_bench.py`. Do not rewrite it from an embedded listing. Re-running this step means checking that both files exist and running:

```bash
env -u CONDA_PREFIX .venv/bin/python -m pytest -q tests/test_lf_run_pool_bench.py
```

Expected: all tests pass.

- [ ] **Step 4: Build the baseline extension**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
uptime; df -g ~/workspace/data_vepyr | tail -1
env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
.venv/bin/python -c "import vepyr, pathlib; print(pathlib.Path(vepyr.__file__).parent)"
```

Expected: the printed path is inside `~/research/git/_wt/vepyr-lf-run-pool/src/vepyr`.

- [ ] **Step 5: Smoke-run the script**

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr
cd ~/research/git/_wt/vepyr-lf-run-pool
.venv/bin/python performance-tests/vepyr/scripts/lf_run_pool_bench.py \
  --out-dir /tmp/lfb-smoke \
  --input chr22=$DATA_VEPYR_DIR/polars_integration/chr22/input/HG002_chr22.vcf.gz \
  --cache-dir $DATA_VEPYR_DIR/cache/116_GRCh38_merged \
  --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
  --plugin-cache-root $DATA_VEPYR_DIR/plugin_cache_116 \
  --workers 8 --modes raw lf vcf --plugin-sets none --repeats 1 --max-load 99
cat /tmp/lfb-smoke/summary.md
```

Expected: three rows. raw about 1.6 s, lf about 1.65 s, vcf about 1.4 s. `batches` is about 28 for raw and lf and 0 for vcf. `peak RSS GiB` is nonzero. There is an `lf/vcf` line. If raw shows `batches` 0, the probe capture failed; fix it before going on.

- [ ] **Step 6: Lint and commit**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
uv run pre-commit run ruff --files performance-tests/vepyr/scripts/lf_run_pool_bench.py || exit 1
uv run pre-commit run ruff-format --files performance-tests/vepyr/scripts/lf_run_pool_bench.py || exit 1
git add performance-tests/vepyr/scripts/lf_run_pool_bench.py docs/superpowers/plans/2026-09-26-lf-run-pool-scaling.md
git commit -m "perf: LazyFrame run-pool bench (raw stream, collect, output_vcf, per-run RSS)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
git log -1 --oneline   # pre-commit may abort the first attempt; re-stage and re-commit, then check
```

---

### Task 2: Baseline, before any engine edit (runbook step 2)

**Files:** none (evidence only, in `$ROOT/e2e-testing/results/fix-lf-run-pool/baseline`)

**Interfaces:**
- Consumes: the Task 1 build (engine master 0cefc99) and `lf_run_pool_bench.py`.
- Produces: `baseline/archive/summary.tsv` and traces (WGS sweep), `baseline/md5.out` and `baseline/reports`, `baseline/lf/results.json` and `summary.md`. Task 8 compares against all of them.

- [ ] **Step 1: Prepare the chr1 slice**

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr
B=$DATA_VEPYR_DIR/polars_integration/lf_bench && mkdir -p $B
[ -f $B/chr1.vcf.gz ] || { bcftools view -r chr1 -Oz -o $B/chr1.vcf.gz \
    $DATA_VEPYR_DIR/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz && tabix -p vcf $B/chr1.vcf.gz; }
zcat < $B/chr1.vcf.gz | grep -vc '^#'   # expect 319349
```

- [ ] **Step 2: Check the host, and free the md5 slices**

```bash
uptime; docker ps --format '{{.Names}}'
find ~/research/git/vepyr/e2e-testing/results/116 -name "vep_chr*_merged.vcf" -delete
df -g ~/workspace/data_vepyr | tail -1
```

If the load average is above 4, or an unrelated container is burning CPU, wait and re-check. If less than 65 GiB is free, **stop and ask the user** what to free (candidates: `target/` directories of stale worktrees under `~/research/git/_wt/`). Never delete user data on your own.

- [ ] **Step 3: Run the WGS worker sweep and the md5 gate exactly as in runbook step 2**

Run the runbook's step 2 blocks verbatim from `~/research/git/_wt/vepyr-lf-run-pool`, with
`RUN=$ROOT/e2e-testing/results/fix-lf-run-pool/baseline`: the `sweep` warm-up at 8, the measured `sweep` at 8 and 1, the trace-line check, then the md5 strict `run_comparison.py` block. After the md5 block, delete the slices again:

```bash
find ~/research/git/vepyr/e2e-testing/results/116 -name "vep_chr*_merged.vcf" -delete
```

**Gate:** every contig's body digest matches in strict mode. If not, stop and report: a red baseline cannot attribute anything.

- [ ] **Step 4: Run the LF bench**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
RUN=$PWD/e2e-testing/results/fix-lf-run-pool/baseline
.venv/bin/python performance-tests/vepyr/scripts/lf_run_pool_bench.py \
  --out-dir $RUN/lf \
  --input chr22=$DATA_VEPYR_DIR/polars_integration/chr22/input/HG002_chr22.vcf.gz \
  --input chr1=$DATA_VEPYR_DIR/polars_integration/lf_bench/chr1.vcf.gz \
  --cache-dir $DATA_VEPYR_DIR/cache/116_GRCh38_merged \
  --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
  --plugin-cache-root $DATA_VEPYR_DIR/plugin_cache_116 \
  --workers 1 4 8 --modes raw lf vcf --plugin-sets none all --repeats 3 \
  > $RUN/lf.log 2>&1 || exit 1
cat $RUN/lf/summary.md
```

Expected shape, from the spec: chr22 merged raw flat from w4 to w8. chr1 plugins raw flat (about 13–15 s) from w4 to w8. The `lf/vcf` ratio at w8 is above 1.2 for chr1 plugins. Plugin runs here use serial takes, because #261 is not on master, so absolute plugin numbers are higher than the spec's.

---

### Task 3: Engine — the budget scales with workers

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (the `STREAM_BUFFER_MB_DEFAULT` block near line 11916, `stream_buffer_mb` near 11922, its call site near 14077, test `stream_buffer_mb_default_and_override` near 18769)

**Interfaces:**
- Produces: `const STREAM_BUFFER_MB_PER_WORKER: usize = 512;` and `fn stream_buffer_mb(workers: usize, env: Option<&str>) -> usize`.

- [ ] **Step 1: Write the failing test**

Replace the body of `stream_buffer_mb_default_and_override` in `mod tests`:

```rust
    #[test]
    fn stream_buffer_mb_default_and_override() {
        // The default grows with the pool, never below the 1 GiB floor.
        assert_eq!(stream_buffer_mb(0, None), STREAM_BUFFER_MB_DEFAULT);
        assert_eq!(stream_buffer_mb(1, None), STREAM_BUFFER_MB_DEFAULT);
        assert_eq!(stream_buffer_mb(2, None), STREAM_BUFFER_MB_DEFAULT);
        assert_eq!(stream_buffer_mb(4, None), 2048);
        assert_eq!(stream_buffer_mb(8, None), 4096);
        // A positive override replaces it outright, below the default too.
        assert_eq!(stream_buffer_mb(8, Some("256")), 256);
        // Zero and garbage fall back to the default.
        assert_eq!(stream_buffer_mb(8, Some("0")), 4096);
        assert_eq!(stream_buffer_mb(8, Some("x")), 4096);
    }
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd ~/research/git/_wt/functions-run-pool
cargo test -p datafusion-bio-function-vep --lib stream_buffer_mb 2>&1 | tail -5
```

Expected: a compile error, `this function takes 1 argument but 2 arguments were supplied`.

- [ ] **Step 3: Implement**

Replace the constant block and the function:

```rust
const MIB: usize = 1 << 20;
/// Floor (MiB) of the byte budget for annotated batches queued by runs behind
/// the head of the streaming run pool (`VEP_STREAM_BUFFER_MB`).
const STREAM_BUFFER_MB_DEFAULT: usize = 1024;
/// Per-worker share of the default budget. A run behind the head holds its
/// whole output until released, and plugin CSQ runs to ~15 KB a row, so a
/// 20k-row run queues ~300 MB. With a fixed 1 GiB only ~3 runs fit and the pool
/// stalls behind its head: chr1 + 5 plugins stopped gaining at 2 GiB for 4
/// workers and 3 GiB for 8. The budget is a cap, not an allocation; the queue
/// only grows as far as the head lags.
const STREAM_BUFFER_MB_PER_WORKER: usize = 512;

/// Byte budget for the streaming run pool's queued output:
/// `max(STREAM_BUFFER_MB_DEFAULT, STREAM_BUFFER_MB_PER_WORKER * workers)`.
/// `env` is `VEP_STREAM_BUFFER_MB`; a positive value replaces the default.
fn stream_buffer_mb(workers: usize, env: Option<&str>) -> usize {
    env.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| {
            STREAM_BUFFER_MB_DEFAULT.max(STREAM_BUFFER_MB_PER_WORKER * workers.max(1))
        })
}
```

At the call site in the `RunPoolState { .. }` literal, `workers` is already a local:

```rust
                                    budget: OutputBudget::new(stream_buffer_mb(
                                        workers,
                                        std::env::var("VEP_STREAM_BUFFER_MB").ok().as_deref(),
                                    )),
```

If `MIB` is already defined right above, keep the one definition. `grep -n "const MIB" annotate_provider.rs` must print one line.

- [ ] **Step 4: Run it and see it pass, with the pool tests**

```bash
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib stream_buffer_mb 2>&1 | tail -3 || exit 1
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib output_budget 2>&1 | tail -3 || exit 1
```

Expected: `test result: ok` for both. Two `output_budget_*` tests pass, unchanged.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "perf(vep): scale the run-pool output budget with workers

The fixed 1 GiB budget stalls runs behind the head once plugin CSQ makes a run
~300 MB: chr1 + 5 plugins at 8 workers, 13.0 s -> 9.0 s with 3 GiB.
Default is now max(1024, 512 x workers) MiB; VEP_STREAM_BUFFER_MB still wins.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Engine — the stateful floor fills the workers, counted over the planned ranges

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`STREAM_MIN_RUN_BUFFERS_STATEFUL` doc near 10888, `stream_run_buffers` near 10896, the planning call site near 15247, test `stream_run_buffers_default_and_floor` near 18550)

**Interfaces:**
- Consumes: nothing from Task 3.
- Produces: `fn stream_run_buffers(buffers: usize, workers: usize, stateful: bool, env: Option<&str>) -> usize`, same signature with the new floor, and `fn covered_buffers(ranges: &[(Vec<RunBounds>, (usize, usize))]) -> usize`.

- [ ] **Step 1: Write the failing tests**

Replace the body of `stream_run_buffers_default_and_floor` and add `covered_buffers_sums_planned_ranges` next to it:

```rust
    #[test]
    fn stream_run_buffers_default_and_floor() {
        // 65 buffers (chr1), 8 workers: ceil(65 / 32) = 3, floored to 4 on stateful.
        assert_eq!(stream_run_buffers(65, 8, true, None), 4);
        assert_eq!(stream_run_buffers(65, 8, false, None), 3);
        // Small stateful contig: the floor gives way so every worker gets a run.
        // chr22 has 11 buffers: 6 runs at 8 workers, 4 runs at 4 workers.
        assert_eq!(stream_run_buffers(11, 8, true, None), 2);
        assert_eq!(stream_run_buffers(11, 4, true, None), 3);
        // One worker, or workers=0 treated as one, keeps the full floor.
        assert_eq!(stream_run_buffers(11, 1, true, None), 4);
        assert_eq!(stream_run_buffers(11, 0, true, None), 4);
        // Fewer buffers than workers: one buffer per run.
        assert_eq!(stream_run_buffers(3, 8, false, None), 1);
        assert_eq!(stream_run_buffers(3, 8, true, None), 1);
        // An empty contig or region never yields a zero-length run.
        assert_eq!(stream_run_buffers(0, 8, true, None), 1);
        // Override wins but not below the effective stateful floor.
        assert_eq!(stream_run_buffers(65, 8, false, Some("2")), 2);
        assert_eq!(stream_run_buffers(65, 8, true, Some("2")), 4);
        assert_eq!(stream_run_buffers(65, 8, true, Some("9")), 9);
        assert_eq!(stream_run_buffers(11, 8, true, Some("1")), 2);
        // Garbage override is ignored.
        assert_eq!(stream_run_buffers(65, 8, false, Some("x")), 3);
        assert_eq!(stream_run_buffers(65, 8, false, Some("0")), 3);
    }

    #[test]
    fn covered_buffers_sums_planned_ranges() {
        assert_eq!(covered_buffers(&[]), 0);
        assert_eq!(covered_buffers(&[(vec![RunBounds::OPEN], (0, 65))]), 65);
        // Two pushed-down regions on a large contig: only their buffers count.
        assert_eq!(
            covered_buffers(&[
                (vec![RunBounds::OPEN], (10, 13)),
                (vec![RunBounds::OPEN], (40, 42)),
            ]),
            5
        );
        // An inverted range contributes nothing rather than underflowing.
        assert_eq!(covered_buffers(&[(vec![RunBounds::OPEN], (7, 7))]), 0);
    }
```

- [ ] **Step 2: Run them and see them fail**

```bash
cd ~/research/git/_wt/functions-run-pool
cargo test -p datafusion-bio-function-vep --lib stream_run_buffers 2>&1 | tail -5
```

Expected: a compile error, `cannot find function covered_buffers`. With `covered_buffers_sums_planned_ranges` temporarily commented out, the other test fails on `left: 4, right: 2` for `(11, 8, true, None)`. Check that, then restore the test.

- [ ] **Step 3: Implement**

```rust
/// Floor on a Merged/RefSeq run's length in buffers. Every seam replays about
/// `overlap_width_bp` of input (about one buffer at whole-genome density), and
/// four-buffer runs keep that replay a small share of the run's own work. It
/// gives way on inputs too small to give every worker a four-buffer run: chr22
/// has 11 buffers, which a fixed floor cut into 3 runs, so no worker count
/// above 3 could help. Idle workers cost more than the extra replay.
const STREAM_MIN_RUN_BUFFERS_STATEFUL: usize = 4;

/// Run length in whole buffers for a contig or region of `buffers` buffers.
/// `env` is `VEP_STREAM_RUN_BUFFERS`; it overrides the formula but never the
/// stateful floor, `min(STREAM_MIN_RUN_BUFFERS_STATEFUL, ceil(buffers /
/// workers))`. Scheduling only: the run cut never changes output.
fn stream_run_buffers(buffers: usize, workers: usize, stateful: bool, env: Option<&str>) -> usize {
    let workers = workers.max(1);
    let floor = if stateful {
        STREAM_MIN_RUN_BUFFERS_STATEFUL
            .min(buffers.div_ceil(workers))
            .max(1)
    } else {
        1
    };
    let from_env = env
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0);
    let chosen = from_env.unwrap_or_else(|| {
        let target_runs = workers * STREAM_RUNS_PER_WORKER;
        buffers.div_ceil(target_runs).max(1)
    });
    chosen.max(floor)
}

/// Buffers the planned ranges cover: the whole contig when nothing was pushed
/// down, only the regions' buffers otherwise, so a small region on a large
/// contig is still cut into enough runs.
fn covered_buffers(ranges: &[(Vec<RunBounds>, (usize, usize))]) -> usize {
    ranges
        .iter()
        .map(|(_, (bk, bk1))| bk1.saturating_sub(*bk))
        .sum()
}
```

At the planning call site, cut by the covered count instead of `b`. `b` is still used to build `ranges` above it; leave that alone. Bind the count once, and add it to the `run_pool`/`plan` trace line right after `buffers` so a test can see what the cut was based on. `compare_runs.py` only reads `*_ms` keys, so a new count field does not touch the performance gate:

```rust
        let covered = covered_buffers(&ranges);
        let run_buffers = stream_run_buffers(
            covered,
            config.annotation_workers,
            stateful,
            std::env::var("VEP_STREAM_RUN_BUFFERS").ok().as_deref(),
        );
        let planned = plan_stream_runs(&boundaries, &ranges, run_buffers, overlap);
        pipeline_trace::emit(
            "run_pool",
            "plan",
            &[
                ("chrom", TraceValue::Str(&chrom)),
                ("buffers", TraceValue::Usize(b)),
                ("covered", TraceValue::Usize(covered)),
                ("run_buffers", TraceValue::Usize(run_buffers)),
                ("runs", TraceValue::Usize(planned.len())),
                ("warm_up", TraceValue::Usize(usize::from(overlap > 0))),
            ],
        );
```

- [ ] **Step 4: Run them and see them pass, with the pool planning tests**

```bash
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib stream_run_buffers 2>&1 | tail -3 || exit 1
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib covered_buffers 2>&1 | tail -3 || exit 1
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib plan_stream_runs 2>&1 | tail -3 || exit 1
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib admission 2>&1 | tail -3 || exit 1
```

Expected: every one reports `test result: ok`.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "perf(vep): let the stateful run floor give way on small contigs and regions

A fixed four-buffer floor cut chr22 Merged (11 buffers) into 3 runs, so the
LazyFrame stream stopped scaling at 3 workers. The floor is now
min(4, ceil(buffers / workers)), with buffers counted over the planned ranges
so a pushed-down region on a large contig is cut the same way.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Local build, floor check, and the full engine suite

**Files:** none committed. The `[patch]` in vepyr's `Cargo.toml` is scratch.

**Interfaces:**
- Consumes: the engine branch after Tasks 3 and 4.
- Produces: a vepyr `.venv` built against the local engine, used by Task 6 before the engine is pushed; the spike numbers in `spike/`.

- [ ] **Step 1: Full engine suite, clippy, fmt**

```bash
cd ~/research/git/_wt/functions-run-pool
df -g ~ | tail -1   # a debug test tree is ~8 GB
cargo fmt --check || exit 1
cargo clippy --all-targets --all-features -- -D warnings || exit 1
set -o pipefail; cargo test -p datafusion-bio-function-vep --lib 2>&1 | tail -3 || exit 1
```

Expected: `test result: ok` with 0 failed. Record the passed count.

- [ ] **Step 2: Point vepyr at the local engine and build**

Append to `~/research/git/_wt/vepyr-lf-run-pool/Cargo.toml` (scratch, never committed):

```toml
[patch."https://github.com/biodatageeks/datafusion-bio-functions.git"]
datafusion-bio-function-vep = { path = "/Users/mwiewior/research/git/_wt/functions-run-pool/datafusion/bio-function-vep" }
```

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
VEP_PIPELINE_TRACE=1 .venv/bin/python - <<'EOF' 2>&1 | grep "run_pool event=plan"
import os, vepyr
D = os.path.expanduser("~/workspace/data_vepyr")
vepyr.annotate(f"{D}/polars_integration/chr22/input/HG002_chr22.vcf.gz", f"{D}/cache/116_GRCh38_merged",
               everything=True, reference_fasta=f"{D}/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa",
               workers=8, show_progress=False).head(1).collect()
EOF
```

Expected: `buffers=11 run_buffers=2 runs=6`. If it still says `run_buffers=4 runs=3`, the build is serving the old extension (maturin/CONDA trap); rebuild before going on.

- [ ] **Step 3: Floor check (the spec's provisional formula)**

The new floor lets `VEP_STREAM_RUN_BUFFERS` reach 1 and 2 on chr22 at w8, so one build covers floors 1, 2 and 4:

```bash
RUN=$PWD/e2e-testing/results/fix-lf-run-pool/spike
for rb in 1 2 4; do
  .venv/bin/python performance-tests/vepyr/scripts/lf_run_pool_bench.py \
    --out-dir $RUN/rb$rb --env VEP_STREAM_RUN_BUFFERS=$rb \
    --input chr22=$DATA_VEPYR_DIR/polars_integration/chr22/input/HG002_chr22.vcf.gz \
    --cache-dir $DATA_VEPYR_DIR/cache/116_GRCh38_merged \
    --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    --plugin-cache-root $DATA_VEPYR_DIR/plugin_cache_116 \
    --workers 4 8 --modes raw --plugin-sets none all --repeats 3 > $RUN/rb$rb.log 2>&1 || exit 1
done
for rb in 1 2 4; do echo "== run_buffers=$rb"; head -6 $RUN/rb$rb/summary.md | tail -4; done
```

At w8, `rb=1` is floored to 2 by the new formula (`ceil(11/8) = 2`), so `rb1` and `rb2` should be equal at w8. At w4, the floor is 3 and all three settings resolve to 3 or 4. So the only real comparison at w8 is floor 2 against 4, and floor 1 is out of reach through the env knob. That is enough to decide.

**Decision rule:**
- floor 2 is faster than floor 4 at w8, core and plugins: keep the formula.
- floor 2 is slower than floor 4 at w8 by more than 5% on either set: **stop and tell the user**. The spec's formula is contradicted, and the spec changes before anything is pushed.

- [ ] **Step 4: Record the result in the plan**

Append a `### Floor check result` note under this task (median walls for rb2 and rb4 at w8, core and plugins, and the decision). Commit it to the vepyr branch:

```bash
git add docs/superpowers/plans/2026-09-26-lf-run-pool-scaling.md
git commit -m "docs: record the run-floor check result

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Floor check result

Measured 2026-09-26 12:20 (load 3.8), chr22 Merged, raw engine stream, local engine 737c5cd, median of 3. `VEP_STREAM_RUN_BUFFERS=1` resolves to the floor of 2 at w8 and 3 at w4, as designed.

| | rb=4 (old floor) | rb=2 (new floor at w8) |
|---|---|---|
| core w8 | 1.627 s | 1.037 s (-36%) |
| plugins w8 | 4.190 s | 2.353 s (-44%) |
| core w4 | 1.558 s | 1.365 s (floor 3) |
| plugins w4 | 4.142 s | 3.288 s (floor 3) |

Decision: keep `min(4, ceil(buffers / workers))`. Floor 2 beats floor 4 by far more than the 5% bar on both sets.

---

### Task 6: vepyr — run-plan tests on the merged golden

**Files:**
- Modify: `tests/test_lazyframe_workers.py` (module docstring; two new tests after `test_collect_equals_serial_with_default_run_length`)

**Interfaces:**
- Consumes: the local-engine build from Task 5, and the fixtures `merged_cache_dir` and `_lazy` / `_assert_same` already in the file.

- [ ] **Step 1: Write the tests**

Update the docstring's last sentences:

```python
"""workers>1 on the LazyFrame path must equal workers=1 row for row, in order.

``buffer_size=7`` turns the 100-variant fixture into ~15 input buffers and
``VEP_STREAM_RUN_BUFFERS=1`` makes every buffer its own run on the Ensembl
cache, so the ordered release crosses a seam at every buffer. On the merged
cache the four-buffer floor (stateful warm-up) gives way when the input is too
small to give every worker a run, so at 8 workers it is cut into two-buffer
runs; the run-plan tests assert that cut from the pipeline trace.
"""
```

Add after `test_collect_equals_serial_with_default_run_length`:

```python
def _run_plan(stderr: str) -> dict[str, int]:
    """Integer fields of the pool's `run_pool event=plan` trace line."""
    import re

    line = next(
        (l for l in stderr.splitlines() if "stage=run_pool event=plan" in l), None
    )
    assert line, "no run_pool plan line; is VEP_PIPELINE_TRACE honoured?"
    return {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)\b", line) if k != "t_ms"}


def _floor(buffers: int, workers: int) -> int:
    """The stateful floor the engine applies: min(4, ceil(buffers / workers)), at least 1."""
    return max(1, min(4, -(-buffers // workers)))


def test_merged_small_input_fills_workers(merged_cache_dir, monkeypatch, capfd):
    monkeypatch.delenv("VEP_STREAM_RUN_BUFFERS", raising=False)
    serial = _lazy(merged_cache_dir, 1).collect()
    capfd.readouterr()
    monkeypatch.setenv("VEP_PIPELINE_TRACE", "1")
    parallel = _lazy(merged_cache_dir, 8).collect()
    plan = _run_plan(capfd.readouterr().err)
    # Fewer than 4 x 8 buffers: the floor is ceil(buffers / 8), not 4.
    assert plan["buffers"] < 32, plan
    assert plan["run_buffers"] == _floor(plan["buffers"], 8), plan
    assert plan["run_buffers"] < 4, plan
    _assert_same(parallel, serial)


def test_merged_region_run_plan_fills_workers(merged_cache_dir, monkeypatch, capfd):
    monkeypatch.delenv("VEP_STREAM_RUN_BUFFERS", raising=False)
    serial = _lazy(merged_cache_dir, 1).collect()
    starts = serial["start"].to_list()
    # About two of the fixture's ~15 seven-row buffers.
    predicate = (pl.col("chrom") == "chr1") & pl.col("start").is_between(
        starts[21], starts[33]
    )
    capfd.readouterr()
    monkeypatch.setenv("VEP_PIPELINE_TRACE", "1")
    pushed = _lazy(merged_cache_dir, 8).filter(predicate).collect()
    plan = _run_plan(capfd.readouterr().err)
    # The cut is based on the region's buffers, not the contig's.
    assert plan["covered"] < plan["buffers"], plan
    assert plan["run_buffers"] == _floor(plan["covered"], 8), plan
    _assert_same(pushed, serial.filter(predicate))
```

- [ ] **Step 2: Run them against the local-engine build**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
set -o pipefail; env -u CONDA_PREFIX .venv/bin/python -m pytest tests/test_lazyframe_workers.py -v 2>&1 | tail -15 || exit 1
```

Expected: every test passes, including the two new ones. If the region test finds `covered` equal to `buffers`, the predicate did not reach the engine as a region; check the `regions` trace line before touching the assertion. Do not loosen either test.

- [ ] **Step 3: Show the first test fails on engine master**

Remove the `[patch]` block from `Cargo.toml`, rebuild (`env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`), and run only the first new test:

```bash
env -u CONDA_PREFIX .venv/bin/python -m pytest tests/test_lazyframe_workers.py::test_merged_small_input_fills_workers -v 2>&1 | tail -5
```

Expected: FAIL on `plan["run_buffers"] == _floor(...)` (master cuts 4-buffer runs). The region test would fail with `KeyError: 'covered'` on master, which proves nothing about the cut, so it is not the one run here. Put the `[patch]` block back and rebuild with the Task 5 Step 2 command, so the tree is on the local engine again.

- [ ] **Step 4: Full Python suite on the local engine**

```bash
set -o pipefail; env -u CONDA_PREFIX .venv/bin/python -m pytest -q 2>&1 | tail -5 || exit 1
```

Expected: no failures. Record the passed count.

- [ ] **Step 5: Commit the tests (not Cargo.toml)**

```bash
uv run pre-commit run ruff --files tests/test_lazyframe_workers.py || exit 1
uv run pre-commit run ruff-format --files tests/test_lazyframe_workers.py || exit 1
git add tests/test_lazyframe_workers.py
git status --short   # Cargo.toml must show as modified and NOT be staged
git commit -m "test: merged LazyFrame run plan fills the workers on small inputs and regions

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
git log -1 --oneline
```

---

### Task 7: Push, pin, open the draft PRs, parity gate (runbook steps 5 and 6)

**Files:**
- Modify: vepyr `Cargo.toml:182-188` (pin comment and rev; drop the scratch `[patch]`)

**Interfaces:**
- Consumes: the engine branch (Tasks 3 and 4) and the vepyr branch (Tasks 1, 5 and 6).
- Produces: `PRS="biodatageeks/datafusion-bio-functions:<n> biodatageeks/vepyr:<m>"` for Tasks 8 and 9.

- [ ] **Step 1: Push the engine branch and read its head**

```bash
cd ~/research/git/_wt/functions-run-pool
git push -u origin perf/lf-run-pool-scaling
HEAD_FN=$(git rev-parse --short=7 HEAD); echo $HEAD_FN
```

- [ ] **Step 2: Pin vepyr to it**

Delete the scratch `[patch]` block from `Cargo.toml`. Replace the `0cefc99` comment paragraph and the rev with:

```toml
# bio-functions <HEAD_FN>: head of biodatageeks/datafusion-bio-functions#<n>
# (perf/lf-run-pool-scaling) on top of 0cefc99, the MERGE commit of #260. The
# streaming run pool's output budget scales with workers and the stateful run
# floor gives way on small contigs and regions, so the LazyFrame path scales
# like output_vcf. Scheduling only; output unchanged. Pinned to the PR head for
# review; re-pin to the merge commit once it merges.
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", rev = "<HEAD_FN>", features = ["cache-builder"] }
```

Fill `<n>` after Step 3 creates the engine PR. Commit the pin once `<n>` is known.

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool
env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
grep -n 'datafusion-bio-function-vep\|\[patch' Cargo.toml   # one rev line, no [patch]
```

- [ ] **Step 3: Open both draft PRs, engine first (runbook step 5)**

Write `/tmp/pr-datafusion-bio-functions.md` and `/tmp/pr-vepyr.md`. Each contains:
- the problem, with the spec's evidence table;
- the two changes, and the `covered_buffers` extension from Review Focus 1;
- "scheduling only; output unchanged", and the gates still to run;
- a link to the other PR.

End each with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Then run the runbook step 5 loop with `BRANCH=perf/lf-run-pool-scaling`, over `datafusion-bio-functions vepyr` only (formats is untouched), and do the body read-back check. Fill `<n>` into the pin comment and commit:

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: pin bio-functions to the run-pool scaling PR head

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
git push -u origin perf/lf-run-pool-scaling
```

- [ ] **Step 4: Parity gate on the pinned build**

```bash
cd ~/research/git/_wt/vepyr-lf-run-pool/e2e-testing/scripts
for c in 22 1; do
  env -u CONDA_PREFIX ../../.venv/bin/python lazyframe_workers_parity.py --release 116 --chrom $c \
    --sweep 1 2 4 8 --profiles ensembl merged refseq merged_plugins \
    --out-dir $ROOT/e2e-testing/results/fix-lf-run-pool/final/parity_chr$c || exit 1
done
env -u CONDA_PREFIX ../../.venv/bin/python region_pushdown_parity.py --release 116 --chrom 22 \
  --profiles ensembl merged refseq --out-dir $ROOT/e2e-testing/results/fix-lf-run-pool/final/regions || exit 1
# The engine these reports were produced on, as Cargo resolved it, next to them.
engine_rev() { sed -n '/name = "datafusion-bio-function-vep"/,/^source/s/.*#\([0-9a-f]\{40\}\)"$/\1/p' "$ROOT/Cargo.lock"; }
engine_rev > $ROOT/e2e-testing/results/fix-lf-run-pool/final/parity_engine_rev.txt
[ -s $ROOT/e2e-testing/results/fix-lf-run-pool/final/parity_engine_rev.txt ] || { echo "could not resolve the engine revision"; exit 1; }
```

**Gate:** every `equal` column is `True` (frames against workers=1, and CSQ against output_vcf at w8). Any `False` is a correctness bug. Stop, triage it, and do not go on to review.

- [ ] **Step 5: Ask both reviewers (runbook step 6)**

Run the runbook step 6 loop over `$PRS`.

---

### Task 8: Review loop to green, then re-verify against the baseline (runbook steps 7 and 8)

**Files:** whatever the review asks for. Each engine fix follows Tasks 3/4's TDD shape, then re-pins vepyr to the new engine head.

- [ ] **Step 1: Iterate to green (runbook step 7)**

Run the runbook's watch loop over `$PRS`. After any engine push, re-pin vepyr to the new engine head (and edit the SHA in the pin comment), rebuild, re-run `tests/test_lazyframe_workers.py`, push, and only then re-request both reviewers. **Gate:** all checks green on both PRs, every bot finding answered, and both bots reviewed the current heads.

- [ ] **Step 2: Re-run the runbook step 2 blocks into `final/` (runbook step 8)**

Before starting, confirm the vepyr pin equals the engine PR's current head:

```bash
gh pr view <n> --repo biodatageeks/datafusion-bio-functions --json headRefOid --jq .headRefOid
grep -n 'datafusion-bio-function-vep' ~/research/git/_wt/vepyr-lf-run-pool/Cargo.toml
```

Then repeat Task 2 Steps 2–4 byte for byte with `RUN=$ROOT/e2e-testing/results/fix-lf-run-pool/final`: host check, slice deletion, WGS sweep, md5 strict, slice deletion, LF bench. Use the same `RUSTFLAGS` build.

Then compare the engine the parity reports were produced on with the final pin, mechanically:

```bash
FINAL_REV=$(sed -n '/name = "datafusion-bio-function-vep"/,/^source/s/.*#\([0-9a-f]\{40\}\)"$/\1/p' "$ROOT/Cargo.lock")
PARITY_REV=$(cat "$ROOT/e2e-testing/results/fix-lf-run-pool/final/parity_engine_rev.txt" 2>/dev/null)
[ -n "$FINAL_REV" ] && [ "$FINAL_REV" = "$PARITY_REV" ] \
  || { echo "parity is stale: ran on '${PARITY_REV:-none}', final pin is $FINAL_REV"; exit 1; }
```

If it stops on stale parity, re-run Task 7 Step 4 on the final pin, then this check again, before any later gate, into the same `final/parity_*` and `final/regions` directories, which rewrites `parity_engine_rev.txt`. Parity evidence from an earlier engine revision does not cover the one being handed off.

- [ ] **Step 2b: Supplementary plugin target on a build stacking #131**

This branch lacks vepyr #131, so the `chr1 all` LF target is measured on a local stack: #131's head with its engine pin moved to this PR's engine head. It is never committed or pushed. Its artifact is `final/supp/lf/results.json` together with `final/supp_gate.txt`, both of which the hand-off cites.

```bash
ENGINE=$(git -C ~/research/git/_wt/functions-run-pool rev-parse --short=7 HEAD)
git -C ~/research/git/vepyr fetch -q origin perf/plugin-csq-single-parse
git -C ~/research/git/vepyr worktree add --detach ~/research/git/_wt/vepyr-lf-supp FETCH_HEAD
cd ~/research/git/_wt/vepyr-lf-supp
sed -i '' "s/rev = \"[0-9a-f]*\", features = \[\"cache-builder\"\] }/rev = \"$ENGINE\", features = [\"cache-builder\"] }/" Cargo.toml
grep -n 'datafusion-bio-function-vep = ' Cargo.toml   # must show rev = "$ENGINE"
env -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr || exit 1
rm -rf target   # the extension is in src/vepyr; the target tree is ~1.6 GB of disk

SUPP=$ROOT/e2e-testing/results/fix-lf-run-pool/final/supp
.venv/bin/python $ROOT/performance-tests/vepyr/scripts/lf_run_pool_bench.py --out-dir $SUPP/lf \
  --input chr22=$DATA_VEPYR_DIR/polars_integration/chr22/input/HG002_chr22.vcf.gz \
  --input chr1=$DATA_VEPYR_DIR/polars_integration/lf_bench/chr1.vcf.gz \
  --cache-dir $DATA_VEPYR_DIR/cache/116_GRCh38_merged \
  --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
  --plugin-cache-root $DATA_VEPYR_DIR/plugin_cache_116 \
  --workers 4 8 --modes raw lf vcf --plugin-sets all --repeats 3 || exit 1
$ROOT/.venv/bin/python $ROOT/performance-tests/vepyr/scripts/lf_run_pool_bench.py --gate $SUPP/lf/results.json \
  --gate-plugins chr1:all --gate-expect chr22:all:raw,lf,vcf:4,8 --gate-expect chr1:all:raw,lf,vcf:4,8 \
  | tee $ROOT/e2e-testing/results/fix-lf-run-pool/final/supp_gate.txt
[ "${pipestatus[1]:-${PIPESTATUS[0]}}" -eq 0 ] || { echo "supplementary plugin gate FAILED"; exit 1; }
```

- [ ] **Step 3: Gates**

```bash
BASE=$ROOT/e2e-testing/results/fix-lf-run-pool/baseline
FINAL=$ROOT/e2e-testing/results/fix-lf-run-pool/final
uv run python "$ROOT/tools/vepyr-fix/compare_runs.py" "$BASE/archive" "$FINAL/archive" || { echo "WGS perf gate FAILED"; exit 1; }
.venv/bin/python performance-tests/vepyr/scripts/lf_run_pool_bench.py --gate "$FINAL/lf/results.json" \
  --gate-expect chr22:none,all:raw,lf,vcf:1,4,8 --gate-expect chr1:none,all:raw,lf,vcf:1,4,8 \
  --gate-baseline "$BASE/lf/results.json" \
  || { echo "LF gate FAILED"; exit 1; }
grep -iE "mismatch|concord" "$FINAL/md5.out" | tail -5
cat "$BASE/lf/summary.md" "$FINAL/lf/summary.md"
```

| Gate | Bar |
|---|---|
| md5 strict, 22 autosomes | every body digest matches |
| WGS output_vcf sweep (`compare_runs.py`) | its VERDICT passes (the sink path is untouched; this proves it) |
| LF success (core) | `lf end-to-end/vcf` at w8 ≤ 1.20 for chr22 none and chr1 none, on this branch |
| LF scaling | raw stream at w8 faster than at w4 for chr22 none, chr22 all, chr1 all, on this branch |
| LF success (plugins, supplementary) | `lf end-to-end/vcf` at w8 ≤ 1.20 for chr1 all, on a local build stacking vepyr #131 on this engine (reported, not part of either PR) |
| LF against baseline | no configuration's median wall more than 10% slower than at baseline (`--gate-baseline`) |
| RSS | reported in absolute GiB, baseline against final, not gated |

Ruling (user-approved during execution, 2026-09-26): vepyr master does not include #131, so on this branch the LF plugin consumer re-parses CSQ once per plugin field (14.6 s on chr22, 67 s on chr1 at baseline) and no engine change can bring `chr1 all` within 1.20. The plugin case is therefore gated on raw-stream scaling here, and its LF target is checked on a local build with #131 stacked. If any LF gate misses, report the numbers and stop. Do not tune the constants without the user: the spec fixes them.

---

### Task 9: Hand off (runbook step 9)

- [ ] **Step 1: Write the hand-off body**

`/tmp/handoff.md` contains:
- the baseline-against-final LF table for w1/w4/w8, chr22 and chr1, none and all, with `lf/vcf` ratios;
- the WGS `compare_runs.py` verdict;
- the md5 strict result;
- the parity reports (`final/parity_chr22`, `final/parity_chr1`, `final/regions`);
- the floor check result;
- the vepyr pin SHA;
- every bot finding and how it was answered;
- the post-merge actions: merge functions, then re-pin vepyr to the merge commit (then to a release tag once one is cut), then merge vepyr; after release, re-run Figure 1's LF curve and write its caption.

- [ ] **Step 2: Run the hand-off script**

```bash
bash "$ROOT/tools/vepyr-fix/handoff.sh" "$PRS" /tmp/handoff.md
```

- [ ] **Step 3: Update memory**

Update `vepyr-lf-consumer-ceiling-design.md` with the PR numbers, the final numbers, and "B′ next". Tell the user the PRs are ready and what is left for them. Do not merge.
