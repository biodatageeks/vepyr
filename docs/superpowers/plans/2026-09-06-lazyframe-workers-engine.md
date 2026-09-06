# LazyFrame Workers: Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `annotate_vep(...)` with `workers>1` on the streaming (RecordBatch) path run N annotation pipelines per contig and yield rows in exactly the order `workers=1` yields them, for Ensembl, Merged and RefSeq caches, with `regions` composed.

**Architecture:** A third execution mode in `ContigAnnotationStream`: when `annotation_workers > 1` and no VCF shard context is present, `prepare_contig_data` plans the contig as an ordered list of grid-aligned runs (the existing `ContigRun`/`WorkerGridSlice`/`RunGate` machinery), and a new `AnnotatingRunPool` state runs up to N run tasks concurrently, each reusing the sharded worker body behind an output enum that sends Arrow batches into a per-run channel instead of a VCF shard file. The pool drains the head run live and releases runs strictly in index order; admission is bounded to `workers + lookahead` runs.

**Tech Stack:** Rust 2021, DataFusion 53, Arrow, tokio (`spawn`, `mpsc::unbounded_channel`, `JoinHandle`), crate `datafusion-bio-function-vep` in `~/research/git/datafusion-bio-functions/datafusion/bio-function-vep`.

**Spec:** `docs/superpowers/specs/2026-09-06-lazyframe-parallel-workers-design.md` (in the vepyr repo). Read it first; the plan argues from it.

## Global Constraints

- Work in the engine clone `~/research/git/datafusion-bio-functions` on a new branch `feat/stream-run-pool` created from `origin/master` after `git fetch origin`. `origin/master` contains `dba94e3` (the current vepyr pin, region runs merged). The clone's working tree currently carries untracked scripts and index files; leave them alone.
- Every task's tests run with `cargo test -p datafusion-bio-function-vep` (`parquet-cache` is a default feature). `cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings` and `cargo fmt` must be clean before each commit.
- Contract: for every cache source, `workers=N` on the streaming path yields the same rows in the same order as `workers=1`, with and without `regions`, with and without a LIMIT.
- The sharded VCF sink path (`vcf_shard_ctx` is `Some`) must not change behaviour. Its tests and vepyr's `test_annotation_workers_preserves_vcf_output` guard this.
- The serial path (`annotation_workers <= 1`) must not run the count pass on Ensembl and must not change output. `ContigAnnotationState::inflight` stays at one.
- Memory in flight is at most `workers + lookahead` runs of output; the per-run channel is unbounded by design (bounded by run size).
- Environment knobs: `VEP_STREAM_RUN_BUFFERS` (run length in buffers) and `VEP_STREAM_LOOKAHEAD_RUNS` (admission lookahead). Changing either never changes output.
- Commit messages follow the repo's conventional style (`feat(vep): ...`, `test(vep): ...`, `refactor(vep): ...`) and end with the session trailer from the harness instructions.

---

## File structure

| File | Responsibility |
|---|---|
| `src/annotate_provider.rs` | Run-pool planning helpers (`stream_run_buffers`, `stream_lookahead_runs`, `plan_stream_runs`, `admission_allows`), `stream_parallel` in `prepare_contig_data`, shared lookup cell in `RunActivationInputs`, `RunOutput` + `annotate_lookup_run` (the generalised worker body), `RunTask`/`RunPoolState`/`spawn_run_task`, `StreamState::AnnotatingRunPool` and its poll arm, guard removal, profile fields. |
| `src/annotate_provider.rs` `mod tests` | Unit tests for planning, admission, the batches output. |
| `src/lib.rs` | Remove `pub(crate) mod ordered_drain;`. |
| `src/ordered_drain.rs` | Deleted (dead code the pool does not need). |

Reference points in `src/annotate_provider.rs` at `dba94e3` (line numbers drift as you edit; search for the quoted identifiers):

- `AnnotateProvider::new` regions guard: `"annotate_vep(): regions require workers=1"` (~3681-3694); its test `regions_with_workers_above_one_are_rejected_at_construction` (~15221).
- `scan`: `let requested_workers` (~5594), sink rejection `"regions are not supported with sharded VCF output"` (~5641).
- `ContigAnnotationConfig` (~9115): `target_partitions`, `annotation_workers`, `vcf_shard_ctx`, `regions`, `fetch_limit`, `projection`, `input_buffer_size`, `cache_source_type`.
- `ContigPipelineProfile` (~9457) and `summary_line` (~9506).
- `GridBufferBoundary` (~10344), `WorkerGridSlice` (~10357), `ContigRun` (~10374), `RunActivationInputs` (~10454), `ActiveRunState` (~10464), `plan_grid_partitions` (~10499), `build_grid_slices` (~10518).
- `ContigPreparedData` (~10763), `ContigReadyState` (~10798), `ContigAnnotationState` (~10822), `ParallelContigState` (~10860).
- `StreamState` enum (~10963), type aliases `PrepareFuture`/`CleanupFuture`/`ShardJoinFuture` (~10954-10961), `make_cleanup_future` (~10997).
- `spawn_lookup_partition_worker` (~11012), `run_maybe_block_in_place` (~11240), `ShardResult` + `spawn_annotation_from_lookup_sharded` (~11266-11421), `abort_lookup_partitions` (~11423).
- `ContigAnnotationStream` struct (~11478), `AbortOnDrop` (~11549), `start_prefetch_next_contig` (~11586), `abort_prefetch` (~11627).
- `annotate_worker_window` (~12633), `warm_up_worker_state` (~12611), `annotate_window_owned` (~12770).
- `poll_next`: `let fetch_limit = self.config.fetch_limit;` (~13096); `PreparingContig` arm `Poll::Ready(Ok(Some(mut ready)))` (~13175); the shard-context guard (~13178-13196); `AnnotatingParallel` arm (~13334); `DrainingWindow` arm (~13657); `CleaningUp` / `ErrorCleaningUp` arms (~13700-13735).
- `count_contig_buffer_boundaries` (~13757), `prepare_contig_data` (~13818): `stateful_parallel` (~13913), `stateful_runs` (~13925), `count_fut` (~13983), grid-slice planning (~14084), runs planning `let runs: VecDeque<ContigRun> = match contig_bounds` (~14147), `Ok(Some(ContigPreparedData {` (~14259).
- `activate_run_lookup` (~14274), `activate_contig_lookups` (~14382): `run_inputs = RunActivationInputs {` (~14425), `shared_parquet_lookup_cell` (~14457), `wprovider.set_parquet_lookup_cell` (~14510).
- `contig_prefetch_enabled` (~14743) and the start of `mod tests` (~14751); test helper `gb(global_row, pos)` used by `plan_two_workers_even_split_no_overlap` (~16969).

---

### Task 1: Run planning helpers

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (new functions next to `plan_grid_partitions`; tests next to `plan_two_workers_even_split_no_overlap`)

**Interfaces:**
- Produces:
  ```rust
  const STREAM_RUNS_PER_WORKER: usize = 4;
  const STREAM_MIN_RUN_BUFFERS_STATEFUL: usize = 4;
  fn stream_run_buffers(buffers: usize, workers: usize, stateful: bool, env: Option<&str>) -> usize
  fn stream_lookahead_runs(workers: usize, env: Option<&str>) -> usize
  fn plan_stream_runs(
      boundaries: &[GridBufferBoundary],
      ranges: &[(Vec<RunBounds>, (usize, usize))],
      run_buffers: usize,
      overlap_width_bp: i64,
  ) -> Vec<ContigRun>
  fn admission_allows(running: usize, head: usize, next_start: usize, total_runs: usize, workers: usize, lookahead: usize) -> bool
  ```

- [ ] **Step 1: Create the branch**

```bash
cd ~/research/git/datafusion-bio-functions
git fetch origin
git switch -c feat/stream-run-pool origin/master
git log --oneline -1
```

Expected: the head is `dba94e3` or a later master commit.

- [ ] **Step 2: Write the failing tests**

Add inside `mod tests` in `annotate_provider.rs`, after `plan_skip_leading_for_tie`:

```rust
    #[test]
    fn stream_run_buffers_default_and_floor() {
        // 65 buffers, 8 workers: ceil(65 / 32) = 3, floored to 4 on stateful.
        assert_eq!(stream_run_buffers(65, 8, true, None), 4);
        assert_eq!(stream_run_buffers(65, 8, false, None), 3);
        // Small contig: at least one buffer per run.
        assert_eq!(stream_run_buffers(3, 8, false, None), 1);
        assert_eq!(stream_run_buffers(3, 8, true, None), 4);
        // Override wins but not below the stateful floor.
        assert_eq!(stream_run_buffers(65, 8, false, Some("2")), 2);
        assert_eq!(stream_run_buffers(65, 8, true, Some("2")), 4);
        assert_eq!(stream_run_buffers(65, 8, true, Some("9")), 9);
        // Garbage override is ignored.
        assert_eq!(stream_run_buffers(65, 8, false, Some("x")), 3);
        assert_eq!(stream_run_buffers(65, 8, false, Some("0")), 3);
    }

    #[test]
    fn stream_lookahead_defaults_to_workers() {
        assert_eq!(stream_lookahead_runs(8, None), 8);
        assert_eq!(stream_lookahead_runs(8, Some("2")), 2);
        assert_eq!(stream_lookahead_runs(8, Some("0")), 0);
        assert_eq!(stream_lookahead_runs(8, Some("bad")), 8);
    }

    #[test]
    fn plan_stream_runs_cuts_open_range_into_fixed_pieces() {
        // 5 buffers of 5000 rows.
        let bs = vec![
            gb(0, 0),
            gb(5000, 100),
            gb(10000, 200),
            gb(15000, 300),
            gb(20000, 400),
            gb(25000, i64::MAX),
        ];
        let ranges = vec![(vec![RunBounds::OPEN], (0usize, 5usize))];
        let runs = plan_stream_runs(&bs, &ranges, 2, 0);
        assert_eq!(runs.len(), 3, "5 buffers in pieces of 2 -> 2+2+1");
        let s = |i: usize| runs[i].slice.as_ref().expect("stream runs carry a slice");
        assert_eq!((s(0).emit_start_row, s(0).emit_end_row), (0, 10000));
        assert_eq!((s(1).emit_start_row, s(1).emit_end_row), (10000, 20000));
        assert_eq!((s(2).emit_start_row, s(2).emit_end_row), (20000, 25000));
        // overlap 0: no warm-up, scan floor == seam.
        assert_eq!(s(1).warm_up_start_row, 10000);
        assert_eq!(s(1).scan_lo_pos, 200);
        assert_eq!(s(1).scan_hi_pos, 401, "non-last piece scans one past its end seam");
        assert_eq!(s(2).scan_hi_pos, i64::MAX, "last piece reads to the contig end");
        assert!(runs.iter().all(|r| r.bounds == vec![RunBounds::OPEN]));
        assert!(runs[1].probe_floor().is_none(), "no warm-up -> no probe floor");
    }

    #[test]
    fn plan_stream_runs_keeps_region_bounds_and_warm_up() {
        let bs = vec![
            gb(0, 0),
            gb(5000, 100),
            gb(10000, 200),
            gb(15000, 300),
            gb(20000, 400),
            gb(25000, i64::MAX),
        ];
        let b1 = RunBounds { lo: Some(150), hi: Some(320) };
        // Region maps to buffers [1, 4): three buffers, run length 1 -> three runs.
        let ranges = vec![(vec![b1], (1usize, 4usize))];
        let runs = plan_stream_runs(&bs, &ranges, 1, 150);
        assert_eq!(runs.len(), 3);
        for r in &runs {
            assert_eq!(r.bounds, vec![b1], "every piece trims to the region bounds");
        }
        let s1 = runs[1].slice.as_ref().unwrap();
        assert_eq!(s1.emit_start_row, 10000);
        assert_eq!(s1.warm_up_start_row, 5000, "overlap 150 reaches back one buffer");
        assert_eq!(runs[1].probe_floor(), Some(200));
    }

    #[test]
    fn plan_stream_runs_empty_grid_plans_nothing() {
        let bs = vec![gb(0, i64::MAX)];
        let ranges = vec![(vec![RunBounds::OPEN], (0usize, 0usize))];
        assert!(plan_stream_runs(&bs, &ranges, 4, 0).is_empty());
    }

    #[test]
    fn admission_bounds_workers_and_lookahead() {
        // workers=2, lookahead=1: at most 3 runs from head onwards.
        assert!(admission_allows(0, 0, 0, 10, 2, 1));
        assert!(admission_allows(1, 0, 1, 10, 2, 1));
        assert!(!admission_allows(2, 0, 2, 10, 2, 1), "both workers busy");
        assert!(!admission_allows(1, 0, 3, 10, 2, 1), "lookahead exhausted");
        assert!(admission_allows(1, 1, 3, 10, 2, 1), "head advanced -> window moves");
        assert!(!admission_allows(0, 9, 10, 10, 2, 1), "no runs left");
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep stream_ 2>&1 | tail -20`
Expected: compile errors, `stream_run_buffers` and friends not found.

- [ ] **Step 4: Implement the helpers**

Add after `build_grid_slices`:

```rust
/// Runs per worker per contig on the streaming run pool: enough pieces that
/// the ordered release does not idle on one straggler, few enough that the
/// per-run lookup activation and (on stateful sources) warm-up stay small.
const STREAM_RUNS_PER_WORKER: usize = 4;
/// Every seam on Merged/RefSeq replays about `overlap_width_bp` of input
/// (about one buffer at whole-genome density); four-buffer runs keep that
/// replay a small share of the run's own work.
const STREAM_MIN_RUN_BUFFERS_STATEFUL: usize = 4;

/// Run length in whole buffers for a contig of `buffers` buffers. `env` is
/// `VEP_STREAM_RUN_BUFFERS`; it overrides the formula but never the stateful
/// floor. Scheduling only: the run cut never changes output.
fn stream_run_buffers(buffers: usize, workers: usize, stateful: bool, env: Option<&str>) -> usize {
    let floor = if stateful {
        STREAM_MIN_RUN_BUFFERS_STATEFUL
    } else {
        1
    };
    let from_env = env.and_then(|v| v.trim().parse::<usize>().ok()).filter(|n| *n > 0);
    let chosen = from_env.unwrap_or_else(|| {
        let target_runs = workers.max(1) * STREAM_RUNS_PER_WORKER;
        buffers.div_ceil(target_runs).max(1)
    });
    chosen.max(floor)
}

/// Extra runs the pool may start beyond the `workers` running ones, so a
/// worker that finishes early is not idle while the head run straggles. `env`
/// is `VEP_STREAM_LOOKAHEAD_RUNS`; default `workers`.
fn stream_lookahead_runs(workers: usize, env: Option<&str>) -> usize {
    env.and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(workers.max(1))
}

/// Cut each `(bounds, [bk, bk1))` buffer range into consecutive pieces of
/// `run_buffers` whole buffers (the last piece may be shorter) and build one
/// grid-aligned run per piece. `overlap_width_bp = 0` yields runs with no
/// warm-up (stateless Ensembl). Runs come out in grid order, which is the
/// release order of the pool.
fn plan_stream_runs(
    boundaries: &[GridBufferBoundary],
    ranges: &[(Vec<RunBounds>, (usize, usize))],
    run_buffers: usize,
    overlap_width_bp: i64,
) -> Vec<ContigRun> {
    let step = run_buffers.max(1);
    let mut runs = Vec::new();
    for (bounds, (bk, bk1)) in ranges {
        if bk >= bk1 {
            continue;
        }
        let mut cuts: Vec<usize> = (*bk..*bk1).step_by(step).collect();
        cuts.push(*bk1);
        for slice in build_grid_slices(boundaries, &cuts, overlap_width_bp) {
            runs.push(ContigRun {
                bounds: bounds.clone(),
                slice: Some(slice),
            });
        }
    }
    runs
}

/// Whether the pool may start run `next_start`: a worker slot is free and the
/// run lies within the release window `[head, head + workers + lookahead)`.
/// The window bounds the runs whose output can be resident at once.
fn admission_allows(
    running: usize,
    head: usize,
    next_start: usize,
    total_runs: usize,
    workers: usize,
    lookahead: usize,
) -> bool {
    next_start < total_runs && running < workers.max(1) && next_start < head + workers.max(1) + lookahead
}
```

`build_grid_slices` already skips empty pieces and sets `scan_hi_pos` to `i64::MAX` only for the contig's last buffer, which is what the tests assert.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep -- stream_ admission_ plan_stream 2>&1 | tail -20`
Expected: all six new tests pass (libtest accepts several filters after `--`).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): plan grid-aligned runs for the streaming run pool"
```

---

### Task 2: `stream_parallel` planning in `prepare_contig_data` and the shared lookup cell

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`prepare_contig_data`, `RunActivationInputs`, `activate_run_lookup`, `activate_contig_lookups`)

**Interfaces:**
- Consumes: Task 1 helpers.
- Produces: in `stream_parallel` mode, `ContigPreparedData.runs` holds the pool's runs in grid order, `ContigPreparedData.grid_slices` is empty, `stateful_parallel` is false, `config.target_partitions == 1`. `RunActivationInputs` gains
  ```rust
  #[cfg(feature = "parquet-cache")]
  parquet_lookup_cell: Arc<crate::lookup_provider::ParquetVariationLookupCell>,
  ```
  (use whatever type alias `LookupProvider::set_parquet_lookup_cell` takes; it is the `Arc<tokio::sync::OnceCell<...>>` created in `activate_contig_lookups`.)

- [ ] **Step 1: Write the failing test** — the mode flags as a pure function

Refactor the two `matches!` booleans into one helper so the mode selection is testable. Add the test next to `stream_run_buffers_default_and_floor`:

```rust
    #[test]
    fn contig_modes_are_exclusive() {
        let m = |workers: usize, sink: bool, source: CacheSourceType, regions: bool| {
            contig_modes(true, workers, sink, source, regions)
        };
        // Serial.
        assert_eq!(m(1, false, CacheSourceType::Ensembl, false), ContigModes { stream_parallel: false, stateful_parallel: false, stateful_runs: false });
        assert_eq!(m(1, false, CacheSourceType::Merged, true), ContigModes { stream_parallel: false, stateful_parallel: false, stateful_runs: true });
        // Sink.
        assert_eq!(m(4, true, CacheSourceType::Merged, false), ContigModes { stream_parallel: false, stateful_parallel: true, stateful_runs: false });
        assert_eq!(m(4, true, CacheSourceType::Ensembl, false), ContigModes { stream_parallel: false, stateful_parallel: false, stateful_runs: false });
        // Streaming pool: never stateful_parallel, never stateful_runs (the pool
        // plans regions itself), for every source.
        assert_eq!(m(4, false, CacheSourceType::Merged, true), ContigModes { stream_parallel: true, stateful_parallel: false, stateful_runs: false });
        assert_eq!(m(4, false, CacheSourceType::Ensembl, false), ContigModes { stream_parallel: true, stateful_parallel: false, stateful_runs: false });
        // No cache root: nothing parallel.
        assert_eq!(contig_modes(false, 4, false, CacheSourceType::Merged, true), ContigModes { stream_parallel: false, stateful_parallel: false, stateful_runs: false });
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep contig_modes_are_exclusive 2>&1 | tail -5`
Expected: compile error, `contig_modes` not found.

- [ ] **Step 3: Implement `contig_modes` and use it in `prepare_contig_data`**

Add above `prepare_contig_data`:

```rust
/// Which planning arms a contig takes. Exactly one of the parallel flags can
/// be set; `stateful_runs` is the serial region path on Merged/RefSeq.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ContigModes {
    /// `workers>1` without a VCF shard context: the streaming run pool.
    stream_parallel: bool,
    /// `workers>1` with a shard context on Merged/RefSeq: grid slices for the sink.
    stateful_parallel: bool,
    /// Serial region runs on Merged/RefSeq: grid-aligned with warm-up.
    stateful_runs: bool,
}

fn contig_modes(
    cache_enabled: bool,
    annotation_workers: usize,
    has_shard_ctx: bool,
    cache_source_type: CacheSourceType,
    has_regions: bool,
) -> ContigModes {
    let stateful = matches!(
        cache_source_type,
        CacheSourceType::Merged | CacheSourceType::RefSeq
    );
    let parallel = cache_enabled && annotation_workers > 1;
    let stream_parallel = parallel && !has_shard_ctx;
    ContigModes {
        stream_parallel,
        stateful_parallel: parallel && has_shard_ctx && stateful,
        stateful_runs: cache_enabled && has_regions && stateful && !stream_parallel,
    }
}
```

In `prepare_contig_data`, replace the `stateful_parallel` and `stateful_runs` bindings (search `let stateful_parallel = cache_enabled`) with:

```rust
    let contig_bounds: Option<Vec<RunBounds>> = config
        .regions
        .as_deref()
        .and_then(|runs| runs.get(&chrom).cloned());
    let ContigModes {
        stream_parallel,
        stateful_parallel,
        stateful_runs,
    } = contig_modes(
        cache_enabled,
        config.annotation_workers,
        config.vcf_shard_ctx.is_some(),
        config.cache_source_type,
        contig_bounds.is_some(),
    );
    if stream_parallel {
        // Parallelism comes from runs; each run's lookup is one ordered
        // partition so the run task needs no fan-in.
        config.target_partitions = 1;
    }
```

(`contig_bounds` is currently bound between the two flags; keep one binding.) Then make the count pass run for the pool too: change `if stateful_parallel || stateful_runs {` in `count_fut` to `if stateful_parallel || stateful_runs || stream_parallel {`.

Replace the runs planning block (`let runs: VecDeque<ContigRun> = match contig_bounds {`) with:

```rust
    let runs: VecDeque<ContigRun> = if stream_parallel {
        let (boundaries, _total_rows, _positions) = grid_count
            .take()
            .expect("stream_parallel implies the count future ran")?;
        let stateful = matches!(
            config.cache_source_type,
            CacheSourceType::Merged | CacheSourceType::RefSeq
        );
        // Ensembl has no cross-buffer state: a plain cut is exact, so its
        // runs get no warm-up. Merged/RefSeq runs replay `overlap_width_bp`.
        let overlap = if stateful { overlap_width_bp } else { 0 };
        let b = boundaries.len().saturating_sub(1);
        let positions: Vec<i64> = boundaries.iter().map(|bd| bd.pos).collect();
        let ranges: Vec<(Vec<RunBounds>, (usize, usize))> = match &contig_bounds {
            None => vec![(vec![RunBounds::OPEN], (0, b))],
            Some(bounds) => crate::regions::plan_runs(bounds, Some(&positions))
                .into_iter()
                .map(|plan| {
                    let buffers = plan.buffers.expect("grid path plans buffer ranges");
                    (plan.bounds, buffers)
                })
                .collect(),
        };
        let run_buffers = stream_run_buffers(
            b,
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
                ("run_buffers", TraceValue::Usize(run_buffers)),
                ("runs", TraceValue::Usize(planned.len())),
                ("warm_up", TraceValue::Usize(usize::from(overlap > 0))),
            ],
        );
        VecDeque::from(planned)
    } else {
        match contig_bounds {
            // ... the existing three arms, unchanged ...
        }
    };
```

`overlap_width_bp` is computed earlier in the function (`compute_overlap_width_bp`); confirm it is in scope where the runs are planned (it is used by the grid-slice planning just above).

- [ ] **Step 4: Share the Parquet lookup cell across runs**

In `RunActivationInputs` add the field from the Interfaces block. In `activate_contig_lookups`, create the cell before `run_inputs` is built and put it in:

```rust
    // One single-flight cell per contig: every run's lookup shares the decoded
    // shard footer + page index (~0.5 GB on chr1) instead of decoding it again.
    #[cfg(feature = "parquet-cache")]
    let shared_parquet_lookup_cell = Arc::new(tokio::sync::OnceCell::new());
    let run_inputs = RunActivationInputs {
        var_table: var_table.clone(),
        vcf_schema,
        cache_schema,
        fallback_coloc_sink,
        #[cfg(feature = "parquet-cache")]
        parquet_lookup_cell: Arc::clone(&shared_parquet_lookup_cell),
    };
```

Delete the later `let shared_parquet_lookup_cell = Arc::new(tokio::sync::OnceCell::new());` inside the `if stateful_parallel` block so the grid slices use the same cell. In `activate_run_lookup`, after `provider.set_parquet_backend(true)` (inside the `if let Some(root) = &config.cache_root` block) add:

```rust
        provider.set_parquet_lookup_cell(Arc::clone(&inputs.parquet_lookup_cell));
```

Destructure `parquet_lookup_cell` out of `inputs` at the top of `activate_run_lookup` alongside the existing fields (the `let RunActivationInputs { .. } = inputs;` pattern), under `#[cfg(feature = "parquet-cache")]`.

- [ ] **Step 5: Run the suite**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: everything passes, including `contig_modes_are_exclusive`. The serial and sink arms are byte-for-byte the previous logic (verify by reading the diff of `prepare_contig_data` once: only the flag bindings, the `count_fut` condition, the `target_partitions` line and the new `if stream_parallel` arm changed).

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): plan streaming run-pool contigs and share the lookup cell across runs"
```

---

### Task 3: Generalise the sharded worker body over its output

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`spawn_annotation_from_lookup_sharded` and neighbours)

**Interfaces:**
- Produces:
  ```rust
  enum RunOutput {
      Shard { shard: crate::vcf_sink::VcfBodyShardWriter, rows_done: Arc<std::sync::atomic::AtomicUsize> },
      Batches { tx: tokio::sync::mpsc::UnboundedSender<RecordBatch> },
  }
  impl RunOutput {
      fn write(&mut self, batch: RecordBatch) -> Result<()>;
      fn window_done(&self, input_rows: usize);
  }
  #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
  struct RunStats { input_rows: usize, output_rows: usize, warm_up_rows: usize }
  async fn annotate_lookup_run(
      shared: Arc<SharedContigAnnotationContext>,
      lookup_rx: tokio::sync::mpsc::Receiver<Result<LookupBatchMessage>>,
      cache_source_type: CacheSourceType,
      projection: Option<Vec<usize>>,
      input_buffer_size: usize,
      slice: Option<WorkerGridSlice>,
      emit_bounds: Option<Vec<RunBounds>>,
      output: &mut RunOutput,
  ) -> Result<RunStats>
  ```
  `spawn_annotation_from_lookup_sharded` keeps its signature and return type.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn run_output_batches_forwards_in_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]));
        let batch = |tag: i64| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![tag]))],
            )
            .unwrap()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut out = RunOutput::Batches { tx };
        out.write(batch(1)).unwrap();
        out.write(batch(2)).unwrap();
        out.window_done(7); // no-op for batches
        drop(out);
        let mut tags = Vec::new();
        while let Ok(b) = rx.try_recv() {
            tags.push(
                b.column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap()
                    .value(0),
            );
        }
        assert_eq!(tags, vec![1, 2]);
        assert!(rx.try_recv().is_err(), "sender dropped -> channel closed");
    }

    #[test]
    fn run_output_batches_errors_when_receiver_is_gone() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let mut out = RunOutput::Batches { tx };
        let err = out.write(batch).expect_err("dropped receiver must surface");
        assert!(err.to_string().contains("run pool"), "{err}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep run_output_batches 2>&1 | tail -5`
Expected: compile error, `RunOutput` not found.

- [ ] **Step 3: Implement `RunOutput`, `RunStats`, `annotate_lookup_run`; make the sharded spawn a wrapper**

Insert before `spawn_annotation_from_lookup_sharded`:

```rust
/// Where a run's annotated batches go: the VCF sink's body shard, or the
/// streaming run pool's per-run channel. The worker body is shared, so the
/// two paths cannot drift.
enum RunOutput {
    Shard {
        shard: crate::vcf_sink::VcfBodyShardWriter,
        rows_done: Arc<std::sync::atomic::AtomicUsize>,
    },
    Batches {
        tx: tokio::sync::mpsc::UnboundedSender<RecordBatch>,
    },
}

impl RunOutput {
    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            // `write_batch` returns `Result<()>` today; if it returns a count,
            // map it to `()` here rather than changing the writer.
            RunOutput::Shard { shard, .. } => shard.write_batch(batch),
            RunOutput::Batches { tx } => tx.send(batch).map_err(|_| {
                DataFusionError::Execution(
                    "run pool output receiver dropped before the run finished".to_string(),
                )
            }),
        }
    }

    /// Progress accounting after a window: the sink's live progress bar reads
    /// `rows_done`; the pool has no such counter.
    fn window_done(&self, input_rows: usize) {
        if let RunOutput::Shard { rows_done, .. } = self {
            rows_done.fetch_add(input_rows, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Per-run counters reported by `annotate_lookup_run`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RunStats {
    /// Emit-range input rows annotated.
    input_rows: usize,
    /// Annotated rows written to the output.
    output_rows: usize,
    /// Warm-up rows replayed state-only (Merged/RefSeq seams).
    warm_up_rows: usize,
}

/// The fused lookup -> gate -> warm-up -> hydrate -> annotate loop of one run
/// (formerly the body of `spawn_annotation_from_lookup_sharded`). Drains one
/// ordered lookup partition, applies the grid slice (skip ties, warm-up
/// replay, rank-stop), cuts windows of `input_buffer_size` input units, and
/// writes every annotated batch to `output`. `emit_bounds` trims each window
/// to a region's intervals (`None` = no trim).
async fn annotate_lookup_run(
    shared: Arc<SharedContigAnnotationContext>,
    mut lookup_rx: tokio::sync::mpsc::Receiver<Result<LookupBatchMessage>>,
    cache_source_type: CacheSourceType,
    projection: Option<Vec<usize>>,
    input_buffer_size: usize,
    slice: Option<WorkerGridSlice>,
    emit_bounds: Option<Vec<RunBounds>>,
    output: &mut RunOutput,
) -> Result<RunStats> {
    let mut worker = AnnotationWorkerState::new(shared)?;
    let mut stats = RunStats::default();
    // ... move the existing loop here verbatim, with these substitutions:
    //   `shard.write_batch(b)?`                -> `stats.output_rows += b.num_rows(); output.write(b)?;`
    //   `shard_ctx.rows_done.fetch_add(window_input_rows, Relaxed)`
    //                                          -> `stats.input_rows += window_input_rows; output.window_done(window_input_rows);`
    //   `annotate_worker_window(&mut worker, &window, projection.as_deref(), None)`
    //                                          -> `annotate_worker_window(&mut worker, &window, projection.as_deref(), emit_bounds.as_deref())`
    //   before each `warm_up_worker_state(&mut worker, wbatches)`:
    //                                             `stats.warm_up_rows += wbatches.iter().map(RecordBatch::num_rows).sum::<usize>();`
    // Drop the `[VEP_RSS] shard partition done` eprintln and the
    // `let _ = (emit_start, emit_end, global_row);` line from here (the
    // wrapper prints the RSS line).
    Ok(stats)
}
```

Then rewrite `spawn_annotation_from_lookup_sharded` as the wrapper:

```rust
fn spawn_annotation_from_lookup_sharded(
    shared: Arc<SharedContigAnnotationContext>,
    lookup_rx: tokio::sync::mpsc::Receiver<Result<LookupBatchMessage>>,
    cache_source_type: CacheSourceType,
    projection: Option<Vec<usize>>,
    input_buffer_size: usize,
    shard_ctx: Arc<crate::vcf_sink::VcfShardContext>,
    shard_path: std::path::PathBuf,
    slice: Option<WorkerGridSlice>,
) -> tokio::task::JoinHandle<Result<ShardResult>> {
    tokio::spawn(async move {
        let shard = crate::vcf_sink::VcfBodyShardWriter::create(
            &shard_path,
            Arc::clone(&shard_ctx.vcf_info_fields),
            Arc::clone(&shard_ctx.unique_format_tags),
            Arc::clone(&shard_ctx.sample_names),
            shard_ctx.coordinate_zero_based,
            shard_ctx.shard_compression,
        )?;
        let mut output = RunOutput::Shard {
            shard,
            rows_done: Arc::clone(&shard_ctx.rows_done),
        };
        annotate_lookup_run(
            shared,
            lookup_rx,
            cache_source_type,
            projection,
            input_buffer_size,
            slice,
            None,
            &mut output,
        )
        .await?;
        let RunOutput::Shard { shard, .. } = output else {
            unreachable!("sharded output is a shard")
        };
        if profiling_enabled() {
            eprintln!(
                "[VEP_RSS] shard partition done shard_rows={} peak_rss={}MB",
                shard.input_rows,
                peak_rss_mb(),
            );
        }
        let input_rows = shard.input_rows;
        let output_lines = shard.lines;
        shard.finish()?;
        Ok(ShardResult {
            input_rows,
            output_lines,
        })
    })
}
```

The original RSS line also printed `worker.colocated_map.len()`; the worker now lives inside `annotate_lookup_run`, so that field is dropped from the line (it is diagnostic only). Keep `ShardResult` computed from the writer's own counters exactly as before, so the sink's row accounting is unchanged.

- [ ] **Step 4: Run the suite**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: the two new tests pass; the `vcf_sink` tests still pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "refactor(vep): share the sharded worker body behind a run output enum"
```

---

### Task 4: `RunPoolState`, `RunTask` and `spawn_run_task`

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (new types next to `ParallelContigState`; new `StreamState` variant)

**Interfaces:**
- Consumes: `RunOutput::Batches`, `annotate_lookup_run`, `RunStats` (Task 3); `activate_run_lookup`, `RunActivationInputs` (Task 2); `admission_allows`, `stream_lookahead_runs` (Task 1).
- Produces:
  ```rust
  struct RunTask { join: Option<JoinHandle<Result<RunStats>>>, rx: UnboundedReceiver<RecordBatch>, stats: Option<RunStats>, started: Instant }
  impl RunTask { fn running(&self) -> bool; fn poll_join(&mut self, cx: &mut TaskCtx<'_>) -> Poll<Result<()>> }
  struct RunPoolState { .. }  // fields below
  impl RunPoolState { fn running(&self) -> usize; fn admit(&mut self); fn abort(&mut self) }
  fn spawn_run_task(pool: &RunPoolState, index: usize, run: ContigRun, preactivated: Option<VecDeque<LookupPartitionHandle>>) -> RunTask
  StreamState::AnnotatingRunPool(RunPoolState)
  ```

- [ ] **Step 1: Write the failing test** — a pool with fake finished tasks admits and releases in order

This test exercises `RunTask::running`, `RunPoolState::running` and `admit`'s use of `admission_allows` without a real contig, by building `RunTask`s whose join handle is an already-resolved `tokio::spawn`:

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_task_reports_running_until_joined() {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<RecordBatch>();
        let join = tokio::spawn(async { Ok(RunStats { input_rows: 3, output_rows: 4, warm_up_rows: 0 }) });
        let mut task = RunTask {
            join: Some(join),
            rx,
            stats: None,
            started: Instant::now(),
        };
        assert!(task.running());
        // Poll until the spawned task has resolved.
        let stats = std::future::poll_fn(|cx| task.poll_join(cx)).await;
        stats.expect("join ok");
        assert!(!task.running());
        assert_eq!(task.stats.unwrap().output_rows, 4);
        // A second poll is a no-op.
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(task.poll_join(cx))).await,
            std::task::Poll::Ready(Ok(()))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_task_surfaces_task_errors() {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<RecordBatch>();
        let join = tokio::spawn(async {
            Err::<RunStats, _>(DataFusionError::Execution("boom".to_string()))
        });
        let mut task = RunTask { join: Some(join), rx, stats: None, started: Instant::now() };
        let err = std::future::poll_fn(|cx| task.poll_join(cx)).await.expect_err("error surfaces");
        assert!(err.to_string().contains("boom"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep run_task_ 2>&1 | tail -5`
Expected: compile error, `RunTask` not found.

- [ ] **Step 3: Implement the types**

Add after `ParallelContigState`'s `impl`:

```rust
/// Guard that aborts a lookup worker when its run task ends or is aborted, so
/// no lookup outlives its consumer.
struct AbortJoinOnDrop(tokio::task::JoinHandle<Result<()>>);

impl Drop for AbortJoinOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One run of the streaming pool: its task, its output channel and, once the
/// task has finished, its stats. Dropping it aborts the task.
struct RunTask {
    join: Option<tokio::task::JoinHandle<Result<RunStats>>>,
    rx: tokio::sync::mpsc::UnboundedReceiver<RecordBatch>,
    stats: Option<RunStats>,
    started: Instant,
}

impl RunTask {
    fn running(&self) -> bool {
        self.stats.is_none()
    }

    /// Poll the task's completion. `Ready(Ok(()))` once its stats are stored
    /// (and on every later call); errors and panics come back as errors.
    fn poll_join(&mut self, cx: &mut TaskCtx<'_>) -> Poll<Result<()>> {
        let Some(join) = self.join.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(join).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(stats))) => {
                self.stats = Some(stats);
                self.join = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(join_err)) => {
                Poll::Ready(Err(DataFusionError::External(Box::new(join_err))))
            }
        }
    }
}

impl Drop for RunTask {
    fn drop(&mut self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

/// State of the `workers>1` streaming path: a pool of run tasks over one
/// contig's grid-ordered runs. The head run's batches are forwarded live;
/// later runs buffer in their channels until released in index order.
struct RunPoolState {
    /// Runs not yet started, in grid order; `next_start` is the index of the
    /// front element.
    pending: VecDeque<ContigRun>,
    /// Lookup partitions of run 0, already activated by
    /// `activate_contig_lookups`; taken when run 0 is spawned.
    first_lookup: Option<VecDeque<LookupPartitionHandle>>,
    total_runs: usize,
    next_start: usize,
    /// Index of the run whose output is being released.
    head: usize,
    /// Started runs not yet released, by index.
    active: HashMap<usize, RunTask>,
    workers: usize,
    lookahead: usize,
    run_inputs: RunActivationInputs,
    chrom: String,
    config: ContigAnnotationConfig,
    session: Arc<SessionContext>,
    shared: Arc<SharedContigAnnotationContext>,
    ephemeral_tables: Vec<String>,
    /// See `ContigReadyState::t_contig_active`.
    t_contig_active: Instant,
    pipeline_profile: Option<SharedContigPipelineProfile>,
    contig_rows: usize,
    /// Set while the head has no batch ready but a later run has finished:
    /// the time the ordered release spends waiting on a straggling head.
    head_wait_started: Option<Instant>,
}

impl RunPoolState {
    fn running(&self) -> usize {
        self.active.values().filter(|t| t.running()).count()
    }

    /// Start runs while a worker slot is free and the release window allows.
    fn admit(&mut self) {
        while admission_allows(
            self.running(),
            self.head,
            self.next_start,
            self.total_runs,
            self.workers,
            self.lookahead,
        ) {
            let index = self.next_start;
            let run = self
                .pending
                .pop_front()
                .expect("pending runs cover every index below total_runs");
            let preactivated = if index == 0 {
                self.first_lookup.take()
            } else {
                None
            };
            let task = spawn_run_task(self, index, run, preactivated);
            self.active.insert(index, task);
            self.next_start += 1;
        }
    }

    /// Abort every task and lookup (error, LIMIT, or drop).
    fn abort(&mut self) {
        self.active.clear(); // RunTask::drop aborts the task, which drops its lookup guard
        self.pending.clear();
        self.first_lookup = None; // LookupPartitionHandle::drop aborts run 0's lookup
    }
}

/// Spawn the task for run `index`: activate its lookup (unless run 0's is
/// handed in), then run the shared worker body into the run's channel.
fn spawn_run_task(
    pool: &RunPoolState,
    index: usize,
    run: ContigRun,
    preactivated: Option<VecDeque<LookupPartitionHandle>>,
) -> RunTask {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let session = Arc::clone(&pool.session);
    let inputs = pool.run_inputs.clone();
    let config = pool.config.clone();
    let chrom = pool.chrom.clone();
    let shared = Arc::clone(&pool.shared);
    let profile = pool.pipeline_profile.clone();
    let join = tokio::spawn(async move {
        let t_start = Instant::now();
        pipeline_trace::emit(
            "run_pool",
            "start",
            &[
                ("chrom", TraceValue::Str(&chrom)),
                ("run", TraceValue::Usize(index)),
            ],
        );
        let mut handles = match preactivated {
            Some(handles) => handles,
            None => {
                activate_run_lookup(
                    session,
                    inputs,
                    config.clone(),
                    chrom.clone(),
                    run.clone(),
                    profile,
                )
                .await?
            }
        };
        if handles.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "streaming run pool expects one lookup partition per run, got {}",
                handles.len()
            )));
        }
        let mut handle = handles.pop_front().expect("one handle");
        let Some((lookup_rx, lookup_join)) = handle.take_spawned_parts() else {
            return Err(DataFusionError::Internal(
                "streaming run pool requires spawned lookup partitions".to_string(),
            ));
        };
        let _lookup_guard = AbortJoinOnDrop(lookup_join);
        let emit_bounds = if run.bounds.iter().all(RunBounds::is_open) {
            None
        } else {
            Some(run.bounds.clone())
        };
        let mut output = RunOutput::Batches { tx };
        let stats = annotate_lookup_run(
            shared,
            lookup_rx,
            config.cache_source_type,
            config.projection.clone(),
            config.input_buffer_size,
            run.slice.clone(),
            emit_bounds,
            &mut output,
        )
        .await?;
        pipeline_trace::emit(
            "run_pool",
            "done",
            &[
                ("chrom", TraceValue::Str(&chrom)),
                ("run", TraceValue::Usize(index)),
                ("input_rows", TraceValue::Usize(stats.input_rows)),
                ("output_rows", TraceValue::Usize(stats.output_rows)),
                ("warm_up_rows", TraceValue::Usize(stats.warm_up_rows)),
                ("elapsed", TraceValue::Duration(t_start.elapsed())),
            ],
        );
        Ok(stats)
    });
    RunTask {
        join: Some(join),
        rx,
        stats: None,
        started: Instant::now(),
    }
}
```

Add the variant to `StreamState`:

```rust
    /// `workers>1` streaming path: the run pool (see `RunPoolState`).
    AnnotatingRunPool(RunPoolState),
```

`ContigRun` needs `#[derive(Clone)]` if it does not have one (`run.clone()` above); `WorkerGridSlice` and `RunBounds` already derive `Clone`. `TaskCtx` is the alias the file already uses for `std::task::Context`.

Until Task 5 wires the arm, `match self.state` in `poll_next` must compile: add a temporary arm `StreamState::AnnotatingRunPool(_) => unreachable!("wired in the next commit")`. Task 5 replaces it.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datafusion-bio-function-vep run_task_ 2>&1 | tail -8`
Expected: both pass. Then `cargo test -p datafusion-bio-function-vep 2>&1 | tail -5` still green.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): run-pool task and state types for the streaming path"
```

---

### Task 5: The `AnnotatingRunPool` arm, mode entry, LIMIT and errors

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`poll_next`: `PreparingContig` arm and the new arm; `ContigPipelineProfile`; `summary_line`)

**Interfaces:**
- Consumes: Task 4 types.
- Produces: `fn fail_run_pool(&mut self, pool: RunPoolState, e: DataFusionError)` on `ContigAnnotationStream`; profile fields `run_pool_runs: usize`, `head_wait: Duration`.

- [ ] **Step 1: Enter the pool from `PreparingContig`**

In the `Poll::Ready(Ok(Some(mut ready)))` arm, the block `if config.annotation_workers > 1 { let Some(shard_ctx) = config.vcf_shard_ctx.clone() else { ...error... }; ...sharded... } else { ...serial... }` becomes a three-way branch. Replace the `let Some(shard_ctx) = ... else { ... }` guard with:

```rust
                        if config.annotation_workers > 1 {
                            let Some(shard_ctx) = config.vcf_shard_ctx.clone() else {
                                // Streaming path: the run pool. Run 0's lookup was
                                // activated by `activate_contig_lookups`; the rest
                                // activate inside their tasks.
                                let mut runs = VecDeque::with_capacity(ready.pending_runs.len() + 1);
                                runs.push_back(ready.active_run.clone());
                                runs.append(&mut ready.pending_runs);
                                let total_runs = runs.len();
                                let workers = config.annotation_workers.max(1);
                                let lookahead = stream_lookahead_runs(
                                    workers,
                                    std::env::var("VEP_STREAM_LOOKAHEAD_RUNS").ok().as_deref(),
                                );
                                record_contig_profile(&ready.pipeline_profile, |profile| {
                                    profile.run_pool_runs = total_runs;
                                });
                                let mut pool = RunPoolState {
                                    pending: runs,
                                    first_lookup: Some(std::mem::take(&mut ready.lookup_partitions)),
                                    total_runs,
                                    next_start: 0,
                                    head: 0,
                                    active: HashMap::new(),
                                    workers,
                                    lookahead,
                                    run_inputs: ready.run_inputs.clone(),
                                    chrom: ready.chrom,
                                    config,
                                    session,
                                    shared: Arc::clone(&ready.shared_context),
                                    ephemeral_tables: ready.ephemeral_tables,
                                    t_contig_active: ready.t_contig_active,
                                    pipeline_profile: ready.pipeline_profile.clone(),
                                    contig_rows: 0,
                                    head_wait_started: None,
                                };
                                pool.admit();
                                self.state = StreamState::AnnotatingRunPool(pool);
                                // Committed to annotating this contig: overlap the
                                // NEXT contig's data phase with it.
                                self.start_prefetch_next_contig();
                                continue;
                            };
                            // ... existing sharded code, unchanged from `let shared = Arc::clone(&ready.shared_context);` on ...
```

The old error string `"parallel annotation (threads>1) requires a VCF shard context"` disappears. `ready.lookup_partitions` is a `VecDeque<LookupPartitionHandle>`; `std::mem::take` leaves an empty deque behind, and `ready` is dropped at the end of the arm.

- [ ] **Step 2: Add the error helper**

Next to `abort_prefetch`:

```rust
    /// Tear a run pool down on error: abort its tasks, deregister the contig's
    /// tables, then surface `e` through `ErrorCleaningUp`.
    fn fail_run_pool(&mut self, mut pool: RunPoolState, e: DataFusionError) {
        pool.abort();
        let fut = make_cleanup_future(
            Arc::clone(&pool.session),
            std::mem::take(&mut pool.ephemeral_tables),
        );
        self.state = StreamState::ErrorCleaningUp(fut, e);
        self.abort_prefetch();
    }
```

- [ ] **Step 3: Replace the temporary arm with the real one**

```rust
                StreamState::AnnotatingRunPool(_) => {
                    let StreamState::AnnotatingRunPool(mut pool) =
                        std::mem::replace(&mut self.state, StreamState::Done)
                    else {
                        unreachable!()
                    };
                    let limit_reached = fetch_limit.is_some_and(|limit| self.rows_emitted >= limit);
                    if limit_reached || pool.head == pool.total_runs {
                        // Contig done, or LIMIT satisfied: remaining runs could
                        // only produce rows that would be dropped.
                        pool.abort();
                        profile_end!(
                            &format!("{}: TOTAL", pool.chrom),
                            pool.t_contig_active,
                            format!("{} rows", pool.contig_rows)
                        );
                        emit_contig_pipeline_profile(&pool.shared.profile, &pool.chrom);
                        let fut = make_cleanup_future(
                            Arc::clone(&pool.session),
                            std::mem::take(&mut pool.ephemeral_tables),
                        );
                        self.state = StreamState::CleaningUp(fut);
                        continue;
                    }
                    pool.admit();
                    // Completions and errors of the runs behind the head. Their
                    // output stays in their channels until they are released.
                    let head = pool.head;
                    let mut failure: Option<DataFusionError> = None;
                    for (index, task) in pool.active.iter_mut() {
                        if *index == head || !task.running() {
                            continue;
                        }
                        if let Poll::Ready(Err(e)) = task.poll_join(cx) {
                            failure = Some(e);
                            break;
                        }
                    }
                    if let Some(e) = failure {
                        self.fail_run_pool(pool, e);
                        continue;
                    }
                    // A completion may have freed a worker slot.
                    pool.admit();
                    let any_finished_behind = pool.active.values().any(|t| !t.running());
                    let task = pool
                        .active
                        .get_mut(&head)
                        .expect("the head run is admitted before any later run");
                    match task.rx.poll_recv(cx) {
                        Poll::Ready(Some(batch)) => {
                            if let Some(started) = pool.head_wait_started.take() {
                                record_contig_profile(&pool.pipeline_profile, |profile| {
                                    profile.head_wait += started.elapsed();
                                });
                            }
                            let batch = match fetch_limit {
                                Some(limit) => {
                                    let remaining = limit.saturating_sub(self.rows_emitted);
                                    if remaining == 0 {
                                        self.state = StreamState::AnnotatingRunPool(pool);
                                        continue; // the limit check above ends the contig
                                    }
                                    if batch.num_rows() > remaining {
                                        batch.slice(0, remaining)
                                    } else {
                                        batch
                                    }
                                }
                                None => batch,
                            };
                            self.rows_emitted += batch.num_rows();
                            pool.contig_rows += batch.num_rows();
                            record_contig_profile(&pool.pipeline_profile, |profile| {
                                profile.output_batches += 1;
                                profile.output_rows += batch.num_rows();
                            });
                            self.state = StreamState::AnnotatingRunPool(pool);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(None) => {
                            // The head's sender is gone: its task has ended.
                            match task.poll_join(cx) {
                                Poll::Pending => {
                                    self.state = StreamState::AnnotatingRunPool(pool);
                                    return Poll::Pending;
                                }
                                Poll::Ready(Ok(())) => {
                                    pipeline_trace::emit(
                                        "run_pool",
                                        "release",
                                        &[
                                            ("chrom", TraceValue::Str(&pool.chrom)),
                                            ("run", TraceValue::Usize(head)),
                                            ("elapsed", TraceValue::Duration(task.started.elapsed())),
                                        ],
                                    );
                                    pool.active.remove(&head);
                                    pool.head += 1;
                                    self.state = StreamState::AnnotatingRunPool(pool);
                                    continue;
                                }
                                Poll::Ready(Err(e)) => {
                                    self.fail_run_pool(pool, e);
                                    continue;
                                }
                            }
                        }
                        Poll::Pending => {
                            if any_finished_behind && pool.head_wait_started.is_none() {
                                pool.head_wait_started = Some(Instant::now());
                            }
                            self.state = StreamState::AnnotatingRunPool(pool);
                            return Poll::Pending;
                        }
                    }
                }
```

Borrow note: `task` borrows `pool.active` mutably while `pool.head_wait_started`, `pool.contig_rows`, `pool.pipeline_profile` and `pool.chrom` are read or written. Rust accepts disjoint field borrows within one function body, but not through a method; keep these as direct field accesses (as written) and compute `any_finished_behind` before taking `task`.

- [ ] **Step 4: Profile fields**

In `ContigPipelineProfile` add:

```rust
    /// Streaming run pool: runs planned for the contig (0 on other paths).
    run_pool_runs: usize,
    /// Streaming run pool: time the ordered release waited on the head run
    /// while a later run had already finished.
    head_wait: Duration,
```

In `summary_line`, append ` run_pool_runs={} head_wait={:.3}s` to the format string after `ordered_drain_wait={:.3}s` and add `self.run_pool_runs, self.head_wait.as_secs_f64()` at the end of the argument list, matching how `ordered_drain_wait` is passed.

- [ ] **Step 5: Drop the stale error tests, run the suite**

`grep -n "requires a VCF shard" datafusion/bio-function-vep/src` must return nothing but comments; delete any test asserting that message. Then:

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: green.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): workers>1 on the streaming path via an ordered run pool"
```

---

### Task 6: Lift the regions guard, delete the dead re-sequencer

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`AnnotateProvider::new`, test `regions_with_workers_above_one_are_rejected_at_construction`)
- Modify: `datafusion/bio-function-vep/src/lib.rs`
- Delete: `datafusion/bio-function-vep/src/ordered_drain.rs`

- [ ] **Step 1: Rewrite the construction test**

Replace `regions_with_workers_above_one_are_rejected_at_construction` with:

```rust
    #[cfg(feature = "parquet-cache")]
    #[tokio::test]
    async fn regions_with_workers_above_one_are_accepted_at_construction() {
        let session = Arc::new(SessionContext::new());
        let vcf_schema = Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
        ]);
        let tmp = tempfile::tempdir().unwrap();
        let build = |options: &str| {
            AnnotateProvider::new(
                Arc::clone(&session),
                "vcf".to_string(),
                tmp.path().to_string_lossy().to_string(),
                AnnotationBackend::Parquet,
                CacheSourceType::Merged,
                Some(options.to_string()),
                vcf_schema.clone(),
            )
        };
        // The streaming run pool plans regions itself; the sharded sink still
        // rejects the combination at scan time.
        build(r#"{"workers":2,"regions":[{"chrom":"chr1","start":1,"end":2}]}"#)
            .expect("regions with workers>1 are planned by the streaming run pool");
        build(r#"{"workers":1,"regions":[{"chrom":"chr1","start":1,"end":2}]}"#)
            .expect("workers=1 with regions is accepted");
        build(r#"{"workers":4}"#).expect("workers>1 without regions is unchanged");
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep regions_with_workers_above_one 2>&1 | tail -5`
Expected: FAIL, the first `build` returns the `regions require workers=1` error.

- [ ] **Step 3: Remove the guard and the module**

In `AnnotateProvider::new`, delete the whole `if regions.is_some() { ... "regions require workers=1" ... }` block (keep `let regions = crate::regions::parse_regions_option(...)?;`). The sink is still covered by the scan-time check `"regions are not supported with sharded VCF output"`, which stays.

```bash
git rm datafusion/bio-function-vep/src/ordered_drain.rs
sed -i '' '/pub(crate) mod ordered_drain;/d' datafusion/bio-function-vep/src/lib.rs
grep -rn "ordered_drain\|OrderedWindowDrain" datafusion/bio-function-vep/src ; echo "exit=$?"
```

Expected: only the `ordered_drain_wait` profile field name matches (that is a different identifier and stays).

- [ ] **Step 4: Run the suite**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -10`
Expected: green, including the sink rejection test for regions.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -2
git add -A datafusion/bio-function-vep/src
git commit -m "feat(vep): allow regions with workers>1 on the streaming path; drop the unused ordered drain"
```

---

### Task 7: End-to-end parity against vepyr fixtures, then the PR

**Files:**
- Modify: `~/research/git/vepyr/Cargo.toml` (temporary `[patch]`, NOT committed)

- [ ] **Step 1: Point vepyr at the working tree and rebuild**

Append temporarily to vepyr's `Cargo.toml`:

```toml
[patch."https://github.com/biodatageeks/datafusion-bio-functions.git"]
datafusion-bio-function-vep = { path = "/Users/mwiewior/research/git/datafusion-bio-functions/datafusion/bio-function-vep" }
```

```bash
cd ~/research/git/vepyr
CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr 2>&1 | tail -3
```

(Both `VIRTUAL_ENV` and `CONDA_PREFIX` are set in this shell and make maturin refuse; unset them for the build.)

- [ ] **Step 2: Parity smoke on the Ensembl and merged golden fixtures**

```bash
cd ~/research/git/vepyr
CONDA_PREFIX= VIRTUAL_ENV= VEP_STREAM_RUN_BUFFERS=1 uv run python - <<'EOF'
import json, os, sys
sys.path.insert(0, "tests")
from pathlib import Path
import polars as pl, pyarrow as pa
from cache_metadata import copy_cache_with_source_metadata
from vepyr._core import create_annotator
G = Path("tests/data/golden")
scratch = Path(os.environ.get("SCRATCH", "/tmp")) / "run_pool_smoke"
scratch.mkdir(parents=True, exist_ok=True)
def run(cache, opts, skip_csq=False, limit=None):
    ann = create_annotator(str(G / "input.vcf.gz"), cache, json.dumps(opts), skip_csq, limit)
    return pl.from_arrow(pa.Table.from_batches(list(ann), schema=ann.schema))
base = {"everything": True, "reference_fasta_path": str(G / "reference.fa"), "buffer_size": 7}
ok = True
for name, src, kind in [("ensembl", G / "cache", "ensembl"), ("merged", Path("tests/data/golden_merged/cache"), "merged")]:
    cache = str(copy_cache_with_source_metadata(str(src), scratch / f"cache_{name}", kind, "115"))
    serial = run(cache, base)
    for workers in (2, 3):
        par = run(cache, {**base, "workers": workers})
        eq = par.equals(serial)
        print(f"{name} workers={workers} rows={par.height} equal={eq}")
        ok &= eq
    lo, hi = int(serial["start"][30]), int(serial["start"][60])
    pred = (pl.col("chrom") == "chr1") & pl.col("start").is_between(lo, hi)
    par = run(cache, {**base, "workers": 3, "regions": [{"chrom": "chr1", "start": lo, "end": hi}]})
    eq = par.equals(serial.filter(pred))
    print(f"{name} workers=3 regions rows={par.height} equal={eq}")
    ok &= eq
    head = run(cache, {**base, "workers": 3}, limit=7)
    eq = head.equals(serial.head(7))
    print(f"{name} workers=3 limit=7 rows={head.height} equal={eq}")
    ok &= eq
print("ALL OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
EOF
```

Expected: every line prints `equal=True` and the script ends with `ALL OK`. `VEP_STREAM_RUN_BUFFERS=1` forces one-buffer runs, so the 100-variant fixture at `buffer_size=7` runs about 15 runs per collect with a seam at every buffer; on the merged cache the stateful floor keeps runs at four buffers regardless. Then run the script a second time without `VEP_STREAM_RUN_BUFFERS` in the environment so the default formula is exercised too; both runs must print `ALL OK`. If the merged case differs, compare `par` and `serial` on `SYMBOL`/`HGNC_ID` first: a mismatch there means a seam was not warmed up (check `run_pool plan` and `run_pool done` lines with `VEP_PIPELINE_TRACE=1`; `warm_up_rows` must be non-zero for every run but the first).

- [ ] **Step 3: Sink path unchanged**

```bash
cd ~/research/git/vepyr
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_build_cache.py -k workers_preserves_vcf_output -q 2>&1 | tail -3
CONDA_PREFIX= VIRTUAL_ENV= uv run pytest tests/test_region_pushdown_parity.py tests/test_golden.py tests/test_golden_merged.py -q 2>&1 | tail -3
```

Expected: all pass.

- [ ] **Step 4: Remove the `[patch]`, final checks, push, PR**

```bash
cd ~/research/git/vepyr && git checkout -- Cargo.toml Cargo.lock
cd ~/research/git/datafusion-bio-functions
cargo test -p datafusion-bio-function-vep 2>&1 | tail -5
cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -3
git push -u origin feat/stream-run-pool
gh pr create --title "feat(vep): workers>1 on the streaming path via an ordered run pool" --body-file - <<'EOF'
## Summary
- `annotate_vep()` with `workers>1` and no VCF shard context now runs a pool of N run tasks per contig instead of erroring
- contigs are planned into grid-aligned runs (regions composed, same `WorkerGridSlice`/`RunGate` as region runs); Merged/RefSeq runs warm up over `overlap_width_bp`, Ensembl runs cut plainly
- the sharded worker body is shared behind `RunOutput` (`Shard` for the sink, `Batches` for the pool), so the sink path is unchanged
- runs release strictly in grid order; the head run streams live (LIMIT gets its first batch early); admission is bounded to `workers + lookahead` runs
- every run of a contig shares one Parquet lookup cell (also a small win for serial region runs)
- the construction-time `regions require workers=1` guard is removed (the sink still rejects regions at scan time); `src/ordered_drain.rs` deleted
- knobs for the measurement sweep: `VEP_STREAM_RUN_BUFFERS`, `VEP_STREAM_LOOKAHEAD_RUNS`; trace stage `run_pool`; profile fields `run_pool_runs`, `head_wait`

Design: vepyr `docs/superpowers/specs/2026-09-06-lazyframe-parallel-workers-design.md`

## Test plan
- [x] `cargo test -p datafusion-bio-function-vep`
- [x] vepyr golden fixtures (Ensembl + merged, `buffer_size=7`, `VEP_STREAM_RUN_BUFFERS=1`): `workers=2/3` equals `workers=1` row for row and in order; with `regions`; with a LIMIT
- [x] vepyr `test_annotation_workers_preserves_vcf_output` (sink path byte-identical)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

Then read the PR body back with `gh api repos/{owner}/{repo}/pulls/<n> --jq .body | head` (`gh pr edit` has silently failed on this repo before). Record the PR number and, after merge, the squash SHA: the vepyr plan pins to it.

---

## Self-review notes

- Spec coverage: mode selection (T2 `contig_modes`, T5 entry), run planning incl. Ensembl zero warm-up, stateful floor, regions subdivision, degenerate grid (T1, T2), shared lookup cell (T2), generalised worker body with trim (T3), pool/admission/ordered release/head streamed live (T4, T5), LIMIT abort (T5), errors and drop safety (T4 `RunTask::drop`, `AbortJoinOnDrop`; T5 `fail_run_pool`), regions guard lifted with the sink rejection kept (T6), tracing and profile fields (T2, T4, T5), dead module removed (T6), fixture parity on both caches with and without regions and with a LIMIT (T7). The index requirement is enforced in vepyr (see the vepyr plan), per the spec.
- Names used across tasks: `stream_run_buffers`, `stream_lookahead_runs`, `plan_stream_runs`, `admission_allows`, `contig_modes`/`ContigModes`, `RunActivationInputs.parquet_lookup_cell`, `RunOutput::{Shard, Batches}`, `RunStats`, `annotate_lookup_run`, `AbortJoinOnDrop`, `RunTask::{running, poll_join}`, `RunPoolState::{running, admit, abort}`, `spawn_run_task`, `StreamState::AnnotatingRunPool`, `ContigAnnotationStream::fail_run_pool`, profile `run_pool_runs`/`head_wait`, trace stage `run_pool` with events `plan`/`start`/`done`/`release`.
- The engine has no VCF-plus-cache fixture; the end-to-end parity lives in T7 against vepyr's golden fixtures and is repeated as pytest in the vepyr plan.
- Order dependency: T1 → T2 → T3 → T4 → T5 → T6 → T7. T3 can be done before T2 if convenient; nothing else reorders.
