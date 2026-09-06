# Parallel workers on the LazyFrame path

**Date:** 2026-09-06
**Status:** Approved (design)
**Implementation repos:** `datafusion-bio-functions` (`datafusion/bio-function-vep`), then `vepyr`
**Related:** `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md`
(region runs, grid-aligned warm-up on the streaming path; engine PR #239, vepyr PR #81),
engine design 2026-06-25 §5/§7 (grid slices and warm-up on the sharded VCF path),
`docs/superpowers/plans/2026-06-21-single-workers-knob.md` (the `workers` knob)

## Problem

`vepyr.annotate(..., workers=N)` has one concurrency knob. With `output_vcf` it runs N
within-contig annotation pipelines and is byte-identical to `workers=1` at every N. On
the LazyFrame path the same knob is accepted, forwarded to the engine, and then rejected
at the first contig:

```
parallel annotation (threads>1) requires a VCF shard context; it is only
supported via the VCF output sink
```

The engine's `workers>1` branch exists only for the VCF sink. Its workers write VCF body
shards to a temp directory and an assembler thread concatenates them. Nothing hands
Arrow batches back to a caller. The streaming path, which the Polars IO plugin drains,
is one ordered lookup stream per contig and one window at a time.

This design adds a parallel streaming mode to the engine so that a LazyFrame collect at
`workers=N` runs N annotation pipelines per contig and yields rows in exactly the order
`workers=1` yields them.

## Scope

In scope:

- `workers>1` on the LazyFrame path (`annotate()` without `output_vcf`) for Ensembl,
  Merged and RefSeq caches, with results identical to `workers=1` row for row and in
  the same order.
- Composition with region pushdown: a LazyFrame predicate on `chrom`/`start`/`end` is
  pushed to the engine automatically (PR #81), so a parallel collect with a filter must
  work. The engine guard `regions require workers=1` is lifted for the streaming mode.
- Bounded memory: the number of runs in flight or awaiting release is capped.
- LIMIT (`head(n)`, `n_rows`) on the parallel path, including early first-batch delivery.
- Tests at both layers and the LazyFrame-versus-VCF md5 gate at `workers>1`.
- Documentation and docstring corrections (they currently say the path is serial).

Out of scope:

- Any change to the sharded VCF sink. It stays as is, including its rejection of
  `regions`.
- Concurrency inside a run. The serial state machine's window-level `inflight` queue
  stays at one; it is unsound above one on Merged/RefSeq (see Background).
- Cross-contig concurrency. Contigs stay sequential; the existing next-contig prefetch
  keeps overlapping the next contig's prepare with the current contig's annotation.
- Cost-balanced run cuts (`VEP_GRID_BALANCE`). Runs are cut by buffer count.
- An explicit `annotate(regions=...)` argument (still deferred from the regions spec).

## Background: what the engine already has

All references are to `datafusion/bio-function-vep/src/annotate_provider.rs` at the
current vepyr pin (`dba94e3`) unless stated.

**The knob.** The JSON key is `workers`. `AnnotateProvider::scan` parses it into
`ContigAnnotationConfig::annotation_workers` and raises the session's target
partitions to at least that value. `ContigAnnotationStream::poll_next` branches on it
when a contig's prepare future resolves (`StreamState::PreparingContig`): `>1` requires
`ContigAnnotationConfig::vcf_shard_ctx`, which only `vcf_sink::drive_sharded_vcf_annotation`
sets, and otherwise returns the error above; `<=1` builds `ContigAnnotationState` and
enters `AnnotatingContig`.

**Runs are worker slices.** PR #239 added region runs to the streaming path. A
`ContigRun { bounds, slice: Option<WorkerGridSlice> }` carries the same
`WorkerGridSlice` the sharded workers use (`scan_lo_pos`, `emit_start_pos`,
`scan_hi_pos`, `skip_leading_rows`, `warm_up_start_row`, `emit_start_row`,
`emit_end_row`). `ContigRun::lookup_filter` builds the run's VCF filter,
`ContigRun::gate` builds a `regions::RunGate` (a factored-out copy of the sharded
worker's inline gate: drop leading position ties, divert warm-up rows, rank-stop), and
`activate_run_lookup` builds a `LookupProvider` for one run and spawns its lookup
partition workers. `ActivatingRun` swaps a contig's lookup to the next run when the
current run drains. Runs are planned in `prepare_contig_data` from the buffer-grid
count pass (`count_contig_buffer_boundaries`, which runs concurrently with the context
load) through `regions::plan_runs` and `build_grid_slices`.

**Warm-up.** Merged and RefSeq annotation depends on per-buffer HGNC donation state
(`persisted_buffer_transcripts`). A cut in the input is a seam; the engine reconstructs
the state at a seam by replaying whole buffers within `overlap_width_bp` (max
transcript span + one 1 Mb cache region) before the seam (`warm_up_worker_state`).
`build_grid_slices` walks the warm-up start back to the first buffer within that reach;
with `overlap_width_bp = 0` the warm-up start equals the emit start and no rows are
replayed. Ensembl has no cross-buffer state: `build_stateful_buffer_local_transcripts_cow`
runs for every source but never diverges from the base transcripts on Ensembl, so a
plain cut is exact. Both the sharded path and the region spec rely on this and both are
byte-identical at every worker count.

**The sharded worker.** `spawn_annotation_from_lookup_sharded` drains one lookup
receiver, applies the gate, replays warm-up, cuts windows of `input_buffer_size` units,
and for each window calls `hydrate_worker_window` and `annotate_worker_window`. The
result of that call is a `VecDeque<RecordBatch>`, already projected. Only then does
`VcfBodyShardWriter::write_batch` format it to text. Every worker has its own
`AnnotationWorkerState` (colocated map, persisted transcripts, overrides, SIFT cache,
window buffer); the expensive immutable context (transcripts, exons, translations,
indexes, consequence engine, plugin registry) is one `Arc<SharedContigAnnotationContext>`
per contig. All workers of a contig share one `tokio::sync::OnceCell` for the Parquet
variation lookup so the shard footer and page index (about 0.5 GB on chr1) are decoded
once per contig.

**The unsound axis.** `ContigAnnotationState::inflight` dispatches windows on the
blocking pool up to `annotation_workers` deep, but each window is seeded with a
snapshot of `persisted_buffer_transcripts` taken at dispatch and the result of window N
is installed only when N is awaited. Two windows in flight would seed N+1 with the
pre-N map and silently drop HGNC carry on Merged/RefSeq. It is unreachable today
because the `>1` branch never enters `AnnotatingContig`. This design leaves it at one.

**Ordered re-sequencer.** `src/ordered_drain.rs` (`OrderedWindowDrain`) is dead code
reserved for a parallel window driver. This design does not need it (the head run is
streamed live rather than buffered to completion); the engine PR removes the module.

## Engine design

### Mode selection

`PreparingContig` gains a third arm. When `annotation_workers > 1` and
`vcf_shard_ctx` is `None`, the contig enters a new `StreamState::AnnotatingRunPool`.
The sink's arm (`vcf_shard_ctx` is `Some`) and the serial arm are unchanged.

`prepare_contig_data` learns the mode as a boolean, `stream_parallel`, derived from the
same two facts. It changes only the planning half described next; the context load,
prefetch and cleanup are untouched.

### Run planning

In `stream_parallel` mode the buffer-grid count pass runs for every cache source, not
only Merged and RefSeq. It already overlaps the context load, and it gives every source
the same run planner and the same gate. Two consequences:

- `stateful_parallel` (grid slices for the sink) stays false in this mode, so the two
  consumers of the single `grid_count` future never coexist. The count is consumed once,
  by the run planner. This is what allows lifting the `regions require workers=1` guard.
- Ensembl runs are built with `overlap_width_bp = 0`, giving `warm_up_start_row ==
  emit_start_row`, `skip_leading_rows` tie handling and rank-stop, and no replay.
  Merged and RefSeq runs use the contig's real `overlap_width_bp`, exactly as region
  runs and sharded slices do today.

Planning steps for one contig with `B` buffers (boundaries of length `B+1`):

1. Region runs first. Without regions the contig is one buffer range `[0, B)`. With
   regions, `regions::plan_runs(bounds, Some(positions))` gives one merged buffer range
   per region group, each with its trim bounds, as today.
2. Each buffer range `[bk, bk1)` is cut into consecutive pieces of `run_buffers` whole
   buffers (the last piece may be shorter). `run_buffers` is chosen once per contig:

   ```
   target_runs = workers * RUNS_PER_WORKER          (RUNS_PER_WORKER = 4)
   run_buffers = max(MIN_RUN_BUFFERS, ceil(B / target_runs))
   ```

   `MIN_RUN_BUFFERS` is 4 on Merged/RefSeq and 1 on Ensembl. The floor exists because
   every seam on a stateful source replays roughly `overlap_width_bp` of input; at
   whole-genome variant density that is about one buffer, so a four-buffer run keeps
   the replay under a quarter of the run's prepare work and a small fraction of its
   annotation work. Contigs with fewer buffers than `workers` simply plan fewer runs.
3. `build_grid_slices(boundaries, cuts, overlap)` turns each piece into a
   `WorkerGridSlice`; the run keeps the region's trim bounds. Runs are numbered in grid
   order across the whole contig; the number is the release order.
4. A degenerate grid (no rows) plans no run and skips the contig, as today.

Environment overrides for the measurement sweep, read where the other `VEP_*` knobs are
read: `VEP_STREAM_RUN_BUFFERS` (fixes `run_buffers`, ignoring the formula but not the
`MIN_RUN_BUFFERS` floor on stateful sources) and `VEP_STREAM_LOOKAHEAD_RUNS` (see
Ordering). The defaults above are provisional and the measurement pass may change them;
changing a default does not change output.

### The run pool

`AnnotatingRunPool` owns:

- `runs: Vec<ContigRun>` in release order, and `next_start: usize`.
- `active: HashMap<usize, RunTask>` keyed by run index. A `RunTask` holds the
  `JoinHandle<Result<RunResult>>` of the run's task, its abort handle, its lookup
  partition handles (for abort), and the receiving end of its output channel.
- `head: usize`, the index of the run whose output is being released.
- The contig-level fields the sink's `ParallelContigState` carries (chrom, config,
  session, shared context, ephemeral tables, activation instant, profile).

A run task is one `tokio::spawn` that awaits `activate_run_lookup` for its run and then
runs the sharded worker body. The lookup is activated inside the task, not on the
stream's poll path, so activation of run j+1 overlaps annotation of run j. The
sharded worker body (`spawn_annotation_from_lookup_sharded`) is generalised over its
output: an enum sink with a `VcfBody(VcfBodyShardWriter)` arm for the sink path and a
`Batches(tokio::sync::mpsc::UnboundedSender<RecordBatch>)` arm for this mode. The
gate, warm-up replay, window cutting and annotation calls are shared verbatim, so the
sink path's behaviour cannot drift from the streaming path's. Trim bounds (regions) are
applied in the worker before the batch is sent, as `filter_batch_to_bounds` does in the
serial loop today. `rows_done` bookkeeping is skipped when there is no shard context.

The per-run channel is unbounded on purpose: its content is bounded by the run's own
size, a run must be able to finish without waiting for the drain (otherwise a straggling
head would stall every worker), and the admission rule below bounds how many such runs
exist.

Every run task of a contig shares the contig's Parquet lookup `OnceCell`, as the
sharded slices do. `activate_run_lookup` gains that parameter; the serial region-run
path passes a fresh cell per contig as well, which is also a small win for it (today
each region run decodes the footer again).

Lookup partitions per run are one: the run's `LookupProvider` is built with
`target_partitions = 1`, since parallelism comes from runs. Colocated sinks are per run,
as they are per partition today.

### Ordering, admission and draining

On every poll of `AnnotatingRunPool`:

1. **Admit.** While `active.len() < workers` and `next_start < head + workers + lookahead`
   and `next_start < runs.len()`, spawn the task for run `next_start` and increment it.
   `lookahead` defaults to `workers` (`VEP_STREAM_LOOKAHEAD_RUNS` overrides). Memory in
   flight is therefore at most `workers + lookahead` runs of output plus one window per
   active task.
2. **Drain the head.** Poll the head run's receiver. A batch is returned to the caller
   immediately, with the same LIMIT truncation and per-row accounting `DrainingWindow`
   applies on the serial path (a pool-specific draining sub-state, since
   `DrainingWindow` carries the serial `ContigAnnotationState`). A closed receiver
   means the run finished: poll its join handle to surface any error, remove it from
   `active`, advance `head`, and loop. When `head == runs.len()` the contig is done:
   transition to `CleaningUp` as the other arms do.
3. **Wake-ups.** The head receiver's readiness wakes the stream. Admission is also
   re-checked whenever a task completes, so a finished non-head run frees its worker
   slot without waiting for the head. A task completion for a non-head run is observed
   through its join handle in the same poll loop (a `FuturesUnordered` of join handles
   or an explicit completion channel; implementation choice).

Because runs are released strictly by index, and run indices follow the grid, the row
order equals the serial order within a contig. Contigs are sequential, so the global
order equals the serial order.

### LIMIT

When the running total of released rows reaches the fetch limit, the pool aborts every
active task and its lookup handles, drops the remaining runs, and the contig ends
through `CleaningUp`; the outer loop already ends the stream on a satisfied limit. The
first batch of a query with a small limit is released as soon as the head run's first
window completes, not after the run finishes.

### Regions with workers>1

The construction-time guard in `AnnotateProvider::new` (`regions require workers=1`)
is removed. It cannot be made conditional on the sink there, because the shard context
is attached after construction; the sink case is already covered by the scan-time
rejection of `regions` whenever a shard context is present, which is unchanged. With
both `regions` and `workers>1`, planning proceeds as in Run planning: region buffer
ranges are subdivided into runs, each carrying its region's trim bounds, and the pool
executes them in grid order. The `grid_count` double-take that motivated the guard
cannot recur because `stateful_parallel` is false in this mode.

### Input index

Each run reads its position window through the VCF provider's range filter. On an
indexed input that is a seek; on an unindexed input it is a full parse per run, which
would multiply the parse cost by the number of runs. `vepyr.annotate()` therefore
raises `ValueError` when `workers > 1` and neither `<vcf>.tbi` nor `<vcf>.csi` exists,
on both output paths, before any engine call. The message mirrors the engine sink's
(`workers>1 requires a tabix-indexed input`). The engine itself does not add a check on
the streaming path: it works on unindexed input, only slower, and the Python check is
the earliest and most uniform place. (The presentation of this design said the engine
would enforce it; the change is deliberate and is the only deviation.)

### Errors and cancellation

A failing run task, lookup worker or activation surfaces its error as the contig's
error: the pool aborts every task and lookup handle and enters `ErrorCleaningUp` with the
original error, as `ParallelContigState::abort` does for the sink. Dropping the stream
(Polars stops collecting, or the annotator is discarded) drops the pool, whose `Drop`
aborts everything it owns, so no task outlives the query.

### Tracing and profiling

`VEP_PIPELINE_TRACE` emits, per contig, a `run_pool/plan` event (`runs`, `run_buffers`,
`lookahead`) and per run `run_pool/start`, `run_pool/done` (input rows, output rows,
elapsed, warm-up rows) and `run_pool/release`. `VEP_PROFILE`'s per-contig
`pipeline_profile` line reports the run count and the head-wait time (time the drain
spent with no head batch available while other runs were complete). These are what the
measurement pass reads; wall time alone does not explain a scaling result.

## Python design (`vepyr`)

- `annotate()`: the index check above. The docstring for `workers` describes both paths
  and drops "serial in this release". The options dict is unchanged (`workers` is
  already forwarded when `>1`).
- `src/annotate.rs`: no functional change. The runtime is already sized from `workers`
  and the session's target partitions already follow it. A comment update where it says
  the streaming path is drained in parallel by Polars (it is not; Polars drains one
  generator).
- `_batch_source`: no change. Region pushdown, projection-driven flags and the LIMIT
  already reach the engine through the same options and SQL.
- Docs: `docs/performance.md` (knob table and a new timing table), `docs/quickstart.md`,
  `docs/dataframes.md` where they state the LazyFrame path is serial.

## Testing

Engine (`datafusion-bio-function-vep`):

- Unit: run planning (cut count and lengths for several `B`/`workers`, the stateful
  floor, Ensembl zero warm-up, region ranges subdivided with trim bounds preserved,
  degenerate grid); admission never exceeds `workers + lookahead`; releases follow run
  index whatever the completion order; LIMIT aborts active tasks and drops pending runs;
  an error in a non-head run surfaces and aborts the pool.
- Fixture: SQL `annotate_vep(...)` on the crate fixtures with `workers` in `{1, 3}` on
  Ensembl and Merged, with and without `regions`, with and without a LIMIT, comparing
  concatenated outputs batch-row for batch-row. The sink's existing byte-identity tests
  guard the shared worker body.

vepyr:

- Golden fixture (`tests/data/golden`, indexed input, Ensembl and Merged caches):
  `annotate(...).collect()` at `workers=2` equals `workers=1` as ordered frames, with
  `skip_csq` in both settings, with a `chrom`/`start` predicate (pushdown plus workers),
  with `head(7)`, and with `everything=True`.
- The index check raises for an unindexed input at `workers=2` on both paths; the
  existing forwarding tests stay.
- e2e (release gate): the md5 LazyFrame-versus-VCF comparison on chr22 and chr1 at
  `workers=4` on the 116 caches with `--everything`, and a wall/RSS table for
  `workers` in `{1, 2, 4, 8}` on chr1 for `docs/performance.md`, read together with
  the `run_pool` trace to attribute any shortfall. Discard the first run of a session
  and check the host load average first.

## Measurement plan

The sweep decides `RUNS_PER_WORKER`, `MIN_RUN_BUFFERS` and the lookahead default. It
varies `VEP_STREAM_RUN_BUFFERS` and `VEP_STREAM_LOOKAHEAD_RUNS` at `workers=8` on chr1
for Ensembl and Merged, records wall, peak RSS, head-wait and warm-up rows, and picks
the smallest run length whose warm-up share stays under five percent on Merged and
whose head-wait is not the dominant idle term. Expected outcome: within-contig scaling
close to the sink's (about 5x at `workers=8`), with the same serial per-contig prepare
tail the sink has.

## Delivery

1. Engine PR on `datafusion-bio-functions`: the mode, planning, pool, generalised
   worker sink, guard change, tracing, tests. Branch `feat/stream-run-pool`.
2. vepyr PR pinned to the engine PR head: index check, docstring, docs, tests, e2e
   runner flag. Re-pin to the merge commit after the engine PR merges, rerun the tests
   and the gate, then merge.

## Deferred

- Cost-balanced run cuts (`VEP_GRID_BALANCE` equivalent) for gene-dense stragglers.
- Overlapping the next contig's first runs with the current contig's tail (cross-contig
  concurrency). Ordering would still hold since runs are numbered globally, but the
  shared context of two contigs would be resident at once.
- Lifting `regions` on the sharded VCF sink.

## Risks

- **Warm-up share on Merged/RefSeq.** If the measured share is high at useful run
  lengths, the floor goes up and small contigs get fewer runs; correctness is unaffected.
- **Head-of-line idle.** A gene-dense head run stalls release while others complete;
  the lookahead bounds the waste but does not remove it. The trace's head-wait is the
  signal; balanced cuts are the deferred fix.
- **Memory.** Worst case is `workers + lookahead` runs of output resident. With the
  defaults and four-buffer runs that is on the order of a few hundred megabytes to a
  few gigabytes at `everything=True`, below the collect footprint Polars already has.
  Users sinking to Parquet keep `row_group_size=5000`.
- **Drift between sink and stream workers.** Mitigated by sharing one worker body
  behind an output enum rather than copying it.
