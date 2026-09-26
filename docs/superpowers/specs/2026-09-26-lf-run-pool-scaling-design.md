# LazyFrame run-pool scaling: output budget and stateful run floor

Date: 2026-09-26. Status: design approved in chat, awaiting written-spec review.

## Problem

`vepyr.annotate()` returned as a LazyFrame stops scaling with `workers` well before
`output_vcf` does. On HG002 chr22 (release 116, Merged cache) `collect()` takes 1.61 s at
workers=4 and 1.65 s at workers=8; with five plugins, 3.8 s at both. `output_vcf` keeps
scaling (1.85 -> 1.42 s core, 2.90 -> 2.33 s with plugins). This is the plateau in the
paper supplement's Figure 1.

The earlier handover attributed the plateau to the single Python loop in
`src/vepyr/__init__.py` `_batch_source`. Measurement shows that is not the main cause.

## Evidence

Probe: wrap `vepyr._create_annotator` so the engine iterator times each `next()` (engine
wait) and the gap between calls (the Python consumer), and iterate the raw engine stream
with no Python work at all. Build: vepyr #131 plus the engine at cd38a19. Two or three
runs per configuration, one warm-up discarded, host load 2-10 (shared machine), so
absolute numbers are indicative and comparisons were run back to back.

| Case (workers=8 unless stated) | Wall | Note |
|---|---|---|
| chr22 Merged, raw engine stream, core | 1.53 s (w4), 1.61 s (w8) | no Python work, still flat |
| chr22 Ensembl, raw engine stream, core | 2.24 / 1.14 / 0.81 s (w1/4/8) | scales |
| chr22 Merged, LF collect, core | 1.65 s; consumer 0.07 s | Python loop negligible |
| chr1 Merged, LF collect, core | 18.5 / 7.8 / 5.3 s (w1/4/8); consumer 0.7 s | scales |
| chr1 Merged + 5 plugins, raw engine stream | 15.1 s (w4), 13.0-14.4 s (w8) | flat, no consumer |
| chr1 Merged + 5 plugins, output_vcf | ~16 s (w4), 10.9 s (w8) | scales |

Two engine causes, both in the streaming run pool of `datafusion-bio-function-vep`
(`annotate_provider.rs`, engine PR #240), explain the flat rows:

1. **Output budget.** Runs behind the head of the ordered release queue their batches
   against one byte budget, `VEP_STREAM_BUFFER_MB`, default 1024 MiB. With plugins the
   CSQ string is about 15 KB per row, so a 20k-row run queues about 300 MB and only
   about three runs fit; the rest block until the head releases. Raising the budget on
   chr1 + 5 plugins:

   | Budget (MiB) | 1024 | 2048 | 3072 | 4096 | 8192 |
   |---|---|---|---|---|---|
   | w4 raw stream | 15.6 s | 13.9 s | | | 13.8 s |
   | w8 raw stream | 13.0 s | 11.0 s | 9.0 s | 9.0 s | 9.0 s |

   Peak RSS of the raw stream at w8 moves from 8.5 to 9.0 GiB; the LF collect peak stays
   about 14 GiB (the collected frame dominates). Core at w8: 4.85 -> 4.1 s at 4096 MiB.
   The full LF collect with plugins at w8 goes 16 -> 11.5 s, against 10.9 s for
   `output_vcf`. Larger runs (`VEP_STREAM_RUN_BUFFERS=9` or `17`) at the default budget
   are twice as slow (24-28 s), which is how the budget was identified.

2. **Stateful run floor.** On Merged and RefSeq each run replays about one buffer of
   warm-up, so `stream_run_buffers` never cuts a run shorter than
   `STREAM_MIN_RUN_BUFFERS_STATEFUL = 4` buffers. chr22 has 11 buffers, so the pool plans
   3 runs (`VEP_PIPELINE_TRACE`: `buffers=11 run_buffers=4 runs=3`) and no worker count
   above 3 can help. Ensembl, floor 1, plans 11 runs and scales.

A third cause is left to a follow-up spec (B'): with plugins the Python consumer splits
the CSQ string into plugin columns, about 0.9 s per 50k rows (5.7 s on chr1), roughly half
the wall once cause 1 is lifted.

## Goal and success criteria

`collect()` at workers=8 within 20% of `output_vcf` at workers=8 on chr22 and chr1, core
and five plugins, except chr22 with plugins, which needs B'. Values and row order stay
identical to workers=1. No Python API change.

## Design

All changes are in `datafusion/bio-function-vep/src/annotate_provider.rs`, in one PR
branched from engine master (0cefc99 at the time of writing), independent of the open
plugin-take PR #261.

### Budget default scales with workers

`stream_buffer_mb(env)` becomes `stream_buffer_mb(workers, env)`:

- default `max(STREAM_BUFFER_MB_DEFAULT, STREAM_BUFFER_MB_PER_WORKER * workers)`, with
  `STREAM_BUFFER_MB_DEFAULT = 1024` and a new `STREAM_BUFFER_MB_PER_WORKER = 512`, so
  w1-w2 keep 1 GiB, w4 gets 2 GiB and w8 gets 4 GiB;
- a positive `VEP_STREAM_BUFFER_MB` still replaces the default outright; zero or garbage
  is ignored, as today.

The call site that builds `RunPoolState.budget` passes the `workers` it already holds.
`OutputBudget` is unchanged: the head stays exempt and the permit is still held until
the stream hands the batch to the caller. The budget is a cap, not an allocation: the
queue only grows as far as the head lags.

### Stateful floor fills the workers on small contigs

In `stream_run_buffers(buffers, workers, stateful, env)` the stateful floor becomes

```
floor = min(STREAM_MIN_RUN_BUFFERS_STATEFUL, ceil(buffers / workers)).max(1)
```

instead of the constant 4. Large contigs are unchanged (chr1, 65 buffers, w8: still 4,
17 runs). Small contigs get about one run per worker: chr22 w8 -> 2 buffers, 6 runs;
chr22 w4 -> 3 buffers, 4 runs. The extra warm-up replay only occurs where workers would
otherwise sit idle. `VEP_STREAM_RUN_BUFFERS` is still floored, now at this effective
floor. Ensembl (non-stateful) keeps floor 1.

The formula is provisional: the first task of the plan measures floors 1, 2 and 4 on
chr22 and chr1 (Merged, w4 and w8, core and plugins) with a throwaway build. If the data
contradicts it, the formula is revised here before any test is written.

### Correctness argument

Both changes are scheduling only. The budget decides when a queued batch may be sent,
never its content. The run cut moves seams, and every seam already goes through the
grid-aligned warm-up that engine PR #240 gates for equality with workers=1; this change
only makes that path fire at more seams on small stateful contigs. The parity gate below
exercises exactly those seams.

### Doc comments

The comments on `STREAM_BUFFER_MB_DEFAULT`, the new per-worker constant,
`STREAM_MIN_RUN_BUFFERS_STATEFUL` and `stream_run_buffers` state the reason for each
number: the per-worker budget from the chr1 plugin saturation, the floor from warm-up
replay versus idle workers.

## Testing

Unit tests in the engine:

- `stream_run_buffers_default_and_floor` gains `(11, 8, true) -> 2`, `(11, 4, true) -> 3`,
  `(65, 8, true) -> 4` unchanged, `(3, 8, true) -> 1`, and an override below the effective
  floor lifted to it (`(11, 8, true, Some("1")) -> 2`). Existing Ensembl cases unchanged.
- A new `stream_buffer_mb` test: `(1, None) -> 1024`, `(2, None) -> 1024`,
  `(8, None) -> 4096`, `(8, Some("512")) -> 512`, `(8, Some("0")) -> 4096`,
  `(8, Some("x")) -> 4096`.
- The existing `OutputBudget` async tests stay as they are.

Parity gate (release gate, not the iteration loop): `e2e-testing/scripts/lazyframe_workers_parity.py`,
every frame at workers 2, 4 and 8 equal to workers=1, on chr22 and chr1, Merged, RefSeq
and Ensembl caches, core and five plugins. Plus the md5 e2e on the VCF sink to show that
path untouched.

Performance gate, engine master against the PR, both through a vepyr build pinned to
each:

- chr22 and chr1, core and five plugins, workers 1, 4 and 8;
- raw engine stream, LF `collect()`, and `output_vcf`;
- one subprocess per configuration so peak RSS is per configuration; one warm-up
  discarded, median of three; `uptime` and `docker ps` checked before each block;
- pass: the success criteria above. The RSS change is reported, not gated.

Plugin numbers on master run with serial plugin takes (no #261), so absolute plugin
timings will differ from the evidence table; the scaling ratio is what is gated.

## Delivery

- Engine PR in `biodatageeks/datafusion-bio-functions` from master; vepyr PR that pins
  it (by rev until an engine release tag exists).
- vepyr-fix flow: baseline first, draft PRs, codex and claude review loop to green,
  re-verify against the baseline, hand to the maintainer. No merge by the agent.
- After release: re-run the supplement's Figure 1 LF curve and write its caption.

## Out of scope

- B': engine-emitted typed plugin columns so the LF path neither builds nor parses the
  CSQ string (the third cause). Separate spec.
- Releasing the GIL in `StreamingAnnotator.__next__` (`src/annotate.rs`), which blocks
  in `block_on` with the GIL held; not a measured bottleneck today.
- A Python-side prefetch pool or a native Polars IO source: the Python loop is not the
  ceiling.
- The VCF sink's sharded path.
