# Narrow Contig-Prefetch Split — Implementation Plan

**Goal:** Split the prefetched unit so the next contig's **context** is built ahead of
time but its **N lookup workers are not spawned** until the contig actually starts.
Recovers ~2.5 GB of the prefetch's peak-RSS cost for ~4.2s of its time win.

**Where:** `datafusion-bio-functions`, `datafusion/bio-function-vep/src/annotate_provider.rs`.
Branch off `perf/parallelize-contig-prelude` (currently `abf19a5`, pushed).
vepyr consumes it via the temporary `[patch]` already in `vepyr/Cargo.toml`.

---

## Why (all numbers measured on HG002, 4,096,123 variants, merged 116 cache, plain output)

`abf19a5` prefetches the next contig's whole `prepare_contig_context` while the
current contig annotates. That hid 12.71s of a 24.3s per-contig prelude:

| workers | before prefetch | with prefetch |
|---|---|---|
| 1 | 04:20 / 3.88 GB | 04:22 / 3.88 GB (gated off, control) |
| 2 | 02:31 / 4.66 GB | 02:18 / 6.58 GB |
| 4 | 01:38 / 5.75 GB | 01:25 / 9.06 GB |
| 8 | 01:15 / 7.57 GB | 01:04 / 11.38 GB |

The +3.81 GB at w=8 is a clean measurement (both builds annotate all 22 contigs;
prefetch is the only difference). It decomposes as:

- **~1.7 GB N-independent** — one contig's context. Measured same-phase: chr2's
  `after_context_load` peak sits 1744 MB above chr1's `after_prepare`.
- **~0.31 GB per worker** — from (3.81 − 1.92) / 6 workers. Closely matches the
  independently measured 0.286 GB/worker baseline slope, i.e. prefetch duplicates
  the *per-worker* startup footprint because `prepare_contig_context` ends by
  spawning N lookup workers.

`prepare_contig_context` internals, instrumented via the `prepare done` trace event
added in `86d790a` (WGS w=8, summed over 22 contigs):

| component | per contig | total |
|---|---|---|
| context load (shards, parse, indexes, boundary count) | 0.157–0.532s | **8.50s** |
| provider build + lookup-worker spawn | 0.154–0.281s (flat) | **4.21s** |

So deferring the second component costs 4.21s and should recover the ~0.31 GB × N
term. Projected at w=8: **~68s at ~8.9 GB**, vs 64s/11.38 GB (full prefetch) and
75s/7.57 GB (no prefetch) — keeps most of both the prelude win and the page-index
win from `6d91488`.

> An earlier note in this investigation claimed the deferred part was worth only
> 736 MB and rejected the split on that basis. That figure came from the
> `after_sift_load → after_prepare` peak-RSS delta, which understates it because
> peak RSS is monotonic and cannot separate concurrent contributors. The 0.31 GB/worker
> figure above supersedes it.

---

## Split point

`prepare_contig_context` starts at `annotate_provider.rs:12580`.

Split at **`:12977`**, where `vcf_has_chr` is resolved and the grid loop begins.

- **Data phase (prefetchable):** everything above `:12977` — identity validation,
  variation schema read, `load_contig_context` joined with
  `count_contig_buffer_boundaries`, SIFT store, `SharedContextIndexes`,
  `transcript_cache_regions`, `compute_overlap_width_bp`.
- **Deferred phase:** the `if stateful_parallel { … }` block through **`:13030`**
  (`grid_slices = slices;` plus the `lookup_partitions.len()` profile record), which
  builds N `LookupProvider`s and calls `spawn_lookup_full_contig_worker`.

### Values the deferred phase captures

Carry these in one `ContigPreparedData` struct returned by the data phase:

1. `session: Arc<SessionContext>`
2. `config: ContigAnnotationConfig`
3. `chrom: String`
4. `var_table` (variation table name)
5. `grid_vcf_schema`
6. `grid_cache_schema`
7. `task_ctx` (or rebuild from `session.task_ctx()` in the deferred phase)
8. `shared_parquet_lookup_cell: Arc<ParquetVariationLookupCell>` (from `6d91488`)
9. `vcf_has_chr: bool` (hoisted in `86d790a` — keep it resolved once per contig)
10. `boundaries` + `overlap_width_bp` (or the already-computed `slices`)

Plus everything currently placed into `ContigReadyState` / `SharedContigAnnotationContext`
so the deferred phase can finish constructing it.

## Wiring — no state-machine changes needed

Both `PrepareFuture` construction sites live in `StreamState::StartContig`. Compose
the two phases inside the future at each site:

```rust
// prefetch path (take_prefetched_prepare)
Box::pin(async move {
    let data = handle.await.map_err(|e| DataFusionError::External(Box::new(e)))??;
    finish_contig_prepare(data).await          // spawns lookups now, not during prefetch
})

// cold path
Box::pin(async move {
    let data = prepare_contig_data(session, cache, chrom, config, full_schema).await?;
    finish_contig_prepare(data).await
})
```

`start_prefetch_next_contig` then spawns only `prepare_contig_data`. `StreamState`,
`AnnotatingParallel`, shard-id assignment (`next_global_shard_id`, assigned at the
`PreparingContig → AnnotatingParallel` transition) and the assembler are untouched.

Keep the `prepare done` trace emit at the end of the **deferred** phase so the
split stays measurable, and consider a second emit at the end of the data phase.

## Gotchas

- `abf19a5` gates prefetch to `annotation_workers > 1` (the call sits inside that
  branch). Preserve that — at w=1 the prelude is a small share of a much longer
  contig and a second resident context is not worth the memory.
- `ephemeral_tables` is `Vec::new()` at `:12622` and never pushed to, so there is no
  session-table collision between two in-flight contigs. If that ever changes, names
  must become contig-unique before prefetching anything.
- The prefetch handle must still be aborted on drop (`cleanup_registered_tables_on_drop`)
  — with the split it holds no spawned lookup tasks, but abort is still correct.

## Verification

Byte-identical output is the bar; the harness already exists in the session scratchpad.

1. `chr21` @ w=1 and w=8, `chr1` @ w=16 — body MD5 must match
   `aa686ea955e2c561bda37bc2f1d88092` (chr21) and `230e0c5c792e9ecd5a7c537d9d664219` (chr1).
2. 5-contig input (chr1, chr2, chr20, chr21, chr22) @ w=16 — extract each contig and
   compare against its standalone MD5. This is the test that actually exercises
   prefetch overlap; `subset_md5.sh` does the extraction.
3. WGS @ w=4 and w=8 via `performance-tests/vepyr/scripts/benchmark_vepyr_once.py`
   (`--expected-records 4096123`) for peak RSS plus the VEP record-parity check.
   Output must stay 29,410,585,470 bytes.
4. `cargo clippy` clean, `cargo test --lib` (900 tests) green, `cargo fmt --check`.

Note `everything=True` implies `check_existing`, so the colocated path is already
covered by the MD5 checks.

## Measurement discipline (learned the hard way in this investigation)

Peak RSS is monotonic, so a delta between two phase boundaries cannot attribute
memory to one of several concurrent contributors. Four wrong attributions were made
that way in this investigation. **Prefer ablation** — vepyr exposes `check_existing`,
`af_gnomade`, `af_gnomadg`, `max_af`, `hgvs`, `everything` as explicit flags, and a
two-run A/B on a flag settles ownership in minutes. A one-flag ablation showed the
co-located + AF path costs ~5 MB, refuting a hypothesis that had produced a whole
(reverted) commit; that patch is preserved as `colocated_prune.patch` in the session
scratchpad but should not land.

`dhat-heap` is wired as a global allocator in `vepyr/src/lib.rs:33-35` but **no
`dhat::Profiler` is ever created**, so the feature currently collects nothing. Closing
that gap is the prerequisite for real allocation-site attribution.
