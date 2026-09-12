# Contig-Prefetch Port to Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Overlap contig i+1's context prepare (data phase only, no lookup-worker
spawn) with contig i's annotation, removing the ~18.7 s (w=8) / ~30.3 s (w=16)
serial per-contig prelude from the WGS critical path on engine master.

**Architecture:** Split `prepare_contig_context` into a prefetchable
`prepare_contig_data` (identity check, schema reads, context load, grid count +
slice planning, SIFT store, shared context build — no VCF lookup workers) and an
`activate_contig_lookups` (LookupProvider build + plan scan + worker spawn) that
runs at contig start. `ContigAnnotationStream` spawns the data phase for the
next contig as a `tokio::spawn` task the moment the current contig enters
annotation, and consumes it at the next `StartContig`.

**Tech Stack:** Rust, tokio, DataFusion 53 — `datafusion-bio-functions` repo,
crate `datafusion/bio-function-vep`, single file `src/annotate_provider.rs`
(plus vepyr for verification).

## Global Constraints

- Base: `origin/master` of `github.com/biodatageeks/datafusion-bio-functions` (ce5c64a). New branch: `perf/contig-prefetch-master`.
- Working copy: create a dedicated worktree `/Users/mwiewior/research/git/dbf-prefetch` — do NOT reuse the main clone (it sits on `perf/cost-weighted-grid-balance` with untracked files) or `dbf-master` (pinned for baseline builds).
- Reference implementations (read, do not rebase — they conflict with #209's rewrite): `abf19a5` (prefetch state machine), `2829956` (= squashed `fe7bf66`, narrow data/activate split, defines `ContigPreparedData`, `prepare_contig_data`, `finish_contig_prepare`), `85dc990` (byte-budget spawn deferral).
- Env gate style must match master: unset/`"1"` = default, `"0"`/`"false"` = off (see `VEP_EARLY_WORKERS` parsing at `annotate_provider.rs:12940`). New gate: `VEP_CONTIG_PREFETCH` — default ON when `config.annotation_workers > 1`, forced ON at any worker count by `"1"`/`"true"`, OFF by `"0"`/`"false"`.
- Output must stay byte-identical with the gate on and off (prefetch is a scheduling change only; slice boundaries and annotation are untouched).
- No new crate dependencies. `cargo fmt`, `cargo clippy`, `cargo test -p datafusion-bio-function-vep` must pass after every task.
- Commit messages follow repo convention: `perf(vep): …` / `test(vep): …`.
- vepyr consumes the branch via the existing `[patch]` in `vepyr/Cargo.toml` (currently pointing at `/Users/mwiewior/research/git/dbf-master/...`; Task 5 repoints it).

---

### Task 1: Worktree + baseline

**Files:**
- No source changes. Creates the worktree and proves the baseline is green.

**Interfaces:**
- Produces: worktree at `/Users/mwiewior/research/git/dbf-prefetch` on branch `perf/contig-prefetch-master`, baseline test/clippy pass recorded.

- [ ] **Step 1: Create branch + worktree off origin/master**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-functions
git fetch origin
git worktree add -b perf/contig-prefetch-master ../dbf-prefetch origin/master
```

- [ ] **Step 2: Verify the baseline is green**

Run:
```bash
cd /Users/mwiewior/research/git/dbf-prefetch/datafusion/bio-function-vep
cargo test 2>&1 | tail -5
cargo clippy --all-targets 2>&1 | tail -3
```
Expected: all tests pass, no clippy errors. If the baseline is red, STOP and report — do not build on a broken base.

---

### Task 2: Split `prepare_contig_context` into data + activate phases (pure refactor)

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs:12715-13298` (`prepare_contig_context`)

**Interfaces:**
- Consumes: master's `prepare_contig_context(session, cache, chrom, config, full_schema) -> Result<Option<ContigReadyState>>`.
- Produces (exact signatures Task 3 relies on):

```rust
struct ContigPreparedData {
    chrom: String,
    config: ContigAnnotationConfig,          // post identity-validation (vep_semantics set)
    cache: Arc<PartitionedAnnotationCache>,
    pipeline_profile: Option<SharedContigPipelineProfile>,
    profile_handle: Option<SharedContigPipelineProfile>,
    ephemeral_tables: Vec<String>,
    shared_context: Arc<SharedContigAnnotationContext>,
    grid_slices: Vec<WorkerGridSlice>,
    stateful_parallel: bool,
    var_table: String,
    vcf_schema: Schema,                      // for the byte-budget/serial provider
    cache_schema: Schema,
    fallback_coloc_sink: ColocatedSink,
    t_contig: Instant,                       // kept for downstream contig END timing
}

async fn prepare_contig_data(
    session: Arc<SessionContext>,
    cache: Arc<PartitionedAnnotationCache>,
    chrom: String,
    config: ContigAnnotationConfig,
    full_schema: SchemaRef,
) -> Result<Option<ContigPreparedData>>;

async fn activate_contig_lookups(
    session: Arc<SessionContext>,
    data: ContigPreparedData,
) -> Result<Option<ContigReadyState>>;
```

`prepare_contig_context` keeps its signature and becomes the composition, so every existing call site is untouched in this task:

```rust
async fn prepare_contig_context(
    session: Arc<SessionContext>,
    cache: Arc<PartitionedAnnotationCache>,
    chrom: String,
    config: ContigAnnotationConfig,
    full_schema: SchemaRef,
) -> Result<Option<ContigReadyState>> {
    match prepare_contig_data(Arc::clone(&session), cache, chrom, config, full_schema).await? {
        None => Ok(None),
        Some(data) => activate_contig_lookups(session, data).await,
    }
}
```

**What moves where** (line references are master ce5c64a):

*`prepare_contig_data` keeps:* identity validation (`:12733`), vcf_only_schema, `var_table`, vcf/cache schema reads (`:12779-12796`), `load_contig_transcripts` + `count_contig_buffer_boundaries` (the `tokio::join!` at `:12971`), `tmp_provider` + engine build, SIFT store load (`:13004`), shared-ctx build start, `load_contig_context_rest` (`rest_fut`), **the grid-slice planning** (the weights + `plan_grid_partitions_balanced` / `plan_grid_partitions` part of `grid_fut`, `:13055-13112`), `SharedContextIndexes`, plugin registry, `SharedContigAnnotationContext` construction (`:13237`).

*`activate_contig_lookups` gets everything that spawns or scans lookups:*
- the up-front `LookupProvider::new` + byte-budget `spawn_lookup_partition_worker` loop and the serial `spawn_lookup_stream_worker` arm (`:12808-12889`) — this is the #208 deferral applied to master;
- from `grid_fut`: the per-slice `LookupProvider` builds, `shared_parquet_lookup_cell`, the concurrent `wp.scan(...)` plan builds (`VEP_CONCURRENT_PLANS`), and `spawn_lookup_full_contig_worker` (`:13114-13230`);
- assembly of `ContigReadyState { lookup_partitions, grid_slices, shared_context, ephemeral_tables, chrom, t_contig }` (`:13291`).

**Consequences accepted in this task** (documented in code comments):
- The `early_workers` interleave (`rest_fut`/`grid_fut` `tokio::join!` at `:13278`) collapses: `prepare_contig_data` awaits `rest_fut` and plans slices; workers spawn strictly after. `VEP_EARLY_WORKERS` becomes a no-op — delete the gate and its comment. Cost is ~0.3 s on the first contig only once Task 3 lands (every later contig's data phase is fully hidden); the reference branch made the same call.
- Profile attribution: `prepare_contig_data` records its own elapsed into `prepare_total` before returning; `activate_contig_lookups` records its elapsed into `prepare_total` too (use two local `Instant`s; do NOT use `t_contig.elapsed()` in activate — after Task 3 that span contains the previous contig's annotation).
- The `[VEP_PROFILE] ------ contig {chrom} START ------` eprintln moves from the top of `prepare_contig_data` to the top of `activate_contig_lookups`; the data phase instead emits `pipeline_trace::emit("prefetch", "data_start", ...)` / `("prefetch", "data_done", ...)` with the chrom and elapsed.

- [ ] **Step 1: Perform the split exactly as mapped above**

- [ ] **Step 2: Verify no behavior change**

Run:
```bash
cd /Users/mwiewior/research/git/dbf-prefetch/datafusion/bio-function-vep
cargo test 2>&1 | tail -5 && cargo clippy --all-targets 2>&1 | tail -3 && cargo fmt --check
```
Expected: green. This is a pure refactor; any test change is a bug in the split.

- [ ] **Step 3: Commit**

```bash
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "refactor(vep): split prepare_contig_context into data and activate phases"
```

---

### Task 3: Prefetch gate decision + failing unit test

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (near the other env-gate helpers; tests go in the file's existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `fn contig_prefetch_enabled(annotation_workers: usize, env: Option<&str>) -> bool` — pure function taking the raw env value so it is unit-testable; the call site passes `std::env::var("VEP_CONTIG_PREFETCH").ok().as_deref()`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn contig_prefetch_gate_decision_table() {
    // unset: on only for workers>1
    assert!(!contig_prefetch_enabled(1, None));
    assert!(contig_prefetch_enabled(2, None));
    assert!(contig_prefetch_enabled(16, None));
    // "0"/"false": always off
    assert!(!contig_prefetch_enabled(8, Some("0")));
    assert!(!contig_prefetch_enabled(8, Some("false")));
    // "1"/"true": on even for workers=1 (experiment override)
    assert!(contig_prefetch_enabled(1, Some("1")));
    assert!(contig_prefetch_enabled(1, Some("true")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test contig_prefetch_gate_decision_table`
Expected: FAIL — `contig_prefetch_enabled` not defined.

- [ ] **Step 3: Implement**

```rust
/// VEP_CONTIG_PREFETCH gate: prefetch the next contig's data phase during the
/// current contig's annotation. Default ON for parallel annotation (workers>1),
/// where the idle prelude is on the critical path; "1"/"true" forces it on for
/// workers=1 experiments, "0"/"false" disables it everywhere.
fn contig_prefetch_enabled(annotation_workers: usize, env: Option<&str>) -> bool {
    match env {
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false") => false,
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true") => true,
        _ => annotation_workers > 1,
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test contig_prefetch_gate_decision_table`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): add VEP_CONTIG_PREFETCH gate decision"
```

---

### Task 4: Prefetch state machine in `ContigAnnotationStream`

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` — `ContigAnnotationStream` struct (`:10725` area), `poll_next` state loop (`:12148-12250` area), cleanup (`cleanup_registered_tables_on_drop`, `:10753`).

**Interfaces:**
- Consumes: `prepare_contig_data`, `activate_contig_lookups`, `contig_prefetch_enabled` (Tasks 2–3, exact signatures above).
- Produces: no new public API; behavioral feature gated by `VEP_CONTIG_PREFETCH`.

**Design (ported from `abf19a5`, adapted to the narrow split):**

New stream field:

```rust
/// In-flight prefetch of the NEXT contig's data phase (spawned when the
/// current contig enters annotation; depth is always <= 1). The contig name
/// rides along so cleanup can log what was discarded. The task spawns no
/// lookup workers (data phase only), so aborting it leaks nothing.
prefetched_data: Option<(String, tokio::task::JoinHandle<Result<Option<ContigPreparedData>>>)>,
```

Helper methods on `ContigAnnotationStream`:

```rust
fn start_prefetch_next_contig(&mut self) {
    if self.prefetched_data.is_some() {
        return; // depth stays at one
    }
    if !contig_prefetch_enabled(
        self.config.annotation_workers,
        std::env::var("VEP_CONTIG_PREFETCH").ok().as_deref(),
    ) {
        return;
    }
    // Pop the contig NOW so StartContig cannot double-prepare it.
    let Some(chrom) = self.contigs.pop_front() else { return };
    let session = Arc::clone(&self.session);
    let cache = Arc::clone(&self.cache);
    let config = self.config.clone();
    let full_schema = self.full_schema.clone();
    let prefetch_chrom = chrom.clone();
    let handle = tokio::spawn(async move {
        prepare_contig_data(session, cache, prefetch_chrom, config, full_schema).await
    });
    self.prefetched_data = Some((chrom, handle));
}

/// Build the PrepareFuture for StartContig: consume the prefetch if one is in
/// flight, otherwise run the full prepare inline (first contig, gate off).
fn next_prepare_future(&mut self, chrom_from_queue: Option<String>) -> Option<PrepareFuture> { ... }
```

State-loop wiring:
1. `StartContig` (`:12150`): if `self.prefetched_data` is `Some((chrom, handle))`, take it and build the future as `Box::pin(async move { let data = handle.await.map_err(|e| DataFusionError::Execution(format!("contig prefetch task failed: {e}")))??; match data { None => Ok(None), Some(d) => activate_contig_lookups(session, d).await } })`; do NOT pop from `self.contigs` (the prefetch already popped it). Otherwise pop from `self.contigs` and use `prepare_contig_context` as today. The LIMIT-pushdown early-return must run BEFORE consuming the prefetch, and must abort a pending handle when it bails.
2. On entering annotation for the current contig — both the serial transition to `AnnotatingContig` and the sharded transition to `AnnotatingParallel` (the `Poll::Ready(Ok(Some(mut ready)))` arm at `:12183`) — call `self.start_prefetch_next_contig()` immediately after the transition succeeds.
3. Cleanup: in `cleanup_registered_tables_on_drop` and in the error-cleanup transitions, `if let Some((_, handle)) = self.prefetched_data.take() { handle.abort(); }`. Also abort in the `Done` transitions of `StartContig` (limit reached / queue empty).
4. `ContigAnnotationStream::new` (`:10732`): initialize `prefetched_data: None`.

Error semantics (document in a comment): a prefetch failure surfaces when its contig STARTS, preserving today's user-visible ordering; the current contig's annotation is never interrupted by a bad next contig.

- [ ] **Step 1: Implement the field, helpers, and wiring exactly as above**

- [ ] **Step 2: Verify compile + existing tests**

Run: `cargo test 2>&1 | tail -5 && cargo clippy --all-targets 2>&1 | tail -3`
Expected: green.

- [ ] **Step 3: Commit**

```bash
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "perf(vep): prefetch the next contig's data phase while the current one annotates"
```

---

### Task 5: Wire vepyr to the branch and run the golden parity tests

**Files:**
- Modify: `/Users/mwiewior/research/git/vepyr/Cargo.toml` (the `[patch]` path)

**Interfaces:**
- Consumes: the built branch worktree from Task 4.
- Produces: a vepyr `.venv` build running the prefetch engine; golden tests green.

- [ ] **Step 1: Repoint the patch and rebuild**

In `/Users/mwiewior/research/git/vepyr/Cargo.toml` change the patch line to:

```toml
[patch."https://github.com/biodatageeks/datafusion-bio-functions.git"]
datafusion-bio-function-vep = { path = "/Users/mwiewior/research/git/dbf-prefetch/datafusion/bio-function-vep" }
```

Run:
```bash
cd /Users/mwiewior/research/git/vepyr
env -u CONDA_PREFIX -u CONDA_DEFAULT_ENV uv run maturin develop --release 2>&1 | tail -3
```
Expected: `🛠 Installed vepyr-0.3.0`. (Both `VIRTUAL_ENV` and `CONDA_PREFIX` being set breaks maturin — the `env -u` is required on this machine.)

- [ ] **Step 2: Run vepyr's golden + annotate tests**

Run:
```bash
cd /Users/mwiewior/research/git/vepyr
uv run pytest tests/test_golden.py tests/test_annotate.py -x -q 2>&1 | tail -5
```
Expected: PASS. These exercise real multi-contig annotation against fixture caches — the closest thing to an engine integration test.

- [ ] **Step 3: Commit (engine repo bookkeeping only — the vepyr patch stays uncommitted)**

No engine commit here; the vepyr `Cargo.toml` patch is machine-local and must NOT be committed to vepyr (it carries the existing "TEMPORARY" comment).

---

### Task 6: Byte-identical A/B on the WGS input

**Files:**
- No source changes. Uses `/Users/mwiewior/workspace/data_vepyr` inputs and the sweep runner pattern from the scalability session.

- [ ] **Step 1: Hash a gate-OFF run**

```bash
cd /Users/mwiewior/research/git/vepyr
out=/Users/mwiewior/workspace/data_vepyr/output/116/prefetch_ab/off.vcf
mkdir -p "$(dirname $out)"; rm -f "$out"
VEP_CONTIG_PREFETCH=0 .venv/bin/python -P - "$out" <<'EOF'
import sys, vepyr
vepyr.annotate(
    vcf="/Users/mwiewior/workspace/data_vepyr/input/HG002_normalized.vcf.gz",
    cache_dir="/Users/mwiewior/workspace/data_vepyr/cache/116_GRCh38_merged",
    everything=True, hgvs=True, workers=8, compression="plain",
    reference_fasta="/Users/mwiewior/workspace/data_vepyr/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa",
    output_vcf=sys.argv[1], show_progress=False)
EOF
shasum -a 256 "$out" | tee /tmp/prefetch_off.sha; rm -f "$out"
```

(Delete between runs — the disk has ~65 GiB free and each plain output is 29.4 GiB.)

- [ ] **Step 2: Hash a gate-ON run and compare**

Same command with `VEP_CONTIG_PREFETCH=1` and `on.vcf`.
Expected: identical SHA-256. If the hashes differ, STOP — the split changed behavior; bisect Tasks 2/4 before proceeding.

---

### Task 7: Instrumented re-measurement (acceptance)

**Files:**
- No source changes. Reuses `run_one_scaling.py` / `timestamp_lines.py` from the scalability session (scratchpad) or re-creates them — they call `vepyr.annotate(..., workers=N, show_progress=False)` with `VEP_PROFILE=1` and timestamp stderr lines.

**Acceptance criteria (all from the 2026-08-11 baseline on this machine):**
- w=8 annotation wall: **≤ ~66 s** (baseline 77.9 s; ~18.7 s of prepare was serial, first contig still pays its own).
- Per-contig activation gap (time between the previous contig's `pipeline_profile` line and the next `------ contig X START ------`, which now marks activation): **< 0.5 s** for every contig after the first.
- `max_rss_gb` at w=8: **≤ baseline + 2.0 GB** (baseline 7.4 GB; the prefetch holds one extra contig's context, ~1.7 GB measured on the old branch).
- Output record count 4,096,123 (checked in Task 6 by the golden/hash runs).

- [ ] **Step 1: Run w=8 and w=16 with VEP_PROFILE=1, gate default (on)**

- [ ] **Step 2: Parse the timestamped log** — per-contig activation gaps + `prepare_total` sums, exactly as in the scalability analysis (contig START = activation now; `prefetch data_start/data_done` trace events give the overlapped span).

- [ ] **Step 3: Record the numbers** in the PR description and update the project memory note `vepyr-worker-scaling-t4-decomposition` (mark cause 1 as fixed, with measured before/after).

If acceptance fails: check first that the prefetch actually overlapped (the `prefetch data_done` trace timestamp must land inside the previous contig's annotation window). The known failure mode is polling the JoinHandle only on stream poll — the prefetch MUST be `tokio::spawn`ed (Task 4), not stored as an unspawned future.

---

### Task 8: Push + PR

- [ ] **Step 1: Push the branch**

```bash
cd /Users/mwiewior/research/git/dbf-prefetch
git push -u origin perf/contig-prefetch-master
```

- [ ] **Step 2: Open the PR against master**

```bash
gh pr create --title "perf(vep): prefetch the next contig's data phase (WGS w=8: 78s -> <measured>s)" \
  --body "$(cat <<'EOF'
Port of the contig-prefetch narrow split (#206/#208 lineage) onto the post-#209
master. Splits prepare_contig_context into a prefetchable data phase and an
activation phase; the stream prefetches contig i+1's data phase during contig
i's annotation. Gate: VEP_CONTIG_PREFETCH (default on for workers>1).

Measured (HG002 WGS, merged 116, plain, 16-core M-series):
- <w=8 / w=16 before/after table from Task 7>
- Byte-identical output gate on/off (SHA-256, Task 6).
- RSS delta: <measured> (bounded by one contig's context; the narrow split
  avoids the +0.31 GB/worker term of the full prefetch).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage:** cause-1 fix = cross-contig overlap of the prepare data phase; Tasks 2+4 implement it, 3 gates it, 5–7 verify parity and the win, 8 lands it. The byte-budget deferral (#208 content) is folded into Task 2's activate phase, so Ensembl-cache workers>1 benefits too.
- **Known trade-offs stated:** `VEP_EARLY_WORKERS` interleave removed (first-contig-only cost); +~1.7 GB steady-state RSS with the gate on; prefetch errors surface at the failing contig's start.
- **Out of scope (explicitly):** cause 2 (intra-contig utilization / `VEP_GRID_BALANCE` default), cause 3 (engine CPU inflation), prefetch depth > 1, and prefetching the SIFT store lazily. Do not fold these in.
