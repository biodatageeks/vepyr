# Handover: Polars supplement benchmarks, plugin performance fix, LazyFrame ceiling

Date: 2026-09-26. Written at the end of a long session whose context ran out. Start here.

## TL;DR: what to do next, in order

1. **LazyFrame consumer-ceiling design:** continue the brainstorm. The priority is agreed; the approach is not chosen yet. See section 4.
2. **Verify the engine at `a144cf0`:** build vepyr against it locally and re-run the plugin parity check. See section 3.
3. **Supplement:** add the Figure 1 caption about the LF plateau, and write the benchmark README. See section 2.
4. **The user merges** vepyr #131, bio-functions #261, vepyr #132 and vepyr #133. Never merge them yourself. After #261 merges, re-pin #132 to the merge commit, and later to a release tag.

## 1. Where things are

| Thing | Location |
|---|---|
| Benchmark branch | `perf/polars-supplement` (main checkout `~/research/git/vepyr`), draft PR **biodatageeks/vepyr#133** |
| Spec / plan | `docs/superpowers/specs/2026-09-24-polars-supplement-benchmarks-design.md`, `docs/superpowers/plans/2026-09-24-polars-supplement-benchmarks.md` |
| Progress ledger (full history, rulings) | `.superpowers/sdd/2026-09-24-polars-supplement-benchmarks/progress.md` (git-ignored) |
| Results | `performance-tests/polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_20260924/`: A, B, B_sliced, C, C_old_build_pre_fix, vep, csv, verify.json, build.json |
| Large data (not in git) | `~/workspace/data_vepyr/polars_integration/chr22/` (input, VEP outputs, plugin slices, run logs) |
| Paper supplement | `~/research/git/papers/vepyr/supplementary/`, pushed to origin/main as `6e62eb6`; Overleaf-synced |
| Plugin-fix worktrees | `~/research/git/_wt/vepyr-plugin-csq` (#131), `~/research/git/_wt/functions-plugin-take` (#261), `~/research/git/_wt/vepyr-bump-plugin-take` (#132) |
| Fixed-build Python used for C and B | `~/research/git/_wt/vepyr-plugin-csq/.venv/bin/python`. It holds vepyr #131 plus engine cd38a19 through an uncommitted Cargo `[patch]` that has since been removed from Cargo.toml. The installed .so still carries it. **Do not `uv sync` in that worktree** or the build is lost. |

The user's uncommitted edits to `notebooks/annotate.ipynb` and `notebooks/build_cache.ipynb` live in the main checkout. Never stage them.

## 2. Supplement benchmarks: status

All experiments are done on HG002 chr22 (50,861 records), release 116, median of 3 after a warm-up. Pairing is VEP `--fork N` against vepyr `workers=N+1`.

- **Parity gate:** 18/18 queries match `filter_vep` on the fixed build (`verify.json`). The only reported-not-gated difference is that polars-bio decodes HGVSp `%3D` to `=`.
- **A (filter only):** Polars is 16–57× faster than filter_vep (median 40×).
- **B (end to end):** vepyr is 61–154× faster than VEP + filter_vep (median 97×).
  - Core queries at 1/8 processes: VEP 419/101 s, vepyr 3.0/1.2 s.
  - Plugin queries: VEP 1220/253 s, vepyr 8.2/4.1 s.
- **B_sliced (the fair region comparison):** R1 23–59×; R2/R3 only 2–6×. The whole-chromosome region rows (~1,100×) are the naive VEP workflow and are labelled as such.
- **C (pushdown):**
  - Region: 3–8× at w1, 1.2–1.6× at w8.
  - Projection: up to 1.8× on core queries and up to 4.7× on plugin queries.
- **VEP matrix:**
  - Core: 376.9 / 185.1 / 101.3 / 58.5 s.
  - Plugins: 1148.1 / 613.5 / 326.3 / 180.9 s.
  - Only plugins `--fork 7` drifts from serial VEP (93 lines).

**Open items:**
- Figure 1 needs a caption. The LF plugin curve flattens at 4→8 workers; the cause is section 4.
- Write `performance-tests/polars-integration/README.md` (reproduction block, deviations, parity summary, load guard = 4).
- Re-run C and B once #131, #261 and #132 are merged and released. They currently use a local build; see `build.json`.
- Decide on chr1.

## 3. Plugin slowdown fix: status

Root cause (ablation plus `sample`):
- **The main cost** was vepyr re-parsing the whole CSQ string once per plugin field, 38 times per batch.
- **The secondary cost** was the engine taking plugin lookups serially.

| PR | What | State |
|---|---|---|
| vepyr **#131** `perf/plugin-csq-single-parse` @ 9501251 | parse CSQ once; parse only the read plugin columns; hand the engine only the plugins that are read | green, both reviewers clean, draft |
| bio-functions **#261** `perf/plugin-take-concurrent` @ a144cf0 | plugin timer (`VEP_ENGINE_PROFILE` `plugin_take=`, `[VEP_PLUGIN_PROFILE]`); concurrent takes on a dedicated fork-safe pool (`VEP_PLUGIN_TAKE_THREADS`) | green, Codex clean, draft |
| vepyr **#132** `perf/bump-engine-plugin-take` @ bd7852d | pins #261 head a144cf0 | green, draft |

**Results on chr22:**
- LF + 5 plugins: 24.3 s / 8.1 GiB → about 9 s / 4 GiB.
- `select(CADD_PHRED)`: 19.8 → 5.9 s.
- output_vcf: w1 9.0 → ~7.5 s; w4 3.5 → ~3.1 s.
- Parity is byte-identical to VEP 116 throughout. That was verified at cd38a19; **it has not been re-verified at a144cf0**, whose fork-safety change leaves output unchanged per the engine agent.

The CADD chr22 take reads 874 of its 994 MB shard. That is the plugin floor, and its fix is the cache layout, which is out of scope.

Evidence scripts, throwaway: the session scratchpad `plugin_debug/` (`ablate.py`, `ablate2.py`, `bench_plugins.py` (a copy is in `e2e-testing/results/fix-20260925-0942/`), `lf_test.py`, `lf_csq_noplug.py`, `w_test.py`). Baseline and after timings: `e2e-testing/results/fix-20260925-0942/{baseline,after}/`.

## 4. LazyFrame consumer ceiling: design brainstorm (in progress)

**Finding:** LF paths stop scaling at about 4 workers, even with **no plugins and no CSQ**.
- Unfiltered collect: 1.62 s at w4, 1.67 s at w8.
- With plugins: about 3.4–3.9 s.
- output_vcf keeps scaling: 3.0 → 2.4 s with plugins.
- Ruled out by measurement: the engine plugin pool size (4 vs 8 threads makes no difference), and the Python plugin parse (`select(CSQ)` without parsing also plateaus).
- **Cause:** the single Python loop in `src/vepyr/__init__.py` `_batch_source`. It does from_arrow, renames, parse, projection, filter and yield, one batch at a time.

**The user chose:** the priority is scaling `collect()` and the sinks. Constraints: identical values and batch order, the same Python API, and build on the current engine and Polars IO-plugin design. Assumed target: collect at w8 within ~20% of output_vcf at w8.

**Approaches presented (none chosen yet):**
- **A.** An order-preserving thread-pool prefetch for per-batch conversion in Python. vepyr only; it works only if the heavy ops release the GIL. Recommended first.
- **B.** Move renames, projection and pruning into the engine, so the single loop gets cheaper.
- **C.** A native Rust Polars IO source (pyo3-polars). It removes the ceiling but has the largest cost.

**Next step:** profile one w8 collect. Split each batch's time into from_arrow / column ops / filter / engine wait, and measure how much of it holds the GIL. Choose A or B with the user, write the spec at `docs/superpowers/specs/`, get it reviewed, then writing-plans. It is architectural, so follow the full brainstorming path.

## 5. Traps learned this session

- `uv run` needs `env -u CONDA_PREFIX`. Measured builds use `RUSTFLAGS="-C target-cpu=native" uv sync` (release).
- The pre-commit ruff-format hook aborts commits. Re-stage, re-commit, and check `git log -1` before citing a SHA.
- The pre-commit stash/restore can clobber a tracked file that a background job is writing (this lost verify.json once). Don't commit while a job writes tracked files.
- Port 8765 is taken by an unrelated server; rclone for plugin sources uses 18765.
- A stacked PR merged right after its base lands on the stale branch. Check ancestry to master.
- The host is shared: other projects' Docker containers (marquez, Oracle, bigquery-emulator) start on their own and contaminate VEP timings. Check `docker ps` and the load before timing.
- zsh: `set -- $var` does not word-split. Loop over explicit words.

Memory files: `vepyr-lf-plugin-csq-reparse-slowdown`, `vepyr-lf-consumer-ceiling-design`, `vepyr-polars-supplement-spec`, `stacked-pr-merge-lands-on-stale-base`.
