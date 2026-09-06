# Region Pushdown: Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Teach `annotate_vep()` a `regions` option so the streaming (LazyFrame) path prepares only the selected contigs, looks up only the selected position windows, and stays exact on Merged/RefSeq caches by aligning each range to the input-buffer grid with warm-up replay.

**Architecture:** A new pure module `regions.rs` owns option parsing, contig resolution, buffer-range mapping, the rank gate and the output trim. `annotate_provider.rs` gains per-contig *runs*: the contig context is prepared once, then each run activates its own filtered lookup, gates rows through warm-up/emit/rank-stop, and trims emitted rows to the run's bounds before projection. Without `regions` every contig has exactly one open run and no code path changes.

**Tech Stack:** Rust 2021, DataFusion 53 (`Expr`, `col`, `lit`, `TableProvider::scan`), Arrow (`filter_record_batch`, `cast`), tokio, serde_json. Crate `datafusion-bio-function-vep` at `~/research/git/datafusion-bio-functions/datafusion/bio-function-vep`.

**Spec:** `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md` (in the vepyr repo). Read it first; the plan argues from it.

## Global Constraints

- Work in the engine clone `~/research/git/datafusion-bio-functions` on a new branch `feat/regions-option` created from `origin/master` (`44f5954`, the current vepyr pin). Run `git fetch origin` first.
- Every task's tests run with `cargo test -p datafusion-bio-function-vep` (the `parquet-cache` feature is on by default). `cargo clippy -p datafusion-bio-function-vep --all-targets` and `cargo fmt` must be clean before each commit.
- `regions` coordinates are 1-based closed on the `start` column, the same coordinate system the VCF provider emits when built with `coordinate_system_zero_based = false`. No conversion anywhere.
- Contract: with `regions` the output must equal the unrestricted output filtered to `start` inside the regions, row for row, for every cache source. Extraction and planning may over-read; they must never under-read.
- `regions` combined with a VCF shard context (sharded VCF output) is a `DataFusionError::Plan`.
- Without `regions` the default path must not run the count pass, must not build a gate, and must not change output.
- Commit messages follow the repo's conventional-commit style (`feat(vep): ...`, `test(vep): ...`) and end with the session trailer from the harness instructions.

---

## File structure

| File | Responsibility |
|---|---|
| `src/regions.rs` (new) | Pure: `RegionSpec` parsing/validation, `RunBounds`, merging, resolution to VCF contig spellings, contig restriction, buffer-range mapping, `RunGate`, `filter_batch_to_bounds`. No DataFusion session access. |
| `src/lib.rs` | `pub(crate) mod regions;` |
| `src/annotate_provider.rs` | Option storage on `AnnotateProvider`, contig restriction in `scan_with_transcript_engine_partitioned`, `ContigAnnotationConfig.regions`, run planning in `prepare_contig_data`, `activate_run_lookup`, gate in `apply_lookup_batch_message`, `ActivatingRun` state, output trim in `annotate_worker_window`. |
| `src/annotate_provider.rs` `mod tests` | Warm-up/gate composition parity test on the LRIF1 donation fixture. |

Reference points in `annotate_provider.rs` at `44f5954` (line numbers drift as you edit; search for the quoted identifiers):

- `AnnotateProvider` struct: `pub struct AnnotateProvider {` (~3636); constructor `pub(crate) fn new(` (~3664).
- Contig discovery and selection: `let vcf_contigs = self.discover_vcf_contigs().await?;` (~5613) and `select_cache_backed_contigs(` (~5622).
- `ContigAnnotationConfig` struct (~9067) and its construction `let config = ContigAnnotationConfig {` (~5690).
- `GridBufferBoundary`, `WorkerGridSlice`, `build_grid_slices` (~10285-10400).
- `ContigPreparedData` (~10589), `ContigReadyState` (~10623), `ContigAnnotationState` (~10640).
- `StreamState` enum (~10772).
- `apply_lookup_batch_message` (~12598), `poll_lookup_partitions` (~12715).
- Contig-done transition: comment `// No window to produce and nothing in flight — contig done.` (~13239).
- `AwaitingWindow` / `DrainingWindow` arms (~13262-13360).
- `prepare_contig_data` (~13478): `stateful_parallel` (~13573), `count_fut` (~13630), `compute_overlap_width_bp` (~13704), `grid_slices` planning (~13731-13792).
- `activate_contig_lookups` (~13884): provider build with `set_vcf_filter` (~13920-13940), `parallel_lookup` arm (~13941-13990).
- `annotate_window_owned` (~12541), `annotate_worker_window` (~12420).
- `warm_up_worker_state` (~12399), `run_maybe_block_in_place`.
- Test helpers: `minimal_contig_annotation_config` (~14750), `make_tx` (~16168), `make_buffer_batch_many` (~16601), `warmup_reconstructs_serial_persisted_state` (~16445).

---

### Task 1: `regions.rs` — option parsing, `RunBounds`, merging

**Files:**
- Create: `datafusion/bio-function-vep/src/regions.rs`
- Modify: `datafusion/bio-function-vep/src/lib.rs` (add `pub(crate) mod regions;` next to `pub(crate) mod ordered_drain;`)

**Interfaces:**
- Produces:
  ```rust
  pub(crate) struct RegionSpec { pub chrom: String, pub start: Option<i64>, pub end: Option<i64> }
  pub(crate) fn parse_regions_option(options_json: Option<&str>) -> Result<Option<Vec<RegionSpec>>>
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub(crate) struct RunBounds { pub lo: Option<i64>, pub hi: Option<i64> }
  impl RunBounds { pub const OPEN: RunBounds; pub fn contains(&self, pos: i64) -> bool; pub fn is_open(&self) -> bool }
  pub(crate) fn merge_bounds(bounds: Vec<RunBounds>) -> Vec<RunBounds>
  ```

- [ ] **Step 1: Create the branch**

```bash
cd ~/research/git/datafusion-bio-functions
git fetch origin
git switch -c feat/regions-option origin/master
git log --oneline -1   # expect 44f5954
```

- [ ] **Step 2: Write the failing tests**

Create `datafusion/bio-function-vep/src/regions.rs` with only the test module for now:

```rust
//! Genomic-region restriction for `annotate_vep()`: option parsing, run
//! planning over the input-buffer grid, and the row gate that keeps a range
//! cut exact on stateful (Merged/RefSeq) caches. Pure code; no session access.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_absent_and_present() {
        assert_eq!(parse_regions_option(None).unwrap(), None);
        assert_eq!(parse_regions_option(Some(r#"{"everything":true}"#)).unwrap(), None);
        let specs = parse_regions_option(Some(
            r#"{"regions":[{"chrom":"chr1","start":10,"end":20},{"chrom":"chr2"},{"chrom":"chrX","start":5},{"chrom":"chrY","end":9}]}"#,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(specs.len(), 4);
        assert_eq!(specs[0], RegionSpec { chrom: "chr1".into(), start: Some(10), end: Some(20) });
        assert_eq!(specs[1], RegionSpec { chrom: "chr2".into(), start: None, end: None });
        assert_eq!(specs[2], RegionSpec { chrom: "chrX".into(), start: Some(5), end: None });
        assert_eq!(specs[3], RegionSpec { chrom: "chrY".into(), start: None, end: Some(9) });
    }

    #[test]
    fn parse_rejects_bad_shapes() {
        for bad in [
            r#"{"regions":"chr1"}"#,
            r#"{"regions":[]}"#,
            r#"{"regions":[{"start":1}]}"#,
            r#"{"regions":[{"chrom":""}]}"#,
            r#"{"regions":[{"chrom":"chr1","start":0}]}"#,
            r#"{"regions":[{"chrom":"chr1","start":"5"}]}"#,
            r#"{"regions":[{"chrom":"chr1","start":9,"end":8}]}"#,
            r#"{"regions":[{"chrom":"chr`1"}]}"#,
        ] {
            let err = parse_regions_option(Some(bad)).unwrap_err().to_string();
            assert!(err.contains("regions"), "{bad}: {err}");
        }
    }

    #[test]
    fn bounds_contains_and_open() {
        assert!(RunBounds::OPEN.is_open());
        assert!(RunBounds::OPEN.contains(1));
        let b = RunBounds { lo: Some(10), hi: Some(20) };
        assert!(b.contains(10) && b.contains(20));
        assert!(!b.contains(9) && !b.contains(21));
        assert!(RunBounds { lo: None, hi: Some(5) }.contains(1));
        assert!(!RunBounds { lo: Some(5), hi: None }.contains(4));
    }

    #[test]
    fn merge_bounds_sorts_and_merges_overlapping_and_adjacent() {
        let merged = merge_bounds(vec![
            RunBounds { lo: Some(30), hi: Some(40) },
            RunBounds { lo: Some(10), hi: Some(20) },
            RunBounds { lo: Some(21), hi: Some(25) }, // adjacent to 10-20
            RunBounds { lo: Some(35), hi: Some(50) }, // overlaps 30-40
        ]);
        assert_eq!(
            merged,
            vec![RunBounds { lo: Some(10), hi: Some(25) }, RunBounds { lo: Some(30), hi: Some(50) }]
        );
    }

    #[test]
    fn merge_bounds_open_sides_absorb() {
        let merged = merge_bounds(vec![
            RunBounds { lo: Some(100), hi: Some(200) },
            RunBounds { lo: None, hi: Some(50) },
            RunBounds { lo: Some(150), hi: None },
        ]);
        assert_eq!(
            merged,
            vec![RunBounds { lo: None, hi: Some(50) }, RunBounds { lo: Some(100), hi: None }]
        );
        assert_eq!(merge_bounds(vec![RunBounds::OPEN, RunBounds { lo: Some(1), hi: Some(2) }]), vec![RunBounds::OPEN]);
    }
}
```

Add to `src/lib.rs`, after `pub(crate) mod ordered_drain;`:

```rust
pub(crate) mod regions;
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: compile error, `parse_regions_option` and `RunBounds` not found.

- [ ] **Step 4: Implement**

Above the test module in `regions.rs`:

```rust
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

/// One `regions` entry as given in `options_json`, 1-based closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionSpec {
    pub chrom: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

fn plan_err(msg: String) -> DataFusionError {
    DataFusionError::Plan(format!("annotate_vep(): regions {msg}"))
}

fn optional_coordinate(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Option<i64>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| plan_err(format!("'{key}' must be an integer")))?;
            if v < 1 {
                return Err(plan_err(format!("'{key}' must be >= 1, got {v}")));
            }
            Ok(Some(v))
        }
        Some(_) => Err(plan_err(format!("'{key}' must be an integer"))),
    }
}

/// Parse and validate the `regions` option. `Ok(None)` when absent.
pub(crate) fn parse_regions_option(options_json: Option<&str>) -> Result<Option<Vec<RegionSpec>>> {
    let Some(raw) = options_json else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(raw).map_err(|e| {
        DataFusionError::Plan(format!("annotate_vep() options_json must be valid JSON: {e}"))
    })?;
    let Some(entries) = value.get("regions") else {
        return Ok(None);
    };
    let entries = entries
        .as_array()
        .ok_or_else(|| plan_err("must be an array of {chrom, start?, end?} objects".into()))?;
    if entries.is_empty() {
        return Err(plan_err("must not be empty".into()));
    }
    let mut specs = Vec::with_capacity(entries.len());
    for entry in entries {
        let obj = entry
            .as_object()
            .ok_or_else(|| plan_err("entries must be objects".into()))?;
        let chrom = obj
            .get("chrom")
            .and_then(Value::as_str)
            .ok_or_else(|| plan_err("entries need a string 'chrom'".into()))?;
        if chrom.is_empty() || chrom.contains('`') {
            return Err(plan_err(format!("invalid chrom {chrom:?}")));
        }
        let start = optional_coordinate(obj, "start")?;
        let end = optional_coordinate(obj, "end")?;
        if let (Some(s), Some(e)) = (start, end) {
            if s > e {
                return Err(plan_err(format!("start {s} > end {e} on {chrom}")));
            }
        }
        specs.push(RegionSpec { chrom: chrom.to_string(), start, end });
    }
    Ok(Some(specs))
}

/// A closed position interval on `start`; `None` sides are open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunBounds {
    pub lo: Option<i64>,
    pub hi: Option<i64>,
}

impl RunBounds {
    pub const OPEN: RunBounds = RunBounds { lo: None, hi: None };

    pub fn is_open(&self) -> bool {
        self.lo.is_none() && self.hi.is_none()
    }

    pub fn contains(&self, pos: i64) -> bool {
        self.lo.is_none_or(|lo| pos >= lo) && self.hi.is_none_or(|hi| pos <= hi)
    }
}

/// Sort by lower bound and merge intervals that overlap or touch
/// (`hi + 1 >= next.lo`). Open sides absorb whatever they reach.
pub(crate) fn merge_bounds(mut bounds: Vec<RunBounds>) -> Vec<RunBounds> {
    bounds.sort_by_key(|b| (b.lo.unwrap_or(i64::MIN), b.hi.unwrap_or(i64::MAX)));
    let mut merged: Vec<RunBounds> = Vec::with_capacity(bounds.len());
    for b in bounds {
        match merged.last_mut() {
            Some(last)
                if last.hi.is_none_or(|hi| b.lo.is_none_or(|lo| lo <= hi.saturating_add(1))) =>
            {
                last.hi = match (last.hi, b.hi) {
                    (None, _) | (_, None) => None,
                    (Some(a), Some(c)) => Some(a.max(c)),
                };
            }
            _ => merged.push(b),
        }
    }
    merged
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/regions.rs datafusion/bio-function-vep/src/lib.rs
git commit -m "feat(vep): parse and merge the regions option"
```

---

### Task 2: Resolve regions to VCF contigs and restrict the contig list

**Files:**
- Modify: `datafusion/bio-function-vep/src/regions.rs`
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`AnnotateProvider` struct + `new`, `scan_with_transcript_engine_partitioned`, `ContigAnnotationConfig`, `minimal_contig_annotation_config` in tests)

**Interfaces:**
- Consumes: `RegionSpec`, `RunBounds`, `merge_bounds` (Task 1); `crate::cache::manifest::contig_alias_set(&str) -> HashSet<String>`.
- Produces:
  ```rust
  pub(crate) type ContigRuns = HashMap<String, Vec<RunBounds>>;   // key: the VCF's own contig spelling
  pub(crate) fn resolve_regions(specs: &[RegionSpec], vcf_contigs: &[String]) -> ContigRuns
  pub(crate) fn restrict_contigs(contigs: Vec<String>, runs: &ContigRuns) -> Vec<String>
  ```
  and fields `AnnotateProvider.regions: Option<Vec<RegionSpec>>`, `ContigAnnotationConfig.regions: Option<Arc<ContigRuns>>`.

- [ ] **Step 1: Write the failing tests** (append inside `mod tests` in `regions.rs`)

```rust
    fn s(chrom: &str, start: Option<i64>, end: Option<i64>) -> RegionSpec {
        RegionSpec { chrom: chrom.into(), start, end }
    }

    #[test]
    fn resolve_matches_aliases_and_keeps_vcf_spelling() {
        let vcf = vec!["chr1".to_string(), "chr2".to_string(), "chrM".to_string()];
        let runs = resolve_regions(
            &[s("1", Some(10), Some(20)), s("chr1", Some(15), Some(30)), s("MT", None, None), s("chr9", None, None)],
            &vcf,
        );
        assert_eq!(runs.len(), 2, "chr9 is not in the VCF: {runs:?}");
        assert_eq!(runs["chr1"], vec![RunBounds { lo: Some(10), hi: Some(30) }]);
        assert_eq!(runs["chrM"], vec![RunBounds::OPEN]);
    }

    #[test]
    fn restrict_contigs_keeps_vcf_order_and_drops_unlisted() {
        let mut runs = ContigRuns::new();
        runs.insert("chr3".into(), vec![RunBounds::OPEN]);
        runs.insert("chr1".into(), vec![RunBounds::OPEN]);
        let kept = restrict_contigs(vec!["chr1".into(), "chr2".into(), "chr3".into()], &runs);
        assert_eq!(kept, vec!["chr1".to_string(), "chr3".to_string()]);
        assert!(restrict_contigs(vec!["chr2".into()], &runs).is_empty());
    }
```

And in `annotate_provider.rs` `mod tests`, next to `cache_format_is_accepted`:

```rust
    #[cfg(feature = "parquet-cache")]
    #[tokio::test]
    async fn regions_option_is_validated_at_construction() {
        let session = Arc::new(SessionContext::new());
        let vcf_schema = Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
        ]);
        let tmp = tempfile::tempdir().unwrap();
        let err = AnnotateProvider::new(
            Arc::clone(&session),
            "vcf".to_string(),
            tmp.path().to_string_lossy().to_string(),
            AnnotationBackend::Parquet,
            CacheSourceType::Ensembl,
            Some(r#"{"regions":[{"chrom":"chr1","start":9,"end":8}]}"#.to_string()),
            vcf_schema,
        )
        .expect_err("start > end must be rejected");
        assert!(err.to_string().contains("regions"), "{err}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep regions 2>&1 | tail -20`
Expected: compile errors for `resolve_regions`, `restrict_contigs`, `ContigRuns`; the provider test fails because construction succeeds.

- [ ] **Step 3: Implement in `regions.rs`**

```rust
use std::collections::HashMap;

/// Per-contig merged intervals, keyed by the VCF's own contig spelling.
pub(crate) type ContigRuns = HashMap<String, Vec<RunBounds>>;

/// Map each spec onto the VCF contig it names (through the alias set, so
/// `1`/`chr1` and `M`/`MT`/`chrM`/`chrMT` match) and merge per contig. Specs
/// naming a contig the VCF does not have select nothing and are dropped.
pub(crate) fn resolve_regions(specs: &[RegionSpec], vcf_contigs: &[String]) -> ContigRuns {
    let mut by_alias: HashMap<String, &String> = HashMap::new();
    for contig in vcf_contigs {
        for alias in crate::cache::manifest::contig_alias_set(contig) {
            by_alias.entry(alias).or_insert(contig);
        }
    }
    let mut runs: HashMap<String, Vec<RunBounds>> = HashMap::new();
    for spec in specs {
        if let Some(contig) = by_alias.get(&spec.chrom) {
            runs.entry((*contig).clone())
                .or_default()
                .push(RunBounds { lo: spec.start, hi: spec.end });
        }
    }
    runs.into_iter().map(|(k, v)| (k, merge_bounds(v))).collect()
}

/// Keep only contigs that have a run, in the order given.
pub(crate) fn restrict_contigs(contigs: Vec<String>, runs: &ContigRuns) -> Vec<String> {
    contigs.into_iter().filter(|c| runs.contains_key(c)).collect()
}
```

- [ ] **Step 4: Implement in `annotate_provider.rs`**

1. Add `regions: Option<Vec<crate::regions::RegionSpec>>,` to `pub struct AnnotateProvider` (after `options_json`). In `AnnotateProvider::new`, before building `Self`, add:

```rust
        let regions = crate::regions::parse_regions_option(options_json.as_deref())?;
```
   and set `regions,` in the struct literal (every construction site of `AnnotateProvider { .. }` inside the file must set it; grep `AnnotateProvider {` and add `regions: None` where a literal is built outside `new`, if any).

2. Add `regions: Option<Arc<crate::regions::ContigRuns>>,` to `struct ContigAnnotationConfig` (after `vcf_shard_ctx`). Set `regions: None,` in `minimal_contig_annotation_config()` in the tests module.

3. In `scan_with_transcript_engine_partitioned`, directly after `let vcf_contigs = self.discover_vcf_contigs().await?;`:

```rust
        let contig_runs: Option<Arc<crate::regions::ContigRuns>> = match self.regions.as_deref() {
            None => None,
            Some(specs) => {
                if self.vcf_shard_ctx.is_some() {
                    return Err(DataFusionError::Plan(
                        "annotate_vep(): regions are not supported with sharded VCF output \
                         (workers>1); run with workers=1"
                            .to_string(),
                    ));
                }
                Some(Arc::new(crate::regions::resolve_regions(specs, &vcf_contigs)))
            }
        };
```
   and replace the `select_cache_backed_contigs(...)` line with:

```rust
        let contigs = select_cache_backed_contigs(&vcf_contigs, &cache_chroms, cache.base_dir())?;
        let contigs = match contig_runs.as_deref() {
            Some(runs) => crate::regions::restrict_contigs(contigs, runs),
            None => contigs,
        };
```
   The existing `if contigs.is_empty() { return Ok(Arc::new(EmptyExec::new(projected_schema))); }` already turns an empty selection into an empty result. Add `regions: contig_runs,` to the `ContigAnnotationConfig { .. }` literal.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep regions 2>&1 | tail -20`
Expected: all `regions::` tests and `regions_option_is_validated_at_construction` pass.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/regions.rs datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): resolve regions to VCF contigs and restrict the contig list"
```

---

### Task 3: Buffer-range mapping and run planning

**Files:**
- Modify: `datafusion/bio-function-vep/src/regions.rs`

**Interfaces:**
- Consumes: `RunBounds`.
- Produces:
  ```rust
  pub(crate) fn buffer_range_for_bounds(boundary_positions: &[i64], bounds: RunBounds) -> (usize, usize)
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub(crate) struct RunPlan { pub bounds: Vec<RunBounds>, pub buffers: Option<(usize, usize)> }
  pub(crate) fn plan_runs(bounds: &[RunBounds], boundary_positions: Option<&[i64]>) -> Vec<RunPlan>
  ```
  `boundary_positions` are the `pos` of the contig's `GridBufferBoundary`s (length `B+1`, last is `i64::MAX`). `buffers = (bk, bk1)` are buffer indices, `bk < bk1 <= B`.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn buffer_range_maps_to_whole_buffers() {
        // Buffers: [0..100), [100..200), [200..300), [300..]
        let pos = [10, 100, 200, 300, i64::MAX];
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(150), hi: Some(250) }), (1, 3));
        // exactly on a boundary
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(100), hi: Some(200) }), (1, 3));
        // below the first row: clamp to buffer 0
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(1), hi: Some(5) }), (0, 1));
        // beyond the last boundary: through the last buffer
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(350), hi: Some(900) }), (3, 4));
        // open sides
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: None, hi: Some(150) }), (0, 2));
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(250), hi: None }), (2, 4));
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds::OPEN), (0, 4));
    }

    #[test]
    fn buffer_range_on_position_tie_includes_the_earlier_buffer() {
        // A boundary at pos 100 with rows_before_pos > 0 means rows at 100 sit
        // in BOTH buffer 0 and buffer 1; a run starting at 100 must include 0.
        let pos = [10, 100, 200, i64::MAX];
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(100), hi: Some(100) }), (1, 2));
        // lo strictly inside buffer 0 that ends with a tie at 100: buffer 0.
        assert_eq!(buffer_range_for_bounds(&pos, RunBounds { lo: Some(99), hi: Some(100) }), (0, 2));
    }

    #[test]
    fn plan_runs_without_grid_is_one_run_per_interval() {
        let b = [RunBounds { lo: Some(1), hi: Some(5) }, RunBounds { lo: Some(50), hi: None }];
        assert_eq!(
            plan_runs(&b, None),
            vec![
                RunPlan { bounds: vec![b[0]], buffers: None },
                RunPlan { bounds: vec![b[1]], buffers: None },
            ]
        );
    }

    #[test]
    fn plan_runs_with_grid_merges_touching_buffer_ranges() {
        let pos = [10, 100, 200, 300, 400, i64::MAX];
        let b = [
            RunBounds { lo: Some(20), hi: Some(30) },   // buffer 0
            RunBounds { lo: Some(120), hi: Some(130) }, // buffer 1 -> touches (0,1)+(1,2)
            RunBounds { lo: Some(350), hi: Some(360) }, // buffer 3
        ];
        assert_eq!(
            plan_runs(&b, Some(&pos)),
            vec![
                RunPlan { bounds: vec![b[0], b[1]], buffers: Some((0, 2)) },
                RunPlan { bounds: vec![b[2]], buffers: Some((3, 4)) },
            ]
        );
    }

    #[test]
    fn plan_runs_with_empty_grid_yields_no_runs() {
        assert!(plan_runs(&[RunBounds::OPEN], Some(&[i64::MAX])).is_empty());
        assert!(plan_runs(&[RunBounds::OPEN], Some(&[])).is_empty());
    }
```

Note the tie test: `buffer_range_for_bounds` sees only positions, not `rows_before_pos`; for `lo == boundary pos` it returns the buffer *starting* at that position, and the gate's `skip_leading_rows` (Task 4, taken from the slice) drops tie rows that belong to the previous buffer. That matches how `build_grid_slices` treats a seam.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: compile errors for `buffer_range_for_bounds`, `RunPlan`, `plan_runs`.

- [ ] **Step 3: Implement**

```rust
/// Map an interval onto whole input buffers. `boundary_positions[k]` is the
/// `start` of the first row of buffer `k`; the last entry (`i64::MAX`) is the
/// terminal boundary, so there are `len - 1` buffers. Returns `(bk, bk1)` with
/// `bk < bk1`: the first buffer whose start is at or below `lo` (buffer 0 when
/// `lo` precedes every row) through the last buffer whose start is at or below
/// `hi`, exclusive. Including buffer `bk` even when `lo` falls inside it is a
/// superset; the output trim removes the extra rows.
pub(crate) fn buffer_range_for_bounds(boundary_positions: &[i64], bounds: RunBounds) -> (usize, usize) {
    let b = boundary_positions.len().saturating_sub(1);
    if b == 0 {
        return (0, 0);
    }
    let bk = match bounds.lo {
        None => 0,
        // number of buffer starts <= lo, minus one; clamp at 0
        Some(lo) => boundary_positions[..b].partition_point(|&p| p <= lo).saturating_sub(1),
    };
    let bk1 = match bounds.hi {
        None => b,
        Some(hi) => boundary_positions[..b].partition_point(|&p| p <= hi).max(bk + 1).min(b),
    };
    (bk, bk1)
}

/// One lookup pass over a contig: the original intervals it serves (for the
/// output trim) and, on the grid path, the whole-buffer range it must annotate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunPlan {
    pub bounds: Vec<RunBounds>,
    pub buffers: Option<(usize, usize)>,
}

/// Plan the runs of one contig. Without a grid (`None`, the stateless Ensembl
/// path) every merged interval is its own run. With a grid, intervals map to
/// buffer ranges and ranges that overlap or touch merge into one run so no
/// buffer is warmed up and annotated twice.
pub(crate) fn plan_runs(bounds: &[RunBounds], boundary_positions: Option<&[i64]>) -> Vec<RunPlan> {
    let bounds = merge_bounds(bounds.to_vec());
    let Some(positions) = boundary_positions else {
        return bounds
            .into_iter()
            .map(|b| RunPlan { bounds: vec![b], buffers: None })
            .collect();
    };
    if positions.len() < 2 {
        return Vec::new();
    }
    let mut runs: Vec<RunPlan> = Vec::new();
    for b in bounds {
        let (bk, bk1) = buffer_range_for_bounds(positions, b);
        match runs.last_mut() {
            Some(last) if last.buffers.is_some_and(|(_, end)| bk <= end) => {
                last.bounds.push(b);
                let (start, end) = last.buffers.unwrap();
                last.buffers = Some((start, end.max(bk1)));
            }
            _ => runs.push(RunPlan { bounds: vec![b], buffers: Some((bk, bk1)) }),
        }
    }
    runs
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: all pass. If `buffer_range_on_position_tie_includes_the_earlier_buffer` fails on the `(99, 100)` case, re-check `partition_point(|&p| p <= lo)`: for `lo = 99` over `[10, 100, 200]` it is 1, minus one is 0. Correct.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/regions.rs
git commit -m "feat(vep): plan region runs over the input-buffer grid"
```

---

### Task 4: `RunGate` — rank gate for warm-up, emit and rank-stop

**Files:**
- Modify: `datafusion/bio-function-vep/src/regions.rs`

**Interfaces:**
- Produces:
  ```rust
  pub(crate) struct RunGate { .. }
  pub(crate) struct GateOutput { pub warm_up: Option<RecordBatch>, pub emit: Option<RecordBatch>, pub reached_emit: bool, pub done: bool }
  impl RunGate {
      pub fn new(skip_leading_rows: usize, warm_up_start_row: usize, emit_start_row: usize, emit_end_row: usize) -> Self;
      pub fn feed(&mut self, batch: RecordBatch) -> GateOutput;
      pub fn needs_warm_up(&self) -> bool;   // emit_start_row > warm_up_start_row
  }
  ```
  Semantics mirror `spawn_annotation_from_lookup_sharded`: the first `skip_leading_rows` rows are dropped; the next rows have global rank starting at `warm_up_start_row`; ranks `< emit_start_row` are warm-up, ranks in `[emit_start_row, emit_end_row)` are emit, ranks `>= emit_end_row` are discarded and `done` becomes true.

- [ ] **Step 1: Write the failing tests**

```rust
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(starts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"; starts.len()])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(starts.to_vec())) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    fn starts(b: &RecordBatch) -> Vec<i64> {
        b.column(1).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
    }

    #[test]
    fn gate_splits_skip_warmup_emit_and_stops() {
        // ranks after skipping 1 tie row: 10,11,12 | 13,14,15 | 16,17
        let mut gate = RunGate::new(1, 10, 13, 16);
        assert!(gate.needs_warm_up());
        let out = gate.feed(batch(&[99, 100, 101])); // 99 skipped; ranks 10,11 warm-up
        assert_eq!(starts(out.warm_up.as_ref().unwrap()), vec![100, 101]);
        assert!(out.emit.is_none() && !out.reached_emit && !out.done);
        let out = gate.feed(batch(&[102, 103, 104])); // rank 12 warm-up, 13,14 emit
        assert_eq!(starts(out.warm_up.as_ref().unwrap()), vec![102]);
        assert_eq!(starts(out.emit.as_ref().unwrap()), vec![103, 104]);
        assert!(out.reached_emit && !out.done);
        let out = gate.feed(batch(&[105, 106, 107])); // 15 emit; 16,17 discarded
        assert!(out.warm_up.is_none());
        assert_eq!(starts(out.emit.as_ref().unwrap()), vec![105]);
        assert!(out.done);
    }

    #[test]
    fn gate_without_warm_up_emits_from_the_first_row() {
        let mut gate = RunGate::new(0, 0, 0, 2);
        assert!(!gate.needs_warm_up());
        let out = gate.feed(batch(&[1, 2, 3]));
        assert!(out.warm_up.is_none());
        assert_eq!(starts(out.emit.as_ref().unwrap()), vec![1, 2]);
        assert!(out.reached_emit && out.done);
    }

    #[test]
    fn gate_reports_reached_emit_once_and_handles_empty_batches() {
        let mut gate = RunGate::new(0, 0, 2, usize::MAX);
        let out = gate.feed(batch(&[]));
        assert!(out.warm_up.is_none() && out.emit.is_none() && !out.reached_emit);
        let out = gate.feed(batch(&[1, 2, 3]));
        assert!(out.reached_emit);
        let out = gate.feed(batch(&[4]));
        assert!(!out.reached_emit, "reached_emit fires only on the crossing batch");
        assert_eq!(starts(out.emit.as_ref().unwrap()), vec![4]);
        assert!(!out.done, "open upper rank never stops");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep regions::tests::gate 2>&1 | tail -20`
Expected: compile errors for `RunGate`.

- [ ] **Step 3: Implement**

```rust
use datafusion::arrow::record_batch::RecordBatch;

/// Rank gate for one run: drops the position-tie rows that belong to the
/// previous buffer, routes warm-up rows and emit rows, and stops at the emit
/// end. Same semantics as the sharded worker's inline gate.
#[derive(Debug)]
pub(crate) struct RunGate {
    to_skip: usize,
    next_rank: usize,
    emit_start_row: usize,
    emit_end_row: usize,
    warm_up_start_row: usize,
    reached_emit: bool,
}

pub(crate) struct GateOutput {
    pub warm_up: Option<RecordBatch>,
    pub emit: Option<RecordBatch>,
    /// True on the batch that first reaches `emit_start_row` (or when there
    /// is no warm-up region, on the first non-empty batch). The caller replays
    /// the collected warm-up rows before pushing `emit`.
    pub reached_emit: bool,
    /// True once every row up to `emit_end_row` has been seen.
    pub done: bool,
}

impl RunGate {
    pub fn new(
        skip_leading_rows: usize,
        warm_up_start_row: usize,
        emit_start_row: usize,
        emit_end_row: usize,
    ) -> Self {
        Self {
            to_skip: skip_leading_rows,
            next_rank: warm_up_start_row,
            emit_start_row,
            emit_end_row,
            warm_up_start_row,
            reached_emit: false,
        }
    }

    pub fn needs_warm_up(&self) -> bool {
        self.emit_start_row > self.warm_up_start_row
    }

    pub fn feed(&mut self, mut batch: RecordBatch) -> GateOutput {
        let mut out = GateOutput { warm_up: None, emit: None, reached_emit: false, done: false };
        if self.to_skip > 0 {
            let drop = self.to_skip.min(batch.num_rows());
            batch = batch.slice(drop, batch.num_rows() - drop);
            self.to_skip -= drop;
        }
        let n = batch.num_rows();
        if n == 0 {
            return out;
        }
        let batch_start = self.next_rank;
        let batch_end = batch_start + n;
        self.next_rank = batch_end;
        let warm_end = self.emit_start_row.saturating_sub(batch_start).min(n);
        let emit_to = self.emit_end_row.saturating_sub(batch_start).min(n);
        if warm_end > 0 {
            out.warm_up = Some(batch.slice(0, warm_end));
        }
        if !self.reached_emit && batch_end >= self.emit_start_row {
            self.reached_emit = true;
            out.reached_emit = true;
        }
        if emit_to > warm_end {
            out.emit = Some(batch.slice(warm_end, emit_to - warm_end));
        }
        out.done = batch_end >= self.emit_end_row;
        out
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/regions.rs
git commit -m "feat(vep): add the run rank gate"
```

---

### Task 5: Output trim `filter_batch_to_bounds`

**Files:**
- Modify: `datafusion/bio-function-vep/src/regions.rs`

**Interfaces:**
- Produces: `pub(crate) fn filter_batch_to_bounds(batch: &RecordBatch, start_idx: usize, bounds: &[RunBounds]) -> Result<RecordBatch>` — keeps rows whose `start` (any integer type; cast to Int64) lies in at least one of `bounds`. An all-open `bounds` returns the batch unchanged.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn filter_batch_keeps_rows_inside_any_interval() {
        let b = batch(&[5, 10, 15, 20, 25, 30]);
        let out = filter_batch_to_bounds(
            &b,
            1,
            &[RunBounds { lo: Some(10), hi: Some(15) }, RunBounds { lo: Some(30), hi: None }],
        )
        .unwrap();
        assert_eq!(starts(&out), vec![10, 15, 30]);
        let same = filter_batch_to_bounds(&b, 1, &[RunBounds::OPEN]).unwrap();
        assert_eq!(same.num_rows(), 6);
    }

    #[test]
    fn filter_batch_accepts_uint32_start() {
        use datafusion::arrow::array::UInt32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("start", DataType::UInt32, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(vec![1u32, 7, 9])) as Arc<dyn Array>]).unwrap();
        let out = filter_batch_to_bounds(&b, 0, &[RunBounds { lo: Some(7), hi: Some(9) }]).unwrap();
        assert_eq!(out.num_rows(), 2);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep regions::tests::filter_batch 2>&1 | tail -10`
Expected: compile error, `filter_batch_to_bounds` not found.

- [ ] **Step 3: Implement**

```rust
use datafusion::arrow::array::{BooleanArray, Int64Array};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::DataType;

/// Keep the rows whose `start` lies inside at least one interval. Used after
/// annotation (before projection) because stateful runs annotate whole
/// buffers and indexed reads are overlap-based.
pub(crate) fn filter_batch_to_bounds(
    batch: &RecordBatch,
    start_idx: usize,
    bounds: &[RunBounds],
) -> Result<RecordBatch> {
    if bounds.iter().any(RunBounds::is_open) || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let starts = cast(batch.column(start_idx), &DataType::Int64)?;
    let starts = starts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("start column did not cast to Int64".into()))?;
    let mask: BooleanArray = starts
        .iter()
        .map(|v| v.map(|pos| bounds.iter().any(|b| b.contains(pos))))
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep regions:: 2>&1 | tail -20`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/regions.rs
git commit -m "feat(vep): trim annotated batches to run bounds"
```

---

### Task 6: Run planning in `prepare_contig_data` and per-run lookup activation

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs`

**Interfaces:**
- Consumes: `regions::{RunBounds, RunPlan, plan_runs}`, `build_grid_slices`, `count_contig_buffer_boundaries`, `LookupProvider` setters, `spawn_lookup_partition_worker`.
- Produces (all private to `annotate_provider.rs`):
  ```rust
  #[derive(Clone, Debug)]
  struct ContigRun { bounds: Vec<RunBounds>, slice: Option<WorkerGridSlice> }
  impl ContigRun { fn open() -> Self; fn is_open(&self) -> bool; fn lookup_filter(&self, chrom: &str) -> Expr; fn probe_floor(&self) -> Option<i64> }
  #[derive(Clone)]
  struct RunActivationInputs { var_table: String, vcf_schema: Schema, cache_schema: Schema, fallback_coloc_sink: ColocatedSink }
  async fn activate_run_lookup(session: Arc<SessionContext>, inputs: RunActivationInputs, config: ContigAnnotationConfig, chrom: String, run: ContigRun, pipeline_profile: Option<SharedContigPipelineProfile>) -> Result<VecDeque<LookupPartitionHandle>>
  ```
  New fields: `ContigPreparedData.runs: VecDeque<ContigRun>`, `ContigReadyState.{active_run: ContigRun, pending_runs: VecDeque<ContigRun>, run_inputs: RunActivationInputs, pipeline_profile: Option<SharedContigPipelineProfile>}`.

- [ ] **Step 1: Write the failing test** (in `mod tests` of `annotate_provider.rs`)

```rust
    #[test]
    fn contig_run_lookup_filter_and_probe_floor() {
        let open = ContigRun::open();
        assert!(open.is_open());
        assert_eq!(format!("{}", open.lookup_filter("chr1")), format!("{}", col("chrom").eq(lit("chr1"))));
        assert_eq!(open.probe_floor(), None);

        let ensembl = ContigRun {
            bounds: vec![RunBounds { lo: Some(10), hi: Some(20) }],
            slice: None,
        };
        assert_eq!(
            format!("{}", ensembl.lookup_filter("chr1")),
            format!(
                "{}",
                col("chrom").eq(lit("chr1")).and(col("start").gt_eq(lit(10_i64))).and(col("start").lt_eq(lit(20_i64)))
            )
        );

        let stateful = ContigRun {
            bounds: vec![RunBounds { lo: Some(150), hi: Some(160) }],
            slice: Some(WorkerGridSlice {
                worker_id: 0,
                scan_lo_pos: 100,
                emit_start_pos: 140,
                scan_hi_pos: 201,
                skip_leading_rows: 0,
                warm_up_start_row: 0,
                emit_start_row: 5000,
                emit_end_row: 10000,
            }),
        };
        assert_eq!(
            format!("{}", stateful.lookup_filter("chr1")),
            format!(
                "{}",
                col("chrom").eq(lit("chr1")).and(col("start").gt_eq(lit(100_i64))).and(col("start").lt(lit(201_i64)))
            )
        );
        assert_eq!(stateful.probe_floor(), Some(140));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep contig_run_lookup_filter 2>&1 | tail -10`
Expected: compile error, `ContigRun` not found.

- [ ] **Step 3: Add `ContigRun` and `RunActivationInputs`**

Place directly after the `WorkerGridSlice` definition:

```rust
use crate::regions::{RunBounds, RunGate};

/// One lookup pass over a contig. `bounds` are the caller's intervals this run
/// serves (the output trim); `slice` is the whole-buffer window with warm-up
/// on the stateful (Merged/RefSeq) grid path, `None` on the stateless path.
#[derive(Clone, Debug)]
struct ContigRun {
    bounds: Vec<RunBounds>,
    slice: Option<WorkerGridSlice>,
}

impl ContigRun {
    fn open() -> Self {
        Self { bounds: vec![RunBounds::OPEN], slice: None }
    }

    fn is_open(&self) -> bool {
        self.slice.is_none() && self.bounds.iter().all(RunBounds::is_open)
    }

    /// The VCF filter for this run's lookup scan: the contig plus the position
    /// window the run must read (the slice's scan window on the grid path, the
    /// merged interval itself otherwise).
    fn lookup_filter(&self, chrom: &str) -> Expr {
        let mut filter = col("chrom").eq(lit(chrom));
        match &self.slice {
            Some(s) => {
                filter = filter.and(col("start").gt_eq(lit(s.scan_lo_pos)));
                if s.scan_hi_pos != i64::MAX {
                    filter = filter.and(col("start").lt(lit(s.scan_hi_pos)));
                }
            }
            None => {
                let lo = self.bounds.iter().map(|b| b.lo).min().flatten();
                let hi = self.bounds.iter().map(|b| b.hi).max().flatten();
                if self.bounds.iter().all(|b| b.lo.is_some()) {
                    if let Some(lo) = lo {
                        filter = filter.and(col("start").gt_eq(lit(lo)));
                    }
                }
                if self.bounds.iter().all(|b| b.hi.is_some()) {
                    if let Some(hi) = hi {
                        filter = filter.and(col("start").lt_eq(lit(hi)));
                    }
                }
            }
        }
        filter
    }

    /// Warm-up rows are read only to replay buffer state; skip their variation
    /// probe (mirrors `VEP_SKIP_WARMUP_LOOKUP` on the sharded path).
    fn probe_floor(&self) -> Option<i64> {
        self.slice
            .as_ref()
            .filter(|s| s.scan_lo_pos < s.emit_start_pos)
            .map(|s| s.emit_start_pos)
    }

    fn gate(&self) -> Option<RunGate> {
        self.slice.as_ref().map(|s| {
            RunGate::new(s.skip_leading_rows, s.warm_up_start_row, s.emit_start_row, s.emit_end_row)
        })
    }
}

/// What a later run of the same contig needs to build its lookup after the
/// first run has consumed `ContigPreparedData`.
#[derive(Clone)]
struct RunActivationInputs {
    var_table: String,
    vcf_schema: Schema,
    cache_schema: Schema,
    fallback_coloc_sink: ColocatedSink,
}
```

Note on `lookup_filter` for `slice: None` with several bounds: `plan_runs(.., None)` yields exactly one interval per run, so the `all(..)` guards are only defensive.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p datafusion-bio-function-vep contig_run_lookup_filter 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Plan runs in `prepare_contig_data`**

1. Add `runs: VecDeque<ContigRun>,` to `struct ContigPreparedData`.
2. Near `let stateful_parallel = cache_enabled && ...` add:

```rust
    let contig_bounds: Option<Vec<RunBounds>> = config
        .regions
        .as_deref()
        .and_then(|runs| runs.get(&chrom).cloned());
    // A bounded run on a stateful source needs the buffer grid so the cut can
    // be aligned to whole buffers and warmed up (design: stateful runs).
    let stateful_runs = cache_enabled
        && contig_bounds.is_some()
        && matches!(
            config.cache_source_type,
            CacheSourceType::Merged | CacheSourceType::RefSeq
        );
```
3. Change the count-pass condition from `if stateful_parallel {` to `if stateful_parallel || stateful_runs {` (the `count_fut` block), keeping `grid_balance_enabled` as the `collect_positions` argument.
4. After `grid_slices` planning (after the `if stateful_parallel { ... }` block that fills `grid_slices`), add:

```rust
    let runs: VecDeque<ContigRun> = match contig_bounds {
        None => VecDeque::from([ContigRun::open()]),
        Some(bounds) if stateful_runs => {
            let (boundaries, _total_rows, _positions) = grid_count
                .take()
                .expect("stateful_runs implies the count future ran")?;
            let positions: Vec<i64> = boundaries.iter().map(|b| b.pos).collect();
            crate::regions::plan_runs(&bounds, Some(&positions))
                .into_iter()
                .map(|plan| {
                    let (bk, bk1) = plan.buffers.expect("grid path plans buffer ranges");
                    let slice = build_grid_slices(&boundaries, &[bk, bk1], overlap_width_bp)
                        .into_iter()
                        .next();
                    ContigRun { bounds: plan.bounds, slice }
                })
                .collect()
        }
        Some(bounds) => crate::regions::plan_runs(&bounds, None)
            .into_iter()
            .map(|plan| ContigRun { bounds: plan.bounds, slice: None })
            .collect(),
    };
    pipeline_trace::emit(
        "regions",
        "runs",
        &[("chrom", TraceValue::Str(&chrom)), ("runs", TraceValue::Usize(runs.len()))],
    );
```
   `grid_count` is currently consumed by `expect(...)` inside the `stateful_parallel` block; make it `let mut grid_count = ...` and use `.take()` in both places (the two conditions are mutually exclusive on the LazyFrame path because `stateful_parallel` requires `annotation_workers > 1`, which is only reachable through the sink, where `regions` is rejected).
5. Set `runs,` in the `ContigPreparedData { .. }` literal.

- [ ] **Step 6: Extract `activate_run_lookup` and use it from `activate_contig_lookups`**

Move the provider construction and the `parallel_lookup` arm into a free function. The body is the existing code from `let mut provider = LookupProvider::new(` through the end of the `else if parallel_lookup { ... }` arm, with two changes: the filter comes from `run.lookup_filter(&chrom)` instead of `col("chrom").eq(lit(&*chrom))`, and `provider.set_probe_floor_pos(run.probe_floor())` is called after the filter.

```rust
/// Build the filtered lookup for one run and spawn its position-ordered
/// partition workers. Shared by the first run (from `activate_contig_lookups`)
/// and every later run of the same contig (from the `ActivatingRun` state).
async fn activate_run_lookup(
    session: Arc<SessionContext>,
    inputs: RunActivationInputs,
    config: ContigAnnotationConfig,
    chrom: String,
    run: ContigRun,
    pipeline_profile: Option<SharedContigPipelineProfile>,
) -> Result<VecDeque<LookupPartitionHandle>> {
    let RunActivationInputs { var_table, vcf_schema, cache_schema, fallback_coloc_sink } = inputs;
    let mut provider = LookupProvider::new(
        Arc::clone(&session),
        config.vcf_table.clone(),
        var_table,
        vcf_schema,
        cache_schema,
        config.cache_columns.clone(),
        config.extended_probes,
        config.allowed_failed,
    )?;
    provider.set_vcf_filter(Some(run.lookup_filter(&chrom)));
    provider.set_probe_floor_pos(run.probe_floor());
    provider.set_target_partitions(config.target_partitions);
    #[cfg(feature = "parquet-cache")]
    if let Some(root) = &config.cache_root {
        provider.set_cache_root(root.clone());
        if config.parquet_backend {
            provider.set_parquet_backend(true);
        }
    }
    // ... existing `parallel_lookup` arm body verbatim (scan, colocated sinks,
    // spawn_lookup_partition_worker per partition), returning `handles`; the
    // existing non-cache `else` arm (spawn_lookup_stream_worker with
    // `fallback_coloc_sink`) stays as the fallback when `config.cache_root`
    // is None.
}
```

In `activate_contig_lookups`:
- destructure the new `runs` field and pop the first run: `let mut runs = runs; let active_run = runs.pop_front().unwrap_or_else(ContigRun::open);`
- build `let run_inputs = RunActivationInputs { var_table: var_table.clone(), vcf_schema: vcf_schema.clone(), cache_schema: cache_schema.clone(), fallback_coloc_sink: Arc::clone(&fallback_coloc_sink) };`
- replace the `else if parallel_lookup { ... } else { ... }` arms of the `lookup_partitions` assignment with `activate_run_lookup(Arc::clone(&session), run_inputs.clone(), config.clone(), chrom.clone(), active_run.clone(), pipeline_profile.clone()).await?` (the `stateful_parallel` arm stays as is).
- add `active_run`, `pending_runs: runs`, `run_inputs`, `pipeline_profile: pipeline_profile.clone()` to `ContigReadyState` (add the four fields to the struct).

- [ ] **Step 7: Build and run the whole suite**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: everything passes (no behaviour change yet: every contig has one open run whose filter is the old `chrom = c`).

- [ ] **Step 8: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): plan per-contig runs and activate lookups per run"
```

---

### Task 7: Gate rows through warm-up/emit in the streaming state machine

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`ContigAnnotationState`, its construction in the `PreparingContig` ready arm, `apply_lookup_batch_message`, `poll_lookup_partitions`)

**Interfaces:**
- Consumes: `ContigRun::gate()`, `RunGate::feed`, `warm_up_worker_state`, `run_maybe_block_in_place`.
- Produces:
  ```rust
  struct ActiveRunState { bounds: Vec<RunBounds>, gate: Option<RunGate>, warm_up: Vec<RecordBatch>, warmed_up: bool }
  impl ActiveRunState { fn from_run(run: &ContigRun) -> Self }
  ```
  New fields on `ContigAnnotationState`: `run: ActiveRunState`, `pending_runs: VecDeque<ContigRun>`, `run_inputs: RunActivationInputs`, `pipeline_profile: Option<SharedContigPipelineProfile>`. `apply_lookup_batch_message` now returns `Result<()>`.

- [ ] **Step 1: Write the failing test** — warm-up/gate composition parity on the LRIF1 donation fixture (next to `warmup_reconstructs_serial_persisted_state`)

```rust
    #[test]
    fn gated_run_reconstructs_serial_state_at_a_mid_contig_cut() {
        // Same donor/recipient pair as `warmup_reconstructs_serial_persisted_state`.
        let mut tx_recipient = make_tx("XM_017001769.3", Some("55791"), Some("LRIF1"), Some("EntrezGene"), None);
        tx_recipient.chrom = "chr1".to_string();
        tx_recipient.start = 110_874_957;
        tx_recipient.end = 110_963_922;
        tx_recipient.biotype = "protein_coding".to_string();
        let mut tx_donor = make_tx("ENST00000369763", Some("ENSG00000121931"), Some("LRIF1"), Some("HGNC"), Some("HGNC:30299"));
        tx_donor.chrom = "chr1".to_string();
        tx_donor.start = 110_947_190;
        tx_donor.end = 110_963_922;
        tx_donor.biotype = "protein_coding".to_string();
        let shared = minimal_shared_contig_annotation_context_with_context(
            vec![tx_recipient, tx_donor],
            Vec::new(),
            Vec::new(),
        );
        // Buffer 0 donates; buffer 1 is the run we want. Serial = both buffers
        // through the full prepare path in order.
        let buffer0 = vec![make_buffer_batch_many("chr1", &[109_528_491, 110_870_290, 110_947_275, 112_649_983])];
        let buffer1 = vec![make_buffer_batch_many("chr1", &[112_700_000, 112_800_000])];
        let mut serial = AnnotationWorkerState::new(Arc::clone(&shared)).unwrap();
        for buf in [&buffer0, &buffer1] {
            let (chrom, lo, hi) = buffer_variant_bounds(buf).unwrap().unwrap();
            let _ = prepare_buffer_annotation_context(&mut serial, buf, &chrom, lo, hi).unwrap();
        }

        // Gated run: rows of buffer 0 are warm-up (ranks 0..4), buffer 1 is
        // emitted (ranks 4..6). Feed as one stream of batches, as the lookup would.
        let run = ContigRun {
            bounds: vec![RunBounds { lo: Some(112_700_000), hi: Some(112_800_000) }],
            slice: Some(WorkerGridSlice {
                worker_id: 0,
                scan_lo_pos: 109_528_491,
                emit_start_pos: 112_700_000,
                scan_hi_pos: i64::MAX,
                skip_leading_rows: 0,
                warm_up_start_row: 0,
                emit_start_row: 4,
                emit_end_row: 6,
            }),
        };
        let mut worker = AnnotationWorkerState::new(shared).unwrap();
        let mut active = ActiveRunState::from_run(&run);
        let mut emitted: Vec<RecordBatch> = Vec::new();
        for batch in buffer0.into_iter().chain(buffer1.into_iter()) {
            let out = active.gate.as_mut().unwrap().feed(batch);
            if let Some(w) = out.warm_up {
                active.warm_up.push(w);
            }
            if out.reached_emit && !active.warmed_up {
                warm_up_worker_state(&mut worker, std::mem::take(&mut active.warm_up)).unwrap();
                active.warmed_up = true;
            }
            if let Some(e) = out.emit {
                emitted.push(e);
            }
        }
        let (chrom, lo, hi) = buffer_variant_bounds(&emitted).unwrap().unwrap();
        let _ = prepare_buffer_annotation_context(&mut worker, &emitted, &chrom, lo, hi).unwrap();

        assert!(!serial.persisted_buffer_transcripts.is_empty(), "fixture must persist state");
        assert_eq!(
            worker.persisted_buffer_transcripts, serial.persisted_buffer_transcripts,
            "gated run must reconstruct the serial carry at the cut"
        );
        assert_eq!(emitted.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep gated_run_reconstructs 2>&1 | tail -10`
Expected: compile error, `ActiveRunState` not found.

- [ ] **Step 3: Add `ActiveRunState` and the new `ContigAnnotationState` fields**

After `ContigRun`:

```rust
/// Gate state of the run currently being drained.
struct ActiveRunState {
    bounds: Vec<RunBounds>,
    gate: Option<RunGate>,
    warm_up: Vec<RecordBatch>,
    warmed_up: bool,
}

impl ActiveRunState {
    fn from_run(run: &ContigRun) -> Self {
        let gate = run.gate();
        let warmed_up = gate.as_ref().is_none_or(|g| !g.needs_warm_up());
        Self { bounds: run.bounds.clone(), gate, warm_up: Vec::new(), warmed_up }
    }

    /// `None` for an open run (no trim), `Some` otherwise.
    fn trim_bounds(&self) -> Option<Vec<RunBounds>> {
        if self.bounds.iter().all(RunBounds::is_open) {
            None
        } else {
            Some(self.bounds.clone())
        }
    }
}
```

Add to `struct ContigAnnotationState`:

```rust
    run: ActiveRunState,
    pending_runs: VecDeque<ContigRun>,
    run_inputs: RunActivationInputs,
    pipeline_profile: Option<SharedContigPipelineProfile>,
```

and in the `PreparingContig` ready arm where `let ann = ContigAnnotationState { .. }` is built, add:

```rust
                                run: ActiveRunState::from_run(&ready.active_run),
                                pending_runs: std::mem::take(&mut ready.pending_runs),
                                run_inputs: ready.run_inputs.clone(),
                                pipeline_profile: ready.pipeline_profile.clone(),
```

(`ready` must be `mut` there; it already is.)

- [ ] **Step 4: Route batches through the gate in `apply_lookup_batch_message`**

Change the signature to `fn apply_lookup_batch_message(ann: &mut ContigAnnotationState, message: LookupBatchMessage) -> Result<()>` and replace the tail (`if rows > 0 { ann.contig_rows += rows; ann.worker.window_buffer.push(message.batch); }`) with:

```rust
    let Some(gate) = ann.run.gate.as_mut() else {
        if rows > 0 {
            ann.contig_rows += rows;
            ann.worker.window_buffer.push(message.batch);
        }
        return Ok(());
    };
    let out = gate.feed(message.batch);
    if let Some(warm) = out.warm_up {
        ann.run.warm_up.push(warm);
    }
    if out.reached_emit && !ann.run.warmed_up {
        // Replay whole grid buffers strictly before the seam, state-only, so
        // the carried HGNC state is what the serial run would hold here.
        let warm_up = std::mem::take(&mut ann.run.warm_up);
        run_maybe_block_in_place(|| warm_up_worker_state(&mut ann.worker, warm_up))?;
        ann.run.warmed_up = true;
    }
    if let Some(emit) = out.emit {
        ann.contig_rows += emit.num_rows();
        ann.worker.window_buffer.push(emit);
    }
    if out.done && !ann.worker.lookup_done {
        // Rank-stop: every row of the run's emit range has been seen.
        ann.worker.lookup_done = true;
        abort_annotation_lookup_partitions(ann);
    }
    Ok(())
```

In `poll_lookup_partitions`, the call site becomes:

```rust
            Poll::Ready(Ok(Some(LookupPartitionPoll::Batch(message)))) => {
                finish_lookup_waits(ann);
                if let Err(e) = apply_lookup_batch_message(ann, message) {
                    return Poll::Ready(Err(e));
                }
                made_progress = true;
            }
```

and in the `Poll::Ready(Ok(None))` arm (stream ended), after `ann.worker.lookup_done = true;`, add the tiny-tail replay:

```rust
                if !ann.run.warmed_up {
                    let warm_up = std::mem::take(&mut ann.run.warm_up);
                    if let Err(e) = run_maybe_block_in_place(|| warm_up_worker_state(&mut ann.worker, warm_up)) {
                        return Poll::Ready(Err(e));
                    }
                    ann.run.warmed_up = true;
                }
```

Check that `abort_annotation_lookup_partitions` followed by further polls is safe: `poll_lookup_partitions` loops only `while !ann.worker.lookup_done`, so the fan-in is not polled again.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: `gated_run_reconstructs_serial_state_at_a_mid_contig_cut` passes and nothing regresses.

- [ ] **Step 6: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): gate run rows through warm-up, emit and rank-stop"
```

---

### Task 8: Output trim before projection and the `ActivatingRun` transition

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs` (`annotate_window_owned`, `annotate_worker_window`, the window dispatch in `AnnotatingContig`, the contig-done transition, `StreamState`, drop/cleanup arms)

**Interfaces:**
- Consumes: `filter_batch_to_bounds`, `activate_run_lookup`, `ActiveRunState::trim_bounds`.
- Produces: `StreamState::ActivatingRun { fut: RunActivateFuture, annotation_state: ContigAnnotationState }` with `type RunActivateFuture = Pin<Box<dyn Future<Output = Result<VecDeque<LookupPartitionHandle>>> + Send>>`; `annotate_window_owned(.., emit_bounds: Option<Vec<RunBounds>>)`; `annotate_worker_window(.., emit_bounds: Option<&[RunBounds]>)`; `AnnotationWorkerState::reset_for_next_run(&mut self)`.

- [ ] **Step 1: Write the failing test** for the reset helper (in `mod tests`)

```rust
    #[test]
    fn reset_for_next_run_clears_per_run_state_but_keeps_colocated() {
        let shared = minimal_shared_contig_annotation_context_with_features(Vec::new(), Vec::new());
        let mut worker = AnnotationWorkerState::new(shared).unwrap();
        worker.window_buffer.push(make_buffer_batch_many("chr1", &[1, 2]));
        worker.next_input_buffer_id = 7;
        worker.lookup_done = true;
        let mut coloc = HashMap::new();
        coloc.insert(ColocatedKey::default(), ColocatedData::default());
        worker.colocated_map = Arc::new(coloc);
        worker.reset_for_next_run();
        assert!(worker.window_buffer.is_empty());
        assert_eq!(worker.next_input_buffer_id, 0);
        assert!(!worker.lookup_done);
        assert!(worker.persisted_buffer_transcripts.is_empty());
        assert_eq!(worker.colocated_map.len(), 1, "colocated map is per contig");
    }
```

If `ColocatedKey`/`ColocatedData` do not implement `Default`, build them with the constructors the existing tests near `clinical_colocated_data` use.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep reset_for_next_run 2>&1 | tail -10`
Expected: compile error, no method `reset_for_next_run`.

- [ ] **Step 3: Implement the reset, the trim, and the transition**

1. On `impl AnnotationWorkerState`:

```rust
    /// Per-run state goes back to what a fresh worker holds; the colocated map
    /// is per contig and is kept. The next run reconstructs its carried state
    /// through warm-up.
    fn reset_for_next_run(&mut self) {
        self.window_buffer.clear();
        self.input_buffer_accumulator = InputBufferAccumulator::default();
        self.next_input_buffer_id = 0;
        self.persisted_buffer_transcripts.clear();
        self.lookup_done = false;
    }
```

2. `annotate_worker_window` gains `emit_bounds: Option<&[RunBounds]>`. Inside its per-batch loop, immediately after the annotated batch is produced and before the projection is applied, insert:

```rust
            let annotated = match emit_bounds {
                Some(bounds) => {
                    let start_idx = tmp_provider.schema().index_of("start")?;
                    crate::regions::filter_batch_to_bounds(&annotated, start_idx, bounds)?
                }
                None => annotated,
            };
            if annotated.num_rows() == 0 {
                continue;
            }
```
   (`annotated` is whatever the loop names the pre-projection batch; the VCF columns come first in the provider schema, so `index_of("start")` on `tmp_provider.schema()` is the input `start` column.) Every existing caller passes `None`; the sharded path is unchanged.

3. `annotate_window_owned` gains a trailing `emit_bounds: Option<Vec<RunBounds>>` parameter and forwards `emit_bounds.as_deref()` to `annotate_worker_window`. In the `AnnotatingContig` dispatch (`tokio::task::spawn_blocking(move || annotate_window_owned(...))`), add `let emit_bounds = ann.run.trim_bounds();` before the closure and pass it as the last argument.

4. Add the state variant and future type next to `PrepareFuture`:

```rust
type RunActivateFuture =
    Pin<Box<dyn Future<Output = Result<VecDeque<LookupPartitionHandle>>> + Send>>;
```
```rust
    /// A later run of the current contig is building its filtered lookup.
    ActivatingRun {
        fut: RunActivateFuture,
        annotation_state: ContigAnnotationState,
    },
```

5. At the contig-done transition (the comment `// No window to produce and nothing in flight — contig done.`), insert BEFORE `abort_annotation_lookup_partitions(&mut ann);`:

```rust
                    if let Some(next) = ann.pending_runs.pop_front() {
                        abort_annotation_lookup_partitions(&mut ann);
                        ann.worker.reset_for_next_run();
                        ann.run = ActiveRunState::from_run(&next);
                        let fut: RunActivateFuture = Box::pin(activate_run_lookup(
                            Arc::clone(&ann.session),
                            ann.run_inputs.clone(),
                            ann.config.clone(),
                            ann.chrom.clone(),
                            next,
                            ann.pipeline_profile.clone(),
                        ));
                        self.state = StreamState::ActivatingRun { fut, annotation_state: ann };
                        continue;
                    }
```

6. Add the poll arm, modelled on `AwaitingWindow`:

```rust
                StreamState::ActivatingRun { .. } => {
                    let StreamState::ActivatingRun { mut fut, annotation_state: mut ann } =
                        std::mem::replace(&mut self.state, StreamState::Done)
                    else {
                        unreachable!()
                    };
                    match fut.as_mut().poll(cx) {
                        Poll::Pending => {
                            self.state = StreamState::ActivatingRun { fut, annotation_state: ann };
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(handles)) => {
                            ann.lookup_partitions =
                                LookupPartitionFanIn::new(handles, LOOKUP_PARTITION_QUEUE_BATCHES);
                            ann.lookup_wait_started = None;
                            ann.ordered_lookup_wait_started = None;
                            self.state = StreamState::AnnotatingContig(ann);
                            continue;
                        }
                        Poll::Ready(Err(e)) => {
                            let fut = make_cleanup_future(
                                Arc::clone(&ann.session),
                                std::mem::take(&mut ann.ephemeral_tables),
                            );
                            self.state = StreamState::ErrorCleaningUp(fut, e);
                            self.abort_prefetch();
                            continue;
                        }
                    }
                }
```

7. `cleanup_registered_tables_on_drop` (and any other `match &mut self.state` / `match self.state` over `StreamState`) gets an `ActivatingRun { annotation_state, .. }` arm doing exactly what the `AwaitingWindow { annotation_state, .. }` arm does.

8. `LIMIT` interplay: the `limit_reached` cleanup path runs before the contig-done check, so a satisfied limit still ends the contig without activating further runs. Verify by reading the arm order; no code change expected.

- [ ] **Step 4: Run the suite**

Run: `cargo test -p datafusion-bio-function-vep 2>&1 | tail -15`
Expected: all pass, including `reset_for_next_run_clears_per_run_state_but_keeps_colocated`.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-function-vep --all-targets 2>&1 | tail -3
git add datafusion/bio-function-vep/src/annotate_provider.rs
git commit -m "feat(vep): trim run output to bounds and chain runs per contig"
```

---

### Task 9: End-to-end check against vepyr fixtures and PR

**Files:**
- Modify: `~/research/git/vepyr/Cargo.toml` (temporary `[patch]`, NOT committed)

- [ ] **Step 1: Point vepyr at the working tree and rebuild**

In the vepyr clone, append temporarily to `Cargo.toml`:

```toml
[patch."https://github.com/biodatageeks/datafusion-bio-functions.git"]
datafusion-bio-function-vep = { path = "/Users/mwiewior/research/git/datafusion-bio-functions/datafusion/bio-function-vep" }
```

Then:

```bash
cd ~/research/git/vepyr
CONDA_PREFIX= VIRTUAL_ENV= RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr 2>&1 | tail -3
```

(Both `VIRTUAL_ENV` and `CONDA_PREFIX` are set in this shell and make maturin refuse; unset them for the build.)

- [ ] **Step 2: Smoke the option from Python against the Ensembl and merged golden fixtures**

```bash
cd ~/research/git/vepyr
CONDA_PREFIX= VIRTUAL_ENV= uv run python - <<'EOF'
import json, sys
sys.path.insert(0, "tests")
from pathlib import Path
import polars as pl, pyarrow as pa
from cache_metadata import copy_cache_with_source_metadata
from vepyr._core import create_annotator
G = Path("tests/data/golden")
scratch = Path("/private/tmp/claude-501/-Users-mwiewior-research-git-vepyr/9d6eda02-1533-4321-bc90-ab02a6b09fd9/scratchpad")
for name, src, kind in [("ensembl", G / "cache", "ensembl"), ("merged", Path("tests/data/golden_merged/cache"), "merged")]:
    cache = str(copy_cache_with_source_metadata(str(src), scratch / f"cache_{name}", kind, "115"))
    def run(opts):
        ann = create_annotator(str(G / "input.vcf.gz"), cache, json.dumps(opts), True, None)
        return pl.from_arrow(pa.Table.from_batches(list(ann), schema=ann.schema))
    base = {"everything": True, "reference_fasta_path": str(G / "reference.fa"), "buffer_size": 7}
    full = run(base)
    lo, hi = int(full["start"][30]), int(full["start"][60])
    pushed = run({**base, "regions": [{"chrom": "chr1", "start": lo, "end": hi}]})
    ref = full.filter((pl.col("chrom") == "chr1") & pl.col("start").is_between(lo, hi))
    print(name, "rows", pushed.height, "equal:", pushed.equals(ref))
    assert pushed.equals(ref)
EOF
```

Expected: both lines print `equal: True`. If the merged case differs, compare `pushed` and `ref` row by row on `SYMBOL`/`HGNC_ID` first: a mismatch there means the warm-up did not reach the donor buffer (check `overlap_width_bp` and `warm_up_start_row` in the `regions runs` trace with `VEP_PIPELINE_TRACE=1`).

- [ ] **Step 3: Remove the `[patch]`, run the full engine suite, push, open the PR**

```bash
cd ~/research/git/vepyr && git checkout -- Cargo.toml Cargo.lock
cd ~/research/git/datafusion-bio-functions
cargo test -p datafusion-bio-function-vep 2>&1 | tail -5
cargo clippy -p datafusion-bio-function-vep --all-targets -- -D warnings 2>&1 | tail -3
git push -u origin feat/regions-option
gh pr create --title "feat(vep): regions option for annotate_vep (exact range cuts on Merged/RefSeq)" --body-file - <<'EOF'
## Summary
- `regions` option in `options_json`: `[{chrom, start?, end?}]`, 1-based closed on `start`, resolved to VCF contigs through the alias set, merged per contig
- contigs without a region are skipped; an empty selection is an empty result
- per-contig *runs*: the context is prepared once, each run activates its own filtered lookup (index seek when the VCF is indexed)
- Merged/RefSeq: runs are aligned to whole input buffers via the existing count pass, warmed up with the existing `warm_up_worker_state`, rank-stopped; emitted rows are trimmed to the requested bounds before projection
- `regions` with sharded VCF output is rejected (`workers=1` only)

Design: vepyr `docs/superpowers/specs/2026-09-06-region-predicate-pushdown-design.md`

## Test plan
- [x] `cargo test -p datafusion-bio-function-vep`
- [x] vepyr golden fixtures (Ensembl + merged, `buffer_size=7`): `regions` output equals full output filtered in Polars

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

Record the PR number and, after merge, the merge SHA: the vepyr plan pins it.

---

## Self-review notes

- Spec coverage: option (T1, T2), contig selection (T2), runs (T6), stateful runs with count pass and warm-up (T6, T7), output trim (T8), rank-stop (T7), sharded rejection (T2), default path unchanged (T6 step 7, T7/T8 suite runs), unit tests for parsing/merging/mapping/gate/trim (T1-T5), donation-fixture parity (T7), no-regions default (T6 step 7).
- The engine has no VCF-plus-Parquet-cache fixture of its own; the end-to-end parity check lives in Task 9 against vepyr's golden fixtures and is repeated as pytest in the vepyr plan.
- Names used across tasks: `RegionSpec`, `RunBounds`, `ContigRuns`, `RunPlan`, `RunGate`, `GateOutput`, `filter_batch_to_bounds`, `ContigRun`, `RunActivationInputs`, `ActiveRunState`, `activate_run_lookup`, `reset_for_next_run`, `StreamState::ActivatingRun`, `ContigAnnotationConfig.regions`, `AnnotateProvider.regions`.
