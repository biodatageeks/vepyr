# Single `--workers` Concurrency Knob Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse vepyr's three concurrency knobs (`--forks`, `--workers`, `--threads`) into a single `--workers` knob that maps to the engine's proven within-contig fused pipeline.

**Architecture:** The engine (`datafusion-bio-functions`) already exposes exactly one within-contig parallelism knob: `AnnotateVcfConfig.threads` (drives lookup partitions + annotate workers + VCF format concurrency; byte-identical to serial). All knob proliferation lives in **vepyr**, which layers cross-contig `forks` + per-contig `workers` + within-contig `threads` on top. This plan removes `forks` and the old per-contig `workers` semantics from vepyr's public API, PyO3 bindings, CLI drivers, and docs, and redefines `workers` as the name of the within-contig knob (i.e. vepyr `workers` → engine `threads`). **No `datafusion-bio-functions` code changes are required.**

**Tech Stack:** Python 3 (PyO3 extension `vepyr._core`), Rust (pyo3, tokio, datafusion), pytest, cargo test, maturin.

## Global Constraints

- **Engine is unchanged.** Do not modify `datafusion-bio-functions`. vepyr's single `workers` knob maps onto the existing `AnnotateVcfConfig.threads` field by setting `threads = workers`, `forks = Some(0)`, `workers = 1`, `target_partitions = 1` in the Rust config struct vepyr builds.
- **Semantics of the new knob:** `workers` (default `1`) = number of within-contig fused annotation pipelines. `workers > 1` requires a tabix-indexed (bgzip + `.tbi`) input VCF — this is enforced by the engine (`vcf_sink.rs` validation); vepyr does not pre-check the index.
- **Removed outright (no deprecation shims):** the `forks` parameter, the `threads` parameter, and the old per-contig-lookup meaning of `workers`. Passing `forks=`, `threads=`, `chrom_parallelism=`, or `target_partitions=` to `vepyr.annotate()` must raise `TypeError` (they are simply not parameters).
- **PyO3 ABI changes in lockstep:** the Rust `annotate_vcf`/`create_annotator` signatures and the Python callers + `.pyi` stub must all change together; rebuild with `maturin develop` before running Python tests.
- After all tasks: `cargo test` green, `cargo clippy -- -D warnings` clean, `cargo fmt -- --check` clean, `pytest tests/` green (modulo pre-existing skips for missing golden cache).
- `partitions` (cache-build knob, `build_cache`) is **out of scope** — it is unrelated to annotation concurrency. Do not touch it.

---

## File Structure

Files modified (all in `/Users/mwiewior/research/git/vepyr`):

- `src/annotate.rs` — Rust glue: drop `forks`/`workers` params, derive tokio runtime size from the `threads` JSON value, simplify the options-normalization helper, build `AnnotateVcfConfig` with the single-knob mapping. Owns the Rust unit tests for this logic.
- `src/lib.rs` — PyO3 `#[pyfunction]` signatures for `annotate_vcf` / `create_annotator`: drop `forks`/`workers` args.
- `src/vepyr/_core.pyi` — type stubs: drop `forks`/`workers` from `annotate_vcf` / `create_annotator`.
- `src/vepyr/__init__.py` — public `annotate()`: drop `forks`/`threads` params, redefine `workers`, rewrite validation + options-dict construction + native call sites.
- `tests/test_annotate.py` — rewrite the forks/workers/threads tests to the single-knob contract.
- `tests/test_build_cache.py` — convert `test_annotation_forks_preserves_vcf_output` to a `workers`-based parity test.
- `e2e-testing/scripts/run_annotation_fast.py` — drop `--forks`/`--threads` CLI args; keep `--workers`.
- `e2e-testing/scripts/run_annotation_fast_all.py` — drop `--forks` CLI arg; keep `--workers`.
- `README.md`, `docs/quickstart.md`, `docs/performance.md` — doc updates.

---

## Task 1: Rust glue — single-knob mapping in `annotate.rs` + `lib.rs` + `.pyi`

**Files:**
- Modify: `src/annotate.rs` (helpers ~15-105, `annotate_to_vcf_file` ~166-350, `create_streaming_annotator` ~353-433, tests ~435-492)
- Modify: `src/lib.rs:89-144`
- Modify: `src/vepyr/_core.pyi:17-44`
- Test: Rust unit tests inside `src/annotate.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces (Rust → Python boundary):
  - `_core.annotate_vcf(vcf_path, cache_dir, output_path, options_json, show_progress=True, compression="", on_batch_written=None) -> int`
  - `_core.create_annotator(vcf_path, cache_dir, options_json, skip_csq=True, limit=None) -> StreamingAnnotator`
  - Both read the within-contig knob from the `"threads"` key of `options_json` (default `1`). No `forks`/`workers` positional args remain.

- [ ] **Step 1: Rewrite the Rust unit tests to express the new contract**

In `src/annotate.rs`, replace the entire `#[cfg(test)] mod tests { ... }` block (currently lines ~435-492) with:

```rust
#[cfg(test)]
mod tests {
    use super::{normalize_options, worker_thread_count};

    #[test]
    fn normalize_preserves_threads_and_cache_format() {
        let (json, fmt) =
            normalize_options(r#"{"cache_format":"lance","threads":4}"#).unwrap();
        let opts: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, "lance");
        assert_eq!(opts["threads"], 4);
        assert_eq!(opts["cache_format"], "lance");
    }

    #[test]
    fn default_cache_format_when_absent() {
        let (_json, fmt) = normalize_options(r#"{}"#).unwrap();
        assert_eq!(fmt, "indexed_parquet");
    }

    #[test]
    fn invalid_cache_format_is_rejected() {
        pyo3::Python::initialize();
        let err = normalize_options(r#"{"cache_format":"fjall"}"#).unwrap_err();
        assert!(err.to_string().contains("cache_format"));
    }

    #[test]
    fn worker_thread_count_is_at_least_one() {
        assert_eq!(worker_thread_count(0), 1);
        assert_eq!(worker_thread_count(1), 1);
        assert_eq!(worker_thread_count(8), 8);
    }
}
```

- [ ] **Step 2: Run the Rust tests to verify they fail (symbols not defined yet)**

Run: `cd /Users/mwiewior/research/git/vepyr && cargo test --lib annotate 2>&1 | tail -20`
Expected: FAIL — compile error `cannot find function normalize_options` / `worker_thread_count`.

- [ ] **Step 3: Replace the helper functions at the top of `annotate.rs`**

In `src/annotate.rs`, replace the four helper functions `effective_session_partitions`, `effective_runtime_threads`, `runtime_for_parallelism`, and `options_json_with_parallelism` (currently lines ~15-105) with the following three functions:

```rust
/// Number of tokio worker threads to provision. The DataFusion plan uses
/// `block_in_place()` while resolving table metadata, so a multi-thread
/// scheduler is required even for the single-pipeline (`threads=1`) case.
fn worker_thread_count(threads: usize) -> usize {
    threads.max(1)
}

fn runtime_for_threads(threads: usize) -> PyResult<Arc<Runtime>> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_thread_count(threads))
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))?;
    Ok(Arc::new(runtime))
}

/// Validate and normalize the options JSON. Ensures `cache_format` is present
/// and supported; returns `(normalized_json, cache_format)`. The within-contig
/// parallelism knob travels as the `"threads"` key and is read by the caller.
fn normalize_options(options_json: &str) -> PyResult<(String, String)> {
    let mut opts: Value = serde_json::from_str(options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let object = opts
        .as_object_mut()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("options JSON must be an object"))?;
    let cache_format = object
        .get("cache_format")
        .and_then(|v| v.as_str())
        .unwrap_or("indexed_parquet")
        .to_string();
    if !matches!(
        cache_format.as_str(),
        "indexed_parquet" | "legacy_fjall" | "lance"
    ) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "cache_format must be 'indexed_parquet', 'legacy_fjall', or 'lance'",
        ));
    }
    object.insert("cache_format".to_string(), Value::from(cache_format.clone()));
    let options_json = serde_json::to_string(&opts).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    Ok((options_json, cache_format))
}

/// Read the within-contig parallelism knob from the options JSON `"threads"`
/// key. Defaults to 1; values <= 0 are treated as 1.
fn threads_from_options(opts: &Value) -> usize {
    opts.get("threads")
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}
```

- [ ] **Step 4: Update `annotate_to_vcf_file` signature and body**

In `src/annotate.rs`, change the `annotate_to_vcf_file` function. Replace its signature (lines ~166-178) — remove the `forks: usize` and `workers: usize` parameters:

```rust
#[allow(clippy::too_many_arguments)]
pub fn annotate_to_vcf_file(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<Py<PyAny>>,
) -> PyResult<usize> {
```

Then replace the options/runtime preamble (currently lines ~187-197, from `let (options_json, cache_format) = options_json_with_parallelism(...)` through `let rt = runtime_for_parallelism(...)?;`) with:

```rust
    let (options_json, cache_format) = normalize_options(options_json)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let threads = threads_from_options(&opts);

    let backend = cache_format.as_str();
    let rt = runtime_for_threads(threads)?;
```

Then in the `AnnotateVcfConfig { ... }` literal, replace the four parallelism fields (currently lines ~314-325: `forks: Some(forks)`, `workers,`, the `threads:` block, and `target_partitions:`) with:

```rust
        // Single within-contig parallelism knob. vepyr's `workers` maps onto
        // the engine's `threads` (the proven byte-identical fused pipeline).
        // The forks/per-contig-workers cross-contig path is intentionally
        // pinned off.
        forks: Some(0),
        workers: 1,
        threads,
        target_partitions: 1,
```

- [ ] **Step 5: Update `create_streaming_annotator` signature and body**

In `src/annotate.rs`, change `create_streaming_annotator`. Replace its signature (lines ~353-363) — remove `forks`/`workers`:

```rust
#[allow(clippy::too_many_arguments)]
pub fn create_streaming_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
) -> PyResult<StreamingAnnotator> {
```

Replace the preamble (currently lines ~364-376, from `let (options_json, cache_format) = options_json_with_parallelism(...)` through `let session_partitions = effective_session_partitions(...);` inside the async block) so it reads:

```rust
    let (options_json, cache_format) = normalize_options(options_json)?;
    let opts: Value = serde_json::from_str(&options_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
    })?;
    let threads = threads_from_options(&opts);
    let rt = runtime_for_threads(threads)?;

    let (stream, schema) = rt.block_on(async {
        let backend = cache_format.as_str();
        let session_partitions = worker_thread_count(threads);
```

(Leave the rest of the async block — `SessionConfig::new().with_target_partitions(session_partitions)` onward — unchanged. Note the previously-unused `_opts` binding is now the real `opts` used above, so delete the old `let _opts: Value = ...` line that followed the options call.)

- [ ] **Step 6: Update PyO3 signatures in `lib.rs`**

In `src/lib.rs`, replace `annotate_vcf` (lines ~91-118) with:

```rust
/// Annotate a VCF and write results directly to a VCF file.
/// Returns the number of rows written.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, output_path, options_json, show_progress=true, compression="", on_batch_written=None))]
#[allow(clippy::too_many_arguments)]
fn annotate_vcf(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<Py<PyAny>>,
) -> PyResult<usize> {
    annotate::annotate_to_vcf_file(
        py,
        vcf_path,
        cache_dir,
        output_path,
        options_json,
        show_progress,
        compression,
        on_batch_written,
    )
}
```

And replace `create_annotator` (lines ~120-144) with:

```rust
/// Create a streaming VEP annotator that yields PyArrow RecordBatches.
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json, skip_csq=true, limit=None))]
fn create_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
) -> PyResult<annotate::StreamingAnnotator> {
    annotate::create_streaming_annotator(py, vcf_path, cache_dir, options_json, skip_csq, limit)
}
```

- [ ] **Step 7: Update the type stub `_core.pyi`**

In `src/vepyr/_core.pyi`, replace the `annotate_vcf` stub (lines 17-32) to drop `forks`/`workers`:

```python
def annotate_vcf(
    vcf_path: str,
    cache_dir: str,
    output_path: str,
    options_json: str,
    show_progress: bool = True,
    compression: str = "",
    on_batch_written: Callable[[int, int, int], None] | None = None,
) -> int:
    """Annotate a VCF and write results directly to a VCF file.

    Returns the number of rows written.
    """
    ...
```

And replace the `create_annotator` stub (lines 34-44):

```python
def create_annotator(
    vcf_path: str,
    cache_dir: str,
    options_json: str,
    skip_csq: bool = True,
    limit: int | None = None,
) -> StreamingAnnotator:
    """Create a streaming VEP annotator that yields PyArrow RecordBatches."""
    ...
```

- [ ] **Step 8: Run the Rust tests to verify they pass**

Run: `cd /Users/mwiewior/research/git/vepyr && cargo test --lib annotate 2>&1 | tail -20`
Expected: PASS — `normalize_preserves_threads_and_cache_format`, `default_cache_format_when_absent`, `invalid_cache_format_is_rejected`, `worker_thread_count_is_at_least_one` all pass.

- [ ] **Step 9: Verify clippy + fmt are clean**

Run: `cd /Users/mwiewior/research/git/vepyr && cargo clippy --lib -- -D warnings 2>&1 | tail -15 && cargo fmt -- --check`
Expected: no warnings, no diff. (If `fmt --check` reports a diff, run `cargo fmt` and re-check.)

- [ ] **Step 10: Commit**

```bash
cd /Users/mwiewior/research/git/vepyr
git add src/annotate.rs src/lib.rs src/vepyr/_core.pyi
git commit -m "refactor(vepyr): single within-contig workers knob in Rust glue

Drop forks/workers PyO3 args; the within-contig knob travels via the
options-JSON 'threads' key (vepyr workers -> engine threads). Pin the
cross-contig forks path off in AnnotateVcfConfig."
```

---

## Task 2: Python `annotate()` — collapse to a single `workers` knob

**Files:**
- Modify: `src/vepyr/__init__.py` (signature ~452-454, validation ~647-652, options dict ~662-671 and ~735-738, native call sites ~789-799, ~838-846, ~854-873)
- Test: `tests/test_annotate.py`

**Interfaces:**
- Consumes: the rebuilt `_core.annotate_vcf` / `_core.create_annotator` from Task 1 (no `forks`/`workers` args).
- Produces: `vepyr.annotate(..., workers: int = 1)`. `workers > 1` sets `options_json["threads"] = workers`. `forks`/`threads`/`chrom_parallelism`/`target_partitions` are not parameters (passing them → `TypeError`).

- [ ] **Step 1: Rebuild the extension so Python sees the new binding**

Run: `cd /Users/mwiewior/research/git/vepyr && maturin develop 2>&1 | tail -5`
Expected: build succeeds, `vepyr._core` reinstalled.

- [ ] **Step 2: Rewrite the affected tests in `tests/test_annotate.py`**

Make the following edits.

(a) Replace `test_has_forks_and_workers_params` (lines 47-56) with:

```python
    def test_has_workers_param_only(self):
        import vepyr

        sig = inspect.signature(vepyr.annotate)
        assert "forks" not in sig.parameters
        assert "threads" not in sig.parameters
        assert "target_partitions" not in sig.parameters
        assert "chrom_parallelism" not in sig.parameters
        workers = sig.parameters["workers"]
        assert workers.default == 1
```

(b) Replace `test_parallelism_forwards_to_streaming_annotator` (lines 69-111) with:

```python
    def test_workers_forward_to_streaming_annotator(self, monkeypatch):
        import pyarrow as pa
        import vepyr

        seen = []

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        def fake_create_annotator(
            vcf_path,
            cache_dir,
            options_json,
            skip_csq=True,
            limit=None,
        ):
            seen.append((json.loads(options_json), limit))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            workers=4,
        )

        assert isinstance(lf, pl.LazyFrame)
        assert seen[0][1] is None
        assert seen[0][0]["cache_format"] == "lance"
        assert seen[0][0]["threads"] == 4
        assert "forks" not in seen[0][0]
        assert "contig_parallelism" not in seen[0][0]
        assert "annotation_workers" not in seen[0][0]
```

(c) Replace `test_single_chrom_workers_do_not_enable_chunked_lookup` (lines 113-151) with:

```python
    def test_workers_one_omits_threads_key(self, monkeypatch):
        import pyarrow as pa
        import vepyr

        seen = []

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        def fake_create_annotator(
            vcf_path,
            cache_dir,
            options_json,
            skip_csq=True,
            limit=None,
        ):
            seen.append(json.loads(options_json))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(INPUT_VCF, CACHE_DIR, workers=1)

        assert isinstance(lf, pl.LazyFrame)
        assert "threads" not in seen[0]
        assert "forks" not in seen[0]
```

(d) Delete `test_multi_chrom_single_worker_enables_chunked_lookup` entirely (lines 153-191) — the multi-fork / chunked-lookup behavior no longer exists.

(e) In `test_pick_options_forward_to_vcf_writer`, update the `fake_annotate_vcf` signature (lines 378-388) — remove the trailing `forks,` and `workers,`:

```python
        def fake_annotate_vcf(
            vcf_path,
            cache_dir,
            output_path,
            options_json,
            show_progress,
            compression,
            on_batch_written,
        ):
            seen["options"] = json.loads(options_json)
            return 0
```

(f) In `test_buffer_size_forwards_to_vcf_writer`, apply the identical signature change to its `fake_annotate_vcf` (lines 430-440): remove the trailing `forks,` and `workers,`.

(g) Replace `test_forks_forwards_to_vcf_writer` (lines 472-517) with:

```python
    def test_workers_forward_to_vcf_writer(self, monkeypatch):
        import vepyr

        seen = {}

        def fake_annotate_vcf(
            vcf_path,
            cache_dir,
            output_path,
            options_json,
            show_progress,
            compression,
            on_batch_written,
        ):
            seen["options"] = json.loads(options_json)
            return 0

        monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf=out_path,
                show_progress=False,
                workers=4,
            )
            assert result == out_path
            assert seen["options"]["cache_format"] == "lance"
            assert seen["options"]["threads"] == 4
            assert "forks" not in seen["options"]
            assert "contig_parallelism" not in seen["options"]
        finally:
            os.unlink(out_path)
```

(h) Replace `test_forks_rejects_invalid_values` (lines 543-554) and `test_workers_gt_one_requires_forks` (lines 581-591) with these two tests (the old rules are gone; `forks`/`threads` must now be rejected as unknown kwargs):

```python
    @pytest.mark.parametrize("removed", ["forks", "threads"])
    def test_removed_knobs_rejected(self, removed):
        import vepyr

        with pytest.raises(TypeError, match=removed):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                **{removed: 2},
            )
```

(i) In `test_notebook_progress_updates_on_main_thread`, update its `fake_annotate_vcf` signature (lines 627-637): remove the trailing `forks,` and `workers,`.

(Leave `test_workers_rejects_invalid_values` at lines 568-579 unchanged — it still asserts `workers <= 0` raises. Leave `test_chrom_parallelism_removed_from_public_api` unchanged.)

- [ ] **Step 3: Run the rewritten tests to verify they fail against the current Python `annotate()`**

Run: `cd /Users/mwiewior/research/git/vepyr && python -m pytest tests/test_annotate.py -k "workers or removed_knobs" -x 2>&1 | tail -25`
Expected: FAIL — e.g. `test_removed_knobs_rejected` fails because `annotate()` still accepts `forks=`/`threads=`; `test_workers_forward_to_vcf_writer` fails because the options dict still contains `forks`/`contig_parallelism` and no `threads`.

- [ ] **Step 4: Update the `annotate()` signature in `__init__.py`**

In `src/vepyr/__init__.py`, replace the three lines (452-454):

```python
    forks: int = 0,
    workers: int = 1,
    threads: int = 1,
```

with:

```python
    workers: int = 1,
```

- [ ] **Step 5: Update validation in `__init__.py`**

Replace the validation block (lines 647-652):

```python
    if isinstance(forks, bool) or not isinstance(forks, int) or forks < 0:
        raise ValueError("forks must be a non-negative integer")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer")
    if workers > 1 and forks <= 0:
        raise ValueError("workers > 1 requires forks > 0")
```

with:

```python
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer")
```

- [ ] **Step 6: Remove the forks block from options-dict construction**

Replace the forks if/else block (lines 662-671):

```python
    if forks > 0:
        opts["contig_parallelism"] = forks
        opts["annotation_workers"] = workers
        opts["forks"] = workers
        opts["inline_lookup"] = False
        if forks > 1:
            opts["chunked_buffer_lookup"] = True
    else:
        opts["forks"] = 0
        opts["inline_lookup"] = True
```

with (nothing — delete the whole block). The engine defaults to the strict single-lane inline path when no `forks`/`contig_parallelism` keys are present.

- [ ] **Step 7: Map `workers` onto the within-contig `threads` key**

Replace the threads block (lines 735-738):

```python
    if threads and threads > 1:
        # Single within-contig parallelism knob: N per-partition annotation
        # pipelines. Requires a tabix-indexed (bgzip+.tbi) input VCF.
        opts["threads"] = threads
```

with:

```python
    if workers > 1:
        # Single within-contig parallelism knob: N per-partition annotation
        # pipelines. Requires a tabix-indexed (bgzip+.tbi) input VCF.
        opts["threads"] = workers
```

- [ ] **Step 8: Drop `forks`/`workers` from the native call sites**

(a) In the `_annotate_vcf(...)` call inside `_run` (lines 789-799), remove the trailing `forks,` and `workers,` arguments so it ends:

```python
                    _result[0] = _annotate_vcf(
                        vcf,
                        cache_dir,
                        output_vcf,
                        options_json,
                        False,
                        comp,
                        callback,
                    )
```

(b) In the probe call (lines 838-846), remove the trailing `forks,` and `workers,`:

```python
    probe = _create_annotator(
        vcf,
        cache_dir,
        options_json,
        skip_csq,
        None,
    )
```

(c) Replace the capture tuple (lines 854-861):

```python
    _vcf, _cache_dir, _opts, _skip, _forks, _workers = (
        vcf,
        cache_dir,
        options_json,
        skip_csq,
        forks,
        workers,
    )
```

with:

```python
    _vcf, _cache_dir, _opts, _skip = (
        vcf,
        cache_dir,
        options_json,
        skip_csq,
    )
```

(d) In `_batch_source` (lines 865-873), remove the trailing `_forks,` and `_workers,`:

```python
        annotator = _create_annotator(
            _vcf,
            _cache_dir,
            _opts,
            _skip,
            n_rows,
        )
```

- [ ] **Step 9: Update the docstring parameter list**

In the `annotate()` docstring, find the `forks` / `workers` parameter descriptions (under "Engine tuning") and replace them with a single `workers` entry. Use exactly:

```
    workers : int
        Number of within-contig fused annotation pipelines (``1`` = serial).
        The single concurrency knob. Values greater than 1 require a
        tabix-indexed (bgzip + ``.tbi``) input VCF.
```

(Remove any `forks`/`threads` lines from the docstring. If the exact prose differs, match on the parameter name and replace the whole paragraph.)

- [ ] **Step 10: Run the full `test_annotate.py` suite**

Run: `cd /Users/mwiewior/research/git/vepyr && python -m pytest tests/test_annotate.py 2>&1 | tail -25`
Expected: PASS (golden-cache integration tests may `skip` if the cache is absent — that is fine; the monkeypatched + signature + validation tests must pass).

- [ ] **Step 11: Commit**

```bash
cd /Users/mwiewior/research/git/vepyr
git add src/vepyr/__init__.py tests/test_annotate.py
git commit -m "refactor(vepyr): collapse annotate() to a single workers knob

Remove forks/threads params; workers>1 now sets the within-contig
'threads' option. Drop forks/workers from native call sites."
```

---

## Task 3: CLI drivers — single `--workers` flag

**Files:**
- Modify: `e2e-testing/scripts/run_annotation_fast.py` (args 163-184, validation 221-226, call 726-728)
- Modify: `e2e-testing/scripts/run_annotation_fast_all.py` (arg 138-147, validation 172-177, `run_chromosome` 191-218, call 729-737)

**Interfaces:**
- Consumes: `vepyr.annotate(..., workers=...)` from Task 2.
- Produces: both scripts expose only `--workers` (default `1`). `--forks` and `--threads` are removed.

- [ ] **Step 1: `run_annotation_fast.py` — remove `--forks` and `--threads`, keep `--workers`**

Replace the three argparse blocks (lines 163-184) — `--forks`, `--workers`, `--threads` — with just `--workers`:

```python
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Within-contig parallel annotation pipelines; >1 requires a "
        "tabix-indexed input (default: %(default)s)",
    )
```

- [ ] **Step 2: `run_annotation_fast.py` — simplify validation**

Replace the validation block (lines 221-226):

```python
    if args.forks < 0:
        p.error("--forks must be a non-negative integer")
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
    if args.workers > 1 and args.forks <= 0:
        p.error("--workers > 1 requires --forks > 0")
```

with:

```python
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
```

- [ ] **Step 3: `run_annotation_fast.py` — fix the `annotate()` call**

Replace the call args (lines 726-728):

```python
            forks=args.forks,
            workers=args.workers,
            threads=args.threads,
```

with:

```python
            workers=args.workers,
```

- [ ] **Step 4: `run_annotation_fast.py` — verify it parses and reports help correctly**

Run: `cd /Users/mwiewior/research/git/vepyr && python e2e-testing/scripts/run_annotation_fast.py --help 2>&1 | grep -E "\-\-(forks|threads|workers)"`
Expected: only a `--workers` line appears; no `--forks`, no `--threads`.

- [ ] **Step 5: `run_annotation_fast_all.py` — remove `--forks`**

Delete the `--forks` argparse block (lines 138-147). Leave the `--workers` block that follows it unchanged.

- [ ] **Step 6: `run_annotation_fast_all.py` — simplify validation**

Replace the validation block (lines 172-177):

```python
    if args.forks < 0:
        p.error("--forks must be a non-negative integer")
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
    if args.workers > 1 and args.forks <= 0:
        p.error("--workers > 1 requires --forks > 0")
```

with:

```python
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
```

- [ ] **Step 7: `run_annotation_fast_all.py` — update `run_chromosome` to drop `forks`**

Replace the `run_chromosome` signature (lines 191-199):

```python
def run_chromosome(
    chrom_num,
    cache="ensembl",
    backend="lance",
    forks=0,
    workers=1,
    force=False,
    skip_comparison=False,
):
```

with:

```python
def run_chromosome(
    chrom_num,
    cache="ensembl",
    backend="lance",
    workers=1,
    force=False,
    skip_comparison=False,
):
```

Then in the `cmd` list inside `run_chromosome` (lines 210-213), remove the `--forks` pair:

```python
        "--forks",
        str(forks),
        "--workers",
        str(workers),
```

becomes:

```python
        "--workers",
        str(workers),
```

- [ ] **Step 8: `run_annotation_fast_all.py` — update the call site**

In the `run_chromosome(...)` call (lines 729-737), remove the `forks=args.forks,` line, leaving `workers=args.workers,`.

- [ ] **Step 9: Verify both scripts import and parse**

Run: `cd /Users/mwiewior/research/git/vepyr && python e2e-testing/scripts/run_annotation_fast_all.py --help 2>&1 | grep -E "\-\-(forks|threads|workers)" && python -m pytest tests/test_run_annotation_fast.py 2>&1 | tail -10`
Expected: only `--workers` shown for `_all.py`; `test_run_annotation_fast.py` passes (or skips if it needs data, but does not error on import).

- [ ] **Step 10: Commit**

```bash
cd /Users/mwiewior/research/git/vepyr
git add e2e-testing/scripts/run_annotation_fast.py e2e-testing/scripts/run_annotation_fast_all.py
git commit -m "refactor(vepyr): single --workers flag in e2e CLI drivers"
```

---

## Task 4: Parity test + docs

**Files:**
- Modify: `tests/test_build_cache.py:762-786`
- Modify: `README.md:88-112`, `docs/quickstart.md:125-138`, `docs/performance.md:82-83`

**Interfaces:**
- Consumes: `vepyr.annotate(..., workers=...)` from Task 2.

- [ ] **Step 1: Convert the forks parity test to a workers parity test**

In `tests/test_build_cache.py`, replace `test_annotation_forks_preserves_vcf_output` (lines 762-786) with a `workers`-based parity test. `workers > 1` requires a tabix-indexed input, so build one from `sample.vcf` with pysam; skip the parallel arm if pysam is unavailable:

```python
    def test_annotation_workers_preserves_vcf_output(self, built_cache, tmp_path):
        out, _, _, _ = built_cache
        input_vcf = ENSEMBL_CACHE_DIR / "sample.vcf"
        serial_vcf = tmp_path / "serial.vcf"
        parallel_vcf = tmp_path / "parallel.vcf"

        vepyr.annotate(
            str(input_vcf),
            out,
            check_existing=True,
            output_vcf=str(serial_vcf),
            show_progress=False,
            workers=1,
        )

        # workers>1 needs a tabix-indexed (bgzip+.tbi) input.
        try:
            import pysam
        except ImportError:
            pytest.skip("pysam not available for tabix-indexed parallel input")

        indexed_vcf = tmp_path / "sample.vcf.gz"
        pysam.tabix_compress(str(input_vcf), str(indexed_vcf), force=True)
        pysam.tabix_index(str(indexed_vcf), preset="vcf", force=True)

        vepyr.annotate(
            str(indexed_vcf),
            out,
            check_existing=True,
            output_vcf=str(parallel_vcf),
            show_progress=False,
            workers=2,
        )

        assert read_vcf_data_lines(parallel_vcf) == read_vcf_data_lines(serial_vcf)
```

- [ ] **Step 2: Run the parity test**

Run: `cd /Users/mwiewior/research/git/vepyr && python -m pytest tests/test_build_cache.py::TestBuildCache::test_annotation_workers_preserves_vcf_output 2>&1 | tail -15`
Expected: PASS, or SKIP if the ensembl cache / pysam are unavailable. (If the class name differs, find the test by method name.)

- [ ] **Step 3: Update `README.md`**

Replace lines 88-112 (the `forks` prose + the two `forks=`/`forks=8` examples) with:

```markdown
`workers` controls how many within-contig annotation pipelines run
concurrently. `workers=1` is the serial path; `workers > 1` requires a
tabix-indexed (bgzip + `.tbi`) input VCF.

```python
df = vepyr.annotate(
    "input.vcf.gz",
    cache_dir,
    workers=4,
).collect()
```

`build_cache()` writes variation as `chrN_warm.parquet` and
`chrN_cold.parquet` files, plus cold-position and variant-bloom indexes.
Re-running
`build_cache()` is idempotent by default; pass `overwrite=True` to rebuild
existing cache outputs.

```python
out = vepyr.annotate(
    "input.vcf.gz",
    cache_dir,
    workers=8,
    output_vcf="annotated.vcf",
)
```
```

- [ ] **Step 4: Update `docs/quickstart.md`**

Replace lines 125-138 (the fjall `forks`/`workers` prose + the `forks=1, workers=4` example) with:

```markdown
`workers` controls how many within-contig annotation pipelines run
concurrently. `workers=1` is the serial path; `workers > 1` requires a
tabix-indexed (bgzip + `.tbi`) input VCF.

```python
df = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/parquet/115_GRCh38_ensembl",
    workers=4,
).collect()
```
```

- [ ] **Step 5: Update `docs/performance.md`**

Replace the two table rows (lines 82-83):

```markdown
| `forks` | `0` | Active chromosome lanes. `0` uses the strict single-lane path with `workers=1`; values greater than 0 require `use_fjall=True`. |
| `workers` | `1` | Annotation workers per active chromosome. Values greater than 1 require `forks > 0`. |
```

with a single row:

```markdown
| `workers` | `1` | Within-contig annotation pipelines. `1` is serial; values greater than 1 require a tabix-indexed (bgzip + `.tbi`) input VCF. |
```

- [ ] **Step 6: Final repo-wide sweep for stragglers**

Run: `cd /Users/mwiewior/research/git/vepyr && grep -rnE "forks=|threads=|--forks|--threads" --include="*.py" --include="*.md" . | grep -viE "worker_threads|rt-multi-thread|tabix" | grep -v "/target/"`
Expected: no matches (every `forks=`/`threads=`/`--forks`/`--threads` reference has been removed or is an unrelated `worker_threads` mention).

- [ ] **Step 7: Full test + lint gate**

Run: `cd /Users/mwiewior/research/git/vepyr && cargo test 2>&1 | tail -10 && cargo clippy -- -D warnings 2>&1 | tail -5 && cargo fmt -- --check && python -m pytest tests/ 2>&1 | tail -15`
Expected: cargo green, clippy clean, fmt clean, pytest green (pre-existing skips allowed).

- [ ] **Step 8: Commit**

```bash
cd /Users/mwiewior/research/git/vepyr
git add tests/test_build_cache.py README.md docs/quickstart.md docs/performance.md
git commit -m "docs+test(vepyr): document single workers knob; workers-based parity test"
```

---

## Out of Scope / Follow-ups

- **Engine-internal rename `AnnotateVcfConfig.threads` -> `workers`** and deletion of the now-unreachable cross-contig forks / `chromosome_lanes` execution machinery in `datafusion-bio-functions` (`vcf_sink.rs` `VepConcurrencyPlan::from_forks`, the chromosome-lane `JoinSet` path, the per-contig-lookup `workers` field). This is a larger, higher-risk internal refactor and overlaps the existing "Lance-only dead-code removal" track. After this plan, vepyr never triggers the forks path, so those fields are dead from vepyr's perspective but still compile and pass engine tests. Defer.
- **`partitions`** (cache-build parallelism in `build_cache`) is deliberately untouched — it is not an annotation concurrency knob.

---

## Self-Review

- **Spec coverage:** single `--workers` knob = within-contig fused (Task 1 Rust mapping + Task 2 Python); remove `--forks`/`--threads`/old-`workers` outright (Task 1 bindings, Task 2 params/validation, Task 3 CLI); docs (Task 4). Engine untouched (Global Constraints). Covered.
- **Type consistency:** `normalize_options` / `worker_thread_count` / `threads_from_options` / `runtime_for_threads` are defined in Task 1 Step 3 and used in Steps 4-5 and tested in Step 1. PyO3 signatures (Task 1 Steps 6-7) match the Python call sites edited in Task 2 Step 8 and the fakes in Task 2 Step 2 (`fake_create_annotator(vcf_path, cache_dir, options_json, skip_csq=True, limit=None)`; `fake_annotate_vcf(vcf_path, cache_dir, output_path, options_json, show_progress, compression, on_batch_written)`).
- **Placeholder scan:** no TBD/"handle errors"/"similar to" — all steps carry concrete code or exact commands.
