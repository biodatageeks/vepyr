# Annotation Target Partitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt upstream `datafusion-bio-functions` PR #163 in vepyr and expose safe fjall annotation parallelism through `vepyr.annotate(target_partitions=N)`.

**Architecture:** The upstream crate owns the core execution change: partition-local fjall lookup sinks, bounded worker tasks, and ordered partition draining. vepyr should bump to the upstream PR revision, expose `target_partitions` as a Python/Rust bridge argument, and apply it only when `use_fjall=True`; parquet/non-fjall annotation must stay single-partition to avoid the data-loss regression identified in the upstream PR review.

**Tech Stack:** Rust 2021, PyO3 0.25, DataFusion 50.3, `datafusion-bio-function-vep`, Python wrapper in `src/vepyr/__init__.py`, pytest, cargo, maturin.

---

## Current Upstream Facts

- Upstream PR: https://github.com/biodatageeks/datafusion-bio-functions/pull/163
- Current PR head inspected on 2026-05-20: `3d6befd2817b43dd16bedc05e2f34eaeff692e16`
- Current local pin in `Cargo.toml`: `datafusion-bio-function-vep` rev `9b60f86de4765096538547f320dbbc476ea37031`
- The PR adds `AnnotateVcfConfig::target_partitions`, partition-local colocated sinks, fjall lookup partition workers, ordered receiver draining, and partition-invariance tests.
- The PR states `target_partitions` is a DataFusion `SessionConfig` setting, not an `annotate_vep()` JSON option.
- The PR currently has a P1 review finding: applying `target_partitions > 1` to the non-fjall/parquet path can drop rows because that path still executes only partition 0. vepyr must guard this even if upstream later fixes it.

## File Structure

- Modify `Cargo.toml`: bump `datafusion-bio-function-vep` to PR #163 head `3d6befd2817b43dd16bedc05e2f34eaeff692e16`.
- Modify `Cargo.lock`: regenerate after the dependency bump.
- Modify `src/vepyr/__init__.py`: add public `target_partitions`, validate it, keep it out of `options_json`, and pass it separately to native calls.
- Modify `src/lib.rs`: add `target_partitions` to the PyO3 `annotate_vcf` and `create_annotator` signatures.
- Modify `src/annotate.rs`: add native `target_partitions` parameters, apply them to `SessionConfig` and `AnnotateVcfConfig`, and clamp non-fjall execution to 1.
- Modify `src/vepyr/_core.pyi`: update native stubs.
- Modify `tests/test_annotate.py`: add public API/validation/routing tests and update fake native call signatures.
- Modify `tests/test_build_cache.py`: add a small built-cache fjall invariance test using `target_partitions=1` and `target_partitions=2`.
- Modify `e2e-testing/scripts/run_annotation_fast.py` and `e2e-testing/scripts/run_annotation_fast_all.py`: add a benchmark CLI flag for target partitions.
- Modify `tests/test_run_annotation_fast.py`: cover the new e2e CLI flag.
- Modify `README.md`, `docs/quickstart.md`, `docs/performance.md`, and `e2e-testing/README.md`: document fjall-only annotation parallelism.

## Compatibility Decision

Expose one public Python parameter:

```python
target_partitions: int = 1
```

Rules:

```text
target_partitions == 1             -> valid for parquet and fjall
target_partitions > 1 + use_fjall  -> valid and enables upstream ordered fjall lookup workers
target_partitions > 1 + parquet    -> ValueError
target_partitions <= 0 or bool     -> ValueError
```

Do not serialize `target_partitions` into `options_json`. Pass it as a separate native argument so `annotate_vep()` receives only supported annotation options.

---

### Task 1: Add Failing Python API Tests

**Files:**
- Modify: `tests/test_annotate.py`

- [ ] **Step 1: Add a signature test**

Add this test to `TestAnnotate`:

```python
def test_has_target_partitions_param(self):
    import vepyr

    sig = inspect.signature(vepyr.annotate)
    p = sig.parameters["target_partitions"]
    assert p.default == 1
```

- [ ] **Step 2: Add validation tests**

Add these tests near the existing `buffer_size` validation tests:

```python
@pytest.mark.parametrize("value", [0, -1, True])
def test_target_partitions_rejects_invalid_values(self, value):
    import vepyr

    with pytest.raises(
        ValueError, match="target_partitions must be a positive integer"
    ):
        vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            output_vcf="unused.vcf",
            show_progress=False,
            target_partitions=value,
        )

def test_target_partitions_requires_fjall_when_greater_than_one(self):
    import vepyr

    with pytest.raises(
        ValueError, match="target_partitions > 1 requires use_fjall=True"
    ):
        vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            output_vcf="unused.vcf",
            show_progress=False,
            target_partitions=2,
        )
```

- [ ] **Step 3: Add a VCF-output routing test**

Add this test near `test_buffer_size_forwards_to_vcf_writer`:

```python
def test_target_partitions_forwards_to_vcf_writer_outside_options(self, monkeypatch):
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
        target_partitions,
    ):
        seen["options"] = json.loads(options_json)
        seen["target_partitions"] = target_partitions
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
            use_fjall=True,
            target_partitions=4,
        )
        assert result == out_path
        assert seen["target_partitions"] == 4
        assert seen["options"]["use_fjall"] is True
        assert "target_partitions" not in seen["options"]
    finally:
        os.unlink(out_path)
```

- [ ] **Step 4: Add a LazyFrame routing test**

Add this test near the basic LazyFrame tests:

```python
def test_target_partitions_forwards_to_streaming_annotator(self, monkeypatch):
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
        target_partitions=1,
    ):
        seen.append((json.loads(options_json), target_partitions, limit))
        return FakeAnnotator()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

    lf = vepyr.annotate(
        INPUT_VCF,
        CACHE_DIR,
        use_fjall=True,
        target_partitions=4,
    )

    assert isinstance(lf, pl.LazyFrame)
    assert seen[0][1] == 4
    assert seen[0][2] is None
    assert seen[0][0]["use_fjall"] is True
    assert "target_partitions" not in seen[0][0]
```

- [ ] **Step 5: Run the new tests and confirm they fail**

Run:

```bash
uv run pytest \
  tests/test_annotate.py::TestAnnotate::test_has_target_partitions_param \
  tests/test_annotate.py::TestAnnotate::test_target_partitions_forwards_to_vcf_writer_outside_options \
  tests/test_annotate.py::TestAnnotate::test_target_partitions_forwards_to_streaming_annotator \
  -q
```

Expected: tests fail because `vepyr.annotate()` has no `target_partitions` parameter yet.

---

### Task 2: Implement The Python Public API

**Files:**
- Modify: `src/vepyr/__init__.py`
- Modify: `tests/test_annotate.py`

- [ ] **Step 1: Add the public parameter**

In `vepyr.annotate`, add `target_partitions` under engine tuning:

```python
# Engine tuning
cache_size_mb: int = 1024,
target_partitions: int = 1,
skip_csq: bool = True,
```

- [ ] **Step 2: Document the parameter**

Add this docstring entry near `cache_size_mb`:

```python
target_partitions : int
    DataFusion target partitions for fjall annotation lookup workers
    (default: 1). Values greater than 1 require ``use_fjall=True`` and
    preserve VCF/input row order by draining upstream lookup partitions in
    order.
```

- [ ] **Step 3: Add validation**

Place this after the `buffer_size` validation:

```python
if (
    isinstance(target_partitions, bool)
    or not isinstance(target_partitions, int)
    or target_partitions <= 0
):
    raise ValueError("target_partitions must be a positive integer")
if target_partitions > 1 and not use_fjall:
    raise ValueError("target_partitions > 1 requires use_fjall=True")
```

- [ ] **Step 4: Keep `target_partitions` out of annotation options**

Leave this block unchanged except for not adding `target_partitions`:

```python
opts: dict = {
    "extended_probes": extended_probes,
    "buffer_size": buffer_size,
}
```

- [ ] **Step 5: Pass `target_partitions` to native VCF output**

Change the `_annotate_vcf` call to append the separate argument:

```python
_result[0] = _annotate_vcf(
    vcf,
    cache_dir,
    output_vcf,
    options_json,
    False,
    comp,
    callback,
    target_partitions,
)
```

- [ ] **Step 6: Pass `target_partitions` to streaming annotator creation**

Change the probe call:

```python
probe = _create_annotator(vcf, cache_dir, options_json, skip_csq, None, target_partitions)
```

Change the closure captures:

```python
_vcf, _cache_dir, _opts, _skip, _target_partitions = (
    vcf,
    cache_dir,
    options_json,
    skip_csq,
    target_partitions,
)
```

Change the per-collect annotator call:

```python
annotator = _create_annotator(
    _vcf,
    _cache_dir,
    _opts,
    _skip,
    n_rows,
    _target_partitions,
)
```

- [ ] **Step 7: Update existing fake `_annotate_vcf` functions**

Every fake `_annotate_vcf` in `tests/test_annotate.py` must accept the appended argument:

```python
def fake_annotate_vcf(
    vcf_path,
    cache_dir,
    output_path,
    options_json,
    show_progress,
    compression,
    on_batch_written,
    target_partitions,
):
    seen.append(json.loads(options_json))
    return 0
```

For fake callbacks that currently assert progress behavior, keep their existing body and only add the `target_partitions` parameter to the signature.

- [ ] **Step 8: Run focused Python tests**

Run:

```bash
uv run pytest tests/test_annotate.py -q
```

Expected: pure Python tests should pass except failures caused by the native `_core` signatures not yet accepting the new argument.

---

### Task 3: Adopt Upstream PR #163 And Update The Native Bridge

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/lib.rs`
- Modify: `src/annotate.rs`
- Modify: `src/vepyr/_core.pyi`

- [ ] **Step 1: Bump `datafusion-bio-function-vep`**

Change the dependency in `Cargo.toml` to:

```toml
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", rev = "3d6befd2817b43dd16bedc05e2f34eaeff692e16", features = ["cache-builder"] }
```

Leave the `datafusion-bio-format-*` pins unchanged unless `cargo update` proves the upstream crate requires a compatible format revision.

- [ ] **Step 2: Regenerate the lockfile**

Run:

```bash
cargo update -p datafusion-bio-function-vep
```

Expected: `Cargo.lock` records:

```text
source = "git+https://github.com/biodatageeks/datafusion-bio-functions.git?rev=3d6befd2817b43dd16bedc05e2f34eaeff692e16#3d6befd2817b43dd16bedc05e2f34eaeff692e16"
```

- [ ] **Step 3: Add native target-partition helper**

In `src/annotate.rs`, add near the runtime static:

```rust
fn effective_target_partitions(use_fjall: bool, target_partitions: usize) -> usize {
    if use_fjall {
        target_partitions.max(1)
    } else {
        1
    }
}
```

- [ ] **Step 4: Extend `annotate_to_vcf_file`**

Change the function signature in `src/annotate.rs`:

```rust
pub fn annotate_to_vcf_file(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<PyObject>,
    target_partitions: usize,
) -> PyResult<usize> {
```

Replace the backend derivation with reusable booleans:

```rust
let use_fjall = opts
    .get("use_fjall")
    .and_then(|v| v.as_bool())
    .unwrap_or(false);
let backend = if use_fjall { "fjall" } else { "parquet" };
let target_partitions = effective_target_partitions(use_fjall, target_partitions);
```

Add the new upstream field to `AnnotateVcfConfig`:

```rust
target_partitions,
```

- [ ] **Step 5: Extend `create_streaming_annotator`**

Change the function signature in `src/annotate.rs`:

```rust
pub fn create_streaming_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
    target_partitions: usize,
) -> PyResult<StreamingAnnotator> {
```

Inside the async block passed to `rt.block_on`, parse options before creating the session:

```rust
let opts: Value = serde_json::from_str(options_json).map_err(|e| {
    pyo3::exceptions::PyValueError::new_err(format!("Invalid options JSON: {e}"))
})?;
let use_fjall = opts
    .get("use_fjall")
    .and_then(|v| v.as_bool())
    .unwrap_or(false);
let backend = if use_fjall { "fjall" } else { "parquet" };
let target_partitions = effective_target_partitions(use_fjall, target_partitions);

let config = SessionConfig::new().with_target_partitions(target_partitions);
let ctx = SessionContext::new_with_config(config);
register_vep_functions(&ctx);
```

Remove the later duplicate `let opts: Value` parse and duplicate backend derivation.

- [ ] **Step 6: Extend PyO3 wrapper functions**

Change `annotate_vcf` in `src/lib.rs`:

```rust
#[pyo3(signature = (vcf_path, cache_dir, output_path, options_json, show_progress=true, compression="", on_batch_written=None, target_partitions=1))]
#[allow(clippy::too_many_arguments)]
fn annotate_vcf(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    output_path: &str,
    options_json: &str,
    show_progress: bool,
    compression: &str,
    on_batch_written: Option<PyObject>,
    target_partitions: usize,
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
        target_partitions,
    )
}
```

Change `create_annotator` in `src/lib.rs`:

```rust
#[pyfunction]
#[pyo3(signature = (vcf_path, cache_dir, options_json, skip_csq=true, limit=None, target_partitions=1))]
fn create_annotator(
    py: Python<'_>,
    vcf_path: &str,
    cache_dir: &str,
    options_json: &str,
    skip_csq: bool,
    limit: Option<usize>,
    target_partitions: usize,
) -> PyResult<annotate::StreamingAnnotator> {
    annotate::create_streaming_annotator(
        py,
        vcf_path,
        cache_dir,
        options_json,
        skip_csq,
        limit,
        target_partitions,
    )
}
```

- [ ] **Step 7: Update native stubs**

In `src/vepyr/_core.pyi`, change `annotate_vcf`:

```python
def annotate_vcf(
    vcf_path: str,
    cache_dir: str,
    output_path: str,
    options_json: str,
    show_progress: bool = True,
    compression: str = "",
    on_batch_written: Callable[[int, int, int], None] | None = None,
    target_partitions: int = 1,
) -> int:
```

Change `create_annotator`:

```python
def create_annotator(
    vcf_path: str,
    cache_dir: str,
    options_json: str,
    skip_csq: bool = True,
    limit: int | None = None,
    target_partitions: int = 1,
) -> StreamingAnnotator:
```

- [ ] **Step 8: Run native verification**

Run:

```bash
cargo check
```

Expected: build passes with `target_partitions` present in the `AnnotateVcfConfig` struct literal.

- [ ] **Step 9: Commit boundary**

Run:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/annotate.rs src/vepyr/_core.pyi
git commit -m "feat: expose fjall annotation target partitions"
```

---

### Task 4: Add A Small Fjall Partition-Invariance Test

**Files:**
- Modify: `tests/test_build_cache.py`

- [ ] **Step 1: Add helpers to compare VCF data lines**

Add near the top of `tests/test_build_cache.py`:

```python
def read_vcf_data_lines(path: Path) -> list[str]:
    with open(path) as handle:
        return [line for line in handle if not line.startswith("#")]
```

- [ ] **Step 2: Add the built-cache annotation test**

Add this test inside `TestBuildCacheIntegration` after the fjall store tests:

```python
def test_fjall_annotation_target_partitions_preserves_vcf_output(
    self, built_cache, tmp_path
):
    out, _, _, _ = built_cache
    input_vcf = ENSEMBL_CACHE_DIR / "sample.vcf"
    serial_vcf = tmp_path / "serial.vcf"
    parallel_vcf = tmp_path / "parallel.vcf"

    vepyr.annotate(
        str(input_vcf),
        out,
        check_existing=True,
        use_fjall=True,
        output_vcf=str(serial_vcf),
        show_progress=False,
        target_partitions=1,
    )
    vepyr.annotate(
        str(input_vcf),
        out,
        check_existing=True,
        use_fjall=True,
        output_vcf=str(parallel_vcf),
        show_progress=False,
        target_partitions=2,
    )

    assert read_vcf_data_lines(parallel_vcf) == read_vcf_data_lines(serial_vcf)
```

- [ ] **Step 3: Run the focused integration test**

Run:

```bash
uv run maturin develop
uv run pytest tests/test_build_cache.py::TestBuildCacheIntegration::test_fjall_annotation_target_partitions_preserves_vcf_output -q
```

Expected: test passes and confirms vepyr's Python API produces identical VCF data lines for fjall target partitions 1 and 2 on the small fixture cache.

---

### Task 5: Add E2E Benchmark CLI Controls

**Files:**
- Modify: `e2e-testing/scripts/run_annotation_fast.py`
- Modify: `e2e-testing/scripts/run_annotation_fast_all.py`
- Modify: `tests/test_run_annotation_fast.py`

- [ ] **Step 1: Add `--target-partitions` to `run_annotation_fast.py`**

In `parse_args()`, add after `--backend`:

```python
p.add_argument(
    "--target-partitions",
    type=int,
    default=1,
    help="Fjall annotation target partitions (default: %(default)s)",
)
```

After `args = p.parse_args()`, add:

```python
if args.target_partitions <= 0:
    p.error("--target-partitions must be a positive integer")
if args.target_partitions > 1 and args.backend != "fjall":
    p.error("--target-partitions > 1 requires --backend fjall")
```

- [ ] **Step 2: Pass the CLI value to `vepyr.annotate`**

Change the annotation call in `run_annotation_fast.py`:

```python
vepyr.annotate(
    chrom_vcf_gz,
    args.cache_dir,
    everything=True,
    reference_fasta=args.fasta,
    use_fjall=(args.backend == "fjall"),
    output_vcf=output_vcf,
    target_partitions=args.target_partitions,
    **args.annotate_kwargs,
)
```

- [ ] **Step 3: Add `--target-partitions` to `run_annotation_fast_all.py`**

Add the parser argument:

```python
p.add_argument(
    "--target-partitions",
    type=int,
    default=1,
    help="Fjall annotation target partitions forwarded to run_annotation_fast.py (default: %(default)s)",
)
```

Validate it after parsing:

```python
args = p.parse_args()
if args.target_partitions <= 0:
    p.error("--target-partitions must be a positive integer")
if args.target_partitions > 1 and args.backend != "fjall":
    p.error("--target-partitions > 1 requires --backend fjall")
return args
```

- [ ] **Step 4: Forward the value through `run_chromosome`**

Change the function signature:

```python
def run_chromosome(
    chrom_num, cache="ensembl", backend="fjall", target_partitions=1, force=False
):
```

Append to the command:

```python
"--target-partitions",
str(target_partitions),
```

Change the caller:

```python
ok = run_chromosome(
    n,
    cache=cache,
    backend=backend,
    target_partitions=args.target_partitions,
    force=not args.no_force,
)
```

- [ ] **Step 5: Add CLI parser tests**

In `tests/test_run_annotation_fast.py`, add:

```python
def test_parse_args_accepts_target_partitions(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        ["run_annotation_fast.py", "chr1", "--backend", "fjall", "--target-partitions", "4"],
    )

    args = module.parse_args()

    assert args.target_partitions == 4

def test_parse_args_rejects_parallel_parquet(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        ["run_annotation_fast.py", "chr1", "--backend", "parquet", "--target-partitions", "2"],
    )

    with pytest.raises(SystemExit):
        module.parse_args()
```

Also add `import pytest` at the top of the file.

- [ ] **Step 6: Run script tests**

Run:

```bash
uv run pytest tests/test_run_annotation_fast.py -q
```

Expected: tests pass.

---

### Task 6: Update User-Facing Documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/quickstart.md`
- Modify: `docs/performance.md`
- Modify: `e2e-testing/README.md`

- [ ] **Step 1: Document Python usage**

Add this example near the fjall backend example in `README.md` and `docs/quickstart.md`:

```python
df = vepyr.annotate(
    "input.vcf.gz",
    "/data/vep/parquet/115_GRCh38_ensembl",
    use_fjall=True,
    target_partitions=4,
).collect()
```

Add this sentence:

```text
`target_partitions` controls fjall lookup workers during annotation. Values greater than 1 require `use_fjall=True`; parquet annotation remains single-partition to preserve correctness.
```

- [ ] **Step 2: Update performance tuning table**

In `docs/performance.md`, add:

```markdown
| `target_partitions` | `1` | Fjall annotation lookup parallelism. Use with `use_fjall=True`; parquet annotation ignores this path and is guarded to 1. |
```

- [ ] **Step 3: Document e2e benchmark usage**

In `e2e-testing/README.md`, add:

```bash
uv run python run_annotation_fast.py chr22 --backend fjall --target-partitions 4
uv run python run_annotation_fast_all.py --backend fjall --target-partitions 4
```

- [ ] **Step 4: Run doc-adjacent checks**

Run:

```bash
uv run pytest tests/test_run_annotation_fast.py -q
```

Expected: script docs and parser behavior remain aligned.

---

### Task 7: Final Verification

**Files:**
- Verify all touched files.

- [ ] **Step 1: Format Rust**

Run:

```bash
cargo fmt
```

Expected: no formatting errors.

- [ ] **Step 2: Format Python**

Run:

```bash
uv run ruff format src/vepyr tests e2e-testing/scripts
```

Expected: files formatted.

- [ ] **Step 3: Build native extension**

Run:

```bash
uv run maturin develop
```

Expected: extension builds against `datafusion-bio-function-vep` rev `3d6befd2817b43dd16bedc05e2f34eaeff692e16`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_annotate.py tests/test_build_cache.py::TestBuildCacheIntegration::test_fjall_annotation_target_partitions_preserves_vcf_output tests/test_run_annotation_fast.py -q
```

Expected: tests pass.

- [ ] **Step 5: Run Rust checks**

Run:

```bash
cargo check
```

Expected: check passes.

- [ ] **Step 6: Run optional full Python suite**

Run:

```bash
uv run pytest -q
```

Expected: full test suite passes or only skips tests for unavailable external genomic fixtures.

- [ ] **Step 7: Run optional real-data benchmark smoke test**

Run from `e2e-testing/scripts` when the external data paths in `e2e-testing/README.md` are available:

```bash
uv run python run_annotation_fast.py chr22 --backend fjall --target-partitions 4 --force
```

Expected: generated VCF has the same comparison result as `--target-partitions 1`, with improved annotation throughput on multi-core machines.

---

## Self-Review

- Spec coverage: covers dependency adoption, Python API, native bridge, fjall-only safety guard, tests, e2e scripts, docs, and verification.
- Placeholder scan: no placeholder tokens or unspecified file paths remain.
- Type consistency: public `target_partitions` is `int` in Python and `usize` in Rust; it is passed separately from `options_json`.
- Risk callout: parquet/non-fjall paths reject `target_partitions > 1` at the Python layer and clamp to 1 at the Rust layer.
