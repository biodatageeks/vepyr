# Plugin Factory — Plan B: Toolkit (`vepyr` becomes the thing the catalogue depends on)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Give `vepyr-plugins` (Plan C) everything it needs to test a plugin manifest with nothing but `pip install vepyr` and a downloaded mini-cache — no Rust toolchain, no 34 GB cache, no Perl.

**Architecture:** Three deliverables in the `vepyr` repo. (1) `vepyr.build_plugin_cache(...)` — a PyO3 binding over the engine's `PluginCacheBuilder`, which the prior handoff described as "already shipped" and which **does not exist**. (2) `vepyr.parity` — the CSQ comparator, lifted out of a 809-line benchmark script into an importable module so the plugin gate and the WGS e2e suite share one implementation. (3) A **built** (not sliced) region mini-cache, published as a release asset.

**Tech Stack:** Rust + PyO3 0.28 (abi3-py310), Python, DataFusion.

**Source spec:** `datafusion-bio-functions/docs/superpowers/specs/2026-07-13-vep-plugin-port-factory-design.md` (§4, §5, §6). Plan A (the engine half) is merged: PRs #194, #195 → `master-sitekwb`.

---

## Two discoveries that change the spec, found while preparing this plan

**1. The mini-cache must be BUILT, not sliced.** The spec (§5) says: slice every table of `_cache_v115` to a region. That cannot work. The local variation cache has **no `tier` column** — verified directly, its 76 columns are `chrom, start, end, variation_name, allele_string, …` and `tier` is not among them — while `plugin_cache::join` does:

```sql
SELECT chrom, start, allele_string, tier FROM <variation shard>
```

So a plugin build against a slice of `_cache_v115` fails on a missing column. That cache predates the tiering work. The fix is not to weaken the engine (tiering is a real warm/cold code path the parity gate should exercise) but to **rebuild the region's variation shard with the current builder** — which already exists as `examples/build_parquet_variation_chrom` and reads the native Ensembl cache we have on disk.

**2. `vepyr` pins the engine to `v0.13.1`, which has no `plugin_cache` at all.** (`Cargo.toml:46`. `plugin_cache` landed in `v0.14.0`.) Nothing in this plan compiles until that pin moves. Task 1.

---

## Branch policy

Same convention as the engine repo: cut **`master-sitekwb`** from `vepyr`'s `master` and treat it as `main`. Feature branches off it, PRs into it. **Never commit to `master`/`main`.**

```bash
cd /Users/wojtek/Documents/vepyr/vepyr
git fetch origin && git checkout -b master-sitekwb origin/master && git push -u origin master-sitekwb
git worktree add ../vepyr-worktrees/toolkit -b feat/plugin-cache-toolkit master-sitekwb
```

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `Cargo.toml` | Deps | Modify: bump `datafusion-bio-function-vep` past `plugin_cache`; add the `parquet-cache` feature |
| `src/plugin_cache.rs` | **New.** The `build_plugin_cache` PyO3 binding | Create |
| `src/lib.rs` | pymodule registration (`lib.rs:146-152`) | Modify: register the new function |
| `src/vepyr/_core.pyi` | Type stubs | Modify: signature for `build_plugin_cache` |
| `src/vepyr/parity.py` | **New.** The CSQ comparator, importable | Create (lifted from `e2e-testing/scripts/run_annotation_fast.py:384` `compare_vcfs`) |
| `scripts/build_mini_cache.py` | **New.** Builds the region mini-cache fixture | Create |
| `tests/test_plugin_cache.py` | Python-side tests | Create |
| `tests/test_parity.py` | Comparator tests | Create |

---

### Task 1: Move the engine pin so `plugin_cache` exists at all

**Files:** `Cargo.toml`

- [ ] **Step 1: Confirm the gap**

```bash
cd /Users/wojtek/Documents/vepyr/vepyr
grep -n "datafusion-bio-function-vep" Cargo.toml     # expect: tag = "v0.13.1"
```
`plugin_cache` is not in `v0.13.1` — it landed in `v0.14.0`, and the hardening (Plan A) is on `master-sitekwb` above that.

- [ ] **Step 2: Repoint at the merged Plan A commit**

In `Cargo.toml`, replace the `tag = "v0.13.1"` pin on `datafusion-bio-function-vep` with a **rev** pin at the Plan A merge commit, and add the feature that gates the plugin-cache builder:

```toml
datafusion-bio-function-vep = { git = "https://github.com/biodatageeks/datafusion-bio-functions.git", rev = "f7b9e66", features = ["cache-builder", "parquet-cache"] }
```

(Rev, not branch: a moving branch pin makes builds unreproducible. When the engine cuts a release containing Plan A, move to that tag.)

- [ ] **Step 3: Build and run the existing suite — nothing may regress**

```bash
cargo build --release
uv run pytest tests/ -x -q
```
Expected: builds; the existing tests pass unchanged. If the engine bump breaks an existing call site, that is a real incompatibility — **report it, do not paper over it.**

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: pin the engine at the plugin_cache merge (v0.13.1 predates the subsystem)"
```

---

### Task 2: `vepyr.build_plugin_cache` — the function the handoff claimed already existed

The engine API is `PluginCacheBuilder::new(&manifest, manifest_file, variation_cache_dir, out)` → `.with_chrom_filter([..])` → `.with_overwrite(bool)` → `.build_all()`, and the manifest comes from `SourceManifest::load(path)` (which now also `validate()`s). Mirror that.

**Files:** `src/plugin_cache.rs` (new), `src/lib.rs`, `src/vepyr/_core.pyi`, `tests/test_plugin_cache.py` (new)

- [ ] **Step 1: Write the failing Python test**

`tests/test_plugin_cache.py`:

```python
"""The plugin-cache builder binding: the surface vepyr-plugins' CI depends on."""

from pathlib import Path

import pytest

import vepyr


def test_build_plugin_cache_rejects_an_invalid_manifest(tmp_path: Path) -> None:
    """A manifest that fails engine validation must raise, not build a broken cache."""
    manifest = tmp_path / "bad.source.toml"
    manifest.write_text(
        '# no [[source]] and no [[value_columns]] -> structurally useless\n'
        'plugin_name = "bad"\n'
        'coordinate_system = "1-based"\n'
        'ingest_sql = "SELECT 1"\n'
    )
    with pytest.raises(Exception) as exc:
        vepyr.build_plugin_cache(
            manifest=str(manifest),
            variation_cache_dir=str(tmp_path),
            out=str(tmp_path / "out"),
        )
    # The engine's validate() names the plugin and the missing block.
    assert "bad" in str(exc.value)
```

- [ ] **Step 2: Run it and watch it fail**

```bash
uv run pytest tests/test_plugin_cache.py -x -q
```
Expected: `AttributeError: module 'vepyr' has no attribute 'build_plugin_cache'`.

- [ ] **Step 3: Write the binding**

`src/plugin_cache.rs`:

```rust
//! PyO3 binding over the engine's plugin-cache builder.
//!
//! `vepyr-plugins`' CI builds a plugin's cache from a TOML source manifest with
//! nothing but `pip install vepyr` — no Rust toolchain. This is that entry point.

use std::path::{Path, PathBuf};

use datafusion_bio_function_vep::plugin_cache::builder::PluginCacheBuilder;
use datafusion_bio_function_vep::plugin_cache::source_manifest::SourceManifest;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Build a plugin cache from a declarative source manifest.
///
/// `manifest`             path to `<plugin>.source.toml`
/// `variation_cache_dir`  cache root containing `variation/<chrom>.parquet`
/// `out`                  output root; writes `plugin/<name>/`
/// `source_path`          override the manifest's source path (single-source manifests)
/// `chroms`               restrict the build (empty/None = every chrom in the cache)
/// `overwrite`            clean rebuild instead of an UPSERT into the prior manifest
///
/// Returns the built chromosomes as `[{chrom, rows, warm, cold}, ...]`.
#[pyfunction]
#[pyo3(signature = (manifest, variation_cache_dir, out, source_path=None, chroms=None, overwrite=false))]
pub fn build_plugin_cache(
    py: Python<'_>,
    manifest: &str,
    variation_cache_dir: &str,
    out: &str,
    source_path: Option<&str>,
    chroms: Option<Vec<String>>,
    overwrite: bool,
) -> PyResult<Vec<PluginChrom>> {
    let manifest_path = PathBuf::from(manifest);
    // SourceManifest::load() also runs validate(), so a self-contradictory manifest
    // (vcf source declaring 0-based, a [source.csv] block under provider = "vcf",
    // no sources, no value columns, ...) raises here instead of building a cache
    // that silently annotates nothing.
    let mut src = SourceManifest::load(&manifest_path).map_err(to_py_err)?;

    if let Some(p) = source_path {
        if src.sources.len() != 1 {
            return Err(PyRuntimeError::new_err(format!(
                "source_path= is ambiguous: manifest '{}' declares {} sources",
                src.plugin_name,
                src.sources.len()
            )));
        }
        src.sources[0].path = p.to_string();
    }

    let manifest_file = file_name_of(&manifest_path);
    let chroms = chroms.unwrap_or_default();

    // The builder is async; release the GIL while it runs.
    py.detach(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        rt.block_on(async {
            let mut b = PluginCacheBuilder::new(
                &src,
                &manifest_file,
                Path::new(variation_cache_dir),
                Path::new(out),
            );
            if !chroms.is_empty() {
                b = b.with_chrom_filter(chroms.clone());
            }
            b = b.with_overwrite(overwrite);
            let cache = b.build_all().await.map_err(to_py_err)?;
            Ok(cache
                .chroms
                .iter()
                .map(|c| PluginChrom {
                    chrom: c.chrom.clone(),
                    rows: c.rows,
                    warm: c.warm,
                    cold: c.cold,
                })
                .collect())
        })
    })
}

/// One built chromosome. `warm == 0` with `rows > 0` means NOT ONE row joined the
/// variation cache — the manifest is almost certainly wrong (contig naming,
/// coordinate system, or allele-string format). The engine also logs a warning.
#[pyclass(get_all)]
#[derive(Clone)]
pub struct PluginChrom {
    pub chrom: String,
    pub rows: usize,
    pub warm: usize,
    pub cold: usize,
}

#[pymethods]
impl PluginChrom {
    fn __repr__(&self) -> String {
        format!(
            "PluginChrom(chrom='{}', rows={}, warm={}, cold={})",
            self.chrom, self.rows, self.warm, self.cold
        )
    }
}

fn to_py_err(e: datafusion::common::DataFusionError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn file_name_of(p: &Path) -> String {
    p.file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| p.to_string_lossy().into_owned())
}
```

> The exact types of `ChromEntry`'s count fields (`rows`/`warm`/`cold`) must be read from
> `plugin_cache/cache_manifest.rs` — if they are `u64` rather than `usize`, use that. Do not guess;
> a wrong integer type is a compile error, so the compiler will tell you.

In `src/lib.rs`, add the module and register the function beside the existing three
(`lib.rs:146-152`):

```rust
mod plugin_cache;
// ... inside the #[pymodule] fn:
    m.add_function(wrap_pyfunction!(plugin_cache::build_plugin_cache, m)?)?;
    m.add_class::<plugin_cache::PluginChrom>()?;
```

- [ ] **Step 4: Add the type stub**

In `src/vepyr/_core.pyi`, matching the style of the existing entries:

```python
class PluginChrom:
    chrom: str
    rows: int
    warm: int
    cold: int

def build_plugin_cache(
    manifest: str,
    variation_cache_dir: str,
    out: str,
    source_path: str | None = None,
    chroms: list[str] | None = None,
    overwrite: bool = False,
) -> list[PluginChrom]: ...
```

Re-export it from `src/vepyr/__init__.py` the way the existing functions are re-exported.

- [ ] **Step 5: Build and run green**

```bash
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
uv run pytest tests/test_plugin_cache.py -x -q
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/plugin_cache.rs src/lib.rs src/vepyr/_core.pyi src/vepyr/__init__.py tests/test_plugin_cache.py
git commit -m "feat: vepyr.build_plugin_cache — the binding the handoff assumed existed"
```

---

### Task 3: `vepyr.parity` — one CSQ comparator, two consumers

`e2e-testing/scripts/run_annotation_fast.py:384` defines `compare_vcfs`, an 809-line script's field-by-field CSQ comparator. It already handles the hard parts: merge-join on the variant key, CSQ entry-count and entry-**order** semantics (with an explicit rationale comment at `:374-380`), per-field mismatch counts with examples. Plan C needs exactly this, restricted to a plugin's fields. Copying it would fork it.

**Files:** `src/vepyr/parity.py` (new), `tests/test_parity.py` (new), `e2e-testing/scripts/run_annotation_fast.py` (modify: import instead of define)

- [ ] **Step 1: Read the original first**

Read `e2e-testing/scripts/run_annotation_fast.py:384-620` in full before touching anything. The extraction must be **behaviour-preserving** — this function is the arbiter of a 4-million-variant WGS benchmark, and quietly changing its semantics would invalidate results nobody would re-check.

- [ ] **Step 2: Write the failing test**

`tests/test_parity.py` — cover what Plan C actually depends on:

```python
"""The CSQ comparator: the arbiter of every plugin's parity gate."""

from vepyr.parity import compare_csq_fields


def test_identical_files_agree() -> None:
    ...

def test_a_differing_plugin_field_is_reported_with_its_value() -> None:
    """A mismatch must name the field, the key, and BOTH values — a bare count is useless."""
    ...

def test_over_emission_is_a_mismatch() -> None:
    """vepyr populating a field VEP left empty is a failure, not a bonus.
    (This is one of the two bugs the manual AlphaMissense e2e caught that unit tests missed.)"""
    ...

def test_fields_outside_the_requested_set_are_ignored() -> None:
    """The plugin gate compares ONLY the plugin's CSQ fields; core-field drift is
    a separate verdict (see the blame-attribution rule in the spec, §6.1)."""
    ...
```

Write these as real tests over small in-memory/temp VCF pairs. Do not stub them.

- [ ] **Step 3: Extract**

Move `compare_vcfs` (and only the helpers it needs) into `src/vepyr/parity.py`, exposing:

```python
def compare_csq_fields(
    truth_vcf: str | Path,
    test_vcf: str | Path,
    fields: Sequence[str] | None = None,   # None = every shared CSQ field
) -> ComparisonResult: ...
```

`ComparisonResult` is a dataclass carrying at least: total keys compared, per-field mismatch counts, per-field examples (key + both values), entry-count mismatches, and over-emission counts. Keep the entry-order semantics **exactly** as the original, comment and all — that comment is the record of a real decision.

Then have `e2e-testing/scripts/run_annotation_fast.py` **import** it rather than define it.

- [ ] **Step 4: Prove the extraction is behaviour-preserving**

This is the step that matters. Run the WGS e2e comparator before and after the extraction on the same inputs and diff the reports:

```bash
# whatever inputs the script's README specifies; a single chromosome is enough
uv run python e2e-testing/scripts/run_annotation_fast.py --chrom chr22 ... > /tmp/after.txt
git stash && uv run python e2e-testing/scripts/run_annotation_fast.py --chrom chr22 ... > /tmp/before.txt && git stash pop
diff /tmp/before.txt /tmp/after.txt    # must be empty
```
If the inputs are unavailable locally, say so explicitly and fall back to a golden-file test over a committed VCF pair — but **do not skip the equivalence check silently.**

- [ ] **Step 5: Commit**

```bash
git add src/vepyr/parity.py tests/test_parity.py e2e-testing/scripts/run_annotation_fast.py
git commit -m "refactor: extract the CSQ comparator into vepyr.parity (one implementation, two consumers)"
```

---

### Task 4: The mini-cache — built, not sliced

The parity gate needs a cache small enough for CI. The full one is 34 GB, and (see discoveries above) the local one has **no `tier` column**, so slicing it is not an option.

Region: **`chr22:22,000,000–23,500,000`** — chosen because it contains `chr22:22,893,742 C>G`, the locus on which AlphaMissense parity was manually confirmed, so Plan C's regression test is meaningful.

**Files:** `scripts/build_mini_cache.py` (new)

- [ ] **Step 1: Rebuild chr22's variation shard with the CURRENT builder (this is what supplies `tier`)**

The engine ships the tool already:

```bash
cargo run --release -p datafusion-bio-function-vep \
  --features lance-cache,cache-builder \
  --example build_parquet_variation_chrom -- \
  --cache-root /Users/wojtek/Documents/vepyr/_cache_v115/homo_sapiens/<release>_GRCh38 \
  --output-dir /tmp/mini_cache_full \
  --chrom chr22 --cache-source-type <ensembl|merged|refseq> --partitions 8 --overwrite
```

Resolve `<release>` and the source type from what is actually on disk under `_cache_v115/homo_sapiens/` — read it, do not assume. **Verify the output has the column that started all this:**

```bash
python -c "import pyarrow.parquet as pq; n=pq.ParquetFile('/tmp/mini_cache_full/variation/chr22.parquet').schema_arrow.names; print('tier' in n, len(n))"
```
Expected: `True`. If it is `False`, STOP — the whole mini-cache design rests on this.

- [ ] **Step 2: Build the other tables for chr22**

`build_parquet_context_chrom` and `build_parquet_translation_sift_chrom` (same `examples/` dir) cover the remaining tables the annotator needs. Read each example's `--help`/header for its flags. Produce a chr22-only cache root.

- [ ] **Step 3: Slice every table to the region, and cut the FASTA window**

`scripts/build_mini_cache.py`: filter each parquet table to `22_000_000 <= start <= 23_500_000` (respect each table's own coordinate column — read the schema, do not assume they all use `start`), and cut the matching window out of `Homo_sapiens.GRCh38.dna.primary_assembly.fa`. Emit a cache root with the same layout as the full one, plus a `chrom_manifest.json` the runtime accepts.

- [ ] **Step 4: The fixture's own test — the one the spec's §12 risk section demands**

A mini-cache that silently drops a transcript would produce phantom "core drift" and poison the blame-attribution rule the whole gate depends on. So:

```
annotate the region's VCF with the FULL cache  -> output A
annotate the same VCF with the MINI cache      -> output B
require A == B (body-identical)
```
Anything less is trusting the slicer. Report the real diff.

- [ ] **Step 5: Report size and publish**

```bash
du -sh /tmp/mini_cache_region
```
Target: tens of MB. Publish as a GitHub release asset on `vepyr` (**ask the user before publishing** — it is an outward-facing artifact), and record the URL + checksum in `scripts/build_mini_cache.py`'s header so Plan C's CI can fetch it.

- [ ] **Step 6: Commit**

```bash
git add scripts/build_mini_cache.py
git commit -m "feat: build the region mini-cache fixture (built, not sliced — the old cache has no tier column)"
```

---

### Task 5: The end-to-end proof — everything Plan C will do, done once by hand

- [ ] **Step 1: Build the real AlphaMissense plugin cache against the mini-cache, through the Python API**

```python
import vepyr
chroms = vepyr.build_plugin_cache(
    manifest="/Users/wojtek/Documents/vepyr/vepyr-plugins/plugins/alphamissense/alphamissense.source.toml",
    source_path="<a chr22-region slice of AlphaMissense_hg38.tsv.gz>",
    variation_cache_dir="<mini-cache root>",
    out="/tmp/plugin_cache",
    chroms=["chr22"],
)
print(chroms)
```

- [ ] **Step 2: Read the numbers, and do not accept a healthy-looking lie**

`warm == 0` with `rows > 0` means **not one row joined the variation cache** — the build "succeeded" and the plugin will annotate nothing. Plan A added a warning for exactly this. If you see it, the mini-cache or the source slice is wrong; fix it rather than proceeding.

- [ ] **Step 3: Annotate with the plugin cache and confirm the CSQ fields appear**

Use the existing `annotate_vcf` binding with `plugin_cache_root` pointed at the output, over the region's VCF, and confirm `am_class` / `am_pathogenicity` are populated on missense lines and empty elsewhere.

- [ ] **Step 4: Commit nothing; report**

This task produces no code — it produces the evidence that Plan C is buildable. Report the actual numbers.

---

## Definition of done

`pip install vepyr` + the mini-cache release asset is enough to build a plugin cache from a manifest and diff two VCFs field-by-field — with no Rust toolchain, no 34 GB cache, and no Perl. That is precisely the surface Plan C's CI stands on.

## Explicitly NOT in this plan

The parity harness itself, `parity.toml`, the golden VEP outputs, the CI workflow, and the three clients (AlphaMissense / REVEL / a VCF plugin) — those are **Plan C**, in the `vepyr-plugins` repo. This plan only builds what Plan C imports.
