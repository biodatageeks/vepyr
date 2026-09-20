# `sink_vcf` Writer Fixes (formats + polars-bio) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pb.sink_vcf` writes a spec-valid VCF from a frame that carries renamed
input columns (`INFO_AF`) next to same-named annotation columns (`AF`): GT first in
FORMAT, `INFO_<id>` written as `<id>=`, and `##FILTER`/`##ALT`/`##fileformat` kept.

**Architecture:** The FORMAT key order is decided in the formats serializer, so GT-first
is fixed there for every caller. Everything else is polars-bio's write path, which
already owns the mapping from frame columns to VCF fields (`fmt_<id>` is handled there
today): it projects the DataFrame down to VCF columns, aliasing `INFO_<id>` back to
`<id>`, before the formats writer sees a schema. No formats API changes.

**Tech Stack:** Rust (arrow 58, DataFusion 53), PyO3, pytest; repos
`datafusion-bio-formats` (crate `datafusion-bio-format-vcf`) and `polars-bio`.

**Spec:** `vepyr/docs/superpowers/specs/2026-09-20-lazyframe-vcf-columns-and-sink-vcf-design.md` §3.4
(this plan corrects it: the id mapping lives in polars-bio, not formats — see Task 2's
rationale; and formats already emits FILTER/ALT/contig lines from schema metadata,
`header_builder.rs:93-133`, so that gap is polars-bio's too).

## Global Constraints

- Both working trees are **dirty/drifted** (`datafusion-bio-formats` on `feat/cooler`;
  `polars-bio` has uncommitted `Cargo.lock`, `CHANGELOG.md`, `.cargo/config.toml`).
  Work in fresh worktrees from each repo's default branch; never touch those trees.
- Name rule, verbatim from the spec: a column named `INFO_<id>` is written under `<id>`
  when `<id>` is declared in `info_fields` — and then a bare column `<id>` is **not** an
  INFO field. Scoped to declared ids, so a real field called `INFO_x` is left alone.
- No change to output for frames without an `INFO_`-prefixed column.
- polars-bio pins formats by `rev =` (`Cargo.toml:53`); all five `datafusion-bio-format-*`
  entries move to the same rev together.
- No merging; PRs are opened as drafts for human review.

---

### Task 1: formats — GT first in FORMAT when the key order is not carried

**Files:**
- Modify: `datafusion/bio-format-vcf/src/serializer.rs` (`build_format_and_samples`, ~`:1105-1140`)
- Test: same file, `mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: `batch_to_vcf_lines` emits `GT` as the first FORMAT key whenever `GT` is among
  `format_fields` and no carried `_vcf_format_keys` value dictates the order. Signature unchanged.

- [ ] **Step 1: Worktree**

```bash
git -C ~/research/git/datafusion-bio-formats fetch origin
git -C ~/research/git/datafusion-bio-formats worktree add ~/research/git/_wt/formats-gt-first -b fix/vcf-format-gt-first origin/master
cd ~/research/git/_wt/formats-gt-first
```

- [ ] **Step 2: Write the failing test** (append inside `mod tests` in `serializer.rs`)

```rust
#[test]
fn gt_is_written_first_whatever_order_the_caller_lists() {
    // VCF 4.x: "the first sub-field must always be the genotype (GT) if it is present".
    let mut gt_meta = HashMap::new();
    gt_meta.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT".to_string());
    let schema = Arc::new(Schema::new(vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
        Field::new("DP", DataType::Int32, true),
        Field::new("GT", DataType::Utf8, true).with_metadata(gt_meta),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![Some(".")])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["G"])),
            Arc::new(Float64Array::from(vec![Some(50.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            Arc::new(Int32Array::from(vec![Some(25)])),
            Arc::new(StringArray::from(vec![Some("0/1")])),
        ],
    )
    .unwrap();

    // Header order puts DP before GT, as the GIAB HG002 header does.
    let lines = batch_to_vcf_lines(
        &batch,
        &[],
        &["DP".to_string(), "GT".to_string()],
        &["S1".to_string()],
        false,
    )
    .unwrap();
    let line = lines[0].to_string();
    assert!(line.contains("\tGT:DP\t0/1:25"), "got: {line}");
}
```

If `VcfRecordLine` has no `to_string()`, use whatever the neighbouring
`test_batch_to_vcf_lines_single_sample` (`:1757`) uses to get the text; copy its
imports for the array types.

- [ ] **Step 3: Run it, expect failure**

Run: `cargo test -p datafusion-bio-format-vcf gt_is_written_first -- --nocapture`
Expected: FAIL, line contains `DP:GT\t25:0/1`.

- [ ] **Step 4: Implement** — in `build_format_and_samples`, replace the `None` arm

```rust
        None => (format_fields.iter().map(String::as_str).collect(), 0),
```

with

```rust
        // No carried order: the caller's list is header order, which need not
        // start with GT. The specification requires GT first when present.
        None => {
            let mut selected: Vec<&str> = format_fields.iter().map(String::as_str).collect();
            if let Some(pos) = selected.iter().position(|key| *key == "GT") {
                selected[..=pos].rotate_right(1);
            }
            (selected, 0)
        }
```

The carried arm is left alone on purpose: a carried list reproduces the source record,
and a source that put GT elsewhere is reproduced as written.

- [ ] **Step 5: Run the crate's tests**

Run: `cargo test -p datafusion-bio-format-vcf`
Expected: PASS. If an existing test asserted a non-GT-first order, it encoded the bug:
update its expectation and say so in the commit body.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy -p datafusion-bio-format-vcf --all-targets -- -D warnings
git add datafusion/bio-format-vcf/src/serializer.rs
git commit -m "fix(vcf): write GT first in FORMAT when the key order is not carried"
```

- [ ] **Step 7: Draft PR**

```bash
git push -u origin fix/vcf-format-gt-first
gh pr create --draft --repo biodatageeks/datafusion-bio-formats --head fix/vcf-format-gt-first \
  --title "fix(vcf): write GT first in FORMAT when the key order is not carried" \
  --body "VCF 4.x requires GT to be the first FORMAT key when present. The serializer emitted the caller's order, which is header order for every caller that does not carry per-record keys (polars-bio sink_vcf; the VEP sink with preserve_record_layout off). Reproduces on a plain pb.scan_vcf -> pb.sink_vcf round trip of GIAB HG002 (DP:GQ:ADALL:AD:GT). Carried key lists are left as written."
```

Record the PR head sha: `git rev-parse HEAD` → used as `FORMATS_REV` in Task 4.

---

### Task 2: polars-bio — project to VCF columns and write `INFO_<id>` as `<id>`

**Why here and not in formats:** polars-bio classifies INFO columns with
`info_meta.get(name)` (`src/write.rs:380`). On a vepyr frame that holds both `INFO_AF`
(the input's field) and `AF` (VEP's frequency), that test picks the **annotation**
column and would write VEP's value under the input's key. The classification is wrong
before the formats writer is reached, so the fix belongs where the classification is.

**Files:**
- Modify: `src/write.rs` (`execute_vcf_streaming_write` ~`:548`, new helper above it)
- Test: `tests/test_vcf_write_info_prefix.py` (new)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `fn vcf_write_projection(schema: &Schema, info_ids: &HashSet<String>) -> Option<Vec<(String, String)>>`
  — `(source column, output name)` pairs, or `None` when no `INFO_`-prefixed declared id
  exists (then the write path is byte-for-byte today's).

- [ ] **Step 1: Worktree**

```bash
git -C ~/research/git/polars-bio fetch origin
git -C ~/research/git/polars-bio worktree add ~/research/git/_wt/pb-sink-vcf-info-prefix -b fix/sink-vcf-info-prefix origin/master
cd ~/research/git/_wt/pb-sink-vcf-info-prefix && uv sync
```

- [ ] **Step 2: Write the failing test** — `tests/test_vcf_write_info_prefix.py`

```python
"""sink_vcf on a frame whose input INFO column was renamed to INFO_<id>."""

import polars as pl
import polars_bio as pb
from polars_bio._metadata import set_coordinate_system

HEADER = {
    "info_fields": {
        "AF": {"number": "A", "type": "Float", "description": "Cohort allele frequency"},
        "DP": {"number": "1", "type": "Integer", "description": "Depth"},
    },
    "format_fields": {},
    "sample_names": [],
}


def _frame() -> pl.LazyFrame:
    lf = pl.LazyFrame(
        {
            "chrom": ["chr1", "chr1"],
            "start": pl.Series([100, 200], dtype=pl.UInt32),
            "end": pl.Series([100, 200], dtype=pl.UInt32),
            "id": [".", "."],
            "ref": ["A", "C"],
            "alt": ["G", "T"],
            "qual": [50.0, 60.0],
            "filter": ["PASS", "PASS"],
            "INFO_AF": pl.Series([[0.25], [0.5]], dtype=pl.List(pl.Float32)),
            "DP": pl.Series([10, 20], dtype=pl.Int32),
            # an annotation column that shares the input field's id
            "AF": pl.Series([0.001, 0.002], dtype=pl.Float32),
        }
    )
    pb.set_source_metadata(lf, format="vcf", path="", header=HEADER)
    set_coordinate_system(lf, zero_based=False)
    return lf


def _read(path):
    text = path.read_text().splitlines()
    return [l for l in text if l.startswith("##")], [l for l in text if not l.startswith("#")]


def test_prefixed_input_column_is_written_under_its_vcf_id(tmp_path):
    out = tmp_path / "out.vcf"
    pb.sink_vcf(_frame(), str(out))
    header, records = _read(out)

    assert sum(l.startswith("##INFO=<ID=AF,") for l in header) == 1
    assert not any("INFO_AF" in l for l in header + records)
    assert [r.split("\t")[7] for r in records] == ["AF=0.25;DP=10", "AF=0.5;DP=20"]


def test_a_real_field_named_with_the_prefix_is_left_alone(tmp_path):
    header = {
        "info_fields": {
            "INFO_X": {"number": "1", "type": "Integer", "description": "really named so"}
        },
        "format_fields": {},
        "sample_names": [],
    }
    lf = _frame().select("chrom", "start", "end", "id", "ref", "alt", "qual", "filter").with_columns(
        pl.Series("INFO_X", [1, 2], dtype=pl.Int32)
    )
    pb.set_source_metadata(lf, format="vcf", path="", header=header)
    set_coordinate_system(lf, zero_based=False)
    out = tmp_path / "out.vcf"
    pb.sink_vcf(lf, str(out))
    _, records = _read(out)
    assert [r.split("\t")[7] for r in records] == ["INFO_X=1", "INFO_X=2"]
```

- [ ] **Step 3: Run it, expect failure**

Run: `uv run pytest tests/test_vcf_write_info_prefix.py -v`
Expected: first test FAILS — today `AF` resolves to the annotation column, so INFO reads
`AF=0.001;DP=10` and `INFO_AF` is dropped. Second test PASSES (guard against over-reach).

- [ ] **Step 4: Implement the helper** — add above `execute_vcf_streaming_write` in `src/write.rs`

```rust
/// Output projection for a VCF write, or `None` when the frame holds no renamed
/// input column.
///
/// An input INFO id that matches another column's name reaches the frame as
/// `INFO_<id>` (the annotation engine keeps the bare name for its own column). Such a
/// column is the INFO field; the bare `<id>` column is not. Scoped to ids the header
/// declares, so a field whose real id starts with `INFO_` is untouched.
fn vcf_write_projection(
    schema: &Schema,
    info_ids: &std::collections::HashSet<String>,
) -> Option<Vec<(String, String)>> {
    let renamed: std::collections::HashMap<&str, &str> = schema
        .fields()
        .iter()
        .filter_map(|field| {
            let name = field.name().as_str();
            let id = name.strip_prefix("INFO_")?;
            (info_ids.contains(id) && !info_ids.contains(name)).then_some((name, id))
        })
        .collect();
    if renamed.is_empty() {
        return None;
    }
    let shadowed: std::collections::HashSet<&str> = renamed.values().copied().collect();
    Some(
        schema
            .fields()
            .iter()
            .filter_map(|field| {
                let name = field.name().as_str();
                if let Some(id) = renamed.get(name) {
                    Some((name.to_string(), (*id).to_string()))
                } else if shadowed.contains(name) {
                    None // the same-named annotation column is not the INFO field
                } else {
                    Some((name.to_string(), name.to_string()))
                }
            })
            .collect(),
    )
}
```

- [ ] **Step 5: Use it** — in `execute_vcf_streaming_write`, replace

```rust
    // Get the schema from the DataFrame
    let schema = df.schema().inner().clone();
```

with

```rust
    // A renamed input column is written under its VCF id; see vcf_write_projection.
    let info_ids: std::collections::HashSet<String> = vcf_metadata
        .as_ref()
        .and_then(|(info_meta, _, _, _)| info_meta.as_deref())
        .and_then(|json| serde_json::from_str::<HashMap<String, serde_json::Value>>(json).ok())
        .map(|meta| meta.into_keys().collect())
        .unwrap_or_default();
    let df = match vcf_write_projection(df.schema().inner(), &info_ids) {
        Some(columns) => df.select(
            columns
                .into_iter()
                .map(|(source, output)| datafusion::prelude::col(format!("\"{source}\"")).alias(output))
                .collect::<Vec<_>>(),
        )?,
        None => df,
    };

    // Get the schema from the DataFrame
    let schema = df.schema().inner().clone();
```

If `col("\"name\"")` does not resolve a mixed-case identifier in this DataFusion
version, use `datafusion::common::Column::from_name(source)` wrapped in `Expr::Column`.

- [ ] **Step 6: Build and run**

Run: `uv run maturin develop --release && uv run pytest tests/test_vcf_write_info_prefix.py -v`
Expected: both PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/write.rs tests/test_vcf_write_info_prefix.py
git commit -m "fix(vcf): write an INFO_<id> column under its VCF id and skip the same-named column"
```

---

### Task 3: polars-bio — keep `##FILTER`, `##ALT` and `##fileformat` on write

**Files:**
- Modify: `polars_bio/io.py` (`_write_file`, ~`:4034-4070`), `src/option.rs` (`VcfWriteOptions`, ~`:1238-1280`), `src/write.rs` (`write_vcf_streaming`, `execute_vcf_streaming_write`, the `VcfMetadataJson` alias)
- Test: `tests/test_vcf_write_header_lines.py` (new)

**Interfaces:**
- Consumes: nothing.
- Produces: `VcfWriteOptions(..., filters_metadata: Option<String>, alt_definitions_metadata: Option<String>, file_format: Option<String>)` — JSON strings in the shape the header metadata already has (`header["filters"]`, `header["alt_definitions"]`, `header["version"]`).

- [ ] **Step 1: Write the failing test** — `tests/test_vcf_write_header_lines.py`

```python
import polars as pl
import polars_bio as pb

VCF = """##fileformat=VCFv4.2
##FILTER=<ID=LowQual,Description="Low quality">
##ALT=<ID=DEL,Description="Deletion">
##contig=<ID=chr1,length=248956422>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tG\t50\tLowQual\tDP=10
"""


def test_filter_alt_and_fileformat_survive_a_round_trip(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text(VCF)
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    header = [l for l in out.read_text().splitlines() if l.startswith("##")]
    assert "##fileformat=VCFv4.2" in header
    assert '##FILTER=<ID=LowQual,Description="Low quality">' in header
    assert '##ALT=<ID=DEL,Description="Deletion">' in header
```

- [ ] **Step 2: Run it, expect failure**

Run: `uv run pytest tests/test_vcf_write_header_lines.py -v`
Expected: FAIL — `##fileformat=VCFv4.3`, no FILTER or ALT line.

- [ ] **Step 3: Python — forward the three values** in `_write_file` (`polars_bio/io.py:4050-4070`)

`header["filters"]` and `header["alt_definitions"]` are already the parsed
`bio.vcf.filters` / `bio.vcf.alternative_alleles` JSON (`metadata_extractors.py:284-290`),
i.e. `[{"id": ..., "description": ...}]` — the `Vec<FilterMetadata>` /
`Vec<AltAlleleMetadata>` shape the header builder reads — so they pass through unchanged.
After the `contigs_json` lines add:

```python
            if vcf_header.get("filters"):
                filters_json = json.dumps(vcf_header["filters"])
            if vcf_header.get("alt_definitions"):
                alt_definitions_json = json.dumps(vcf_header["alt_definitions"])
            file_format = vcf_header.get("version")
```

initialise `filters_json = alt_definitions_json = file_format = None` beside
`contigs_json = None`, and pass them on:

```python
        vcf_opts = VcfWriteOptions(
            zero_based=zero_based,
            info_fields_metadata=info_fields_json,
            format_fields_metadata=format_fields_json,
            sample_names=sample_names_json,
            contigs_metadata=contigs_json,
            filters_metadata=filters_json,
            alt_definitions_metadata=alt_definitions_json,
            file_format=file_format,
        )
```

- [ ] **Step 4: Rust — carry them to the schema metadata**

`src/option.rs`, `VcfWriteOptions`: add after `contigs_metadata`

```rust
    /// FILTER definitions as JSON string: [{"id": "LowQual", "description": "..."}]
    #[pyo3(get, set)]
    pub filters_metadata: Option<String>,
    /// ALT definitions as JSON string: [{"id": "DEL", "description": "..."}]
    #[pyo3(get, set)]
    pub alt_definitions_metadata: Option<String>,
    /// Source `##fileformat` value, e.g. "VCFv4.2"
    #[pyo3(get, set)]
    pub file_format: Option<String>,
```

extend the signature to
`(zero_based=true, info_fields_metadata=None, format_fields_metadata=None, sample_names=None, contigs_metadata=None, filters_metadata=None, alt_definitions_metadata=None, file_format=None)`,
add the three parameters and fields to `new`, and `None` for each in `default()`.

`src/write.rs`: import `VCF_FILTERS_KEY, VCF_ALTERNATIVE_ALLELES_KEY, VCF_FILE_FORMAT_KEY`
beside `VCF_CONTIGS_KEY` (`:26`); replace the alias (`:40-45`) with

```rust
/// info, format, samples, then header-level metadata as (schema key, JSON/text) pairs.
type VcfMetadataJson = (
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<(&'static str, String)>,
);
```

in `write_vcf_streaming` build the last element instead of `vcf_opts.contigs_metadata.clone()`:

```rust
                    [
                        (VCF_CONTIGS_KEY, &vcf_opts.contigs_metadata),
                        (VCF_FILTERS_KEY, &vcf_opts.filters_metadata),
                        (VCF_ALTERNATIVE_ALLELES_KEY, &vcf_opts.alt_definitions_metadata),
                        (VCF_FILE_FORMAT_KEY, &vcf_opts.file_format),
                    ]
                    .into_iter()
                    .filter_map(|(key, value)| value.clone().map(|value| (key, value)))
                    .collect::<Vec<_>>(),
```

and in `execute_vcf_streaming_write` rename `contigs_meta`/`contigs_json` to
`header_meta`/`header_entries` (the heuristics arm yields `Vec::new()`), replacing the
contigs block with

```rust
    // Header-level metadata the upstream header builder turns into ##contig,
    // ##FILTER, ##ALT and ##fileformat lines.
    let schema_with_metadata = if header_entries.is_empty() {
        schema_with_metadata
    } else {
        let mut metadata = schema_with_metadata.metadata().clone();
        for (key, value) in header_entries {
            metadata.insert(key.to_string(), value);
        }
        Arc::new(schema_with_metadata.as_ref().clone().with_metadata(metadata))
    };
```

Task 2's `info_ids` extraction destructures this tuple as `(info_meta, _, _, _)`; it
is unaffected by the last element's type.

- [ ] **Step 5: Build, run, commit**

```bash
uv run maturin develop --release && uv run pytest tests/test_vcf_write_header_lines.py tests/test_vcf_write_info_prefix.py -v
cargo fmt && cargo clippy --all-targets -- -D warnings
git add polars_bio/io.py src/option.rs src/write.rs tests/test_vcf_write_header_lines.py
git commit -m "fix(vcf): keep FILTER, ALT and fileformat header lines on write"
```

---

### Task 4: polars-bio — pick up GT-first, prove the round trip, open the PR

**Files:**
- Modify: `Cargo.toml` (all `datafusion-bio-format-*` revs, `:49-60`), `Cargo.lock`, `CHANGELOG.md`
- Test: `tests/test_vcf_write_header_lines.py` (one more test)

**Interfaces:**
- Consumes: `FORMATS_REV`, the Task 1 PR head sha.

- [ ] **Step 1: Failing test** — append to `tests/test_vcf_write_header_lines.py`

```python
def test_gt_is_the_first_format_key(tmp_path):
    src = tmp_path / "in.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1,length=248956422>\n"
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
        "chr1\t100\t.\tA\tG\t50\tPASS\t.\tGT:DP\t0/1:25\n"
    )
    out = tmp_path / "out.vcf"
    pb.sink_vcf(pb.scan_vcf(str(src)), str(out))
    record = [l for l in out.read_text().splitlines() if not l.startswith("#")][0].split("\t")
    assert record[8:10] == ["GT:DP", "0/1:25"]
```

Run: `uv run pytest tests/test_vcf_write_header_lines.py::test_gt_is_the_first_format_key -v`
Expected: FAIL with `['DP:GT', '25:0/1']`.

- [ ] **Step 2: Bump the pin**

```bash
sed -i '' "s/rev = \"1c86f0c9d19ccc85eaf196508c26d70a220f21f9\"/rev = \"$FORMATS_REV\"/" Cargo.toml
grep -c "$FORMATS_REV" Cargo.toml        # expect one per datafusion-bio-format-* entry
cargo update -p datafusion-bio-format-vcf
uv run maturin develop --release
```

If the old rev string differs by the time this runs, read it from `Cargo.toml:53` first.

- [ ] **Step 3: Full suite**

Run: `uv run pytest tests -x -q`
Expected: PASS. A VCF-writing test that asserted header order in FORMAT encoded the bug;
fix its expectation.

- [ ] **Step 4: Changelog, commit, draft PR**

Add under the unreleased heading of `CHANGELOG.md`:

```markdown
- `sink_vcf`/`write_vcf`: GT is written first in FORMAT; `##FILTER`, `##ALT` and the
  source `##fileformat` are kept; a column named `INFO_<id>` is written under `<id>`
  when the header declares `<id>` (frames from `vepyr.annotate`).
```

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md tests/test_vcf_write_header_lines.py
git commit -m "build: bump datafusion-bio-formats for GT-first FORMAT"
git push -u origin fix/sink-vcf-info-prefix
gh pr create --draft --repo biodatageeks/polars-bio --head fix/sink-vcf-info-prefix \
  --title "fix(vcf): spec-valid sink_vcf output and INFO_<id> columns" \
  --body-file /tmp/pr-polars-bio.md
```

`/tmp/pr-polars-bio.md`: the three defects with the HG002 reproduction
(`DP:GQ:ADALL:AD:GT`), the name rule verbatim from Global Constraints, the pinned
formats PR, and the three new test files.

- [ ] **Step 5: After the formats PR merges** (human merges): re-pin to the merge commit,
`cargo update -p datafusion-bio-format-vcf`, rerun Step 3, push. Do not merge.
