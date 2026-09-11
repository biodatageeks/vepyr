# Engine: `gff` provider and `lookup = "interval"` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a plugin source manifest read a GFF3 file (`provider = "gff"`) and declare an interval-keyed lookup (`lookup = "interval"`) so the PhenotypeOrthologous plugin can be built and probed by the engine.

**Architecture:** Two stacked PRs on `biodatageeks/datafusion-bio-functions`. PR 1 adds a `ProviderKind::Gff` arm on top of `datafusion-bio-format-gff`'s `GffTableProvider` (flat attribute columns, per-chromosome slicing through the existing tabix materialiser). PR 2 adds a `LookupKind` (`point` today, new `interval`) that drops `allele_string`, skips the variation tier join at build time, and at runtime loads the whole per-chromosome shard into per-discriminator COITrees probed with the variant's VEP-normalised span (first overlapping row in file order wins).

**Tech Stack:** Rust 2021, DataFusion 53, Arrow/Parquet 58, `datafusion-bio-format-gff` tag `v1.12.1`, `coitrees` 0.4 (already a dependency; same `COITree<usize, u32>` idiom as `transcript_consequence.rs:886-963`), noodles (bgzf/tabix/csi) at the workspace revs, serde/toml.

**Spec:** `docs/superpowers/specs/2026-09-11-gff-plugin-source-and-phenotypeorthologous-design.md` (in vepyr).

## Global Constraints

- Work in a git worktree of `/Users/mwiewior/research/git/datafusion-bio-functions` branched from `origin/master` at `4b0bd00` (vepyr's current pin). Do not branch from the local `fix/mnv-allele-trim-parity` checkout.
- bio-formats crates are pinned by **tag** `v1.12.1` in `datafusion/bio-function-vep/Cargo.toml`; the new `datafusion-bio-format-gff` dependency must use the same `tag = "v1.12.1"` form, never `rev`.
- Crate: `datafusion/bio-function-vep`. Module: `src/plugin_cache/`. Run tests with `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache` from the repo root; run `cargo fmt --all` and `cargo clippy -p datafusion-bio-function-vep --features parquet-cache --all-targets -- -D warnings` before each commit.
- Existing point-lookup behaviour must not change: every existing `plugin_cache` test keeps passing unmodified except where a signature gains a parameter (those tests pass `LookupKind::Point` / the old key list).
- PR 1 = Tasks 1–3 (branch `feat/plugin-gff-provider`). PR 2 = Tasks 4–9 (branch `feat/plugin-interval-lookup`, stacked on PR 1). Draft PRs, no merging by the agent (vepyr-fix discipline).
- Commit message trailer on every commit:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01Ybip12LHW1qoorZjs3TwYT`.

---

## PR 1 — `gff` provider

### Task 1: Manifest surface for `gff`

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/source_manifest.rs:19-29` (`ProviderKind`), `:60-77` (params structs), `:79-111` (`SourceSpec`), `:271-320` (`validate`)
- Test: same file, `mod tests`

**Interfaces:**
- Produces: `ProviderKind::Gff`; `pub struct GffParams { pub attributes: Vec<String> }`; `SourceSpec.gff: Option<GffParams>`.

- [ ] **Step 1: Write the failing tests** (append inside `mod tests` of `source_manifest.rs`)

```rust
    const GFF_MANIFEST: &str = r##"
plugin_name = "po"
coordinate_system = "1-based"
ingest_sql = "SELECT chrom, start, \"end\", gene_id, \"Rat_gene_id\" AS rat FROM plugin_po_src"

[[source]]
provider = "gff"
path = "/tmp/po.gff3.gz"
url = "https://example.org/po.gff3.gz"
md5 = "20e5401a198d7d3db66a982c037d3ad4"
index = "tabix"
  [source.gff]
  attributes = ["gene_id", "Rat_gene_id"]

[[value_columns]]
column = "rat"
csq_field = "PO_Rat"
type = "Utf8"
"##;

    #[test]
    fn parses_gff_source_with_attributes() {
        let m: SourceManifest = toml::from_str(GFF_MANIFEST).unwrap();
        m.validate().unwrap();
        assert_eq!(m.sources[0].provider, ProviderKind::Gff);
        assert_eq!(
            m.sources[0].gff.as_ref().unwrap().attributes,
            vec!["gene_id".to_string(), "Rat_gene_id".to_string()]
        );
        assert_eq!(m.sources[0].index, Some(SourceIndex::Tabix));
    }

    #[test]
    fn gff_source_requires_attributes() {
        let missing = GFF_MANIFEST.replace(
            "  [source.gff]\n  attributes = [\"gene_id\", \"Rat_gene_id\"]\n",
            "",
        );
        let m: SourceManifest = toml::from_str(&missing).unwrap();
        let err = m.validate().unwrap_err().to_string();
        assert!(err.contains("[source.gff]"), "{err}");

        let empty = GFF_MANIFEST.replace(
            "attributes = [\"gene_id\", \"Rat_gene_id\"]",
            "attributes = []",
        );
        let m: SourceManifest = toml::from_str(&empty).unwrap();
        let err = m.validate().unwrap_err().to_string();
        assert!(err.contains("at least one attribute"), "{err}");
    }

    #[test]
    fn gff_table_is_rejected_on_other_providers() {
        let wrong = GFF_MANIFEST
            .replace("provider = \"gff\"", "provider = \"bed\"")
            .replace("index = \"tabix\"\n", "");
        let m: SourceManifest = toml::from_str(&wrong).unwrap();
        let err = m.validate().unwrap_err().to_string();
        assert!(err.contains("[source.gff]") && err.contains("Bed"), "{err}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::source_manifest`
Expected: compile error, `no variant named Gff` / `no field gff`.

- [ ] **Step 3: Implement**

In `source_manifest.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Vcf,
    Csv,
    Tsv,
    Parquet,
    Bed,
    /// GFF3 via `datafusion-bio-format-gff`. `[source.gff].attributes` names the
    /// attribute keys exposed as flat Utf8 columns next to the eight fixed
    /// GFF columns (`chrom, start, end, type, source, score, strand, phase`).
    Gff,
}

/// GFF provider parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct GffParams {
    /// Attribute keys to project as flat nullable Utf8 columns, in this order.
    /// Values are percent-decoded by the reader and otherwise untouched.
    pub attributes: Vec<String>,
}
```

Add to `SourceSpec` after `csv`:

```rust
    #[serde(default)]
    pub gff: Option<GffParams>,
```

In `validate()`, inside the `for source in &self.sources` loop, before the tabix checks:

```rust
            match (source.provider, source.gff.as_ref()) {
                (ProviderKind::Gff, None) => {
                    return Err(DataFusionError::Execution(format!(
                        "plugin '{}' {} is a gff source but has no [source.gff] table",
                        self.plugin_name,
                        source.label()
                    )));
                }
                (ProviderKind::Gff, Some(gff)) if gff.attributes.is_empty() => {
                    return Err(DataFusionError::Execution(format!(
                        "plugin '{}' {} must list at least one attribute in \
                         [source.gff].attributes",
                        self.plugin_name,
                        source.label()
                    )));
                }
                (other, Some(_)) if other != ProviderKind::Gff => {
                    return Err(DataFusionError::Execution(format!(
                        "plugin '{}' {} declares [source.gff] for a {other:?} source",
                        self.plugin_name,
                        source.label()
                    )));
                }
                _ => {}
            }
```

Extend the tabix provider check at `:291-300` to include `ProviderKind::Gff`:

```rust
                if !matches!(
                    source.provider,
                    ProviderKind::Csv | ProviderKind::Tsv | ProviderKind::Vcf | ProviderKind::Gff
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "plugin '{}' declares a tabix index for a {:?} source; tabix indexes are \
                         supported only for csv/tsv/vcf/gff providers",
                        self.plugin_name, source.provider
                    )));
                }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::source_manifest`
Expected: all PASS, including the pre-existing ones.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/plugin_cache/source_manifest.rs
git commit -m "feat(plugin-cache): declare gff sources with [source.gff].attributes"
```

### Task 2: `GffTableProvider` registration arm

**Files:**
- Modify: `datafusion/bio-function-vep/Cargo.toml:41-44` (dependency)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/provider.rs:13-27` (imports), `:46-119` (`materialize_tabix_chrom` suffix), `:167-279` (`register_sources_impl`)
- Test: `provider.rs` `mod tests`

**Interfaces:**
- Consumes: `ProviderKind::Gff`, `SourceSpec.gff` (Task 1); `GffTableProvider::new(file_path: String, attr_fields: Option<Vec<String>>, object_storage_options: Option<ObjectStorageOptions>, coordinate_system_zero_based: bool) -> Result<Self>` from `datafusion_bio_format_gff::table_provider`.
- Produces: table `plugin_<name>_src[_<part>]` with columns `chrom Utf8, start UInt32, end UInt32, type Utf8, source Utf8, score Float32?, strand Utf8, phase UInt32?` + one nullable Utf8 column per attribute.

- [ ] **Step 1: Add the dependency**

In `datafusion/bio-function-vep/Cargo.toml` after the `datafusion-bio-format-bed` line:

```toml
datafusion-bio-format-gff = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", tag = "v1.12.1" }
```

Run: `cargo check -p datafusion-bio-function-vep --features parquet-cache` — Expected: OK (same tag, no duplicate crate graph).

- [ ] **Step 2: Write the failing tests** (append to `mod tests` in `provider.rs`)

```rust
    /// Three gene features on two contigs. `%3B` must decode, the leading space
    /// on the rat phenotype must survive, and the mouse attribute is absent on
    /// the second row.
    const GFF_ROWS: &[(&str, usize, usize, &str)] = &[
        ("1", 600000, 605000, "ID=gene:ENSG1;gene_id=ENSG1;Rat_gene_id=RNO1;Rat_Orthologous_phenotype= a b|c%3Bd;Mouse_gene_id=MUS1"),
        ("1", 610000, 612000, "ID=gene:ENSG2;gene_id=ENSG2;Rat_gene_id=RNO2;Rat_Orthologous_phenotype=x"),
        ("2", 100, 200, "ID=gene:ENSG3;gene_id=ENSG3;Rat_gene_id=RNO3;Rat_Orthologous_phenotype=y"),
    ];

    fn gff_body() -> String {
        let mut s = String::from("##gff-version 3\n");
        for &(c, st, en, attrs) in GFF_ROWS {
            s.push_str(&format!("{c}\ttest\tgene\t{st}\t{en}\t.\t+\t.\t{attrs}\n"));
        }
        s
    }

    fn write_bgzf_tabix_gff(path: &std::path::Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = noodles_bgzf::io::Writer::new(file);
        let mut indexer = noodles_tabix::index::Indexer::default();
        indexer.set_header(
            csi::binning_index::index::header::Builder::gff()
                .set_line_skip_count(0)
                .build(),
        );
        writeln!(writer, "##gff-version 3").unwrap();
        let mut chunk_start = writer.virtual_position();
        for &(c, st, en, attrs) in GFF_ROWS {
            writeln!(writer, "{c}\ttest\tgene\t{st}\t{en}\t.\t+\t.\t{attrs}").unwrap();
            let chunk_end = writer.virtual_position();
            indexer
                .add_record(
                    c,
                    Position::try_from(st).unwrap(),
                    Position::try_from(en).unwrap(),
                    Chunk::new(chunk_start, chunk_end),
                )
                .unwrap();
            chunk_start = chunk_end;
        }
        writer.finish().unwrap();
        let index_file = std::fs::File::create(format!("{}.tbi", path.display())).unwrap();
        let mut index_writer = noodles_tabix::io::Writer::new(index_file);
        index_writer.write_index(&indexer.build()).unwrap();
    }

    fn gff_manifest(path: &std::path::Path, tabix: bool) -> SourceManifest {
        let index = if tabix { "index = \"tabix\"" } else { "" };
        toml::from_str(&format!(
            r##"
plugin_name = "po"
coordinate_system = "1-based"
ingest_sql = "SELECT 1"

[[source]]
provider = "gff"
path = "{}"
{index}
  [source.gff]
  attributes = ["gene_id", "Rat_gene_id", "Rat_Orthologous_phenotype", "Mouse_gene_id"]

[[value_columns]]
column = "rat"
csq_field = "PO_Rat"
type = "Utf8"
"##,
            path.display()
        ))
        .unwrap()
    }

    async fn collect_gff(ctx: &SessionContext) -> Vec<(String, u32, u32, Option<String>, Option<String>, Option<String>)> {
        use datafusion::arrow::array::UInt32Array;
        let batches = ctx
            .sql("SELECT chrom, start, \"end\", gene_id, \"Rat_Orthologous_phenotype\", \"Mouse_gene_id\" FROM plugin_po_src ORDER BY start")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let chrom = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let start = b.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
            let end = b.column(2).as_any().downcast_ref::<UInt32Array>().unwrap();
            let gene = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
            let rat = b.column(4).as_any().downcast_ref::<StringArray>().unwrap();
            let mouse = b.column(5).as_any().downcast_ref::<StringArray>().unwrap();
            for r in 0..b.num_rows() {
                let opt = |a: &StringArray| (!a.is_null(r)).then(|| a.value(r).to_string());
                out.push((chrom.value(r).to_string(), start.value(r), end.value(r), opt(gene), opt(rat), opt(mouse)));
            }
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gff_plain_and_gzip_expose_flat_attributes() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("po.gff3");
        std::fs::write(&plain, gff_body()).unwrap();
        let gz = dir.path().join("po.gff3.gz");
        write_gz(&gz, &gff_body());

        for path in [plain, gz] {
            let ctx = SessionContext::new();
            let temps = register_sources(&ctx, &gff_manifest(&path, false)).await.unwrap();
            assert!(temps.is_empty(), "gff needs no staging temp: {path:?}");
            let rows = collect_gff(&ctx).await;
            assert_eq!(rows.len(), 3, "{path:?}");
            assert_eq!(rows[0].0, "2");
            assert_eq!((rows[1].1, rows[1].2), (600000, 605000), "1-based coordinates pass through");
            assert_eq!(rows[1].3.as_deref(), Some("ENSG1"));
            assert_eq!(rows[1].4.as_deref(), Some(" a b|c;d"), "leading space kept, %3B decoded");
            assert_eq!(rows[1].5.as_deref(), Some("MUS1"));
            assert_eq!(rows[2].5, None, "absent attribute is NULL");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gff_tabix_source_materializes_only_requested_contig() {
        let dir = tempfile::tempdir().unwrap();
        let gz = dir.path().join("po.gff3.gz");
        write_bgzf_tabix_gff(&gz);
        let manifest = gff_manifest(&gz, true);

        let ctx = SessionContext::new();
        let temps = register_sources_for_chrom(&ctx, &manifest, "chr1").await.unwrap();
        assert_eq!(temps.len(), 1);
        let rows = collect_gff(&ctx).await;
        assert_eq!(rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(), vec!["1", "1"]);
        assert_eq!(rows[0].4.as_deref(), Some(" a b|c;d"));

        // A contig absent from the index yields zero rows, not an error.
        let ctx = SessionContext::new();
        register_sources_for_chrom(&ctx, &manifest, "X").await.unwrap();
        assert!(collect_gff(&ctx).await.is_empty());

        // Unscoped registration of a tabix source is refused, as for csv.
        let ctx = SessionContext::new();
        assert!(register_sources(&ctx, &manifest).await.is_err());
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::provider::tests::gff`
Expected: FAIL — `register_sources_impl` has no `Gff` arm (non-exhaustive match compile error).

- [ ] **Step 4: Implement the arm**

Imports in `provider.rs`:

```rust
use datafusion_bio_format_gff::table_provider::GffTableProvider;
```

Change `materialize_tabix_chrom` to take the temp suffix so a GFF slice keeps a GFF name (the reader sniffs compression by content, but a recognisable suffix helps debugging):

```rust
fn materialize_tabix_chrom(path: &str, chrom: &str, suffix: &str) -> Result<(String, tempfile::TempPath)> {
    ...
    let mut tmp = tempfile::Builder::new()
        .prefix("plugin_src_")
        .suffix(suffix)
        .tempfile()
```

and pass `".tsv"` from `materialize_plain` (`:140`). Add the arm before `ProviderKind::Bed`:

```rust
            ProviderKind::Gff => {
                let gff = spec.gff.as_ref().ok_or_else(|| {
                    DataFusionError::Execution(format!("gff source '{table}' missing [source.gff]"))
                })?;
                // Same lock-step rule as the VCF/BED arms: `ingest_sql` sees the
                // coordinate system the manifest declares.
                let zero_based = matches!(
                    manifest.coordinate_system,
                    CoordinateSystem::ZeroBasedHalfOpen
                );
                // A tabix-indexed GFF is sliced to the requested contig exactly
                // like an indexed TSV (records copied verbatim, so the temp is
                // valid GFF3). Plain or gzip GFF is opened in place: the reader
                // detects compression by content and reads it whole.
                let path = if spec.index == Some(SourceIndex::Tabix) {
                    let chrom = chrom.ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "BGZF/tabix source '{}' requires a chromosome-scoped plugin-cache build",
                            spec.path
                        ))
                    })?;
                    let (plain, temp) = materialize_tabix_chrom(&spec.path, chrom, ".gff3")?;
                    temps.push(temp);
                    plain
                } else {
                    spec.path.clone()
                };
                let provider = GffTableProvider::new(path, Some(gff.attributes.clone()), None, zero_based)
                    .map_err(|e| DataFusionError::Execution(format!("open GFF source '{table}': {e}")))?;
                ctx.register_table(&table, Arc::new(provider))?;
            }
```

Update the module doc at `:1-11` with one sentence: "GFF uses `datafusion-bio-format-gff`'s `GffTableProvider` with the manifest's `[source.gff].attributes` projected as flat Utf8 columns."

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::provider`
Expected: PASS. If `gff_tabix_source_materializes_only_requested_contig` fails because the reader reports zero rows for the temp slice, the header comment line is the cause: `Builder::gff()` sets `meta = '#'`, and the query result never contains header lines, so check that the temp file's first record starts with a contig, then check `GffTableProvider` accepts a file without `##gff-version` (it does in v1.12.1; if not, write `##gff-version 3\n` at the top of the temp in the Gff arm before the records).

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-function-vep/Cargo.toml Cargo.lock datafusion/bio-function-vep/src/plugin_cache/provider.rs
git commit -m "feat(plugin-cache): gff source provider via datafusion-bio-format-gff"
```

### Task 3: PR 1 hygiene and draft PR

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/mod.rs:1-8` (module doc mentions GFF), `datafusion/bio-function-vep/examples/build_plugin.rs:1-18` (doc comment: "`gff` sources with `index = \"tabix\"` are sliced per chromosome like indexed TSV")

- [ ] **Step 1: Full test, fmt, clippy**

Run: `cargo fmt --all && cargo clippy -p datafusion-bio-function-vep --features parquet-cache --all-targets -- -D warnings && cargo test -p datafusion-bio-function-vep --features parquet-cache`
Expected: clean; all tests pass.

- [ ] **Step 2: Commit and open the draft PR**

```bash
git add -A datafusion/bio-function-vep/src/plugin_cache/mod.rs datafusion/bio-function-vep/examples/build_plugin.rs
git commit -m "docs(plugin-cache): mention the gff provider"
git push -u origin feat/plugin-gff-provider
gh pr create --draft --title "feat(plugin-cache): gff source provider" --body-file <(cat <<'EOF'
Adds `provider = "gff"` to plugin source manifests, backed by `datafusion-bio-format-gff` (tag v1.12.1). `[source.gff].attributes` projects attribute keys as flat Utf8 columns; `index = "tabix"` slices per chromosome through the existing tabix materialiser.

First half of the PhenotypeOrthologous port; the interval lookup follows in a stacked PR. Spec: vepyr `docs/superpowers/specs/2026-09-11-gff-plugin-source-and-phenotypeorthologous-design.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01Ybip12LHW1qoorZjs3TwYT
EOF
)
```

---

## PR 2 — `lookup = "interval"`

### Task 4: `LookupKind` in both manifests

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/cache_manifest.rs:140-175` (`CacheManifest`), `:177-186` (enums), `:230-268` (`from_source`)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/source_manifest.rs:176-212` (`SourceManifest`), `validate`
- Modify: `datafusion/bio-function-vep/src/plugin_cache/builder.rs:279-293` (`schema_matches`)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/registry.rs:381-409` (test helper `write_empty_manifest` gains `lookup: Default::default()`)
- Test: `cache_manifest.rs`, `source_manifest.rs` test modules

**Interfaces:**
- Produces: `pub enum LookupKind { Point, Interval }` (serde lowercase, `Default = Point`) in `cache_manifest.rs`; `SourceManifest.lookup: LookupKind`; `CacheManifest.lookup: LookupKind`; `pub fn key_columns(lookup: LookupKind) -> Vec<String>`.

- [ ] **Step 1: Write the failing tests**

In `cache_manifest.rs` tests:

```rust
    #[test]
    fn legacy_manifest_without_lookup_reads_as_point() {
        let json = r#"{
            "plugin_name": "demo",
            "source_manifest": "demo.source.toml",
            "key_columns": ["chrom","start","end","allele_string"],
            "value_columns": [{"column":"s","csq_field":"S","type":"Float32"}],
            "chroms": []
        }"#;
        let m: CacheManifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.lookup, LookupKind::Point);
    }

    #[test]
    fn interval_manifest_records_lookup_and_key_columns() {
        let src: SourceManifest = toml::from_str(
            r##"
plugin_name = "po"
coordinate_system = "1-based"
lookup = "interval"
ingest_sql = "SELECT 1"
[[source]]
provider = "gff"
path = "/tmp/po.gff3.gz"
  [source.gff]
  attributes = ["gene_id"]
[[match_column]]
column = "gene_id"
template = "{Gene}"
[[value_columns]]
column = "rat"
csq_field = "PO_Rat"
type = "Utf8"
"##,
        )
        .unwrap();
        let cache = CacheManifest::from_source(&src, "po.source.toml");
        assert_eq!(cache.lookup, LookupKind::Interval);
        assert_eq!(cache.key_columns, vec!["chrom", "start", "end"]);
        let json = serde_json::to_string(&cache).unwrap();
        assert!(json.contains("\"lookup\":\"interval\""));
    }
```

In `source_manifest.rs` tests:

```rust
    #[test]
    fn interval_lookup_rejects_minimised_allele_match() {
        let text = GFF_MANIFEST.replace(
            "coordinate_system = \"1-based\"",
            "coordinate_system = \"1-based\"\nlookup = \"interval\"\nallele_match = \"minimised\"",
        );
        let m: SourceManifest = toml::from_str(&text).unwrap();
        let err = m.validate().unwrap_err().to_string();
        assert!(err.contains("allele_match"), "{err}");
    }

    #[test]
    fn lookup_defaults_to_point() {
        let m: SourceManifest = toml::from_str(CADD_LIKE).unwrap();
        assert_eq!(m.lookup, crate::plugin_cache::cache_manifest::LookupKind::Point);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::cache_manifest plugin_cache::source_manifest`
Expected: compile errors on `LookupKind` / `.lookup`.

- [ ] **Step 3: Implement**

`cache_manifest.rs`, next to `FieldOrder`:

```rust
/// How the runtime finds a shard row for a consequence line.
///
/// `Point` is the original exact probe on `(start, allele_string, <match…>)`
/// with variation-inherited tiering. `Interval` rows carry a genomic span and
/// no allele: a row matches when its `[start, end]` overlaps the variant's
/// VEP-normalised span and every match discriminator agrees. The first such
/// row in file order wins, as a tabix-backed Ensembl plugin returns records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LookupKind {
    #[default]
    Point,
    Interval,
}

/// The shard key columns a lookup kind stores, in shard order.
pub fn key_columns(lookup: LookupKind) -> Vec<String> {
    match lookup {
        LookupKind::Point => vec!["chrom".into(), "start".into(), "end".into(), "allele_string".into()],
        LookupKind::Interval => vec!["chrom".into(), "start".into(), "end".into()],
    }
}
```

`CacheManifest` gains, after `field_order`:

```rust
    /// Lookup kind the shards were built for. Absent in caches built before
    /// interval plugins existed, which are all point lookups.
    #[serde(default)]
    pub lookup: LookupKind,
```

`from_source`: `key_columns: key_columns(src.lookup)` and `lookup: src.lookup`.

`SourceManifest` gains, after `field_order`:

```rust
    /// Lookup kind; `interval` drops `allele_string` and the variation tier
    /// join and matches rows by span overlap plus discriminators.
    #[serde(default)]
    pub lookup: crate::plugin_cache::cache_manifest::LookupKind,
```

`validate()` end, before value-column checks:

```rust
        if self.lookup == crate::plugin_cache::cache_manifest::LookupKind::Interval
            && self.allele_match != crate::plugin_cache::cache_manifest::AlleleMatch::Exact
        {
            return Err(DataFusionError::Execution(format!(
                "plugin '{}' sets allele_match = {:?} with lookup = \"interval\"; interval rows \
                 carry no allele, so allele_match has no meaning there",
                self.plugin_name, self.allele_match
            )));
        }
```

`builder.rs::schema_matches`: prepend `a.lookup == b.lookup &&` to the boolean.

`registry.rs` test helper `write_empty_manifest` and any other `CacheManifest { .. }` literal in tests: add `lookup: Default::default(),`.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/plugin_cache/{cache_manifest,source_manifest,builder,registry}.rs
git commit -m "feat(plugin-cache): LookupKind (point|interval) on source and cache manifests"
```

### Task 5: Build path for interval shards

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/normalize.rs:56-79` (`wrap_normalization`)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/write.rs:26-44` (`plugin_output_schema`)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/dedup.rs:40-51` and `check_assume_unique_sample` (key names parameter)
- Modify: `datafusion/bio-function-vep/src/plugin_cache/build.rs:410-660` (`build_plugin_chrom_staged`), new fn `write_interval_shard`
- Test: `normalize.rs`, `write.rs`, `build.rs` test modules

**Interfaces:**
- `wrap_normalization(inner_view: &str, coord: CoordinateSystem, lookup: LookupKind, match_columns: &[String], value_columns: &[String]) -> String`
- `plugin_output_schema(lookup: LookupKind, matches: &[MatchColumn], values: &[ValueColumn]) -> SchemaRef`
- `dedup_keep_first(stream, key_columns: &[String]) -> Result<Vec<RecordBatch>>` and `check_assume_unique_sample(stream, key_columns: &[String])` where `key_columns` is the **full** key list (`["start","allele_string",<match…>]` for point, `["start","end",<match…>]` for interval). Add `pub fn probe_key_columns(lookup: LookupKind, match_columns: &[String]) -> Vec<String>` in `dedup.rs` that builds it.
- `write_interval_shard(deduped: Vec<RecordBatch>, out_schema: &SchemaRef, path: &Path, chrom: &str, plugin_name: &str) -> Result<(usize, usize, usize)>` in `build.rs` (rows, warm=0, cold=rows).

- [ ] **Step 1: Write the failing tests**

`normalize.rs`:

```rust
    #[test]
    fn interval_projection_has_no_allele_string() {
        let sql = wrap_normalization(
            "plugin_po_ingest",
            CoordinateSystem::OneBased,
            LookupKind::Interval,
            &["gene_id".to_string()],
            &["rat".to_string()],
        );
        assert!(!sql.contains("allele_string"), "{sql}");
        assert!(sql.contains("CAST(\"end\" AS BIGINT) AS \"end\", gene_id, rat"), "{sql}");
    }
```

(and pass `LookupKind::Point` in the three existing tests.)

`write.rs`:

```rust
    #[test]
    fn interval_schema_drops_allele_string_and_keeps_tier() {
        let schema = plugin_output_schema(
            LookupKind::Interval,
            &[MatchColumn { column: "gene_id".into(), template: "{Gene}".into() }],
            &f32_value_col(),
        );
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["chrom", "start", "end", "gene_id", "am_pathogenicity", "tier"]);
    }
```

`build.rs` (new test at the end of its `mod tests`, using the GFF helpers — copy `GFF_ROWS`, `gff_body` and `write_gz` from `provider.rs` tests into a small `#[cfg(test)] pub(crate) mod test_gff` module in `provider.rs` so both files share them):

```rust
    #[tokio::test(flavor = "multi_thread")]
    async fn interval_build_writes_span_rows_in_file_order_without_tier_join() {
        use crate::plugin_cache::provider::test_gff::{gff_body, write_gz};
        let dir = tempfile::tempdir().unwrap();
        let gz = dir.path().join("po.gff3.gz");
        write_gz(&gz, &gff_body());
        let manifest: SourceManifest = toml::from_str(&format!(
            r##"
plugin_name = "po"
coordinate_system = "1-based"
lookup = "interval"
field_order = "alphabetical"
ingest_sql = """
SELECT chrom, start, "end", gene_id,
       "Rat_gene_id" AS rat_gene_id, "Rat_Orthologous_phenotype" AS rat_phenotype
FROM plugin_po_src
"""
[[source]]
provider = "gff"
path = "{}"
  [source.gff]
  attributes = ["gene_id", "Rat_gene_id", "Rat_Orthologous_phenotype"]
[[match_column]]
column = "gene_id"
template = "{{Gene}}"
[[value_columns]]
column = "rat_gene_id"
csq_field = "PO_Rat_geneid"
type = "Utf8"
[[value_columns]]
column = "rat_phenotype"
csq_field = "PO_Rat_phenotype"
type = "Utf8"
"##,
            gz.display()
        ))
        .unwrap();
        // The variation shard is unused by an interval build; any readable
        // shard satisfies the signature. `write_synthetic_variation` (existing
        // helper in this test module, build.rs:679) with no rows is enough.
        let variation = dir.path().join("chr1.parquet");
        write_synthetic_variation(&variation, &[]);
        let out = dir.path().join("cache");
        let entry = build_plugin_chrom(&manifest, "po.source.toml", &variation, &out, "1")
            .await
            .unwrap();
        assert_eq!((entry.rows, entry.warm, entry.cold), (2, 0, 2));
        let shard = out.join("plugin/po/chr1.parquet");
        let batches = read_all(&shard).await;
        let names: Vec<_> = batches[0].schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["chrom", "start", "end", "gene_id", "rat_gene_id", "rat_phenotype", "tier"]);
        let starts: Vec<u32> = batches.iter().flat_map(|b| b.column(1).as_any().downcast_ref::<UInt32Array>().unwrap().values().to_vec()).collect();
        assert_eq!(starts, vec![600000, 610000]);
        let tiers: Vec<i8> = batches.iter().flat_map(|b| b.column(6).as_any().downcast_ref::<Int8Array>().unwrap().values().to_vec()).collect();
        assert_eq!(tiers, vec![1, 1]);
    }
```

`write_synthetic_variation` already exists in `build.rs`'s test module (`:679`). Add `read_all` next to it:

```rust
    async fn read_all(path: &std::path::Path) -> Vec<RecordBatch> {
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
        let file = tokio::fs::File::open(path).await.unwrap();
        let mut stream = ParquetRecordBatchStreamBuilder::new(file).await.unwrap().build().unwrap();
        let mut out = Vec::new();
        while let Some(b) = futures::TryStreamExt::try_next(&mut stream).await.unwrap() {
            out.push(b);
        }
        out
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::normalize plugin_cache::write plugin_cache::build::tests::interval`
Expected: compile errors (signatures) then, once signatures exist, the build test fails because the shard has `allele_string` / the join runs.

- [ ] **Step 3: Implement**

`normalize.rs`:

```rust
pub fn wrap_normalization(
    inner_view: &str,
    coord: CoordinateSystem,
    lookup: LookupKind,
    match_columns: &[String],
    value_columns: &[String],
) -> String {
    let start_expr = match coord {
        CoordinateSystem::OneBased => "CAST(start AS BIGINT)".to_string(),
        CoordinateSystem::ZeroBasedHalfOpen => "CAST(start AS BIGINT) + 1".to_string(),
    };
    let mut projection = format!(
        "canonical_contig(chrom) AS chrom, {start_expr} AS start, CAST(\"end\" AS BIGINT) AS \"end\""
    );
    if lookup == LookupKind::Point {
        projection.push_str(", allele_string");
    }
    for col in match_columns.iter().chain(value_columns.iter()) {
        projection.push_str(&format!(", {col}"));
    }
    format!("SELECT {projection} FROM {inner_view}")
}
```

`write.rs`:

```rust
pub fn plugin_output_schema(lookup: LookupKind, matches: &[MatchColumn], values: &[ValueColumn]) -> SchemaRef {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
    ];
    if lookup == LookupKind::Point {
        fields.push(Field::new("allele_string", DataType::Utf8, false));
    }
    ...
```

`dedup.rs`: replace the hard-coded `key_names` with the parameter, and add:

```rust
/// The runtime probe key a shard row must be unique on, in shard order.
pub fn probe_key_columns(lookup: LookupKind, match_columns: &[String]) -> Vec<String> {
    let mut key: Vec<String> = match lookup {
        LookupKind::Point => vec!["start".into(), "allele_string".into()],
        LookupKind::Interval => vec!["start".into(), "end".into()],
    };
    key.extend(match_columns.iter().cloned());
    key
}
```

Update both call sites in `build.rs:491-495` to `let key_cols = probe_key_columns(src.lookup, &match_cols);` and pass `&key_cols`. Update dedup tests to pass the full key list.

`build.rs`: after the `info!("read+normalize+dedup done …")` block and before the memory-pool setup, insert:

```rust
    let out_schema = plugin_output_schema(src.lookup, &src.match_columns, &src.value_columns);
    let plugin_dir = output_cache_root.join("plugin").join(&src.plugin_name);
    std::fs::create_dir_all(&plugin_dir)
        .map_err(|e| DataFusionError::Execution(format!("mkdir {}: {e}", plugin_dir.display())))?;
    let file_name = format!("{}.parquet", canonical_chrom_label(chrom));
    let shard_path = plugin_dir.join(&file_name);
    let build_tmp = plugin_dir.join(format!("{file_name}.build.tmp"));
    let scratch = ScratchGuard::new([build_tmp.clone()]);

    if src.lookup == LookupKind::Interval {
        // No allele → nothing to inherit a tier from. Sort by (start, arrival
        // order) so the shard preserves the tabix/file order VEP iterates, and
        // stamp every row cold.
        let (rows, warm, cold) =
            write_interval_shard(deduped, &out_schema, &build_tmp, chrom, &src.plugin_name).await?;
        info!(
            "plugin_cache[{}/{chrom}]: interval shard written, rows={rows}, {:.1}s total",
            src.plugin_name,
            t_start.elapsed().as_secs_f64()
        );
        return finish_staged(scratch, build_tmp, shard_path, file_name, chrom, rows, warm, cold);
    }
```

and move the existing `out_schema`/`plugin_dir`/`file_name`/`shard_path`/`build_tmp`/`scratch` block (`:546-556`) up so it is declared once (delete the later duplicate). Extract the tail at `:623-658` into

```rust
#[allow(clippy::too_many_arguments)]
fn finish_staged(
    scratch: ScratchGuard,
    build_tmp: PathBuf,
    shard_path: PathBuf,
    file_name: String,
    chrom: &str,
    rows: usize,
    warm: usize,
    cold: usize,
) -> Result<StagedShard> {
    if warm + cold == 0 {
        let _ = std::fs::remove_file(&build_tmp);
        return Ok(StagedShard {
            entry: ChromEntry { chrom: canonical_chrom_label(chrom), file: file_name, rows: 0, warm: 0, cold: 0 },
            staged: None,
            live: shard_path,
        });
    }
    scratch.disarm();
    Ok(StagedShard {
        staged: Some(build_tmp),
        live: shard_path,
        entry: ChromEntry { chrom: canonical_chrom_label(chrom), file: file_name, rows, warm, cold },
    })
}
```

and call it from the point path too (behaviour unchanged). New writer:

```rust
/// Interval shards skip the tier join: concatenate the deduped batches, order
/// by `(start, arrival ordinal)`, append `tier = 1`, and write.
async fn write_interval_shard(
    deduped: Vec<RecordBatch>,
    out_schema: &SchemaRef,
    path: &Path,
    chrom: &str,
    plugin_name: &str,
) -> Result<(usize, usize, usize)> {
    use datafusion::arrow::array::{Int8Array, UInt64Array};
    use datafusion::arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};

    let mut writer = PluginShardWriter::create(path, Arc::clone(out_schema))?;
    let non_empty: Vec<RecordBatch> = deduped.into_iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        let rows = writer.finish()?;
        return Ok((rows, 0, rows));
    }
    let all = concat_batches(&non_empty[0].schema(), &non_empty)
        .map_err(|e| DataFusionError::Execution(format!("concat interval rows: {e}")))?;
    let n = all.num_rows();
    let ordinal: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..n as u64));
    let start_idx = all.schema().index_of("start")?;
    let indices = lexsort_to_indices(
        &[
            SortColumn { values: Arc::clone(all.column(start_idx)), options: None },
            SortColumn { values: ordinal, options: None },
        ],
        None,
    )
    .map_err(|e| DataFusionError::Execution(format!("sort interval rows: {e}")))?;
    let mut columns: Vec<ArrayRef> = all
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| DataFusionError::Execution(format!("take interval rows: {e}")))?;
    let mut fields: Vec<Field> = all.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("tier", DataType::Int8, false));
    columns.push(Arc::new(Int8Array::from(vec![1i8; n])));
    let sorted = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| DataFusionError::Execution(format!("interval batch: {e}")))?;
    let reordered = reproject_cast(&sorted, out_schema)?;
    let tier_idx = out_schema.index_of("tier")?;
    let out_start_idx = out_schema.index_of("start")?;
    inspect_tier_start_order(&reordered, out_start_idx, tier_idx, chrom, plugin_name, None)?;
    writer.write(&reordered)?;
    let rows = writer.finish()?;
    Ok((rows, 0, rows))
}
```

`reproject_cast` already exists in `build.rs` (used at `:327`); it casts `start`/`end` BIGINT → UInt32 and orders columns to `out_schema`. Update the point path to pass `src.lookup` where `plugin_output_schema` / `wrap_normalization` are called.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache`
Expected: PASS, including every pre-existing build/join test (point path unchanged).

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/plugin_cache/{normalize,write,dedup,build,provider}.rs
git commit -m "feat(plugin-cache): build interval shards without allele or tier join"
```

### Task 6: Runtime `IntervalLookup`

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/lookup.rs` (new struct after `PluginBufferSlice`)
- Test: `lookup.rs` `mod tests`

**Interfaces:**
- Produces:
  ```rust
  pub struct IntervalLookup { rows: Vec<IntervalRow>, trees: HashMap<Vec<Option<String>>, COITree<usize, u32>>, n_values: usize }
  struct IntervalRow { values: Vec<PluginScalar> }   // file order == Vec index == tree metadata
  impl IntervalLookup {
      pub async fn open(shard: &Path, match_columns: Vec<String>, value_columns: Vec<String>) -> Result<Self>;
      pub fn probe(&self, span_start: u32, span_end: u32, match_values: &[Option<String>]) -> Option<&[PluginScalar]>;
      pub fn n_values(&self) -> usize;
  }
  ```

- [ ] **Step 1: Write the failing tests**

```rust
    fn write_interval_shard(path: &std::path::Path) {
        use crate::plugin_cache::cache_manifest::LookupKind;
        let matches = vec![MatchColumn { column: "gene_id".into(), template: "{Gene}".into() }];
        let vals = vec![ValueColumn { column: "rat".into(), csq_field: "PO_Rat".into(), ty: ValueType::Utf8, description: None }];
        let schema = plugin_output_schema(LookupKind::Interval, &matches, &vals);
        // Three rows for ENSG1 in file order wide, narrow, late (all cover 160;
        // a tree visits them in its own order, so the ordinal tie-break is what
        // makes "wide" win) and one row for ENSG2 with no value.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1", "1", "1", "1"])),
                Arc::new(UInt32Array::from(vec![100u32, 150, 155, 400])),
                Arc::new(UInt32Array::from(vec![300u32, 200, 350, 500])),
                Arc::new(StringArray::from(vec!["ENSG1", "ENSG1", "ENSG1", "ENSG2"])),
                Arc::new(StringArray::from(vec![Some("wide"), Some("narrow"), Some("late"), None])),
                Arc::new(Int8Array::from(vec![1i8, 1, 1, 1])),
            ],
        )
        .unwrap();
        let mut w = PluginShardWriter::create(path, schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn interval_probe_overlaps_and_gates_on_discriminator() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chr1.parquet");
        write_interval_shard(&path);
        let lk = IntervalLookup::open(&path, vec!["gene_id".into()], vec!["rat".into()]).await.unwrap();
        let g1 = [Some("ENSG1".to_string())];
        let g2 = [Some("ENSG2".to_string())];
        // inside all three ENSG1 spans → first in FILE order, whatever the tree visits first
        assert_eq!(lk.probe(160, 160, &g1).unwrap(), &[PluginScalar::Str("wide".into())]);
        // only "late" covers 320..350
        assert_eq!(lk.probe(320, 320, &g1).unwrap(), &[PluginScalar::Str("late".into())]);
        // boundaries are inclusive
        assert_eq!(lk.probe(300, 300, &g1).unwrap(), &[PluginScalar::Str("wide".into())]);
        assert_eq!(lk.probe(99, 100, &g1).unwrap(), &[PluginScalar::Str("wide".into())]);
        assert!(lk.probe(351, 351, &g1).is_none());
        // an insertion span [pos, pos+1] straddling a start boundary
        assert_eq!(lk.probe(399, 400, &g2).unwrap(), &[PluginScalar::Null]);
        // discriminator gate
        assert!(lk.probe(160, 160, &[Some("ENSG9".to_string())]).is_none());
        assert!(lk.probe(160, 160, &[None]).is_none());
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::lookup::tests::interval`
Expected: compile error, `IntervalLookup` undefined.

- [ ] **Step 3: Implement** (append to `lookup.rs`)

```rust
/// One interval row's values, in shard column order. Its position in
/// `IntervalLookup::rows` is the file ordinal, which is also the COITree
/// metadata, so the tie-break "first record in file order" is a `min` over
/// the ordinals the tree reports.
struct IntervalRow {
    values: Vec<PluginScalar>,
}

/// Whole-shard interval lookup for [`LookupKind::Interval`] plugins: one
/// `COITree` per discriminator tuple over closed 1-based spans, read once at
/// open and probed synchronously. `probe` returns the overlapping row with the
/// smallest file ordinal, which is the record a tabix-backed Ensembl plugin
/// takes. A plugin with no match columns has a single tree.
pub struct IntervalLookup {
    rows: Vec<IntervalRow>,
    trees: HashMap<Vec<Option<String>>, COITree<usize, u32>>,
    n_values: usize,
}

impl IntervalLookup {
    pub async fn open(shard: &Path, match_columns: Vec<String>, value_columns: Vec<String>) -> Result<Self> {
        let file = tokio::fs::File::open(shard).await.map_err(|e| {
            DataFusionError::Execution(format!("open plugin shard '{}': {e}", shard.display()))
        })?;
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|e| DataFusionError::Execution(format!("open interval shard: {e}")))?;
        let arrow_schema = builder.schema().clone();
        let mut wanted = vec!["start".to_string(), "end".to_string()];
        wanted.extend(match_columns.iter().cloned());
        wanted.extend(value_columns.iter().cloned());
        let roots: Vec<usize> = wanted
            .iter()
            .map(|n| arrow_schema.index_of(n).map_err(|e| DataFusionError::Execution(format!("interval shard column {n}: {e}"))))
            .collect::<Result<_>>()?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        let mut stream = builder
            .with_projection(mask)
            .with_batch_size(8192)
            .build()
            .map_err(|e| DataFusionError::Execution(format!("build interval stream: {e}")))?;
        let n_match = match_columns.len();
        let n_values = value_columns.len();
        let mut rows: Vec<IntervalRow> = Vec::new();
        let mut intervals: HashMap<Vec<Option<String>>, Vec<Interval<usize>>> = HashMap::new();
        while let Some(b) = stream.try_next().await.map_err(|e| DataFusionError::Execution(format!("read interval batch: {e}")))? {
            // Projection keeps file column order: start, end, match…, value…
            let schema = b.schema();
            let col = |name: &str| -> Result<usize> { schema.index_of(name).map_err(|e| DataFusionError::Execution(e.to_string())) };
            let start = b.column(col("start")?).as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| DataFusionError::Execution("start column must be UInt32".into()))?;
            let end = b.column(col("end")?).as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| DataFusionError::Execution("end column must be UInt32".into()))?;
            let match_idx: Vec<usize> = match_columns.iter().map(|n| col(n)).collect::<Result<_>>()?;
            let value_idx: Vec<usize> = value_columns.iter().map(|n| col(n)).collect::<Result<_>>()?;
            for r in 0..b.num_rows() {
                let mut key = Vec::with_capacity(n_match);
                for &i in &match_idx {
                    key.push(string_value(b.column(i).as_ref(), r)?);
                }
                let mut values = Vec::with_capacity(n_values);
                for &i in &value_idx {
                    values.push(decode_scalar(b.column(i).as_ref(), r)?);
                }
                let ordinal = rows.len();
                rows.push(IntervalRow { values });
                // Closed 1-based span; positions fit i32 for every contig.
                intervals.entry(key).or_default().push(Interval::new(
                    i32::try_from(start.value(r)).unwrap_or(i32::MAX),
                    i32::try_from(end.value(r)).unwrap_or(i32::MAX),
                    ordinal,
                ));
            }
        }
        let trees = intervals
            .into_iter()
            .map(|(key, ivs)| (key, COITree::new(&ivs)))
            .collect();
        Ok(Self { rows, trees, n_values })
    }

    pub fn n_values(&self) -> usize {
        self.n_values
    }

    /// First row (file order) whose inclusive span overlaps `[span_start, span_end]`
    /// under the given discriminators; `None` on a miss or a `None` discriminator
    /// that the shard stores as `Some`.
    pub fn probe(&self, span_start: u32, span_end: u32, match_values: &[Option<String>]) -> Option<&[PluginScalar]> {
        let tree = self.trees.get(match_values)?;
        let (first, last) = (
            i32::try_from(span_start).unwrap_or(i32::MAX),
            i32::try_from(span_end).unwrap_or(i32::MAX),
        );
        let mut best: Option<usize> = None;
        tree.query(first, last, |node| {
            let ordinal = *GenericInterval::<usize>::metadata(node);
            best = Some(best.map_or(ordinal, |b| b.min(ordinal)));
        });
        best.map(|i| self.rows[i].values.as_slice())
    }
}
```

Imports: `use coitrees::{COITree, GenericInterval, Interval, IntervalTree};` (the `IntervalTree` trait provides `query`). `HashMap::get` with a `&[Option<String>]` key works because `Vec<T>: Borrow<[T]>`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::lookup`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/plugin_cache/lookup.rs
git commit -m "feat(plugin-cache): IntervalLookup, per-discriminator COITrees probed by variant span"
```

### Task 7: Registry branch and `probe_all` span

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/registry.rs:18-31` (`PluginEntry`), `:131-200` (`open`), `:281-302` (`take_buffer_all`), `:305-366` (`SliceEntry`, `probe_all`)
- Test: `registry.rs` `mod tests`

**Interfaces:**
- `enum LookupHandle { Point(PluginLookup), Interval(Arc<IntervalLookup>) }` (private), `PluginEntry.lookup: Option<LookupHandle>`.
- `BufferSlices::probe_all(&self, start: u32, allele_string: &str, fallback_key: Option<(u32, &str)>, span: (u32, u32), attrs: &[Option<&str>]) -> Vec<PluginScalar>` — `span` is the variant's VEP-normalised inclusive span with `span.0 <= span.1` (the caller swaps insertion coordinates).

- [ ] **Step 1: Write the failing test** (in `registry.rs` tests; build the interval shard with the same helper as Task 6 — move `write_interval_shard` into a `pub(crate)` test helper module in `lookup.rs`, e.g. `pub(crate) mod test_support`)

```rust
    #[tokio::test(flavor = "multi_thread")]
    async fn interval_plugin_probes_by_span_and_gene() {
        use crate::plugin_cache::cache_manifest::LookupKind;
        use crate::plugin_cache::lookup::test_support::write_interval_shard;
        let dir = tempfile::tempdir().unwrap();
        let cache_root = dir.path();
        let plugin_dir = cache_root.join("plugin").join("po");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        write_interval_shard(&plugin_dir.join("chr1.parquet"));
        CacheManifest {
            plugin_name: "po".into(),
            source_manifest: "po.source.toml".into(),
            key_columns: crate::plugin_cache::cache_manifest::key_columns(LookupKind::Interval),
            match_columns: vec![MatchColumnRecord { column: "gene_id".into(), template: "{Gene}".into() }],
            value_columns: vec![ValueColumnRecord { column: "rat".into(), csq_field: "PO_Rat".into(), ty: "Utf8".into(), description: None }],
            chroms: vec![ChromEntry { chrom: "chr1".into(), file: "chr1.parquet".into(), rows: 3, warm: 0, cold: 3 }],
            sources: vec![],
            cache_source_version: None,
            allele_match: Default::default(),
            field_order: Default::default(),
            assume_unique: Some(true),
            lookup: LookupKind::Interval,
        }
        .write(&plugin_dir)
        .unwrap();

        let reg = PluginRegistry::open(cache_root, "1", None).await.unwrap();
        assert_eq!(reg.csq_fields(), vec!["PO_Rat"]);
        // Interval plugins ignore the buffer take; an empty start list is fine.
        let slices = reg.take_buffer_all(&[]).await.unwrap();
        let ns = build_attr_namespace(
            "intron_variant", "ENSG1", "SYM", "Transcript", "ENST1", "lncRNA",
            "", "", "", "", "", "", "", "A", "G",
        );
        assert_eq!(slices.probe_all(160, "A/G", None, (160, 160), &ns), vec![PluginScalar::Str("wide".into())]);
        assert_eq!(slices.probe_all(350, "A/G", None, (350, 350), &ns), vec![PluginScalar::Null]);
        let other = build_attr_namespace(
            "intron_variant", "ENSG2", "SYM", "Transcript", "ENST2", "lncRNA",
            "", "", "", "", "", "", "", "A", "G",
        );
        assert_eq!(slices.probe_all(160, "A/G", None, (160, 160), &other), vec![PluginScalar::Null]);
        // No transcript → empty namespace → discriminator None → miss.
        assert_eq!(slices.probe_all(160, "A/G", None, (160, 160), &[]), vec![PluginScalar::Null]);
    }
```

`build_attr_namespace` (`template.rs:96`) takes exactly these 15 `&str` arguments in this order: consequence, gene, symbol, feature_type, feature, biotype, hgvsc, hgvsp, cdna_pos, cds_pos, protein_pos, amino_acids, codons, ref_allele, alt_allele; empty strings become `None`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::registry::tests::interval`
Expected: FAIL (compile: `probe_all` arity / `lookup` field type).

- [ ] **Step 3: Implement**

`registry.rs`:

```rust
use std::sync::Arc;
use crate::plugin_cache::cache_manifest::LookupKind;
use crate::plugin_cache::lookup::{IntervalLookup, PluginBufferSlice, PluginLookup, PluginScalar};

enum LookupHandle {
    Point(PluginLookup),
    Interval(Arc<IntervalLookup>),
}

struct PluginEntry {
    ...
    lookup: Option<LookupHandle>,
}
```

In `open`, replace the `Some(PluginLookup::open(...).await?)` with:

```rust
                    Some(match m.lookup {
                        LookupKind::Point => LookupHandle::Point(
                            PluginLookup::open(&shard, match_columns, value_columns).await?,
                        ),
                        LookupKind::Interval => LookupHandle::Interval(Arc::new(
                            IntervalLookup::open(&shard, match_columns, value_columns).await?,
                        )),
                    })
```

`take_buffer_all`:

```rust
        for p in &self.plugins {
            let (slice, interval) = match &p.lookup {
                Some(LookupHandle::Point(lk)) => {
                    let batch = lk.take_buffer(sorted_unique_starts).await?;
                    (Some(PluginBufferSlice::from_batch(&batch, p.n_match, p.n_values)?), None)
                }
                Some(LookupHandle::Interval(il)) => (None, Some(Arc::clone(il))),
                None => (None, None),
            };
            entries.push(SliceEntry { csq_fields_len: p.csq_fields.len(), emit_order: p.emit_order.clone(), allele_match: p.allele_match, match_templates: p.match_templates.clone(), slice, interval });
        }
```

`SliceEntry` gains `interval: Option<Arc<IntervalLookup>>`. `probe_all` gains `span: (u32, u32)` after `fallback_key`, and the hit computation becomes:

```rust
            let hit: Option<Vec<PluginScalar>> = if let Some(il) = &e.interval {
                il.probe(span.0, span.1, &match_values).map(|v| v.to_vec())
            } else {
                e.slice.as_ref().and_then(|s| s.probe(start, allele_string, &match_values)).or_else(|| { /* existing minimised fallback */ })
            };
```

Update the existing registry tests that call `probe_all` to pass a span (`(start, start)`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- plugin_cache::registry`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/plugin_cache/{registry,lookup}.rs
git commit -m "feat(plugin-cache): registry opens interval lookups; probe_all takes the variant span"
```

### Task 8: Wire the variant span into `annotate_provider`

**Files:**
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs:7005-7016` (per-transcript probe) and `:7068-7077` (placeholder probe)
- Test: `datafusion/bio-function-vep/src/annotate_provider.rs` existing `test_csq_field_projection_*` unaffected; new integration test in `tests/` is covered by vepyr's fixture (Task V2 of the vepyr plan). Add one unit test for the span helper.

**Interfaces:**
- Produces: `pub(crate) fn plugin_probe_span(vcf_pos: i64, ref_allele: &str, alt_allele: &str) -> (u32, u32)` in `allele.rs`, next to `vep_norm_end` (`allele.rs:981`).

- [ ] **Step 1: Write the failing test** (in `allele.rs` tests)

```rust
    #[test]
    fn plugin_probe_span_is_vep_span_with_insertions_swapped() {
        // SNV
        assert_eq!(plugin_probe_span(100, "A", "G"), (100, 100));
        // deletion CT>C: VEP start 101, end 101
        assert_eq!(plugin_probe_span(100, "CT", "C"), (101, 101));
        // insertion C>CT: VEP start 101, end 100 → swapped [100, 101]
        assert_eq!(plugin_probe_span(100, "C", "CT"), (100, 101));
        // MNV
        assert_eq!(plugin_probe_span(100, "AC", "GT"), (100, 101));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- allele::tests::plugin_probe_span`
Expected: compile error.

- [ ] **Step 3: Implement**

`allele.rs`:

```rust
/// The genomic span a tabix-backed Ensembl plugin queries for a variant: the
/// VEP-normalised `[start, end]`, with an insertion's `start > end` swapped the
/// way `PhenotypeOrthologous.pm` and `ReferenceQuality.pm` do before calling
/// `get_data`. Inclusive, 1-based, clamped to `u32`.
pub(crate) fn plugin_probe_span(vcf_pos: i64, ref_allele: &str, alt_allele: &str) -> (u32, u32) {
    let s = vep_norm_start(vcf_pos, ref_allele, alt_allele);
    let e = vep_norm_end(vcf_pos, ref_allele, alt_allele);
    let (lo, hi) = if s > e { (e, s) } else { (s, e) };
    (u32::try_from(lo).unwrap_or(0), u32::try_from(hi).unwrap_or(0))
}
```

`annotate_provider.rs`, per-transcript block: before `let scalars = plugin_slices…` add

```rust
                            let probe_span = plugin_probe_span(start_val, &ref_al, &alt_allele);
```

and pass `probe_span` as the new fourth argument of `s.probe_all(...)`. Same two lines in the placeholder block. Import `plugin_probe_span` alongside `plugin_probe_input_allele`.

- [ ] **Step 4: Run to verify**

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache`
Expected: PASS (whole crate).

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-function-vep/src/{allele,annotate_provider}.rs
git commit -m "feat(vep): probe interval plugins with the variant's VEP span"
```

### Task 9: PR 2 hygiene, docs, draft PR

**Files:**
- Modify: `datafusion/bio-function-vep/src/plugin_cache/mod.rs` (module doc: two lookup kinds), `datafusion/bio-function-vep/examples/build_plugin.rs` doc (interval builds still need `--variation-cache-dir` to enumerate chromosomes; no tier join runs)
- Modify: `docs/superpowers/specs/2026-07-05-custom-vep-plugin-caches-design.md:582-598` (§9): replace the sentence declaring per-interval plugins secondary with a pointer: "Interval plugins are supported since PR #<n> via `lookup = \"interval\"`; see vepyr's 2026-09-11 spec."

- [ ] **Step 1: fmt, clippy, full tests**

Run: `cargo fmt --all && cargo clippy -p datafusion-bio-function-vep --features parquet-cache --all-targets -- -D warnings && cargo test -p datafusion-bio-function-vep --features parquet-cache`
Expected: clean.

- [ ] **Step 2: Golden regression** (the existing benchmark test that pins CSQ layouts must be untouched)

Run: `cargo test -p datafusion-bio-function-vep --features parquet-cache -- golden`
Expected: PASS.

- [ ] **Step 3: Commit and open the stacked draft PR**

```bash
git add -A datafusion/bio-function-vep/src/plugin_cache/mod.rs datafusion/bio-function-vep/examples/build_plugin.rs docs/superpowers/specs/2026-07-05-custom-vep-plugin-caches-design.md
git commit -m "docs(plugin-cache): document lookup = interval"
git push -u origin feat/plugin-interval-lookup
gh pr create --draft --base feat/plugin-gff-provider --title "feat(plugin-cache): interval lookup kind for span-keyed plugins" --body-file <(cat <<'EOF'
Adds `lookup = "interval"` to plugin manifests. Interval shards carry `(chrom, start, end, <match…>, <values…>, tier=1)`, skip the variation tier join, and are probed at runtime through per-discriminator COITrees by overlap with the variant's VEP-normalised span (first row in file order wins, as a tabix-backed Ensembl plugin returns records).

Second half of the PhenotypeOrthologous port (gene-keyed via `{Gene}`). Stacked on #<PR1>. Spec: vepyr `docs/superpowers/specs/2026-09-11-gff-plugin-source-and-phenotypeorthologous-design.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01Ybip12LHW1qoorZjs3TwYT
EOF
)
```

- [ ] **Step 4: Hand off** — record both PR numbers and head SHAs in the vepyr plan's Task V1 (the pin), then drive the bot review loop per the vepyr-fix skill ("@codex review" / "@claude review", gate every reply on `git rev-parse HEAD` changing).
