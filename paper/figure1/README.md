# Figure 1

Editable multipanel figure with a monochrome layout inspired by the architecture diagrams in [polars-bio Figure 1A/Z](https://academic.oup.com/bioinformatics/article/41/12/btaf640/8362264). Geometry and content are original to vepyr.

## Files and rebuilding

- `figure1.drawio`: editable native draw.io cells; Panel C is an embedded Python-generated SVG.
- `figure1.svg`, `figure1.png`, `figure1.pdf`: matching vector and review exports.
- `build_figure.py`: diagram layout and draw.io/SVG generation.
- `build_performance.py`: benchmark ingestion and SVG plotting using the Python standard library.
- `CAPTION.md`: draft manuscript caption.
- `data/`: two header-only templates for missing measurements.

From this directory:

```bash
make                 # generate draw.io + SVG; validate geometry and XML
make preview         # also render PNG/PDF with headless Chrome/Chromium
make drawio-export   # alternative native export, requires draw.io Desktop
```

No Python packages are required. XML validation uses `xmllint`. The preview renderer supports Chrome/Chromium on PATH and the standard macOS Chrome application path. Native draw.io export supports `drawio` on PATH or the standard macOS application path. draw.io Desktop is not installed on the authoring machine: the supplied PNG/PDF use Chrome, and native draw.io rendering has not been visually verified. Manual export is not required.

Make lasting changes in the Python builders. Regeneration overwrites the draw.io file, so preserve a separate copy before editing it manually. Only final outputs are retained here; intermediate charts are embedded directly.

## Architecture and cache evidence

Implementation snapshot: vepyr `1989fe7b14bd7bcba7ceb36e551b24b926c52da8`, bio-function-vep v0.20.1 (`8b1abd09478b97aed6ce42d2a5b60912b315855b`), Parquet 58.0.0. Where older prose differs from the pinned writer, the figure follows the code.

- A1 labels the Polars interface as DataFrame API / SQL and separates annotation dataflow from the dashed query-demand return. Selected columns and required predicate inputs determine HGVS, known-variant, CSQ and plugin work. Supported genomic predicates become regions; a requested row count becomes a limit. AF/consequence predicates still filter annotated batches, not arbitrary engine internals. See [the Python API](https://github.com/biodatageeks/vepyr/blob/1989fe7b14bd7bcba7ceb36e551b24b926c52da8/src/vepyr/__init__.py) and local `docs/dataframes.md`. This interface label is deliberately broader than Polars' [expression API](https://docs.pola.rs/api/python/stable/reference/expressions/index.html).
- A2 magnifies the selected cache box; A3 magnifies the selected `variation/chr1.parquet` file. Variation accounts for 36.7 GiB of the 41.9 GiB local `115_GRCh38_merged` cache (87.6%, allocated disk space). This local historical-cache observation is not a universal format size claim.
- A2 gives both builders equal visibility: `vepyr.build_cache()` converts Ensembl Storable/Sereal cache data, and `vepyr.build_plugin_cache()` ingests TSV, CSV, Parquet, VCF and BED. The BED provider exposes four columns (`chrom`, `start`, `end`, `name`); the label does not imply full BED12 feature support. Both arrows terminate at Parquet shards, meaning a common storage format with separate core/plugin schemas and directories, not a merged table.
- The plugin build is summarized as declarative TOML + SQL ingestion + schema/output-field mapping. SQL selects and transforms raw-source columns; the engine supplies normalization, tier assignment and runtime matching. This describes vepyr's data-backed plugins, not a claim to replace every arbitrary-computation plugin supported by VEP. The contrast with Perl modules is specific to the [Ensembl VEP plugin API](https://mart.ensembl.org/info/docs/tools/vep/script/vep_plugins.html), not VEP's separate custom-annotation interface. See local `docs/plugins.md`, `tests/test_build_plugin_cache.py`, `src/lib.rs` and `src/vepyr/__init__.py`.
- Detailed plugin parameters are intentionally omitted from the figure. The declarative input specification (`*.source.toml`) and generated provenance manifest (`manifest.json`) are different artifacts. The latter still pins the requested ref/resolved commit and records source URLs, declared/verified checksums and input/index fingerprints. It is a cache-level JSON sidecar, distinct from core context entities' row-level provenance columns. In the figure, `<name>` denotes a plugin-specific directory, not a measured example build.
- A3 shows typed variation keys, encoded annotation payload and file-level release/source identity in Arrow metadata. Variation does not carry the transcript-style provenance column block.
- Warm/cold denotes an on-disk tier, not OS page-cache state. Warm positions have maximum global AF ≥ 1%, expanded by a one-base radius. Warm and cold runs are sorted by start within one file; tier boundaries need not coincide with row-group boundaries. Both tiers are queried. Plugin rows inherit variation tiers through key matching; unmatched rows are cold.
- Lookup resolves candidate pages from the page directory/indexes, reads positions to locate exact rows, then takes projected payload. Transcript context instead uses a projected scan and an interval index. See the pinned [cache builder](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/cache/build.rs), [Parquet writer](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/parquet_cache/write.rs) and [reader](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/parquet_cache/scan.rs).

### Writer defaults versus existing files

| Current writer profile | Entities | Row-group cap | Dictionary | Page targets |
|---|---|---:|---|---|
| Point lookup | variation, translation_sift | 1,000,000 rows | off | 4 KiB / 512 rows |
| Context scan | transcript, exon, translation_core, regulatory, motif | 50,000 rows | on | Parquet defaults: 1 MiB / 20,000 rows |

Both profiles use ZSTD level 3. Page sizes are best-effort targets, not exact encoded or compressed sizes; leaf-column page boundaries can differ. Large groups amortize metadata, small indexed pages reduce point-lookup decoding, and smaller context groups bound buffering for wide rows. A controlled parameter sweep remains pending; the figure does not claim empirically optimal settings.

Existing cache files need not use these defaults. PyArrow metadata inspection of local release-115 chr1 shards (Ensembl, RefSeq and merged) found full groups of 100,000 rows for variation, 8,000 for transcript, 45,000 for exon and 6,000 for translation_core, with dictionary encoding present. The figure labels its settings as current code defaults, not physical settings of those historical files or all benchmark caches.

## Validation and benchmarks

Panel B separates component tests, ported behavior fixtures and end-to-end checks. The six qualified release/source reports in `e2e-testing/reports/fast_chr1_chr22_*_summary_*.md` each compare 4,096,123 normalized autosomal variants, with zero archived field-gate mismatches: Ensembl 80 fields, RefSeq 85, merged 86, for releases 115 and 116. Strict/canonical VCF-body MD5 is a separate harness capability; these reports are not relabeled as MD5 runs.

Panel C reads repository summaries:

- VEP: `performance-tests/vep/outputs/116/merged_fork_scaling/summary.tsv`, `elapsed_seconds`.
- vepyr: `performance-tests/vepyr/outputs/116/merged_worker_scaling/summary.tsv`, `process_elapsed_wall` (not the narrower `annotation_seconds`).

At eight forks/workers, process wall times are 6,326 s and 187.67 s: 33.7-fold. These are preliminary single runs, not a paired replicated study. The separate macOS M2 chr22/release-115 VEP run is not mixed with release-116 whole-genome measurements or shown as a paired vepyr comparison.

Two intentional placeholders accept tab-separated rows; repeats are summarized by their median:

- `data/macos_merged.tsv`: `tool`, `parallelism`, `seconds`, `replicate`.
- `data/filter_benchmark.tsv`: `pipeline`, `seconds`, `peak_rss_mb`, `replicate`; labels `VEP + filter_vep` and `vepyr + SQL`.

Final benchmarks need matched inputs, release/cache, annotations, output semantics, storage, OS page-cache state and timing boundaries, with repeats and dispersion. OS cold/warm benchmarking is distinct from A3's static variation tiers. The optional single-thread cache-type comparison is omitted pending comparable measurements.

Panel D's rare/unknown AF example retains whole variant rows. It intentionally omits `filter_vep --only_matched`, which changes consequence-level output semantics. Per-consequence filters require aligned list expansion or explicit reduction; ontology expansion must also be explicit. See `docs/dataframes.md` and the [Ensembl filter documentation](https://mart.ensembl.org/info/docs/tools/vep/script/vep_filter.html).
