# Plugins

vepyr supports external per-variant annotation databases as **plugins**: a raw
source (TSV/CSV/Parquet) is converted into a frequency-tiered, per-chromosome
Parquet cache whose values are emitted as extra VEP CSQ output fields.
AlphaMissense is supported today; more are planned.

Plugin manifests live in the public [vepyr-plugins](https://github.com/biodatageeks/vepyr-plugins)
repository and are selected by **plugin name + git tag**, so different plugins can
be pinned to different releases.

## Building a plugin cache

```python
import vepyr

vepyr.build_plugin_cache(
    plugin="alphamissense",          # dir in vepyr-plugins
    version="v0.2.0",                # git tag of vepyr-plugins for THIS plugin's manifest
    source_path="AlphaMissense_hg38.tsv.gz",  # the raw source DATA (not in vepyr-plugins)
    cache_dir="/data/115_GRCh38_merged",      # existing Ensembl variation cache (supplies tier)
    plugin_cache_root="/data/plugin_cache",   # output: plugin/<name>/chr*.parquet + manifest.json
    chroms=None,                     # None = all chroms present under <cache_dir>/variation/
    plugins_repo=None,               # optional local clone of vepyr-plugins for OFFLINE builds
)
```

One call builds one plugin at one version. To combine plugins at different
versions (e.g. AlphaMissense `v0.2.0` + ClinVar `v0.3.0`), call
`build_plugin_cache` once per plugin into the **same** `plugin_cache_root`.

The manifest is resolved from the public vepyr-plugins repo at `version`
(cloned on demand), or from a local clone via `plugins_repo` for fully offline
builds. Tiering (warm/cold) is **inherited from the variation cache** at
`cache_dir` — plugins declare no tier policy of their own.

## Annotating with plugins

Point `annotate()` at the built cache root; the plugin CSQ fields appear
automatically (header + per-transcript body). `plugin_cache_root=None` (default)
is byte-identical to a plugin-free run.

```python
vepyr.annotate(
    "sample.vcf",
    "/data/115_GRCh38_merged",
    everything=True,
    reference_fasta="Homo_sapiens.GRCh38.dna.primary_assembly.fa",
    plugin_cache_root="/data/plugin_cache",
    output_vcf="sample.annotated.vcf",
)
```

## Manifest structure

A plugin's `plugins/<name>/<name>.source.toml` declares how to ingest the raw
source and map it to CSQ fields.

!!! note "TOML ordering"
    Top-level scalar keys (`plugin_name`, `coordinate_system`, `ingest_sql`)
    MUST precede any `[[table]]` header, or TOML absorbs them into the preceding
    table.

- **`plugin_name`** — plugin identifier (also the cache dir name).
- **`coordinate_system`** — `"1-based"` or `"0-based-half-open"` (drives the
  build-time coordinate shift to the variation cache's 1-based convention).
- **`ingest_sql`** — a `SELECT` over the raw source view `plugin_<name>_src`
  that MUST project the fixed key columns `chrom`, `start`, `end`,
  `allele_string` (`ref/alt`), plus any discriminator column(s) and the value
  column(s).
- **`[[source]]`** — the raw source file(s). `provider` is one of the recognized
  raw-source types `csv`, `tsv`, `parquet`, `vcf`, `bed` (see
  [Table providers](#build-pipeline-table-providers-tables-views) for which are
  wired today — `csv`/`tsv`/`parquet` are implemented; `vcf`/`bed` are recognized
  but not yet wired). `path` is overridden at build time by `source_path`. A
  `[source.csv]` block (for `csv`/`tsv`) declares `delimiter`, `has_header`,
  `comment`, `compression`, and an ordered `schema` of `{name, type}`. Multiple
  `[[source]]` blocks with a `part` key register as `plugin_<name>_src_<part>`.
- **`[[match_column]]`** *(optional, 0+)* — a per-transcript discriminator:
  `column` (the stored discriminator column) + `template` (built at runtime from
  the engine-attribute namespace, see below). Omit entirely for per-variant
  plugins (the value is emitted on every transcript line).
- **`[[value_columns]]`** *(1+)* — `column`, `csq_field` (output field name),
  `type` (`Utf8` / `Float32` / `Int32`). **Declaration order = CSQ output order.**

There is **no `[tier]` block** — tiering is inherited from the variation cache.

### Example: AlphaMissense (per-transcript)

```toml
plugin_name       = "alphamissense"
coordinate_system = "1-based"
ingest_sql = """
SELECT chrom,
       CAST(pos AS INT) AS start,
       CAST(pos AS INT) AS end,
       concat(ref, '/', alt) AS allele_string,
       protein_variant AS protein_variant,
       CAST(am_pathogenicity AS FLOAT) AS am_pathogenicity,
       am_class AS am_class
FROM plugin_alphamissense_src
"""

[[source]]
provider = "tsv"
path = "AlphaMissense_hg38.tsv.gz"
  [source.csv]
  delimiter   = "\t"
  has_header  = false
  comment     = "#"
  compression = "gzip"
  schema = [
    { name = "chrom",            type = "Utf8" },
    { name = "pos",              type = "Utf8" },
    { name = "ref",              type = "Utf8" },
    { name = "alt",              type = "Utf8" },
    { name = "genome",           type = "Utf8" },
    { name = "uniprot_id",       type = "Utf8" },
    { name = "transcript_id",    type = "Utf8" },
    { name = "protein_variant",  type = "Utf8" },
    { name = "am_pathogenicity", type = "Utf8" },
    { name = "am_class",         type = "Utf8" },
  ]

[[match_column]]
column   = "protein_variant"
template = "{ref_aa}{Protein_position}{alt_aa}"

[[value_columns]]
column = "am_class"
csq_field = "am_class"
type = "Utf8"

[[value_columns]]
column = "am_pathogenicity"
csq_field = "am_pathogenicity"
type = "Float32"
```

### Example: a per-variant plugin (no discriminator)

```toml
plugin_name       = "demo_score"
coordinate_system = "1-based"
ingest_sql = """
SELECT chrom, CAST(pos AS INT) AS start, CAST(pos AS INT) AS end,
       concat(ref, '/', alt) AS allele_string, CAST(score AS FLOAT) AS demo_score
FROM plugin_demo_score_src
"""

[[source]]
provider = "tsv"
path = "demo.tsv.gz"
  [source.csv]
  delimiter = "\t"
  has_header = false
  compression = "gzip"
  schema = [
    { name = "chrom", type = "Utf8" }, { name = "pos", type = "Utf8" },
    { name = "ref", type = "Utf8" }, { name = "alt", type = "Utf8" },
    { name = "score", type = "Utf8" },
  ]

[[value_columns]]
column = "demo_score"
csq_field = "DEMO_SCORE"
type = "Float32"
```

## Engine-attribute namespace

A `[[match_column]].template` may reference these per-consequence attributes
(the values the transcript engine computes for CSQ output). Each is optional:
**if any attribute a template references is absent, the discriminator is empty
and the plugin emits no value for that transcript line** — this is how
missense-only gating works (a non-missense consequence has no amino-acid change).

| Attribute | Description |
|---|---|
| `Consequence` | Consequence type(s) for the transcript (e.g. `missense_variant`). |
| `Gene` | Ensembl gene stable ID. |
| `Feature_type` | Feature type (e.g. `Transcript`). |
| `Feature` | Transcript stable ID — the transcript-id discriminator (e.g. dbNSFP). |
| `BIOTYPE` | Transcript biotype (e.g. `protein_coding`). |
| `HGVSc` | HGVS coding-sequence notation. |
| `HGVSp` | HGVS protein notation. |
| `cDNA_position` | Position in cDNA. |
| `CDS_position` | Position in the CDS. |
| `Protein_position` | 1-based amino-acid position. |
| `Amino_acids` | Reference/alternate amino acids as `ref/alt` (e.g. `W/R`); single value when unchanged. |
| `Codons` | Reference/alternate codons. |
| `ref_aa` | Reference amino acid (left of `/` in `Amino_acids`). |
| `alt_aa` | Alternate amino acid (right of `/` in `Amino_acids`). |
| `ref` | VCF reference allele. |
| `alt` | VCF alternate allele. |

Examples: AlphaMissense (amino-acid change) `template = "{ref_aa}{Protein_position}{alt_aa}"`
→ `W320R`; a transcript-keyed plugin `template = "{Feature}"`.

A new attribute is added upstream only when a plugin needs a value not already
listed here — the common discriminators are all present, so most plugins are
manifest-only.

## Cache format & lookup internals

A plugin cache is a set of per-chromosome Parquet shards
(`plugin/<name>/chr*.parquet`) plus a `manifest.json`. The shards use the same
point-lookup-optimized layout as the Ensembl variation cache, so a lookup reads
only the handful of pages that could contain the queried positions — never the
whole file.

### Shard schema

Columns, in order: the key columns `chrom` (Utf8), `start` / `end` (UInt32,
1-based), `allele_string` (Utf8, `ref/alt`); then any **match-discriminator**
column(s) (e.g. `protein_variant`); then the **value** columns (the CSQ fields);
then a derived **`tier`** column (Int8: `0` = warm, `1` = cold). The variation
frequency columns used to compute the tier are **not** stored — only the tier
survives.

### Parquet storage

The writer properties (shared with the variation cache) are tuned for
random point lookups rather than scans:

| Property | Value | Why |
|---|---|---|
| Compression | ZSTD, level 3 | Good ratio; fast enough to decode per page. |
| Dictionary encoding | **disabled** | Avoids a per-take dictionary load; ZSTD recovers the ratio (the no-dict file is actually smaller). |
| Data page size | ≤ 4 KiB | Small pages → fine-grained page index → a lookup touches minimal bytes. |
| Data page row count | ≤ 512 rows | Same — bounds how many rows a single page decode yields. |
| Statistics | **Page-level** | Emits `ColumnIndex` + `OffsetIndex` in the footer — the read-side position→page directory. |
| Row group size | 1,000,000 rows | Large groups keep footer/metadata overhead low; page index gives intra-group resolution. |
| Sorting columns | `(tier, start)` | Physical clustering: warm rows first, then cold, each ascending by `start`. |

### Page index → the `PageDir`

Because page-level statistics are enabled, each shard's footer carries a
`ColumnIndex` (per-page min/max of `start`) and an `OffsetIndex` (per-page byte
offset + row range). At open time the lookup builds a **`PageDir`** over the
`start` leaf column from these indexes. Resolving a set of query positions to the
minimal set of candidate page row-ranges is then a metadata-only operation — no
column data is read until the ranges are known.

### Row groups

Shards use 1,000,000-row row groups. Row groups bound the footer metadata size;
within a group the small (≤512-row) pages plus the page index provide the actual
point-lookup resolution. A whole chromosome is typically one or a few row groups.

### Tiering — how warm/cold is calculated

Tiering clusters common variants together on disk so a batch of nearby query
positions touches fewer, denser pages. The tier is **inherited from the Ensembl
variation cache**, not recomputed per plugin:

- The variation cache marks a genomic `start` **warm (tier 0)** when its maximum
  global allele frequency is **≥ 0.01** (`WARM_AF_THRESHOLD`), within a ±1
  position radius (`WARM_POSITION_RADIUS`); otherwise **cold (tier 1)**.
- At build time the plugin rows are `LEFT JOIN`-ed onto the variation shard on
  `(chrom, start, allele_string)` and take `COALESCE(v.tier, 1)` — i.e. a plugin
  row inherits the variation record's tier, and **any position with no variation
  match is cold**. (Saturation predictors like AlphaMissense are therefore almost
  entirely cold, since most possible substitutions are not common variants.)
- The shard is written **warm rows first, then cold**, each pass sorted by
  `start`, matching the `(tier, start)` physical sort.

Plugins declare **no** tier policy of their own — there is no `[tier]` block in
the manifest.

### Build pipeline — table providers, tables & views

The per-chromosome build registers the raw source and then transforms it through
a short chain of SQL objects:

1. **Table** — the raw source file is registered as a DataFusion **table**
   `plugin_<name>_src` (or `plugin_<name>_src_<part>` for multi-file sources) via
   the matching provider (below).
2. **Ingest view** — `plugin_<name>_ingest`, a `CREATE OR REPLACE VIEW` wrapping
   the manifest's `ingest_sql` (maps raw columns → the key/discriminator/value
   columns).
3. **Normalized view** — `plugin_<name>_norm`, which applies the `canonical_contig`
   UDF and the coordinate shift (to the variation cache's 1-based convention) and
   filters to the target chromosome.
4. The normalized view is tier-joined against the variation shard and the result
   is written to the Parquet shard.

**Table providers** (`[[source]].provider`):

| Provider | Status | Notes |
|---|---|---|
| `csv` | ✅ implemented | Built-in DataFusion CSV reader. |
| `tsv` | ✅ implemented | CSV reader with tab delimiter. gzip inputs are decompressed to a temp file first (DataFusion is built without the `compression` feature, so `register_csv` can't read `.gz` directly). |
| `parquet` | ✅ implemented | Built-in DataFusion Parquet reader. |
| `vcf` | ⛔ not implemented | Reserved for a future bio-formats-backed provider. |
| `bed` | ⛔ not implemented | Reserved for a future bio-formats-backed provider. |

### Runtime lookup — async reader + monotonic cursor, in batches

Annotation runs in position-ordered buffers. For each buffer the runtime does one
**page-scoped, three-phase take** per plugin shard (`take_buffer`), reading only
that buffer's candidate pages:

1. **Resolve** — the buffer's sorted, de-duplicated `start` positions are mapped
   through the `PageDir` to candidate page **row-ranges** (`resolve_ranges`).
   Metadata only; no data read.
2. **Locate** — a `start`-only projected read over just those pages (a
   `RowSelection` built from the ranges) streams `start` values back in batches
   through a **`CoalescingAsyncReader`** — an async Parquet reader that merges
   nearby page byte-ranges (within a 512 KiB gap) into single I/O calls. A
   **monotonic row-offset cursor** (`ranges.flat_map(a..b)`) advances exactly one
   step per streamed row, staying in lockstep with the selection, and records the
   exact file offset of every row whose `start` is in the buffer's probe set. The
   cursor only ever moves forward, so there is no back-seeking.
3. **Take** — a final projected read at those exact offsets pulls just the payload
   columns (`allele_string`, match discriminators, values) for the matched rows
   into one compact `RecordBatch` — the `PluginBufferSlice`.

The engine then probes that in-memory slice **synchronously** per transcript
consequence: filter by `allele_string` and, if the plugin declares a
`[[match_column]]`, by the discriminator built from the template. The variation
lookup (`variation_lookup.rs`) is untouched — the plugin lookup reuses the same
`page_dir` primitives alongside it.

## Planned plugins

| Plugin | Description | Source |
|---|---|---|
| **AlphaMissense** | Protein pathogenicity predictions (DeepMind) | [Zenodo](https://zenodo.org/records/8208688) |
| **CADD v1.7** | Combined Annotation Dependent Depletion scores (SNVs + indels) | [cadd.gs.washington.edu](https://cadd.gs.washington.edu/) |
| **SpliceAI** | Deep-learning splice variant predictions | [Illumina/SpliceAI](https://github.com/Illumina/SpliceAI) |
| **ClinVar** | NCBI clinical variant classifications | [ncbi.nlm.nih.gov/clinvar](https://www.ncbi.nlm.nih.gov/clinvar/) |
| **dbNSFP v4.x** | Aggregated functional prediction scores (30+ predictors) | [dbNSFP](https://sites.google.com/site/jpaborern/dbNSFP) |
