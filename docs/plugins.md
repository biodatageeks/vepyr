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
- **`[[source]]`** — `provider` (`tsv`/`csv`/`parquet`), `path` (overridden at
  build time by `source_path`), and a `[source.csv]` block (`delimiter`,
  `has_header`, `comment`, `compression`, and an ordered `schema` of
  `{name, type}`).
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

## Planned plugins

| Plugin | Description | Source |
|---|---|---|
| **AlphaMissense** | Protein pathogenicity predictions (DeepMind) | [Zenodo](https://zenodo.org/records/8208688) |
| **CADD v1.7** | Combined Annotation Dependent Depletion scores (SNVs + indels) | [cadd.gs.washington.edu](https://cadd.gs.washington.edu/) |
| **SpliceAI** | Deep-learning splice variant predictions | [Illumina/SpliceAI](https://github.com/Illumina/SpliceAI) |
| **ClinVar** | NCBI clinical variant classifications | [ncbi.nlm.nih.gov/clinvar](https://www.ncbi.nlm.nih.gov/clinvar/) |
| **dbNSFP v4.x** | Aggregated functional prediction scores (30+ predictors) | [dbNSFP](https://sites.google.com/site/jpaborern/dbNSFP) |
