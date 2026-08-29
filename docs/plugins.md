# Plugins

vepyr supports external per-variant annotation databases as **plugins**: a raw
source (TSV/CSV/Parquet) is converted into a frequency-tiered, per-chromosome
Parquet cache whose values are emitted as extra VEP CSQ output fields.
Five plugins are implemented today: **CADD**, **SpliceAI**, **AlphaMissense**,
**ClinVar** and **dbNSFP**.

Plugin manifests live in the public [vepyr-plugins](https://github.com/biodatageeks/vepyr-plugins)
repository and are selected by **plugin name + git tag**, so different plugins can
be pinned to different releases.

!!! tip "Don't want to build one?"
    Prebuilt release-116 plugin caches for CADD, SpliceAI, AlphaMissense and
    ClinVar are published for download — see
    [Plugin caches](downloads.md#plugin-caches). dbNSFP is the exception: its
    licence forbids redistributing a converted cache, so that one must be built
    from your own registered download.

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

!!! warning "Plugins apply to VCF output only"
    `plugin_cache_root` is read on the `output_vcf` path. The streaming
    annotator behind a returned `LazyFrame` ignores it, so a `LazyFrame` result
    carries no plugin fields — not as columns, and not inside `CSQ` even with
    `skip_csq=False`. Passing [`plugins`](#choosing-which-plugins-run) without
    `output_vcf` warns for this reason; `plugin_cache_root` on its own is
    silently ignored.

### Choosing which plugins run

Selection is **directory-shaped**: every plugin under
`<plugin_cache_root>/plugin/` is applied. There is no per-call version
argument — a plugin's version is fixed when
[`build_plugin_cache`](api.md#vepyr.build_plugin_cache) writes it.

`plugins` narrows that set to a subset of the directories present:

```python
vepyr.annotate(
    "sample.vcf",
    "/data/116_GRCh38_merged",
    plugin_cache_root="/data/plugin_cache",
    plugins=["clinvar", "cadd"],   # only these two
    output_vcf="sample.annotated.vcf",
)
```

| `plugins=` | Effect |
|---|---|
| omitted / `None` | every plugin under the root — the default |
| `["clinvar", "cadd"]` | only those two |
| `[]` | none; equivalent to a plugin-free run |
| `["nope"]` | `ValueError`, listing the plugins that *are* available |

Order is irrelevant — the engine sorts discovered plugins by
`(csq_rank, plugin_name)`, so the CSQ layout does not depend on how you list
them. `plugins` requires `plugin_cache_root`.

The three alternatives to `plugins` all still work, and are what you want when
a selection is permanent rather than per-call: pass `plugin_cache_root=None`
for no plugins, build separate roots for separate combinations, or add and
remove plugin directories under an existing root.

??? note "How the subset is materialised"

    The engine has no plugin filter. Both the CSQ header and the per-transcript
    body discover `<root>/plugin/` independently, so filtering one and not the
    other would leave the header advertising fields the body never fills,
    shifting every later value.

    `plugins` therefore hands the engine a root that already contains only the
    selected plugins — a temporary directory whose files are hard-linked to the
    originals. The two discovery passes then cannot disagree, and nothing is
    copied.

    Files are linked individually rather than the plugin directory being
    symlinked, because a directory symlink needs `target_is_directory=True` on
    Windows plus a privilege non-elevated sessions lack. Where hard links are
    unavailable — a temporary directory on a different volume — it falls back
    to per-file symlinks, then to an error naming `TMPDIR`.

    The subset lives as long as it is needed: released when the annotation
    finishes for `output_vcf`, and tied to the `LazyFrame` otherwise, since a
    frame re-opens the cache on every `collect()`.

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
  `comment`, `compression`, and an ordered `schema` of `{name, type}`.
  A manifest may declare several `[[source]]` blocks, each with its own `part`.
  They are registered as `plugin_<name>_src_<part>` and combined by the
  manifest's own `ingest_sql`, so there is no need to concatenate the files
  first — CADD's separate SNV and indel sources are the worked example.
  `build_plugin_cache()` takes a plain path for a single-source manifest, and a
  `{part: path}` mapping for a multi-source one:

  ```python
  vepyr.build_plugin_cache(
      "cadd", version,
      source_path={
          "snv": ".../whole_genome_SNVs.tsv.gz",
          "indel": ".../gnomad.genomes.r4.0.indel.tsv.gz",
      },
      ...
  )
  ```

  The mapping must cover every declared part and name no unknown one. A bare
  path against a multi-source manifest is rejected — one path cannot address two
  sources, and the unmapped ones would silently read their placeholders. A
  mapping is likewise rejected when a `[[source]]` declares no `part`, since
  there is then no key to address it by.
- **`[[match_column]]`** *(optional, 0+)* — a per-transcript discriminator:
  `column` (the stored discriminator column) + `template` (built at runtime from
  the engine-attribute namespace, see below). Omit entirely for per-variant
  plugins (the value is emitted on every transcript line).
- **`[[value_columns]]`** *(1+)* — `column`, `csq_field` (output field name),
  `type` (`Utf8` / `Float32` / `Int32`). **Declaration order = CSQ output order.**
- **`allele_match`** *(optional)* — `"exact"` (default) or `"minimised"`. Which
  one is correct is decided by the plugin's own Ensembl implementation, not by
  preference; see [Allele matching](#allele-matching-exact-vs-minimised).

There is **no `[tier]` block** — tiering is inherited from the variation cache.

## Allele matching: `exact` vs `minimised`

Ensembl's plugins do **not** agree on how a variant is compared to a row of
their data file, so neither can vepyr. Getting this wrong is silent: the wrong
setting either drops annotations Ensembl reports, or invents ones it does not.

Two rules are in use upstream:

| rule | what Ensembl does | plugins |
|---|---|---|
| `minimised` | calls `get_matched_variant_alleles()`, which runs `trim_sequences()` over **both** the variant and the data row — trimming shared prefix *and* suffix, in both orders — then compares `(ref, alt, pos)` | CADD, AlphaMissense, ClinVar |
| `exact` | compares `(start, ref, alt)` verbatim | SpliceAI, dbNSFP |

**ClinVar is `minimised`, despite being loaded with `--custom ...,exact`.** The
`exact` there names the *overlap mode*, not the allele rule — core still
minimises the alleles first, then requires exact overlap. The golden VEP 116
reference settles it: `chr1:65364614 GT>TT` is annotated `ClinVar=1258041`,
whose source record is the 1 bp SNV `G>T` at the same position. Reaching it
from `GT/TT` needs the shared trailing `T` trimmed, so this is full
`trim_sequences()` behaviour — a suffix trim, not just the narrower
leading-anchor-base shift. A verbatim comparison would emit nothing, and a
1 bp feature could not have exactly overlapped the 2 bp unminimised variant
either.

The distinction bites whenever a record is **not in minimal form**. `bcftools
norm -m -both` splits a multi-allelic record without re-trimming its halves, so
this is routine rather than exotic:

```text
chr21:13973877  REF=TTGTGTGTGTGTG  ALT=GTGTGTGTGTGTG   # really just T>G
chr21:26062230  REF=AAC            ALT=ACAC            # really just an inserted C
```

CADD keys the first as `T/G` in its per-base file. Under `minimised` Ensembl
reduces the variant and matches; under `exact` it does not — and SpliceAI, whose
rows at that position are plain SNVs, reports nothing. Both are correct *for
their own plugin*.

### Trim order is load-bearing

`trim_sequences()` can trim prefix-first or suffix-first, and for a non-minimal
indel **the two orders land on different coordinates — and therefore different
variants**. Ensembl's VCF parser builds the `VariationFeature` prefix-first
(left-first), so that is the only order that reproduces its output:

| VCF record | left-first (Ensembl) | right-first |
|---|---|---|
| `CGTGTGT/CGTGT` | `GT/-` at 13836153 — no row there, so empty, as VEP reports | `GT/-` at 13836149 — a **different variant's** score |
| `AAC/ACAC` | `-/C` at 26062231 — the row VEP reports | same |

Getting this backwards is silent and produces confident wrong answers: on chr21
a right-first reduction invented 5,288 CADD scores VEP does not emit, and
regressed ClinVar and SpliceAI from clean.

!!! warning "Never set `minimised` to gain hits"
    Reducing alleles for an `exact` plugin produces annotations Ensembl does not
    emit — SpliceAI gained 68 spurious hits on chr21 that way. The setting is a
    statement about what upstream does, not a tuning knob.

To decide the setting for a new plugin, read its `.pm`: if `run()` calls
`get_matched_variant_alleles`, use `minimised`; if it compares `$vf->{start}`
and the allele strings with `==`/`eq`, use `exact`.

!!! note "Ensembl's fetch window"
    `get_matched_variant_alleles` is not the whole story upstream. CADD.pm first
    fetches `[VF.start - 2, VF.end]` from the tabix file, so a row whose *file*
    position falls outside that window is never even considered — which is why
    VEP reports nothing for `CGTGTGT/CGTGT` even though a `GT/-` row exists
    nearby. Left-first reduction happens to land outside that row's reach too,
    so vepyr agrees without emulating the window; if a future mismatch traces
    back here, this is the mechanism to check.

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


--8<-- "includes/cache-internals.md"

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

## Supported plugins

All five plugins below are implemented and validated against the golden Ensembl
VEP 116 reference. Four have a **prebuilt cache** published on Hugging Face —
see [Plugin caches](downloads.md#plugin-caches) for the download commands.

| Plugin | CSQ fields | Discriminator | Allele match | Prebuilt cache | Source |
|---|--:|---|---|---|---|
| **CADD** v1.7 | 2 | — (per variant) | `minimised` | [`…plugin_cadd`](downloads.md#plugin-caches) | [cadd.gs.washington.edu](https://cadd.gs.washington.edu/) |
| **SpliceAI** | 9 | `{SYMBOL}` | `exact` | [`…plugin_spliceai`](downloads.md#plugin-caches) | [Illumina/SpliceAI](https://github.com/Illumina/SpliceAI) |
| **AlphaMissense** | 2 | `{ref_aa}{Protein_position}{alt_aa}` | `minimised` | [`…plugin_alphamissense`](downloads.md#plugin-caches) | [Zenodo](https://zenodo.org/records/8208688) |
| **ClinVar** | 6 | — (per variant) | `minimised` | [`…plugin_clinvar`](downloads.md#plugin-caches) | [ncbi.nlm.nih.gov/clinvar](https://www.ncbi.nlm.nih.gov/clinvar/) |
| **dbNSFP** | 19 | `{ref_aa}/{alt_aa}` | `exact` | **not published** — build locally | [dbNSFP](https://www.dbnsfp.org/) |

The CSQ fields each plugin emits, in output order:

| Plugin | Fields |
|---|---|
| CADD | `CADD_RAW`, `CADD_PHRED` |
| SpliceAI | `SpliceAI_pred_SYMBOL`, `SpliceAI_pred_DS_AG`, `SpliceAI_pred_DS_AL`, `SpliceAI_pred_DS_DG`, `SpliceAI_pred_DS_DL`, `SpliceAI_pred_DP_AG`, `SpliceAI_pred_DP_AL`, `SpliceAI_pred_DP_DG`, `SpliceAI_pred_DP_DL` |
| AlphaMissense | `am_class`, `am_pathogenicity` |
| ClinVar | `ClinVar`, `ClinVar_CLNSIG`, `ClinVar_CLNREVSTAT`, `ClinVar_CLNDN`, `ClinVar_CLNVC`, `ClinVar_CLNVI` |
| dbNSFP | 19 predictor score/prediction pairs (`SIFT4G_*`, `Polyphen2_HDIV_*`, `Polyphen2_HVAR_*`, `MutationTaster_*`, …) |

!!! note "dbNSFP is supported but cannot be mirrored"
    The dbNSFP licence permits academic use of the data but not redistribution
    of a converted copy, so no prebuilt cache is published. Register for and
    download the source yourself, then build the cache with
    [`build_plugin_cache`](api.md#vepyr.build_plugin_cache) as above — the
    manifest in [vepyr-plugins](https://github.com/biodatageeks/vepyr-plugins)
    is public.

!!! warning "Licence terms differ per plugin"
    **CADD**, **SpliceAI** and **AlphaMissense** restrict use to academic /
    non-profit research; commercial use needs a licence from the respective
    provider. **ClinVar** is unrestricted (NCBI public domain). Each Hugging
    Face dataset card carries the specific terms.
