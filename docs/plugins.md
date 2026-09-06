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
    overwrite=False,                 # True replaces an existing plugin/<name>/ tree
    verify_source=True,              # hash each source against the manifest's md5 first
)
```

`source_path` also accepts a `dict[str, str]` for a manifest whose `[[source]]`
entries are split across several files, keyed by part name — those register as
`plugin_<name>_src_<part>`. The call returns one
`(chrom, rows, warm, cold)` tuple per chromosome written.

One call builds one plugin at one version. To combine plugins at different
versions (e.g. AlphaMissense `v0.2.0` + ClinVar `v0.3.0`), call
`build_plugin_cache` once per plugin into the **same** `plugin_cache_root`.

The manifest is resolved from the public vepyr-plugins repo at `version`
(cloned on demand), or from a local clone via `plugins_repo` for fully offline
builds. The exact resolution is recorded in the cache's `manifest.json` as
`cache_source_version: "<version>@<commit SHA>"`. An incremental build may add
or replace chromosomes only when that value matches the existing cache; use a
full build with `overwrite=True` to move a plugin cache to another revision.
Tiering (warm/cold) is **inherited from the variation cache** at
`cache_dir` — plugins declare no tier policy of their own.

### Source verification

A byte-parity-validated cache is only as good as the bytes it was built from,
so before the first chromosome is ingested the build MD5-hashes each resolved
`source_path` once (streaming, bounded memory — CADD's 87 GB SNV file costs a
few minutes of I/O on top of a multi-hour build) and compares it with the
manifest's `md5`:

| `verify_source` | Behaviour |
|---|---|
| `True` / `"strict"` (default) | A mismatch raises before anything is written, naming the part, the expected and actual digests and the upstream `url`. |
| `"warn"` | A mismatch is logged and the build continues; the digest actually found is recorded. For deliberate builds against a re-compressed or derived artifact, such as AlphaMissense's BGZF build input (its plugin README documents the preprocessing). |
| `False` / `"skip"` | Nothing is hashed. Use it for a chromosome slice cut with `tabix`, whose digest can never match the whole file's. |

A manifest that declares no `md5` is never hashed, whatever the mode. The
outcome is recorded under `sources` in the emitted `manifest.json` (see
[Cache format](#cache-format-lookup-internals)), and an incremental
`chroms=[...]` build against a file whose size and mtime match an earlier
verified record trusts that record instead of re-hashing — a per-chromosome
CADD workflow that calls `build_plugin_cache` 22 times hashes its input once.

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

### Plugin-only annotation

Pass `fields="core"` to emit VEP's eleven VCF-side default fields followed by
the selected plugin blocks:

```python
result = vepyr.annotate(
    "sample.vcf",
    "/data/116_GRCh38_merged",
    fields="core",
    plugin_cache_root="/data/plugin_cache",
    plugins=["cadd"],
)
```

VCF output records the exact ordered layout in the `CSQ` `Format:` header. An
explicit list or tuple may be used instead of `"core"`; its order is
preserved. Fields needed by plugin match templates are still computed even
when they are not emitted. With `fields=None` the full CSQ layout is written.

On the DataFrame path `fields=` is not needed: each plugin field is always a
named column after the base columns, typed by the manifest's `type` and
shaped by its `match_columns`: one value per row for a per-variant plugin
(`CADD_PHRED: String`), one per consequence entry for a per-feature plugin
(`am_pathogenicity: List(Float32)`). A `select()` decides what the engine
computes, plugin lookup included. See
[Polars DataFrames](dataframes.md#plugin-columns).

For why some scores remain strings to preserve VCF body MD5 matches, which
numeric types retain each field's precision, and Polars casting examples,
see [String scores and numeric casts in the vepyr-plugins README](https://github.com/biodatageeks/vepyr-plugins/blob/master/README.md#string-scores-and-numeric-casts).

### Choosing which plugins run

By default every plugin under `<plugin_cache_root>/plugin/` is applied. There
is no per-call version argument — a plugin's version is fixed when
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

Order is significant: `plugins=["clinvar", "cadd"]` emits the ClinVar block
before the CADD block in both the CSQ header and every CSQ value. Duplicates
are rejected, and the value must be a list or tuple rather than an unordered
set. With `plugins=None`, the engine discovers every cached plugin and emits
them in alphabetical plugin-name order. `plugins` requires
`plugin_cache_root`.

The three alternatives to `plugins` all still work, and are what you want when
a selection is permanent rather than per-call: pass `plugin_cache_root=None`
for no plugins, build separate roots for separate combinations, or add and
remove plugin directories under an existing root.

## Manifest structure

A plugin's `plugins/<name>/<name>.source.toml` declares how to ingest the raw
source and map it to CSQ fields.

!!! note "TOML ordering"
    Top-level scalar keys (`plugin_name`, `coordinate_system`, `ingest_sql`)
    MUST precede any `[[table]]` header, or TOML absorbs them into the preceding
    table.

### Top level

| Key | Type | Required | Description |
|---|---|---|---|
| `plugin_name` | string | yes | Plugin identifier; also the cache directory name. |
| `coordinate_system` | `1-based` \| `0-based-half-open` | yes | How `ingest_sql` reads positions. Drives the build-time shift to the variation cache's 1-based convention, and for `vcf`/`bed` sources it also sets the provider's own flag so both agree. |
| `ingest_sql` | string | yes | `SELECT` over the raw source table(s). MUST project `chrom`, `start`, `end`, `allele_string` (`ref/alt`), plus any discriminator and value columns. |
| `[[source]]` | table array | yes, 1+ | The raw file(s) — see below. |
| `[[value_columns]]` | table array | yes, 1+ | The emitted CSQ fields — see below. |
| `[[match_column]]` | table array | no (default none) | Per-transcript discriminator(s) — see below. Omit for per-variant plugins. |
| `allele_match` | `exact` \| `minimised` | no (default `exact`) | Which comparison the plugin's own Ensembl implementation uses. See [Allele matching](#allele-matching-exact-vs-minimised) — it is a statement about upstream, not a tuning knob. |
| `field_order` | `declared` \| `alphabetical` | no (default `declared`) | Order of this plugin's fields in CSQ. `declared` mirrors Ensembl `--custom`, `alphabetical` mirrors `--plugin`. |
| `assume_unique` | bool | no (default `false`) | Declare that the source never repeats a probe key, skipping the dedup pass. The build **samples the data to check the claim** rather than trusting it. |

### `[[source]]`

| Key | Type | Required | Description |
|---|---|---|---|
| `provider` | `csv` \| `tsv` \| `parquet` \| `vcf` \| `bed` | yes | Reader for this file. All five work — see [Table providers](#build-pipeline-table-providers-tables-views). |
| `path` | string | yes | Placeholder; **always** overridden at build time by `source_path`. |
| `url` | string | yes | Provenance: the canonical **upstream** download URL of this raw file (the publisher's FTP/bucket, never a mirror or a Drive share). Pin a dated release where the top-level file moves (e.g. ClinVar's weekly `clinvar.vcf.gz`). When the built input is a local re-compression of the upstream file (e.g. a BGZF+tabix rebuild of a plain gzip), `url` still names the upstream file. Never fetched; copied into the built cache's `manifest.json` and quoted in verification errors. |
| `md5` | string | yes | Provenance: 32 lowercase hex MD5 of the file at `url`. Take it from the publisher's checksum file where one exists (CADD `MD5SUMs`, ClinVar `.md5`, GCS object metadata), otherwise compute it on the downloaded copy and say so in a comment. This is the digest the build [verifies](#source-verification) `source_path` against. A manifest keeps this one digest; when the build input is a derived artifact of `url` (AlphaMissense's BGZF+tabix re-compression of the upstream plain gzip) the preprocessing is documented in the plugin's README and the build runs with `verify_source="warn"` or `False`. |
| `part` | string | no | Names this source when a manifest declares several. Registers as `plugin_<name>_src_<part>`, and makes `source_path` take a `{part: path}` mapping. |
| `index` | `tabix` | no | Random-access index. Explicit rather than inferred from a `.gz` suffix, because ordinary gzip is not seekable. On `csv`/`tsv` it **requires** `compression = "gzip"` (i.e. BGZF) — a plain gzip source with `index = "tabix"` is rejected at parse time. |
| `record_layout` | bool | no (default `false`) | `vcf` sources only: carry the raw record layout through the provider. |
| `[source.csv]` | table | for `csv`/`tsv` | Parsing options — see below. Not used by `parquet`/`vcf`/`bed`. |

### `[source.csv]`

| Key | Type | Default | Description |
|---|---|---|---|
| `delimiter` | string | `"\t"` | Field separator. |
| `has_header` | bool | `false` | Whether row 1 is a header. |
| `comment` | string | none | Lines starting with this are skipped (e.g. `"#"`). |
| `compression` | string | none | `"gzip"` is the recognised value. gzip inputs are decompressed to a temp file first, since DataFusion is built without the `compression` feature. |
| `schema` | array of `{name, type}` | empty | Ordered column list for headerless or explicitly typed input. `type` is `Utf8`, `Float32` or `Int32` — declaring everything `Utf8` and casting in `ingest_sql` is the common pattern. |

### `[[match_column]]`

| Key | Type | Required | Description |
|---|---|---|---|
| `column` | string | yes | The discriminator column stored in the shard. |
| `template` | string | yes | Built at runtime from the [engine-attribute namespace](#engine-attribute-namespace), e.g. `{ref_aa}{Protein_position}{alt_aa}`. If any attribute it references is absent for a consequence, the discriminator is empty and the plugin emits nothing on that line. |

### `[[value_columns]]`

| Key | Type | Required | Description |
|---|---|---|---|
| `column` | string | yes | Column produced by `ingest_sql`. |
| `csq_field` | string | yes | Output CSQ field name. |
| `type` | `Utf8` \| `Float32` \| `Int32` | yes | Stored Arrow type. |
| `description` | string | no | Emitted as the `##<FIELD>=<description>` header line, matching what Ensembl writes for plugin fields. |

Declaration order is **not** necessarily output order — `field_order` decides
that, and it is `alphabetical` for four of the five shipped plugins. See
[the note on emitted order](#supported-plugins).

### Multi-part sources

A manifest may declare several `[[source]]` blocks, each with its own `part`.
They register as `plugin_<name>_src_<part>` and are combined by the manifest's
own `ingest_sql`, so the files need not be concatenated first — CADD's separate
SNV and indel sources are the worked example:

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

The mapping must cover every declared part and name no unknown one. A bare path
against a multi-source manifest is rejected — one path cannot address two
sources, and the unmapped ones would silently read their placeholders. A mapping
is likewise rejected when a `[[source]]` declares no `part`, since there is then
no key to address it by.

There is **no `[tier]` block** — tiering is inherited from the variation cache.

## Allele matching: `exact` vs `minimised`

Ensembl's plugins do **not** agree on how a variant is compared to a row of
their data file, so neither can vepyr. Getting this wrong is silent: the wrong
setting either drops annotations Ensembl reports, or invents ones it does not.

Two rules are in use upstream:

| rule | what Ensembl does | plugins |
|---|---|---|
| `minimised` | calls `get_matched_variant_alleles()`, which runs `trim_sequences()` over **both** the variant and the data row before comparing | CADD, AlphaMissense, ClinVar |
| `exact` | compares position and allele strings verbatim | SpliceAI, dbNSFP |

Both rules compare the same three things — position, REF, ALT. The setting does
not change the shape of the key, and it does not change the first lookup: every
plugin is probed with the parser-level `(start, allele_string)`. What `minimised`
adds is a **second lookup on a miss**, against the fully reduced key. `exact`
plugins get one probe and stop.

Because the fallback is only consulted after a miss, `minimised` can add hits
but can never change one the primary probe already found.

### The two keys

A key is the **pair** `(start, allele_string)` — two separate shard columns, and
two separate arguments to the probe. `allele_string` holds only `REF/ALT`
(`"A/T"`, `"GT/-"`); it never carries the position. Below, `A/T @ 26032805` is
shorthand for that pair, not for the string itself.

| | primary key `(start, allele_string)` | fully reduced (fallback key) |
|---|---|---|
| **Built by** | `vcf_to_vep_input_allele()` | `plugin_probe_allele()` |
| **Mirrors** | Ensembl's VCF → `VariationFeature` parse | Ensembl's `trim_sequences()` |
| **Trims prefix** | one base only, the VCF anchor | the whole shared prefix |
| **Trims suffix** | never | the whole shared suffix |
| **Applies to** | indels only; SNV/MNV untouched | every variant class |
| **Moves position** | `+1` when the anchor is stripped | `+1` per prefix base; suffix never moves it |
| **Empty side** | `-` | `-` |
| **Used by** | every plugin, always | `minimised` plugins, and only after a miss |

The same variants through both:

| VCF record | primary `(start, allele_string)` | fully reduced | differ? |
|---|---|---|---|
| `100 A/G` | `A/G` @ 100 | `A/G` @ 100 | no |
| `200 CA/C` | `A/-` @ 201 | `A/-` @ 201 | no |
| `26032805 AAT/TAT` | `AAT/TAT` @ 26032805 | `A/T` @ 26032805 | **yes** |
| `13973877 TTGTGTGTGTGTG/GTGTGTGTGTGTG` | unchanged @ 13973877 | `T/G` @ 13973877 | **yes** |
| `26062230 AAC/ACAC` | `AC/CAC` @ 26062231 | `-/C` @ 26062231 | **yes** |
| `13836148 CGTGTGT/CGTGT` | `GTGTGT/GTGT` @ 13836149 | `GT/-` @ 13836153 | **yes** |

Which key each rule reaches for:

| | probes the primary key | probes the fully reduced key |
|---|---|---|
| `exact` — SpliceAI, dbNSFP | yes | never |
| `minimised` — CADD, AlphaMissense, ClinVar | yes | only if the first missed **and** the two keys differ |

The first two rows are the common case: the keys are identical, so `minimised`
does a single lookup exactly like `exact` — it costs nothing on well-formed
input. The rest are equal-length MNVs and `bcftools norm -m -both` leftovers,
where the parser keeps a shared suffix it never trims. That is the class where
the rule decides the answer.

!!! note "Why MNVs dominate these examples"
    The parser suffix-trims **indels only**, never same-length substitutions.
    So an untrimmed MNV keeps its entire shared suffix in `allele_string`, and
    the reduced key is genuinely a different lookup.

### A locus where both rules fire

`chr21:26032805 AAT>TAT` — a real HG002 record that `bcftools norm -m -both`
left un-trimmed. It is really an `A>T` SNV: trimming the shared trailing `AT`
gives `A/T`, and since nothing is trimmed from the front the position does not
move.

Both plugins are probed with the same primary key and both miss; only the rule
differs after that:

| | primary probe `(26032805, AAT/TAT)` | fallback `(26032805, A/T)` | emitted |
|---|---|---|---|
| **CADD** — `minimised` | miss | **hit** — `CADD_PHRED=0.239`, `CADD_RAW=-0.380109` | the scores |
| **SpliceAI** — `exact` | miss | *not attempted* — a row **does** exist there (`ds_ag=0.00`, `symbol=APP`) | nothing |

Ensembl VEP 116 emits exactly this at that locus: `CADD_PHRED=0.239`,
`CADD_RAW=-0.380109`, and empty SpliceAI fields.

That SpliceAI row is the point. The data is present and the reduced key would
reach it — `exact` is what stops vepyr from claiming it, because Ensembl's
`SpliceAI.pm` never reduces. Flipping SpliceAI to `minimised` turns loci like this
one into annotations VEP does not emit — 68 of them on chr21.

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
reduces the variant and matches; under `exact` it would not. SpliceAI is also
empty here, but for an unrelated reason — it carries no rows anywhere near
`chr21:13973877`, since it only covers splice regions. For a locus where the
*rule* is what separates the two, see
[the worked example above](#a-locus-where-both-rules-fire).

### Trim order is load-bearing

`trim_sequences()` can trim prefix-first or suffix-first, and for a non-minimal
indel **the two orders land on different coordinates — and therefore different
variants**. Ensembl's VCF parser builds the `VariationFeature` prefix-first
(left-first), so that is the only order that reproduces its output:

Reduction runs on the **anchor-trimmed** pair the VCF parser produces, not on
the raw record — a leading base shared by REF and ALT is already gone, and the
position already advanced, before any of this. Quoting a raw record against a
post-trim coordinate is how you get an off-by-one:

| chr21 VCF record | parser hands over | left-first (Ensembl) | right-first |
|---|---|---|---|
| `13836148 CGTGTGT/CGTGT` | `13836149 GTGTGT/GTGT` | `GT/-` at 13836153 — no row there, so empty, as VEP reports | `GT/-` at 13836149 — a **different variant's** score |
| `26062230 AAC/ACAC` | `26062231 AC/CAC` | `-/C` at 26062231 — the row VEP reports | same |

Only the **prefix** loop moves the coordinate; the suffix loop trims bases
without touching it. That is the whole reason the two orders disagree: trimming
the suffix first consumes bases the prefix loop would otherwise have consumed
*while advancing `start`*, so the position ends up short.

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
url  = "https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz"
md5  = "9fd167735f16a1b87da6eb3e4c25fcb5"
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
url  = "https://example.org/demo/v1/demo.tsv.gz"
md5  = "d41d8cd98f00b204e9800998ecf8427e"
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
| `SYMBOL` | Gene symbol (e.g. `APP`) — SpliceAI's discriminator. |
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
whole file. `manifest.json` records the requested plugin ref and its exact
resolved commit in `cache_source_version`, making cache provenance immutable and
auditable even when the requested ref is a branch.

### Provenance in `manifest.json`

Besides the schema, CSQ mapping and per-shard row/tier counts, the manifest
records one `sources` entry per `[[source]]` so a shard's provenance survives
without the source file:

```json
"sources": [
  {
    "file": "AlphaMissense_hg38.bgz.tsv.gz",
    "url": "https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz",
    "md5": "9fd167735f16a1b87da6eb3e4c25fcb5",
    "verified_md5": "46d0028375cf95088bd014ff6855cffd",
    "size": 628407716,
    "mtime_ns": 1783322828158000000,
    "ino": 194298162,
    "ctime_ns": 1787813446862801296,
    "index": {
      "file": "AlphaMissense_hg38.bgz.tsv.gz.tbi",
      "verified_md5": "7e925b94f5afd9ef184bde4de8aedeb5",
      "size": 684511,
      "mtime_ns": 1783322837454000000,
      "ino": 194298178,
      "ctime_ns": 1787813449824439001
    }
  }
]
```

`part` is present for multi-file manifests (CADD). `url` and `md5` are copied
from the source manifest. `verified_md5` is the digest the build actually
computed over the resolved file — absent when verification was skipped or the
manifest declared no digest, and *different* from `md5` only after a `"warn"`
build, as here: AlphaMissense's build input is a BGZF re-compression of the
upstream file, so the recorded digest is the artifact's, not the upstream's. `file`, `size`, `mtime_ns`, `ino` and `ctime_ns` fingerprint the hashed
file for incremental builds: an unchanged file is not re-hashed, while a
replacement or rewrite (a new inode, a fresh change time) always is. For a
tabix source the `.tbi` gets the same treatment under `index` — it is always
hashed and fingerprinted, so a changed index is a changed input — and it is
checked to describe the data it sits beside: for every contig it names, the
record its first chunk points at must carry that contig, which refuses an
index built from another version of the file in strict and warn mode alike. Every
chromosome is built to a staging file and the sources are re-checked after
each; only when all of them passed are the shards made live and the manifest
written, so a source that changes mid-build leaves the cache exactly as it
was. A filtered rebuild whose input hashes differently from the earlier
build's drops that build's chromosomes from the manifest rather than mixing
releases. Chromosomes whose input was never verified — a cache built before
this block existed (no `sources` key), or one built with `verify_source="skip"`
— cannot be attributed to a verified input, so a verifying `chroms=[...]` build
that would carry them over is refused: rebuild every chromosome
(`overwrite=True`), or add chromosomes with `verify_source="skip"`, which makes
no claim.

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

| Provider | Notes |
|---|---|
| `csv` | Built-in DataFusion CSV reader. |
| `tsv` | CSV reader with tab delimiter. gzip inputs are decompressed to a temp file first (DataFusion is built without the `compression` feature, so `register_csv` can't read `.gz` directly). |
| `parquet` | Built-in DataFusion Parquet reader. |
| `vcf` | `VcfTableProvider` from bio-formats. Every INFO field the header declares is exposed and `ingest_sql` projects down to what it needs; set `record_layout` on the source to carry the raw record through. |
| `bed` | `BedTableProvider` from bio-formats, BED4 only — `chrom`, `start`, `end`, `name`, whatever the file's variant. |

All five are implemented. `vcf` and `bed` take their zero/one-based
interpretation from the manifest's `coordinate_system` rather than the file's
own convention, so `ingest_sql` always sees the system the manifest declares.

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

The CSQ fields each plugin emits, **in emitted order**:

| Plugin | Fields |
|---|---|
| CADD | `CADD_PHRED`, `CADD_RAW` |
| SpliceAI | `SpliceAI_pred_DP_AG`, `SpliceAI_pred_DP_AL`, `SpliceAI_pred_DP_DG`, `SpliceAI_pred_DP_DL`, `SpliceAI_pred_DS_AG`, `SpliceAI_pred_DS_AL`, `SpliceAI_pred_DS_DG`, `SpliceAI_pred_DS_DL`, `SpliceAI_pred_SYMBOL` |
| AlphaMissense | `am_class`, `am_pathogenicity` |
| ClinVar | `ClinVar`, `ClinVar_CLNSIG`, `ClinVar_CLNREVSTAT`, `ClinVar_CLNDN`, `ClinVar_CLNVC`, `ClinVar_CLNVI` |
| dbNSFP | `CADD_phred`, `CADD_raw`, `GERP++_RS`, `MetaLR_pred`, `MetaLR_score`, `MetaSVM_pred`, `MetaSVM_score`, `MutationTaster_pred`, `MutationTaster_score`, `PROVEAN_pred`, `PROVEAN_score`, `Polyphen2_HDIV_score`, `Polyphen2_HVAR_score`, `REVEL_score`, `SIFT4G_pred`, `SIFT4G_score`, `VEST4_score`, `phastCons100way_vertebrate`, `phyloP100way_vertebrate` |

!!! note "Emitted order is not manifest order"
    A manifest's `field_order` decides this, and it is not the order the
    `[[value_columns]]` happen to be written in:

    | `field_order` | Emits | Mirrors |
    |---|---|---|
    | `declared` (default) | manifest declaration order | Ensembl `--custom` |
    | `alphabetical` | sorted by CSQ field name | Ensembl `--plugin` |

    Four of the five are `alphabetical`, because Ensembl loads them with
    `--plugin`. ClinVar is `declared`, because it is loaded with `--custom` —
    which is why it is the one plugin whose emitted order matches how its
    manifest reads. `dbNSFP` carries its own `CADD_phred`/`CADD_raw`, distinct
    from the CADD plugin's upper-case `CADD_PHRED`/`CADD_RAW`.

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
