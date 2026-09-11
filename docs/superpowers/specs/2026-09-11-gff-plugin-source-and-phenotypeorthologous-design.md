# GFF plugin sources and the PhenotypeOrthologous plugin

Date: 2026-09-11. Status: approved in chat, implementation not started.

## 1. Goal

Add a `gff` source provider to the VEP plugin-cache machinery and port the
Ensembl VEP `PhenotypeOrthologous` plugin (VEP_plugins release/116) to the
vepyr-plugins catalog, gated byte-for-byte against Ensembl VEP 116 on the
normalized HG002 input with the merged cache. Publish the resulting cache on
Hugging Face.

The plugin was chosen over `GO` because GO's GFF is not a published file: VEP
generates it from the Ensembl core MySQL database on first run. (The 2026_04
geneset on `ftp.ebi.ac.uk/pub/ensemblorganisms` does carry `xref.tsv.gz` with
GO rows and `genes.gff3.gz` with transcript spans, both md5-pinned, so GO can
follow later on the same `gff` provider once a transcript-keyed lookup exists.)

## 2. Scope

In scope:

- `provider = "gff"` in plugin source manifests (engine + catalog validator).
- A second lookup kind, `lookup = "interval"`, next to today's exact-position
  probe.
- `plugins/phenotypeorthologous/phenotypeorthologous.source.toml`.
- VEP 116 oracle references for chr22 and chr1, plus a resumable whole-sample
  driver (chr1-22; the HG002 benchmark VCF has no sex chromosomes).
- A `merged_phenotypeorthologous` comparison profile in vepyr.
- Docs in vepyr and vepyr-plugins; a published cache
  `biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous`.

Out of scope:

- The plugin's `model=rat|mouse` option: all four fields are always emitted.
- GO, or any transcript-keyed lookup.
- Teaching the cache-QA tool (branch `feat/cache-qa-profile`) the interval
  shard shape. The cache is published by hand, as the v0.1.1 caches were.
- A COITree-backed interval index. See §4.3.

## 3. Reference behaviour (VEP_plugins release/116, `PhenotypeOrthologous.pm`)

Source file, pinned:

| item | value |
|---|---|
| url | `https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz` |
| md5 (computed locally; Ensembl's `CHECKSUMS` uses BSD `sum`) | `20e5401a198d7d3db66a982c037d3ad4` |
| `.tbi` md5 | `1eb833a26c247418910685289c6528cc` |
| size | 3,424,946 bytes, 16,409 features (`gene` 16,167, `ncRNA_gene` 242) |
| contigs | 1-22, X, MT, GL000220.1, KI270721.1, KI270728.1, KI270734.1 (no Y) |
| gene_id | unique per row; file sorted by start within contig |
| plugin code | `PhenotypeOrthologous.pm` at VEP_plugins `a7e03a5c6497e29e0598eed7b4795f953d9a1b5f`, sha256 `89be8f30dd464f81913bef367831e8a08258e4085207f8291c956487f47de8ac` |

Attributes used: `gene_id`, `Rat_gene_id`, `Rat_Orthologous_phenotype`,
`Mouse_gene_id`, `Mouse_Orthologous_phenotype`. 12,375 rows carry mouse
values, 15,869 carry rat values, 11,835 both. Phenotype values contain `|`
separators, commas (6,772 rows) and at least one leading space.

Matching, per transcript consequence line (`feature_types` is `Transcript`):

1. `get_data($vf->{chr}, $vf_start, $vf_end)` with `expand_left(0)` and
   `expand_right(0)`: a tabix region query with the **variant's** span. For
   insertions VEP has `start = end + 1`; the plugin swaps them, so the query
   span is `[end, start]`. The `chr` prefix is stripped to match the file.
2. Of the records overlapping that span, in file order, the first whose
   `gene_id` equals `$transcript->{_gene}->stable_id` wins.
3. Result: up to four keys, absent when the attribute is absent.

Output: VEP sorts plugin header keys, so the CSQ tail is, in order,
`PhenotypeOrthologous_Mouse_geneid`, `PhenotypeOrthologous_Mouse_phenotype`,
`PhenotypeOrthologous_Rat_geneid`, `PhenotypeOrthologous_Rat_phenotype`.
Header descriptions (verbatim):

- `PhenotypeOrthologous_Rat_geneid`: `PhenotypeOrthologous RatGene associated with Rat`
- `PhenotypeOrthologous_Rat_phenotype`: `PhenotypeOrthologous RatPhenotypes associated with orthologous genes in Rat`
- `PhenotypeOrthologous_Mouse_geneid`: `PhenotypeOrthologous MouseGene associated with Mouse`
- `PhenotypeOrthologous_Mouse_phenotype`: `PhenotypeOrthologous MousePhenotypes associated with orthologous genes in Mouse`

VCF escaping (`OutputFactory/VCF.pm`): `,` and `|` become `&`, `;` becomes
`%3B`, whitespace runs become `_`. The engine's shared `csq_escape` already
implements these rules, so a leading space renders as a leading `_`.

Consequences for the port: a flanking (upstream/downstream) variant only
matches when it still lies inside the gene span; RefSeq transcripts in the
merged cache never match (their gene id is not an ENSG id); regulatory,
motif and intergenic lines are empty.

## 4. Engine design (datafusion-bio-functions, `plugin_cache/`)

Base: `origin/master` at `4b0bd00`, which is also vepyr's pin.

### 4.1 `gff` provider (PR 1)

- `ProviderKind::Gff` in `source_manifest.rs`. New optional table
  `[source.gff]` with `attributes: Vec<String>` (required, non-empty for
  gff). `index = "tabix"` becomes legal for gff (BGZF + sibling `.tbi`).
  `record_layout` stays VCF-only.
- `provider.rs::register_sources_impl` arm: with `index = "tabix"`, slice the
  chromosome through the existing `materialize_tabix_chrom` (records are
  copied verbatim, so the temp file is valid GFF3) and open the temp path;
  otherwise open the manifest path directly (plain or gzip, detected by
  content, not extension). Construct
  `GffTableProvider::new(path, Some(attributes), None, zero_based)` from
  `datafusion-bio-format-gff` at the already-pinned tag `v1.12.1` (tag form,
  never `rev`, per the Cargo.toml note in vepyr). `zero_based` follows the
  manifest `coordinate_system` exactly as the VCF/BED arms do.
- The table exposes `chrom, start, end, type, source, score, strand, phase`
  plus one nullable Utf8 column per requested attribute. The reader
  percent-decodes attribute values and does not trim them.
- Tests: a synthetic 3-feature GFF3 as plain text, gzip and BGZF+tbi; an
  attribute containing `%3B`; a value with a leading space; a missing
  attribute yields NULL; a contig absent from the index yields zero rows.

### 4.2 `lookup = "interval"` (PR 2, stacked on PR 1)

Manifest:

- Top-level `lookup = "point" | "interval"`, default `point`, serialised
  lowercase. `interval` requires at least one `[[source]]` whose rows carry
  `start` and `end`; `allele_match` must be left at its default (validator
  error otherwise, it has no meaning without an allele); `assume_unique` and
  `field_order` keep their meaning; `match_column` templates are optional.
- `CacheManifest` gains `lookup` (serde default `point`, so existing caches
  read unchanged) and `key_columns` becomes `["chrom","start","end"]` for
  interval caches. `ChromEntry.warm` is 0 and `cold` equals `rows`.

Build (`build.rs`, `normalize.rs`, `join.rs`, `write.rs`):

- Normalised projection is `chrom, start, end, <match…>, <value…>`, no
  `allele_string`; the coordinate shift for `0-based-half-open` applies to
  `start` as today.
- Dedup keeps the first row per `(start, end, <match…>)` in arrival order.
- The variation tier join is skipped; `tier` is written as the literal
  `CAST(1 AS TINYINT)`. The shard must preserve tabix order, so an ordinal
  is attached at ingest and the final `ORDER BY` is `(start, ordinal)`. The
  existing `inspect_tier_start_order` guard and the `(tier, start)` writer
  properties remain valid.
- `plugin_output_schema` for interval caches:
  `chrom, start, end, <match…>, <value…>, tier`.
- `PluginCacheBuilder` still takes the variation cache dir; it is used only
  to enumerate chromosomes for interval builds.

Runtime (`lookup.rs`, `registry.rs`, `annotate_provider.rs`):

- New `IntervalLookup::open(shard)`: reads the whole per-chromosome shard
  once (projection `start, end, <match…>, <value…>`) into
  `HashMap<Vec<Option<String>>, Vec<IntervalRow>>` keyed by the discriminator
  tuple, each vector in file order. Shards are small (PhenotypeOrthologous:
  at most 1,754 rows on chr1).
- `probe(vf_start, vf_end, &match_values) -> Option<&[PluginScalar]>`
  returns the first row with `start <= vf_end && end >= vf_start`.
- `PluginRegistry::open` branches on `manifest.lookup` into
  `PluginLookupKind::{Point(PluginLookup), Interval(IntervalLookup)}`;
  `take_buffer_all` skips interval plugins; `probe_all` receives the
  variant's VEP-normalised span in addition to the existing point key.
- The variant span passed to the probe is derived from the same
  VEP-normalised allele the point key uses: SNV/MNV/deletion
  `[vep_start, vep_start + len(ref) - 1]`; insertion (empty trimmed ref)
  `[vep_start - 1, vep_start]`, which is VEP's swapped `[end, start]`.
- The no-transcript placeholder line probes with an empty attribute
  namespace, exactly like point plugins: a plugin with a discriminator gates
  to empty, a discriminator-less interval plugin still populates.
- Values go through `csq::format_scalar`, so escaping is the shared engine
  rule. Header descriptions come from `value_columns[].description`.

Limitation, documented: a discriminator-less interval plugin scans its
chromosome's rows linearly per probe. Acceptable for tracks of a few thousand
intervals; a COITree index is the upgrade path if a large track arrives.

### 4.3 Tests (engine)

- Manifest: `lookup` parsing and defaults; `interval` rejects
  `allele_match = "minimised"`; old `manifest.json` without `lookup` reads
  as `point`.
- Build: an interval manifest over the synthetic GFF yields the expected
  schema, order and `tier = 1`; dedup keeps the first of two identical keys.
- Runtime: overlap at both boundaries, insertion span, discriminator miss,
  first-in-file-order tie-break, escaping of `,`, `|`, whitespace and a
  leading space.
- Regression: the existing point-lookup tests and the golden benchmark are
  untouched; enabling an interval plugin adds exactly its four columns.

## 5. Catalog (vepyr-plugins)

`plugins/phenotypeorthologous/phenotypeorthologous.source.toml`:

```toml
plugin_name       = "phenotypeorthologous"
coordinate_system = "1-based"
lookup            = "interval"
field_order       = "alphabetical"        # VEP sorts plugin header keys
assume_unique     = true                  # one row per gene_id in the file
ingest_sql = """
SELECT chrom, start, "end",
       gene_id,
       "Mouse_gene_id"               AS mouse_gene_id,
       "Mouse_Orthologous_phenotype" AS mouse_phenotype,
       "Rat_gene_id"                 AS rat_gene_id,
       "Rat_Orthologous_phenotype"   AS rat_phenotype
FROM plugin_phenotypeorthologous_src
"""

[[source]]
provider = "gff"
path     = "PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
url      = "https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
md5      = "20e5401a198d7d3db66a982c037d3ad4"   # computed locally; Ensembl's CHECKSUMS is BSD sum
index    = "tabix"
  [source.gff]
  attributes = ["gene_id", "Mouse_gene_id", "Mouse_Orthologous_phenotype",
                "Rat_gene_id", "Rat_Orthologous_phenotype"]

[[match_column]]
column   = "gene_id"
template = "{Gene}"

[[value_columns]]
column = "mouse_gene_id"
csq_field = "PhenotypeOrthologous_Mouse_geneid"
type = "Utf8"
description = "PhenotypeOrthologous MouseGene associated with Mouse"
# … mouse_phenotype, rat_gene_id, rat_phenotype likewise (descriptions in §3)
```

Validator (`scripts/validate_manifests.py`): add `gff` to `PROVIDERS`;
require `[source.gff].attributes` (non-empty list of strings) for gff and
reject it elsewhere; allow `index = "tabix"` for gff; accept top-level
`lookup ∈ {point, interval}` and reject `allele_match` when `interval`.
README manifest table, the "Supported plugins" list and
`.claude/skills/adding-a-plugin/SKILL.md` gain the GFF and interval sections.
Release: `minor` bump → `v0.2.0` after merge.

## 6. vepyr

- `Cargo.toml` pin → engine master after PR 1 and PR 2 merge.
- Python: no functional change is required. `per_variant = not
  match_columns` already types the plugin columns as per-transcript
  `List(String)`. Validation messages that list plugin names pick the new one
  up from the manifest.
- `e2e-testing/scripts/comparison/profiles.py`: profile
  `merged_phenotypeorthologous` = merged cache + `plugins=("phenotypeorthologous",)`,
  per-contig reference template
  `output/116/plugins/HG002_chr{chrom}_phenotypeorthologous_vep116`.
- `e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh <chrom>`:
  downloads and md5-checks the GFF and `.tbi` into
  `$DATA/plugin_input/phenotypeorthologous/`, pins `PhenotypeOrthologous.pm`
  by sha256 under `output/116/plugins/plugin_code/`, slices
  `HG002_norm.vcf.gz` per contig, runs VEP with **BGZF output**
  (`--compress_output bgzip`), indexes it, asserts exactly four plugin
  sub-fields in the CSQ header, and writes a `.plugins` sidecar.
- `e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh [chroms]`:
  resumable per-contig driver, default chr1-22, `VEP_REFERENCE_JOBS=2`.
- The reference builder records its own provenance sidecar (plugin sha256 and
  the source url + md5); the file is md5-pinned rather than rolling, so
  `source_identity.sh` is not extended.
- Tests: `tests/data/plugin_gff/` synthetic GFF3 covering two golden chr1
  variants (one inside a gene span with a matching gene, one transcript
  whose variant lies outside its gene span) and a value with `,`, `|` and a
  leading space; assertions in `tests/test_annotate.py` on the exact CSQ
  tail and the LazyFrame columns; profile tests in
  `tests/test_comparison_profiles.py`.
- Docs: `docs/plugins.md` (GFF source table, `[source.gff]`, `lookup`
  kinds, supported-plugins row and field order), `docs/downloads.md` (new
  dataset repo), `docs/testing-vep.md` (new profile and scripts),
  `docs/architecture.md`, `README.md` plugin line, `e2e-testing/README.md`.

## 7. Oracle procedure

Recipe identical to the five-plugin references except for the plugin flag
and BGZF output. `DATA=~/workspace/data_vepyr`.

```bash
# chr22 (iteration), then chr1
e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh 22
e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh 1

# whole normalized HG002 sample (chr1-22), resumable, hours
VEP_REFERENCE_JOBS=2 nohup e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh \
  > $DATA/output/116/plugins/logs/phenotypeorthologous_all.log 2>&1 &
```

The VEP invocation inside the builder:

```bash
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$DATA":/data -v "$DATA/output/116/plugins/plugin_code":/plugins:ro \
  ensemblorg/ensembl-vep:release_116.0 \
  vep --cache --cache_version 116 --dir_cache /data --offline --merged \
      --everything --no_stats --force_overwrite --vcf --compress_output bgzip \
      --fasta /data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
      --input_file  /data/input/slices/HG002_norm_chr${CHROM}.vcf.gz \
      --output_file /data/output/116/plugins/HG002_chr${CHROM}_phenotypeorthologous_vep116.vcf.gz \
      --dir_plugins /plugins \
      --plugin PhenotypeOrthologous,file=/data/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz
```

Comparison:

```bash
uv run python e2e-testing/scripts/run_comparison.py --release 116 \
  --profile merged_phenotypeorthologous --chroms 22 \
  --plugin-cache $DATA/plugin_cache_v0.2.0 --workers 4 --bgzf --force
uv run python e2e-testing/scripts/md5_concordance.py \
  --pair <vep slice> <vepyr bgzf> --mode strict --explain --explain-limit 0
```

## 8. Publishing

- Build all 25 contigs (chr1-22, X, Y, MT) with
  `build_plugin_cache("phenotypeorthologous", "v0.2.0", …,
  verify_source="strict")` into `$DATA/plugin_cache_v0.2.0`. chrY has no
  rows and is listed with 0 rows and no shard, like SpliceAI's chrMT.
- `hf upload biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous
  <dir> . --type dataset`, then tag `v0.2.0`; card lists source url, md5,
  verified md5, contig row counts and the Ensembl data licence. Verify by
  `hf datasets info --expand siblings` and a fresh download of `manifest.json`.

## 9. Gates

1. Before any engine change: `merged_plugins` chr22 strict md5 concordance
   recorded as the regression baseline.
2. After PR 1 + PR 2: baseline unchanged.
3. PhenotypeOrthologous chr22 and chr1: 100% `both_nonempty_equal` on the
   four fields, strict md5 concordance PASS, `--workers 1` vs `--workers 4`
   body byte-identical, CSQ header byte-identical.
4. Whole sample: per-contig strict concordance on chr1-22 before the cache
   is tagged on the Hub.

## 10. PR series

| # | repo | content | depends on |
|---|---|---|---|
| 1 | datafusion-bio-functions | `gff` provider arm + tests | — |
| 2 | datafusion-bio-functions | `lookup = "interval"` build + runtime + tests | 1 |
| 3 | vepyr-plugins | validator, manifest, README, skill doc | 2 (for the build to run) |
| 4 | vepyr | pin bump, profile, scripts, fixtures, docs | 1, 2 merged; 3 tagged |
| 5 | Hub | cache upload + card | 3 tagged, 4 green |

Per the vepyr-fix discipline: draft PRs, bot review loop to green, no merges
by the agent; the human merges and re-pins.

## 11. Risks

- `$transcript->{_gene}->stable_id` versus the CSQ `Gene` column: assumed
  equal for Ensembl transcripts. The chr22 oracle confirms it before engine
  code is written (grep the reference for a populated line on a
  well-known gene and compare with the `Gene` column).
- Engine repo local checkout is on `fix/mnv-allele-trim-parity`, which has
  diverged from the pin; the work branches from `origin/master`.
- VEP's `--compress_output bgzip` must exist in the 116.0 image; fall back to
  piping through `bgzip` if not.
- GSD is not initialised in vepyr; the user approved bypassing it for this
  work on 2026-09-11.
