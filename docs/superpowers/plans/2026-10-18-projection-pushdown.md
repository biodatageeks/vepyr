# Polars projection pushdown to the engine — Plan (vepyr-fix step 1)

**Request (verbatim):** "I would like to have an option for Polars to be able to
project minimal subset of columns needed to answer the query and if some stages
can be skipped since no need to return some columns it should be skipped"

**Status:** analysis done, awaiting human go-ahead. No repo edited.

**Decisions (human, 2026-10-18):**
1. Scope E1–E6 (E6 added 2026-10-18 after the variant-level query examples);
   E7 (VCF scan narrowing) deferred; E8 (projection-driven context column
   lists: output-only transcript/motif columns) rejected — context load is
   0.40 s of a ~6 s chr1 run, so ≤1–3% wall for a column-by-column parity audit.
2. Pushdown on by default: `annotate(..., projection_pushdown=True)`; `False`
   restores `SELECT *` with the flags as given.
3. The engine derives the flag groups (everything / HGVS / colocated / AF
   groups) from the projection; vepyr sends `everything` plus the column list
   and deletes `_HGVS_COLUMNS`, `_COLOCATED_COLUMNS`,
   `_EVERYTHING_ONLY_COLUMNS` and `_flags_for_projection`.

## The defect / gap

`annotate()`'s LazyFrame turns a Polars `select()` into narrower engine *flags*
only (`src/vepyr/__init__.py:223` `_flags_for_projection`, `:2305`
`_batch_source`), then always runs `SELECT *` / `SELECT * EXCLUDE ("CSQ")` over
`annotate_vep(...)` (`src/annotate.rs:696-705`). DataFusion's projection therefore
never reaches `AnnotateProvider::scan` (engine v0.23.1
`annotate_provider.rs:16127`), so the engine's existing projection-driven skips
(cache-only variation columns `:16200-16229`, `skip_typed_cols` `:5794`,
SIFT/DOMAINS/miRNA/genomic-reference gates) never fire on the LazyFrame path, and
several stages that no projected column needs are never skipped by the engine at all.

Trigger: any `annotate(...).select([...]).collect()` that does not read every column.

## Evidence (measured 2026-10-18, scratch prototype, HG002, 116 ensembl, workers=1)

Prototype: env-gated explicit SELECT list from vepyr + instrumented engine
(scratch worktrees, not committed). Output row-hash identical in every arm.

- Parquet columns read on chr1 (identical in both arms except variation):
  transcript 49/76, exon 6/32, translation_core 10/10, regulatory 5/29,
  motif 11/34, translation_sift 3/3 (everything only), variation 18/26 (no AF
  flags) or 24/26 (AF flags); explicit SELECT drops 5 variation columns
  (clinical_impact, minor_allele, minor_allele_freq, clinvar_ids, cosmic_ids;
  dbsnp_ids stays because variation_name pulls it).
- **Regression in the engine today** (reached by any projection that omits the
  cache-only columns): typed-column builder resolves `cache_*` columns per row
  with `schema.index_of(..).ok()` (`annotate_provider.rs:8006,8021,8030,8039,8051,8066,8081`);
  on a miss arrow-schema 58.3 formats an error listing every field
  (`schema.rs:398-406`). typed_columns 1.11→1.76 s on chr1. Scratch fix with
  `column_with_name`: 1.03 s, md5 identical.
- chr1 A/B with that fix (6 kept reps/arm, interleaved, load 5.5–12):

  | query | wall | CPU | peak RSS | variation take |
  |---|---|---|---|---|
  | Consequence,SYMBOL,IMPACT,Feature | −4% (6/6) | −11% | −39% | −10% |
  | +Existing_variation,AF,gnomADg_AF | −6% (6/6) | −8% | −35% | −7% |
  | most_severe_consequence only | −15% (6/6) | −21% | −52% | −10% |
  | SIFT,PolyPhen,HGVSc (everything) | −3% (4/6) | −4% | −27% | −5% |

- Not skipped today, measured on chr1: the full variation lookup runs with
  check_existing=false (727k probes, ~4.1 s variation_take, 13 columns);
  the transcript engine (~4.5 s evaluate_prepared) runs even for
  variant-level-only projections; `cached_fast_rows` is always 0.

## Ensembl rule (1a-bis)

No VEP rule is being ported; the invariant is *every returned column is
byte-identical to the same column from a full run*. The rule that licenses the
biggest skip, at `ensembl-vep release/116.0`:

- `modules/Bio/EnsEMBL/VEP/CacheDir.pm:168` — the cache variation source is
  created only `if($self->param('check_existing') && $info->{variation_cols})`.
- `modules/Bio/EnsEMBL/VEP/Config.pm:439-443` — `check_frequency af af_1kg
  af_gnomad af_gnomade af_gnomadg max_af pubmed` set `check_existing => 1`.
- `CacheDir.pm:165` — regulatory source only with `--regulatory` (vepyr always
  annotates regulatory; not changed here).

So VEP never performs the existing-variant lookup without check_existing; the
engine doing it unconditionally is pure cost when nothing consumes it.

## Ownership

| Repo | Verdict | Why |
|---|---|---|
| datafusion-bio-functions | **owner** (stage skipping) | every skip decision, context column lists, lookup projection and typed builders live in `annotate_provider.rs` / `cache/lookup_exec.rs` / `parquet_cache/variation_lookup.rs` |
| vepyr | **owner** (pushdown + option) | chooses the SQL (`src/annotate.rs:696`); maps frame columns to engine columns |
| datafusion-bio-formats | **untouched** | `VcfTableProvider` already skips INFO/FORMAT parsing at group level when not projected (`physical_exec.rs:265-295`, all four record loops); vepyr already narrows INFO/FORMAT fields via the constructor (`src/annotate.rs:622-655`). The engine requesting the full VCF schema (`lookup_provider.rs:272-279`) is an engine item (E7, optional). Ensembl-cache crate is not on the annotation read path. |

## Changes, in dependency order

### Engine (datafusion-bio-functions, branch off v0.23.1 → master)

- **E1. Hoist `cache_*` index resolution out of the row loop** (7 sites above)
  to once per batch via `column_with_name`; harden the per-probe
  `BatchProbeIndices::new` (`lookup_exec.rs:1038`), `resolve_batch_coloc_indices`
  (`:1775`), `batch_output_indices` (`:1656`) the same way. Low risk; fixes a
  regression that exists today. ~0.7 s/chr1 on a typed projection.
- **E2. One "needed outputs" derivation** in `scan()` from the projection
  (plus `fields`), shared by `scan_with_transcript_engine_partitioned`
  (`:5791`) and `annotate_worker_window` (`:13869`), replacing the two copies of
  skip_csq/skip_typed_cols. Per decision 3 it also derives the flag groups
  from the projected columns, generalising `from_options_json_with_field_pruning`
  (`:1332-1420`, today keyed on `fields` and only when typed columns are
  absent, `:5804`): everything-only, HGVS (hgvsc/hgvsp/shift), colocated
  (check_existing) and each AF group are switched off when no projected column
  (or CSQ field) needs them; flags the caller did not set are never switched
  on. The engine-side table becomes the single column→group map (the
  gnomAD-subpopulation granularity follows the engine's AF groups). No
  projection (`None`) keeps today's behaviour exactly, so the VCF sink and
  md5 gate are unaffected. Medium risk (two derivations unified; HGVS and
  everything gating move from vepyr into the engine).
- **E3. Skip the variation lookup** when `!check_existing` and no cache-only
  column is projected: zero-probe pass-through in `VariationLookupExec` that
  keeps row order/partitioning. Drop the dead `consequence_types` /
  `most_severe_consequence` from `preferred_columns` (absent from 116 shards)
  and fix the stale `clin_sig_allele` comment (`:1076`). Read only the AF
  column pairs of the enabled AF groups (`variation_projection_columns`,
  `lookup_exec.rs:476-498`, today all three pairs or none). ~4.1 s CPU/chr1.
  Medium risk: run pool (`:12366`), grid (`:14752`), warm-up paths.
- **E4. Per-column typed builders** (bitmask of projected typed columns)
  instead of all-or-nothing, and per-column stage gates derived from it:
  `needs_sift_polyphen` becomes "SIFT or PolyPhen typed column projected, or
  CSQ emits them" (today `!skip_typed_cols || …`, `:2001-2010`), so the
  `translation_sift` scan, `sift_load` (`:15481`) and the per-consequence
  `lookup_sift_polyphen` calls are skipped unless SIFT/PolyPhen is needed;
  `needs_domains` / `needs_mirna` (`:6261-6274`) likewise. Verified gap
  2026-10-18 (chr22, instrumented): `select(Consequence, DOMAINS)` keeps
  `everything` and reads translation_sift 3/3 though no SIFT/PolyPhen column
  is returned; `select(Consequence)` and `select(Consequence, MANE_SELECT)` do
  not read it. Low risk, moderate gain.
- **E5. Gate secondary work on need:** plugin take only when CSQ/plugin
  columns are needed (`:6124`); decode `protein_features` only when DOMAINS is
  needed (`:10203`); HGVS shift prelude + window hydration only when
  HGVSc/HGVSp/HGVS_OFFSET/USED_REF is needed (`:6693`, `:13156`) — hydration
  also affects RefSeq/BAM-edited sequences, so the gate must include a merged
  or RefSeq cache.
  **Refined 2026-10-18:** hydration (`hydrate_transcript_cdna_from_reference`,
  `:8957`) rewrites `cdna_seq` for any transcript without a cached
  `spliced_seq`, Ensembl included, and feeds codon/amino-acid consequences —
  so the FASTA reader is KEPT whenever the transcript engine runs (it cost
  0.000 s on chr1 Ensembl). Only the HGVS shift prelude (0.49 s/chr1 under
  everything vs 0.006 s without FASTA) is gated on an HGVS-dependent column.
  Today vepyr drops `reference_fasta_path` for flagless projections; that is
  value-checked on the Ensembl cache only and may be a latent mismatch on
  merged/RefSeq — the LF parity check compares against a full run on merged.
- **E6. Variant-level bypass:** projection of only VCF
  columns + Allele/VARIANT_CLASS/Existing_variation/frequency/cache-only →
  skip context load, SIFT and the transcript engine (~4.5 s + context/chr1).
  Riskiest: row gating (star, ref==alt, null, ALT=. drop, `:6393-6465`,
  `:6625-6650`) must be reproduced exactly.
- **E7 (proposed: defer). Narrow the engine's VCF scan** (`lookup_provider.rs:279`
  `.select(...)`) to the columns the lookup and passthrough need. Small gain
  given vepyr already narrows INFO/FORMAT.

**Never skipped for any entry-based projection:** regulatory and motif
(feed most_severe_consequence `:6763`, entry count/order, PICK `:3233`);
`mature_mirna_regions` (drives Consequence `mature_miRNA_variant`,
`transcript_consequence.rs:1960`).

### vepyr (after the engine rev exists)

- **V1. Push the column list:** `create_annotator(..., *, columns: list[str] | None = None)`
  keyword-only with default (27+ test fakes hard-code the 5-arg signature);
  `annotate.rs` builds `SELECT "a","b",...` when given, else today's SQL.
  Mapping from `needed` to engine names: plugin columns → `CSQ`; `_shadowed`
  renames reversed; carried `INFO_*`/`fmt_*` pass through; `_vcf_info_keys` /
  `_vcf_format_keys` only when needed, else also drop `vcf_record_layout`
  from engine opts; stale `INFO_CSQ` never; `fields=` (no select) → 
  `selected_dataframe_columns`; schema probe stays `SELECT *`; Polars 1.44.2
  passes `['chrom']` for `select(pl.len())` and `None` for `count()` /
  `select(pl.all())`.
- **V2. The option:** `annotate(..., projection_pushdown: bool = True)`;
  `False` = `SELECT *` with flags as given (A/B and debugging).
- **V2b. Remove the Python flag tables** (decision 3): `_flags_for_projection`
  (`__init__.py:223-262`) and the three column constants (`:61-145`) go;
  vepyr keeps only what the engine cannot know — plugin-column → CSQ mapping,
  plugin match-template `required` fields (`plugin_column_inputs`), INFO/FORMAT
  field narrowing (`fields_for_query`), regions. `TestProjectionPruning`
  (`tests/test_annotate.py:1590`) is rewritten against the engine behaviour;
  its value-identity test (`:1797-1829`) is kept as the guard.
- **V3. Pin bump** `Cargo.toml:217` only (formats pins unchanged).
- **V4. Docs:** `docs/dataframes.md:198-245`, `docs/architecture.md:89-127` +
  `docs/diagrams/polars-pushdown.mmd` and SVGs, `docs/plugins.md:134`,
  `annotate()` docstring `__init__.py:1785-1792`.

## Failing tests (written first, after go-ahead)

- Engine: `scan(Some(projection))` vs `scan(None)` value equality for typed
  projections *without* cache-only columns (no such test exists — every in-file
  `.scan(` passes `None`); a lookup-skipped run asserting zero probes and equal
  values; typed-builder per-column projection equality; a `Consequence,DOMAINS`
  projection asserting the SIFT store is never opened and values are equal.
  Fixture: the in-file
  synthetic contexts, plus an e2e chr22 run for real caches.
- vepyr: recording fake asserting the SQL column list per projection
  (plugins→CSQ, shadowed, layout columns, `pl.len()`); real chr22
  projected-vs-full `collect()` equality across the 4 benchmark projections,
  `pl.len()` row count, `workers=2`, a regions predicate, and plugins.
- Known fixture blind spots: chr1 golden has no MNV, multi-allelic or ALT=.
  record; HG002 chr22/chr1 do carry multi-allelics after normalisation —
  `pl.len()` row-count parity must be checked on a file with ALT=. records.

## Pin cascade

Engine change → engine rev/tag (v0.23.2) → vepyr `Cargo.toml:217` only.
No formats change, so `:218-219` stay at v1.14.0.

## Expected gate movement

- **md5 parity (output_vcf):** unchanged. The VCF sink already projects
  (`vcf_sink.rs:1934-1941`); E1/E3 do not fire under `--everything`.
- **WGS perf gate (output_vcf):** neutral within noise; E1 removes a
  per-row cost the sink path does not hit (it never selects typed columns).
- **LazyFrame (new A/B, the harness above on chr22 + chr1):** V1+E1 alone
  gives the table above. E3 should take the non-colocated projections' ~4 s
  lookup CPU off the run (chr1 base CPU −30% or more; wall depends on overlap,
  lookup_wait 0.8 s today). E4/E5 small. E6 (if taken) would make
  variant-level projections several times faster.
