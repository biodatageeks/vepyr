# Plugin annotation slowdown: vepyr CSQ re-parse + engine serial plugin takes

Date: 2026-09-25. Status: plan awaiting go-ahead (vepyr-fix runbook, step 1d). Decisions so far: A1+A2 (yes), A3 plugin subset (yes), two vepyr PRs (yes), scope = A + B now, typed engine plugin columns as the next PR.
Found while benchmarking the Polars supplement (`perf/polars-supplement`, Experiment C).

## The defect

With five plugin caches (ClinVar, SpliceAI, CADD, AlphaMissense, dbNSFP) on the merged
cache, `annotate(...).collect()` on HG002 chr22 (50,861 records, `everything=True`,
workers=1) takes **24.1 s** and peaks at 8.0 GiB. The same input written by
`output_vcf` takes **9.3 s** at 1.2 GiB. With no plugins the two take 3.3 s and 4.2 s.
This affects every record shape, because the cost scales with the CSQ string length ×
the plugin field count, not with the variant type.

Measured by ablation (one subprocess per config, median of 2 after a warm-up;
scratchpad `plugin_debug/ablate.py`, `ablate2.py`):

| config (merged, w=1) | LazyFrame collect | output_vcf |
|---|---|---|
| no plugins | 3.33 s | 4.17 s |
| + clinvar (6 fields, 1.7 MB shard) | 6.24 s | 4.31 s |
| + alphamissense (2 fields) | 5.03 s | – |
| + cadd (2 fields, 948 MB) | 7.22 s | – |
| + spliceai (9 fields, 382 MB) | 9.52 s | – |
| + dbnsfp (19 fields, 85 MB) | 11.07 s | 4.56 s |
| all 5 (38 fields) | 24.11 s | 9.32 s |
| LF, `skip_csq=False`, no plugins | 4.08 s | – |

A `sample` profile of the LF runs puts the busy CPU in `_polars_runtime`
(TwoWaySearcher, ListStringChunkedBuilder), not in `_core`.

The defect has two independent causes.

1. **vepyr (owner), ~15 s of the 24 s.** `src/vepyr/__init__.py:2043-2062` builds each
   plugin column with its own `CSQ.str.split(",").list.eval(str.split("|").list.get(i))`.
   That re-tokenises the whole everything-CSQ once per plugin field (38× per batch).
   It runs unconditionally: `read_plugins` (`:1936-1939`) only feeds flag inference, never
   gates the parse. The same closure runs under `pb.sink_vcf`, so the LazyFrame → VCF
   path pays it too, even though it discards the parsed columns.
2. **engine datafusion-bio-functions (owner), ~5 s of output_vcf's 9.3 s.** At 0cefc99:
   - `plugin_cache/registry.rs:295-323` `take_buffer_all` awaits the five plugins
     serially per buffer under one `block_on` (`annotate_provider.rs:6071-6073`);
   - each `PluginLookup::take_buffer` (`lookup.rs:160-236`) opens the shard twice, at
     `:174` and `:210`;
   - all five plugins are Point lookups; chr22 is 11 buffers of 5000, so 110 serial
     open+stream round trips;
   - there is no plugin timer in `VEP_ENGINE_PROFILE` (`annotate_provider.rs:77-121`), and
     `IoCounters` (`lookup.rs:165`) are never logged. The per-plugin split of the 5 s is
     therefore unmeasured: cadd-only and spliceai-only on output_vcf were not run.

**datafusion-bio-formats: untouched.** At v1.13.0 it reads no plugin Parquet. Its VCF
serializer writes CSQ as one Utf8 cell (`bio-format-vcf/src/serializer.rs:900-950`).
No pin moves.

## Ensembl VEP source (runbook 1a-bis)

This fix ports no Ensembl rule. It must change no output byte: the CSQ layout, the
plugin order (the `--plugin` order, then `--custom` last, already enforced through
csq_rank and the plugins list) and the values stay as they are. The gate is byte-identical
strict md5 against the existing VEP 116 plugin reference. So nothing is quoted from
ensembl-vep, and that is deliberate.

## Changes, in dependency order

### A. vepyr: parse CSQ once, and only for the plugin columns the query reads
- A1: build the entry/field token list once per batch (`split(",")`, then `split("|")` once
  per entry, keeping only the trailing `n_plugin` fields). Then derive every plugin column
  from it. The current semantics are preserved exactly:
  - per-variant: first non-null, cast to the dtype;
  - per-feature: a typed list;
  - `"" → null`;
  - `null_on_oob`.
- A2: parse only the plugin columns the query reads (`read_plugins`), not all 38.
  Already true today, and kept: when a query reads neither `CSQ` nor any plugin column,
  vepyr drops `plugin_cache_root`/`plugins` from the engine options and skips CSQ
  (`__init__.py:1976-1983`). So a plain core-column `select()` pays nothing. A2 closes the
  partial case, e.g. `select("CADD_PHRED")`, which today parses every plugin field.
- A3 (user decision 2026-09-25): when the query does not read the raw `CSQ`, pass the
  engine only the plugins that own a read plugin column, keeping their configured order.
  The engine then skips the other plugins' lookups. CSQ positions shift with the subset:
  the trailing plugin fields are those of the selected plugins only, so the field offsets
  are computed from the subset specs. When `CSQ` is read, or with no projection (bare
  `collect()`, `pb.sink_vcf`), every configured plugin is passed as today, so the CSQ string
  and its declared layout do not change. Tests: `select` of one plugin column gives values
  identical to the full run, for each of the 5 plugins and for two-plugin mixes,
  per-variant and per-feature.
- Blast radius, checked:
  - `_flags_for_projection` / `plugin_column_inputs` and the provenance header keep the
    same inputs;
  - the shadow/rename rules (`:1841-1858`) are unchanged;
  - `fields=` and `preserve_record_layout` are independent.

### B. engine: measure first, then run plugin takes concurrently
- B1: instrument. Add a `plugin_take` timer to `VEP_ENGINE_PROFILE` around
  `annotate_provider.rs:6040-6076`, plus per-plugin elapsed time and a log of
  `IoCounters.snapshot()`. This settles how the 5 s splits by plugin before any
  behaviour change.
- B2: run `take_buffer_all` with `join_all` over the plugins. It preserves input order, so
  the CSQ block order is unchanged. Each `PluginLookup` owns immutable path/meta/page_dir,
  and nothing is shared or mutable.
- B3 (only if B1 shows opens matter): reuse one open file per shard per contig instead of
  two opens per take.
- **Next PR, decided 2026-09-25 (user chose "1+2 now, 3 next"):** typed plugin output columns from the engine (it
  would retire A entirely and drop the LF plugin memory to near the no-plugin frame; `PluginScalar` exists at the `csq::field_suffix` call sites
  `annotate_provider.rs:7121-7140, 7186-7203`), and projection-driven plugin skipping
  in the engine.

## Tests (runbook step 3)

No existing fixture can SEE a performance defect. Tests guard semantics, and a
measurement shows the defect.
- vepyr: characterisation tests that pass before and after A:
  - plugin column values and dtypes for per-variant and per-feature fields;
  - a CSQ entry with fewer fields (the `null_on_oob` path);
  - an empty value (becomes null);
  - an escaped comma inside a value;
  - a select without plugin columns returns the same frame. A2: no plugin column is
    computed.

  Existing coverage: `tests/test_annotate.py:273-382, 463, 1213-1234` and
  `tests/test_vcf_columns.py:354-373, 1164-1239`. The failing test for A2 asserts
  that a projection with no plugin column does not evaluate the plugin parse, via a spy
  on the expression builder. The rest is regression protection.
- engine: extend `tests/plugin_csq_emission.rs` to five plugins × several buffers, and assert
  that the CSQ bytes are identical to the serial path. There is no bench at the pin; B1's timer
  is the measurement.

## Gates

- **Parity (must not move):**
  - strict md5, vepyr vs VEP 116, merged_plugins profile, chr22:
    `run_comparison.py --release 116 --profile merged_plugins --chroms 22 --comparison-mode md5 --md5-mode strict`;
  - the LazyFrame → `pb.sink_vcf` body is still byte-identical to output_vcf;
  - the supplement's parity gate (`performance-tests/polars-integration/scripts/verify.py`,
    18 queries) still passes 18/18.
- **Performance (expected to move), chr22 w=1 via the ablation scripts:**
  - LF + 5 plugins: 24.1 s → about 9-10 s after A1+A2 (the LF - output_vcf gap closes),
    then lower after B;
  - output_vcf + 5 plugins: 9.3 s → estimate 5-7 s after B2 (bounded below by the
    slowest single plugin; B1 fixes the estimate);
  - no-plugin paths: unchanged, ±noise.
  Before/after with `tools/vepyr-fix/compare_runs.py` where it applies.

## Pin cascade

- functions: branch `perf/plugin-take-concurrent` off 0cefc99 (origin/master). One PR with B1+B2(+B3).
- vepyr: branch `perf/plugin-csq-single-parse` off master. PR 1 = A (no pin change,
  independent of the engine). PR 2 (or the same PR, stacked) = pin bump to the functions PR head,
  re-pinned to the merge commit after merge.
- formats: none.

## After the fixes

Re-run the supplement's Experiment C plugin rows (P1-P5) and continue its timed pipeline
(VEP fork matrix, Experiment B) on the fixed build. Its tables report the fixed numbers
and a sentence on the fix.
