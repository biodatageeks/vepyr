# Plugin cache QA profile: invariants, content profile, and dataset-card publishing

**Date:** 2026-09-05
**Status:** Approved (design)
**Implementation repo:** `vepyr` (`e2e-testing/scripts/cache_qa/`)
**Related:** `docs/plugins.md` (cache layout), `docs/downloads.md` (published caches),
`e2e-testing/scripts/comparison/` (package pattern to follow)

## Problem

A plugin cache is a directory of per-contig Parquet shards plus a `manifest.json`
(see `docs/plugins.md`). The builder records row, warm and cold counts per shard and,
since engine PR #228, the verified source digests, but nothing checks the shards
themselves after a build, and the Hugging Face dataset cards
(`biodatageeks/vepyr_116_GRCh38_plugin_<name>`) describe the data by hand.

Two gaps follow:

1. A shard that violates a structural assumption the runtime relies on (row order,
   allele form, schema, contig) is only caught when an annotation run mismatches VEP.
2. Nothing on the card tells a user what is actually in the files: null shares,
   value ranges, category mixes, per-contig sizes.

This design adds one Polars-based tool that verifies the structural invariants,
profiles the content, writes a machine-readable `qa_profile.json`, regenerates a
`## Quality profile` section in the dataset card, and, on request, publishes shards,
manifest, JSON and card as one Hugging Face commit tagged with the vepyr-plugins
release. A failed invariant blocks publishing.

Polars is the engine deliberately: the repo already depends on it, most checks are
domain-specific and would be custom code under dbt, Soda or Great Expectations as
well, and the Polars streaming engine handles the 8.4 billion CADD rows without a
second runtime. Revisit Soda Core over DuckDB only if plugin QA becomes recurring
monitoring with alerting and history.

## Scope

In scope:

- Structural invariants (hard failures) and a content profile (informational) over a
  local cache root `<root>/plugin/<name>/`.
- `qa_profile.json` written next to the shards' staging copy and uploaded with them.
- A regenerated card section between marker comments in the plugin's `README.md`.
- A `--publish` path that stages shards, `manifest.json`, `qa_profile.json` and
  `README.md` by hard link and runs one `hf upload` commit plus a tag move.
- Unit tests on synthetic shards; no network in tests.

Out of scope for this version:

- Source coverage (needs the source file; a build-time concern).
- Lookup smoke tests through `annotate()`.
- Thresholds on profile metrics (null share, ranges). Profile numbers are reported,
  never judged.
- Reading shards from `hf://` paths. The scan source is one function argument, so this
  is a later addition without redesign.
- Rewriting the hand-written `Total ≈ … rows` line in the card's `## Contents`
  section. The new section carries the same numbers.

## Inputs

- `<root>/plugin/<name>/manifest.json` as written by `build_plugin_cache`. Fields used:
  `plugin_name`, `key_columns`, `match_columns[].column`, `value_columns[].column`,
  `value_columns[].type`, `chroms[]` (`chrom`, `file`, `rows`, `warm`, `cold`),
  `cache_source_version`, `allele_match`.
- The shards named by `chroms[].file`. A contig listed with `rows == 0` may have no
  file (SpliceAI chrMT); any other missing file is an invariant failure.
- The vepyr-plugins source manifest is *not* read. Everything needed is in
  `manifest.json`, so the tool works on a downloaded cache too.

Dedup policy comes from the manifest's `assume_unique` key, which engine PR
biodatageeks/datafusion-bio-functions#234 adds to `CacheManifest` as an optional
boolean: `true` when the build skipped dedup on the source's claim, `false` when dedup
ran, absent when unknown (a cache built before the key existed, or one that carried
chromosomes from such a cache). `false` fails on duplicates; `true` reports them as a
warning. When the key is absent the tool falls back to a per-plugin table
(`clinvar`, `alphamissense`, `dbnsfp` deduplicated; `spliceai`, `cadd` assume unique)
and says so in the invariant detail; an unknown plugin without the key is treated as
deduplicated (strict).

## Invariants

Each invariant yields `pass`, `fail` or `warn`, a one-line detail, and where useful a
per-contig breakdown. Any `fail` makes the run exit 1 and disables `--publish`.
`warn` never blocks.

| id | check | failure detail |
|---|---|---|
| `schema` | Shard columns are exactly key columns, match columns, value columns and `tier`, in that order, with the manifest's types (`Utf8`→`String`, `Float32`, `Int32`, …; `chrom` String, `start`/`end` UInt32, `tier` Int8). | first differing column per shard |
| `contig` | Every `chrom` value equals the shard's contig (`chr22.parquet` → `chr22`). | count of foreign rows per shard |
| `order` | Rows are non-decreasing on the lexicographic key `(tier, start, allele_string, match columns…)`. This is the total order the tier stage emits since engine #230 and what `PageDir` lookups assume for `(tier, start)`. | count of descending steps per shard |
| `tier_domain` | `tier` is 0 or 1 and never null. | count of other values |
| `manifest_counts` | Per contig, `rows`, warm (`tier == 0`) and cold (`tier == 1`) counts equal `manifest.json`. | expected vs found per contig |
| `manifest_files` | Every manifest contig with `rows > 0` has its file; no `chr*.parquet` exists that the manifest does not list. | missing / unlisted names |
| `positions` | `start >= 1` and `end >= start - 1` (an insertion has `end = start - 1`). | count of violations per shard |
| `allele_form` | `allele_string` is `REF/ALT` with both parts non-empty. For `allele_match == "minimised"` plugins, REF and ALT do not share a first base unless one of them is `-`. | count of violations per shard |
| `duplicates` | No two rows share (key columns + match columns). `fail` for deduplicated plugins, `warn` for `assume_unique` plugins. | duplicate row count per shard |

Implementation notes:

- `order` and `positions` are per-shard lazy scans reduced to counts:
  `pl.col(c).shift(1)` comparisons combined into one lexicographic
  "descending step" expression, then `.sum()`. No sort, no collect of the shard.
- `duplicates` groups by the key on the streaming engine and counts groups with
  `len > 1`; the per-shard grouping fits because a shard holds one contig.
- `allele_form` uses `str.split_exact("/", 1)` and first-character comparison.
- `schema` uses `scan_parquet(...).collect_schema()` and reads no data.

## Content profile

Computed once over all shards with one `pl.scan_parquet([files])`, grouped by `chrom`
for the contig table and ungrouped for the plugin totals, collected on the streaming
engine (`collect(engine="streaming")`).

Per contig: `rows`, `warm`, `cold`, `warm_share`, `bytes` (file size), `start_min`,
`start_max`.

Per column, for every match and value column (key columns and `tier` get only the
null share):

- `null_share`, `empty_share` (empty string or `.`), both per contig and overall.
- `distinct`: exact `n_unique` when the overall value is at most 10,000, otherwise
  `approx_n_unique`, flagged `approx: true`.
- `numeric`: present when the column is numeric, or is text and at least one value
  casts to Float64 (`cast(pl.Float64, strict=False)`). Holds `parsable_share`, `min`,
  `max`, `mean`, `p50`, `p95`. Quantiles come from the streaming engine and are
  approximate; `min`, `max`, `mean` and the shares are exact.
- `top_values`: the ten most frequent values with counts when `distinct <= 50`.

Scores stored as text (CADD, SpliceAI DS, most dbNSFP columns) are stored that way so
the CSQ output reproduces the source formatting; `parsable_share` is the useful
signal for them, and the numeric summary describes the parsable part only.

## Outputs

### `qa_profile.json`

```json
{
  "plugin": "clinvar",
  "cache_source_version": "v0.1.1@3e1c0394…",
  "generated_at": "2026-09-05T12:00:00Z",
  "tool": {"vepyr": "0.4.0", "polars": "1.39.3", "schema_version": 1},
  "status": "pass",
  "invariants": [
    {"id": "order", "status": "pass", "detail": "0 descending steps in 25 shards"},
    {"id": "duplicates", "status": "warn", "detail": "412 duplicate keys (assume_unique source)",
     "per_contig": {"chr1": 40, "chr2": 33}}
  ],
  "summary": {"rows": 4439569, "warm": 113018, "cold": 4326551, "bytes": 85012345, "contigs": 25},
  "contigs": [
    {"chrom": "chr1", "file": "chr1.parquet", "rows": 401099, "warm": 10445, "cold": 390654,
     "warm_share": 0.026, "bytes": 7400000, "start_min": 925952, "start_max": 248936893}
  ],
  "columns": [
    {"name": "clnsig", "role": "value", "dtype": "String", "null_share": 0.0, "empty_share": 0.0012,
     "distinct": 31, "approx": false,
     "top_values": [["Uncertain_significance", 1900000], ["Likely_benign", 1200000]],
     "numeric": null,
     "per_contig": {"chr1": {"null_share": 0.0, "empty_share": 0.001}}}
  ]
}
```

`status` is `fail` if any invariant failed, else `warn` if any warned, else `pass`.
`schema_version` lets a later reader tell the layout apart.

### Card section

Regenerated between `<!-- qa-profile:start -->` and `<!-- qa-profile:end -->`. If the
markers are absent the section is inserted before `## Usage`; if that heading is
absent it is appended. The section is:

```markdown
## Quality profile

Generated 2026-09-05 by `profile_plugin_cache.py` (vepyr 0.4.0, Polars 1.39.3) from
the shards in this commit; machine-readable copy in [`qa_profile.json`](qa_profile.json).

### Invariants

| check | status | detail |
|---|---|---|
| schema | ✅ pass | 25 shards match the manifest |
| duplicates | ⚠️ warn | 412 duplicate keys (assume_unique source) |

### Contigs

| contig | rows | warm | cold | warm % | size |
|---|--:|--:|--:|--:|--:|
| chr1 | 401,099 | 10,445 | 390,654 | 2.6% | 7.4 MB |
| … | | | | | |
| **total** | **4,439,569** | **113,018** | **4,326,551** | **2.5%** | **85 MB** |

### Columns

| column | role | type | null % | empty % | distinct | numeric (min / p50 / p95 / max) | top values |
|---|---|---|--:|--:|--:|---|---|
| clnsig | value | String | 0.00 | 0.12 | 31 | — | Uncertain_significance (1.9M), Likely_benign (1.2M), … |
| am_pathogenicity | value | Float32 | 0.00 | — | ~68M | 0.000 / 0.121 / 0.912 / 1.000 | — |
```

Per-contig column detail stays in the JSON only; the card shows plugin-wide column
stats to keep the section readable for dbNSFP's 19 value columns.

Rendering is idempotent: running the tool twice on the same shards yields the same
README bytes except for `generated_at`.

## Package layout and CLI

```
e2e-testing/scripts/profile_plugin_cache.py     entry point (argparse, calls cache_qa.cli.main)
e2e-testing/scripts/cache_qa/__init__.py
e2e-testing/scripts/cache_qa/manifest.py        load + validate manifest.json, contig/file resolution, dedup policy table
e2e-testing/scripts/cache_qa/invariants.py      one function per invariant -> InvariantResult
e2e-testing/scripts/cache_qa/profile.py         contig table + column stats (Polars, streaming)
e2e-testing/scripts/cache_qa/report.py          assemble qa_profile.json (dataclasses -> dict)
e2e-testing/scripts/cache_qa/card.py            render section, splice into README between markers
e2e-testing/scripts/cache_qa/stage.py           hard-link staging dir, hf upload + tag move via injected runner
e2e-testing/scripts/cache_qa/cli.py             argument parsing, orchestration, exit codes
```

```
profile_plugin_cache.py <plugin> --root <plugin_cache_root>
    [--out <qa_profile.json>]          default: <root>/plugin/<name>/qa_profile.json
    [--readme <README.md>]             card to update in place; default: none
    [--readme-from-hub <repo>]         fetch the current card from the Hub first, then update it
    [--publish <repo> --tag <tag>]     stage + upload + tag; requires a README source
    [--commit-message <text>]
    [--json-only]                      skip the card even if --readme is given
```

Exit codes: 0 pass or warn; 1 an invariant failed (nothing uploaded); 2 usage or
I/O error (missing manifest, unreadable shard).

`--publish` sequence: verify `status != "fail"`; build the staging directory
(`<scratch>/stage_<plugin>/`) with hard links to every listed shard plus
`manifest.json`, the freshly written `qa_profile.json` and `README.md`; run
`hf upload <repo> <stage> . --repo-type dataset --commit-message …`; delete and
recreate `<tag>` on the new head; read the repo back through the Hub API and confirm
every staged file's size (and LFS sha256 where present) matches, and that the tag
points at the new head. Any mismatch exits 1 after printing what differed.

The Hub commands go through a small `Runner` protocol (`run(argv) -> CompletedProcess`)
injected by `cli.py`, so tests substitute a fake and the real path shells out to `hf`.

## Error handling

- Missing or malformed `manifest.json`: exit 2 with the path and the missing key.
- A shard the manifest lists but the file is absent with `rows > 0`: `manifest_files`
  fails; the profile still runs on the shards present so the report is complete.
- A shard that Polars cannot open: exit 2 naming the file; no partial JSON is written.
- `--publish` without `--readme` or `--readme-from-hub`: exit 2 (the card would be
  stale on the Hub).
- `hf` not on PATH or not logged in: exit 2 before any upload with the command that
  failed.
- Verification after upload failing: exit 1; the commit stays on the Hub (uploads are
  not reverted) and the message says which file or tag to inspect.

## Testing

Unit tests under `tests/test_cache_qa_*.py`, following `tests/test_comparison_*.py`:
Polars builds three-contig synthetic caches (a few hundred rows) in `tmp_path`
together with a matching `manifest.json`, through one fixture that returns a builder
so a test can perturb a single aspect.

- `invariants`: one passing case and one failing case per invariant (wrong column
  type, foreign contig row, one swapped row pair, `tier = 2`, off-by-one manifest
  count, unlisted shard, `end < start - 1`, shared leading base, duplicate key), plus
  the `assume_unique` warn path and the missing-file-with-zero-rows allowance.
- `profile`: known-answer stats (null and empty shares, exact distinct, numeric
  min/max/mean on parsable text, top values, per-contig rows and bytes).
- `report`: JSON keys and `status` aggregation.
- `card`: insertion before `## Usage`, replacement between existing markers,
  idempotence, and the fallback append.
- `stage`: hard links exist for every listed file; the fake runner receives the
  expected `hf upload` and tag argv; verification failure surfaces as exit 1.
- `cli`: exit codes 0, 1 and 2 for the three outcomes; `--publish` refused on `fail`.

Runtime target on the build host: seconds for ClinVar and AlphaMissense, under a
minute for dbNSFP and SpliceAI, single-digit minutes for CADD. Peak memory bounded by
the streaming engine; no shard is collected whole.

## Rollout

1. Land the package and tests.
2. Run it on the five v0.1.1 caches in `~/workspace/data_vepyr/plugin_cache_v0.1.1`;
   publish the four public ones with `--publish … --tag v0.1.1` so the cards gain the
   section; dbNSFP stays local (cannot be published, see `docs/downloads.md`).
3. Replace the ad-hoc publish script used on 2026-09-05 with `--publish`.
4. Add a short "Quality profile" paragraph to `docs/downloads.md` pointing at the
   section and the JSON.
