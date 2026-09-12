# Remove the fjall/Lance-era scaffolding from the variation lookup

**Date:** 2026-09-12
**Workflow:** `docs/runbooks/applying-a-vepyr-fix.md`, step 1b artifact
**Status:** implemented. Engine PR biodatageeks/datafusion-bio-functions#248, vepyr PR biodatageeks/vepyr#107, both draft, both awaiting review.

## The defect

This is a maintenance change, not a parity fix. The fjall key-value backend and
the Lance backend were removed from `datafusion-bio-function-vep` in engine
PRs #181 (Lance-only) and #184 (Parquet backend); neither crate is a dependency
today. What remains in the variation lookup path is the scaffolding that used to
choose between them, now with exactly one branch reachable, plus names that
describe storage that no longer exists:

- `probe_warm_position` is a stub with ten unused parameters that always returns
  `LookupDecision::UseFjall`; two of the three `LookupDecision` arms are
  unreachable.
- `WarmColdVariationBackend`, `WarmColdVariationIndexMode` (whose only variant
  names a bloom filter that does not exist), `warm_source_label`, `KvMatchMode`
  (one variant, stored as `_match_mode`), `PendingColdProbe.position_key`
  (always `0`), two aliased imports from `cache/variant_key.rs` that are never
  used, and `chrom_to_code` computed only to feed an unused parameter.
- `cache/key_encoding.rs` (the fjall byte-key encoder, 399 lines) and
  `cache/variant_key.rs` (188 lines) have no live caller outside each other and
  the dead sites above; the contig registry in `key_encoding.rs` has zero
  writers anywhere in the workspace, so `chrom_to_code` is pure and unobserved.
- `ParquetPositionCursor` / `new_cursor` / the `_cursor` parameter of
  `resolve_and_take` are a placeholder the body never reads.
- `LookupProfile` declares 108 counters. 31 are written in production, 18 are
  never assigned anywhere, ~59 are assigned only inside one unit test, and
  `primary_match` has no writer at all. Two of the 31 (`warm_probes`,
  `warm_not_covered`) are written only by the stub being deleted.
- `IoCounters` are created in every take and `snapshot()` is never called in
  the workspace, so the one measurement the Parquet path could report (bytes and
  read-ops per take) is discarded.
- `KvLookupExec` / `KvLookupStream` name a key-value store that is not there;
  the `Cold*`/`pending_cold_*` family and the `[vep-kv-profile-detail]` /
  `VEP_KV_PROFILE*` / `VEP_LANCE_*` diagnostics carry the same vocabulary; five
  error strings still say "Lance batch", four in `cache/build.rs` say "Lance
  variation batch", one in `lookup_provider.rs` says "Lance variation cache
  root".
- Public compatibility aliases: `AnnotationBackend::parse` and
  `CacheFormat::parse` still accept `"lance"`, the `cache_format` option accepts
  `"lance"`, and `to_options_json_with_backend` ignores its argument. vepyr
  itself still passes the literal `"lance"` as the annotation-store backend
  token in two places and returns a permanent `None` fjall slot in the
  build-stats tuple.

No record shape triggers anything; annotation output must be byte-identical
before and after.

### Ensembl VEP source (runbook 1a-bis)

Not applicable. No Ensembl rule is ported or changed; the change touches no
consequence, allele, HGVS or output logic. Stated here so the omission is
deliberate rather than skipped.

## Ownership

| Repo | Verdict | Evidence |
|---|---|---|
| `datafusion-bio-formats` | untouched | no code references fjall, Lance, `KvLookup*`, `key_encoding`, `variant_key`, `VEP_KV*`, `VEP_LANCE*`; no crate depends on `bio-function-vep`; both consumers pin `v1.12.1`. Only history docs mention them (`openspec/changes/add-fjall-variation-lookup/`, still not archived, human follow-up) |
| `datafusion-bio-functions` | **owner** of Parts A and B, and of the alias removal in Part C | every item above lives in `datafusion/bio-function-vep/src/{cache/lookup_exec.rs, cache/key_encoding.rs, cache/variant_key.rs, cache/row_index.rs, parquet_cache/variation_lookup.rs, cache_builder.rs, annotation_store.rs, vcf_sink.rs, annotate_provider.rs, lookup_provider.rs}` |
| `vepyr` | **carrier**: pin bump plus the two token literals, the tuple slot, CLAUDE.md | `src/annotate.rs:157,495`; `src/lib.rs:185-193`; `src/vepyr/__init__.py:794`; `tests/test_build_cache.py:455,479,482`; `src/vepyr/_core.pyi:26,38`; `CLAUDE.md:13` |

Analysis sources: formats agent read the `feat/cooler` worktree (31 commits past
`v1.12.1`, all cooler work); functions agent read worktree HEAD `46d8c20` on
`fix/mnv-allele-trim-parity` and confirmed the four lookup files are
byte-identical at the pinned `9f74fd9`; vepyr agent read `master` at `62112d6`.
Line numbers quoted in this plan are HEAD-of-worktree numbers for
`lookup_exec.rs` (identical file at both revs) and may shift by ~200 lines in
`annotate_provider.rs` at the pin.

### Base commit

vepyr pins engine `rev = 9f74fd9` ("chore: release v0.21.0"). The engine
worktree HEAD `46d8c20` has diverged: the pin is **not** an ancestor of HEAD
(HEAD lacks six master commits, the pin lacks HEAD's four). The refactor branch
is therefore created from `origin/master` (which contains `9f74fd9`), not from
the current worktree HEAD, and the worktree branch is left where it is.

## What proves it: tests and gates

There is no failing test to write in the runbook's sense; the property is "no
behaviour changed". The proof is:

1. `cargo clippy --all-targets --all-features -- -D warnings`,
   `cargo doc --no-deps --all-features`, `cargo test --all` in the engine (CI
   `ci.yml:45-62`), with the tests listed under "tests that must change"
   rewritten rather than deleted where they still assert something real.
2. vepyr `cargo test --no-default-features --features mimalloc`, clippy, and
   `uv run pytest` including the seven golden suites
   (`tests/test_golden*.py`), which compare CSQ bytes against Ensembl VEP
   fixtures on the chr1 golden and the MNV fixture.
3. **Quality gate:** chr22 strict-mode md5 body digests, baseline before any
   edit and again on the stacked branches, byte-identical:
   ```
   cd e2e-testing/scripts
   uv run python run_comparison.py --release 116 --chroms 22 \
       --comparison-mode md5 --md5-mode strict --bgzf
   ```
   Reference data is present under `~/workspace/data_vepyr` (VEP 116 merged
   output + tbi, `cache/116_GRCh38_merged`, input VCF at the legacy root
   location, FASTA + fai).
4. **Performance gate:** see open question 1. The runbook's WGS sweep needs
   ~65 GiB free and the data volume has 34 GiB. Nothing in the change alters a
   code path that runs per row except removing a redundant per-probe call, so
   the proposal is to record the chr22 md5 run's wall time before and after as
   a coarse alarm and not run the WGS sweep.

Coverage gap to state plainly: no existing test asserts on the `DisplayAs`
string, the "Lance batch" error strings, or the env var names, so those renames
are proven only by compilation and the md5 gate.

## The change, per repo, in dependency order

### datafusion-bio-functions (one PR, three commits)

**Commit 1: delete dead code, no rename.**

- `cache/lookup_exec.rs`
  - remove `probe_warm_position`, `LookupDecision`, and the `match` at the call
    site; the row loop pushes one `PendingColdProbe` per probe start directly.
  - hoist `ensure_parquet_lookup` out of the per-probe path: call it once per
    distinct chrom in the existing per-chrom grouping loop, immediately before
    `resolve_and_take`. This keeps the "cell holds another contig" guard for
    every chrom a batch contains (the drain already groups by chrom), and only
    drops the redundant repeat calls.
  - remove `WarmColdVariationBackend`, `WarmColdVariationIndexMode`,
    `warm_source_label`, `VariationLookupStorage::as_str`,
    `format_variation_lookup_profile_line`'s `warm_dir` / `cold_dir` /
    `variant_bloom_index_dir` arguments, `KvMatchMode` and the `match_mode` /
    `_match_mode` fields and constructor parameter, `PendingColdProbe.position_key`,
    the two `warm_*` aliased imports, the `chrom_to_code` import and call, and
    the `"position_key"` entry in `cold_parquet_projection_columns`.
  - `LookupProfile`: keep the 29 production-written counters that survive the
    stub's removal, the eight fed from `ColdChunkProbeMetrics` (all live), and
    add `variation_read_bytes` / `variation_read_ops` from the take. Remove the
    18 never-written fields, the ~59 test-only fields, `primary_match`, and the
    detail lines that print only zeros (`io`, `warm`, `position_index`,
    `variant_bloom`, `cold_parquet_row_groups`, `cold_parquet_pages`).
  - `input_slice_rows`: drop the `is_parquet()` branch (always true).
- `parquet_cache/variation_lookup.rs`: remove `ParquetPositionCursor`,
  `new_cursor`, and the `_cursor` parameter; return the `IoCounters` snapshot on
  `TakenVariationRows` so the profile can report it (this touches only
  `variation_runtime.rs`; `CoalescingAsyncReader::new` in `page_dir.rs` is left
  as is, so `sift.rs`, `write.rs`, `plugin_cache/lookup.rs` are untouched).
- delete `cache/key_encoding.rs` and `cache/variant_key.rs`; move
  `ResolvedRowIds` from `cache/row_index.rs` into `cache/variation_runtime.rs`
  and delete `row_index.rs`; update `cache/mod.rs`; fix the dangling doc
  comment at `plugin_cache/normalize.rs:19`.
- `lookup_provider.rs`: drop the `KvMatchMode::Exact` argument and the
  discarded `parquet_backend` field/setter (`let _ = self.parquet_backend;`).

**Commit 2: rename, symbol-scoped (no textual `warm`/`cold` substitution).**

- `KvLookupExec` → `VariationLookupExec`, `KvLookupStream` →
  `VariationLookupStream`, `new_parquet` → `new`, `kv_profile_enabled` →
  `lookup_profile_enabled`, `DEFAULT_LANCE_LOOKUP_PROCESS_BATCH_ROWS` →
  `DEFAULT_LOOKUP_SLICE_ROWS`, `PendingColdProbe` → `PendingProbe`,
  `ColdProbeResult` → `ProbeResult`, `ColdChunkProbeMetrics` → `ProbeMetrics`,
  `pending_cold_*` → `pending_*`, `cold_parquet_*` profile fields →
  `variation_*`, `cold_parquet_load` → `variation_take`.
- Not renamed, on purpose: `probe_floor_pos`, `warm_up_skipped_rows`, the grid
  warm-up machinery in `regions.rs` and `annotate_provider.rs`, and every use
  of `tier` in `parquet_cache/write.rs` / `page_dir.rs` (a real property of the
  shard layout).
- Env vars and prefixes: `VEP_LOOKUP_PROFILE`, `VEP_LOOKUP_PROFILE_DETAILED`,
  `VEP_LOOKUP_SLICE_ROWS` read first, with `VEP_KV_PROFILE`,
  `VEP_KV_PROFILE_DETAILED`, `VEP_LANCE_LOOKUP_PROCESS_BATCH_ROWS` as fallbacks
  for one release; `VEP_LANCE_PROFILE` folded into the detailed flag. Prefixes
  `[vep-kv-profile]` / `[vep-kv-profile-detail]` → `[vep-lookup-profile]` /
  `[vep-lookup-profile-detail]`. Update the tracked consumers in the same
  repo: `.claude/skills/vep-perf-profiling/SKILL.md` and
  `scripts/breakdown.sh`; the untracked `scripts/visualize_*.py` are the
  human's and are not edited.
- Strings and comments: the five "Lance batch" errors in `lookup_exec.rs`, the
  two "Lance SIFT" errors in `annotate_provider.rs`, the four "Lance variation
  batch" errors in `cache/build.rs`, "requires a Lance variation cache root" in
  `lookup_provider.rs`, `KeyU64LanceLookup` in the `sift.rs` module doc, the
  `cache_common.rs` header, the fjall comments at `annotate_provider.rs:873`
  and `:13994`, `lookup_exec.rs:2551`, and the stale `lance-cache` feature
  comment at `.github/workflows/ci.yml:27`.

**Commit 3: drop the compatibility aliases.**

- `annotation_store.rs:21`: accept `"parquet"` only; message names the
  accepted value.
- `cache_builder.rs:29`: same for `CacheFormat::parse`.
- `annotate_provider.rs` `cache_format` option: `"parquet"` only.
- `vcf_sink.rs`: remove `to_options_json_with_backend`,
  `cache_format_for_backend`, and the `_backend` parameter of
  `cache_source_type_from_cache_source_for_backend`. The public
  `annotate_to_vcf(…, backend, …)` and the SQL `annotate_vep(vcf, cache,
  backend, …)` **keep their arity** so vepyr does not need a signature change
  in this cascade; the token is still validated, it just has one legal value.

**Tests that must change (engine):**

| Test | Action |
|---|---|
| `lookup_exec.rs` `lookup_profile_detail_line_formats_probe_decode_match_breakdown` | rewrite against the reduced profile: stages line, match line, variation take line with bytes/ops |
| `lookup_exec.rs` `variation_lookup_profile_line_identifies_parquet_backend` | reduce to storage + cache_root assertions |
| `lookup_exec.rs` `warm_source_label_is_parquet` | delete with the function |
| `variation_lookup.rs:418`, `write.rs:603` | drop `new_cursor()` / cursor argument |
| `cache_builder.rs:280` `cache_format_parser_accepts_parquet_and_lance_alias` | invert: `"lance"` is rejected |
| `annotation_store.rs:41` `backend_parse_accepts_parquet_and_lance_alias` | invert |
| `annotate_provider.rs` `cache_format_is_accepted` (~`:16008`) | send `"parquet"`; add a rejection case for `"lance"` |
| `vcf_sink.rs:2192` `test_to_options_json_with_backend_emits_cache_format` | delete with the function |
| `vcf_sink.rs:2540` `provenance_records_the_real_cache_format_not_the_backend_token` | pass `"parquet"`; keep the "no `lance` in provenance" assertion |
| `key_encoding.rs`, `variant_key.rs` unit tests | deleted with the modules |

### vepyr (one PR, pinned to the engine PR head)

- `Cargo.toml:122` rev → engine PR head, with the paragraph the pin comment
  convention (`Cargo.toml:40-121`) expects; `Cargo.lock` follows.
- `src/annotate.rs:157` and `:495`: `"lance"` → `"parquet"`; rewrite the
  comments at `:152-156` and `:492-494`. **This must land in the same tree as
  the pin bump**: with the alias gone, the old literal fails at runtime
  (`DataFusionError::Plan`), not at compile time.
- `src/lib.rs:129,146,185-193`: return `(entity, parquet_files)`;
  `src/vepyr/__init__.py:794` unpack becomes two names;
  `tests/test_build_cache.py:455,479,482`; `src/vepyr/_core.pyi:26,38` (note
  the stub is already drifted, missing `entity` and `chroms`; fix the return
  annotation only).
- `CLAUDE.md:13`: "Parquet and fjall (embedded KV store) formats" → "Parquet".
- Not touched: the tests that assert fjall is *absent* or *rejected*
  (`test_build_cache.py:47-56,170-175,980-981,1134-1135`,
  `test_annotate.py:1035-1044`, `src/annotate.rs:577-582`,
  `test_comparison_cli.py:41-45`, whose `--backend` was never a flag), and the
  untracked `docs/assets/annotation_fork_scaling.svg` /
  `e2e-testing/scripts/plot_annotation_scaling.py` captions.
- Branch created from `master` at `62112d6`; the dirty `notebooks/annotate.ipynb`
  and the untracked `.planning/`, `e2e-testing/115-116/` etc. stay out of it.

### datafusion-bio-formats

No change, no PR, no pin movement.

## Pin cascade

```
formats  v1.12.1  (unchanged)
   └─ functions PR: refactor/remove-fjall-lance-scaffolding  (from origin/master)
         └─ vepyr PR: refactor/remove-fjall-lance-scaffolding  (rev = functions PR head)
```

After merge the human re-pins vepyr from the PR head to the merge commit.

## Expected gate movement

| Gate | Expected |
|---|---|
| chr22 strict md5, 116 merged | identical body digest before and after |
| engine `cargo test --all`, clippy, doc | green after the test rewrites above |
| vepyr pytest incl. golden suites | green; `test_build_cache` tuple unpack updated |
| performance | none expected; the only per-row change is fewer calls to an idempotent guard. WGS sweep not run unless question 1 says otherwise |
| stderr diagnostics | `[vep-kv-profile*]` lines change prefix and lose their all-zero sections; `EXPLAIN` shows `VariationLookupExec` |

## Decisions (runbook 1c/1d, answered 2026-09-12)

1. **Performance gate: skipped entirely.** No WGS sweep (34 GiB free vs ~65
   GiB needed) and no chr22 wall-time alarm. The PR body states this. The
   quality gate (chr22 strict md5 before and after) is the only measured gate.
2. **PR granularity:** one engine PR with three commits (delete / rename /
   drop aliases) and one vepyr PR pinned to its head.
3. **`IoCounters`: left as is.** Not surfaced in the profile, not removed. The
   profile therefore does not gain `variation_read_bytes` / `variation_read_ops`;
   `TakenVariationRows` and `variation_runtime.rs` are unchanged except for
   hosting `ResolvedRowIds`.
4. **Env var fallbacks: none.** Only `VEP_LOOKUP_PROFILE`,
   `VEP_LOOKUP_PROFILE_DETAILED` and `VEP_LOOKUP_SLICE_ROWS` are read.
   `VEP_KV_PROFILE`, `VEP_KV_PROFILE_DETAILED`, `VEP_LANCE_PROFILE` and
   `VEP_LANCE_LOOKUP_PROCESS_BATCH_ROWS` are removed; the engine repo's
   `vep-perf-profiling` skill is updated in the same PR.

Go-ahead: given by the human through these answers, after the plan and the
open questions were posted. Step 2 (chr22 md5 baseline) starts after this
edit; no code file is touched before that baseline completes.
