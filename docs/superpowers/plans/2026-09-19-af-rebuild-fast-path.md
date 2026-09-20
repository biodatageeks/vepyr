# Allocation-free AF rebuild in the variation lookup

**Date:** 2026-09-19
**Workflow:** `docs/runbooks/applying-a-vepyr-fix.md`, step 1b artifact
**Status:** plan confirmed 2026-09-19 (step 1d). Baseline captured before any edit (strict md5 22/22; WGS 78.5 s @8 workers, 313.3 s @1). Engine PR: biodatageeks/datafusion-bio-functions#250 (draft). vepyr pin PR: this branch.

## The defect

A performance defect, not a correctness one. After every variation point lookup,
`SinglePathParquetVariationLookup::to_logical_batch` rebuilds the 27 allele
frequency `Utf8` columns from the struct-of-arrays Parquet encoding
(`af_<grp>_alleles: List<Utf8>`, `af_<grp>_freqs: List<Float32>`). Measured on
HG002 at engine `96021f2`, `everything=True`, workers=1, warm cache:

| | chr22 | chr1 |
|---|---|---|
| variation lookup (`variation_take`) | 1.27 s | 10.37 s |
| of which the AF rebuild | 0.47 s (37%) | 4.46 s (43%) |
| frequency values formatted | 2.05 M | 19.7 M |

94% of the rebuild is `reconstruct_af_group_string` at ~200 ns per value; the
split back into 27 columns is 5%. The cause, all anchors at the pinned rev
(`datafusion/bio-function-vep/src/`):

- `parquet_cache/encode.rs:27-46` `format_g` — per value: `format!("{:.*e}")`,
  `split_once('e')` + `parse::<i32>()`, then a second `format!`, plus
  `strip_trailing_zeros` (`:49-55`) and `format_exp` (`:58-61`), each returning a
  fresh `String`. 3 allocations on the fixed branch, 4 on the scientific one.
- `encode.rs:254,258` — `alleles.value(r)` / `freqs.value(r)`: a per-row
  `ArrayRef` slice + downcast, twice per group per row.
- `encode.rs:263-274` — `Vec<String>` parts per population, `format!("{}:{}")`
  per value, `parts.join(",")`, `segments.join("|")`.
- `parquet_cache/variation_lookup.rs:281,292` — the joined group string is built
  only to be handed to `unbundle_af_columns` → `cache/af_bundle.rs:121-139`
  `split_group`, which splits it straight back into the members.

Trigger: any run that projects an AF column (`af`, `af_1kg`, `af_gnomade`,
`af_gnomadg`, `max_af`, or `everything`). Every matched row pays it; the rebuild
runs unconditionally on the taken rows (`variation_lookup.rs:133`).

### Ensembl VEP source (runbook 1a-bis)

No VEP rule is being ported, so there is no rule to quote for the *change*. What
the source does settle is the contract the change must not break. At
`release/116.0`, `modules/Bio/EnsEMBL/VEP/OutputFactory.pm:1216-1217`
(`add_colocated_frequency_data`, which starts at `:1184`):

```perl
foreach my $pair(split(',', $ex->{$key})) {
  my ($a, $f) = split(':', $pair);
```

VEP consumes the cache's `allele:freq[,allele:freq…]` text per population
verbatim. The engine's rebuilt strings are that same input, so the only valid
rule for this patch is **byte-identical strings**. The formats agent confirmed
the text originates as a verbatim copy of the raw cache column
(`bio-format-ensembl-cache/src/variation.rs:608-611`, no float parse) and
measured, on the raw 115 and 116 chr22 caches, that all ~131 M AF values have at
most 4 significant digits and that `%.4g` reproduces every one of them (0
differences, double arithmetic). The engine enforces the f32 version of that at
build time: `encode.rs:135` rejects any value where `format_g4(value) != text`.

Consequence: **`format_g4` defines what a stored value is and must not change.**
The new formatter has to equal it, not replace it.

## Ownership

| Repo | Verdict | Evidence |
|---|---|---|
| `datafusion-bio-functions` | **owner** | formatter, 2-array codec, bundle split, lookup and the only consumer all live in `bio-function-vep`; sole production caller of `reconstruct_af_group_string` is `variation_lookup.rs:281` |
| `vepyr` | carrier | no reference to any of these symbols; it never parses or reformats AF text (`src/vepyr/__init__.py`, `src/annotate.rs`); change = the rev at `Cargo.toml:122` + `Cargo.lock:1276` |
| `datafusion-bio-formats` | untouched | one-way dependency (functions → formats); copies AF text verbatim at cache build time only; stays `tag = "v1.12.1"` in both downstream repos |

### Base commit

Engine `origin/master` = `96021f2` = the rev vepyr pins (as last fetched), so
the engine PR starts from exactly the code vepyr runs. The human's engine
checkout is on `fix/mnv-allele-trim-parity` (46d8c20, pre-#248, with untracked
files) and **must not be used**: `resolve_and_take` has a different signature
there. Work happens in a new `git worktree` off a freshly fetched
`origin/master`; the human's working tree is not touched.

## What proves it: tests and gates

**There is no test that can fail today and pass after**, because nothing about
the output is wrong today. Said outright per step 1b. What replaces it:

1. **Characterisation test, committed first, green on the OLD code.**
   `to_logical_batch` output is currently pinned by nothing: both end-to-end
   tests (`variation_lookup.rs:336-451`, `write.rs:529-641`) look up 1–2 columns
   by name. New test writes a small shard and asserts the full logical batch:
   column order (non-AF in file order, then each present group's members
   contiguously in `AF_GROUPS` order), every member field
   `Utf8`/nullable/no metadata, `null_count == 0` on all 27 (null group → `""`,
   never NULL — `lookup_exec.rs:1721-1722` would otherwise turn `""` into NULL
   in `lookup_variants()` output), schema metadata carried, full group width
   even when one member is projected, and exact bytes for: multi-allelic rows,
   a null group, a null freq slot mid-row (`"A:0.1||A:0.3"`), scientific
   notation, `0`, `1`, an empty list, an empty batch.
2. **Differential test, new path vs the existing one** —
   `reconstruct_af_members(a, f, n)` must equal
   `split_group(&reconstruct_af_group_string(a, f, n)?, n)` cell for cell over
   generated inputs, including the cases the writer never produces but the
   reader must survive: a freqs list shorter than `n_alleles * n_pops` (the
   `idx < fr.len()` guard at `encode.rs:268`; with direct child-array indexing a
   missing guard reads the *next row's* values), sliced and concatenated
   `ListArray`s (`phys` is a `concat_batches` result, `variation_lookup.rs:225`),
   null list slots with non-empty offset ranges, alleles null but freqs not.
3. **Formatter differential** — `write_g4` vs `format_g4`: every 4-significant-
   digit decimal across exponents −12..=3, each at −1/0/+1 ulp (catches rounding
   carries such as 9.9996e-3 → `0.01`, the `e == -4`/`-5` switch, `e == 3`),
   plus a fixed-seed sample of 2 M random finite f32 in normal CI; the
   exhaustive f32 sweep as an `#[ignore]` test. `-0.0`, subnormals, values
   ≥ 1e4 and negatives go through the fallback and are asserted equal.
   Non-finite values panic in `format_g4` today (`encode.rs:34`) and cannot be
   stored (the build gate panics the same way); the fallback preserves that and
   a test pins it.

Session evidence that this is achievable (scratch prototype, not in any repo):
all 27 columns byte-identical on the real matched rows of chr22 (119,840) and
chr1 (1,182,911); formatter equal on 20.3 M extra values; e2e output md5
identical on chr22 and chr1.

**Which harness can see the change.** The vepyr pytest goldens contain **zero**
scientific-notation AF values and no multi-allelic records, so a green
`uv run pytest` says nothing about the exponent branch. The strict md5 gate does
reach it (562 of 50,861 chr22 records carry sci-notation AFs in the VEP 116
reference), but only for the matched allele. Unmatched-allele segments are
visible to no vepyr harness — hence tests 1 and 2 live in the engine.

**The perf gate cannot see the change.** `tools/vepyr-fix/compare_runs.py` sums
`*_ms` keys from `[VEP_PIPELINE_TRACE]`; the `stage=lookup` events carry counts
only, and the lookup is timed by a different facility (`VEP_LOOKUP_PROFILE*`,
`cache/lookup_exec.rs:688-693`) that the runbook does not export and the script
does not parse. So the gate is still run — it must show **no regression** — but
the *win* is read from `[vep-lookup-profile]` lines captured on both bookends by
adding `VEP_LOOKUP_PROFILE_DETAILED=1` to the runbook environment, identically
for baseline and final.

## The change, per repo, in dependency order

### datafusion-bio-functions (one PR, branch `perf/af-rebuild-fast-path`)

**Commit 1 — characterisation test (old code, green).** Test 1 above, in
`parquet_cache/variation_lookup.rs` tests. No production change.

**Commit 2 — the fast path.** In `parquet_cache/encode.rs`:
- `write_g4(out: &mut impl fmt::Write, f: f32)` — one `{:.3e}` into a 48-byte
  stack buffer, exponent parsed from bytes, the 4 digits placed by hand
  (scientific when `e < -4`, fixed otherwise), trailing zeros stripped on the
  digit array. Anything outside `f.is_finite() && f > 0.0 && e < 4` (and `0.0`,
  handled first) delegates to `format_g4`. Same `{:.3e}` call as today, so the
  rounding is the standard library's in both paths.
- `reconstruct_af_members(alleles, freqs, n_pops) -> Result<Vec<StringArray>>` —
  downcast the two child arrays once, walk `value_offsets()`, one
  `StringBuilder` per population written through `fmt::Write`, null-row check
  first, `idx < f1` guard kept, the two type-mismatch errors kept
  (`encode.rs:255-261`). A memo `HashMap<u32, (u8, [u8; 14])>` keyed on the f32
  bits, **local to the call**: `to_logical_batch` is `&self` on an `Arc`-shared
  lookup used by several workers, so a shared map would need locking and a size
  bound. The measured ~10× was taken with a fresh map per pass (chr1: 53,420
  distinct values among 19.7 M), so per-call scope loses nothing that was
  measured.
- `variation_lookup.rs:272-292` — push the member fields/arrays directly at the
  point where the group string is appended today; drop the `unbundle` call.
  Order, nullability, metadata and full group width are what test 1 pins.
- `format_g4`, `reconstruct_af_group_string`, `split_group`,
  `unbundle_af_columns` stay, unchanged and `pub`: the first is the build gate's
  definition of lossless, the rest are the differential oracle, and removing
  `pub` items from a tagged crate is an API break this PR has no reason to make.
  Doc comments that describe the read path are updated
  (`variation_lookup.rs:9-12`, `encode.rs:148`, `:231-239`, `af_bundle.rs:191`;
  CI runs `cargo doc` with `-D warnings`).
- Tests 2 and 3.

**Commit 3 — make the rebuild visible (see Decisions, Q2).** An `af_rebuild`
sub-timer in the `[vep-lookup-profile-detail]` stages line next to
`variation_take`, and the matching update to the formatting test at
`lookup_exec.rs:2280-2299`. Profile output only; no `[VEP_PIPELINE_TRACE]` field,
because a phase present on one side only fails `compare_runs.py` by design.

CI that gates it: `cargo fmt --check`, `clippy --all-targets --all-features -D
warnings`, `cargo doc -D warnings`, `cargo test --all`, and the `cache-builder`
lib tests; toolchain 1.91.0. Title in the repo's style:
`perf(vep): rebuild the AF columns without per-value allocation (chr1 lookup −35%)`
— numbers replaced by the step-8 measurements before hand-off.

### vepyr (one PR, branch `chore/bump-engine-af-rebuild`, pinned to the engine PR head)

- `Cargo.toml:122` rev → engine PR head; `Cargo.lock:1276` follows.
- A comment block above the pin in the existing convention (`# bio-functions
  <sha>: …`, "pinned by rev because no release has been cut", bio-formats stays
  `v1.12.1`). The current pin has no block (#107 changed only the rev line); this
  PR adds the block for the new rev and does not backfill the old one.
- This plan file. No source, test, version or docs change.
- After the engine PR merges, the human re-pins to the merge commit (runbook
  step 9).

### datafusion-bio-formats

Unchanged. No code path involved at read time, no dependency on the engine, and
the tag must stay `v1.12.1` on both sides or the formats crates compile twice.

## Pin cascade

```
formats   v1.12.1 (unchanged, tag)
   ↑
functions perf/af-rebuild-fast-path   (formats pin unchanged)
   ↑
vepyr     chore/bump-engine-af-rebuild  → rev = functions PR head
```

Two PRs, not three. Any review fix pushed to the engine PR moves its head and
requires re-pinning vepyr before re-requesting review or measuring (step 7).

## Expected gate movement

| Gate | Expectation |
|---|---|
| strict md5, every contig | **no movement** — any moved digest is a bug in this patch |
| engine unit tests | new tests pass on old code (1) and on new code (1, 2, 3) |
| `compare_runs.py` phases / wall / RSS | **within bars, no claim of a win**: the lookup overlaps a slower downstream stage, so at workers=1 wall moved 17.11 → 16.92 s on chr1 (−1%, inside shared-host noise). At 8 workers the cores are contended, so some of the saved CPU may surface as wall; that is a hypothesis to be measured, not a promise. |
| `[vep-lookup-profile]` `variation_take` | **−25 to −35%** (scratch build: chr22 1.27 → 0.92 s, chr1 10.37 → 6.75 s) |
| user CPU | about **−10%** on a single-contig run (chr1 32.3 → 29.1 s) |
| peak RSS | flat; the memo is ≤ ~55 k entries × ~24 B ≈ 1.3 MB per call and the joined group strings are no longer materialised |

If the md5 gate is green and `variation_take` does not drop by at least 20% on
the native release build, the patch has not done what this plan says and is not
handed off.

## Decisions (runbook 1c/1d, answered 2026-09-19)

Answers: **1 — full gates; 2 — yes, include the sub-timer; 3 — two PRs (engine + vepyr pin), no separate PR for the timing write-up; 4 — yes, fresh worktree off `origin/master`; 5 — not answered, the proposed GSD bypass is applied.** The questions as asked:

1. **Gate scope.** Runbook default is the full 22-contig strict md5 (~20 min)
   plus the WGS worker sweep at 8 and 1 workers (~15 min + warm-up), each run
   twice (baseline and final) — roughly 1.5–2 h of machine time, ~29 GB of
   output per WGS run on a volume with 87 GiB free (floor 65). #107, also a pure
   refactor, used chr22 md5 only and no sweep. Proposed: **full md5 on both
   bookends; WGS sweep on both bookends with `VEP_LOOKUP_PROFILE_DETAILED=1`
   added; plus chr1 and chr22 single-contig runs** for the "exact timing" the
   request asks for. Alternative: chr1 + chr22 md5 and single-contig timings
   only.
2. **Commit 3 (the `af_rebuild` sub-timer).** Proposed: include it, so the next
   change to this code has a number to be held to. Alternative: leave the
   profile untouched and time with out-of-tree instrumentation again.
3. **"As a separate PR".** Read as: engine PR + a separate vepyr pin-bump PR,
   both separate from any other work in flight. Confirm, or say if the timing
   write-up itself is meant to be its own PR.
4. **Engine working tree.** Proposed: `git fetch` + `git worktree add` off
   `origin/master` inside `~/research/git/datafusion-bio-functions`, leaving the
   checked-out `fix/mnv-allele-trim-parity` tree and its untracked files alone.
5. **GSD.** `CLAUDE.md` asks for a GSD entry point before edits, but
   `.planning/ROADMAP.md` does not exist and the recent plans recorded an
   explicit GSD bypass in favour of this runbook. Proposed: same here.

Out of scope, noted so they are not folded in: the AF sub-flags are not
forwarded on the `output_vcf=` path (`src/annotate.rs:190-355`, pre-existing);
passing f32 values downstream instead of strings (would change `MAX_AF`
tie/rounding behaviour); any cache layout change.
