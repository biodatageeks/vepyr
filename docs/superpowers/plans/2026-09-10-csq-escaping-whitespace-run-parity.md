# CSQ escaping: collapse whitespace runs, and reconcile the escapers

**Issue:** `biodatageeks/vepyr#93`
**Date:** 2026-09-10
**Status:** awaiting human go-ahead (runbook step 1d)
**Baseline commit:** vepyr `fedc945` (= `origin/master` head; no `[patch]`, lock resolves
functions `97aa8ca5`, formats `v1.12.1` = `419be98`)

## The defect

VEP applies four substitutions to every CSQ field value, in order, at
`ensembl-vep release/116.0 modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm:401-404`:

```perl
$data =~ s/\,/\&/g;      $data =~ s/\;/\%3B/g;
$data =~ s/\s+/\_/g;     $data =~ s/\|/\&/g;
```

`s/\s+/_/g` is **quantified**: a run of whitespace collapses to exactly one
underscore. The engine's `csq_escape`
(`datafusion-bio-functions@97aa8ca5 datafusion/bio-function-vep/src/annotate_provider.rs:2535-2565`)
substitutes per character, so `A  G` becomes `A__G` where VEP writes `A_G`.

**Record shape that triggers it:** any CSQ-destined string value containing two
or more consecutive whitespace characters, or a tab, in `CLIN_SIG`, `PUBMED`,
`SWISSPROT`, `TREMBL`, or a `Utf8` plugin field.

## Ownership

| Repo | Verdict | Change |
|---|---|---|
| `datafusion-bio-formats` | **carrier, write side only** | **none** — not even a pin bump |
| `datafusion-bio-functions` | **owner** | the fix |
| `vepyr` | **carrier** | pin bump only (`Cargo.toml:95`) |

**formats owns nothing here.** It has no CSQ escaper, no whitespace normaliser
and no INFO escaping/unescaping at all. All four VCF record loops — including the
indexed/tabix path at `physical_exec.rs:2939` that a `read_record` grep misses —
funnel INFO values through one helper, `load_infos_single_pass`
(`physical_exec.rs:713-809`), whose string arm at `:767-769` appends verbatim.
The write side (`serializer.rs:942-986`) emits the pre-escaped bytes untouched;
its only substitution on any written value is ALT's pipe→comma at `:670`.
`origin/master` **is** `v1.12.1` (zero commits between), so vepyr keeps its tag pin.

**vepyr owns nothing here** either. Its only touch of CSQ is
`SELECT * EXCLUDE ("CSQ")` at `src/annotate.rs:504-508`; `no_escape` is pure
passthrough (`src/vepyr/__init__.py:1384-1385` → `src/annotate.rs:212-213`).
There is no unescape anywhere in the repo.

## What the analysis changed about the issue

The issue is right about the defect and wrong about the fix in two ways. Both
were found by reading the Ensembl source (runbook step 1a-bis), not by comparing
outputs.

### A. There are not two escapers. There are four, and one must NOT collapse runs.

| # | Site | Rule today | VEP counterpart | Correct action |
|---|---|---|---|---|
| 1 | `annotate_provider.rs:2535` `csq_escape` | per-char ws | `VCF.pm:401-404` — `\s+` **run** | **collapse** |
| 2 | `plugin_cache/csq.rs:16-28` `escape_csq_value` | per-char ws, `+ '='→%3D` | same | **collapse**; `=` — see B |
| 3 | `annotate_provider.rs:8358` DOMAINS | `.replace(' ')`, `';'`, `'='` | `OutputFactory.pm:1505` `s/[\s;=]/_/g` — **no quantifier** | **leave per-char**; fix the tab gap |
| 4 | `annotate_provider.rs:3567` `format_prediction` | `.replace(' ')` + `"_-_"→"_"` | `OutputFactory.pm:1819-1820` `s/\s+/_/g` then `s/\_\-\_/\_/g` | **collapse** |

Escaper #3 is the trap. VEP's DOMAINS rule is genuinely per-character, so
collapsing runs there would **create** a mismatch. Its real bug is the opposite
one: `.replace(' ', "_")` misses a tab that VEP's `[\s;=]` catches.

Structural finding the issue misses: **there is no general per-field escaping
pass in this engine.** The CSQ chunk is raw interpolation
(`annotate_provider.rs:6795-6830`), so SYMBOL, BIOTYPE, EXON, HGVSc,
Consequence reach the output unescaped, each value relying on its producing
site. VEP escapes *every* column at `VCF.pm:400-405`. `csq_escape` is not the
general escaper the issue takes it for — it has exactly four call sites
(`:2241` PUBMED, `:2244`/`:2246` CLIN_SIG, `:6620` SWISSPROT, `:6622` TREMBL).

### B. Dropping the `=` arm would regress a measured parity gate

The issue's step 2 says to drop `'=' => "%3D"` from the plugin escaper "to match
VEP". VEP 116 indeed has no such rule — its only `=`→`%3D` is HGVSp at
`OutputFactory.pm:1757`, and it is `no_escape`-gated. **But** the arm is recorded
in `docs/superpowers/handovers/2026-08-26-plugin-golden-vep-parity.md:20-36` as a
measured fix: without it, **556 `ClinVar_CLNVI` entries across 7 records**
mismatched (chr13 ×6, chr17 ×1), all BIC `base_change=…` values. It is what got
the plugin profile to 22/22 strict.

The likely root cause is elsewhere: VCF 4.3 requires `=` in an INFO value to be
`%3D`-encoded, so VEP probably passes ClinVar's already-encoded bytes through
verbatim while something on our side decodes them. That is a different fix in a
possibly different repo, and it is **not** in scope here.

### C. Perl `\s` and Rust `char::is_whitespace` disagree

`VCF.pm` has `use strict; use warnings;` and **no** `use utf8` (verified: no
`use open`/`binmode`/`:encoding` anywhere under `modules/Bio/EnsEMBL/VEP/`), so
`\s` uses byte semantics. Measured on perl 5.28.3:

| codepoint | Perl `\s` | Rust `is_whitespace` |
|---|---|---|
| U+0009–U+000D, U+0020 | yes | yes |
| **U+0085 NEL** | **no** | **yes** |
| **U+00A0 NBSP** | **no** | **yes** |

All four Rust escapers currently over-match on NBSP and NEL. Both are plausible
in free-text ClinVar/phenotype strings.

### D. Ancillary facts worth recording

- Substitution **order is unobservable**: the outputs `&`, `%3B`, `_` contain no
  `;`, whitespace or `|`, and no rule produces a `,`. The four commute. The
  issue's emphasis on order is a non-issue, but both `,`→`&` and `|`→`&` must land.
- The POD table at `VCF.pm:76-78` is wrong **twice**: it claims `= ==> %3B`
  (a typo for `;`) and omits the `|`→`&` rule entirely. Issue's typo claim confirmed.
- **No trim.** Leading/trailing runs each collapse to one `_`; `"  x  "` → `"_x_"`.
- `VCF.pm:390` tests **truthiness**. `'0'` is rescued at `:410-412` and pushed
  **unescaped**; other falsy values become `''` at `:415`.
- `-`→`''` blanking (`VCF.pm:396-398`) applies to every column **except** `Allele`,
  and runs **before** escaping — including plugin columns. So the plugin-side
  comment at `plugin_cache/csq.rs:11-15` is at odds with VEP 116, though
  unreachable in practice.
- **115.2 vs 116.0**: `VCF.pm` differs by one line — the copyright year. Both revs
  `rev-parse`d before diffing (`release/116.0` = `57ea5c52`, `release/115.2` =
  `2beada0d`), so the empty result is real, not a null comparison.
- `ensembl-variation origin/release/116` (`2fb834b9`) has mirror copies in the
  **legacy** `Utils/VEP.pm`, not on the 116 execution path.
- Ported assertions at `t/OutputFactory_VCF.t:244-248` and `:329-334` — the issue
  transcribed both correctly.

## Exposure: currently zero, genome-wide

Measured, not assumed:

| Corpus | Result |
|---|---|
| Real VEP 116 reference `vep_chr1_merged.vcf` (2.4 GB, 323,430 records) | `grep -c '__'` → **0** |
| chr22 cache: variation 15.1M rows, transcript, motif, regulatory, 141,621 protein features | **0** runs, **0** tabs, **0** `=` |
| Plugin caches chr22: cadd 119M, spliceai 52.7M, dbnsfp 1.8M, alphamissense 1.5M, clinvar 96K rows | **0** runs, **0** tabs, **0** `=` |
| Whole-genome strict md5, 22/22 contigs, 4,096,123 records | passing today |

The single-space path *is* heavily exercised and correct: 60,815 `DOMAINS`
analysis values on chr22 alone carry one space (`AFDB-ENSP mappings`,
`PROSITE profiles`, …), and the 22/22 strict pass proves each renders as VEP does.
`ClinVar_CLNDN` already ships with `_` for spaces (ClinVar's own convention), so
the free-text case the issue predicts as riskiest is empty in practice.

**Consequence: this is a correctness/future-proofing fix with no expected output
change on the current corpus** — and a refactor touching a path that is load-bearing
for genome-wide byte parity.

## Failing tests

**Blind spots first.** All seven golden fixtures (`golden`, `golden_merged`,
`golden_merged_pick_allele`, `golden_merged_flag_pick_allele`,
`golden_merged_pick_allele_gene`, `golden_merged_per_gene`, `golden_mnv`) have
**zero** `__`, zero `=` in CSQ, zero tabs — and `DOMAINS` and `CLIN_SIG` are
**empty on every entry of every one**. The golden gate cannot see this defect or
protect the refactor. **Strict md5 is the regression detector.**

### Engine (`datafusion-bio-functions`)

In the `annotate_provider.rs` test module, alongside the five existing
single-character tests at `:19782-19815`:

- `csq_escape("A  G") == "A_G"` — `t/OutputFactory_VCF.t:245`
- `csq_escape("ENST00,00  03|07;301") == "ENST00&00_03&07%3B301"` — `:329-334`
- `csq_escape("a\t  b") == "a_b"` — mixed run
- `csq_escape("A G") == "A_G"` — **positive control**, already passes
- `csq_escape("  x  ") == "_x_"` — no trim
- `csq_escape("a=b") == "a=b"` — built-in does not escape `=` (D2)
- `format_scalar(Str("a=b")) == "a%3Db"` — plugin path still does (D2)
- `format_scalar(Str("-")) == ""` — plugin `-` now blanks (D1); **flips today's
  behaviour**, so this test is the record of the decision
- **negative control for #3:** DOMAINS keeps per-character semantics —
  `"a  b"` stays `"a__b"`
- **new for #3:** a tab in a domain label becomes `_`
- **#4:** `format_prediction` collapses a run
- **D3 predicate, all four escapers:** `\x0B` **is** escaped; NBSP (U+00A0) and
  NEL (U+0085) are **not**. This is the test that fails under both
  `is_whitespace` and `is_ascii_whitespace`, so it pins the explicit predicate.
- **borrowed-`Cow` fast path preserved** (`annotate_provider.rs:19809`)

### vepyr

Per the vepyr analysis, the issue's proposed helpers `_write_plugin_hit_vcf` and
`_first_csq_group` **do not exist**. They are also unnecessary: the existing
`demo_plugin_cache` fixture (`tests/test_annotate.py:128-154`) already keys rows
to golden `INPUT_VCF` variants (chr1:604358 G>C, 604360 T>C). Needed scaffolding
is a `Utf8` manifest variant (current `_FULL_MANIFEST` at
`tests/test_build_plugin_cache.py:12-35` casts to `Float32`) and a source row
whose value is `two  spaces`. Assert through the LazyFrame —
`frame["DEMO"][0] == "two_spaces"` — which is simpler and equally diagnostic.
A tab cannot be used: the source is tab-delimited.

**Hazard to guard:** `src/vepyr/__init__.py:1825` does `.replace("", None)`, so an
empty CSQ token becomes `null`. If `csq_escape`'s `-`→`""` arm leaks onto the
plugin path during unification, every legitimate `-` plugin value silently
becomes `null`. No existing test catches this.

## Change, in dependency order

1. **formats** — none.
2. **functions** —
   a. `csq_escape`: collapse runs via a `prev_ws` flag, single pass, no extra allocation.
   b. `csq_escape` → `pub(crate)`; `plugin_cache/csq.rs` imports it and
      `escape_csq_value` is deleted. Per the analysis this is the *entire*
      visibility change — `lib.rs:42` and `:65` already declare both modules
      `pub mod`, so no module move, no re-export, no cycle. **No `bool`
      parameter**: `-`→`""` now applies on both paths (D1), and the plugin `=`
      step is applied by `format_scalar` around the shared call as a named,
      issue-tracked deviation (D2).
   f. Replace `char::is_whitespace` with the explicit `is_vep_space` predicate
      across all four escapers (D3).
   e. **Spec**: add a "CSQ value escaping" subsection under §5 of
      `docs/superpowers/specs/2026-07-05-custom-vep-plugin-caches-design.md` (D1),
      and correct `plugin_cache/csq.rs:1`, which cites §5.2 for rules §5.2 does
      not contain.
   c. `format_prediction` (#4): collapse runs, matching `OutputFactory.pm:1819-1820`.
   d. DOMAINS (#3): keep per-character; widen `' '` to whitespace so tabs are caught.
3. **vepyr** — `Cargo.toml:95` rev bump + pin-comment rationale (repo convention,
   `Cargo.toml:45-94`); `Cargo.lock:1249` regenerates. **formats stays declared by
   TAG** — `Cargo.toml:84-87` records that cargo keys a git source on its
   *reference*, so a `rev=` of the same commit is a distinct source and the formats
   crates would compile twice (E0308 across `CacheBuilder`).

## Pin cascade

formats: unchanged at `v1.12.1`. functions PR: no formats bump. vepyr PR: pins
the functions PR head. Two PRs, not three.

## Expected gate movement

| Gate | Expectation |
|---|---|
| strict md5, 22/22 | **unchanged, still 22/22** — zero exposure measured above |
| golden (7 fixtures) | **unchanged** — blind to this path |
| perf: phase durations | **unchanged** — one branch in a per-character loop already running |
| perf: wall, peak RSS | **unchanged** |

Any md5 movement means the refactor broke the single-space path that 60,815
chr22 DOMAINS values depend on — that is the signal to watch, not the new tests.

## Decisions taken

### D1 — plugin `-` follows VEP and is blanked (resolved 2026-09-10)

Unifying the escapers collided with `csq_escape` blanking `-` unconditionally
while the plugin path deliberately preserved it
(`plugin_cache/csq.rs:11-15`: *"the built-in `-`→empty convention is deliberately
NOT applied to plugin values, so a legitimate `-` is preserved"*).

**Decision: follow VEP. `-` is blanked for plugin values too, and the unified
`csq_escape` takes no parameter.**

Evidence:

- **VEP blanks plugin `-`, confirmed at the source, not inferred.**
  `OutputFactory/VCF.pm:449` pushes `map {$_->[0]} @{$self->get_plugin_headers}`
  into `@fields`, so plugin columns run through the same
  `for my $col (@{$self->fields})` loop at `:387` — including the `-`→`''`
  blanking at `:396-398`.
- **Unreachable today**, so nothing measurable is at risk either way. All five
  published plugin caches scanned on chr22 for a bare or whitespace-padded `-`
  across every `Utf8` column:

  | cache | rows | Utf8 cols | bare/ws `-` |
  |---|---:|---:|---|
  | alphamissense | 1,466,988 | 4 | none |
  | cadd | 119,170,699 | 4 | none |
  | clinvar | 96,223 | 8 | none |
  | dbnsfp | 1,799,075 | 22 | none |
  | spliceai | 52,715,028 | 8 | none |

- **The green-gate argument for preserving was void.** The 22/22 strict pass
  cannot defend the old behaviour, because the case never arises in the corpus —
  the gate measured nothing there.
- **It makes the unification real.** A single function with no parameter is what
  the issue asks for; one function plus a `blank_dash: bool` would re-encode the
  very split the issue wants removed.

Accepted cost: `PluginScalar::Null` already maps to `""` (`csq.rs:36`), so a
meaningful `-` becomes indistinguishable from a cache miss — and in vepyr,
`src/vepyr/__init__.py:1825` `.replace("", None)` lands both as `null` in the
LazyFrame. **VEP has the identical conflation**, so parity is preserved; a
future plugin whose `-` carries meaning would lose it in VEP too.

**Spec documentation is part of this change.** `plugin_cache/csq.rs:1` cites
"spec §5.2", but `docs/superpowers/specs/2026-07-05-custom-vep-plugin-caches-design.md`
§5.2 covers only the per-buffer/per-transcript lookup flow and **documents no
escaping rules at all** — the `-` convention existed solely as a code comment.
Add a subsection under §5 ("CSQ value escaping") stating: the four VEP
substitutions with `\s+` as a run, that `-`→`""` applies to plugin values as it
does to built-ins, per `VCF.pm:396-398` and `:449`; that `Null` and `-` therefore
both render empty; and that one function serves both paths. Cite `VCF.pm` line
numbers so the next reader does not have to re-derive them.

Pinned by a unit test, since no gate can reach it.

### D2 — the plugin `=` arm stays (resolved 2026-09-10)

`'=' => "%3D"` is **kept** on the plugin path. VEP 116 has no such rule — its only
`=`→`%3D` is HGVSp at `OutputFactory.pm:1757`, `no_escape`-gated — but the arm is
load-bearing: without it, 556 `ClinVar_CLNVI` entries across 7 records mismatch
(`docs/superpowers/handovers/2026-08-26-plugin-golden-vep-parity.md:20-36`).

The root cause is almost certainly upstream: VCF 4.3 requires `=` in an INFO value
to be `%3D`-encoded, so VEP passes ClinVar's already-encoded bytes through while
something on our side decodes them. **File that as a separate issue** against
whichever layer is decoding; do not chase it here.

Consequence for the shape of the fix: `csq_escape` implements VEP's rules exactly
and the `=` step is applied **only** on the plugin path, as a named deviation
rather than a `bool` parameter — a parameter would say "these are two variants of
one rule", when in fact one is VEP and one is a compensating workaround with a
tracking issue. Composition is safe in either order: no VEP substitution emits an
`=`, and none of `&`, `%3B`, `_` contains one, so a post-pass cannot corrupt them.
Preserve the borrowed-`Cow` fast path (`annotate_provider.rs:19809` asserts it).

### D3 — whitespace predicate matches Perl byte-semantics exactly (resolved 2026-09-10)

**Neither Rust stdlib predicate is correct.** Measured, perl 5.28.3 vs rustc:

| codepoint | Perl `\s` | `is_whitespace` | `is_ascii_whitespace` |
|---|---|---|---|
| U+0009, 000A, 000C, 000D, 0020 | yes | true | true |
| **U+000B** vertical tab | **yes** | true | **false** |
| **U+0085** NEL | **no** | **true** | false |
| **U+00A0** NBSP | **no** | **true** | false |

Today's `is_whitespace` over-matches NEL and NBSP. The tempting swap to
`is_ascii_whitespace` would fix those and **introduce a new divergence** by
dropping vertical tab. Use an explicit predicate on all four escapers:

```rust
const fn is_vep_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\x0B' | '\x0C' | '\r')
}
```

`VCF.pm` has no `use utf8` and no `use open`/`binmode`/`:encoding` anywhere under
`modules/Bio/EnsEMBL/VEP/`, so `\s` is byte-semantics — this set, exactly.

### D4 — all four escapers are in scope (resolved 2026-09-10)

Including the two the issue never mentions: #3 DOMAINS (stays **per-character**
per `OutputFactory.pm:1505` `s/[\s;=]/_/g`, but widens `' '` to `is_vep_space` so
a tab is caught) and #4 `format_prediction` (**collapses** runs per
`OutputFactory.pm:1819-1820`). One coherent escaping change.

## Open questions

None. All four resolved above; awaiting the step 1d go-ahead.
