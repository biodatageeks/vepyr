# `ALT=.` non-variant parity (vepyr#97)

**Status:** awaiting human go-ahead at runbook step 1d. No file in any of the three
checkouts has been edited. This artifact is the gate.

**Goal:** a record whose first ALT is `.` must be dropped, as Ensembl VEP 116.0 drops
it, and must be retainable through a new `allow_non_variant` flag — instead of being
annotated as a fabricated one-base deletion with frameshift consequences.

**Issue:** https://github.com/biodatageeks/vepyr/issues/97

---

## 1. The defect

The formats VCF reader joins a record's alternate bases into one non-nullable string
column. For `ALT=.` noodles yields **zero** alternate bases, so the join produces the
**empty string**. The engine's row guard recognises only Arrow NULL and the literal
`"*"`, so the empty string passes both arms and reaches
`vcf_to_vep_allele("G", "")`, which trims to `("G", "-")` — a deletion. The record is
then classified and annotated as one, producing frameshift consequences for a record
that carries no alternate allele at all.

**Triggering record shape:** any VCF data line whose first ALT is `.`, e.g.
`chr1  604360  .  T  .  50  PASS  .`. Also, per §2c, `ALT=.,C`.

### Measured, not assumed

| corpus | records | `ALT=.` | `ALT=*` | multi-allelic |
|---|---|---|---|---|
| `$DATA_VEPYR_DIR/input/HG002_normalized.vcf.gz` | 4,096,123 | **0** | 512 (all standalone) | **0** |
| `HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` (e2e source) | 4,048,342 | **0** | — | — |
| `tests/data/golden/input.vcf` | 100 (91 SNV, 9 indel) | **0** | 0 | 0 |
| all 26 `e2e-testing/**/input_chr*.vcf.gz` | 50k–331k each | **0** each | — | — |

`HG002_normalized` was counted twice by independent means — `bcftools query -f '%ALT\n'`
and a raw `gzcat | awk` text scan that bypasses bcftools entirely — because bcftools
could in principle normalise a missing ALT on the way out. Both give 0.

**Consequence: genome-wide exposure in the parity corpus is zero.** The strict-md5
gate is a pure regression guard for this fix, not a number to re-baseline.

---

## 2. What the analysis changed about the issue

The issue's proposed fix is wrong in four specific ways. Each is a quotation from
source, not an inference.

### 2a. Do not reuse the star-allele path for the default case

The issue says to fix this "next to the existing star-allele arm … using the same
shape as the `*` arm: no CSQ, a counter, and the row dropped from the annotated
output", and to make `allow_non_variant` reuse that same path.

**The star arm does not drop the row, and VEP does not treat `*` and `.` alike.**

- The engine's `*` arm is `csq_builder.append_null(); most_builder.append_null();
  append_null_annotation_row!(); continue;`. The row loop is structurally 1:1 —
  passthrough columns are taken verbatim from the input batch by index
  (`annotate_provider.rs:7797-7804`) and `RecordBatch::try_new` at `:7902` requires
  every builder to produce exactly `batch.num_rows()` values. **Dropping a row inside
  that loop is impossible.** The star arm emits the row with a null CSQ.
- Ensembl handles the two at different layers with different granularity. `*` is a
  **per-allele** skip in ensembl-variation
  (`VariationFeatureOverlap.pm:484`, `next if $allele eq '*' || $allele eq '<DEL:*>'`)
  — the record survives, sibling alts still get CSQs, and it is **always emitted**.
  `.` is a **whole-record** drop in the VEP parser (`Parser/VCF.pm:263-266`,
  `$parser->next(); return $self->create_VariationFeatures;`) — by default the record
  does not appear in any output at all.

So reusing the star arm would give "emitted with no CSQ" for the **default** case,
where VEP gives "not emitted" — i.e. it would apply the `allow_non_variant=true`
behaviour unconditionally and make the new flag a no-op. **The star arm is the right
shape for `allow_non_variant=true` only.** The default needs a real filter.

### 2b. `non_variant => 1` is a dead flag — do not model the design on it

The issue says the record "is kept and flagged `non_variant => 1`". It is set at
`Parser/VCF.pm:353` — and a full-tree `git grep non_variant release/116.0` shows it is
**never read anywhere in modern ensembl-vep**. Its only readers are
`ensembl-variation .../Utils/VEP.pm:1365/1421/1448`, the pre-VEP-90 legacy script.

The real mechanism is the allele string. `Parser/VCF.pm:342`:
`allele_string => $non_variant ? $ref : $ref.'/'.join('/', @$alts)` — a bare `'G'`
with no slash, confirmed by `t/Parser_VCF.t:296`. `VariationFeatureOverlap.pm:479-491`
then finds no allele differing from reference, so no consequence is produced.

### 2c. The predicate is the FIRST ALT only, not the whole string

`Parser/VCF.pm:259`: `if($alts->[0] eq '.')`. VEP tests the first element of the
parsed ALT list. The issue's proposed guard (`alt_allele.is_empty() || alt_allele == "."`)
is applied to the engine's **joined** string, and gets one shape wrong:

| input | column value | VEP 116.0 | issue's guard | first-alt guard |
|---|---|---|---|---|
| `ALT=.` (text/BGZF/BCF reader) | `""` | dropped | fires ✅ | fires ✅ |
| `ALT=.` (Zarr reader) | `"."` | dropped | fires ✅ | fires ✅ |
| `ALT=.,C` | `".\|C"` | **dropped** | **misses ❌** | fires ✅ |
| `ALT=C,.` | `"C\|."` | kept | correctly skips ✅ | correctly skips ✅ |
| `ALT=*` | `"*"` | per-allele skip | n/a (existing arm) | unchanged ✅ |
| `ALT=C,*` | `"C\|*"` | kept, `*` skipped per-allele | n/a | unchanged (see §2e) |

The `== "."` arm is not belt-and-braces: the VCF-Zarr reader emits a **different
encoding into the same column** — `"."` for no-ALT and `,` as separator
(`zarr/arrays.rs:210-229`) — versus `""` and `|` from the text/BGZF/BCF readers. A
guard testing only `is_empty()` would let every Zarr-sourced non-variant through.

**So the guard is:**

```rust
let first_alt = alt_allele.split('|').next().unwrap_or("");
if first_alt.is_empty() || first_alt == "." { … }
```

`split('|').next()` on `""` yields `Some("")`, so the empty case still fires; the
`unwrap_or("")` is belt-and-braces.

### 2d. Do not add the proposed `debug_assert!(!alt.is_empty())`

The issue's step 3 wants `vcf_to_vep_allele` to `debug_assert!(!alt.is_empty())`.
Two reasons not to, as written:

- **It fires on the issue's own reproduction record**, via a call site the issue does
  not mention: `cache/lookup_exec.rs:1101` calls `vcf_to_vep_allele(vcf_ref, vcf_alt)`
  on the colocated cold probe, reading `alts.value_or_empty(row)` at `:1657` with **no
  `*` or empty filter at all**. That runs for every row whenever `--check_existing` /
  `--everything` is on — which the golden gate is — and it runs *before* annotation, so
  a guard at `annotate_provider.rs:6196` does not protect it.
- **It is reachable from public SQL.** `allele.rs:813` is `vep_allele_impl`, backing the
  documented scalar UDF `vep_allele(ref, alt)` (`lib.rs:100`), which skips only Arrow
  nulls (`allele.rs:809`). `SELECT vep_allele('G','')` would abort a debug build.

It is also debug-only, so it would never fire in the shipped release build anyway.

### 2e. Two sibling defects, in scope to *note*, not to fix

Both have **zero exposure** in the parity corpus (§1) and neither is caused by this fix:

- **The annotation path is not multi-allelic-aware.** `alt_allele` is bound to the whole
  joined string at `annotate_provider.rs:6184` and never split; `vcf_to_vep_allele("G","C|T")`
  returns `("G","C|T")` literally, which `classify_variant` (`:7943-7951`) buckets as
  `indel`. This is a *documented, deferred* limitation, not a surprise — pinned by the
  test `plugin_cache/provider.rs:708 vcf_source_multiallelic_alt_shape_is_pinned`,
  stated in `docs/superpowers/specs/2026-06-17-vep-everything-redundancy-analysis.md:179-180`
  ("multi-allelic sites are pre-split to one row per ALT — no inner allele loop"), and
  left open in `openspec/changes/add-vep-pick-modes/design.md:159`.
- **The star guard under-fires.** `alt_allele == "*"` is a whole-string equality, so
  `ALT=C,*` (`"C|*"`) sails past it. All 512 `ALT=*` records in HG002 are standalone,
  which is why this has never shown up in the gate.

### 2f. One factual correction to the issue text

The issue says `unwrap_or(".")` "guards a missing element inside a present list". It
does not: the noodles iterator item is `io::Result<&str>`, not `Option`, so it guards a
*parse error*, and on the text path (`split(',').map(Ok)`) it is unreachable dead code.
Noodles erases `.` to `""` at `noodles-vcf/src/record/fields.rs:49-54` before the
`AlternateBases` wrapper is constructed. The conclusion is unchanged and stronger.

---

## 3. Ownership

**Owner: `datafusion-bio-functions`. Carrier: `vepyr`. No change: `datafusion-bio-formats`.**

- **formats — not the owner.** The empty string is a deliberate, internally consistent
  encoding meaning "zero ALT alleles", and every consumer inside formats reads it
  correctly: the serializer round-trips it back to `.` (`serializer.rs:665-671`), the
  UDF layer documents it (`udfs.rs:143-153`, "`""` / `"."` → 0"), and the BCF reader
  distinguishes a zero-length list (legal) from an empty element (an error,
  `bcf.rs:2654-2663`). Making the column nullable would flip 16 `Field::new("alt", …, false)`
  declarations across the crate plus the Zarr and writer schemas, change a public column
  for every downstream consumer of `bio-format-vcf`, and *still* not fix the bug — the
  engine's NULL arm silently drops such rows with no counter and no path to
  `allow_non_variant`. You would land the default case by accident and still have to
  build the flag in the engine.
- **functions — the owner.** It is where the value is *interpreted as a deletion*
  (`allele.rs:350-354`), and it has no concept of a non-variant record at all:
  `grep -rni "allow_non_variant\|non_variant"` over the VEP crate returns zero hits. The
  only `.`/empty awareness is `annotate_provider.rs:10329-10338 alt_input_units`, which
  is buffer-size accounting and never gates annotation — exactly as the issue says.
- **vepyr — carrier, but with a required code change, not just a pin bump.** See §4c.

**Repo geometry:** the functions pin `b470460` **is** `origin/master` (`b470460..origin/master`
and the reverse are both empty), so the fix branches off master at exactly the code
vepyr builds. The working tree is drifted onto `fix/mnv-allele-trim-parity` (4 commits
ahead); that branch is unrelated to this fix and must not be built on.

---

## 4. The change, per repo, in dependency order

### 4a. `datafusion-bio-formats` — contract tests only, no `src` behaviour change

Confirmed in scope by decision D4. Three additions, because today the contract rests on
nothing but noodles' internals and a noodles bump could silently break the new engine
guard:

1. a reader-contract test that an `ALT=.` record yields `""` in the `alt` column through
   each of the four record loops (a noodles bump could silently change this under the
   engine's new guard);
2. a round-trip test for the `alt_value.is_empty()` arm at `serializer.rs:667` — only
   the `== "."` half is covered today (`serializer.rs:1534-1566`);
3. a doc comment at `table_provider.rs:173` stating the contract.

**No `src` logic changes**, so the built artifact is unchanged and nothing downstream
needs to re-pin — see §6.

### 4b. `datafusion-bio-functions` — the fix

1. **A single ALT classification predicate** (decision D2), replacing the scattered
   whole-string `== "*"` comparisons:
   ```rust
   enum AltKind { Sequence, Star, NonVariant }
   fn alt_kind(alt: &str) -> AltKind   // tests the first '|'-separated element
   ```
   This is slightly larger than the issue proposes but it collapses the guard sites into
   one predicate and gives the `ALT=C,*` sibling defect (§2e) somewhere to land later.
   Per D2 the star under-firing itself is **not** fixed here: `alt_kind` reproduces
   today's `*` behaviour exactly, so the 512 standalone star records are untouched and
   the parity gate stays a valid regression guard.
2. **Guard site 1** — `annotate_provider.rs:6196` (pin; `:6124` on the drifted branch):
   extend to `AltKind::NonVariant`, incrementing a new `non_variant_rows` counter.
3. **Guard site 2** — `cache/lookup_exec.rs:1657`: skip the colocated cold probe for a
   non-variant ALT, so no spurious `Existing_variation` / AF match is built from a
   fabricated `G/-` deletion. **This site is not covered by site 1** and is reached first.
4. **The drop** (default only; under `allow_non_variant=true` the row is kept with a
   null CSQ on **both** output paths, per decision D1). A `BooleanArray` mask built
   during the row loop and applied with
   `filter_record_batch`. The precedent is `regions.rs:344-364`, already used at
   `annotate_provider.rs:13372` (`filter_batch_to_bounds(&annotated, start_idx, bounds)`).
   **Implementation risk, called out rather than glossed:** that existing filter is
   positional (`start_idx`), so applying a new mask *before* it would shift row indices
   under it. The mask must either be composed with the bounds mask at the same point, or
   applied strictly after it. This is the one part of the change I will verify against the
   code before writing it, and I will say in the PR which of the two it turned out to be.
5. `VepFlags` (`annotate_provider.rs:1165`) gains `allow_non_variant: bool`; the existing
   `parse` closure at `:1186` supplies the `false` default for free. Note options are
   parsed by **manual substring search** (`parse_json_bool_option`, `:3917-3930`) — there
   is no serde options struct and unknown keys are silently ignored, which is why
   `allow_non_variant` in `options_json` does nothing today.
6. **`AnnotateVcfConfig`** (`vcf_sink.rs:385`, `#[non_exhaustive]`) gains
   `pub allow_non_variant: bool`, a `false` in its `Default`, and an emit in
   `to_options_json` (`:547-675`). **Every option reaches the engine through that one
   function**; miss it and `output_vcf=` never sees the flag even though LazyFrame does.
7. `EngineAnnotationProfile` (`:77-105`) gains `non_variant_rows` beside
   `null_alt_rows`/`star_allele_rows`, plus the format string at `:121`. Note these
   counters only increment when `VEP_ENGINE_PROFILE` is set (`:29-31`, `:6173`), and the
   profile is constructed per batch, so the line is per-batch per-worker and never
   aggregated — a counter alone is not an observable gate.
8. **No `debug_assert!`** in `vcf_to_vep_allele` (§2d). Instead, `vep_allele_impl`
   (`allele.rs:813`) appends null for an empty ALT argument, which is the honest answer
   for the public UDF and cannot panic a user's query.

### 4c. `vepyr` — four edits, not one

1. **`Cargo.toml`** — bump the `datafusion-bio-function-vep` rev only (line 105). The two
   `bio-formats` crates stay on `tag = "v1.12.1"`: they are declared *by tag* to match how
   `bio-function-vep` declares its own dependency, and a `rev=` of the same commit is a
   different cargo source that compiles the formats crates twice and yields `E0308` on
   `CacheSourceType`. Plus `Cargo.lock`.
2. **`src/vepyr/__init__.py`** — `allow_non_variant: bool = False` in `annotate()`'s
   keyword-only signature (`:1039`), the forwarding line in the `opts` block
   (`:1371-1476`, house pattern `if flag: opts["flag"] = True` so the key is absent when
   false), and a docstring parameter block saying the default drops such records as VEP
   does. `_flags_for_projection` (`:150-277`) needs no change — it starts `dict(opts)` and
   only pops keys it names, so an unrecognised key survives all three return points.
3. **`src/annotate.rs`** — `config.allow_non_variant = opts.get("allow_non_variant")…`.
   **This is the trap.** The LazyFrame path interpolates `options_json` verbatim into SQL
   (`:511`), but the VCF-output path **does not pass it at all**: it re-parses the JSON and
   hand-copies every option field by field into `AnnotateVcfConfig` (`:191-347`). Without
   this line the flag is inert on `output_vcf=` — which is precisely the path the issue's
   own proposed tests use, so one test would fail while its LazyFrame twin passed. Neither
   `82ec2cd` nor `fedc945` touched this file, because neither added a new engine option;
   mirroring them would miss it.

`src/vepyr/_core.pyi` needs no edit (the signature is already `options_json: str`).

4. **`src/vepyr/cli.py`** (decision D3) — `--allow_non_variant` in the "Ensembl VEP
   options" group (`:67-97`) and in `annotate_kwargs()` (`:120-144`), spelled exactly as
   real VEP spells it. The nf-core module passes `task.ext.args` through verbatim, so
   without this a user writing `ext.args = '--allow_non_variant'` gets an argparse error.

---

## 5. The failing test, and why no existing gate can see this

**No existing fixture anywhere can see this defect.** Counted: 0 `ALT=.` records in the
chr1 golden, the merged golden, all 26 e2e inputs, both HG002 corpora, and every VCF
fixture in the formats repo. A green golden gate is not evidence here.

### Engine (`datafusion-bio-functions`)

**The harness the issue's proposed test assumes does not exist.** There is no
`annotate_rows()`, no `micro_cache()`, no `write_vcf()` helper in the crate; the two
integration tests build synthetic `RecordBatch`es and never run `annotate_vep()` against
a cache, and `vep-benchmark/data/golden/*.vcf` are git-LFS pointer stubs in this
checkout. Building that harness is a larger job than the fix. So the split is:

- **Rust unit tests** on `alt_kind` covering all six shapes in §2c's table, each row
  carrying the `Parser/VCF.pm:259` citation;
- a Rust test that `vcf_to_vep_allele("G","-")` still returns `("G","-")` — the issue's
  positive control, that a real deletion is untouched;
- a Rust test on the `to_options_json` round-trip for the new flag;
- **the record-level behavioural assertion goes in vepyr's pytest**, which has a
  committed micro-cache and a working end-to-end path.

### vepyr (`tests/test_annotate.py`)

The issue's proposed tests are usable nearly as written, with three corrections:

- `CACHE_DIR` is `tests/data/golden/cache`, **committed in-repo** (13 files, 5.1 MB), so
  `skip_if_no_cache` passes and these tests really run rather than silently skipping. No
  `$DATA_VEPYR_DIR` dependency.
- `skip_if_no_cache` is module-local to `test_annotate.py`, not in `conftest.py`, so the
  tests must live in that file.
- The proposed positions (604358 / 604360 / 611317) are inside the fixture window and
  604360's REF is `T` — they match `tests/data/golden_mnv/input.vcf`. Good as drafted.

Plus a **cheap forwarding test** that needs no cache and no engine, following the
established pattern at `tests/test_annotate.py:714-756`: monkeypatch `vepyr._annotate_vcf`,
assert `json.loads(options_json)["allow_non_variant"] is True`, and assert the key is
**absent** when the kwarg is false. That is the test that would have caught the
`src/annotate.rs` gap in §4c. Note it intercepts the VCF path only; the LazyFrame side
needs the `_create_annotator` seam.

`assert "CSQ=" not in middle` **will pass** — confirmed in the writer:
`serializer.rs:958-969` omits a null INFO key entirely when it sits at
`position >= carried_count`, and the engine appends CSQ last (`vcf_sink.rs:1558-1559`).
Two caveats worth knowing: if the *input* already carried a `CSQ` key it renders as
`CSQ=.` instead (`:965`), and a source `##INFO=<ID=CSQ,…>` header would make the engine
emit the key twice. The proposed fixture carries `INFO=.`, so it is safe. And the
non-variant row's ALT renders as `.`, not an empty field (`serializer.rs:665-671`), so the
output stays valid VCF.

### Must stay green as the regression guard

The 22-contig strict-md5 body digests, and the golden suites. Per §1 their inputs contain
zero affected records, so **any** movement is a regression.

---

## 6. Pin cascade

| PR | pins | why |
|---|---|---|
| `datafusion-bio-formats` | nothing | tests and one doc comment only |
| `datafusion-bio-functions` | **nothing — deliberately not the formats PR head** | see below |
| `vepyr` | functions PR head | the only real pin bump |

Three PRs, branch `fix/alt-dot-non-variant` in each. **The cascade is two links long, not
three, and that is a decision rather than an oversight.**

The formats PR changes no `src` logic — it adds tests and a doc comment — so the compiled
artifact is byte-identical and pinning to it would buy a reviewer nothing. It would also
actively break the build: vepyr declares the two `bio-formats` crates **by tag**
(`tag = "v1.12.1"`) to match how `bio-function-vep` declares its own dependency on them.
A `rev = ` pin of the same commit is a *different cargo source*, so the formats crates
compile twice and the two copies' types do not unify — `E0308` on `CacheSourceType` and
the writer types. Repinning formats to a branch head therefore requires either moving
both repos to `rev =` in lockstep or cutting a `v1.12.2` tag, and neither is warranted by
a test-only change.

**Consequence for review order:** the formats PR is independent and can merge in any
order. The functions → vepyr link is the real cascade and must be re-pinned after every
upstream push, per runbook step 7.

---

## 7. Expected gate movement

**Quality — no movement, and this is falsifiable.** All 22 autosome strict-mode body
digests must stay byte-identical, because the corpus contains 0 `ALT=.` records, 0
multi-allelic records, and all 512 `ALT=*` records are standalone and keep taking the
unchanged star arm. If any digest moves, the `alt_kind` refactor has changed star or
sequence handling and the fix is wrong — not the baseline.

**Performance — no measurable movement.** The change adds one `split('|').next()` and two
string comparisons per row on a path that already does per-row allele trimming, HGVS
construction and interval-tree lookup, plus one `filter_record_batch` per batch on a mask
that will be all-true for every batch in the corpus. Prediction: every phase within noise,
well inside the 5% per-phase bar; wall and peak RSS within 10% at both 1 and 8 workers.
If a phase regresses >5%, suspect the filter allocation — a mask that is statically
all-true should be skippable without building the filtered batch at all.

---

## 8. Decisions (confirmed by the human, 2026-09-10)

| # | Question | Decision |
|---|---|---|
| D1 | What should `allow_non_variant=True` yield on the LazyFrame path, given that VEP's own VCF and tab outputs disagree? | **Emit the row with a null CSQ**, matching the VCF output path and vepyr's one-row-per-record shape. Both vepyr paths then agree with each other and with VEP's VCF output. Accepted divergence: VEP's *tab* output emits nothing — vepyr has no tab-shaped output to match. |
| D2 | One `alt_kind()` predicate, or a minimal inline guard? | **`alt_kind()`**, collapsing the three scattered `== "*"` comparisons and the unguarded probe site into one predicate. The star under-firing on `ALT=C,*` is **not** fixed here — `alt_kind` reproduces today's `*` behaviour exactly, so the gate stays a valid regression guard. |
| D3 | Add `--allow_non_variant` to the vepyr CLI? | **Yes**, in the same PR, spelled as real VEP spells it, so the nf-core `ext.args` path works. |
| D4 | Add the formats reader-contract tests? | **Yes**, as a third PR. Tests and a doc comment only — no `src` change, and therefore no pin bump (§6). |

### Residual risk carried knowingly

D2 leaves three sibling defects unfixed, all with **zero exposure in the parity corpus**
(§1) and all pre-existing: `ALT=C,*` bypassing the star arm, joined multi-allelic ALTs
annotated as one fabricated allele (a documented, deferred limitation — §2e), and
symbolic `<DEL>`/`<*>` ALTs annotated as sequence changes. None is caused or worsened by
this fix. They should be filed separately; `alt_kind` is the seam they land on.

---

## 9. Version provenance of the Perl analysis

| repo | rev | SHA |
|---|---|---|
| ensembl-vep | tag `release/116.0` | `57ea5c52340acc1f156267f810ad162e26597082` |
| ensembl-vep | tag `release/115.2` | `2beada0d57ca6234f467b14a6c60280f4a082717` |
| ensembl-variation | `origin/release/116` | `2fb834b987ede3824e200197a838ce11e91aeb4b` |
| ensembl-variation | `origin/release/115` | `23c76f60b1592e4df86159cf5530bdc326120c3d` |

**Trap avoided:** in ensembl-vep, `origin/release/116` is *not* 116.0 — it has advanced to
`2cb0bbe2` = tag `release/116.2`. The annotated tag was used throughout. `ensembl-variation`
has no `115.2`/`116.0` tags at all, only branches.

**115.2 vs 116.0: the rule is byte-identical.** `Parser/VCF.pm`, `OutputFactory/VCF.pm`,
`BaseVEP.pm` and ensembl-variation's `VariationFeatureOverlap.pm` each differ by exactly
one insertion and one deletion — the copyright year. `Config.pm` and `Parser.pm` have real
diffs, none touching `non_variant`. These are genuinely non-empty diffs against revs that
provably resolve, so this is not the silent-empty-diff failure mode the runbook warns about.

`allow_non_variant` is declared at `Config.pm:142` as a bare Getopt::Long boolean, is
**absent from `%DEFAULTS`** (so the default is false), and is auto-enabled by `--individual`
and `--individual_zyg` (`Config.pm:410-422`). It has no `--help` text anywhere in the tree.

**Two behaviours in VEP that this fix deliberately does not reproduce**, noted so a future
reader knows they were considered:

- `--process_ref_homs` makes a non-variant record produce a real CSQ, because
  `OutputFactory.pm:510` then switches to `get_all_VariationFeatureOverlapAlleles`, which
  includes the reference allele. So "non-variant ⇒ never a CSQ" is not universally true in
  VEP. Out of scope.
- `--dont_skip` does **not** resurrect an `ALT=.` record (it is consulted at
  `Parser.pm:189`, after a VF exists, and one is never built). No warning and no
  `--skipped_variants_file` entry is written either — the drop is silent. Worth asserting
  so a future "log it as skipped" change is caught.

---

## 10. Provenance map — which changes are ported, which are adapted, which are ours

Runbook step 1a-bis requires the rule being ported to be **quoted** from Ensembl source at
the pinned release. That gate is satisfied for the rule itself. But not every line of this
change *is* the rule: some is stack-internal plumbing with no Ensembl counterpart, and one
item is a deliberate divergence. Classifying them here so a reviewer can tell which is
which, and so nothing is silently presented as "VEP does it this way" when it does not.

**Legend.** *Ported* — a rule quoted from Ensembl source. *Adapted* — an Ensembl rule
re-expressed against a different data representation; the rule is quoted, the spelling is
ours. *Ours* — no Ensembl counterpart exists; a design decision. *Divergence* — Ensembl
does something else, knowingly.

| # | Change | Class | Ensembl provenance |
|---|---|---|---|
| E1 | Drop the record by default | **Ported** | `Parser/VCF.pm:263-266` — `$parser->next(); return $self->create_VariationFeatures;` (tail-recurses; no VF is ever built) |
| E2 | Keep it under a flag, no consequence | **Ported** | `Parser/VCF.pm:260-262`, `:342` (`allele_string => $non_variant ? $ref : …`), `t/Parser_VCF.t:296` (`'allele_string' => 'G'`), `VariationFeatureOverlap.pm:479-491` |
| E3 | Test the **first** ALT, not the joined string | **Ported** | `Parser/VCF.pm:259` — `if($alts->[0] eq '.')` |
| E4 | `alt_kind` keeps `*` behaviour exactly as today | **Ported** | `VariationFeatureOverlap.pm:484` — `next if $allele eq '*' \|\| $allele eq '<DEL:*>'` (per-allele, record survives) |
| E5 | Flag named `allow_non_variant`, default false | **Ported** | `Config.pm:142`; absent from `%DEFAULTS` (`:280`), so undef/false |
| E6 | VCF output: original line, **no** `CSQ` key | **Ported** | `OutputFactory/VCF.pm:341-353` — `@chunks` empty ⇒ the key is never appended |
| E7 | Splitting on `'\|'` to find the first ALT | **Adapted** | Ensembl parses a *comma*-split list (`get_alternatives`). `'\|'` is this stack's own joined encoding (`plugin_cache/provider.rs:761` pins it). The **rule** is E3; the separator is ours. |
| E8 | `first_alt.is_empty()` as a non-variant test | **Adapted** | Ensembl sees the literal `['.']`. noodles erases `.` → `""` at `noodles-vcf/src/record/fields.rs:49-54` *before* the wrapper exists, so the empty string **is** this stack's spelling of Ensembl's `'.'`. The `== "."` arm covers the Zarr reader, which spells it the Ensembl way (`zarr/arrays.rs:210-229`). |
| E9 | Guard the colocated probe (`lookup_exec.rs:1657`) | **Adapted** | No direct quote — in VEP the question cannot arise, because no VF is built, so *no annotation source runs at all*. Derived from E1's control flow, not from a rule about colocated lookup. Sound, but it is an inference from where the `return` sits. |
| E10 | `non_variant_rows` counter in the engine profile | **Ours** | None. VEP drops the record **silently** — no `warning_msg`, no `--skipped_variants_file` entry (the `skipped_variant_msg` calls at `Parser.pm:476-586` are all on paths a non-variant record never reaches). Our counter is internal profiling behind `VEP_ENGINE_PROFILE`, not output, so it does not diverge from VEP's observable behaviour. |
| E11 | `AnnotateVcfConfig` field, `to_options_json`, `src/annotate.rs` mapping, Cargo pins | **Ours** | None, and none needed — pure plumbing internal to this stack. |
| E12 | `vep_allele('G','')` returns null instead of asserting | **Ours** | None. `vep_allele` is a DataFusion SQL UDF this stack exposes (`lib.rs:100`); VEP has no counterpart. Chosen over the issue's `debug_assert!` because that would panic a user's query and is debug-only anyway (§2d). |
| E13 | CLI spelling `--allow_non_variant` | **Ported** (name only) | `Config.pm:142`, the Getopt::Long parameter name. Note it has **no `--help` text anywhere** in ensembl-vep 116.0 — the name is the only thing to port. |
| E14 | LazyFrame under the flag emits a **null-CSQ row** | **Divergence** | VEP's *tab*/default output emits **nothing** for such a record even with the flag (`OutputFactory.pm:265-271` builds lines from output hashes; there are zero). Chosen knowingly as decision D1 to match VEP's *VCF* output (E6) and vepyr's one-row-per-record shape. |

### The one thing we could not establish in the Ensembl source

**A genuinely empty ALT field** (`chr1 100 . G  50 PASS .` — an empty column, not `.`).
`Bio::EnsEMBL::IO::Parser::VCF4::get_alternatives` lives in **ensembl-io, which is not
checked out on this machine**, so what `$alts` holds for that input could not be
established. Reading `Parser/VCF.pm:259` alone, `undef eq '.'` is false under the module's
`use warnings` (`:69-70`), which would mean **not** non-variant — but that is reasoning
about a value we could not observe.

Our `is_empty()` arm (E8) would drop such a record. That is almost certainly right for a
malformed input, and it is not reachable from any conforming VCF — noodles produces `""`
only for `ALT=.`. But it is an **unverified edge**, and per the runbook it is stated here
rather than inferred into a passing test. I will not write a test asserting VEP-parity for
that shape, because there is no source to assert against.

### Two VEP behaviours deliberately not ported

Both quoted, both out of scope, both recorded so a later reader knows they were seen:

- `--process_ref_homs` makes a non-variant record produce a **real CSQ**, because
  `OutputFactory.pm:510` switches to `get_all_VariationFeatureOverlapAlleles`, which
  includes the reference allele. So "non-variant ⇒ never a CSQ" is not universally true in
  VEP. vepyr has no `process_ref_homs`.
- `--dont_skip` does **not** resurrect an `ALT=.` record — it is consulted at
  `Parser.pm:189`, after a VF exists, and one never is. (It *does* resurrect `<NON_REF>`.)
