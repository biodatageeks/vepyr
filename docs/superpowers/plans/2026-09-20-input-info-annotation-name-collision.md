# Input INFO/FORMAT names that collide with annotation columns

**Status:** go-ahead given 2026-09-20 ("go with your recommendations"): all five
questions in §7 are settled as recommended. Runbook step 2 (baseline) in progress;
no fix code has been written. Baseline run directory:
`e2e-testing/results/fix-20260920-1552/baseline`.

**Goal:** `vepyr.annotate()` must annotate a VCF whose INFO (or single-sample FORMAT)
ids match an engine output column — `AF`, `MAX_AF`, `CLIN_SIG`, `SOMATIC`, `SYMBOL`,
`Gene`, `CSQ`, … — instead of failing, and the VCF it writes must match what Ensembl
VEP 116.0 writes for the same input.

**Issues:** https://github.com/biodatageeks/vepyr/issues/121 (user-visible failure),
https://github.com/biodatageeks/datafusion-bio-functions/issues/251 (the fix).

---

## 1. The defect

`AnnotateProvider::new` builds its output schema as *input VCF fields* + `CSQ` +
`most_severe_consequence` + the typed annotation columns, with no de-duplication.
Arrow's `Schema::new` accepts the duplicate; DataFusion rejects it when SQL planning
builds the qualified schema for the table-function scan:

```
RuntimeError: VCF annotation failed: Schema error:
Schema contains duplicate qualified field name "annotate_vep()"."AF"
```

**Triggering input shape:** any VCF whose header declares an INFO id — or, for a
single-sample file, a FORMAT id — equal (case-sensitively) to `CSQ`,
`most_severe_consequence` or any `annotation_column_defs` name. `INFO/AF` alone covers
gnomAD, 1000 Genomes, most joint-called cohort VCFs and any `bcftools +fill-tags`
output; `INFO/CSQ` covers every VCF already annotated by VEP. Multi-sample FORMAT
fields are nested under `genotypes` and cannot collide.

**Who hits it today:** every `output_vcf=` run, the CLI (always `output_vcf`) and the
nf-core module, because the engine sink opens the input with *all* INFO and FORMAT
fields. The LazyFrame path is spared only because vepyr opens the VCF with
`Some(vec![])` for both — so the planned follow-up (LazyFrame carries INFO/FORMAT by
default) would turn this into a default-path failure. That is why this lands first.

### Measured, not assumed

| Run | Result |
|---|---|
| `bcftools view -r chr22:20000000-20200000 HG002_norm.vcf.gz \| bcftools +fill-tags -- -t AF,AC` (466 records) → `annotate(output_vcf=…)`, 116 Ensembl cache, `everything=True` | **fails**, schema error on `AF` |
| same input, LazyFrame path with INFO carried (spike build, `VcfTableProvider(None, None)`) | **fails**, same error |
| same input, LazyFrame path as shipped | passes (reads no INFO) |

**Exposure in every existing gate is zero.** Every fixture and harness input — the
chr1 golden, `hg002_chr22`, the nf-core test data, all `e2e-testing/**/input_chr*.vcf.gz`,
the WGS perf input — is GIAB HG002 with the same 16 INFO ids (`DPSum`, `platforms`, …)
and 6 FORMAT ids (`DP GQ ADALL AD GT PS`). None matches an annotation name. The
strict-md5 gate is therefore a pure no-regression guard for this fix; it cannot see
the defect, and a new fixture is required (§4).

---

## 2. What the analysis changed about the report

Sources read: functions at the pinned **v0.22.0 = `98f4897`**, formats at the pinned
**v1.12.1 = `419be98`**, both from `~/.cargo/git/checkouts` (the working trees are
drifted onto `fix/mnv-allele-trim-parity` and `feat/cooler`; the functions tree does
not even contain `98f4897`). Ensembl VEP at `release/116.0` (`57ea5c52`).

### 2a. The line numbers in the report are from the drifted branch

At the pinned rev the schema block is `annotate_provider.rs:3787-3816`, not
`~3702-3731`. All anchors below are pinned-rev numbers.

### 2b. A rename in the schema is not enough — the key written to the VCF is name-keyed in *formats*

The approved policy (annotation columns keep bare names; the colliding **input**
column becomes `INFO_<id>`) cannot stop at the schema. The engine sink reuses the
formats serializer, which uses one string as lookup key, emitted key and header id:

- `serializer.rs:874` — `batch.schema().index_of(field_name)`
- `serializer.rs:976` — `format!("{field_name}={value_str}")`
- `header_builder.rs:409-415` — `##INFO=<ID={name},…`
- `serializer.rs:692-713` — the carried per-record order (`_vcf_info_keys`) matches
  each *original* id against a name→position map.

A column handed over as `INFO_AF` would be written as `INFO_AF=`, lose its source
position and its `AF=.` handling, and add `##INFO=<ID=INFO_AF` beside the raw
`##INFO=<ID=AF` line. INFO has no "original id" metadata key (FORMAT has
`bio.vcf.field.format_id`).

**Resolution for this fix: undo the rename inside the engine sink, before the
serializer sees the batch.** The sink's projection contains no typed annotation
column (`annotation_output_columns`, `vcf_sink.rs:1201-1220`: core 8, input INFO,
`CSQ`, FORMAT, the two layout columns), so inside the sink the only name that can
still collide is `CSQ` (§2d). Selecting `` `INFO_AF` AS `AF` `` there makes the write
schema, the header merge (`vcf_sink.rs:1578`, by name against `vcf_schema`), and the
carried order all see `AF` again — and formats is untouched.

### 2c. A second by-name lookup breaks after the rename (`tmp_provider`)

`annotate_provider.rs:14813-14817` slices `full_schema.fields()[..vcf_field_count]`
and feeds it back into `AnnotateProvider::new` (`:14964-14972`), while the lookup side
takes the real session schema (`:14839-14844`). After a rename,
`vcf_field_names()` returns `INFO_AF`, the lookup batch still has `AF`, and
`:7836-7843` fails with `expected VCF output column 'INFO_AF' missing`. Two
requirements follow: (1) `:7836-7843` becomes positional — valid, the lookup output
puts the VCF columns first, and every neighbour already is (`vcf_field_count()`
`:3869`, `csq_col_idx` `:5621`/`:13398`); (2) the rename is **idempotent** when `new`
runs on an already-renamed schema.

### 2d. An input that already carries `CSQ` — the rule, quoted from VEP 116.0

`OutputFactory/VCF.pm`, identical in 115.2 and 116.0 (the two files differ only in the
copyright year):

```perl
312  my $fieldname = $self->{vcf_info_field} || 'CSQ';
328  if($line->[7] =~ /(^|\;)$fieldname\=/ && !$self->{keep_csq}) {
329    $line->[7] =~ s/(^|\;)$fieldname\=\S+?(\;\S|$)/$2/;
330    $line->[7] =~ s/^\;//;
348  $line->[7] .= ';' if $line->[7];
349  $line->[7] .= $fieldname.'='.join(",", @chunks);
221  push @headers, $input_header unless ($input_header =~ /ID=$fieldname,/i || $input_header =~ /^##VEP/);
```

So by default VEP **strips** the existing `CSQ=` key, drops the old
`##INFO=<ID=CSQ,…>` header line, and appends the new `CSQ=` **last** — not where the
old one stood. Every other INFO key is copied verbatim in place (`:317-321`), and
input INFO is opaque for ACGT-only records (`Parser/VCF.pm:237-244`: INFO is read
only on the SV path and under `--gp`).

What the engine would do today with the schema error bypassed (read from code, not
run): the select list holds `CSQ` twice (`vcf_sink.rs:1216`, `:1562`), the serializer
maps both to the *input's* column, and the output carries **two `CSQ=` keys holding
the stale input value, with the engine's CSQ never written**; the header keeps the
old `Format:` description (`:1578` wins over `:1584`). Writing `INFO_CSQ` back as
`CSQ=` — the generic rule of §2b — would reproduce exactly that. `CSQ` therefore needs
its own rule: **on the VCF path the input's CSQ column is dropped, not written back**,
its raw header line is dropped, and the token `CSQ` is removed from the carried
`_vcf_info_keys` so the new key lands last as in VEP.

Not ported, stated rather than inferred: `--keep_csq` (duplicate keys, which typed
columns cannot represent), `--vcf_info_field`, and the regex's edge cases (only the
first of several `CSQ=` keys removed; an empty `CSQ=` swallowing the rest of INFO).

### 2e. FORMAT: the reader already has a prefix convention

The formats reader renames a single-sample FORMAT id that clashes with an INFO id to
`fmt_<id>`, then `format_<id>` (`storage.rs:704-722`), and the serializer resolves it
back through `bio.vcf.field.format_id` (`serializer.rs:898-917`). `INFO_` has no
precedent; `fmt_` does. A FORMAT column colliding with an annotation name should
follow the reader: `fmt_<id>`.

Caveat found on the way (code reading, **not** reproduced): the sink passes column
*names* as FORMAT tags (`vcf_sink.rs:1494-1495`), so an input where the reader already
applied `fmt_DP` (INFO and FORMAT both declare `DP`) may be written today with the
key `fmt_DP`. §7 Q5.

---

## 3. Ownership

| Repo | Verdict | Changes |
|---|---|---|
| `datafusion-bio-functions` | **owner** — the duplicate is created at `annotate_provider.rs:3793-3816`; its own sink is the consumer | the fix |
| `datafusion-bio-formats` | carrier — the reader emits a duplicate-free schema and cannot know the engine's names; the serializer is name-keyed but correct | **none, no pin bump** (functions and vepyr both already pin v1.12.1) |
| `vepyr` | carrier — passes a path string, wraps the error | pin bump, regression test, one docs sentence |

Deferred to the follow-up feature, not this fix: teaching the formats writer an
original-INFO-id mapping. `pb.sink_vcf` goes through the formats `insert_into`, not
the engine sink, so §2b's rename-back does not cover it; and because Polars strips
Arrow field metadata, that mapping will need a name-prefix fallback as FORMAT has.

---

## 4. The failing tests

No existing fixture can see the defect, so each test brings its own input.

**functions (owner) — unit, `annotate_provider.rs` tests**
1. input schema with INFO `AF`, `CSQ`, `SYMBOL` → `AnnotateProvider::new` succeeds;
   fields are `INFO_AF`, `INFO_CSQ`, `INFO_SYMBOL`, each keeping its
   `bio.vcf.field.*` metadata; every annotation column keeps its bare name and index.
2. no collision → schema **identical** (names, order, metadata) to today's.
3. idempotent: feeding the renamed VCF slice back into `new` renames nothing further.
4. secondary collision: input has both `AF` and `INFO_AF` → a deterministic, unique
   name (`INFO_AF_2`-style, as the reader's resolver does).
5. single-sample FORMAT `AF` → `fmt_AF`, `format_id` intact.

**functions — sink, `vcf_sink.rs` tests**
6. select list / write schema for an `AF`-colliding input: `AF` written under its
   original key, one `##INFO=<ID=AF` line, no `INFO_AF` anywhere in header or body.
7. `CSQ`-carrying input: exactly one `CSQ=` per record, holding the engine's value,
   **last** in INFO with `preserve_record_layout` on; old header line gone.

**vepyr — `tests/test_annotate.py::TestInputInfoCollidesWithAnnotationColumn`**,
modelled on `TestNonVariantRecords` (`:2642-2734`: inline VCF in `tmp_path`,
`metadata_cache_dir` fixture, read the data lines back): INFO `AF`+`AC` input;
pre-annotated (`CSQ`) input; single-sample FORMAT `AF` input; `workers=2` on a bgzipped
copy (the sharded path resolves names at `vcf_sink.rs:1282`). Asserts: run succeeds,
`AF=<input value>` kept in place, header declares `AF` once, `CSQ` present and last,
no `INFO_` key in the text.

**Parity evidence against Ensembl VEP (step 8, new):** strict `md5_concordance.py
--pair` on two 466-record chr22 inputs — the `+fill-tags AF,AC` slice, and VEP's own
output for the plain slice fed back in (pre-annotated) — against
`ensemblorg/ensembl-vep:release_116.0`. This is the only evidence that compares the
fix with VEP rather than with itself.

---

## 5. The change, in dependency order

**formats:** nothing.

**functions**
1. `AnnotateProvider::new` (`:3787-3816`): collect the reserved names (`CSQ`,
   `most_severe_consequence`, the `annotation_column_defs` names for the active mode,
   RefSeq extras included); rename a colliding input field by its
   `bio.vcf.field.field_type` — INFO → `INFO_<id>`, FORMAT → `fmt_<id>` — resolving
   secondary collisions the way `storage.rs:704-722` does; keep metadata. Collision-only,
   so a collision-free input yields today's schema exactly.
2. `:7836-7843`: positional VCF-column copy instead of `index_of(name)`.
3. `vcf_sink.rs`: carry the (renamed → original) pairs from the provider schema into
   `annotation_output_columns` / the select list as aliases; use the renamed name for
   the sharded index lookup (`:1282`) and the original for serialization; derive
   single-sample FORMAT tags from `format_id` (`:1494-1495`).
4. `vcf_sink.rs`, input `CSQ`: omit the input's CSQ column from the projection, drop
   its raw header line in `merge_annotation_header_lines` (`:827-865`, which today
   strips only `##VEP=` and provenance lines), remove the `CSQ` token from the
   carried `_vcf_info_keys`.

5. Approved rider (2026-09-20, from the LazyFrame `sink_vcf` spec §3.3): build the output
   schema with `Schema::new_with_metadata(fields, vcf_schema.metadata().clone())` so the
   input's schema-level VCF metadata (contigs, filters, sample names, version) survives.
   No VCF byte can move: the sink builds its header from the source file. One unit test.

**vepyr:** pin bump (`Cargo.toml:142`, with the comment paragraph the file's
convention requires), the pytest class, and one sentence in `docs/quickstart.md`: input
INFO keys that share a VEP field name are preserved; VEP's values live in `CSQ`, and an
existing `CSQ` is replaced as VEP does. No Python code change: `_flags_for_projection`
matches bare names, which the approved policy keeps pointing at VEP's columns, and the
LazyFrame schema code finds its boundary by `most_severe_consequence`, not by counting
8 core columns.

## 6. Pin cascade and expected gate movement

| PR | pins |
|---|---|
| functions | formats v1.12.1 (unchanged) |
| vepyr | functions PR head (`rev =`), rewritten to the merge commit or a tag after merge |

Two PRs, not three.

| Gate | Expectation |
|---|---|
| strict md5, 22 autosomes, release 116 | **unchanged** on every contig — no HG002 input has a collision, so nothing is renamed and the rename-back is a no-op |
| new chr22 collision slices vs VEP 116.0 | body digests **match** (new evidence; no baseline exists because today's run fails) |
| performance, workers 1 and 8 | **neutral**: schema construction only; no phase should move beyond noise |

## 7. Open questions

1. **Formats stays untouched for this fix** (rename-back in the engine sink), and the
   writer's original-id mapping moves to the follow-up feature — agreed?
   *Recommended: yes; it keeps this a two-PR cascade with no formats release.*
2. **FORMAT prefix `fmt_`**, following the reader, rather than `FORMAT_` to mirror
   `INFO_`? *Recommended: `fmt_`.*
3. **Input `CSQ`:** port VEP's default (strip and replace, new key last) and leave
   `--keep_csq` unported; on the LazyFrame the old value stays readable as `INFO_CSQ`.
   *Recommended: yes.*
4. **File issues?** One in `biodatageeks/vepyr` (user-visible failure) referencing one
   in `datafusion-bio-functions` (the fix). *Recommended: yes, before the PRs.*
5. **Scope of the `fmt_DP` write-back caveat (§2e):** step 5.3 touches that exact code
   (`format_id`-derived tags). Include a test for an INFO+FORMAT `DP` input here, or
   file it separately? *Recommended: include — same lines, one more test.*

Out of scope, recorded so they are not lost: an INFO id equal to a core column name
(`id`, `filter`) duplicates in the **reader**; an INFO id `cache_<col>` shadows a
lookup column (`lookup_provider.rs:143`); an input INFO named like a **plugin** field
(`CADD_PHRED`, `ClinVar`) collides in vepyr's Python once the LazyFrame carries INFO —
that one belongs to the follow-up feature.
