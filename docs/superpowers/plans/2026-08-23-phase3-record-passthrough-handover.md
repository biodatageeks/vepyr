# Phase 3 — record passthrough to byte-identical VCF output

**Written:** 2026-08-23
**Goal:** make `md5_concordance.py --mode strict` pass on the record body, so
release tags can be cut.

---

## 1. Where things stand

### Merged

| repo | PR | what it fixed |
|---|---|---|
| bio-formats | #240 | QUAL rendered at source precision, not `{:.2}` (also: `format_vcf_float` no longer panics on ±Inf) |
| bio-formats | #242 | source VCF header re-emitted verbatim |
| bio-functions | #212 | CSQ description reads `from Ensembl VEP` |
| bio-functions | #214 | run provenance in the output header |
| bio-functions | #215 | provenance records `cache_format`, not the vestigial `backend` token |

### Committed, not pushed

- **bio-formats** `df53b18` on `feat/vcf-record-passthrough` — the serializer
  half of phase 3. 134 tests green, fmt + clippy clean.
- **vepyr** `e2bcfe9` on `chore/bump-to-header-parity-revs` — rev pins onto the
  merged engine revisions, plus the two new `AnnotateVcfConfig` fields.

### Open issues

- bio-formats #241 — `±Inf` coercion vs error; whether `qual` should be
  `Float32` in the schema rather than widened to `f64`.
- bio-functions #213 — carry the Ensembl cache's data-source versions
  (gnomAD, ClinVar, dbSNP, GENCODE…) into provenance.

### Verified today

Two chromosomes, annotated with the merged engine and compared to the reference
Ensembl VEP 116.0 output:

| | chr21 | chr22 |
|---|---|---|
| records | 55,812 | 50,861 |
| header lines | 236 vs VEP's 236 | 236 vs VEP's 236 |
| canonical body digest | `f1f1cadec368e51cecf93111bed841c7` | `9392f95c482497e25c362d1bb6f55ab5` |
| digest vs pre-change audit | unchanged | unchanged |
| strict body | FAIL — 3 classes | FAIL — 3 classes |

Header parity is reached. The only header line that still differs is
`##bcftools_normCommand`, and that is a harness artifact — the reference VEP run
was produced from a different normalization than the current input. It is in
fact evidence the passthrough works, since vepyr is echoing whatever its input
carried.

**Unchanged digests are the important result:** phases 1 and 2 changed
serialization without touching annotation content, on 106,673 records.

### What strict mode still reports

```
55,812  FORMAT KEYS (-['PS'] +[])
55,812  SAMPLE1 keys
55,714  INFO order
```

`QUAL format` is gone from that list — phase 1 removed it. The remaining three
are phase 3.

---

## 2. Why phase 3 is plumbing, not serializer fixes

Do not attempt to fix these by rendering INFO/FORMAT more cleverly.

- **No global key order exists.** chr6's input carries both
  `GT:PS:DP:ADALL:AD:GQ` and `GT:AD:PS` — `AD` precedes `PS` in one and follows
  it in the other. A file-level order table silently produces wrong output on
  real data.
- **The `PS` drop is lossy by construction.** The Arrow schema gives every record
  every FORMAT key the header declares, so the per-record key list is gone before
  the serializer runs. `filter_all_missing_format_fields` reverse-engineers it
  from which values are non-missing, and cannot distinguish "absent" from
  "present but `.`".

Ensembl VEP avoids all of this by never re-rendering: `OutputFactory/VCF.pm`
copies `$vf->{_line}` and appends to INFO only.

### The approach is empirically proven

On chr21, taking each raw input line and appending the CSQ this engine already
produces reproduces the reference VEP output **byte for byte on 55,812 / 55,812
records (100.0000%)**.

Reproduce with the pattern in
`scratchpad/phase3_proof.py`: zip the bodies of input / VEP / vepyr, splice
`CSQ=` out of vepyr's INFO onto the raw input line, compare to VEP's line. Run
this again on chr22 before starting, as a second confirmation.

---

## 3. What the reader must carry, and why

Two earlier drafts of this section were wrong. Recording both, because each
wrong turn is a trap someone else would walk into.

**Draft 1 — "blocked on a noodles fork."** It concluded phase 3 needed
`noodles-vcf` to expose a record's raw line (`Fields::buf` is `pub(crate)` with
no public accessors). True, and irrelevant: the whole line is not what is
needed. `Record::info()` and `Record::samples()` are already public, and
`Info<'r>` is a thin wrapper over the raw INFO substring that iterates in
**source order**. No fork. Three repos, not four.

**Draft 2 — "one column plus schema metadata."** It proposed carrying only the
per-record FORMAT keys and treating INFO order as a file-level property in
schema metadata, on the evidence that INFO order had **zero ordering conflicts**
on chr1, chr6, chr17 and chr21. That evidence is real but it is a property of
*this corpus*, not a guarantee. A VCF merged from two sources can interleave
orders; a file-level order would then emit wrong output silently — passing here,
failing elsewhere. Do not build parity on it.

### The design

Two columns, keys only, both written only when an opt-in flag is set:

| column | example | distinct values on chr21 |
|---|---|---|
| INFO key order | `platforms;platformnames;datasets;…` | 10 |
| FORMAT keys | `GT:PS:DP:ADALL:AD:GQ` | 1 (2 on chr6) |

Keys, not values. The audit found **zero** INFO-value and sample-value
differences, so values already round-trip through the typed columns. Only order,
and the per-record FORMAT key list, are lost. Both columns dictionary-encode to
almost nothing, against ~700 B/record for whole-line passthrough.

### Why each is unavoidable

- **FORMAT order varies per record.** chr6 carries both
  `GT:PS:DP:ADALL:AD:GQ` and `GT:AD:PS` in one file — different orders of
  overlapping keys. No file-level order can satisfy both.
- **`PS` cannot be recovered.** `##FORMAT=<ID=PS,Number=1,Type=Integer>` means a
  source `.` parses to NULL in an `Int32Array`, identical to the key being
  absent. The distinction is destroyed at parse time. The serializer currently
  guesses via `filter_all_missing_format_fields`, and cannot do better without
  being told the record's key list.
- **INFO order** is per-record for safety, per the draft-2 reasoning above.

### Completeness check

With both columns carried, all three strict-mode classes close: INFO order
directly; FORMAT order directly; `PS` because the serializer stops inferring
keys from non-missing values and emits the carried list. Sample-column key order
follows the FORMAT keys, so it is the same fix. Everything else already matches
— CHROM/POS/ID/REF/ALT/FILTER, INFO values and sample values all showed zero
differences in the audit.

### Residual, worth naming

**QUAL is byte-correct for this corpus, not in general.** Phase 1 renders at f32
source precision, so an input written `50.0` returns as `50`. Every QUAL in
HG002 is integral, so it cannot bite here, but strict parity on an arbitrary VCF
still depends on bio-formats #241 (`qual` as `Float32` rather than widened to
`f64`).

## 4. Implementation plan

Three repos in a chain. Each step is independently testable; do not batch them.

### Step 1 — bio-formats reader

Branch from `feat/vcf-record-passthrough` (it already carries `df53b18`).

1. Add an opt-in flag to the VCF reader/table provider, e.g.
   `carry_record_layout: bool`, defaulting **off**. A non-parity read must pay
   nothing.
2. When on, add two `Utf8` columns — an INFO key order and a FORMAT key string,
   keys only — populated from `record.info()` and `record.samples()` in the
   record loops.
   Name them via constants in `bio-format-core/src/metadata.rs` beside the
   existing `VCF_RAW_RECORD_COLUMN`.

Record loops in `datafusion/bio-format-vcf/src/physical_exec.rs`: `:756`,
`:987` (with `read_record` at `:992`), `:1287`. Check for others with
`grep -n 'read_record\|read_records'`.

3. Teach the serializer to use them: order INFO by the carried key list
   (appending keys the list does not mention, such as a newly added `CSQ`), and
   emit the carried FORMAT key list verbatim instead of deriving one from
   non-missing values. That last part is what restores `PS`.

Verify: a read/write round trip over a VCF whose INFO order differs from its
header declaration and which has a FORMAT key missing in every sample. Assert
byte equality with the input. With the flag off, assert the schema and output
are unchanged.

### Step 2 — bio-functions

1. Bump the bio-formats rev.
2. Enable the carry flag on the input `VcfTableProvider` in
   `annotate_to_vcf` when an opt-in config flag is set.
3. Add both columns to the SELECT list and to `projection_names` in
   `vcf_sink.rs` — both lists must agree or the sharded path diverges from the
   serial one. Search for `select_cols` and `projection_names`.

The serializer already splices INFO; point it at the carried key order and the
carried FORMAT key list.

Verify: an end-to-end test annotating a small VCF whose INFO order differs from
its header declaration, asserting the output line equals input + `;CSQ=…`.

### Step 3 — vepyr

1. Bump both engine revs on `chore/bump-to-header-parity-revs`.
2. Surface the flag through `annotate()`. Suggest defaulting it **on** for VCF
   input, since parity is the product goal — but make that an explicit decision,
   not a default that arrives silently.

---

## 5. The verification gate

Build cleanly first. `maturin develop` fails if both `VIRTUAL_ENV` and
`CONDA_PREFIX` are set:

```bash
cd ~/research/git/vepyr
env -u CONDA_PREFIX -u VIRTUAL_ENV uv run maturin develop --release > /tmp/build.log 2>&1
grep -q 'Installed vepyr' /tmp/build.log && echo OK || tail -20 /tmp/build.log
```

**Check the install marker, not an exit code.** A piped `cargo`/`maturin`
invocation returns the pipe's status, not the build's — this masked two real
failures during phases 1–2 and produced a "successful" run against a stale
binary whose output was byte-identical to the previous one.

Then annotate and compare:

```bash
python3 e2e-testing/scripts/md5_concordance.py \
  --pair e2e-testing/results/116/fast_chr21/vep_chr21_merged.vcf \
         /tmp/vepyr_chr21_parity.vcf \
  --mode strict --explain
```

**Pass condition:** the body digests match, and `--explain` reports no record
differences. Repeat on chr22, then `--results-dir` across all 22.

Expected residual header difference: each side's own provenance lines
(`##VEP*` for VEP, `##datafusion-bio-function-vep*` for vepyr) and the
`##bcftools_normCommand` harness artifact. Annotating both sides from the *same*
normalized input removes the latter.

Consider extending `md5_concordance.py` to exclude vepyr's provenance keys from
the header digest the way it already excludes `##VEP*`, so the header
comparison reports clean.

---

## 6. Traps

- **`AnnotateVcfConfig` is `pub` with `pub` fields and no
  `#[non_exhaustive]`.** #214's two field additions broke vepyr's compile while
  bio-functions' own tests stayed green, because they all use
  `..Default::default()`. Any field you add is semver-breaking. Worth adding
  `#[non_exhaustive]`, and the next tag should be a minor bump.
- **The reviewer on these repos finds real bugs, repeatedly.** Phases 1–2 took
  5–7 review rounds each, and four of eight findings on #214 were the same
  error: matching a substring where the check needed structure. Expect
  iteration; budget for it.
- **QUAL is `f32` on the wire.** noodles returns `Option<f32>` and
  `physical_exec.rs` widens with `v as f64` (`:821`, `:1076`, `:1352`, `:2959`).
  Phase 3 makes this moot for passthrough records, but it still governs any
  reconstructed output.
- **Cost of passthrough, measured:** +703 B/record carried, about **+10.3%** of
  output bytes and **~0.3%** of peak RSS. Input columns transit the annotation
  engine as `Arc` clones (`annotate_provider.rs:7191`), not copies, so the
  engine cost is a refcount. The sink should get *faster* — it drops roughly 20
  heap allocations per row.
- **The e2e corpus is blind to fractional QUAL.** Every QUAL in HG002 is
  integral, hence exact in `f32`. A 4.1M-record pass proves nothing about
  QUAL precision; #240's review caught that class only by reading the code.

---

## 7. Reference

- Comparator: `e2e-testing/scripts/md5_concordance.py` — `canonical` normalizes
  cosmetic serialization, `strict` hashes bytes, `--explain` classifies
  differences. Exit 0 / 1 / 2.
- Audit report: https://claude.ai/code/artifact/f031a41a-03df-4842-90e3-c997b41d775b
- VEP's own rule: `ensembl-vep/modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm`,
  `get_all_lines_by_InputBuffer`.
