# Kickoff prompt for the next session

Copy everything below the line into a new Claude Code session started in
`~/research/git/vepyr`.

---

Continue the vepyr → Ensembl VEP byte-parity work. Phase 3 (record passthrough)
is the last piece; phases 1 and 2 are merged and verified.

**Read first:** `docs/superpowers/plans/2026-08-23-phase3-record-passthrough-handover.md`
in this repo. It has the full status, the empirical proof, the plan,
the two wrong designs I discarded, and the traps. Do not re-derive any of it.

## What you are finishing

`md5_concordance.py --mode strict` currently fails on the record body with
exactly three classes:

```
55,812  FORMAT KEYS (-['PS'] +[])
55,812  SAMPLE1 keys
55,714  INFO order
```

All three vanish if the writer stops re-deriving INFO and FORMAT layout. The
underlying approach is **proven**: on chr21, the raw input line + `";CSQ=" + csq`
reproduces the reference VEP output byte for byte on 55,812/55,812 records
(100.0000%). Re-run that proof on chr22 first as a sanity check — the pattern is
in the handover doc §2.

The serializer half is done and committed: bio-formats `df53b18` on branch
`feat/vcf-record-passthrough`, 134 tests green, unpushed.

## The design — read this before planning anything

Two earlier drafts of the handover were wrong. Both are recorded in its §3;
read them, so you don't repeat either.

- **Not blocked on noodles.** `Record::info()` and `Record::samples()` are
  public, and `Info<'r>` wraps the raw INFO substring and iterates in source
  order. No fork. Three repos: bio-formats → bio-functions → vepyr.
- **Not one column.** Treating INFO order as file-level schema metadata looked
  safe — zero ordering conflicts on chr1/6/17/21 — but that is a property of
  this corpus, not a guarantee. A merged VCF can interleave orders and would
  emit wrong output silently.

**The design:** two `Utf8` columns, **keys only**, written only under an opt-in
flag.

| column | example | distinct values on chr21 |
|---|---|---|
| INFO key order | `platforms;platformnames;datasets;…` | 10 |
| FORMAT keys | `GT:PS:DP:ADALL:AD:GQ` | 1 (2 on chr6) |

Values are not carried — the audit found zero INFO-value and sample-value
differences, so they already round-trip. Both columns dictionary-encode to
almost nothing.

Why each is unavoidable: FORMAT order varies per record (chr6 has both
`GT:PS:DP:ADALL:AD:GQ` and `GT:AD:PS`), and `PS` is `Type=Integer`, so a source
`.` parses to NULL — indistinguishable from the key being absent. The serializer
currently guesses and cannot do better without the record's key list.

The committed serializer work still applies; point it at the carried key order
and FORMAT key list instead of a whole line.

## Definition of done

```bash
python3 e2e-testing/scripts/md5_concordance.py \
  --pair e2e-testing/results/116/fast_chr21/vep_chr21_merged.vcf \
         /tmp/vepyr_chr21_parity.vcf \
  --mode strict --explain
```

reports matching body digests and no record differences, on chr21 and chr22,
then across all 22 via `--results-dir`. That is the gate for cutting release
tags — the user cuts them once md5 is confirmed.

## How to work

- **TDD.** Write the failing test, watch it fail for the right reason, then
  implement. Several bugs in phases 1–2 were caught only because the test was
  written first.
- **Verify build success by the install marker, not an exit code.** A piped
  `cargo`/`maturin` command returns the pipe's status. This masked two real
  failures and produced a "successful" run against a stale binary whose output
  was byte-identical to the previous one:
  ```bash
  env -u CONDA_PREFIX -u VIRTUAL_ENV uv run maturin develop --release > /tmp/build.log 2>&1
  grep -q 'Installed vepyr' /tmp/build.log && echo OK || tail -20 /tmp/build.log
  ```
- **Stage files explicitly.** `git add -A` swept the user's untracked scripts
  into a commit once. Name the paths.
- **Expect review iteration.** These repos' reviewer finds real bugs — 5 to 7
  rounds per PR in phases 1–2. Address findings by removing mechanism where you
  can; four of eight findings on one PR were the same substring-vs-structure
  error, and the fix that finally settled it deleted 200 lines.
- **CLAUDE.md mandates GSD workflows** for repo edits, but `.planning/ROADMAP.md`
  does not exist so `/gsd:quick` errors out. The user has been asking for direct
  edits; confirm that is still fine before your first edit.

## Loose ends you inherit

- **Uncommitted:** the handover doc and this prompt in
  `docs/superpowers/plans/`.
- **Committed, unpushed:** bio-formats `df53b18`
  (`feat/vcf-record-passthrough`); vepyr `e2bcfe9`
  (`chore/bump-to-header-parity-revs`).
- **Open issues:** bio-formats #241 (`±Inf` coercion; `qual` as `Float32`),
  bio-functions #213 (cache data-source versions in provenance).
- **Worth raising:** `AnnotateVcfConfig` is `pub` with `pub` fields and no
  `#[non_exhaustive]`, so field additions are semver-breaking — one broke
  vepyr's compile while bio-functions CI stayed green. The next bio-functions
  tag should be a minor bump.
- **Nice-to-have:** teach `md5_concordance.py` to exclude vepyr's own
  `##datafusion-bio-function-vep*` provenance keys from the header digest, the
  way it already excludes `##VEP*`, so header comparison reports clean.

Ask before starting anything that spans more than one repo at a time.
