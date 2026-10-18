# Issue #199: order transcripts by VEP's `stable_id_dbID` key

Status: **approved 2026-10-18** with the recommendations in section 9: the pick
tie-break is in scope, no vepyr chrMT fixture yet (it comes with #200), and the
engine branch is based on functions `master` (`b960112`).

Issue: [biodatageeks/vepyr#199](https://github.com/biodatageeks/vepyr/issues/199).
Analysis it came from: `docs/superpowers/plans/2026-10-18-clinvar-chrmt-mismatches.md`
on branch `analysis/clinvar-chrmt-mismatches`.

Sources read: functions at the pinned tag `v0.23.1` = `b960112` (immutable cargo
checkout; the human checkout is on `fix/mnv-allele-trim-parity` 46d8c20, where
line numbers differ), formats at `v1.14.0` = `72efe67` (human checkout on
`feat/cooler`), vepyr `master` `3042507`. Ensembl `ensembl-variation` at
`2fb834b` (VEP 116.0) and `db7394a` (116.1/116.2), `ensembl-vep` at
`release/116.0` and `release/116.2`.

## 1. Defect

When one transcript's stable ID is a strict prefix of another's, vepyr orders
their CSQ entries differently from VEP. On chrMT this is RefSeq `ND4` / `ND4L`,
which overlap at MT:10760–10766 and share the 5 kb up/downstream window. 2,030 of
3,134 ClinVar chrMT records differ only by this swap (fixture
`e2e-testing/golden/116/clinvar/mismatches/chrMT/csq_order/`, MT:5772–5774).

The same rule decides pick-mode ties, so `--pick`/`--flag_pick`/`--pick_allele`/
`--per_gene`/`--pick_allele_gene` can choose a different transcript when every
pick criterion ties between two such transcripts.

## 2. The rule, quoted from Ensembl (identical in 116.0 and 116.2)

`ensembl-variation` `modules/Bio/EnsEMBL/Variation/BaseVariationFeature.pm:310-321`
(both `2fb834b` and `db7394a`):

```perl
sub _get_transcript_key {
  my $self = shift;
  my $tr = shift;
  my $key = $tr->stable_id;
  if(my $dbID = $tr->dbID) {
    $key .= '_'.$dbID;
  }
  return $key;
}
```

`VariationFeature.pm:666`:
`return [ map {$self->{transcript_variations}->{$_}} sort keys %{$self->{transcript_variations}} ];`
— Perl's default `sort` is byte-wise string `cmp`. `VariationFeature.pm:852-866`
puts all transcript variations first, then regulatory, then motif features.
`ensembl-vep` `OutputFactory.pm` does not re-sort them (116.0 line 309/485,
116.2 line 310/486).

Pick ties: `OutputFactory.pm` (116.2) `pick_worst_VariationFeatureOverlapAllele`
sorts `@vfoa_info` per category with `sort {$a->{$cat} <=> $b->{$cat}}` (Perl's
sort is a stable merge sort) and keeps the first of any remaining tie, so the
final tie-break is the input order, i.e. the sorted-key order above.

So the comparator is `[u8]::cmp` on `stable_id ++ "_" ++ decimal(db_id)`, with
the suffix omitted when `db_id` is null or 0. It is **not** a tuple comparison:
`ND4_8361687` > `ND4L_8378488` because `_` (0x5F) > `L` (0x4C), and dbIDs compare
as text (`X_9` > `X_10`).

Records confirming it: MT:5772, 5773, 5774 (VEP writes ND4L before ND4 in both
116.0 and 116.2).

## 3. Ownership

| Repo | Verdict | Evidence |
|---|---|---|
| datafusion-bio-functions | **owner** | `transcript_consequence.rs:1044` sorts `tx_refs` by bare `transcript_id`; the CSQ sort (`annotate_provider.rs:6825-6834`) compares `feature_type.rank()` then `transcript_idx`, so CSQ order and the typed/LazyFrame columns (`:6848`, `:7400`) follow that array. `sort_pick_candidates_by_vep_order` (`annotate_provider.rs:3109-3127`, via `pick_assignment_feature_id` `:2963-2979`) compares bare `transcript_id` strings. `db_id` is never loaded: not in `TranscriptFeature` (`transcript_consequence.rs:235`), not in `TRANSCRIPT_CONTEXT_COLUMNS` (`annotate_provider.rs:10125`), not in `load_transcripts` (`:4259`). |
| datafusion-bio-formats | carrier, **no change** | Writes `db_id` Int64 nullable (`schema.rs:183`) from the transcript hash's top-level `dbID` on both the Storable (`transcript.rs:1923-1927`) and text paths (`:1358-1362`), the same value as Perl's `$tr->dbID`. `stable_id` is not rewritten. Export dedup (`export_query.rs:82-88`) leaves no duplicate stable IDs per contig. functions already pins `v1.14.0`. |
| vepyr | carrier, **pin bump only** | Passes CSQ through; no sort, explode or dedup of CSQ entries (`src/vepyr/__init__.py`, `src/annotate.rs`, `src/lib.rs`). |

## 4. Blast radius (read-only scan of every converted transcript cache)

| Cache | Transcripts | `db_id` null/0 | Duplicate stable IDs | Prefix pairs within ±5 kb (overlapping) | Order flips |
|---|---:|---:|---:|---:|---:|
| 116 merged | 903,421 | 0 | 0 | 1,130 (877) | all |
| 116 ensembl | 669,072 | 0 | 0 | 0 | 0 |
| 116 refseq | 234,349 | 0 | 0 | 1,130 (877) | all |
| 115 merged | 765,234 | 0 | 0 | 1,130 (877) | all |

- On primary contigs: chr14 305 (IGH…), chr7 103 (TRB…, TRG…), chr22 87 (IGL…),
  chr2 82 (IGK…), MT 1 (ND4/ND4L). The rest are on alt/patch contigs.
- The IG/TR pairs are RefSeq locus records (`TRB`, biotype `other`) paired with
  segment transcripts (`TRBV5-2`, `IG_V_gene`). Their IDs are not standard RefSeq
  accessions, so the default RefSeq filter drops them on both sides
  (`is_default_refseq_transcript_id`, `annotate_provider.rs:8468-8491`; the MT
  gene-ID exception is `:8474-8483`). Checked in the VEP references: the HG002
  merged and RefSeq outputs have 893 records in the TRB locus and none carries a
  `TR*`/`IG*` RefSeq feature.
- **Under default flags only MT changes.** With `--all_refseq`, the IG/TR pairs
  would also change, toward VEP's order.
- `db_id` is never null or 0 in any converted cache. A cache without the column
  falls back to the bare stable ID, which is today's behaviour.

## 5. The change, in dependency order

**datafusion-bio-formats:** none.

**datafusion-bio-functions** (branch `fix/csq-transcript-order-199` off
`v0.23.1` = `master` `b960112`, in a new worktree so the human checkout stays on
its branch):

1. Add `db_id: Option<i64>` to `TranscriptFeature`; read it in both loaders
   (`"db_id"` in `TRANSCRIPT_CONTEXT_COLUMNS` and the `load_transcripts` column
   list, `schema.index_of("db_id").ok()` so caches without it still load).
2. One helper, e.g. `vep_transcript_order(a, b) -> Ordering`, implementing
   section 2's comparator; zero-allocation (byte iterator over
   `id ++ "_" ++ itoa(db_id)`, with a fast path when neither ID is a prefix of the
   other).
3. Use it at `transcript_consequence.rs:1044` (keeps the invariant
   "`transcript_idx` order == VEP order", so the integer comparison in the CSQ
   sort stays valid at no per-variant cost) and in
   `sort_pick_candidates_by_vep_order` (compare `transcript_idx` for transcript
   features, or the same helper). Update the comments at `:1040-1043` and
   `annotate_provider.rs:6817-6821`.
4. Update the ~18 `TranscriptFeature` test literals (8 in
   `transcript_consequence.rs`, 8 in `annotate_provider.rs`, 2 in `hgvs.rs`).

**vepyr** (branch `fix/csq-transcript-order-199` off `master`): pin
`datafusion-bio-function-vep` to the functions PR head (`rev =`), formats stays
`tag = "v1.14.0"`; commit the stale `Cargo.lock` version line (0.9.0 → 0.10.0)
with it so the `cargo-clippy` hook passes.

## 6. Failing tests first

In datafusion-bio-functions (owner):

1. **Comparator unit tests:** `ND4L_8378488` before `ND4_8361687`; `X_10`
   before `X_9` (text, not numeric); `db_id` None or 0 → bare ID; a lowercase
   continuation (`ab` vs `abc`) does not flip; fixed-length ENST IDs unchanged.
2. **CSQ order through the production path:** two overlapping transcripts
   `ND4` (8361687) and `ND4L` (8378488) built through
   `PreparedContext::new_from_refs` and the CSQ sort, asserting ND4L's entry
   comes first. Fails today.
3. **Pick tie:** two transcripts tied on every pick criterion with IDs
   `ND4`/`ND4L` → VEP's pick is ND4L. Fails today.

No existing engine test has a prefix pair, a duplicate stable ID or a `db_id`.
The existing `transcript_consequences_sort_by_feature_type_then_id`
(`transcript_consequence.rs:21144`) re-implements the sort inside the test, so
it cannot catch this; it should call the production comparator instead.

End to end (vepyr side): the chrMT `csq_order` fixture,
`vepyr annotate -i csq_order/input.vcf ... --dir_cache .../116_GRCh38_merged`,
then `diff` against `vep.vcf` must be empty. Today it differs on all 3 records.
Neither the HG002 golden (chr1–22 only) nor any `tests/` fixture contains an MT
record, so the standard gates cannot see this defect.

## 7. Pin cascade

Final pins after `datafusion-bio-functions#273` merged:

| PR | pins |
|---|---|
| formats | no change; `v1.14.0` |
| functions | #273 squash-merged on functions master as `4704ff9`, on top of #272 (`483a499`) |
| vepyr #204 | functions merge commit `4704ff9a89c1190ca7c9d48f0da44bc5b98353c7` by `rev`; formats `v1.14.0` |

The merge commit is reachable from functions master, so the vepyr review pin no
longer points at a PR-only commit. Move it to a release tag when one carries
this commit.

## 8. Expected gate movement

- **HG002 md5, strict, chr1–22:** no change (the only flipping pairs on those
  contigs are filtered by the default RefSeq whitelist). Any moved digest is a
  regression to triage.
- **chrMT:** `csq_order` fixture diff empty under VEP 116.0 and 116.2; the full
  ClinVar chrMT comparison drops from 2,327 to 297 differing records (the 297
  content mismatches from the codon table and the HGVS flank remain).
- **Performance:** no phase should move. The sort runs once per buffer over that
  buffer's transcripts, and the per-variant CSQ comparison stays an integer
  compare. The gate bar is no phase >5%, wall and RSS within 10%.

## 9. Open questions

1. **Pick tie-break in scope?** VEP's pick fallback is the same sorted-key
   order, and fixing only the CSQ order would leave the two orders disagreeing.
   Recommended: include it.
2. **vepyr regression test.** Add a small vepyr test that annotates the three
   chrMT `csq_order` records against a chrMT slice of the merged cache (new LFS
   fixture, roughly the chrMT transcript/exon/translation/variation Parquet), or
   rely on the engine unit tests plus the manual fixture diff? Recommended: the
   engine tests plus the fixture diff as a step-8 gate; a vepyr fixture can come
   with the codon-table fix (#200), which needs chrMT data anyway.
3. **Base branch vs engine PR #272** (projection pushdown, touches
   `annotate_provider.rs` around `TRANSCRIPT_CONTEXT_COLUMNS`). Resolved: #272
   merged as `483a499`; #273 was merged on top of it.

## 10. Outcome

- Engine: biodatageeks/datafusion-bio-functions `fix/csq-transcript-order-199`
  (`2cd4e06`, review follow-up `585876d`), squash-merged as `4704ff9` on top of
  #272 (`483a499`). The vepyr PR pins the full merge SHA. The two tests that exercise production code failed before the
  fix (`prepared_context_indexes_transcripts_in_vep_order`,
  `test_apply_pick_tie_breaks_by_vep_stable_id_db_id_key`) and pass after;
  1,175 unit tests plus the integration tests pass, clippy and fmt are clean.
- chrMT `csq_order` fixture: empty diff against VEP 116.0 and 116.2.
- Full ClinVar chrMT (3,134 records) against VEP 116.0 and 116.2: 2,837
  identical, 0 order-only, 297 content mismatches (was 807 / 2,030 / 297).
- Baseline gates at vepyr master `3042507`: strict md5 22/22; 8 workers 74.1 s,
  9,101,872 KiB; 1 worker 290.0 s, 4,181,856 KiB. Final gates run on the final
  pins after review. The branch now includes the latest vepyr master handoff
  runbook update; record the final strict md5, performance comparison, and
  `/parity-wgs` result here after they pass on the final head.
