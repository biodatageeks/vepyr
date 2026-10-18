# Issue #200: translate mitochondrial transcripts with their codon table

Status: **approved 2026-10-18** with the recommendations in section 9: engine
fallback plus the reader fix, the cache republish as a separate later step; the
ND2 seq edit split out as #205; branches stacked on #199; a two-layer vepyr
regression test; #202 widened to the 20 large-deletion records.

Issue: [biodatageeks/vepyr#200](https://github.com/biodatageeks/vepyr/issues/200).
Analysis it came from: `docs/superpowers/plans/2026-10-18-clinvar-chrmt-mismatches.md`
on branch `analysis/clinvar-chrmt-mismatches`.

Sources read: formats `v1.14.0` = `72efe67` (the ensembl-cache crate is
byte-identical on `master` `4b58ea6`), functions `v0.23.1` = `b960112`, vepyr
`master` `3042507`, the #199 branches (functions `2cd4e06`, vepyr
`fix/csq-transcript-order-199`). Ensembl `ensembl-variation` at `2fb834b`
(116.0) and `db7394a` (116.2), `ensembl-vep` at `release/116.0` and
`release/116.2`, and BioPerl `Bio::Tools::CodonTable` as shipped in the
`release_116.2` image. The relevant modules are identical between the two
releases, except for the unrelated extTer hunk in
`TranscriptVariationAllele.pm` (#203).

## 1. Defect

The engine translates every transcript with the standard genetic code. VEP
uses the transcript's codon table, which is 2 (vertebrate mitochondrial) for
every MT transcript. 294 of 3,134 ClinVar chrMT records differ, in
`Amino_acids`, `HGVSp`, `Consequence`, `IMPACT`, `SIFT` and `PolyPhen`.
Fixture: `e2e-testing/golden/116/clinvar/mismatches/chrMT/codon_table/`
(12 records in ND1).

## 2. The rule, quoted from Ensembl

**Where the table comes from.** `BaseTranscriptVariation::_codon_table`
(BTV.pm:1146-1164) reads `$tran->{_variation_effect_feature_cache}->{codon_table}`,
then the slice's `codon_table` attribute, then 1. In the raw caches (116 and
115 merged), every MT transcript has VEFC `codon_table = "2"`, stored as a
string; nuclear transcripts store `1` as an integer. The GFF/GTF path sets 2
when `$chr eq 'MT'` (ensembl-vep `BaseGXF.pm:765-767`).

**Table 2 versus table 1** (BioPerl, in the image): TGA `*`→`W`, ATA `I`→`M`,
AGA `R`→`*`, AGG `R`→`*`. Stops: TAA, TAG, AGA, AGG.

**Where VEP uses the table, and where it does not.** This is the part that
needs care. VEP is not consistently table-aware.

Translated with the transcript's table (`translate(undef, undef, undef, $codon_table)`, no `-complete`):

- `TVA::peptide` (TVA.pm:692-788). This feeds `Amino_acids`, every
  consequence predicate and SNV/in-frame HGVSp. A leftover partial codon adds
  `X`, and ambiguous alleles return undef.
- `_ins_del_stop_altered` / `_cil` (VariationEffect.pm:1472-1480,
  1527-1535): the codon now at the old stop position, tested `eq '*'`.
- The reference protein `_peptide` (BTV.pm:1117-1131) comes from the cache's
  `peptide`, which is table 2 with `M` at position 1.

Translated with table 1 (`->translate()` with no table argument), **including
on MT**:

- `_get_fs_peptides` (TVA.pm 2327/2333).
- `_check_for_peptide_duplication` (2444/2450).
- `_stop_loss_extra_AA` (2486/2492).
- `_get_del_peptides` (2549/2553/2559).

Confirmed by two VEP outputs: MT:10121 A>AT is `p.Trp23LeufsTer44` (table 1
gives 44, table 2 would give 10), and MT:14488 is `fsTer14` (table 1 gives 14,
table 2 would give 23).

Start/stop predicates (VariationEffect.pm):

- `start_lost` (851-904), `stop_gained` (1228), `stop_lost` (1260),
  `stop_retained` (1290-1323), `synonymous`/`missense` (1077-1098) compare
  peptides, so they follow the table.
- `start_retained_variant` and the start-altered helpers compare the literal
  string `'ATG'` (VE:941, 1022, 1063). Table 2's extra start codons never
  matter.

**One more rule, out of scope here (section 9, question 2).** For the
reference allele's peptide, VEP applies translation seq edits (TVA.pm:739-770).
Every Ensembl MT transcript carries `amino_acid_sub 1-1 M`, so an Ensembl
ND2 codon-1 change (ATT start) reads `M/I` and `start_lost`, while RefSeq
ND2 reads `I` and `synonymous_variant`. The converted cache has no
seq-edit column, so this needs a new formats column and a cache rebuild.
One ledger record (MT:4472) depends on it; none of the 12 fixture records do.

Records confirming the rule: the agent re-translated every differing CSQ
entry's `Codons` with table 2. All 271 records with an `Amino_acids`
difference match VEP except MT:4472's Ensembl entry, which is the seq-edit
case above.

## 3. Ownership

| Repo | Verdict | Evidence |
|---|---|---|
| datafusion-bio-formats | **owner, layer 1** | `bio-format-ensembl-cache/src/transcript.rs:1596-1597` (JSON/Sereal) and `:2153-2156` (Storable) read top-level `codon_table`, which does not exist; the VEFC helpers right beside them (`json_promoted_vef_sequence` `:1151-1160`, `storable_promoted_vef_sequence` `:1162-1171`) show the right lookup. Every converted cache has the column null on every contig. |
| datafusion-bio-functions | **owner, layer 2** | `translate_codon_bytes` (`transcript_consequence.rs:10012`) and `translate_codon` (`:10347`) hard-code table 1; `codon_table` is not in `TranscriptFeature`, `TRANSCRIPT_CONTEXT_COLUMNS` or `load_transcripts`. |
| vepyr | carrier | Pins, a regression test, and later the cache republish. No vepyr code reads `codon_table` or translates. |

## 4. Blast radius

- **Which transcripts change:** only those whose table is not 1. Today that
  is the 26 MT protein-coding transcripts (13 Ensembl, 13 RefSeq). Nuclear
  output must stay byte-identical; the HG002 md5 gate checks that.
- **SIFT/PolyPhen** key on the alternate amino acid parsed from `Amino_acids`
  (`annotate_provider.rs:3434-3452`), so they follow automatically.
- **The formats fix changes every transcript shard's bytes.** Nuclear rows go
  from null to 1, MT rows to 2. Nothing reads the column today, so no output
  changes, but cache checksums move: `e2e-testing/golden/116/chr22/manifest.json`
  transcript sha256s, and the published `PINS` if caches are republished.
- **Latent table-1 code in formats:** `translation.rs:1041`
  (`translate_cds_table1`) re-translates RNA-edited RefSeq transcripts. On
  MT it bails out today and falls back to the edited peptide. Leave it out of
  this fix and note it in the formats PR.

## 5. The change, in dependency order

**datafusion-bio-formats** (branch off `master` `4b58ea6`):

1. Add `json_vef_value` / `storable_vef_value` (VEFC first, top level
   second) and read `codon_table` through them with the existing `json_i32` /
   `sv_i64`, which already parse strings.
2. Tests: one MT case per path with VEFC `codon_table = "2"` (string), one
   top-level fallback case, and an assertion on the real 115 chr22 fixture
   (`tests/fixtures/real_vep_115_chr22`) that nuclear transcripts read 1.

**datafusion-bio-functions** (stacked on #199's `2cd4e06`; question 3):

1. `TranscriptFeature.codon_table: u8`, resolved once at load: the cache
   column if non-null, otherwise 2 for a transcript with a translation on a
   mitochondrial contig (`is_mitochondrial_chrom`), otherwise 1. Add
   `codon_table` to both transcript loaders.
2. A lookup-table translator per supported table (1 and 2), with table 1 as
   the existing behaviour. Codons with `N` translate to `X`, as today.
3. Thread the table into the sites VEP translates with `_codon_table`:
   `peptide_from_codon_allele` / `pep_allele_string_from_codon_allele_string`
   (Amino_acids, SIFT/PolyPhen key, SNV HGVSp), `local_peptide_from_codon_window`
   and its callers, `reference_aas_for_consequence`,
   `failed_refseq_uses_cached_translation_for_consequence`, the `new_aas` /
   stop checks in `classify_coding_change_with_semantics` and
   `classify_insertion_with_semantics`, `is_stop_codon` inside
   `mutated_cds_stop_preserved`, `frameshift_deletion_partial_stop_lost_from_codon_allele_string`,
   and the `edited_ref_translation_for_hgvsp` fallback.
4. **Leave on table 1**, to match VEP: `translated_alt_protein_for_hgvs`,
   `alternate_translation_for_vep_hgvs` and the alternate-translation fallbacks
   (the engine's copies of `_get_fs_peptides`, `_stop_loss_extra_AA`,
   `_get_del_peptides`, `_check_for_peptide_duplication`). Literal `ATG` start
   checks stay. Each call site gets a comment saying which VEP routine it
   mirrors and why it is or is not table-aware.

**vepyr** (stacked on #204's branch): pin the functions PR head and the formats
PR head (`Cargo.toml:217-219`, and the engine's own formats pins must name the
same revision), plus the regression test (question 4).

## 6. Failing tests first

In datafusion-bio-functions:

- Translator: table 2 maps TGA→W, ATA→M, AGA/AGG→`*`, and agrees with table 1
  elsewhere.
- Fixture-derived classification tests on an ND1-shaped transcript
  (`codon_table = 2`):
  - MT:3307 A>T: `M/L`, `start_lost`, `p.Met1?`.
  - MT:3308 T>G: `M/*`, `stop_gained&start_lost`.
  - MT:3669 A>G: `W`, synonymous.
  - MT:3399 A>T: `M/I`.
- An AGA stop: a COX1-shaped stop-codon change gives `stop_lost` /
  `stop_retained` per table 2.
- A table-1 control: a frameshift HGVSp count on MT stays the table-1 count
  (MT:10121 `p.Trp23LeufsTer44`).
- Fallback: a null cache value on an MT transcript with a translation resolves
  to 2; on a nuclear contig, to 1.

In datafusion-bio-formats: the reader tests above.

End to end: the `codon_table` fixture diff against VEP 116.0 and 116.2 must be
empty, run on today's published cache (engine fallback) and on a cache rebuilt
with the fixed reader.

## 7. Pin cascade

| PR | pins |
|---|---|
| formats | none |
| functions | formats PR head (all 5 `bio-function-vep` entries plus 2 `bio-function-pileup` entries), on top of #199 |
| vepyr | functions PR head + formats PR head (both crates), on top of #204 |

Before merge, the project's practice is a formats release (v1.14.1) and a
functions release, and all three repos move to the tags.

## 8. Expected gate movement

- **HG002 md5, strict, chr1–22:** no change. Nuclear transcripts keep table 1,
  and the lookup-table translator has to be byte-identical to the current one.
  Any moved digest is a regression.
- **chrMT `codon_table` fixture:** diff empty against VEP 116.0 and 116.2.
- **Full ClinVar chrMT** (on top of #199): the 297 content mismatches drop to
  about 26. What remains is outside the codon table:
  - 20 large deletions (2.3–9.1 kb) running past an MT transcript end:
    `start_lost` versus `coding_sequence_variant`, `stop_lost` versus
    `stop_retained_variant`. This is #202's mechanism, which is broader than
    CYTB.
  - MT:9204: extTer under 116.2 (#203), plus a stop-overlap consequence.
  - MT:9203: HGVSp `delextTer?` versus `?extTer?` after a 3' shift.
  - MT:6905: COX1 in-frame dup gets an extra `stop_retained`. Possibly a
    table effect through the reference protein; re-check after the port.
  - The 3 TRNT HGVS-flank records (#201).
  - MT:4472 Ensembl ND2 (the seq-edit rule).
- **Performance:** no phase should move more than 5%. A per-transcript `u8`
  plus a 64-entry lookup is no slower than today's string match.

## 9. Open questions

1. **Published caches.** Engine fallback (null on MT → table 2) plus the
   reader fix, and leave the republish of the converted caches to a separate,
   later step outside vepyr-fix (the runbook excludes cache republishing)?
   Recommended: yes. The fallback makes every existing cache correct the moment
   the engine lands, and the reader fix makes future builds carry the real
   value.
2. **ND2 seq edit (`amino_acid_sub 1-1 M`, MT:4472).** File it as a follow-up
   issue (it needs a new formats column for translation seq edits and a cache
   rebuild) rather than widen #200? Recommended: follow-up.
3. **Base branches.** Stack the functions #200 branch on #199's `2cd4e06`, and
   the vepyr #200 branch on #204's branch, because vepyr can pin only one
   engine revision. Rebase onto master once #199 merges? Recommended: stack.
   The #199 final gates then double as #200's baseline, since they measure the
   same binary #200 starts from.
4. **vepyr regression test.** Commit the raw MT cache (`1-1000000.gz`
   333 KB, `all_vars.gz` 126 KB + `.csi`, `info.txt`) and an MT-only FASTA,
   build it in the test with the fixed reader, assert `codon_table == 2`, and
   diff the 12 fixture records against VEP? Also run
   the same records on a copy of today's published chrMT shards to pin the
   fallback? Recommended: both layers, about 1.5 MB of fixtures as plain git
   blobs, as `tests/data/alt_dot_reference_merged/` does. The VEP oracle
   comes from VEP 116.0 (stored) and is re-checked under 116.2.
5. **The 20 large-deletion records.** #202 is filed for CYTB only, but the
   same mechanism covers 20 records. Widen #202's description rather than
   fold them into #200? Recommended: widen #202.

## 10. Outcome

- formats: biodatageeks/datafusion-bio-formats#262 (`3fe9f8b`). The two
  VEF-cache reader tests and the real chr22 assertion failed on the old reader
  (170 null transcripts) and pass after.
- functions: branch `fix/mt-codon-table-200` (`a325a4d` + `b2f4b51`), stacked on
  #273. The four table-2 classification tests failed with the translators
  forced to table 1; all pass, 1,181 unit tests plus the integration tests.
- **One rule found during verification, not in the plan:** VEP's
  `start_retained_variant` compares the mutated start codon with the literal
  `ATG` (`VariationEffect.pm:1022`). The engine checked for Met, which is the
  same under table 1 but not under table 2 (MT:8529 ATP6 ATG>ATA reads
  `synonymous_variant` in VEP). `b2f4b51` checks the codon; table-1 output
  cannot change.
- chrMT rebuilt from the raw 116 merged cache with the fixed reader:
  `codon_table = 2` on all 74 transcripts, every other transcript column
  identical to the published shard.
- `codon_table` fixture: empty diff against VEP 116.0 and 116.2, on the
  published cache (engine fallback) and on the rebuilt cache.
- Full ClinVar chrMT (3,134 records), on top of #199, against VEP 116.0 and
  116.2: 3,108 identical, 26 differing, all out of scope: the 20
  large-deletion records (#202), the 3 TRNT HGVS-flank records (#201),
  MT:4472 (#205), MT:9204 (#203 plus a stop-overlap consequence) and MT:9203
  (HGVSp `delextTer?`). MT:6905 is fixed. The published and rebuilt caches
  give byte-identical output.
- vepyr: `tests/test_mt_codon_table.py`, with the fixture in
  `tests/data/mt_codon_table_merged/` (1.3 MB). All 3 cases fail on the #199
  build and pass on this one.
