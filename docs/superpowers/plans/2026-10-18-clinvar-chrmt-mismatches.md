# ClinVar chrMT mismatches: root-cause analysis and debugging plan

Status: analysis for review. No engine, reader or cache change has been made.

Scope: the 2,327 of 3,134 chrMT records that differ between Ensembl VEP and
vepyr 0.10.0 in the ClinVar 20261011 parity run
([`e2e-testing/golden/116/clinvar/`](../../../e2e-testing/golden/116/clinvar/README.md)),
using the fixtures in
[`mismatches/chrMT/`](../../../e2e-testing/golden/116/clinvar/mismatches/chrMT/README.md).
Every finding below is checked against **Ensembl VEP 116.2**
(`ensemblorg/ensembl-vep:release_116.2`, `VEP_SUB_VERSION = 2`).

Evidence for this document is in
[`2026-10-18-clinvar-chrmt-mismatches/`](2026-10-18-clinvar-chrmt-mismatches/):
the VEP 116.2 outputs of the three fixtures and of the TRNT experiment, the TRNT
input, and `reproduce.sh`, which re-runs all of it.

Repository revisions read: vepyr `1931257` (this branch's base,
`e2e/clinvar-parity`), datafusion-bio-functions `46d8c20`,
datafusion-bio-formats `dbc62fb`. The converted cache inspected is the local
`116_GRCh38_merged`. The raw cache is `homo_sapiens_merged/116_GRCh38`.

## Summary

| Class | Records | Root cause | Status | Repos |
|---|---:|---|---|---|
| `csq_order` | 2,030 | VEP sorts on `stable_id_dbID`; the engine sorts on `stable_id` | **confirmed** | functions |
| `codon_table` | 294 | the reader drops `codon_table` (wrong key path); the engine only knows table 1 | **confirmed** | formats, functions, caches |
| `hgvs_shift` (a) | 1 | VEP trims HGVSc to transcript ends for deletions spanning them | mechanism to read in VEP | functions |
| `hgvs_shift` (b) | 3 | VEP 3'-shifts TRNT indels by 0/1/1 bases instead of 3/5/4 | **open** | functions |

## 0. VEP 116.2 agrees with 116.0

The stored references were made with `release_116.0`. Each fixture's
`input.vcf` was re-annotated with `release_116.2` with the flags of the golden
run (`--cache --offline --assembly GRCh38 --merged --vcf --everything --hgvs
--fasta ...`):

| Fixture | 116.2 vs 116.0 (body lines) | 116.2 vs vepyr 0.10.0 (body lines) |
|---|---:|---:|
| `codon_table` | 0 | 24 |
| `csq_order` | 0 | 6 |
| `hgvs_shift` | 0 | 8 |

So every mismatch is a mismatch against 116.2 as well. The only change is in the
header: 116.2 writes `VEP="v116.2"` and different git hashes
(`ensembl=116.0d85231`, `ensembl-compara=116.dc107cc`, ...). That matters for
any runner check that pins the 116.0 identity (see `build_vep_plugin_reference.sh`
and `validate_vep_reference_identity()`).

## 1. `csq_order` — confirmed

**Symptom.** Identical CSQ entries; VEP writes RefSeq `ND4L` before `ND4`, vepyr
`ND4` first.

**VEP 116.2.** Transcript variations are returned by
`VariationFeature::get_all_TranscriptVariations` as
`map { $self->{transcript_variations}->{$_} } sort keys %{...}`, and the key is
`BaseVariationFeature::_get_transcript_key` (`BaseVariationFeature.pm:310`):

```perl
my $key = $tr->stable_id;
if (my $dbID = $tr->dbID) { $key .= '_'.$dbID; }
```

The raw cache gives:

| stable_id | dbID | key |
|---|---:|---|
| `ND4L` | 8378488 | `ND4L_8378488` |
| `ND4` | 8361687 | `ND4_8361687` |

`L` (0x4C) sorts before `_` (0x5F), so ND4L comes first. The Ensembl pair
(`ENST00000361335`, `ENST00000361381`) has fixed-length IDs, so it is unaffected.

**Engine.** `transcript_consequence.rs:1024` sorts transcripts by
`transcript_id` alone ("so that source-array index order equals lexicographic
feature-ID order"), and the CSQ sort compares those indices.

**Fix direction.** Sort on `stable_id + "_" + db_id` compared as bytes, and
compare the dbID as text, not as a number. `db_id` is already in the converted
transcript cache (ND4L 8378488, ND4 8361687), so no cache change is needed.

**Blast radius to check.** The order only changes where one stable ID is a
prefix of another overlapping one, or where two transcripts share a stable ID.
On MT the only prefix pair is ND4/ND4L. Scan every contig's transcript cache for
such pairs before changing the engine; HG002 chr1–22 md5 parity must hold.

## 2. `codon_table` — confirmed, two layers

**Symptom.** MT transcripts are translated with the standard code: ATA→Ile
instead of Met, AGA/AGG→Arg instead of stop, TGA→stop instead of Trp.
`Amino_acids`, `HGVSp`, `Consequence`, `IMPACT`, `SIFT` and `PolyPhen` follow.
SIFT and PolyPhen change because their scores are looked up by amino-acid change.

**Raw cache (read with the 116.2 image).** Every protein-coding MT transcript,
Ensembl and RefSeq, has `codon_table = 2` under
`$tr->{_variation_effect_feature_cache}{codon_table}`. The keys of that hash
are `codon_table, five_prime_utr, introns, mapper, sorted_exons,
three_prime_utr, translateable_seq` (plus `peptide`, `protein_features`,
`protein_function_predictions` and `seq_edits` on coding transcripts). The slice
carries no attributes in the offline cache. So in offline mode VEP gets the
codon table from VEFC; without it VEP would fall back to the slice attribute,
then to 1.

**Layer 1, reader (datafusion-bio-formats).**
`bio-format-ensembl-cache/src/transcript.rs` reads the scalar from the top level
of the transcript object:

- JSON path, line 1596: `json_i32(object.get("codon_table"))`
- Storable path, line 2153: `sv_i64(object.get("codon_table"))`

There is no top-level `codon_table`, so the column is null. In the converted
`116_GRCh38_merged/transcript/chrMT.parquet` all 74 transcripts have
`codon_table = null`. Every published cache is affected.

**Layer 2, engine (datafusion-bio-functions).** `translate_codon` and
`translate_codon_bytes` in `transcript_consequence.rs` (lines ~10053 and ~9718)
hard-code table 1. There are 24 call sites, and none reads a per-transcript
table.

**Fix direction.**

1. Reader: read `_variation_effect_feature_cache.codon_table`, falling back to
   the top level. Rebuild the MT cache and check the column says 2.
2. Engine: carry the codon table on `TranscriptFeature` and translate with it
   at every call site.
3. Read 116.2's start/stop logic before coding (`VariationEffect.pm`,
   `TranscriptVariationAllele.pm`, `Bio::Tools::CodonTable`). Table 2 has
   alternative start codons (ATT, ATC, ATA, ATG, GTG), and MT transcripts
   complete their stop codons with RNA edits; CYTB carries
   `_rna_edit = 1141 1141 TAA`.
4. Decide how already-published caches behave (question 1 below).

## 3. `hgvs_shift` — two different problems

### 3a. The 9,120 bp deletion at MT:6467

VEP 116.2 against vepyr:

| Feature | VEP 116.2 | vepyr 0.10.0 |
|---|---|---|
| ATP6 (inside the deletion) | `transcript_ablation`, `c.1_681del` | `transcript_ablation`, HGVSc empty |
| CYTB (deletion covers its 5' end) | `coding_sequence_variant&5_prime_UTR_variant`, `c.1_840del`, no offset, cDNA `?-840` | `start_lost&5_prime_UTR_variant`, `c.3_842del`, `HGVS_OFFSET=2`, cDNA `?-840` |

VEP trims the HGVSc to the transcript's bounds (c.1 up to the cDNA end it
reports) and does not shift. vepyr writes no HGVSc for a transcript the deletion
fully contains, and shifts the partial one. Nothing here is MT-specific: expect
the same on nuclear contigs once the full ClinVar run lands. HG002 has no such
deletions, so the golden suite cannot catch it. The CYTB `start_lost` against
`coding_sequence_variant` difference may be a separate consequence rule; check
it again after the codon-table fix.

**Next step.** Read how 116.2 builds HGVSc for alleles that extend past a
transcript's ends (`TranscriptVariationAllele::hgvs_transcript` and its helpers
for clipping and offsets), then write a failing engine test.

### 3b. TRNT 1 bp indels: VEP does not shift fully

Reference, MT:15925–15946: `CCGGAGAT G AAAA CC TTTTT CC` (A run at
15934–15937, T run at 15940–15944). TRNT and ENST00000387460 span
15888–15953 on the + strand, single exon, so n.N = 15887 + N.

| Record | VEP 116.0 / 116.2 | vepyr | Full 3' shift |
|---|---|---|---|
| 15933 GA>G | `n.47del`, offset empty | `n.50del`, offset 3 | n.50 (15937) |
| 15939 C>CT | `n.54dup`, offset 1 | `n.57dup`, offset 5 | n.57 (15944) |
| 15939 CT>C | `n.54del`, offset 1 | `n.57del`, offset 4 | n.57 (15944) |

The branch README says vepyr places these 3 bases too far 3'. The reverse is
true: vepyr's output is the HGVS-correct 3' shift and matches ClinVar's own
`CLNHGVS=NC_012920.1:m.15937del` / `m.15944dup` / `m.15944del`. VEP shifts by
0, 1 and 1 bases.

What is ruled out so far:

- **Not a VEP version change.** 116.2 gives the same output as 116.0.
- **Not MT-wide.** In the full 116.0 chrMT output VEP shifts 12 other tRNA
  indel entries (e.g. TRNF `n.19dup` offset 2, TRNV `n.55del` offset 1) and 5
  rRNA ones, and vepyr agrees on all of them.
- **Not the neighbouring CYTB.** CYTB ends at 15887, carries a +2 bp RNA edit
  and is `downstream_gene_variant` here. Re-running with
  `--transcript_filter "stable_id in TRNT,ENST00000387460"` and with
  `--transcript_filter "not stable_id in CYTB,ENST00000361789"` gives the same
  `n.47del` / `n.54dup` / `n.54del` (`vep116_2/hgvs_shift_trnt_*.vcf`).
- **Not a transcript sequence edit.** TRNT and ENST00000387460 have no
  `_rna_edit` or `seq_edits` in the raw cache, and their exons span the genome
  sequence above.

**Next steps (find the cause before any engine change).**

1. Instrument VEP 116.2: bind-mount a patched copy of the module that
   implements the transcript 3' shift (`_return_3prime` and the shift-hash
   helpers it calls) into the container, with `warn` lines printing the
   sequence window, start/end, strand and resulting shift for TRNT.
2. Synthetic probes in the same runs: the deletion written at 15934–15937
   positions of the A run, the same 1 bp deletion in a poly-A of another tRNA,
   and VEP's own options (`--shift_3prime 1`, `--shift_genomic 1`) to see which
   path produces the short shift.
3. Only once the mechanism is shown, write a failing engine test and reproduce
   it in the engine (question 2 below).

## 4. Plan

All comparisons are against VEP 116.2.

### Phase 0 — references and baselines

1. The ClinVar input is not on the development machine. Slice MT from
   `clinvar_20261011.vcf.gz` and preprocess it as in the run README
   (`-e 'ALT="."'`, `bcftools norm -m -both`).
2. Produce a full chrMT VEP 116.2 reference (about 26 s) and compare its body
   md5 with the stored 116.0 one. Expected: identical. If not, 116.2 is the
   target and the differences are recorded here.
3. Check whether `run_comparison.py` / `validate_vep_reference_identity()`
   accept a 116.2 header.
4. Run vepyr 0.10.0 / master on the fixtures and confirm they still reproduce
   the stored `vepyr.vcf` byte for byte.

### Phase 1 — `csq_order`

1. Failing engine test with two overlapping transcripts `ND4` / `ND4L` and
   their dbIDs.
2. Sort key `stable_id + "_" + db_id`, compared as bytes.
3. Prefix/duplicate stable-ID scan over all transcript caches.
4. Gates: `csq_order` fixture diff empty against 116.2; HG002 chr1–22 md5
   unchanged.

### Phase 2 — `codon_table`

1. Reader fix and test against a Storable and a Sereal/JSON MT transcript.
2. Engine: per-transcript codon table at all 24 translation sites, after reading
   116.2's start/stop codon handling.
3. Tests from the 12 fixture records (one per distinct amino-acid change).
4. Gates: `codon_table` fixture diff empty against 116.2; HG002 unchanged.

### Phase 3 — `hgvs_shift`

1. 3a: read VEP's clipping, then test and fix.
2. 3b: instrument VEP as above; no engine change until the mechanism is shown.

### Phase 4 — verification

1. All three fixture diffs empty.
2. Full chrMT md5, strict and canonical, against VEP 116.2.
3. chrY still matches; HG002 chr1–22 md5 gate unchanged.

### Delivery

Through the `vepyr-fix` workflow, as three separate pieces: `csq_order`
(engine only, small), `codon_table` (reader + engine + cache rebuild), and
`hgvs_shift` once its cause is known.

## 5. Questions for review

1. **Codon table on published caches.** Rebuild and republish the converted
   caches (116 merged/ensembl/refseq, 115), or also let the engine default to
   table 2 on MT when the column is null, so existing caches work?
   Recommendation: both.
2. **TRNT.** Match VEP's short shift even though vepyr's output is the
   HGVS-correct one? The zero-mismatch goal says yes.
3. **Reference version.** Store a 116.2 chrMT reference and relax the runner's
   116.0 identity check, or keep 116.0 stored and use 116.2 only as a
   cross-check?
