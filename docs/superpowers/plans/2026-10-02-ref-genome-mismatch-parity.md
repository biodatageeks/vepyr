# Coding annotations when the supplied REF differs from the genome

Status: implementation authorized on 2026-10-02, with the additional requirement
to replay the Ensembl VEP source logic exactly. Baseline strict parity passed
at both worker counts before source or test edits; implementation is in progress.

Issue: https://github.com/biodatageeks/vepyr-porting-tests/issues/221

## Defect and reproduced evidence

For normalized `21:25587759 AC>A`, the GRCh38 reference is `TA`. VEP 116
annotates the deletion using the transcript CDS. The current functions engine
rejects coding classification when the supplied, strand-adjusted REF differs
from the CDS slice. The frameshift term and HGVSc survive, while HGVSp, CDS and
protein positions, amino acids, codons, and applicable domains disappear.

Reproduced through the installed vepyr CLI, independently of the porting-test
harness's unused Cargo patch reported in the issue. The installed extension
contains only functions checkout `547dfda` and formats checkout `00487b9` source
paths, matches `target/release/lib_core.dylib` byte for byte, and has SHA256
`128ceae2a5356193768db0a709da1f011500e0fb2a6e493da01e1c53638b8de0`.

The fixture was fetched from porting-tests commit
`c7d6c76ba10c6d94171743907c268e93bb7d4250`, then normalized with
`bcftools norm -m -both --no-version`. No FASTA correction was applied to REF.
Both tools used release 116 Ensembl caches and the same GRCh38 primary-assembly
FASTA. The Docker image was pinned by digest:
`ensemblorg/ensembl-vep@sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11`.

| Case | Docker VEP 116.0 | Current vepyr |
|---|---|---|
| Original four records: deletion/insertion with INFO and plain twins | body MD5 `1b018db69651fc500c0ec1aca821dcd8` | body MD5 `30411e1a6048e59aa446e11e6ef0b84f` |
| Genome-matching twins, `TA>T` / `T>TC` | `05e04c0828106661b3a0b725c2512c72` | identical body |
| Original `A>AC` insertions | fully annotated | identical record bodies |
| Original fixture with VEP `--check_ref` | skips both deletions with REF warnings; retains both insertions | separate VEP control, no equivalent option added |

Fresh Docker output is byte-identical in its body to the committed oracle.
Its complete CSQ values also equal those of the genome-matching twins. The
original fixture has 136 groups and 80 CSQ fields: 8 groups differ, with 8
differences each in HGVSp, CDS_position, Protein_position, Amino_acids and Codons,
plus 4 in DOMAINS. That is **44 differing values**; the issue's prose count of
26 per deletion is an arithmetic error (the field-level evidence gives 22).
Consequence, HGVSc, cDNA position, record fields, INFO and CSQ order agree.

For `ENST00000307301`, the missing values are:

| Field | Docker VEP value |
|---|---|
| HGVSp | `ENSP00000305682.7:p.Thr334ArgfsTer22` |
| CDS_position | `999` |
| Protein_position | `333` |
| Amino_acids | `T/X` |
| Codons | `acT/ac` |
| DOMAINS | `AFDB-ENSP_mappings:AF-Q9NYK5-F1&Low_complexity_(Seg):seg` |

Evidence directory:
`/Users/mwiewior/workspace/data_vepyr/evidence/issue-221-20261002/`.
It contains the downloaded fixture/oracle/config, normalized and control inputs,
all output VCFs/logs, `comparison.json`, `provenance.json`, source snapshots,
`compare.py` (assertions executed successfully), and `reproduce-baseline.sh`
(replay of the successful commands; shell syntax checked). The comparison
script deliberately asserts the current failure; it is baseline evidence, not
the future passing regression test.

The first Docker attempt mounted an existing but empty `homo_sapiens/116_GRCh38`
directory and skipped all records despite exit 0. Those invalid artifacts are
retained as `empty-cache-attempt.*` and excluded from comparisons. The successful
run mounts populated `homo_sapiens_ensembl/116_GRCh38` at Docker's
`homo_sapiens/116_GRCh38`. The replay checks `info.txt`, chr21 and the FASTA index;
the comparison requires all four records and 34 CSQ entries per record.

## Rule in the actual Docker VEP source

Read both immutable source revisions, not just the issue's interpretation:

- VEP `release/116.0`: `57ea5c52340acc1f156267f810ad162e26597082`.
- Variation: `2fb834b987ede3824e200197a838ce11e91aeb4b`, reported by this Docker
  image. Its `TranscriptVariationAllele.pm` was extracted from the image and
  compared byte for byte with the local git object. The issue cites a different
  release-116 revision (`db7394a0`), so use the anchors below.

In VEP `modules/Bio/EnsEMBL/VEP/Parser/VCF.pm:325–335`, a common REF/ALT
anchor is trimmed by comparing the alleles themselves. Here `AC>A` becomes
`C/-` at 25587760. `Parser.pm:860–881` additionally minimizes length-changing
alleles; preserve the current normalization and coordinates.

`Parser.pm:585` checks REF length against the span. Genome checking is explicitly
conditional at `:595–596`:

```perl
# check reference allele if requested
if($self->{check_ref}) {
```

The failure returns at `:624–632`. Genome replacement is separately conditional
on `lookup_ref` at `:635–646`. Neither flag is enabled by `everything` in
`Config.pm:349–377`. Thus default parity does not mean rejecting mismatched REF
or rewriting the original VCF record.

In Variation `modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm:2393`,
the protein reference comes from the transcript:

```perl
my $reference_cds_seq = $self->transcript_variation->_translateable_seq();
```

`_get_alternate_cds` extracts flanks at `:2403–2404`, strand-adjusts ALT at
`:2408–2411`, and splices it between them at `:2415`:

```perl
my $alternate_seq = $upstream_seq . $alt_allele . $downstream_seq;
```

`codon` chooses the transcript CDS for the reference at `:868`. Preserve the
explicit `_rna_edit` exception at `:872–874`, which substitutes the selected
reference allele into reference CDS. `display_codon:901–931` highlights the
coordinate span using allele length, not equality of supplied REF and CDS.
HGVS also restores feature alleles for edited transcripts at `:1507–1514`.

The 115 and 116 refs were verified to exist before comparison. Whole files
contain release changes, so they are not treated as identical. The fix and its
oracle are grounded in the exact Docker 116 sources above.

## Ownership, provenance and actual root cause

Three read-only analyses were run as required by the fix runbook. The sibling
working trees were not switched or changed.

| Repository | Source analyzed | Responsibility / required change |
|---|---|---|
| datafusion-bio-formats | tag v1.13.0, peeled SHA `00487b9edf53cbf36918d7d22a03a79fd0acf863` | Reader and cache transport are correct; no change |
| datafusion-bio-functions | pinned SHA `547dfda844e15be615b3ee94a911f05320452041` | Owns coding classification; implementation and regression tests |
| vepyr | HEAD `1c4b16ab2204779f984bd8e065473df0c4dfbaf5` | Carries behavior; integration tests and functions pin/lock update |

Sibling HEADs are drifted: functions `46d8c20a`, formats `dbc62fb1`. Rust
anchors below refer to immutable Cargo checkouts, not those working branches.
The current functions guard lines are six lines later than the issue's source.

Formats' four VCF loops copy literal REF at
`datafusion/bio-format-vcf/src/physical_exec.rs:989`, `:1239`, `:1528`, `:3141`.
The cache already supplies CDS and protein features in
`datafusion/bio-format-ensembl-cache/src/schema.rs:400–407`. INFO handling is
generic. For this VCFv4.2 fixture, noodles derives the span from REF length
(there is no END). Its VCF >=4.5 SVLEN handling is a separate contract; do not
generalize this fixture into a claim that SVLEN is always ignored.

The engine path is `annotate_provider.rs:6635` → transcript evaluation →
`add_coding_terms` → `classify_coding_change_with_semantics` at
`transcript_consequence.rs:2937` for indels and `:3029` for substitutions.
The frameshift term has already been added at `:2923–2935`. In the classifier,
the input's trimmed C reverse-complements to G, but the minus-strand CDS base
is T. With no edited reference, `:6719–6724` returns None. A second equality
rejection exists at `:6733–6735`; changing only the first one is insufficient.
Absent classification leaves coding fields empty at `:1373–1428`, and
`annotate_provider.rs:6970–6983` cannot look up domains without CDS coordinates.

**Correction to the issue's suggested fix:** the other cited guard in
`literal_indel_protein_hgvs_data` (`:5215–5227`) is not on the reported path.
Its only production caller (`:6322`) is inside
`if is_insertion && refseq_uses_transcript_shift_for_hgvsp(tx)` at `:6287`.
Changing that guard would affect a RefSeq insertion fallback without fixing
this Ensembl deletion. Leave it unchanged.

The classifier already translates the reference/mutated CDS at `:6748–6749`,
derives the frameshift extension at `:7325–7338`, and builds protein HGVS at
`:7347–7356`. Restoring classification supplies the existing HGVSp path through
`:1401–1409`; no separate formatting or domain patch is indicated.

## Implementation plan in dependency order

1. After plan approval, capture the runbook's pre-edit strict-MD5 and performance
   baseline using the same native release build as the final measurements:
   `RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`.
   Preserve the focused failure evidence above. Use isolated worktrees from the
   intended base revisions; retain the drifted sibling checkouts.

2. **Formats:** retain v1.13.0 and existing reader/cache behavior. No cache
   rebuild, schema change, or empty formats PR is warranted.

3. **Functions RED tests:** add ordinary-transcript mismatched-REF tests beside
   the classifier/evaluator tests in `transcript_consequence.rs`. Use synthetic
   plus- and minus-strand CDS fixtures and exact expected coordinates/codons/
   peptides. Add provider/output coverage for the real chr21 fixture so DOMAINS
   and CSQ assembly are tested, not only classification. Record assertion
   failures against the unchanged engine before implementation.

4. **Functions implementation:** select the coding reference locally from the
   checked mapped CDS slice on the ordinary transcript path. Keep the supplied
   REF's length, allele-derived normalization and mapped span for replacement;
   retain ALT and strand handling. Use VEP's actual RNA-edit attribute condition, captured in a new
   `TranscriptFeature.has_rna_edit` field before hydration can infer coordinate
   edits. Both cache decode paths initialize it from the existing typed
   RNA-edit columns. Only this condition permits reference feature-sequence
   substitution; ordinary REF/CDS mismatch is not a rejection. Remove the
   unconditional equality postcheck. Preserve bounds, contiguity, length and missing-sequence
   safeguards. The existing ALT splice and translation should then supply all
   six fields without downstream special cases.

   BAM, mapper, or spliced-sequence state alone does not activate the VEP
   exception. Preserve immutable attribute evidence when inferred edits are
   copied between workers. Test actual edits (including poly-A), no-edit RefSeq
   state, and inferred edits independently.
   Keep canonical HGVSp reference, failed-BAM peptide fallback, offset mapping,
   and the literal insertion helper unchanged. Do not change VariantInput REF,
   strict cache allele matching, or the recent REF==ALT skip.

5. **Functions GREEN tests:** run the new failures and existing RefSeq edited
   reference, offset, failed-BAM, minus-strand USED_REF, CDS-source, issue-118
   boundary, and REF==ALT regressions. Broader wrong-REF substitutions can change
   consequence/severity and activate SIFT/PolyPhen; they need exact oracle
   assertions, not only nonempty-field checks.

6. **vepyr:** add a focused `tests/test_ref_genome_mismatch.py` integration test
   and a purpose-built input/oracle/cache slice containing the chr21 transcripts.
   Verify CLI/output_vcf, LazyFrame CSQ and typed projections, and indexed
   multi-worker output. The existing chr1 golden and MNV fixture use matching
   REF and cannot prove this fix. Update the functions rev and Cargo.lock after
   the upstream fix; no Python or Rust binding signature change is expected.

7. Verify the existing porting fixture against the actual fixed engine and
   require the original oracle MD5, without changing its REF or expected output.
   Verify Cargo's resolved source and compiled binary: an unused patch warning
   is not a valid run of the proposed fix. Use a fresh/isolated porting checkout;
   the local checkout predates this fixture.

8. Rebuild natively, run required pre-commit hooks and targeted Rust/Python
   suites, then repeat the runbook's full parity and performance gates. If a
   fast-iteration maturin build is needed, unset CONDA_PREFIX first and do not
   use that build for either measurement bookend.

## Regression matrix and acceptance

| Case | Required result |
|---|---|
| Original four chr21 records | Exact VEP body MD5 `1b018db69651fc500c0ec1aca821dcd8`; all six fields restored |
| Genome-matching twins and wrong-anchor insertions | Existing bodies remain identical |
| INFO-free twins, chr21+chr22 input, chr21-only cache | Same focused fix; no dependence on INFO, contig count or unrelated cache shards |
| Plus/minus strand wrong-REF SNV, MNV, deletion and delins | Transcript-based reference codons/protein and correct span/highlighting; fresh VEP oracle for each |
| Wrong REF with ALT equal to transcript base | Follow VEP's annotation; do not confuse with literal input REF==ALT |
| In-frame, frameshift and shifted-repeat indels | Correct classification and HGVSp after existing shifting rules |
| Native RefSeq, edited RefSeq, failed BAM and mapper offsets | Exact oracle behavior; special reference handling preserved |
| Missing CDS, discontinuous mapping, boundary/incomplete CDS | Existing omission and fallback behavior preserved |
| CLI, direct Rust, LazyFrame CSQ/typed fields and indexed workers | Consistent behavior through shared engine |

The original four-record case and genome-matching twins have full VCF-body
assertions. Synthetic classifier cases use exact codon/peptide values from
unmodified Docker VEP methods for eight allele shapes on both strands. Existing
Rust tests cover the RefSeq, boundary and fallback paths. This is not a claim of
full VCF parity for every possible mismatched-reference allele shape.

Broader full-VCF Docker probes at the same locus exposed pre-existing behavior
outside issue 221: HGVSc uses the supplied REF where VEP uses reference sequence
(and may minimize to a different event or suppress reference-equal HGVS), and a
complex in-frame delins receives `inframe_deletion` instead of VEP's
`protein_altering_variant`. These are retained in the external `matrix.*`
evidence for separate work, not accepted as exact VCF parity. They do not occur
in the original deletion/insertion fixture or its corrected-reference controls.

One adjacent difference is fixed here because it is directly in the coding
fields being restored: VEP `display_codon` uppercases every nonempty ALT,
including a shorter replacement. Both frameshift and in-frame delins now use
that rule; pure deletion formatting is unchanged.

Expected parity movement: the focused failing fixture becomes byte-identical;
valid-reference controls and existing full-genome digests should remain stable.
If any full-genome digest changes, use field comparison to explain it and retain
strict MD5 as the gate. Do not accept unaccounted changes as expected fallout.

Performance risk has two parts. For matching-reference input, the classifier
already loads the CDS and slices the affected span before the failing guard.
Select a borrowed slice locally: add no FASTA/cache lookup, full-CDS clone, or
extra translation pass. Preserve the matching-reference path and avoid
duplicating RefSeq-state checks unnecessarily. Little overhead is expected,
but this is a source-based expectation, not a measured result.

For mismatched coding records, the current early return skips alternate-CDS
construction, translation, protein HGVS and domain lookup. Correct annotation
necessarily executes more work and emits more data. Inputs with many such
records can therefore take longer and allocate more temporary memory; cost
depends on affected transcript count and sequence length. Measure a separate
mismatch-heavy batch to quantify that cost, while using matching-reference
input to detect overhead imposed on previously complete annotations. Do not
hide an ordinary-input regression behind the additional correctness work.

The pre-edit baseline is captured below. After a discarded warm-up,
compare baseline/final at 1 and 8
workers on the same host, build flags, input and output settings. Require every
tracked phase within 5%, wall time and peak RSS within 10%, using
`tools/vepyr-fix/compare_runs.py`. Report absolute RSS movement too. Capture
baseline before any implementation or test edit, per the runbook.

## Publication and hand-off after implementation

Two linked draft PRs are sufficient: functions first, then vepyr pinned to the
functions PR head. Keep formats v1.13.0 in both. Re-pin vepyr after every
upstream review change, verify the actual dependency graph, and run the review
and verification loop from `docs/runbooks/applying-a-vepyr-fix.md`. No upstream
tracking issue or change to porting issue #125's fixture decision is required
for this plan. Human review/merge and the subsequent merge-commit re-pin remain
the runbook's final hand-off; the functions implementation is tracked in PR #264.

## Implementation evidence (in progress)

- Native baseline build used the prescribed uv sync command with native CPU flags.
- All 4,096,123 WGS records passed strict MD5 at 1 and 8 workers; body digest
  `157fca5b065c20eb7cec3e7a18ef28b7`. BGZF output and an 8-GiB free-space
  floor were used identically in the measurement driver.
- Baseline annotation: 8 workers 72.180 s, RSS 8,663,024 KiB; 1 worker
  353.931 s, RSS 4,059,904 KiB. Warm-up was discarded.
- Rust RED: three new tests compiled and failed at assertions. Python RED:
  six failed and three genome-corrected controls passed. Trimmed-cache output
  digests match the earlier full-cache reproduction on every input mode.
- All 1,163 Rust unit tests and 7 integration tests pass (3 existing tests ignored).
- The initial Python build passes 1,696 tests (2 skipped), including all nine new
  mismatch/control tests. The final pinned build is re-tested before measurement.
- Direct execution of Docker VEP's unmodified codon/display_codon and
  alternate-CDS methods yielded gCt/gTt for ordinary/BAM-only state, and
  gAt/gTt for RNA-edit attributes, including poly-A. The minimal coordinate
  objects do not replace the VEP sequence methods under test.
