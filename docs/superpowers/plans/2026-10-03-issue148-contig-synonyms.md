# Issue 148: native chromosome synonyms in converted caches

## Original plan: objective and authorization

User requested the next data issue after merging PR #157, on a feature branch
from remote master. Vepyr branch `feat/issue-148-contig-synonyms` starts at
`e906fbb38fdc422dbfe86aae2c390d258e1ce500`; functions starts at remote master
`34369217daf373eb5b2d41605ba6cfaa3656494f`. Existing working trees are untouched.
This is the next separately reviewed fix in the authorized issue-by-issue queue.
No merge or auto-merge is authorized.

Before starting this fix, the original #147 ported fixture was confirmed PASS
on newly built remote master, with a fresh Docker VEP 116.2 run. Stored oracle,
fresh oracle and actual body MD5 all equal d81d73e777b5700e6fe5d7994b085726.

## Defect and ownership

`NC_000021.9:25585733 C>T` fails before annotation because the converted cache
cannot associate the accession with chromosome 21. Two ported fixtures share
this exact witness:

- `resolve_a_refseq_chromosome_accession_for_existing_variation_lookup_112b9b`
- `resolve_a_refseq_chromosome_accession_through_synonyms_2fa9d1`

Functions owns cache conversion and annotation resolution;
vepyr carries its pin and public integration tests. Formats preserves input
CHROM correctly and needs no changes.

Three independent read-only ownership analyses examined actual pinned sources.
Functions pin 3436921: cache/manifest.rs:121 supports only bare/chr and MT aliases;
annotate_provider.rs:5857 admission consequently rejects the accession. Resolving
only admission or shard paths is insufficient: variation keys, transcript
matching, buffer hydration, FASTA and plugin selection also use contig names.
The raw input name must still drive VCF/index scans, filters, ordering and output.

Native merged 116 has chr_synonyms.txt (122,763 bytes); converted merged 116 lacks
it. Functions CacheBuilder::build_entity validates raw identity at
cache_builder.rs:160 and then enters entity conversion, whose early skip is
cache/build.rs:625. Metadata must be copied after validation and before that
skip, supporting existing shards without rebuilding them.

## Ensembl rule

VEP 116.2 source pin 2cb0bbe216bb31c75de8f8000e2da7ff4fb7b451:
- CacheDir.pm:254 auto-selects `<cache>/chr_synonyms.txt`.
- BaseVEP.pm:644-656 parses whitespace-separated synonyms and inserts each pair
  in both directions: `$synonyms->{$ref}->{$syn} = 1` and the reverse.
- BaseVEP.pm:700-735 keeps an exact valid contig first, then tries explicit
  synonyms, then adds/removes chr. File parsing does not infer graph transitivity.
- AnnotationSource/Cache/VariationTabix.pm:154,224 resolves the name for the
  source, while OutputFactory/VCF.pm:314-317 emits original `_line` contents.
- ensembl-variation db7394a02523a6e519cb0d89f84b2889bac12737
  VariationFeature.pm:1733,1809,1845-1847 reads reference sequence from the
  underlying slice. Thus annotation must resolve the source reference name;
  the original output spelling is a separate concern.

Do not parse accession digits into human chromosomes or infer assembly mappings.
Direct synonym pairs are the data contract. Retain deterministic exact-name
precedence and existing canonical behavior; test ambiguous aliases explicitly.

## Implementation and regression plan

1. Capture native-release performance (discard warm-up, measure 1/8 workers) and
   strict WGS body MD5 baseline before engine/test edits. Same normalized HG002,
   merged 116 cache and reference FASTA as #147.
2. Reproduce actual accession failure and canonical control using fresh Docker
   VEP 116.2 and the same normalized bytes. Record inputs, exits and digests.
3. Add failing upstream tests for metadata export/resume, cache-driven synonym
   resolution, canonical/exact-name precedence and unknown-name behavior.
   Add actual variation/transcript annotation checks that retain source CHROM.
4. Preserve the native sidecar in the converter. Load/cache its resolver once;
   carry resolved names through annotation and cache lookup while retaining raw
   names in reader predicates and output. Avoid per-row file reads/allocations
   for ordinary canonical input. No API flag is necessary.
5. Add Python regressions covering plain/CLI/indexed multi-worker output,
   LazyFrame projections/filter pushdown, mixed canonical/accession names,
   conversion and no-sidecar compatibility. Add an indel oracle to reach FASTA.
6. Verify supplied cache through an explicit evidence overlay using the same
   entity shards plus the matching native sidecar; do not invent aliases or
   silently search arbitrary sibling directories. Document refreshing metadata
   through conversion while retaining completed shards.
7. Test, lint via pre-commit, open functions draft PR then pin its head in the
   separate vepyr draft PR, request both reviewers, resolve findings and run
   final performance and strictMD5 gates. Hand off without merging.

## Expected gates

The two #148 fixtures must move ERROR->PASS, body MD5
edb68e24656fe9d502a797e109473115, original NC_000021.9 preserved. Canonical 21
control must remain 05bf31b59c70febbbace9660e9b5288e. Other campaign statuses
should not change. New master already has 178 PASS / 9 FAIL / 2 ERROR.
WGS strictMD5 must match all 22 autosomes / 4,096,123 records. This existing reference
is VEP 116.0 and is distinct from fresh 116.2 issue oracles. Performance gate:
all measured phases within 5%, wall time and peak RSS within 10%, at 1 and 8 workers.

Evidence is retained locally in the original workspace under
`e2e-testing/results/fix-issue148-20261003/`; it is not checked into this PR.

## Implementation and review checkpoint

The plan above was written before implementation, in the original workspace.
This tracked copy makes it available to reviewers of the isolated feature branch.

- Owner: [datafusion-bio-functions PR266](https://github.com/biodatageeks/datafusion-bio-functions/pull/266),
  current head `7bcec01d81c224be888f9a852f7936affde7ff93`.
- Carrier: [vepyr PR158](https://github.com/biodatageeks/vepyr/pull/158),
  pinning that exact engine head; formats remains v1.13.0.
- The initial regressions failed before implementation. The current pin passes
  1,177 upstream Rust tests (3 ignored), 20 synonym integration/conversion tests,
  and 5 previous lowercase-allele regressions. An earlier broader Python run
  passed 633 tests (2 skipped).
- Both original witnesses and an additional accession insertion match fresh
  VEP 116.2 Docker output on identical normalized inputs. Canonical controls
  also match. Full campaign: 180 PASS, 9 FAIL, 0 ERROR; exactly the two issue #148
  errors became passes and all other body digests are unchanged.
- Review fixes propagate synonym read errors with their path, remove obsolete
  metadata when the native cache no longer supplies it, and preserve source
  permissions on Unix, including repairing an existing owner-only copy.
  Each defect was reproduced by a failing test before its correction.
- Metadata preservation also covers all four standalone chromosome builders
  through the shared build layer, with native identity validated first. Four
  direct-builder regressions failed before that follow-up and now pass.
- The metadata refresh uses an evidence overlay of the supplied merged cache.
  All 7,688 original shard sizes and mtimes remain unchanged, with no Parquet
  files written. The overlay metadata was repaired from 0600 to native 0644.
- Final performance and strict whole-genome MD5 results must be posted in the
  PR handoff after both reviews and CI pass. Interrupted pre-review measurements
  are retained separately and do not count as final evidence.

Merge order is engine first, then a human-authorized re-pin to the engine merge
commit, then vepyr. Neither merge nor auto-merge is performed by this workflow.
