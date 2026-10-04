# Issue 156: retain cached variation strand through conversion and lookup

Native negative-strand known variants lose their strand during conversion and
runtime projection. Both co-located and primary matching then assume +1. Input
A/G must match cached T/C on -1 and reject T/G and A/G on -1. The raw cache
allele labels must remain available for allele-specific frequency lookup.

## Ownership and source rule

All three requested read-only repository analyses were completed before this
plan. Formats already preserves native nullable Int8 strand: schema.rs:129,
variation.rs:559 and util.rs:67,86 at 00487b9. It needs no production change.
Functions owns the dropped column and both matching paths. Vepyr carries the
engine pin and native-conversion/API/CLI/LazyFrame integration coverage.

VEP 116.2 2cb0bbe Database/Variation.pm:167–188 selects seq_region_strand;
Dumper/Variation.pm:237–265 retains -1 and omits +1. BaseCacheVariation.pm:124–128
defaults missing/dot/zero to +1. AnnotationType/Variation.pm:168–193 supplies
strands to active and unshifted matching. Variation 116 db7394a
Utils/Sequence.pm:1182,1196 complements differing strands and 1233–1237 retains
original input/cache allele labels in the match result.

Dumper/Variation.pm:333–350,398–402 keys frequencies by original cached allele.
OutputFactory.pm:1235–1242 uses the matched b_allele to select them. Clinical
reference orientation has a different rule at OutputFactory.pm:1101–1120;
preserve it rather than blindly complementing clinical labels or AF keys.

## Implementation

Branch functions from verified remote master 789aa6 and vepyr from e906fbb.
This fix is independent of the unmerged #149 and #150 algorithms.

1. Preserve optional nullable Int8 strand in functions cache schema, conversion
   and runtime projection. Validate its type when present. Older files without
   the column retain their existing forward behavior; null and zero also mean +1.
2. Carry cached strand through BatchProbeIndices and co-located matching's active
   and unshifted comparisons. Retain original b_allele for AF selection.
3. Fix the separate primary exact-match path even when no co-located sink exists.
   Keep the common forward fast path and the public scalar allele-matcher API;
   only negative rows need the orientation-aware coordinate matcher.
4. Add converter-to-physical-Parquet-to-annotation coverage with clearly labelled
   synthetic native variations, true/false matches, distinct multiallelic AF,
   clinical reference controls, forward/null/zero/legacy forms, and indel/MNV cases.
   Cover typed primary attachment, complete VCF, CLI and narrow/full LazyFrame.
5. Pin the exact functions PR head in vepyr, build natively, and iterate both
   reviewers until CI and all applicable gates are green. Never merge.

## Evidence and gates

The untouched native baseline already captured for this issue series is exactly
vepyr e906fbb, functions3436921 and formats00487b9, on the same normalized HG002
input and merged 116 caches. It predates every engine edit in this series:
workers 1/8 performance plus strict 22-autosome body MD5 (4,096,123 records).
Its original artifacts remain immutable under fix-issue150-20261003/baseline.
An independent untouched-master performance confirmation is also being archived;
it does not replace the original baseline or waive a threshold. Record reuse
explicitly. This distinct production fix requires its own final native workers
1/8 performance and strict whole-genome comparison against that baseline.

Use actual Docker VEP 116.2 for the strand helper oracle and any synthetic
whole-VCF oracle, with identical normalized input and corresponding cache data.
Prove the original production path fails synthetic positives/false-positive
controls before changing it. Record input SHA-256, body MD5, exits and exact
engine/build identities. No stock cache data is modified.

DT-6dc0ecf2c5 and DT-a49e2146f6 remain BLOCKED as natural stock-cache ports:
all 14,574,753 inspected native chr21 variation strands were missing. Synthetic
capability tests do not qualify a natural negative-strand record and cannot be
reported as those two ports passing. Existing converted caches that discarded
negative strands need reconversion; missing strand cannot be reconstructed.

The final synthetic suite isolates clinical terms across three loci because
multiple CLIN_SIG terms in actual VEP output have nondeterministic hash order.
This keeps each VCF body directly reproducible without canonicalization. The
three variant-shape oracles plus three clinical oracles all fail on the untouched
baseline. The engine change is functions#269, initially 1616bd88.
