# Estimate ClinVar grid work at actual variant positions

The user requested the original plan's second optimization, ClinVar worker load
balancing, as a stacked PR while the current full-input benchmarks run. This is
separate from the two input-setup edits already in functions #327 / vepyr #277.
The approved direction is to improve estimated work before considering a new
scheduler. No numerical wall-time gain is promised.

## Ownership and frozen source

- Functions owns the estimator. Base:
  `6f996176653a835a04dca77ceee514f5055e2921` (PR #327).
- Vepyr carries the functions pin. Base:
  `f2285c65f6a729d3e547033d219ba243f1ec65bb` (PR #277).
- Formats remains `e0379fb28a0d4d74f8da4c9cbd357182361349f9`; no format/schema
  change or cache rebuild is needed.

Three read-only repo analyses confirmed this ownership before any implementation.

At that functions revision, `annotate_provider.rs:11880` assigns coding-length
cost to an entire transcript span, including introns and UTRs. The existing
minimax partitioner at line 11933 already minimizes the maximum estimated load;
it cannot correct a misleading estimate. A prior chr2 profile had its last
worker finish at 49.03 s and its penultimate worker at 32.51 s. This identifies
an imbalance opportunity, not a predicted whole-input improvement.

`ContigBufferGrid` at line 10475 retains every original input start and ALT-unit
count. The count pass projects start/ALT at lines 16013-16015, preserves each
row at lines 12100-12104, and supplies those positions to the estimator at lines
16441-16443. Thus the estimate can count actual exon-enriched ClinVar positions;
it must not assume uniform variant density over a buffer's genomic envelope.

## Implementation

1. Reuse the existing shared exon/translation indexes, constructing them once
   after context loading and before grid planning. Do not add per-worker copies.
2. Estimate each actual row as ALT units times a positive row base plus base
   transcript-overlap work plus a coding premium only inside exon/CDS
   intersections. Reuse the bounded coding-length premium; do not build sequences.
3. Merge overlapping coding intervals for each transcript so duplicate exon
   metadata cannot count its premium twice. Missing or invalid coding geometry
   retains base transcript cost. Preserve inclusive endpoints and strand symmetry.
4. Replace the mean-derived equal per-buffer floor with explicit input-unit
   work, including the partial final buffer. Keep saturating arithmetic.
5. Retain the minimax partitioner, worker count, 5000-unit logical buffers,
   tie ownership, warm-up reach, HGNC/state replay, output chunks and ordered
   shard assembly. Dynamic tasks and SIFT admission are outside this change.
6. Drop temporary interval arrays before worker activation. Identify the cost
   model and each slice's owned row bounds in the existing diagnostic trace,
   alongside its estimated weight, to correlate cuts with worker completion.

The estimate uses variant starts, not full REF/END geometry. A deletion starting
outside an exon but crossing into it remains an approximation; expanding the
input projection is outside this first candidate. The coefficients and effect
on replay volume must be measured rather than assumed optimal.

No biological annotation rule is being ported. Existing Ensembl VEP116 buffer
semantics remain unchanged: `InputBuffer.pm:210` fills to the configured size,
and lines 223-235 retain the next chromosome's first variant. Consequence,
HGVS, transcript editing and allele minimization code remain unchanged.

## Regression proof

- Two buffers with identical envelopes/counts/ALT totals but interior starts
  concentrated in coding exons versus introns must receive different coding
  premiums. The current span-wide estimate cannot distinguish them.
- Cover intronic and UTR starts, inclusive coding endpoints, both strands,
  overlapping/duplicate exons, missing/invalid CDS, repeated coordinates,
  multi-ALT units, partial final buffers and saturated weights.
- A synthetic skewed grid must change its cut while retaining complete ordered
  ownership of existing logical buffers. Keep existing minimax and seam tests.
- Add or extend a regression on the actual BGZF sharded VCF path across several
  default 5000-unit boundaries, with tied starts, multi-ALT and stateful seams.
  LazyFrame run-pool tests alone do not exercise this path.

The actual BGZF regression uses the existing merged MT fixture with 56,004 rows,
56,008 ALT units and twelve default buffers shared by eight workers. It checks
cost-dependent cuts and exact serial/parallel body and input order. That fixture
does not establish a dedicated HGNC donor/recipient carry seam; existing engine
HGNC replay tests remain the coverage for that behavior.

## Baseline, measurement and stack

The current native inputfix binary is immutable, SHA256
`b92ecf8c69b108bb939c529f82b51b63c7d84e367290534208115de0e76db8eb`.
Its completed ClinVar chr2 diagnostics provide a pre-edit baseline at 1 and 8
workers, with raw body MD5/order passing and no compiler overlap. Archive:
`/private/tmp/vepyr-clinvar-opt-run/followup-optimizations/merge-and-input-followup/control-clinvar-chr2-1/inputfix/archive/`.
The pre-edit source and evidence hashes are frozen in the sibling
`scheduling-pre-edit-baseline.json` and durable benchmark archive.

The user cancelled remaining chromosome runs and requested full HG002 and
ClinVar at eight workers. Those runs, and the subsequently authorized fresh
pre-input-fix baseline, take priority. No local build, test or performance job
may overlap them. Source preparation and remote draft-PR CI may proceed in
isolated worktrees. Preserve every existing failed/noisy observation; do not
claim a skipped confirmation passed.

After that queue finishes, build with the same native release recipe, run
focused and full correctness checks, and compare against the inputfix baseline,
not merged326 (which would conflate this change with input setup). Inspect wall,
total CPU, average cores, RSS, completion spread and replay rows. Keep existing
performance thresholds and exact body MD5/order requirements. Any additional
large local benchmark beyond the authorized queue must follow the user's scope.

Stack functions `perf/clinvar-work-balance` on `perf/grid-input-partitions`
(PR #327), then pin its commit in vepyr's matching branch stacked on PR #277.
Open drafts, request Codex and Claude reviews, and keep unrun validation explicit.
No merge or automatic post-merge re-pin is authorized for these new PRs.
