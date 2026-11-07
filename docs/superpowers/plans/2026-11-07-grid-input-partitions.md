# Avoid repeated input setup in stateful annotation workers

Approved scope: the user requested merging functions #326 and vepyr #275 first,
then the first two input-setup fixes together. Both were merged on 2026-11-07:
functions `344fb2c3eb60154d9dc2b2d92fe58c45e410866a`, vepyr
`7df4a3c9a6c25960a129cb3a77fed0f020369364`. No additional approval is needed
for this already agreed scope.

## Ownership and evidence

The three-repository read-only analysis found both fixes belong in functions.
At the merged source, `lookup_provider.rs:236` executes a one-row prefix query
while planning, although `cache/lookup_exec.rs:1140` ignores the resulting flag
and line 1437 normalizes each row. `annotate_provider.rs:16965` inherits the
session's eight physical input partitions for each logical grid worker. Those
partitions are consumed serially by that worker (`:12716`), so they repeat tabix
loading without increasing concurrency.

Formats stays pinned at `e0379fb28a0d4d74f8da4c9cbd357182361349f9`.
Its VCF provider reads the planning state's partition target at
`table_provider.rs:1429`; `partition_balancer.rs:70` already returns one ordered
assignment for target one. No reader change or index cache is required.
Vepyr carries the new functions pin and evidence only.

No biological rule is ported. Preserve the existing buffer and HGNC semantics:
Ensembl VEP release/116.0 `InputBuffer.pm:210` reads
`while(@$buffer < $buffer_size && (my $vf = $parser->next))` and lines 223–235
retain the first variant of the next chromosome for the next buffer. The fix
must not change row/ALT order, 5000-unit logical buffers, seam warm-up, or HGNC
carry. The ensembl-variation consequence code is untouched.

## Implementation and regression proof

1. Remove private `has_chr_prefix` and its call; retain the public lookup exec
   constructor's legacy boolean argument for compatibility, passing false with
   an explanation that normalization is per row.
2. Add an optional crate-private VCF input partition override to LookupProvider.
   Apply it to the owned SessionState from DataFrame::into_parts before physical
   planning. Never mutate the shared session or repurpose the variation lookup
   cold-reader limit.
3. Set the override to one only for stateful grid workers. Keep all-partition
   consumption as a correctness fallback, and fix its outdated comment.
4. Tests: planning an input whose stream errors if read must succeed without
   polling it (expected red before fix). Indexed BGZF with repeated positions
   and multi-ALT rows must have eight ordinary partitions, one scoped partition
   per concurrent worker, unchanged parent session, and exact filtered row order.
   Exercise ordinary sequential input too. Existing buffer/HGNC/seam tests and
   real chromosome raw MD5 cover the downstream stateful boundary.

## Measurement protocol

Pre-edit native baseline binary SHA256
`c7e6a7517efcd7703d8d550a437679125e74f7e6c4fa56b0f5d34926c6aa28a4`
was built from the final repin (tree identical to merged #275), with Rust 1.91.1
and `RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`.
2,639 Python tests passed. Its wrapper subsequently reported untracked generated
plugin-cache files; tracked source remained clean and the files were preserved.

Evidence: `/private/tmp/vepyr-clinvar-opt-run/followup-optimizations/merge-and-input-followup/`
and the matching directory in the durable 20261105 benchmark archive.
The pre-edit `input-baseline/complete.json` contains 1/8-worker timings, traces,
RSS, CPU seconds, average cores, and exact original body MD5/order checks.
HG002 chr22 (50,861 records) uses a documented derived cache view with original
VCF/TBI, retaining full index cost. Baseline uninstrumented wall: 4.62/2.17 s;
vep-rs 0.5.0: 34.58/20.28 s. vep-rs's chromosome selection has additional whole
input processing; use sliced chromosome controls before generalizing speedups.

All measurements hold the existing measurement.lock. BGZF has a separately
declared 13 GiB reserve; the old plain/whole sweep guards remain unchanged.
Run one warm-up then three rotated baseline/candidate/vep-rs observations at
each worker count, with separate phase traces. Preserve every observation.
Use unchanged compare_runs.py thresholds (phase +5%, wall/RSS +10%, 50 ms floor),
and dense/sparse chromosome controls. A failure requires investigation, not a
weakened threshold. No local whole ClinVar run without asking the user first.

Expected movement: remove the prefix scan at both worker counts, reduce input
streams from up to 64 to 8 per chromosome at eight grid workers. The prior
whole-HG002 estimate was 3–8% wall reduction, unmeasured; no gain is promised.
RSS and CPU should not regress. Strict raw MD5 and input order must hold.

## Publication

One draft functions PR and one draft vepyr pin PR, no formats PR. Native build
both bookends identically; verify current pins, CI, Codex/Claude review, and
local gates. Request authorized GHA WGS/ClinVar parity on the final head.
Hand off for review; the earlier merge authorization applied to #326/#275.
