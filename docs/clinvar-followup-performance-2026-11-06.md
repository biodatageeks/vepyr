# ClinVar HGVS reuse and worker balancing — 2026-11-06

This follow-up removes repeated ownership and HGVS preparation work, then balances
whole VEP buffers using coding-length-aware costs. It follows the serial-reference
baseline at engine `562af65835f443305784b6567004872966922df0`.

## Implementation

- Move or borrow consequence data where ownership permits, including translation
  metadata. Clone translation metadata only when a missing identifier requires
  the existing transcript fallback.
- Share canonical protein text through the existing reference memo. For eligible
  local substitutions on proteins of at least 256 residues, represent alternate
  protein text as a lazy overlay. Complex cases retain the owned fallback. The
  public owned HGVS API and formatting semantics remain unchanged.
- Charge shared text before allocation against the existing 128 MiB reference
  admission budget; retained references retain their budget leases. This budget
  covers selected payloads, not total process RSS. The small replacement vector
  and any full alternate string materialized by a consumer are ordinary output
  allocations outside that budget. Failed canonical-text admission is remembered
  for that memo entry; subsequent requests use the owned fallback.
- Weight transcript overlaps using available coding length, then choose contiguous
  whole-buffer partitions that minimize the largest estimated worker cost. Keep
  the original cuts when already optimal. Worker count, biological buffer
  boundaries, equal-position ownership, warm-up, and output ordering are preserved.

## Provisional chromosome screens

These are single paired process measurements after discarded warm-ups, with plain
VCF output and startup included. The previous arm already contains the ownership
and HGVS changes; the candidate adds the final scheduling changes. These are
screens, not the final repeated comparison or whole-input acceptance.

| Input | Previous time | Candidate time | Previous RSS | Candidate RSS | Earlier vep-rs v0.5.0 context |
|---|---:|---:|---:|---:|---|
| Chr2, 8 workers | 73.87 s | 48.85 s | 5.152 GiB | 4.405 GiB | 50.10 s / 4.017 GiB, single observation |
| Chr17, 8 workers | 23.42 s | 22.88 s | 4.261 GiB | 4.082 GiB | 33.06 s / 4.198 GiB, earlier three-run median |

Chr2 improves 33.9% in time and 14.5% in peak RSS against its paired intermediate
baseline. The official vep-rs figures above come from earlier sessions; they do
not establish a fresh paired win. Regulatory and edited RefSeq coverage also
differs, so vep-rs is a timing reference, not a biological correctness oracle.

The preceding serial metadata change reduced one paired chr2/one-worker process
measurement from 198.88 s to 169.07 s, with peak RSS increasing 34.6 MiB (+0.94%).
The earlier official vep-rs three-run median was 317.38 s.

## Final repeated chromosome comparison

The fresh three-arm matrix is complete. Each configuration has one discarded
warm-up followed by three measurements in rotating engine order. The full
[report and observations](clinvar-final-chromosome-performance-2026-11-07.md)
retain every observation, including warm-ups.

| Input | Previous vepyr | Optimized vepyr | Official vep-rs v0.5.0 | Previous / optimized / vep-rs peak GiB |
|---|---:|---:|---:|---|
| Chr2, 1 worker | 268.22 s | 169.56 s | 312.74 s | 3.742 / 3.648 / 3.946 |
| Chr2, 8 workers | 130.96 s | 48.77 s | 47.57 s | 4.943 / 4.407 / 3.998 |
| Chr17, 1 worker | 115.73 s | 95.69 s | 206.14 s | 3.136 / 3.193 / 3.769 |
| Chr17, 8 workers | 27.36 s | 23.26 s | 31.33 s | 4.280 / 3.939 / 4.109 |

These are median process wall times and peak RSS, with plain VCF output and
startup included. Time falls 15.0–62.8% against the previous vepyr baseline.
Chr2 at eight workers remains 2.5% slower than vep-rs and uses 10.2% more peak
memory. Chr17 at one worker uses 1.8% more memory than previous vepyr; every
configuration passes the unchanged +10% wall/RSS guards. Every output retains
the original pre-edit raw body MD5 and record identity/order.

## Verification and rejected experiment

- Rust: 1,647 passed, 3 ignored. Python: 2,635 passed, 10 skipped, 1 expected
  failure. Pre-commit formatting and Clippy passed on the measured source.
- The partition regression failed before the minimax change. Exhaustive small
  input tests compare it with an independent partition oracle; overflow, empty
  weights, existing optimal cuts, equal-position boundaries and warm-up are covered.
- Every completed annotation retains the original raw vepyr body MD5 and input
  record identity/order: chr2 `4df6ae8cdc714236b95943abd465752d` (401,230 records),
  chr17 `d2f003d60f8c85a91dbc2247d21d855a` (277,114), sparse HG002 chr17
  `927aa175d903867883ca3cf5a5a80510` (108,376).
- Dense ClinVar chr17 and sparse HG002 chr17 passed the unchanged phase +5%,
  wall/RSS +10% gates at one/eight workers on the single permitted reversed
  confirmation. The initial dense attempt failed one setup phase:
  `8/context/done`, 137.205 to 145.947 ms (+6.4%). All initial wall/RSS checks
  and the initial sparse comparison passed. Both complete attempts are retained;
  no thresholds changed and no third attempt was run.
- The first coding-length-weighted scheduler was rejected: chr17 increased from
  23.22 to 27.73 s (+19.4%). Quantile rounding produced uneven contiguous cuts.
  The final minimax partitioner fixes those cuts without retuning the cost formula.
- Diagnostic profiles still show a 16.51 s final-worker tail on chr2. This is an
  outstanding scaling opportunity, not proof of optimal elapsed-time scheduling.

## Parity policy and remaining validation

Whole-ClinVar golden comparison uses the existing `clinvar_merged` profile and
`strict-clin-sig` mode. It normalizes only the order of `%3B` groups within
`CLIN_SIG`; VCF row order and CSQ entry order remain significant. In addition,
every candidate must match the original pre-edit vepyr raw body MD5 and input
identity/order. WGS retains its existing policy. No comparator change is included.

Whole ClinVar and isolated per-chromosome BGZF comparisons against official
vep-rs v0.5.0 remain pending. No whole-input performance or ClinVar golden parity
result is claimed here.

GHA run [42304855077](https://github.com/biodatageeks/vepyr/actions/runs/42304855077)
passed all three WGS profiles (ensembl, merged and refseq), each with 22 contigs
and 4,096,123 records. ClinVar stopped before annotation: `--no-normalize`
attempted to refresh a tabix index under the read-only `/data` mount. The harness
now copies compressed input into writable scratch space when its source
directory or index is read-only, then indexes that byte-identical copy. Source
identity still invalidates stale copies and indexes. Writable inputs keep the
existing path; comparison semantics and annotation code are unchanged.

Four new regression cases failed against the old helper. With the fix, 270
focused harness tests and pre-commit passed. The full 2,635-test Python result
above predates this harness-only fix; CI and both reviewers are requested again
on the updated companion head, including the whole ClinVar/WGS parity workflow.

Native measurement build: Rust 1.91.1,
`RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`.
Candidate extension SHA256:
`46f4074b2bd708e107a089c5c1ef56851480bf7c20c219de149271f5c46566ec`.
Formats remains `e0379fb28a0d4d74f8da4c9cbd357182361349f9`; cache revision remains
`ac2c8ad979ad063816368b0720e6e4501d64a3f3` (VEP 116 GRCh38 merged).

Published engine revision: `8e279811b10b8ce95de465b4bd69b79c0e4af75f`
([functions PR #326](https://github.com/biodatageeks/datafusion-bio-functions/pull/326)).
The four measured Rust files match this commit byte for byte. Locked Cargo
metadata resolves this revision, and all 146 tracked Rust/manifest files across
the upstream repository match its checkout. That includes 76 files in the VEP
crate subtree, the scope used by the preceding serial report; the different
counts describe different verification scopes. Historical reports retain their
own snapshot counts. The only post-build engine change is performance
documentation; the companion lockfile changes only the engine source revision.

[Screen observations, source hashes, and complete standard gate logs](../performance-tests/clinvar-followup-2026-11-06.json).
