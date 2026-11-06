# ClinVar optimization snapshot — 2026-11-06

This snapshot pins [engine PR #326](https://github.com/biodatageeks/datafusion-bio-functions/pull/326)
to `5a62b9cf30b4ab5f13a8cebd98a6fef2a50d91cf`. It adds bounded reference and
prediction reuse, indexed protein translation, whole-buffer worker balancing,
and earlier release of serial and parallel annotation output. Formats stays
at `e0379fb28a0d4d74f8da4c9cbd357182361349f9`; there is no cache-layout or
Python API change. The published dependency is a git revision, with no local
Cargo patch. After upstream merges, re-pin to the merge commit before release.

## Measurements

Normalized ClinVar input, VEP 116 GRCh38 merged cache, primary-assembly FASTA,
full annotation, 5,000-input-unit biological buffers, preserved record layout,
and plain VCF. Timings include process startup and annotation; output validation
is outside timing. Medians use three fresh processes after discarded warm-ups.
The chr17/eight-worker row is an earlier paired session; other rows are a later
comparison on the same shared macOS arm64 host.

| Chromosome | Workers | vepyr | Official vep-rs v0.5.0 | vepyr peak RSS | vep-rs peak RSS |
|---|---:|---:|---:|---:|---:|
| 17 | 1 | 144.69 s | 210.73 s | 3.154 GiB | 3.782 GiB |
| 17 | 8 | 29.16 s | 33.06 s | 4.159 GiB | 4.198 GiB |
| 2 | 1 | 383.39 s | 313.19 s | 3.448 GiB | 3.948 GiB |
| 2 | 8 | 138.74 s | 49.84 s | 4.883 GiB | 4.072 GiB |

The final output-memory step reduced chr17/eight-worker RSS 36.6% relative to
the preceding optimized Stage G build (6.561 to 4.159 GiB), while preserving
throughput (29.34 to 29.16 s). That is the incremental memory comparison, not
the baseline for the complete optimization series.

[Machine-readable measurements](../performance-tests/clinvar-2026-11-06.json)
retain every new measured run and warm-up, input/body identities, binary hashes,
and the reused chr17/eight-worker summary. All observations are retained,
including background host activity. A diagnostic pause occurred only after a
timed child exited; a disk guard stopped before chr2/eight-worker measurements,
which resumed after inactive cache cleanup. No measured process was suspended
and no thresholds were relaxed.

## Validation and provenance

The native build used Rust 1.91.1 and
`RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`.
The measured implementation is engine `8fa75a3f343697c59cef99d7b05ccabaef5047c8`;
the published head adds documentation only. The extension SHA256 is
`01b61cd0f5f661a9eed35b8e7d2eba0cabe777241ec735d5e5d0a826e03c3273`.
Cache revision: `ac2c8ad979ad063816368b0720e6e4501d64a3f3`.

- Engine tests: 1,631 passed, 3 ignored. Python tests: 2,635 passed,
  10 skipped, 1 expected failure. Implementation pre-commit checks passed,
  including Rust formatting and Clippy.
- The unchanged phase/time/RSS gate passed for the final memory change on dense
  ClinVar chr17 and sparse HG002 chr17 at one/eight workers against fresh G controls.
- Every candidate retained exact archived body digests and original input row
  identity/order: chr17 277,114 rows, MD5 `d2f003d60f8c85a91dbc2247d21d855a`;
  chr2 401,230 rows, MD5 `4df6ae8cdc714236b95943abd465752d`;
  sparse HG002 chr17 108,376 rows, MD5 `927aa175d903867883ca3cf5a5a80510`.
- vep-rs reproduces its own body digest across repetitions and worker counts.
  Its regulatory and edited RefSeq coverage differs, so it is a timing reference,
  not the biological parity oracle.

These checks cover the unchanged Rust implementation behind the new git pin.
Publication also verifies locked dependency resolution and source equivalence;
CI checks on the PR remain distinct from the recorded local test results.
Whole-ClinVar validation after these changes has not run and needs explicit
approval. No whole-input performance or parity claim is made.

## Known limitations and follow-up

The current serial reference-cache default is zero. That disables reference
reuse and the local-codon optimization, which penalizes long transcripts such
as TTN. A controlled 1,000-record TTN test gave three-run medians of 6.83 s
with the default, 4.93 s with `VEP_REFERENCE_CACHE_MB=128`, and 4.56 s for
vep-rs. Separate profiling attributes the improvement to coding work, with
identical prediction-page activity.

One exploratory complete-chromosome run each with that override produced
130.72 s / 3.006 GiB on chr17 and 290.10 s / 3.216 GiB on chr2, with exact
original body/order. Those are single diagnostic observations, not acceptance
medians or a changed default in this snapshot.

At eight workers, chr2 has a long tail with roughly one CPU busy. VCF workers
own fixed slices, and the weighting estimates transcript count without coding
length. The existing dynamic stream-run controls do not change that VCF path.
Length-aware balancing and any serial cache-policy change need their own
controlled iteration, with output parity and no speed-for-memory tradeoff.

Memory budgets cover selected payloads rather than complete process RSS.
A single very large annotation row remains unbounded in bytes.
