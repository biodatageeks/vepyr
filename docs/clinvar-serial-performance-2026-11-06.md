# One-worker ClinVar reference reuse — 2026-11-06

Serial annotation now enables the existing bounded reference memo and local-codon translation. The default is a lazy 128 MiB admission budget shared across the execution, including at one worker. Explicit zero/custom overrides and biological fallbacks are unchanged. The previous serial default was zero.

## Measurements

Three fresh-process measurements per engine after a discarded warm-up, with rotating engine order. Same normalized input, full annotation, reference and plain VCF output on the shared macOS arm64 host. Process startup is included; digest and ordering validation is outside timing. Candidate runs leave `VEP_REFERENCE_CACHE_MB` unset. Every observation is retained.

| Input | Previous vepyr | Optimized vepyr | vep-rs v0.5.0 | Previous RAM | Optimized RAM | vep-rs RAM |
|---|---:|---:|---:|---:|---:|---:|
| Chr2, 1 worker | 368.86 s | 279.45 s | 317.38 s | 3.469 GiB | 3.623 GiB | 3.958 GiB |
| Chr17, 1 worker | 140.33 s | 117.61 s | 207.23 s | 3.071 GiB | 3.147 GiB | 3.791 GiB |

- Chr2: 24.2% faster than the previous default and 12.0% faster than vep-rs. Peak RAM changes +4.4% (+157.9 MiB) relative to our baseline and -8.5% relative to vep-rs.
- Chr17: 16.2% faster than the previous default and 43.2% faster than vep-rs. Peak RAM changes +2.5% (+77.1 MiB) relative to our baseline and -17.0% relative to vep-rs.

## Verification

- Rust: 1,632 passed, 3 ignored. Local Python 3.12.8: 2,635 passed, 10 skipped, 1 expected failure. The new long-CDS production-default regression failed before the policy correction and passed after it. Implementation formatting and Clippy checks passed.
- Exact archived output-body digests and input record ordering passed for every run: ClinVar chr2 has 401,230 records, MD5 `4df6ae8cdc714236b95943abd465752d`; chr17 has 277,114, MD5 `d2f003d60f8c85a91dbc2247d21d855a`.
- Dense ClinVar chr17 and sparse HG002 chr17 were also compared at one and eight workers with the unchanged phase (+5%), wall/RSS (+10%) gates. Sparse output retains 108,376 records and MD5 `927aa175d903867883ca3cf5a5a80510`.
- Standard gate verdicts (0 means pass): `{'dense-1': 1, 'sparse-1': 0, 'dense-2': 0, 'sparse-2': 0}`. The final paired attempt was 2. Any initial failures remain in the evidence; no thresholds were changed.
- vep-rs reproduces its own output digests. Its regulatory and edited RefSeq coverage differs, so the timing comparison is not a cross-engine parity claim.

## Provenance and remaining scope

A disk guard stopped orchestration between completed measurements before the last chr17 round. After inactive cache cleanup, only the three pending runs resumed in their original order. The first 21 observation files were preserved by SHA256; no timed process was interrupted or result discarded. The 32 GiB disk guard was unchanged.

Native measurement build: Rust 1.91.1, `RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`. Candidate extension SHA256: `c2ef6a305793e84228e7065a46310b027d73b78a1d84449509f6c7664ce0a6f1`. Measured source parent: `5a62b9cf30b4ab5f13a8cebd98a6fef2a50d91cf`; the exact patch and file hashes are archived. Later edits change comments and documentation only.

Formats stays at `e0379fb28a0d4d74f8da4c9cbd357182361349f9`; cache revision is `ac2c8ad979ad063816368b0720e6e4501d64a3f3` (VEP 116 GRCh38 merged). Cache layout and the Python API are unchanged.

This iteration changes the serial reference-cache policy. Chr2 parallel scheduling remains a separate issue: the earlier eight-worker snapshot was 138.74 s versus vep-rs 49.84 s. Whole-protein copying during HGVS preparation is another unimplemented serial opportunity; no gain is claimed for it.

No whole-ClinVar validation was run. Memory admission limits cover selected payloads, not total RSS.

## Standard regression controls

These traced controls report annotation time, separately from the process timings and official vep-rs context above. Each arm discards an eight-worker warm-up, then measures eight and one workers. The table uses the final complete paired attempt; all earlier attempts remain in the JSON.

| Input | Workers | Baseline annotation | Candidate annotation | Baseline peak RSS | Candidate peak RSS |
|---|---:|---:|---:|---:|---:|
| dense chr17 | 1 | 145.59 s | 124.03 s | 3.103 GiB | 3.131 GiB |
| dense chr17 | 8 | 28.67 s | 29.64 s | 4.323 GiB | 4.295 GiB |
| sparse chr17 | 1 | 7.80 s | 7.78 s | 2.021 GiB | 1.971 GiB |
| sparse chr17 | 8 | 2.51 s | 2.48 s | 2.832 GiB | 2.815 GiB |

Initial gate `dense-1` failed and is retained:

- `8/context/done/elapsed_ms                       144.2        199.7   +38.5%  <-- REGRESSION`
- `8/context/transcripts_done/elapsed_ms           364.2        388.5    +6.7%  <-- REGRESSION`
- `8/prefetch/data_done/elapsed_ms                 551.2        651.0   +18.1%  <-- REGRESSION`

Published engine revision: `562af65835f443305784b6567004872966922df0` ([PR #326](https://github.com/biodatageeks/datafusion-bio-functions/pull/326)).

[All observations and standard-gate phase totals](../performance-tests/clinvar-serial-2026-11-06.json).

Locked Cargo metadata resolves the published engine revision exactly. All 76 tracked engine Rust/manifest files match the published tree; measured functional files are unchanged, with only the recorded comment/documentation edits after the build. The lockfile changes only the engine source revision.
