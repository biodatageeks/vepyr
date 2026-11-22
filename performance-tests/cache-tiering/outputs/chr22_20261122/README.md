# Warm/cold tiering of the variation cache: HG002 chromosome 22 (2026-11-22)

Does the warm/cold layout of the variation cache (rows of positions with a maximum allele
frequency of at least 1% first, the rest after, each run sorted by `start`) speed up vepyr
compared with the same rows sorted by `start` only? Design and plan:
`docs/superpowers/specs/2026-11-22-cache-tiering-gain-design.md`,
`docs/superpowers/plans/2026-11-22-cache-tiering-gain.md`.

## Setup

- vepyr 0.11.0 PyPI wheel (Python 3.12.2, Polars 1.44.2), macOS 15.7.9, Apple M3 Max, 64 GiB.
- Input: chromosome 22 of `HG002_normalized.vcf.gz`, 50,861 records. Merged release-116 cache,
  `--everything` settings of the whole-genome runs (`run_vepyr_worker_scaling.py`, plain VCF).
- Two caches that differ only in `variation/chr22.parquet` (everything else is a symlink to the
  original cache), both written by `relayout` with the engine's `VariationParquetShardWriter`
  (datafusion-bio-function-vep v0.27.1, parquet-rs 59.3.2):
  - `tiered`: the current row order (2 sorted runs);
  - `sorted`: the same rows sorted by `start`, `tier` = 1 (1 run).
- `paramcheck.json`: identical Parquet settings (row groups and rows per group, codec, encodings,
  no dictionary, page statistics, column and offset indexes, sorting columns, writer, key-value
  metadata); both files have 14,802 position-column pages. The original cache file differs from
  both only in the writer version (parquet-rs 58.0.0) and has 15,381 position-column pages.
- Output: in every run (warm and cold, both layouts) the record lines have MD5
  `d5f1a8c3749b5f20656d845c8310380f`, the chromosome 22 checksum of the macOS whole-genome vepyr
  0.11.0 output that matches Ensembl VEP 116.0 record for record.

## Timing

Warm: 10 measured runs per layout and worker count after one discarded run each, ABBA order,
free memory restored before each run (`warm/`). Cold: 3 runs per layout and worker count,
`sudo purge` before each, 0.0% of the variation shard resident in the page cache before every run
(mincore, `cold/runs.csv` column `resident_pct`). Medians (min-max); ratio = sorted / tiered.

| Cache state | Workers | Metric | tiered | sorted | Ratio |
|---|---|---|---|---|---|
| warm | 1 | wall (s) | 3.48 (3.41-3.53) | 4.54 (4.47-4.61) | 1.30 |
| warm | 1 | annotation only (s) | 3.10 | 4.14 | 1.34 |
| warm | 1 | CPU user+sys (s) | 5.08 (4.97-5.14) | 7.35 (7.26-7.46) | 1.44 |
| warm | 1 | peak RSS (GiB) | 1.23 | 1.14 | 0.93 |
| warm | 8 | wall (s) | 1.52 (1.50-1.55) | 1.86 (1.82-1.98) | 1.22 |
| warm | 8 | CPU user+sys (s) | 5.70 (5.66-5.74) | 7.72 (7.67-7.77) | 1.35 |
| warm | 8 | peak RSS (GiB) | 1.58 | 1.58 | 1.00 |
| cold | 1 | wall (s) | 4.41 (4.36-4.49) | 5.19 (5.07-5.24) | 1.18 |
| cold | 1 | CPU user+sys (s) | 5.13 | 7.38 | 1.44 |
| cold | 8 | wall (s) | 2.29 (2.22-2.30) | 2.62 (2.53-2.62) | 1.14 |
| cold | 8 | CPU user+sys (s) | 6.02 | 8.05 | 1.34 |

System CPU time is the same for both layouts (ratio 0.96-1.02); the difference is user CPU
(ratio 1.40-1.50).

## Pages and I/O per 5,000-record buffer

From `pages/`: the copied lookup of `performance-tests/paper-0.11.0/cache-pages-20261121`
(datafusion-bio-functions 46d8c20 logic) run on the two rewritten shards with vepyr's extended
probes; reduction = 1 - tiered/sorted (negative: more with tiering). Buffer 0 and the range over
the 10 full buffers (`pages/pages.csv`, `pages/pages.json`).

| Lookup step | Metric | tiered | sorted | Buffer 0 | Range |
|---|---|---|---|---|---|
| find rows (position column) | pages decoded | 949 | 907 | -4.6% | -4.6% to -1.5% |
| | pages read from the file | 1,177 | 1,142 | -3.1% | -3.1% to -0.1% |
| | bytes read | 2.80 MB | 2.71 MB | -3.6% | -3.6% to -0.7% |
| | read operations | 9 | 6 | -50% | -150% to -20% |
| matched rows | position pages holding a match | 300 | 907 | 66.9% | 66.0% to 73.8% |
| read the matched rows (all columns) | pages decoded | 9,022 | 32,451 | 72.2% | 71.6% to 77.2% |
| | pages read from the file | 104,059 | 88,210 | -18.0% | -18.3% to 9.7% |
| | bytes read | 25.7 MB | 29.6 MB | 13.1% | 13.1% to 25.8% |
| | read operations | 121 | 68 | -78% | -388% to -78% |

## Verdict on chromosome 22

The warm/cold layout makes vepyr faster: wall time is 18-30% shorter with one worker and 14-22%
with eight, warm or cold, and CPU time is 34-44% lower in every case. The saving is user CPU:
the matched records lie on about two thirds fewer pages, so about three quarters fewer pages are
decoded to read their annotation. The position-column lookup itself is not reduced (slightly more
pages and more, smaller reads), and from a cold file cache this extra I/O narrows the wall-time
gain. Peak memory is unchanged with eight workers and 7% higher with one.

Limits: one chromosome and one sample; the page counts come from a copy of the lookup, not from
inside vepyr (0.11.0 has no page counter); the cold runs purge the whole OS file cache (FASTA,
transcripts and input as well), equally for both layouts.

## Files

`warm/` and `cold/`: `runs.csv` (every run), `summary.json`, `summary.md`, `env.json`
(`cold/run_cold_chr22.sh` is the command run for the cold set); `pages/`: tool output and
summary; `paramcheck.json`; `layout-tiered.json`, `layout-sorted.json`.
