# Warm/cold tiering of the variation cache: whole HG002 genome (2026-11-22)

Per-chromosome comparison of three variation caches on HG002 chromosomes 1-22 (4,096,123
records): `tiered` (the warm/cold layout rewritten by `relayout`), `sorted` (the same rows sorted
by `start` only, written the same way) and `original` (the cache vepyr ships, unchanged).
The chromosome 22 set and the method are in `../chr22_20261122/README.md`; design and plan in
`docs/superpowers/specs|plans/2026-11-22-cache-tiering-gain*`.

## Setup

- vepyr 0.11.0 PyPI wheel, macOS 15.7.9, Apple M3 Max, 64 GiB; merged release-116 cache;
  `--everything` settings of the whole-genome runs; plain VCF output (checksummed, then deleted).
- One invocation per chromosome with its slice of `HG002_normalized.vcf.gz`
  (`bcftools view -r chrN`; counts in `input_records.json`). Totals are sums of the
  per-chromosome medians, not a single whole-genome invocation.
- Per chromosome, layout and worker count (1 and 8): one discarded warm-up, 3 warm runs in a
  rotated order (each layout once in each position), then 2 cold runs, each after `sudo purge`
  with 0% of the variation shard in the page cache before the run (mincore). 792 runs, 660
  measured, all valid: exit 0, and per-contig MD5 of the output identical across the three
  layouts (and to the vepyr 0.11.0 whole-genome output that matches VEP 116.0).
- `tiered` and `sorted` have identical Parquet settings for all 22 chromosomes
  (`paramcheck.json`) and the same number of position-column pages. `relayout` streams; its
  output has the same rows and row groups as an in-memory rewrite.
- 17 of 396 warm measured runs started with less than half of the shard cached (a memory
  release before the run evicted it): 6 tiered, 7 sorted, 4 original. Excluding them changes no
  total ratio by more than 0.01.
- The run was paused after chromosome 17 and resumed; chromosome 18 was re-run from scratch.

## Totals (chromosomes 1-22)

| | warm, 1 worker | warm, 8 workers | cold, 1 worker | cold, 8 workers |
|---|---|---|---|---|
| CPU: sorted / tiered | 1.58 | 1.59 | 1.57 | 1.59 |
| CPU: reduction with tiering | 36.7% | 37.0% | 36.4% | 37.3% |
| CPU: original / tiered | 1.004 | 1.004 | 1.004 | 1.005 |
| wall: sorted / tiered | 1.84 | 1.42 | 1.65 | 1.25 |
| wall: reduction with tiering | 45.5% | 29.7% | 39.4% | 20.3% |
| wall: original / tiered | 1.000 | 0.998 | 1.010 | 1.001 |

## CPU time per chromosome (user + system, median, s)

Cells: tiered / sorted / original (sorted / tiered, original / tiered).

| Chromosome | warm, 1 worker | warm, 8 workers | cold, 1 worker | cold, 8 workers |
|---|---|---|---|---|
| chr1 | 31.6 / 49.4 / 31.7 (1.56, 1.00) | 33.3 / 53.6 / 33.4 (1.61, 1.00) | 32.6 / 50.0 / 32.3 (1.53, 0.99) | 33.6 / 55.1 / 33.7 (1.64, 1.00) |
| chr2 | 31.0 / 50.3 / 31.1 (1.62, 1.00) | 32.9 / 55.4 / 33.1 (1.68, 1.00) | 32.0 / 51.6 / 32.3 (1.61, 1.01) | 32.9 / 55.9 / 33.0 (1.70, 1.00) |
| chr3 | 28.5 / 47.2 / 28.7 (1.66, 1.01) | 30.6 / 50.7 / 30.6 (1.66, 1.00) | 28.8 / 47.7 / 28.9 (1.66, 1.00) | 30.7 / 51.7 / 30.6 (1.69, 1.00) |
| chr4 | 25.3 / 43.9 / 25.7 (1.73, 1.02) | 26.4 / 47.4 / 26.5 (1.79, 1.00) | 25.7 / 44.2 / 25.6 (1.72, 1.00) | 26.5 / 47.9 / 26.8 (1.81, 1.01) |
| chr5 | 21.1 / 39.1 / 21.1 (1.86, 1.00) | 22.9 / 41.5 / 23.1 (1.81, 1.01) | 21.3 / 39.0 / 21.5 (1.84, 1.01) | 23.0 / 42.2 / 23.1 (1.84, 1.01) |
| chr6 | 21.2 / 31.2 / 21.3 (1.48, 1.01) | 21.7 / 34.1 / 22.1 (1.57, 1.02) | 21.6 / 31.7 / 21.6 (1.47, 1.00) | 22.3 / 34.8 / 22.2 (1.56, 1.00) |
| chr7 | 19.5 / 29.7 / 19.6 (1.52, 1.00) | 21.5 / 32.9 / 21.5 (1.53, 1.00) | 19.7 / 30.0 / 19.7 (1.53, 1.00) | 21.4 / 33.8 / 21.9 (1.58, 1.02) |
| chr8 | 17.6 / 27.2 / 17.6 (1.55, 1.00) | 19.1 / 30.1 / 19.3 (1.57, 1.01) | 17.8 / 27.7 / 17.9 (1.55, 1.01) | 19.4 / 30.6 / 19.6 (1.57, 1.01) |
| chr9 | 15.1 / 23.0 / 15.0 (1.52, 0.99) | 16.5 / 25.7 / 16.8 (1.55, 1.01) | 15.2 / 23.4 / 15.3 (1.53, 1.01) | 16.8 / 26.1 / 16.9 (1.55, 1.01) |
| chr10 | 18.2 / 27.0 / 18.1 (1.48, 1.00) | 19.9 / 30.1 / 19.9 (1.51, 1.00) | 18.5 / 27.4 / 18.4 (1.49, 1.00) | 20.3 / 30.4 / 20.6 (1.50, 1.02) |
| chr11 | 17.5 / 26.4 / 17.4 (1.51, 1.00) | 19.2 / 29.8 / 19.3 (1.55, 1.01) | 17.8 / 26.8 / 17.8 (1.51, 1.00) | 19.4 / 30.1 / 19.5 (1.55, 1.01) |
| chr12 | 17.0 / 25.5 / 16.9 (1.50, 1.00) | 18.8 / 28.3 / 18.7 (1.50, 0.99) | 17.3 / 25.9 / 17.6 (1.49, 1.01) | 18.9 / 28.6 / 19.1 (1.51, 1.01) |
| chr13 | 10.6 / 17.4 / 10.8 (1.65, 1.02) | 11.5 / 18.9 / 11.5 (1.64, 1.00) | 10.7 / 17.6 / 10.8 (1.65, 1.01) | 12.1 / 19.4 / 11.9 (1.61, 0.98) |
| chr14 | 11.2 / 17.8 / 11.3 (1.59, 1.02) | 12.3 / 18.5 / 12.3 (1.51, 1.01) | 11.5 / 18.2 / 11.7 (1.58, 1.02) | 12.9 / 18.9 / 12.9 (1.46, 1.00) |
| chr15 | 11.1 / 17.0 / 11.1 (1.53, 1.00) | 12.0 / 17.8 / 12.1 (1.48, 1.00) | 11.2 / 17.1 / 11.3 (1.53, 1.01) | 12.3 / 18.1 / 12.3 (1.48, 1.01) |
| chr16 | 10.7 / 16.1 / 10.8 (1.50, 1.01) | 12.1 / 18.1 / 12.1 (1.49, 1.00) | 11.0 / 16.4 / 11.0 (1.49, 1.00) | 12.4 / 18.5 / 12.4 (1.49, 1.00) |
| chr17 | 11.0 / 15.9 / 11.1 (1.44, 1.00) | 12.4 / 17.3 / 12.4 (1.40, 1.00) | 11.3 / 16.2 / 11.4 (1.43, 1.01) | 12.7 / 17.9 / 12.7 (1.41, 1.00) |
| chr18 | 9.0 / 14.2 / 9.0 (1.58, 1.00) | 10.0 / 15.5 / 10.0 (1.55, 1.00) | 9.3 / 14.9 / 9.3 (1.60, 1.00) | 10.4 / 15.9 / 10.4 (1.53, 1.00) |
| chr19 | 9.3 / 13.1 / 9.4 (1.41, 1.01) | 10.8 / 14.5 / 10.8 (1.34, 1.00) | 9.5 / 13.3 / 9.6 (1.40, 1.01) | 11.1 / 14.9 / 11.1 (1.34, 1.00) |
| chr20 | 6.7 / 11.0 / 6.8 (1.64, 1.01) | 7.5 / 11.4 / 7.5 (1.52, 1.01) | 6.9 / 11.1 / 7.0 (1.61, 1.01) | 7.8 / 11.8 / 7.8 (1.51, 1.00) |
| chr21 | 4.1 / 6.5 / 4.2 (1.57, 1.01) | 4.8 / 7.4 / 4.8 (1.54, 1.01) | 4.3 / 6.6 / 4.3 (1.55, 1.01) | 5.0 / 7.7 / 5.1 (1.52, 1.00) |
| chr22 | 5.0 / 7.3 / 5.1 (1.44, 1.00) | 5.8 / 7.7 / 5.8 (1.34, 1.01) | 5.2 / 7.5 / 5.2 (1.44, 1.00) | 6.1 / 8.1 / 6.1 (1.32, 1.00) |
| **total** | 352.2 / 556.1 / 353.7 (1.58, 1.00) | 382.0 / 606.7 / 383.5 (1.59, 1.00) | 359.1 / 564.5 / 360.4 (1.57, 1.00) | 387.9 / 618.2 / 389.8 (1.59, 1.00) |

## Wall time per chromosome (median, s)

| Chromosome | warm, 1 worker | warm, 8 workers | cold, 1 worker | cold, 8 workers |
|---|---|---|---|---|
| chr1 | 17.9 / 33.0 / 18.0 (1.84, 1.01) | 6.8 / 9.6 / 6.8 (1.41, 1.00) | 21.8 / 34.5 / 23.8 (1.58, 1.09) | 8.5 / 10.2 / 8.8 (1.20, 1.03) |
| chr2 | 17.2 / 34.8 / 17.2 (2.03, 1.00) | 6.5 / 9.5 / 6.6 (1.47, 1.02) | 20.0 / 36.8 / 20.1 (1.84, 1.00) | 7.6 / 10.0 / 7.7 (1.31, 1.01) |
| chr3 | 16.4 / 32.1 / 16.4 (1.96, 1.00) | 5.8 / 9.1 / 6.0 (1.57, 1.03) | 18.4 / 33.5 / 18.4 (1.82, 1.00) | 6.9 / 9.8 / 7.0 (1.42, 1.01) |
| chr4 | 13.2 / 32.1 / 13.1 (2.43, 1.00) | 5.0 / 7.9 / 4.7 (1.56, 0.92) | 14.8 / 33.7 / 14.6 (2.28, 0.99) | 5.8 / 8.6 / 6.1 (1.47, 1.04) |
| chr5 | 11.1 / 29.3 / 11.0 (2.64, 0.99) | 3.9 / 7.0 / 3.9 (1.81, 1.02) | 14.2 / 30.4 / 12.8 (2.13, 0.90) | 5.1 / 7.7 / 5.1 (1.53, 1.01) |
| chr6 | 11.8 / 20.1 / 11.9 (1.71, 1.01) | 4.7 / 6.7 / 4.8 (1.42, 1.01) | 13.7 / 21.4 / 13.7 (1.56, 1.00) | 6.3 / 7.5 / 6.1 (1.19, 0.98) |
| chr7 | 11.2 / 19.3 / 11.2 (1.73, 1.00) | 4.0 / 5.2 / 3.9 (1.29, 0.97) | 12.8 / 20.4 / 12.8 (1.60, 1.00) | 5.1 / 6.0 / 5.2 (1.18, 1.02) |
| chr8 | 9.9 / 18.3 / 9.9 (1.85, 1.00) | 3.7 / 5.2 / 3.7 (1.42, 1.01) | 11.3 / 19.5 / 11.3 (1.72, 1.00) | 4.7 / 6.0 / 4.9 (1.28, 1.05) |
| chr9 | 9.0 / 14.8 / 8.9 (1.64, 0.99) | 3.4 / 4.6 / 3.5 (1.35, 1.02) | 10.4 / 16.0 / 10.4 (1.54, 1.00) | 4.7 / 5.4 / 4.7 (1.16, 1.01) |
| chr10 | 11.0 / 16.8 / 10.8 (1.53, 0.99) | 4.0 / 5.2 / 4.0 (1.31, 0.99) | 12.5 / 17.9 / 12.5 (1.43, 1.00) | 5.3 / 5.9 / 5.3 (1.11, 1.00) |
| chr11 | 10.3 / 16.7 / 10.3 (1.61, 0.99) | 4.0 / 5.7 / 4.1 (1.42, 1.02) | 12.3 / 17.7 / 12.2 (1.44, 0.99) | 5.3 / 6.6 / 5.3 (1.24, 1.00) |
| chr12 | 10.0 / 16.1 / 9.9 (1.61, 0.99) | 3.8 / 4.6 / 3.6 (1.22, 0.97) | 11.7 / 17.3 / 11.9 (1.47, 1.01) | 4.8 / 5.4 / 5.0 (1.12, 1.03) |
| chr13 | 5.7 / 12.4 / 5.8 (2.18, 1.02) | 2.5 / 4.6 / 2.5 (1.83, 0.99) | 6.7 / 13.3 / 6.8 (1.97, 1.01) | 3.6 / 5.9 / 3.3 (1.65, 0.91) |
| chr14 | 6.7 / 11.8 / 6.8 (1.76, 1.01) | 2.8 / 3.8 / 2.8 (1.40, 1.01) | 8.1 / 12.8 / 8.2 (1.57, 1.01) | 3.7 / 4.5 / 3.7 (1.21, 1.00) |
| chr15 | 6.9 / 10.9 / 6.8 (1.59, 1.00) | 2.7 / 3.4 / 2.7 (1.24, 1.00) | 8.1 / 11.6 / 8.3 (1.43, 1.01) | 3.7 / 4.1 / 3.7 (1.11, 1.01) |
| chr16 | 6.4 / 10.4 / 6.4 (1.62, 1.00) | 2.9 / 4.0 / 2.7 (1.40, 0.94) | 7.9 / 11.4 / 7.9 (1.43, 0.99) | 3.8 / 4.8 / 3.6 (1.28, 0.96) |
| chr17 | 7.1 / 10.0 / 7.1 (1.41, 1.00) | 3.0 / 3.6 / 2.9 (1.20, 0.96) | 8.9 / 11.0 / 10.4 (1.23, 1.17) | 3.9 / 4.6 / 3.9 (1.16, 1.01) |
| chr18 | 5.3 / 9.4 / 5.4 (1.77, 1.01) | 2.2 / 3.3 / 2.3 (1.49, 1.02) | 6.3 / 10.6 / 6.4 (1.68, 1.01) | 3.7 / 4.0 / 3.1 (1.09, 0.84) |
| chr19 | 6.1 / 7.9 / 6.2 (1.29, 1.00) | 2.8 / 3.1 / 2.8 (1.11, 0.99) | 8.1 / 9.0 / 8.1 (1.11, 1.00) | 3.9 / 4.4 / 3.9 (1.12, 1.00) |
| chr20 | 3.9 / 7.6 / 4.0 (1.94, 1.00) | 1.7 / 2.3 / 1.7 (1.36, 1.02) | 5.0 / 8.4 / 5.0 (1.68, 1.01) | 2.5 / 3.0 / 2.5 (1.19, 1.00) |
| chr21 | 2.7 / 4.7 / 2.7 (1.76, 1.02) | 1.3 / 2.0 / 1.4 (1.51, 1.02) | 3.4 / 5.5 / 3.5 (1.59, 1.01) | 2.0 / 2.6 / 2.0 (1.31, 0.99) |
| chr22 | 3.5 / 4.5 / 3.4 (1.29, 0.99) | 1.6 / 1.9 / 1.6 (1.23, 1.01) | 4.5 / 5.2 / 4.5 (1.17, 1.00) | 2.4 / 2.7 / 2.4 (1.11, 1.00) |
| **total** | 203.2 / 373.1 / 203.2 (1.84, 1.00) | 79.1 / 112.4 / 78.9 (1.42, 1.00) | 241.2 / 397.9 / 243.5 (1.65, 1.01) | 103.3 / 129.6 / 103.4 (1.25, 1.00) |

## Pages

From the copied lookup (`pages/`, vepyr's extended probes, 5,000-record buffers) on each
chromosome's three shards, summed over all buffers. `find` = position-column pages decoded to
locate the rows, `read` = pages decoded to read the matched rows (all columns).

| Chromosome | pages matched (tiered / sorted / original) | pages decoded, find + read (tiered / sorted / original) | decoded: tiered vs sorted | decoded: tiered vs original |
|---|---|---|---|---|
| chr1 | 22057 / 98413 / 24429 | 776219 / 3347126 / 785858 | 76.8% | 1.2% |
| chr2 | 22166 / 103619 / 24801 | 790799 / 3551363 / 801990 | 77.7% | 1.4% |
| chr3 | 19873 / 97823 / 22286 | 717117 / 3320092 / 727286 | 78.4% | 1.4% |
| chr4 | 19711 / 98114 / 22119 | 714661 / 3389297 / 722731 | 78.9% | 1.1% |
| chr5 | 18194 / 91343 / 20481 | 660854 / 3113897 / 670312 | 78.8% | 1.4% |
| chr6 | 14390 / 52473 / 15611 | 485215 / 1910728 / 486032 | 74.6% | 0.2% |
| chr7 | 13250 / 48454 / 14328 | 445738 / 1753241 / 446834 | 74.6% | 0.2% |
| chr8 | 12416 / 46213 / 13518 | 419395 / 1686293 / 421338 | 75.1% | 0.5% |
| chr9 | 10028 / 36896 / 10850 | 338207 / 1335421 / 338522 | 74.7% | 0.1% |
| chr10 | 11421 / 41648 / 12290 | 384346 / 1518521 / 383450 | 74.7% | -0.2% |
| chr11 | 11041 / 42291 / 12008 | 372746 / 1523348 / 376046 | 75.5% | 0.9% |
| chr12 | 11067 / 40985 / 12009 | 372829 / 1475202 / 374907 | 74.7% | 0.6% |
| chr13 | 8420 / 31527 / 9126 | 284913 / 1169998 / 284393 | 75.6% | -0.2% |
| chr14 | 7635 / 27437 / 8199 | 255372 / 995785 / 255186 | 74.4% | -0.1% |
| chr15 | 7146 / 25758 / 7739 | 239461 / 922415 / 240921 | 74.0% | 0.6% |
| chr16 | 7276 / 25985 / 7815 | 245325 / 931062 / 244331 | 73.7% | -0.4% |
| chr17 | 6830 / 23515 / 7370 | 229082 / 824126 / 229375 | 72.2% | 0.1% |
| chr18 | 6446 / 23983 / 6924 | 217627 / 879456 / 216419 | 75.3% | -0.6% |
| chr19 | 5486 / 18236 / 5882 | 184658 / 631947 / 183189 | 70.8% | -0.8% |
| chr20 | 5092 / 18275 / 5525 | 171139 / 655084 / 171961 | 73.9% | 0.5% |
| chr21 | 2983 / 10727 / 3227 | 101259 / 391841 / 100496 | 74.2% | -0.8% |
| chr22 | 3052 / 10553 / 3276 | 102809 / 371361 / 102157 | 72.3% | -0.6% |
| **total** | 245980 / 1014268 / 269813 | 8509771 / 35697604 / 8563734 | 76.2% | 0.6% |

Pages decoded split into find (position column) and read (all columns of the matched rows):

- tiered: find 1,034,960, read 7,474,811
- sorted: find 1,014,300, read 34,683,304
- original: find 1,077,442, read 7,486,292


## Verdict

The warm/cold layout makes vepyr faster on the whole genome. Against the same rows sorted by
position, it uses 36-37% less CPU time in every setting (the sorted layout needs 1.57-1.59 times
as much) and saves 45% (warm) and 39% (cold) of the wall time with one worker, 30% and 20% with
eight. The saving comes from reading the matched rows: they lie on 76% fewer pages, and 78% fewer
pages are decoded to read them, while the pages decoded to find them are 2% more. Per chromosome
the reduction in pages decoded (70.8-78.9%) and the CPU ratio (1.32-1.86) move together; the
smallest gains are on chromosomes 19, 17 and 22. The shipped cache performs like the rewritten
tiered cache (within 1% in every total), so the result applies to the cache vepyr distributes.
