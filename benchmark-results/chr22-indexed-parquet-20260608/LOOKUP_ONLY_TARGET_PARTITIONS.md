# Lookup-only target partitions

Date: 2026-06-09

## Design

- `forks` controls concurrent chromosome lanes.
- `workers` controls annotation runtime workers per active chromosome.
- `target_partitions` controls independent cold Indexed Parquet row-group
  readers.
- `workers` and `target_partitions` are independent. The Parquet readers are
  scoped OS threads, not Tokio annotation workers.
- The DataFusion session remains at one partition. Transcript, exon,
  translation, regulatory, motif, input, and output plans are not repartitioned.
- Loaded row-group chunks are flattened in original partition order before
  insertion into the lookup cache.
- Both parameters default to `1`.

This avoids the rejected global `SessionConfig.target_partitions` experiment,
which multiplied context scans, memory use, and ordered-drain work.

## Benchmark fixture

- chromosome: `chr22`
- output records: `59,013`
- cold row groups touched: all `1,749`
- cache: complete Ensembl 115 GRCh38 Indexed Parquet chr22 cache
- body SHA-256:
  `16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`

Every measured output matched this hash.

## Exploration

The first full matrix covered all 36 combinations where
`1 <= target_partitions <= workers <= 8`, with 3 shuffled repeats each.
The fastest point was `8/8` at `11.152 s`, but this matrix could not determine
whether the improvement came from workers or readers.

The independent sweep then tested workers `1,2,6,10` against reader counts up
to `16`. It showed that reader count is the dominant parameter:

| workers | target | median | median RSS |
|---:|---:|---:|---:|
| 1 | 1 | 19.179 s | 2.63 GiB |
| 1 | 8 | 11.336 s | 3.17 GiB |
| 1 | 12 | 11.186 s | 3.25 GiB |
| 1 | 16 | 11.771 s | 3.19 GiB |
| 2 | 10 | 11.567 s | 3.04 GiB |
| 6 | 10 | 11.107 s | 3.10 GiB |
| 10 | 10 | 11.587 s | 3.10 GiB |

Increasing workers without increasing readers does not reliably help.
Increasing readers reduces lookup wait until approximately `8-12`, after which
I/O/decompression contention, thread scheduling, hydration, and annotation
work dominate. More readers can still reduce raw lookup wait while making the
whole pipeline slower and increasing RSS.

## Historical compact baseline

The original compact suite used 8 configurations and 5 shuffled repeats:

| workers | target | purpose | median | range | median RSS |
|---:|---:|---|---:|---:|---:|
| 1 | 1 | baseline | 18.771 s | 18.528-21.081 s | 2.74 GiB |
| 2 | 2 | old conservative point | 13.744 s | 13.265-15.554 s | 2.70 GiB |
| 1 | 8 | practical optimum | 10.932 s | 10.785-11.258 s | 3.13 GiB |
| 1 | 12 | plateau | 11.025 s | 10.555-12.011 s | 3.19 GiB |
| 1 | 16 | post-plateau contention | 11.248 s | 11.064-11.311 s | 3.51 GiB |
| 2 | 10 | worker control | 10.996 s | 10.681-11.281 s | 3.19 GiB |
| 6 | 10 | worker control | 11.537 s | 10.632-12.072 s | 3.05 GiB |
| 10 | 10 | worker control | 10.917 s | 10.461-11.552 s | 3.09 GiB |

The recommended practical setting for this single-chromosome fixture is
`workers=1,target_partitions=8`. It is within measurement noise of the fastest
points, uses fewer runtime workers, and stays below the higher-memory
post-plateau configurations.

Raw compact results:

- `parallelism-regression-baseline/summary.json`
- `parallelism-regression-baseline/runs.csv`
- `parallelism-regression-baseline/runs.jsonl`
- `parallelism-regression-baseline/profile-*.log`

Exploratory results:

- `workers-target-matrix-1-8/`
- `independent-workers-target-sweep/`
- `workers-sweep-target10-rerun/`
- `extended-w10-p10/`
- `extended-w12-p12/`
- `extended-w14-p14/`

## Extended workers sweep

An additional sweep on 2026-06-10 fixed `target_partitions=10` and tested
`workers=1,2,4,6,8,10,12,16`, with 5 shuffled repeats per value.

| workers | median | range |
|---:|---:|---:|
| 1 | 20.442 s | 14.061-23.294 s |
| 2 | 20.266 s | 11.751-21.128 s |
| 4 | 20.010 s | 10.418-23.184 s |
| 6 | 13.196 s | 11.269-19.905 s |
| 8 | 18.185 s | 17.645-21.323 s |
| 10 | 18.976 s | 15.214-19.806 s |
| 12 | 18.624 s | 11.377-24.839 s |
| 16 | 21.908 s | 16.964-22.965 s |

All `40/40` outputs matched the reference hash. The host performance changed
substantially during this sweep: early runs were commonly `18-23 s`, while
later shuffled runs reached `10-13 s`. Therefore this sweep is valid for
correctness and for rejecting a monotonic workers speedup, but not for selecting
a precise workers optimum. It provides no evidence that workers above `10`
help; `16` was the slowest median. The canonical regression baseline above
remains the reference for performance comparisons.

## Canonical full matrix

The canonical post-change benchmark is now the full Cartesian matrix:

- workers: `1,2,4,6,8,10,12,16`
- target partitions: `1,2,4,6,8,10,12,16`
- configurations: `64`
- standard repeats: `3`
- total runs: `192`
- shuffled execution order
- separate process, body hash, elapsed time, RSS, and profile for every run

The benchmark writes:

- `runs.jsonl`: append-only raw measurements
- `runs.csv`: raw measurements in tabular form
- `summary.json`: medians, ranges, RSS, and equality per matrix cell
- `median_seconds_matrix.csv`: workers-by-target timing matrix
- `median_rss_bytes_matrix.csv`: workers-by-target memory matrix
- `profile-*.log`: detailed profiles

The retained full baseline completed on 2026-06-10:

- result directory: `parallelism-full-matrix-baseline/`
- completed runs: `192/192`
- complete cells: `64/64`
- repeats per cell: `3`
- matching output hashes: `192/192`

Median seconds matrix:

| workers \ target | 1 | 2 | 4 | 6 | 8 | 10 | 12 | 16 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 22.189 | 19.071 | 13.856 | 12.815 | 11.162 | 11.811 | 11.002 | 10.945 |
| 2 | 19.919 | 16.143 | 14.142 | 13.788 | **10.459** | 11.869 | 11.292 | 11.391 |
| 4 | 22.122 | 14.145 | 13.574 | 13.416 | 12.187 | 10.953 | 11.229 | 11.267 |
| 6 | 21.541 | 15.846 | 15.831 | 13.140 | 11.559 | 10.607 | 10.901 | 10.606 |
| 8 | 21.670 | 14.333 | 13.846 | 13.392 | 11.774 | 10.799 | 11.716 | 11.849 |
| 10 | 19.198 | 13.981 | 15.764 | 12.540 | 12.995 | 11.140 | 10.538 | 11.475 |
| 12 | 19.432 | 15.445 | 15.326 | 12.659 | 11.015 | 11.798 | 10.817 | 10.922 |
| 16 | 18.975 | 15.306 | 13.741 | 13.452 | 11.243 | 10.784 | 11.025 | 11.130 |

The fastest median in this full baseline is `workers=2,target=8` at `10.459 s`.
Several cells are close (`10/12`, `6/16`, `6/10`, `16/10`), which is exactly
why future changes must compare the whole matrix instead of one chosen point.

The compact 8-point set remains available as `--smoke-suite` for quick local
checks. It is not sufficient for final performance evaluation.

## Adaptive prefetch wave

The next optimization changed the cold-Parquet prefetch wave from a fixed 64
row groups to an adaptive size:

```text
max(64, target_partitions * 32)
```

The environment variable
`VEP_COLD_PARQUET_PREFETCH_ROW_GROUP_BATCH_SIZE` remains an explicit override.
This keeps the original 64-row-group behavior for one or two readers while
reducing repeated thread creation, file opening, and reader initialization for
larger reader counts.

An exploratory sweep compared wave sizes `64`, `128`, `256`, `512`, and a
single unbounded wave. A single wave improved the parallel configurations but
regressed the single-reader path and increased its memory use, so it was
rejected in favor of the adaptive policy.

The retained adaptive full matrix is:

- result directory: `parallelism-full-matrix-adaptive-prefetch-wave/`
- comparison: `comparison-adaptive-prefetch-wave/`
- completed runs: `192/192`
- matching output hashes: `192/192`
- comparison result: 40 improvements, 17 neutral, 7 nominal regressions

The nominal regressions are concentrated in `target_partitions=1/2`, where the
adaptive policy still uses the unchanged 64-row-group wave. They are therefore
consistent with the substantial host-time variance observed during the earlier
worker sweep rather than with the new batching behavior.

For the changed `target_partitions=4-10` range, most matrix cells improved by
approximately 8-30%. Representative results:

| workers | target | baseline | adaptive | change |
|---:|---:|---:|---:|---:|
| 1 | 4 | 13.856 s | 10.791 s | -22.1% |
| 2 | 4 | 14.142 s | 10.305 s | -27.1% |
| 4 | 6 | 13.416 s | 9.718 s | -27.6% |
| 8 | 6 | 13.392 s | 9.360 s | -30.1% |
| 2 | 8 | 10.459 s | 9.591 s | -8.3% |
| 16 | 10 | 10.784 s | 9.904 s | -8.2% |

## Rejected cost-aware partitioning experiment

A follow-up experiment partitioned each prefetch wave into contiguous reader
ranges balanced by the number of page-index-selected rows instead of by the
number of row groups. The intent was to reduce reader-tail imbalance while
preserving row-group and output ordering.

The retained 12-cell smoke benchmark used 5 shuffled repeats per cell:

- results: `cost-aware-partitioning-smoke/`
- comparison: `comparison-cost-aware-partitioning-smoke/`
- matching output hashes: `60/60`
- improvements: 0
- neutral: 6
- regressions: 6

The largest regression was workers 2 / target 8: `9.591 s` to `10.926 s`
(`+13.9%`). The implementation was therefore rejected and restored. Selected
row count is not a sufficiently accurate proxy for Parquet reader cost on this
cache; equal row-group-count partitioning remains the production behavior.

## Rejected persistent reader pool experiment

A second follow-up kept scoped reader threads alive across all prefetch waves
within one `prefetch_positions` call. Each worker opened the Parquet file once,
received later wave partitions over a channel, and returned tagged results for
ordered assembly. Wave sizing, cache retention, and output ordering were
unchanged.

The same 12-cell smoke set used 5 shuffled repeats:

- results: `persistent-reader-pool-smoke/`
- comparison: `comparison-persistent-reader-pool-smoke/`
- matching output hashes: `60/60`
- improvements: 0
- neutral: 11
- regressions: 1

Most changes were between -2.6% and +1.5%, below the 5% significance threshold.
Workers 2 / target 4 regressed from `10.305 s` to `10.948 s` (`+6.2%`).
The implementation was rejected because the small neutral gains did not justify
the additional worker/channel lifecycle complexity.

## Rejected lookup/annotation lookahead experiment

The final experiment kept one complete VEP input buffer ready ahead of the
buffer being annotated. Lookup targeted two ready buffers, while each
annotation iteration consumed at most one, using the existing bounded lookup
queues and preserving output order.

The same 12-cell smoke set used 5 shuffled repeats:

- results: `pipeline-lookahead-smoke/`
- comparison: `comparison-pipeline-lookahead-smoke/`
- matching output hashes: `60/60`
- improvements: 0
- neutral: 12
- regressions: 0

Median changes ranged from `-2.7%` to `+2.8%`, below the 5% significance
threshold. The implementation was rejected and restored because it added
buffering/state-machine behavior without a measurable throughput improvement.
This concludes the planned parallel lookup optimization experiments.

## Repeat command

Use the full named regression suite after every parallelism change:

```bash
rtk .venv/bin/python e2e-testing/scripts/benchmark_parallel_lookup.py \
  --input-vcf /private/tmp/vepyr-indexed-chr22-full/chr22_sparse_50000.sorted.chr22.single_alt.vcf \
  --cache-dir /private/tmp/vepyr-indexed-chr22-full/parquet/115_GRCh38_ensembl \
  --reference-fasta /Users/lukaszjezapkowicz/.vep/homo_sapiens/115_GRCh38/Homo_sapiens.GRCh38.dna.toplevel.fa \
  --results-dir benchmark-results/chr22-indexed-parquet-20260608/parallelism-regression-NEW_NAME \
  --repeats 3 \
  --regression-suite \
  --skip-serial \
  --profile
```

Never overwrite `parallelism-full-matrix-baseline`; each code change gets a new
results directory and is compared against the full baseline `summary.json`.

Compare the full candidate matrix against the retained full baseline:

```bash
rtk .venv/bin/python e2e-testing/scripts/compare_parallel_benchmarks.py \
  --baseline benchmark-results/chr22-indexed-parquet-20260608/parallelism-full-matrix-baseline \
  --candidate benchmark-results/chr22-indexed-parquet-20260608/parallelism-full-matrix-NEW_NAME \
  --output-dir benchmark-results/chr22-indexed-parquet-20260608/comparison-NEW_NAME
```

The comparison produces `comparison.csv` and `comparison.md`, classifying each
of the 64 cells as improvement, neutral, regression, incorrect, or missing.

## Verification

- functions VCF concurrency tests: `14 passed`
- partitioned cold-row-group prefetch test: passed
- Python parallelism/API tests: `15 passed`
- release editable build: passed
- canonical benchmark: `40/40` output hashes matched
- canonical full matrix: `192/192` output hashes matched
- full 1..8 matrix: `108/108` output hashes matched
- independent sweep: `90/90` output hashes matched

Direct `cargo test annotate::tests` in `vepyr` remains blocked by the existing
macOS PyO3 test-binary linker issue. This does not affect the successful release
extension build or Python tests.
