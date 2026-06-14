# Etap 3 — chromosome-axis concurrency — chr21 + chr22

Machine: MacBook Air M1, 16 GB RAM, 8 cores, low-power mode ON (release build).
Dataset: full chr21 + chr22 raw Ensembl VEP cache (release 115, GRCh38), 29,893,709 rows.
Method: same binary, vary `build_concurrency` (1 = original serial path), partitions=1.
Relative comparison on this machine (NOT the Ryzen/64GB from the presentation).
Raw per-run numbers: summary.json (conc 1 vs 4 + correctness), summary_curve.json (full sweep).

## What changed (vs Etap 1)
Etap 1 only overlapped the 6 *entities*. Etap 3 also runs *chromosomes within each
entity* concurrently, under one global semaphore (`build_concurrency` = max
concurrent (entity,chrom) units). `build_variation` was refactored: per-main-chrom
work extracted into `build_variation_main_chrom` (own context + temp dir), "other"
contigs kept as a single unit (`build_variation_other`); main chroms run via JoinSet.
`build_parquet_entity` and `build_translation` parallelize their chrom loops the same way.

## Scaling (partitions=1)
| build_concurrency | seconds | speedup |
|---:|---:|---:|
| 1 (baseline) | 351.92 | 1.00× |
| 2 | 198.34 | 1.77× |
| 4 | 177.78 | **1.98×** |
| 8 | 178.48 | 1.97× |

Peak RSS 6.4–7.3 GiB (fits 16 GB).

### Why it plateaus at ~2×
Only **2 chromosomes** here → 2 heavy `variation` long-poles. Once both build
concurrently (build_concurrency ≥ 2) wall-clock is bounded by the slowest single
chromosome's variation tier; extra concurrency (4, 8) has no more big units to fill.
The speedup ceiling ≈ number of chromosomes, capped by cores / memory. On the full
genome (24 main chromosomes) the ceiling is far higher — that run belongs on the
64 GB target machine (on 16 GB, concurrent large-chromosome variation tiers would
exceed RAM; chr1 variation alone peaks ~8.7 GiB).

## Correctness
At partitions=1 (deterministic), build_concurrency 1 vs 4 → **16/16 parquet files
logically identical** (order-independent multiset hash). The variation refactor and
chromosome-level concurrency are output-neutral.

## Combined picture (this machine)
- Entity axis alone (Etap 1, chr22): ~1.31×.
- Entity + chromosome axis (Etap 3, chr21+22): ~1.98× (2 chroms).
- Speedup grows with chromosome count → full-genome validation on 64 GB hardware.
