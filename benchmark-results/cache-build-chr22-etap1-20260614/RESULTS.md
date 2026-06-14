# Etap 1 — entity concurrency (`build_concurrency`) — chr22

Machine: MacBook Air M1, 16 GB RAM, 8 cores, low-power mode ON (release build).
Dataset: full chr22 raw Ensembl VEP cache (release 115, GRCh38), 15,262,195 output rows.
Method: same binary, vary `build_concurrency` (1 = original serial path via semaphore=1).
Comparison is relative on this machine (NOT the Ryzen/64GB from the presentation).

## Performance

### partitions = 4 (variation gets some intra-query parallelism)
| build_concurrency | seconds | speedup |
|---:|---:|---:|
| 1 (baseline) | 304.75 | 1.00× |
| 2 | 256.01 | 1.19× |
| 4 | 233.51 | **1.31×** |
| 6 | 246.20 | 1.24× |

### partitions = 1 (variation fully sequential)
| build_concurrency | seconds | speedup |
|---:|---:|---:|
| 1 (baseline) | 208.33 | 1.00× |
| 6 | 165.27 | **1.26×** |

Peak RSS ~6–7 GiB across all configs (fits 16 GB).

## Why only ~1.3×
Entity concurrency overlaps the 5 smaller entities with `variation`, which dominates
runtime and runs as a single unit. Amdahl: max speedup ≈ total / variation_time.
Going past concurrency=4 hurts (8 cores, 4 of them efficiency cores, low-power mode,
scheduling/memory contention). The larger win requires the **chromosome axis**
(Etap 3) so variation's many chromosomes build concurrently — only visible on
multi-chromosome / full-genome inputs.

## Correctness
Gate: at FIXED partitions, vary `build_concurrency` → output must be identical.
- partitions=1, build_concurrency 1 vs 6 → **8/8 parquet files logically identical**
  (order-independent multiset hash via polars `hash_rows`). PASS.
- Conclusion: entity concurrency is **output-neutral**; it introduces no non-determinism.

## Pre-existing non-determinism (NOT caused by this change)
At partitions>1 the build is **not reproducible run-to-run**: two independent serial
(build_concurrency=1) builds at partitions=4 differed in 5/8 files (same row counts,
different content). Cause is multi-partition execution + unstable dedup tie-breaks
(`ROW_NUMBER() ... ORDER BY stable_id NULLS LAST` for exon/transcript) and the
warm/cold variation split across partition boundaries. This predates the
parallelization work and is independent of `build_concurrency`. Worth a separate
look if reproducible caches are required.
