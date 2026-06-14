# Unified scheduler: LPT (#1) + weighted semaphore (#2) — chr19-22

Machine: MacBook Air M1, 16 GB, 8 cores, low-power mode ON (release, partitions=1).
Dataset: chr19+20+21+22, 78,305,358 rows. Relative same-machine comparison.

`build_all` was refactored into a single flat work-unit scheduler: every
(entity, chromosome) unit is discovered, ordered longest-first (LPT, #1), and
run through one weighted semaphore (#2, env `VEPYR_VAR_WEIGHT`).

## #1 LPT (VEPYR_VAR_WEIGHT=1) vs previous nested scheduler (no global LPT)
| build_concurrency | nested (no LPT) | flat + LPT | LPT gain |
|---:|---:|---:|---:|
| 1 | 1047 s (1.00×) | 1037 s (1.00×) | — |
| 2 | 649 s (1.61×) | 585 s (1.77×) | ~10% |
| 4 | 491 s (2.13×) | 427 s (**2.43×**) | ~13% |
| 8 | 421 s (2.49×) | 421 s (2.46×) | ~0% |

LPT helps at mid concurrency (permits < heavy units → ordering matters): the
heavy variation chromosomes start first instead of queueing behind light units.
At concurrency=8 there are enough permits that ordering is irrelevant.
Correctness: conc=1 vs conc=4 → 32/32 parquet files logically identical.

## #2 Weighted semaphore (concurrency=8)
| VEPYR_VAR_WEIGHT | seconds |
|---:|---:|
| 1 (unweighted) | 421 s |
| 3 | 615 s |

On data that fits in RAM, weighting is a **slowdown**: it caps how many
variation tiers run at once (at weight=3, floor(8/3)=2 instead of 4), leaving
cores idle. Its purpose is **not** speed — it is a memory-safety valve that lets
a high `build_concurrency` be used without OOM on memory-constrained machines
(e.g. full genome on 16 GB, where 4+ concurrent large-chromosome variation tiers
would exceed RAM). On the target 64 GB machine, keep `VEPYR_VAR_WEIGHT=1`.

## Takeaways
- Best demo config: flat scheduler, `VEPYR_VAR_WEIGHT=1`, concurrency 4-8 → ~2.46×.
- #1 (LPT) is a free, output-neutral win at mid concurrency; grows with chromosome count / when permits are scarcer than heavy units.
- #2 (weighting) trades speed for memory headroom; only enable when unweighted would OOM.
