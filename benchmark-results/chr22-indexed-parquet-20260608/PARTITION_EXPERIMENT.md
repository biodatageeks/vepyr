# Lookup partition experiment

## Hypothesis

`workers=N` increased Tokio runtime threads, but the VCF path still reported
`lookup_partitions=1`. The experiment separated chromosome lanes from lookup
partitions and passed `workers` as DataFusion target partitions.

## Result

The hypothesis was rejected for the current Indexed Parquet implementation.

| Configuration | Time | Peak RSS | Body SHA-256 |
| --- | ---: | ---: | --- |
| Pre-change workers=2 profile | 27.117 s | 2.22 GiB | baseline body |
| Pre-change workers=4 profile | 24.156 s | 2.38 GiB | baseline body |
| 2 real lookup partitions | 148.916 s | 2.88 GiB | `b4574ee1...` |
| 4 real lookup partitions | 193.529 s | 4.12 GiB | `a32b7be8...` |

The established baseline body hash is
`16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`.
Both partitioned runs produced 59,013 records but different body hashes, and
the 2- and 4-partition hashes also differed from each other.

## Profile evidence

- Two partitions: context load 85.478 s, lookup wait 13.078 s, annotation
  45.282 s, ordered drain wait 13.078 s.
- Four partitions: context load 64.878 s, lookup wait 39.099 s, annotation
  82.939 s, ordered drain wait 39.099 s.
- Before the change, context load was about 0.2 s and annotation about 5.7 s.

Using global `SessionConfig.target_partitions` partitions more than variation
lookup. It also changes context table execution and introduces expensive
contention. The existing ordered fan-in then waits for earlier lookup
partitions, and output order is not invariant.

## Decision

The production pin and wrapper behavior were restored after collecting the
profiles. Do not enable multiple Indexed Parquet lookup partitions through the
global session target.

The next implementation must:

1. Keep context scans and the outer annotation session single-partitioned.
2. Partition only the VCF input consumed by `KvLookupExec`.
3. Use stable contiguous ranges or explicit input ordinals.
4. Restore original record order before annotation/output.
5. Bound per-partition cache state and verify that cold Parquet files are not
   redundantly opened or prefetched by every partition.

Raw logs and measurements are in `profiles-before/` and `profiles-after/`.
