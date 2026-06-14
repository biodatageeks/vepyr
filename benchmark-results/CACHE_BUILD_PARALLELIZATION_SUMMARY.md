# Cache-build parallelization — summary

Goal: parallelize `vepyr.build_cache` on a single machine, preserving cache content
(VEP parity), measured as a relative speedup on the same machine.

Dev/measurement machine: MacBook Air M1, 16 GB RAM, 8 cores, low-power mode ON.
(NOT the Ryzen 9 5950X / 64 GB from the presentation — absolute numbers are not
comparable to it; only relative same-machine speedups are reported.)

## What was built
A single knob `build_concurrency` controls a global semaphore of concurrent
(entity, chromosome) work-units. `build_concurrency=1` reproduces the original
serial build path exactly (semaphore = 1), so baseline and parallel runs use the
SAME binary — zero build/profile/machine variance.

Two axes of concurrency:
- **Entity axis** (Etap 1): the 6 cache entities (variation, transcript, exon,
  translation, regulatory, motif) build concurrently (disjoint output dirs).
- **Chromosome axis** (Etap 3): chromosomes within each entity build concurrently.
  `build_variation` refactored into `build_variation_main_chrom` (per-chrom, own
  context + temp dir) + `build_variation_other` (combined contigs, single unit);
  `build_parquet_entity` / `build_translation` parallelize their chrom loops.
  Runtime worker threads decoupled from DataFusion `partitions`.

## Results (release build, partitions=1)

Entity axis only — chr22 (1 chromosome):
| build_concurrency | speedup |
|---:|---:|
| 1 | 1.00× |
| 4 | 1.31× |
Amdahl-limited: `variation` dominates and is a single unit, so only the 5 smaller
entities overlap with it.

Entity + chromosome axis — chr21+22 (2 chromosomes):
| build_concurrency | speedup |
|---:|---:|
| 1 | 1.00× |
| 2 | 1.77× |
| 4 | 1.98× |
| 8 | 1.97× |
Plateaus at ~2× = number of chromosomes (2 heavy variation long-poles).

Entity + chromosome axis — chr19+20+21+22 (4 chromosomes):
| build_concurrency | seconds | speedup | peak RSS |
|---:|---:|---:|---:|
| 1 | 1047.3 | 1.00× | 10.6 GiB |
| 2 | 649.1 | 1.61× | 8.9 GiB |
| 4 | 491.1 | 2.13× | 10.6 GiB |
| 8 | 421.0 | **2.49×** | 9.9 GiB |
Still climbing at conc=8 (bounded by 8 cores + low-power + RAM).

**Trend: speedup grows with chromosome count** (1 chrom ~1.3×, 2 chroms ~2.0×,
4 chroms ~2.5×). The full genome (24 main chromosomes) scales further, bounded by
cores and RAM — that run belongs on the 64 GB / many-core target machine. On 16 GB,
concurrent large-chromosome variation tiers approach the memory ceiling (chr1
variation alone peaks ~8.7 GiB).

## Correctness
Gate: at FIXED partitions, varying `build_concurrency` must not change output.
- chr22, p=1, conc 1 vs 6 → 8/8 files logically identical.
- chr21+22, p=1, conc 1 vs 4 → 16/16 files logically identical.
Verified with `scripts/compare_cache.py` (order-independent multiset hash per parquet
via polars `hash_rows`). Parallelization is output-neutral.

## Pre-existing non-determinism (separate issue, NOT introduced here)
At `partitions>1` the build is not reproducible run-to-run: two independent serial
builds at partitions=4 differed in several files (same row counts, different content).
Cause: multi-partition execution + unstable dedup tie-breaks (`ROW_NUMBER() ... ORDER
BY stable_id NULLS LAST` for exon/transcript) and warm/cold split across partition
boundaries. Predates this work; relevant if reproducible caches are required.

## Tooling
- `scripts/bench_cache_build.py` — sweep `build_concurrency`/`partitions`, fresh
  subprocess per config, records runs.csv / runs.jsonl / summary.json / RESULTS.md.
  Builds a chromosome-subset raw cache by copying per-chrom dirs (build_cache has no
  chromosome filter).
- `scripts/compare_cache.py` — logical-equality check between two built caches.

## Build notes (M1 16 GB)
- `vepyr/Cargo.toml` has a `[patch]` redirecting `datafusion-bio-function-vep` to the
  in-tree checkout so changes build without pushing. Revert before release.
- Release build needs `CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
  CARGO_INCREMENTAL=0` + `SDKROOT`/`CPATH` (else OOM during compile / snappy C++ error).
- Build via maturin only; never copy the raw `target/release/*.dylib` (code-signing).
