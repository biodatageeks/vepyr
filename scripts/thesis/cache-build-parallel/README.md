# Thesis: parallel VEPYR cache build (single machine)

Scripts used in the master's thesis (Politechnika Warszawska, 2026) to measure
the construction of the VEPYR Parquet cache for release 116 (merged profile,
1904 contigs with a known-variant file, five entities: variation, transcript,
exon, regulatory, motif; translation measured separately).

| Script | Experiment | Result reported in the thesis |
|---|---|---|
| `parallel_entity_queue.py` | dynamic queue: contigs grouped greedily (LPT-style) into 20 groups by `all_vars.gz` size, 100 entity-group tasks, `ProcessPoolExecutor(10)`, `RAYON_NUM_THREADS=1`, `partitions=1` | |
| `run_parallel_queue_5.py` | 5 repetitions of the queue (2026-10-17) | mean 1473.60 s |
| `assemble_parallel_cache.py` | merges per-task outputs (hard links + manifests, aborts on duplicates); not part of the timed run | |
| `full_core_5x.py` | sequential native build of the same five entities, 5 repetitions (2026-10-19) | mean 2540.71 s |
| `full_e2e_all.py` | full build including translation, one contig per call (2026-10-19) | 4193.18 s, 39.05 GB |

## Code and binary

- vepyr: this branch (`cache-build-opt-b-native` 2ec0d4c + lock/runtime commit).
- datafusion-bio-functions: branch `thesis/cache-build-parallel`
  (7ff3bb6 + native single-pass build commit). `Cargo.toml` here still
  contains the local `[patch]` to `../datafusion-bio-functions`; to build
  from a clean clone, check that branch out next to this repository.
- The measured Python extension (`_core.abi3.so`, built 2026-09-30,
  sha256 `5c8b67aa538936f4c70f1fe79cfafcb0a87e053cd73f42bde54db99c5a255c56`)
  was compiled from the then-uncommitted working trees; the commits above are
  the closest reconstruction (see the datafusion-bio-functions commit message).
- Python 3.14 (`.venv-lpt`), macOS on an Apple M5 laptop (32 GiB RAM).

The scripts use absolute paths of the author's machine and write to
`runs/...` relative to the thesis repository root; adjust `DATA`/`RUN`
before reuse.
