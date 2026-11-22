# Warm/cold tiering of the variation cache (2026-11-22)

Backs main Figure 1 C3 (whole chromosome 22: matched records on 71% fewer position pages) and C4
(whole HG002 genome, cold file cache: CPU -36/-37%, wall -39%/-20% with 1/8 workers; position
pages searched +2%, holding a match -76%, pages decoded to read the matches -78%), and the
Methods sentence on the cache layout.

- `wgs/`: chromosomes 1-22, three caches (warm/cold rewritten, sorted by position, the shipped
  cache), 1 and 8 workers, 3 warm + 2 cold repeats; README with all tables.
- `chr22/`: the first, chromosome-22-only measurement (10 warm + 3 cold repeats).

The tools (relayout, runner, page tool, aggregator) and their tests are on vepyr branch
`perf/cache-tiering-gain` (results commit 240e030), `performance-tests/cache-tiering/`.
