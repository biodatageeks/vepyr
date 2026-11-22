# Warm/cold variation-cache tiering: does it speed up annotation? (design)

Date: 2026-11-22. Short design, approved in conversation ("yes, go ahead with chr22 first").

## Intent

Settle whether the warm/cold tiering of the variation cache (rows of positions with a maximum
allele frequency of at least 1% first, the rest after, each run sorted by `start`) really speeds
up annotation or reduces CPU time, compared with the same records sorted by position only. The
paper (Figure 1 C3, Methods) currently claims the matched records lie on 64-72% fewer pages;
a re-measurement with a copy of the lookup found that the position-column scan itself is not
reduced (+4-7% pages). What matters is the end-to-end effect on HG002.

## Goals

- A repeatable script, run first on HG002 chromosome 22, later on the whole HG002 genome.
- Per run: wall time, CPU time (user, system), peak RSS; lookup pages decoded and fetched, bytes
  fetched and read operations if the engine exposes them without code changes (otherwise a
  separate counted pass with the copied lookup, as in `performance-tests/paper-0.11.0/cache-pages-20261121`).
- Per layout: file size, output checksums (both layouts must give identical output, else the run
  does not count).

## Non-goals

- Changing the engine or the cache builder. Rebuilding caches from the VEP cache.
- Other samples, plugin caches, Ensembl/RefSeq caches (possible later).

## Approach (chosen)

Re-sort and rewrite the existing variation Parquet files (the user's proposal), with the engine's
own writer (`parquet_cache::write` variation writer properties and batch sizes) for BOTH layouts:

- `tiered`: the current row order, rewritten unchanged;
- `sorted`: all rows sorted by `start`, `tier` set to one value (as the builder does for a
  chromosome without warm positions).

Rewriting both removes writer differences (an earlier hand-written control had 14,802 vs 15,381
position pages). A control cache directory links every other entity (transcript, exon, regulatory,
motif, translation, reference policy) to the original cache, so only `variation/` differs.

Alternatives not taken: a builder switch that skips tiering (engine change, full rebuild from
the VEP cache, hours); a hand-written writer (different page boundaries, unfair).

## Protocol

- vepyr 0.11.0 PyPI wheel (`~/workspace/venvs/vepyr-0.11.0`), merged release-116 cache,
  `HG002_normalized.vcf.gz` restricted to chr22 (50,861 records), `--everything` settings as in the
  supplement whole-genome runs.
- 1 and 8 workers; layouts alternated (ABBA), 5 measured repeats each after one discarded
  warm-up per layout; free memory restored before each run (benchmark protocol notes).
- One extra run per layout after purging the OS file cache, for disk reads.
- Output is not kept: per-contig MD5 of record lines is computed and compared, then discarded
  (disk: 87 GiB free, 27 GiB of variation files per layout for the whole genome).
- Results: one CSV row per run, a summary (median and spread per layout, gain), README.

## Left to the builder

The exact Rust tool shape (a small binary against the engine crate), how per-run CPU time is
captured (`/usr/bin/time -l` on macOS), and whether page/byte counters come from an existing
engine profiling switch or the copied lookup.
