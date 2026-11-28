# Warm/cold tiering of the variation cache: measurement tools

Tools behind the tiering comparison of the vepyr paper (main Figure 1, panels C3 and C4, and the
Methods sentence on the cache layout). They rewrite the variation shards of a cache in two layouts
with the engine's own writer, time vepyr on each layout and count the Parquet pages each layout
touches.

The results are not in this directory: they are in
[`performance-tests/paper-0.11.0/cache-tiering-20261122/`](../paper-0.11.0/cache-tiering-20261122/)
(`chr22/` and `wgs/`, each with a README that gives the setup and the tables). The design and the
plan are in `docs/superpowers/specs/2026-11-22-cache-tiering-gain-design.md` and
`docs/superpowers/plans/2026-11-22-cache-tiering-gain.md`.

## Contents

| Path | What |
|---|---|
| `relayout/` | Rust crate with its own workspace (not part of the vepyr build). `relayout` rewrites one variation shard in the warm/cold order (`tiered`) or sorted by `start` only (`sorted`), always with the engine's `VariationParquetShardWriter`. `paramcheck` exits 1 if two shards differ in any Parquet setting (page boundaries excluded). |
| `make_layout_cache.py` | Assembles a cache directory in which only the variation shards of the requested contigs are rewritten; every other entry is a symlink to the source cache. |
| `run_tiering_gain.py` | Times vepyr on the layouts through `performance-tests/vepyr/scripts/run_vepyr_worker_scaling.py`: one warm-up per layout, repeats in ABBA order, optional cold runs after a purge. A run counts only if its per-contig MD5 equals that of the first valid run. |
| `contig_md5.py` | Record count and MD5 of the record lines of a VCF, per contig, read without filling the file cache. |
| `pages/` | Rust crate `pages76`: a copy of the engine's lookup that records, per lookup buffer, the pages and byte ranges it requests. |
| `pages_summary.py` | Compares the page tool's output for two layouts (`pages.csv`, `pages.json`). |
| `aggregate_wgs.py` | Per-chromosome lines and whole-genome totals (`wgs_summary.md`, `wgs_summary.json`). |
| `run_wgs_tiering.zsh`, `run_wgs_pages.zsh` | The whole-genome runs, one chromosome at a time. Paths of the data directory and the Python interpreter are set at the top of each script. |
| `tests/` | pytest tests of the Python tools. |

## Build and test

```bash
cd performance-tests/cache-tiering
(cd relayout && cargo build --release)   # relayout/target/release/{relayout,paramcheck}
(cd pages && cargo build --release)      # pages/target/release/pages76
python -m pytest tests
```

`make_layout_cache.py` refuses to write unless the destination has the size of the rewritten shards
plus 20 GiB free. Four of its tests go through that check, so they fail on a volume with less free
space.
