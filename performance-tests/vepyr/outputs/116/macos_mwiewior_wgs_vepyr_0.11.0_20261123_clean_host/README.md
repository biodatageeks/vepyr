# vepyr 0.11.0, HG002 whole genome, merged cache, macOS, clean host (2026-11-23)

Source of the macOS vepyr times in the paper's supplementary Table 17, Figure 1E and the main text,
via `data/wgs_scaling.csv` in the paper repository. HG002 (chromosomes 1-22, 4,096,123 records), merged
release-116 cache, `--everything`, plain VCF, Apple M3 Max, AC power.

## Host state

Measured on a freshly started system (up 2 minutes at the start, no swap in use) with Spotlight
indexing disabled (`sudo mdutil -a -i off`), with nothing else running. On the same host after 10 hours
of uptime, with 16 GB in swap and Spotlight's indexer using up to 47 GB, identical runs were
5-15% slower and varied by 10-17 s. Swap-outs during a run were the clearest sign of a slow run (see
`../macos_mwiewior_wgs_vepyr_0.11.0_20261121` for the earlier, single-run set).

## Protocol

`run_clean_host.zsh` (`ENGINES=vepyr`) ran two harnesses one after the other:

1. `engine-harness/`: the command of the engine comparison (supplement Table 35):
   `python -c vepyr.annotate(...)` under `/usr/bin/time -l`. One discarded 8-worker run, then three
   repeats in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1. Per-contig checksums at 1 and 8 workers.
2. `raw/`: the worker-scaling harness `run_vepyr_worker_scaling.py` of vepyr PR #293 (record count
   after the measurement, read with `F_NOCACHE`; `--require-ac-power`), via `run_macos_rerun.zsh`,
   in the same orders.

Before every run at least 20 GiB of memory were free. Swap-outs (`vm_stat`) were counted over every
timed run; a run that swapped would have been repeated. No run swapped (`raw/swaplog.csv`,
`engine-harness/engines.csv`). Every output had the record MD5 `157fca5b065c20eb7cec3e7a18ef28b7`.

## Results (medians of three runs)

| Workers | Scaling harness, process wall | Engine harness, wall | Peak RSS (scaling) |
|---|---|---|---|
| 1 | 195.01 s | 190.25 s | 4.19 GiB |
| 2 | 119.09 s | 115.69 s | 5.87 GiB |
| 4 | 76.81 s | 75.24 s | 6.78 GiB |
| 8 | 55.21 s | 54.11 s | 9.18 GiB |

Engine harness, CPU (user + system): 325.6 s at 1 worker, 383.1 s at 8 workers.

Output VCFs and logs are not kept.
