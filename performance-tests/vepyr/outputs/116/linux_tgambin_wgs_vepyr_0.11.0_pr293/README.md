# Linux vepyr 0.11.0 merged worker scaling, PR #293 protocol

Whole-genome HG002 (4,096,123 records), merged release-116 cache, plain VCF, measured 2026-11-23 on the Linux
host of [`linux_tgambin_wgs_vepyr_0.11.0_20261121`](../linux_tgambin_wgs_vepyr_0.11.0_20261121) (Ubuntu
22.04.3, Linux 6.8.0-138, AMD Ryzen 9 5950X, 62 GiB RAM, NVMe SSD). It replaces that set in the paper, which
counted the output records inside the timed process and ran once per worker count. vepyr 0.11.0 PyPI wheel,
cache (Hugging Face pin `ac2c8ad`), input and reference are those of the earlier set (`raw/environment.txt`).

## Protocol

`run.sh` uses the benchmark scripts of vepyr PR #293 at `3b06135` and the protocol of the macOS clean-host set
([`macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host`](../macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host)):
one discarded 8-worker run, then three repeats in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1; swap-outs
(`/proc/vmstat` `pswpout`) counted over each run, and a run that swapped repeated; per-contig checksums of every
output, which was then deleted. The process wall covers Python start-up, import, annotation and writing the VCF;
the record count runs after it (PR #293).

Two fixes to `run.sh` were needed on Linux before it could get past its first run; the script was then run end
to end on the chr1:1-3,000,000 smoke input (2,799 records) before the whole genome:

- The runner reads `benchmark_workers.txt` next to itself for the default of `--workers`, even when `--workers`
  is given, so that file is now extracted from `3b06135` too; without it the first run failed with
  `FileNotFoundError`.
- `ls "$dir"/*.vcf "$SSD/$label"/*.vcf | head -1` exits with status 2 because one of the globs never matches,
  which ended the script under `set -euo pipefail` right after the first run; `|| true` was added.

## Host state

Restarted 54 minutes before the start (20:34), browser closed, nothing else running. The weekly `fstrim` and the
daily `unattended-upgrades` run started after login; the measurement started once both had finished and the load
average was below 1. `vm.swappiness` 60 (default), 2 GiB swap, empty throughout. The discarded run swapped out
80 pages, 4 on its repeat and none on the third attempt, while the file cache filled for the first time; no
measured run swapped (`raw/swaplog.csv`). Before every measured run 59.8-59.9 GiB were available, 29 GiB of it
file cache (`raw/free_memory.csv`).

The `power_source` column of the summaries is wrong on this host: PR #293 reads every
`/sys/class/power_supply/*/online`, and the only entry here is the battery of a wireless keyboard
(`hidpp_battery_0`, type `Battery`, scope `Device`), whose flag follows the keyboard's sleep state. It gave
`Battery Power` for 12 runs and `AC Power` for one; the host is a desktop on mains power throughout.

## Results (medians of three runs)

| Workers | Process wall | Annotation only | Annotation range | CPU (user + sys) | Peak RSS |
|---|---|---|---|---|---|
| 1 | 302.09 s | 301.85 s | 301.80-303.85 s | 536 s | 3.29 GiB |
| 2 | 178.22 s | 177.96 s | 176.86-178.00 s | 513 s | 4.72 GiB |
| 4 | 110.37 s | 110.12 s | 109.44-110.63 s | 545 s | 4.78 GiB |
| 8 | 78.59 s | 78.32 s | 77.65-78.47 s | 658 s | 5.44 GiB |

The process wall now exceeds the annotation by 0.24-0.27 s. The three repeats lie within 1.1% of each other, and
the discarded 8-worker run (79.78 s) within 1.5% of the median. Eight workers are 3.85x faster than one. All 13
outputs have the record MD5 `157fca5b065c20eb7cec3e7a18ef28b7` and the 22 per-contig checksums of the macOS
clean-host set. `medians.tsv` holds the medians; `wgs_scaling_rows.csv` the vepyr columns of the four Linux rows
of the paper's `data/wgs_scaling.csv`.

## Against the earlier Linux sets

| Workers | Annotation, 2026-11-21 | Annotation, now | Change | Process wall, 2026-11-21 (with record count) | Process wall, now | Change |
|---|---|---|---|---|---|---|
| 1 | 307.95 s | 301.85 s | -2.0% | 319.77 s | 302.09 s | -5.5% |
| 2 | 181.40 s | 177.96 s | -1.9% | 195.13 s | 178.22 s | -8.7% |
| 4 | 122.29 s | 110.12 s | -10.0% | 136.75 s | 110.37 s | -19.3% |
| 8 | 90.12 s | 78.32 s | -13.1% | 102.19 s | 78.59 s | -23.1% |

About 12 s of every earlier process wall was the record count. The annotation time never included it, yet it is
also lower now, by 2% at 1 and 2 workers and 10-13% at 4 and 8. The earlier set ran once per worker count, with
the browser open, right after the cache had been copied and hashed; its discarded 8-worker run annotated 10%
slower than the measured one, against 1.5% here.

Against the macOS clean-host medians (195.01, 119.09, 76.81 and 55.21 s) the Linux process wall is 1.55, 1.50,
1.44 and 1.42 times as long. Against Linux vepyr 0.10.0 (`linux_vepyr_0.10.0_20261015`: single runs, another
order, outputs compressed on the HDD in between) the annotation is 1.65, 1.68, 1.84 and 2.10 times as fast at 1,
2, 4 and 8 workers.

## Artifacts

`raw/<run>/` holds `summary.tsv` and `merged_workers<N>.{metrics.json,time.txt,md5.json}` per run (`warmup` is
the discarded run); `raw/` also `environment.txt`, `free_memory.csv`, `swaplog.csv` and `swapped_runs.txt`
(empty). As in the macOS clean-host set, console logs, stderr (progress bars) and stdout (empty) are left out;
they are kept with the extracted scripts on the measurement machine at
`/home/tgambin/workspace/vep_data/archive/116/linux_pr293_console/`.
