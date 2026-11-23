# Linux vepyr 0.11.0 merged worker scaling, PR #293 protocol (pending)

To be run on the Linux host of [`linux_tgambin_wgs_vepyr_0.11.0_20261121`](../linux_tgambin_wgs_vepyr_0.11.0_20261121)
(Ubuntu 22.04, AMD Ryzen 9 5950X). It replaces that set in the paper, which counted the output records inside the
timed process and ran once per worker count.

`run.sh` uses the benchmark scripts of vepyr PR #293 at `3b06135` (read from git, so any checkout works) and the
protocol of the macOS clean-host set
([`macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host`](../macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host)):
one discarded 8-worker run, then three repeats in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1; swap-outs counted over each
run and a run that swapped repeated; per-contig checksums, which must give the output MD5
`157fca5b065c20eb7cec3e7a18ef28b7` of macOS.

How to run (about 1.5 h; nothing else should run meanwhile):

1. Restart the machine, log in, close the browser and other heavy programs.
2. `git -C ~/workspace/vepyr fetch origin && git -C ~/workspace/vepyr checkout perf/paper-results-0.11.0`
   (or copy this folder anywhere on the NVMe SSD), then `bash run.sh`. Paths are those of the earlier
   Linux `run.sh`; it stops if any of them is not on `/dev/nvme0n1p5` or the SSD has less than 75 GiB free.
3. Commit `raw/`, `medians.tsv`, `wgs_scaling_rows.csv` and `run.sh` here (no VCFs). `wgs_scaling_rows.csv` holds
   the vepyr columns of the four Linux rows of the paper's `data/wgs_scaling.csv`.

The summary step was checked on the macOS clean-host results, from which it reproduces the published medians
(195.01, 119.09, 76.81 and 55.21 s) and the output MD5. The Linux-specific parts (`/proc/vmstat`, `findmnt`,
`df -BG`) have not been run yet.
