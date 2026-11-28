# Performance tests

- [`vep/`](vep/README.md): Ensembl VEP fork-scaling runner, plotter and outputs
- [`vepyr/`](vepyr/README.md): vepyr worker-scaling runner, plotter and outputs
- [`polars-integration/`](polars-integration/README.md): vepyr's LazyFrame and output paths
  against Ensembl VEP plus `filter_vep` on chr22
- [`paper-0.11.0/`](paper-0.11.0/README.md): index from each table and figure of the vepyr paper
  to the raw results behind it, and the result sets that have no other directory

Generated VCFs, the staging FASTA and the caches stay outside Git; the run directories keep the
commands, time logs and summaries of their runs.

Results measured with vepyr versions before 0.11.0 are not kept on master. They are on the branch
[`archive-pre-0.11.0`](https://github.com/biodatageeks/vepyr/tree/archive-pre-0.11.0).

## Ensembl VEP vs vepyr 0.11.0 (release 116, whole genome)

Both tools annotated the same normalized HG002 GRCh38 input with the same release-116 merged
cache, `--everything --hgvs` and the GRCh38 primary assembly FASTA, writing plain, uncompressed
VCF. The measured number is whole-process wall time in both cases (`docker run` for VEP, the
Python process for vepyr), so start-up and writing the output are inside every figure.

The two parallelism knobs are not the same unit. VEP `--fork N` starts N annotation children next
to the parent that reads and writes, so `--fork 1` already runs two processes, while vepyr's
default `workers=1` is a single pipeline. The table therefore pairs the runs by process count:
VEP without `--fork` against `workers=1`, `--fork 1` against `workers=2`, `--fork 3` against
`workers=4` and `--fork 7` against `workers=8`.

| Processes | VEP setting | VEP wall | vepyr setting | vepyr wall | VEP / vepyr |
|---|---|---|---|---|---|
| **macOS** | | | | | |
| 1 | no fork | 8:45:41 (31,541 s) | `workers=1` | 190.3 s | 166× |
| 2 | `--fork 1` | 4:29:09 (16,149 s) | `workers=2` | 115.7 s | 140× |
| 4 | `--fork 3` | 2:30:05 (9,005 s) | `workers=4` | 75.2 s | 120× |
| 8 | `--fork 7` | 1:29:32 (5,372 s) | `workers=8` | 54.1 s | 99× |
| **Linux** | | | | | |
| 1 | no fork | 10:53:12 (39,192 s) | `workers=1` | 302.1 s | 130× |
| 2 | `--fork 1` | 5:57:20 (21,440 s) | `workers=2` | 178.2 s | 120× |
| 4 | `--fork 3` | 3:10:18 (11,418 s) | `workers=4` | 110.4 s | 103× |
| 8 | `--fork 7` | 1:54:18 (6,858 s) | `workers=8` | 78.6 s | 87× |

### Environment and sources

- Input: HG002 GRCh38 v4.2.1 benchmark VCF split with `bcftools norm -m -both`, chromosomes
  1-22, 4,096,123 records, SHA-256
  `76931747a93b077464046236fb7357e0bc8ca8e4581e6f19a95c97334867965b`.
- macOS host: MacBook Pro, Apple M3 Max (12 performance + 4 efficiency cores), 64 GiB RAM.
  - VEP 116.0 (September 2026, one run per setting):
    [`vep/outputs/116/macos_mwiewior_wgs_20260914`](vep/outputs/116/macos_mwiewior_wgs_20260914)
    (`summary.tsv`; `--fork 3` and `--fork 7` in `merged_fork3.time.txt` and
    `merged_fork7.time.txt`).
  - vepyr 0.11.0 (November 2026, median of three runs on a freshly started host):
    [`vepyr/outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host`](vepyr/outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host)
    (the engine-harness column of its README).
- Linux host: AMD Ryzen 9 5950X, 62 GiB RAM, NVMe SSD, Ubuntu 22.04.3.
  - VEP 116.0 (one run per setting):
    [`vep/outputs/116/merged_fork_scaling`](vep/outputs/116/merged_fork_scaling) (`summary.tsv`).
  - vepyr 0.11.0 (November 2026, median of three runs):
    [`vepyr/outputs/116/linux_tgambin_wgs_vepyr_0.11.0_pr293`](vepyr/outputs/116/linux_tgambin_wgs_vepyr_0.11.0_pr293)
    (`medians.tsv`).

### Output parity

For this input the VCF record lines written by vepyr 0.11.0 equal those of VEP 116.0 on every
chromosome, with the Ensembl, the RefSeq and the merged cache, and with the plugin profiles on the
merged cache. The per-chromosome reports are in
[`paper-0.11.0/vepyr-0110-supplement-20261121/parity`](paper-0.11.0/vepyr-0110-supplement-20261121/parity).

### Reading the numbers

- GNU time's RSS for VEP is that of the host Docker CLI (about 27 MB), not VEP's memory inside
  the container. VEP's memory was measured separately, in
  [`paper-0.11.0/vep-peakmem-fork7-20261121`](paper-0.11.0/vep-peakmem-fork7-20261121).
- VEP `--fork 1` still has a parent process; the separate no-fork run is VEP's serial path.
- The vepyr process wall covers Python start-up, import, annotation and writing the VCF. The
  record count that verifies the output runs after the timed process (see
  [the timing definition](vepyr/README.md#timing-definition)).
- VEP ran once per setting. The vepyr value is the median of three runs.

To measure again, use the runners described in [`vep/README.md`](vep/README.md) and
[`vepyr/README.md`](vepyr/README.md).
