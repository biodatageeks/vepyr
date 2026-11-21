# Ensembl VEP peak memory with --fork 7 (2026-11-21)

Backs the VEP peak-memory value of main-text Figure 1A (3.4 GiB summed RSS; 1.9 GiB summed PSS).

## Run

- Ensembl VEP 116.0, Docker image for arm64, on the Apple M3 Max host.
- Input `HG002_normalized.vcf.gz` (4,096,123 records), merged cache, `--everything --hgvs`,
  VCF output, `--fork 7` (8 processes). Command line in `run_with_sampler.sh`.
- A sampler in the same container summed `Rss` and `Pss` of `/proc/<pid>/smaps_rollup` over all
  processes every 5 s (`mem.tsv`: epoch seconds, process count, summed RSS kB, summed PSS kB).

## Results

| Quantity | Value |
|---|---|
| Peak summed RSS | 3,575,780 kB (3.41 GiB) |
| Peak summed PSS | 1,972,006 kB (1.88 GiB) |
| Records written | 4,096,123 |
| Wall time, with the sampler running | 5,518 s (`times.txt`, exit status 0) |

RSS counts pages shared between the forked processes once per process; PSS divides them among the
processes, so the summed PSS is the closer estimate of the physical memory used.
`cgroup_memory_peak.txt` (14,239,035,392 bytes) is the cgroup's `memory.peak`, which also counts the
page cache filled by reading the cache, FASTA and input; it is not used in the paper.

## Files

`mem.tsv`, `run_with_sampler.sh`, `times.txt`, `cgroup_memory_peak.txt`. The VCF output and the
console output of VEP are not included.
