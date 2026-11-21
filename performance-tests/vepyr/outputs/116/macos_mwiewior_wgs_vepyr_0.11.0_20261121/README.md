# macOS vepyr 0.11.0 merged worker scaling

Whole-genome HG002 GRCh38 measurements from 2026-11-21 with the merged
release-116 cache and 8, 4, 2 and 1 workers. Only vepyr was measured; the
Ensembl VEP times of `performance-tests/vep/outputs/116/macos_mwiewior_wgs_20260914`
remain the reference.

## Environment and inputs

- MacBook Pro, Apple M3 Max (12 performance + 4 efficiency cores), 64 GiB RAM,
  macOS 15.7.9; on battery power during the runs.
- vepyr 0.11.0 installed from the macOS arm64 PyPI wheel, not compiled locally.
- Python 3.12.2, PyArrow 24.0.0, Polars 1.44.2, polars-bio 0.38.0.
- Input: `HG002_normalized.vcf.gz`, 4,096,123 records.
- Reference: `Homo_sapiens.GRCh38.dna.primary_assembly.fa`.
- Cache: the local Parquet conversion of the release-116 merged cache
  (`cache/116_GRCh38_merged`), as in the earlier macOS runs.
- Plain VCF output, `preserve_record_layout=True`.

## Procedure

`run.zsh` calls the unchanged `performance-tests/vepyr/scripts/run_vepyr_worker_scaling.py`
of tag 0.11.0 once per worker count: one discarded run with 8 workers, then 8,
4, 2 and 1 workers. At least 20 GiB of memory were free before the discarded
run (`raw/free_memory.csv`). After each run the record lines were checksummed
per contig (`raw/merged_workers*.md5.json`) and the output was deleted. At
every worker count the 22 contig checksums equal those of the vepyr 0.11.0
output that matches Ensembl VEP 116.0 record for record.

This is the second of two sets measured that day. During the first, other
light commands ran on the host; its times were 1:19.00, 1:44.51, 2:28.97 and
3:47.97 for 8, 4, 2 and 1 workers.

## Results

| Workers | Process wall | Annotation only | Peak RSS |
|---|---|---|---|
| 1 | 3:55.02 | 221.9 s | 4.2 GiB |
| 2 | 2:28.67 | 135.1 s | 6.2 GiB |
| 4 | 1:43.11 | 89.8 s | 6.6 GiB |
| 8 | 1:19.71 | 66.4 s | 8.4 GiB |

The process wall time includes Python start-up, writing the 29.4 GB plain VCF
and the runner's count of the output records.
