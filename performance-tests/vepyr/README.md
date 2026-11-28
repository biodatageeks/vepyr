# Vepyr performance tests

This directory contains reproducible worker-scaling benchmarks for the PyPI
release of Vepyr. Benchmark data and generated VCF files stay outside the
repository.

## Merged WGS benchmark

The default paths keep every measured read and write on the SSD:

- input: `/home/tgambin/workspace/vep_data/input/HG002_normalized.vcf.gz`
- cache: `/home/tgambin/workspace/vep_data/cache/116_GRCh38_merged`
- reference: `/home/tgambin/workspace/vep_data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa`
- measured output: `/home/tgambin/workspace/vep_data/output/116/vepyr_merged_worker_scaling`

After each measured call returns, the VCF and its logs are moved to:

`/home/tgambin/workspace/vep_data2/116/vepyr_merged_worker_scaling`

Run the full worker sequence, in descending order:

```bash
performance-tests/vepyr/scripts/run_vepyr_merged_worker_scaling.sh
```

The benchmark aborts unless the imported vepyr matches the version this
checkout declares (override with `VEPYR_EXPECTED_VERSION`). The measured call is equivalent to:

```python
lf = vepyr.annotate(
    vcf=input_vcf,
    cache_dir=cache_dir,
    reference_fasta=reference_fasta,
    workers=workers,
    output_vcf=output_vcf,
)
```

### Timing definition

Two times are recorded for every worker count:

- `annotation_seconds` (metrics JSON and `summary.tsv`) measures only the
  `vepyr.annotate()` call inside the benchmark process.
- `process_elapsed_wall` (`summary.tsv`, and the `*.time.txt` record in GNU
  time's layout) measures the whole `benchmark_vepyr_once.py` process: Python
  start-up, importing vepyr, the annotation and writing the output VCF. This
  is the same work the engine-comparison harness times, which runs
  `python -c 'import vepyr; vepyr.annotate(...)'`.

The record count that verifies the output reads the whole VCF back (29.4 GB
for the plain HG002 output). It runs in `run_vepyr_worker_scaling.py` after
the benchmark process has exited and is not timed; it sets `output_records`,
`records_match_vep`, `output_bytes` and `status` (`ok` or
`record_count_mismatch`; a mismatch stops the sweep and the runner exits 1).
Run on its own, `benchmark_vepyr_once.py` leaves `status` at `annotated`.
Summary writes and the transfer to `vep_data2` are not timed either.

Result sets measured before this definition counted the records inside the
benchmark process, so their `process_elapsed_wall` includes reading the
output back (at eight workers on the M3 Max, 79.7 s process wall against
66.4 s of annotation) and is not comparable with the engine-comparison
harness; their
`annotation_seconds` is unaffected. That applies to
`outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261121` and
`outputs/116/linux_tgambin_wgs_vepyr_0.11.0_20261121`, and to the result sets
of earlier vepyr versions, which are kept on the branch `archive-pre-0.11.0`.
The sets `macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host` and
`linux_tgambin_wgs_vepyr_0.11.0_pr293` use this definition.

### Power source

Each run records `power_source` (`AC Power`, `Battery Power` or `unknown`) in
its metrics JSON and in `summary.tsv`: on macOS from the first line of
`pmset -g batt`, on Linux from `/sys/class/power_supply/*/online` (`unknown`
when the host exposes no power supply, as most servers do). A laptop on
battery is slower, so pass `--require-ac-power` to refuse to start a run on
battery. Result sets measured before this field existed do not record it;
`macos_mwiewior_wgs_vepyr_0.11.0_20261121` was measured on battery power.

Before every worker, the runner requires at least `40 GiB` of free SSD space.

The default expected full-WGS record count is `4,096,123`. It can be overridden
without changing the script:

```bash
VEP_EXPECTED_RECORDS=4096123 \
  performance-tests/vepyr/scripts/run_vepyr_merged_worker_scaling.sh
```

Selected workers can be run explicitly:

```bash
performance-tests/vepyr/scripts/run_vepyr_merged_worker_scaling.sh 8 4
```

## RefSeq WGS benchmark

The RefSeq benchmark uses the cache at
`/home/tgambin/workspace/vep_data/cache/116_GRCh38_refseq`, writes each measured
VCF on the SSD, and archives it immediately after the measurement under
`/home/tgambin/workspace/vep_data2/116/vepyr_refseq_worker_scaling`.

Run the full worker sequence after the merged benchmark has finished:

```bash
performance-tests/vepyr/scripts/run_vepyr_refseq_worker_scaling.sh
```

Run a small RefSeq cache smoke test before the full WGS benchmark:

```bash
performance-tests/vepyr/scripts/run_vepyr_refseq_smoke_test.sh
```

Selected workers can be run explicitly:

```bash
performance-tests/vepyr/scripts/run_vepyr_refseq_worker_scaling.sh 8 4
```

## Small merged smoke test

The smoke test uses `chr1:1-3000000` (2,799 variants in the current input). It
derives the expected record count from the completed VEP merged VCF and runs
Vepyr with workers `2` and `1`:

```bash
performance-tests/vepyr/scripts/run_vepyr_merged_smoke_test.sh
```

Its measured files are first written under `vep_data` and then archived under
`/home/tgambin/workspace/vep_data2/116/vepyr_merged_worker_scaling_smoke`.

## Unattended overnight run

The overnight runner waits for an active `rclone` process, validates the RefSeq
cache, resumes its download if needed, runs the RefSeq smoke test, then runs the
full merged and RefSeq benchmarks. It finally copies only lightweight artifacts
into this directory and generates both Vepyr scalability figures:

```bash
performance-tests/vepyr/scripts/run_vepyr_overnight.sh
```

Runtime status and the complete log are written to:

`/home/tgambin/workspace/vep_data2/116/vepyr_overnight`
