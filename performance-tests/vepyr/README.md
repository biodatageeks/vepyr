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

The benchmark is pinned to `vepyr==0.3.0` and aborts if another version is
imported. The measured call is equivalent to:

```python
lf = vepyr.annotate(
    vcf=input_vcf,
    cache_dir=cache_dir,
    everything=True,
    reference_fasta=reference_fasta,
    workers=workers,
    hgvs=True,
    output_vcf=output_vcf,
)
```

`annotation_seconds` measures only this call. Record counting, summary writes,
and transfer to `vep_data2` happen after that timer has stopped.
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
