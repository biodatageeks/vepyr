#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

source "$script_dir/vepyr_benchmark_env.sh"

expected_records=${VEP_EXPECTED_RECORDS:-4096123}

if [[ $# -gt 0 ]]; then
  workers=("$@")
else
  workers=("${benchmark_workers[@]}")
fi

exec "$vepyr_python" -P "$script_dir/run_vepyr_worker_scaling.py" \
  --input-vcf "$data_vepyr_dir/input/HG002_normalized.vcf.gz" \
  --cache-dir "$data_vepyr_dir/cache/${release}_GRCh38_refseq" \
  --cache-type refseq \
  --reference-fasta "$data_vepyr_dir/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa" \
  --ssd-output-dir "$data_vepyr_dir/output/$release/vepyr_refseq_worker_scaling" \
  --archive-dir "$archive_root/$release/vepyr_refseq_worker_scaling" \
  --expected-records "$expected_records" \
  --compression "$vepyr_compression" \
  ${force_flag:+"$force_flag"} \
  ${require_separate_fs:+"$require_separate_fs"} \
  --workers "${workers[@]}"
