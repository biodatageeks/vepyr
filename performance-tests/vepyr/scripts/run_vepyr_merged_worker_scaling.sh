#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

vepyr_python=${VEPYR_PYTHON:-python3}
data_vepyr_dir=${DATA_VEPYR_DIR:-/home/tgambin/workspace/vep_data}
archive_root=${VEPYR_ARCHIVE_ROOT:-/home/tgambin/workspace/vep_data2}
release=${RELEASE:-116}
expected_records=${VEP_EXPECTED_RECORDS:-4096123}

if [[ $# -gt 0 ]]; then
  workers=("$@")
else
  workers=(16 8 4 2 1)
fi

exec "$vepyr_python" -P "$script_dir/run_vepyr_worker_scaling.py" \
  --input-vcf "$data_vepyr_dir/input/HG002_normalized.vcf.gz" \
  --cache-dir "$data_vepyr_dir/cache/${release}_GRCh38_merged" \
  --cache-type merged \
  --reference-fasta "$data_vepyr_dir/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa" \
  --ssd-output-dir "$data_vepyr_dir/output/$release/vepyr_merged_worker_scaling" \
  --archive-dir "$archive_root/$release/vepyr_merged_worker_scaling" \
  --expected-records "$expected_records" \
  --workers "${workers[@]}"
