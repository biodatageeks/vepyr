#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

vepyr_python=${VEPYR_PYTHON:-python3}
data_vepyr_dir=${DATA_VEPYR_DIR:-/home/tgambin/workspace/vep_data}
archive_root=${VEPYR_ARCHIVE_ROOT:-/home/tgambin/workspace/vep_data2}
release=${RELEASE:-116}
smoke_chrom=${SMOKE_CHROM:-chr1}
smoke_start=${SMOKE_START:-1}
smoke_end=${SMOKE_END:-3000000}

"$script_dir/prepare_vepyr_merged_smoke_input.sh"

smoke_vcf="$data_vepyr_dir/input/vepyr_smoke/HG002_normalized.${smoke_chrom}_${smoke_start}_${smoke_end}.vcf.gz"
expected_file="$data_vepyr_dir/input/vepyr_smoke/vep_merged.${smoke_chrom}_${smoke_start}_${smoke_end}.records.txt"
expected_records=$(<"$expected_file")

if [[ $# -gt 0 ]]; then
  workers=("$@")
else
  workers=(2)
fi

exec "$vepyr_python" -P "$script_dir/run_vepyr_worker_scaling.py" \
  --input-vcf "$smoke_vcf" \
  --cache-dir "$data_vepyr_dir/cache/${release}_GRCh38_refseq" \
  --cache-type refseq \
  --reference-fasta "$data_vepyr_dir/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa" \
  --ssd-output-dir "$data_vepyr_dir/output/$release/vepyr_refseq_worker_scaling_smoke" \
  --archive-dir "$archive_root/$release/vepyr_refseq_worker_scaling_smoke" \
  --expected-records "$expected_records" \
  --name-prefix "HG002_${smoke_chrom}_${smoke_start}_${smoke_end}" \
  --workers "${workers[@]}"
