#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

source "$script_dir/vepyr_benchmark_env.sh"

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
  --compression "$vepyr_compression" \
  ${force_flag:+"$force_flag"} \
  ${require_separate_fs:+"$require_separate_fs"} \
  --workers "${workers[@]}"
