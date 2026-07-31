#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

source "$script_dir/vepyr_benchmark_env.sh"

smoke_chrom=${SMOKE_CHROM:-chr1}
smoke_start=${SMOKE_START:-1}
smoke_end=${SMOKE_END:-3000000}

input_vcf="$data_vepyr_dir/input/HG002_normalized.vcf.gz"
vep_vcf="$archive_root/$release/merged_fork_scaling/HG002_annotated_wgs_everything_hgvs_merged_forknone.vcf"
smoke_dir="$data_vepyr_dir/input/vepyr_smoke"
smoke_vcf="$smoke_dir/HG002_normalized.${smoke_chrom}_${smoke_start}_${smoke_end}.vcf.gz"
expected_file="$smoke_dir/vep_merged.${smoke_chrom}_${smoke_start}_${smoke_end}.records.txt"
region="${smoke_chrom}:${smoke_start}-${smoke_end}"

require_file() {
  if [[ ! -f "$1" ]]; then
    printf '%s is missing: %s\n' "$2" "$1" >&2
    exit 1
  fi
}

require_file "$input_vcf" 'Input VCF'
require_file "$input_vcf.tbi" 'Input VCF index'
require_file "$vep_vcf" \
  'Ensembl VEP reference output the smoke test compares against'

mkdir -p "$smoke_dir"

if [[ ! -f "$smoke_vcf" ]]; then
  bcftools view \
    --regions "$region" \
    --output-type z \
    --output "$smoke_vcf" \
    "$input_vcf"
fi

if [[ ! -f "$smoke_vcf.tbi" ]]; then
  bcftools index --tbi "$smoke_vcf"
fi

input_records=$(bcftools index --nrecords "$smoke_vcf")
vep_records=$(
  awk \
    -F $'\t' \
    -v chrom="$smoke_chrom" \
    -v start="$smoke_start" \
    -v end="$smoke_end" \
    '
      /^#/ { next }
      $1 == chrom && $2 >= start && $2 <= end { count++; next }
      $1 == chrom && $2 > end { print count; printed = 1; exit }
      END { if (!printed) print count + 0 }
    ' \
    "$vep_vcf"
)

if [[ "$input_records" != "$vep_records" ]]; then
  printf 'Input has %s records but VEP has %s records in %s\n' \
    "$input_records" "$vep_records" "$region" >&2
  exit 1
fi

printf '%s\n' "$vep_records" > "$expected_file"
printf 'smoke_vcf=%s\nexpected_records=%s\n' "$smoke_vcf" "$vep_records"
