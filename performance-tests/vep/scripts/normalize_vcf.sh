#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 INPUT.vcf.gz REFERENCE.fa OUTPUT.normalized.vcf.gz" >&2
  exit 2
fi

input_vcf=$1
reference_fasta=$2
output_vcf=$3

bcftools norm \
  -m -any \
  -f "$reference_fasta" \
  -Oz \
  -o "$output_vcf" \
  "$input_vcf"

bcftools index -t "$output_vcf"

echo "normalized_vcf=$output_vcf"
echo "records=$(bcftools index -n "$output_vcf")"
