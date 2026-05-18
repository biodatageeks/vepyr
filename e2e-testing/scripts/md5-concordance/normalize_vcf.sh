#!/usr/bin/env bash
set -euo pipefail

INPUT_VCF="$1"
OUTPUT_GZ="$2"
OUTPUT_VCF="${OUTPUT_GZ%.gz}"

mkdir -p "$(dirname "$OUTPUT_GZ")"

if [ ! -s "$OUTPUT_GZ" ] || [ ! -s "$OUTPUT_GZ.tbi" ]; then
    bcftools norm -m -both -o "$OUTPUT_VCF" "$INPUT_VCF"
    bgzip -f "$OUTPUT_VCF"
    tabix -p vcf "$OUTPUT_GZ"
fi

echo "$OUTPUT_GZ"
