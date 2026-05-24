#!/usr/bin/env bash
set -euo pipefail

INPUT_VCF="$1"
OUTPUT_GZ="$2"
FASTA="$3"
OUTPUT_VCF="${OUTPUT_GZ%.gz}"
NORM_FASTA="$OUTPUT_GZ.fa"

mkdir -p "$(dirname "$OUTPUT_GZ")"

if [ "${FORCE:-0}" = "1" ] || [ ! -s "$OUTPUT_GZ" ] || [ ! -s "$OUTPUT_GZ.tbi" ]; then
    ln -sf "$FASTA" "$NORM_FASTA"
    awk '{
        print
        if ($1 == "MT") {
            $1 = "chrM"; print
        } else if ($1 ~ /^([0-9]+|X|Y)$/) {
            $1 = "chr" $1; print
        }
    }' "$FASTA.fai" > "$NORM_FASTA.fai"

    bcftools norm -f "$NORM_FASTA" -m -both -o "$OUTPUT_VCF" "$INPUT_VCF"
    bgzip -f "$OUTPUT_VCF"
    tabix -p vcf "$OUTPUT_GZ"
fi

echo "$OUTPUT_GZ"
