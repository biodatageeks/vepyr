#!/usr/bin/env bash
set -euo pipefail

INPUT_VCF="$1"
VEP_CACHE_DIR="$2"
FASTA="$3"
OUTPUT_VCF="$4"
PROFILE="${5:-merged}"

VEP_IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_115.2}"
VEP_ARGS=()
if [ -n "${VEP_FORKS:-}" ]; then
    VEP_ARGS+=(--fork "$VEP_FORKS")
fi
if [ "${VEP_COMPRESS_OUTPUT:-0}" = "1" ]; then
    VEP_ARGS+=(--compress_output bgzip)
fi

case "$PROFILE" in
    merged)
        CACHE_NAME="homo_sapiens_merged"
        PROFILE_ARGS=(--merged)
        ;;
    refseq)
        CACHE_NAME="homo_sapiens_refseq"
        PROFILE_ARGS=(--refseq)
        ;;
    vep|ensembl)
        CACHE_NAME="homo_sapiens"
        PROFILE_ARGS=()
        ;;
    *)
        echo "PROFILE must be one of: vep, merged, refseq" >&2
        exit 2
        ;;
esac

mkdir -p "$(dirname "$OUTPUT_VCF")"

INPUT_DIR="$(cd "$(dirname "$INPUT_VCF")" && pwd -P)"
INPUT_BASE="$(basename "$INPUT_VCF")"
OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_VCF")" && pwd -P)"
OUTPUT_BASE="$(basename "$OUTPUT_VCF")"
FASTA_DIR="$(cd "$(dirname "$FASTA")" && pwd -P)"
FASTA_BASE="$(basename "$FASTA")"
VEP_CACHE_DIR="$(cd "$VEP_CACHE_DIR" && pwd -P)"

if [ "$INPUT_DIR" = "$OUTPUT_DIR" ]; then
    IO_MOUNTS=(-v "$INPUT_DIR:/work")
    DOCKER_INPUT="/work/$INPUT_BASE"
    DOCKER_OUTPUT="/work/$OUTPUT_BASE"
else
    IO_MOUNTS=(-v "$INPUT_DIR:/input:ro" -v "$OUTPUT_DIR:/output")
    DOCKER_INPUT="/input/$INPUT_BASE"
    DOCKER_OUTPUT="/output/$OUTPUT_BASE"
fi

echo "Running VEP $PROFILE -> $OUTPUT_VCF"

DOCKER_CMD=(docker run --rm \
    -v "$VEP_CACHE_DIR:/opt/vep/.vep/$CACHE_NAME/115_GRCh38:ro" \
    "${IO_MOUNTS[@]}" \
    -v "$FASTA_DIR:/fasta:ro" \
    "$VEP_IMAGE" \
    vep \
    --dir /opt/vep/.vep \
    --cache \
    "${PROFILE_ARGS[@]}" \
    --offline \
    --assembly GRCh38 \
    --input_file "$DOCKER_INPUT" \
    --output_file "$DOCKER_OUTPUT" \
    --vcf \
    --force_overwrite \
    --no_stats \
    --everything \
    --hgvs \
    --fasta "/fasta/$FASTA_BASE")

if [ "${#VEP_ARGS[@]}" -gt 0 ]; then
    DOCKER_CMD+=("${VEP_ARGS[@]}")
fi

"${DOCKER_CMD[@]}"

if [ "${VEP_COMPRESS_OUTPUT:-0}" = "1" ]; then
    tabix -f -p vcf "$OUTPUT_VCF"
fi
