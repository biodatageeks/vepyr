#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MODE="${1:-both}"
DATA_DIR="${DATA_DIR:-"$HOME/workspace/data_vepyr"}"
PROFILE="${PROFILE:-merged}"
BACKEND="${BACKEND:-fjall}"
FEATURES="everything.hgvs"

INPUT_VCF="${INPUT_VCF:-"$DATA_DIR/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"}"
VEP_VCF="${VEP_VCF:-"$DATA_DIR/HG002_annotated_wgs_everything_hgvs_${PROFILE}.vcf"}"
CACHE_DIR="${CACHE_DIR:-"$DATA_DIR/115_GRCh38_${PROFILE}"}"
FASTA="${FASTA:-"$DATA_DIR/Homo_sapiens.GRCh38.dna.primary_assembly.fa"}"
OUT_DIR="${OUT_DIR:-"$ROOT/e2e-testing/results/concordance"}"

STEM="${PROFILE}.${FEATURES}.${BACKEND}"
NORM_GZ="$OUT_DIR/input.normalized.vcf.gz"
VEPYR_VCF="$OUT_DIR/vepyr.${STEM}.vcf"
PATCHED_VCF="$OUT_DIR/vepyr.${STEM}.container-patched.vcf"
MD5_REPORT="$OUT_DIR/canonical-md5.${STEM}.txt"
DATAFRAME_REPORT="$OUT_DIR/dataframe.${STEM}.txt"

case "$MODE" in
    md5|dataframe|both) ;;
    *) echo "usage: $0 [md5|dataframe|both]" >&2; exit 2 ;;
esac

mkdir -p "$OUT_DIR"

test -f "$INPUT_VCF"
test -f "$VEP_VCF"
test -d "$CACHE_DIR"
test -f "$FASTA"

NORM_GZ="$("$SCRIPT_DIR/prep/normalize_vcf.sh" "$INPUT_VCF" "$NORM_GZ")"

export UV_CACHE_DIR="${UV_CACHE_DIR:-"$ROOT/.uv-cache"}"

if [ "${FORCE:-0}" = "1" ] || [ ! -s "$VEPYR_VCF" ]; then
    uv run python "$SCRIPT_DIR/prep/run_vepyr_vcf.py" \
        "$NORM_GZ" "$CACHE_DIR" "$FASTA" "$VEPYR_VCF" \
        --backend "$BACKEND" \
        --profile "$PROFILE"
else
    echo "Using existing $VEPYR_VCF"
fi

if [ "$MODE" = "md5" ] || [ "$MODE" = "both" ]; then
    uv run python "$SCRIPT_DIR/md5/patch_vcf_container_for_md5.py" "$NORM_GZ" "$VEPYR_VCF" "$PATCHED_VCF"
    uv run python "$SCRIPT_DIR/md5/canonical_md5_vcf.py" "$VEP_VCF" "$PATCHED_VCF" | tee "$MD5_REPORT"
fi

if [ "$MODE" = "dataframe" ] || [ "$MODE" = "both" ]; then
    uv run python "$SCRIPT_DIR/data_frame/compare_annotation_frames.py" "$VEP_VCF" "$VEPYR_VCF" | tee "$DATAFRAME_REPORT"
fi
