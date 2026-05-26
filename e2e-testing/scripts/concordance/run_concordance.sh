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
CACHE_DIR="${CACHE_DIR:-"$DATA_DIR/115_GRCh38_${PROFILE}"}"
FASTA="${FASTA:-"$DATA_DIR/Homo_sapiens.GRCh38.dna.primary_assembly.fa"}"
OUT_DIR="${OUT_DIR:-"$ROOT/e2e-testing/results/concordance"}"
DATAFRAME_CHUNK_SIZE="${DATAFRAME_CHUNK_SIZE:-250000}"
DATAFRAME_PROGRESS_EVERY="${DATAFRAME_PROGRESS_EVERY:-1000000}"

STEM="${PROFILE}.${FEATURES}.${BACKEND}"
NORM_GZ="$OUT_DIR/input.decomposed-left-normalized.vcf.gz"
VEP_VCF="${VEP_VCF:-"$OUT_DIR/vep.${STEM}.vcf"}"
VEPYR_VCF="$OUT_DIR/vepyr.${STEM}.vcf"
PATCHED_VCF="$OUT_DIR/vepyr.${STEM}.container-patched.vcf"
MD5_REPORT="$OUT_DIR/canonical-md5.${STEM}.txt"
DATAFRAME_REPORT="$OUT_DIR/dataframe.${STEM}.txt"

case "$PROFILE" in
    merged) DEFAULT_VEP_CACHE_DIR="$DATA_DIR/homo_sapiens_merged/115_GRCh38" ;;
    refseq) DEFAULT_VEP_CACHE_DIR="$DATA_DIR/homo_sapiens_refseq/115_GRCh38" ;;
    vep|ensembl) DEFAULT_VEP_CACHE_DIR="$DATA_DIR/homo_sapiens/115_GRCh38" ;;
    *) echo "PROFILE must be one of: vep, merged, refseq" >&2; exit 2 ;;
esac
VEP_CACHE_DIR="${VEP_CACHE_DIR:-"$DEFAULT_VEP_CACHE_DIR"}"

case "$MODE" in
    md5|dataframe|both) ;;
    *) echo "usage: $0 [md5|dataframe|both]" >&2; exit 2 ;;
esac

mkdir -p "$OUT_DIR"

test -f "$INPUT_VCF"
test -d "$CACHE_DIR"
test -d "$VEP_CACHE_DIR"
test -f "$FASTA"

NORM_GZ="$("$SCRIPT_DIR/prep/normalize_vcf.sh" "$INPUT_VCF" "$NORM_GZ" "$FASTA")"

if [ "${FORCE_VEP:-0}" = "1" ] || [ ! -s "$VEP_VCF" ] || [ "$NORM_GZ" -nt "$VEP_VCF" ]; then
    "$SCRIPT_DIR/prep/run_vep_vcf.sh" \
        "$NORM_GZ" "$VEP_CACHE_DIR" "$FASTA" "$VEP_VCF" "$PROFILE"
else
    echo "Using existing $VEP_VCF"
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-"$ROOT/.uv-cache"}"

if [ "${FORCE:-0}" = "1" ] || [ ! -s "$VEPYR_VCF" ] || [ "$NORM_GZ" -nt "$VEPYR_VCF" ]; then
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
    uv run python "$SCRIPT_DIR/data_frame/compare_annotation_frames.py" \
        "$VEP_VCF" "$VEPYR_VCF" \
        --chunk-size "$DATAFRAME_CHUNK_SIZE" \
        --progress-every "$DATAFRAME_PROGRESS_EVERY" | tee "$DATAFRAME_REPORT"
fi
