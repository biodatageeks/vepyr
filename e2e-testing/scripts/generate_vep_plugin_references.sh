#!/usr/bin/env bash
# Generate resumable per-autosome VEP 116 five-plugin references.

set -euo pipefail

DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
TARGET="${VEP_PLUGIN_REFERENCE_DIR:-$DATA/output/116/plugins}"
PLUGIN_DIR="${VEP_PLUGIN_DIR:-$TARGET/plugin_code}"
SOURCE_URL="${VEP_PLUGIN_SOURCE_URL:-http://localhost:18080}"
JOBS="${VEP_REFERENCE_JOBS:-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILDER="$SCRIPT_DIR/build_vep_plugin_reference.sh"
WORK_ROOT="$TARGET/.work"
LOG_DIR="$TARGET/logs"

if [[ "$JOBS" -lt 1 ]]; then
  echo "ERROR: VEP_REFERENCE_JOBS must be positive" >&2
  exit 1
fi

mkdir -p "$TARGET" "$WORK_ROOT" "$LOG_DIR"

is_complete() {
  local chrom="$1"
  local output="$TARGET/HG002_chr${chrom}_5plugins_vep116_caddfix.vcf.gz"
  [[ -s "$output" && -s "$output.tbi" ]] || return 1

  local n_plugin
  n_plugin=$(tabix -H "$output" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -cE \
    'SpliceAI_pred|CADD_(RAW|PHRED|raw|phred)|am_(class|pathogenicity)|ClinVar|SIFT4G|Polyphen2|MutationTaster|PROVEAN|VEST4|MetaSVM|MetaLR|REVEL|GERP|phyloP|phastCons')
  [[ "$n_plugin" -eq 38 ]]
}

run_one() {
  local chrom="$1"
  local output="$TARGET/HG002_chr${chrom}_5plugins_vep116_caddfix.vcf"
  local log="$LOG_DIR/chr${chrom}.log"

  if is_complete "$chrom"; then
    echo "SKIP chr${chrom}: validated output already exists"
    return
  fi

  local work
  work=$(mktemp -d "$WORK_ROOT/chr${chrom}.XXXXXX")
  echo "START chr${chrom}: work=$work log=$log"
  if DATA_VEPYR_DIR="$DATA" \
      VEP_PLUGIN_DIR="$PLUGIN_DIR" \
      VEP_PLUGIN_SOURCE_URL="$SOURCE_URL" \
      VEP_OUTPUT_VCF="$output" \
      VEP_KEEP_PLAIN=0 \
      "$BUILDER" "$chrom" "$work" > "$log" 2>&1; then
    if ! is_complete "$chrom"; then
      echo "ERROR chr${chrom}: builder returned success but validation failed" >&2
      return 1
    fi
    rm -rf "$work"
    echo "DONE chr${chrom}: $output.gz"
  else
    echo "ERROR chr${chrom}: see $log (work preserved at $work)" >&2
    return 1
  fi
}

if [[ "$#" -gt 0 ]]; then
  chroms=("$@")
else
  chroms=({1..22})
fi

fail=0
pids=()
labels=()
for chrom in "${chroms[@]}"; do
  run_one "$chrom" &
  pids+=("$!")
  labels+=("$chrom")

  if [[ "${#pids[@]}" -eq "$JOBS" ]]; then
    for i in "${!pids[@]}"; do
      if ! wait "${pids[$i]}"; then
        echo "FAILED chr${labels[$i]}" >&2
        fail=1
      fi
    done
    pids=()
    labels=()
  fi
done

for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    echo "FAILED chr${labels[$i]}" >&2
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "All requested references are complete under $TARGET"
