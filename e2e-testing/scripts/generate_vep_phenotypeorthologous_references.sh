#!/usr/bin/env bash
# Resumable per-contig VEP 116 PhenotypeOrthologous references (default chr1-22).
#   VEP_REFERENCE_JOBS  concurrent chromosomes (default 2)
set -euo pipefail

DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
TARGET="$DATA/output/116/plugins"
JOBS="${VEP_REFERENCE_JOBS:-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILDER="$SCRIPT_DIR/build_vep_phenotypeorthologous_reference.sh"
WORK_ROOT="$TARGET/.work_po"
LOG_DIR="$TARGET/logs"
mkdir -p "$TARGET" "$WORK_ROOT" "$LOG_DIR"

expected_provenance() {
  sed -n 's/^PLUGIN_SHA256="\([0-9a-f]\{64\}\)"$/PLUGIN PhenotypeOrthologous \1/p' "$BUILDER"
  local url md5 name
  name="$(sed -n 's/^SRC_NAME="\(.*\)"$/\1/p' "$BUILDER")"
  url="$(sed -n 's/^SRC_URL="\(.*\)"$/\1/p' "$BUILDER")"
  md5="$(sed -n 's/^SRC_MD5="\([0-9a-f]\{32\}\)"$/\1/p' "$BUILDER")"
  url="${url//\$SRC_NAME/$name}"
  printf 'SOURCE %s md5=%s\n' "$url" "$md5"
}

is_complete() {
  local out="$TARGET/HG002_chr${1}_phenotypeorthologous_vep116.vcf.gz"
  [[ -s "$out" && -s "$out.tbi" && -s "$out.plugins" ]] || return 1
  local n
  n=$(tabix -H "$out" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -c '^PhenotypeOrthologous_' || true)
  [[ "$n" -eq 4 ]] || return 1
  diff -q <(sort "$out.plugins") <(expected_provenance | sort) >/dev/null
}

run_one() {
  local chrom="$1" log="$LOG_DIR/po_chr${1}.log"
  if is_complete "$chrom"; then echo "SKIP chr${chrom}"; return; fi
  local work
  work=$(mktemp -d "$WORK_ROOT/chr${chrom}.XXXXXX")
  echo "START chr${chrom}: log=$log"
  if "$BUILDER" "$chrom" "$work" > "$log" 2>&1 && is_complete "$chrom"; then
    rm -rf "$work"; echo "DONE chr${chrom}"
  else
    echo "ERROR chr${chrom}: see $log (work kept at $work)" >&2; return 1
  fi
}

if [[ "$#" -gt 0 ]]; then chroms=("$@"); else chroms=({1..22}); fi
fail=0; pids=(); labels=()
for chrom in "${chroms[@]}"; do
  run_one "$chrom" & pids+=("$!"); labels+=("$chrom")
  if [[ "${#pids[@]}" -eq "$JOBS" ]]; then
    for i in "${!pids[@]}"; do wait "${pids[$i]}" || { echo "FAILED chr${labels[$i]}" >&2; fail=1; }; done
    pids=(); labels=()
  fi
done
for i in "${!pids[@]}"; do wait "${pids[$i]}" || { echo "FAILED chr${labels[$i]}" >&2; fail=1; }; done
[[ "$fail" -eq 0 ]] && echo "All requested PhenotypeOrthologous references are complete under $TARGET"
exit "$fail"
