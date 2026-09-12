#!/usr/bin/env bash
# Build the PhenotypeOrthologous shard(s), compare vepyr with the VEP 116
# reference through run_comparison.py --profile merged_phenotypeorthologous,
# and gate each chromosome on strict body/header md5 concordance.
#
# Usage: ./run_phenotypeorthologous_comparison.sh [chroms...]   (default 1..22)
#   DATA_VEPYR_DIR          data root (default ~/workspace/data_vepyr)
#   VEPYR_PLUGIN_REPO       local vepyr-plugins clone (default ~/research/git/vepyr-plugins)
#   VEPYR_PLUGIN_REF        git ref of the manifest to build from (default v0.2.0)
#   VEPYR_PLUGIN_CACHE      plugin cache root (default $DATA/plugin_cache_po_<ref>)
#   VEP_COMPARISON_WORKERS  vepyr workers (default 4)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
export DATA_VEPYR_DIR="$DATA"
PLUGIN_REPO="${VEPYR_PLUGIN_REPO:-$HOME/research/git/vepyr-plugins}"
PLUGIN_REF="${VEPYR_PLUGIN_REF:-v0.2.0}"
PLUGIN_CACHE="${VEPYR_PLUGIN_CACHE:-$DATA/plugin_cache_po_${PLUGIN_REF//\//_}}"
CACHE_DIR="$DATA/cache/116_GRCh38_merged"
SRC="$DATA/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
REF_DIR="$DATA/output/116/plugins"
LOG_DIR="$REF_DIR/po_comparison_logs"
WORKERS="${VEP_COMPARISON_WORKERS:-4}"
BUILDER="$SCRIPT_DIR/build_vep_phenotypeorthologous_reference.sh"
GENERATOR="$SCRIPT_DIR/generate_vep_phenotypeorthologous_references.sh"
mkdir -p "$LOG_DIR"

if [[ "$#" -gt 0 ]]; then chroms=("$@"); else chroms=({1..22}); fi

# One build call for every requested chromosome: the builder verifies the
# source once and stages all shards before committing any of them.
build_shards() {
  local list; list="$(printf '"%s",' "${chroms[@]}")"
  (cd "$REPO_DIR" && uv run python - <<PYBUILD
import vepyr
print(vepyr.build_plugin_cache(
    "phenotypeorthologous", "$PLUGIN_REF",
    source_path="$SRC", cache_dir="$CACHE_DIR", plugin_cache_root="$PLUGIN_CACHE",
    chroms=[${list%,}], plugins_repo="$PLUGIN_REPO", overwrite=True, verify_source="strict"))
PYBUILD
  )
}

fail=0
[[ -s "$SRC" ]] || "$BUILDER" 22 >/dev/null   # the builder downloads and md5-checks the source
build_shards | tee "$LOG_DIR/build.log"
for chrom in "${chroms[@]}"; do
  # The generator validates an existing reference against the builder's pinned
  # plugin sha256 and source md5 (its `.plugins` sidecar) and the four-field
  # header, rebuilding a stale one, so a reference from an older plugin file or
  # source can never be compared against the freshly built cache.
  if ! VEP_REFERENCE_JOBS=1 "$GENERATOR" "$chrom" > "$LOG_DIR/reference_chr${chrom}.log" 2>&1; then
    echo "FAIL chr${chrom}: reference build/validation failed, see $LOG_DIR/reference_chr${chrom}.log" >&2; fail=1; continue
  fi
  if ! (cd "$REPO_DIR" && uv run python e2e-testing/scripts/run_comparison.py \
      --release 116 --profile merged_phenotypeorthologous --chroms "$chrom" \
      --plugin-cache "$PLUGIN_CACHE" --workers "$WORKERS" --bgzf --force) \
      > "$LOG_DIR/compare_chr${chrom}.log" 2>&1; then
    echo "FAIL chr${chrom}: comparison error, see $LOG_DIR/compare_chr${chrom}.log" >&2; fail=1; continue
  fi
  results="$REPO_DIR/e2e-testing/results/116/fast_chr${chrom}"
  if (cd "$REPO_DIR" && uv run python e2e-testing/scripts/md5_concordance.py \
        --pair "$results/vep_chr${chrom}_merged_phenotypeorthologous.vcf" \
               "$results/vepyr_parquet_chr${chrom}_merged_phenotypeorthologous.vcf.gz" \
        --mode strict --explain --explain-limit 0) > "$LOG_DIR/strict_chr${chrom}.log" 2>&1; then
    echo 0 > "$LOG_DIR/strict_chr${chrom}.exit"; echo "PASS chr${chrom}: strict md5 concordant"
  else
    echo 1 > "$LOG_DIR/strict_chr${chrom}.exit"; echo "FAIL chr${chrom}: see $LOG_DIR/strict_chr${chrom}.log" >&2; fail=1
  fi
done
exit "$fail"
