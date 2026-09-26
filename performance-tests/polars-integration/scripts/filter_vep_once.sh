#!/usr/bin/env bash
# One filter_vep run in the VEP 116 container. Paths must be under $DATA_VEPYR_DIR.
# No --only_matched: the full CSQ is kept, as on the vepyr side.
set -euo pipefail
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.0}"
PANELS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../panels" && pwd)"
# Sets REPLY to the /data/... path for $1, or exits 1 if $1 resolves outside
# $DATA. Runs in the main shell (not a subshell) so the exit actually stops
# the script -- inside a $(...) command substitution, `exit` would only end
# that subshell and the failure would pass through silently.
in_data() {
  local dir base p data_resolved
  dir="$(cd -P "$(dirname "$1")" && pwd -P)"
  base="$(basename "$1")"
  p="$dir/$base"
  data_resolved="$(cd -P "$DATA" && pwd -P)"
  case "$p" in
    "$data_resolved"/*) REPLY="/data/${p#"$data_resolved"/}" ;;
    *)
      echo "error: '$p' is outside DATA ('$data_resolved')" >&2
      exit 1
      ;;
  esac
}
in_data "$1"; IN="$REPLY"
in_data "$2"; OUT="$REPLY"
exec docker run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
  -v "$DATA:/data" -v "$PANELS:/panels:ro" "$IMAGE" \
  filter_vep --format vcf --force_overwrite \
    --input_file "$IN" --output_file "$OUT" --filter "$3"
