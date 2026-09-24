#!/usr/bin/env bash
# One filter_vep run in the VEP 116 container. Paths must be under $DATA_VEPYR_DIR.
# No --only_matched: the full CSQ is kept, as on the vepyr side.
set -euo pipefail
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.0}"
PANELS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../panels" && pwd)"
in_data() { local p; p="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"; echo "/data/${p#"$(cd "$DATA" && pwd)/"}"; }
exec docker run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
  -v "$DATA:/data" -v "$PANELS:/panels:ro" "$IMAGE" \
  filter_vep --format vcf --force_overwrite \
    --input_file "$(in_data "$1")" --output_file "$(in_data "$2")" --filter "$3"
