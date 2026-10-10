#!/usr/bin/env bash
# Smoke check of the runner host: image present, staged data consistent, disk.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; repo="$(cd "$here/../.." && pwd)"
data="${1:-/data/vepyr}"
tag="$(cat "$here/IMAGE")"
docker image inspect "$tag" >/dev/null || { echo "image $tag missing: run build-image.sh"; exit 1; }
python3 "$repo/e2e-testing/scripts/wgs_manifest.py" check --data "$data" --manifest "$repo/e2e-testing/golden/wgs-manifest.json"
mkdir -p /scratch/vepyr-wgs
avail=$(df -Pk /scratch/vepyr-wgs | awk 'NR==2 {print $4}')
[ "$avail" -gt $((20*1024*1024)) ] || { echo "less than 20 GB free under /scratch"; exit 1; }
echo "runner OK: $tag, data consistent, $((avail/1024/1024)) GB free"
