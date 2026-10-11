#!/usr/bin/env bash
# Build the /parity-wgs job image on the runner host. Re-run after changing
# the Dockerfile, and bump IMAGE (and parity-wgs.yml's WGS_IMAGE) together.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tag="$(cat "$here/IMAGE")"
docker build --pull -t "$tag" -f "$here/Dockerfile" "$here"
docker run --rm "$tag" bash -c 'python3.12 --version && uv --version && bcftools --version | head -1 && tabix --version | head -1'
echo "built $tag"
