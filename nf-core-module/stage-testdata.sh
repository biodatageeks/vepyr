#!/usr/bin/env bash
# Stage the golden fixture into the layout nf-core/test-datasets expects.
#
# Usage: ./stage-testdata.sh <output-dir>
#
# Copy the resulting data/ tree into a clone of the `modules` branch of
# nf-core/test-datasets and open a PR. The paths produced here are exactly the
# ones modules/nf-core/vepyr/annotate/tests/main.nf.test reads.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <output-dir>" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
golden="${repo_root}/tests/data/golden"
target="$1/data/genomics/homo_sapiens/vepyr"

if [[ ! -d "${golden}/cache" ]]; then
    echo "golden fixture not found at ${golden}/cache" >&2
    exit 1
fi

mkdir -p "${target}"
cp -R "${golden}/cache" "${target}/cache"
cp "${golden}/input.vcf.gz" "${golden}/input.vcf.gz.tbi" "${target}/"
cp "${golden}/reference.fa" "${golden}/reference.fa.fai" "${target}/"

echo "staged to ${target}"
du -sh "${target}"
