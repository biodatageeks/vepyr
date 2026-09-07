#!/usr/bin/env bash
# Stage the golden fixture into the layout nf-core/test-datasets expects.
#
# Usage: ./stage-testdata.sh <output-dir>
#
# Copy the resulting data/ tree into a clone of the `modules` branch of
# nf-core/test-datasets and open a PR. The paths produced here are exactly the
# ones modules/nf-core/vepyr/annotate/tests/main.nf.test reads.
#
# Requires `uv` and the project environment, because the cache cannot simply be
# copied -- see below. Safe to rerun against the same output directory.
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

# The checked-in Parquet shards carry no bio.vep.cache_version /
# bio.vep.cache_source_type, and the engine refuses a metadata-less cache:
#   cache identity validation failed ... missing bio.vep.cache_version
# So stamp the identity while copying, exactly as the pytest fixtures do.
# The helper rewrites the shards with the parquet-rs writer the engine reads
# with, and clears any existing target first, which keeps reruns idempotent.
uv run --project "${repo_root}" python - "${golden}/cache" "${target}/cache" "${repo_root}" <<'PY'
import sys
from pathlib import Path

source, dest, repo_root = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path.insert(0, repo_root)
from tests.cache_metadata import copy_cache_with_source_metadata

out = copy_cache_with_source_metadata(source, Path(dest), "ensembl", "115")
print(f"stamped cache -> {out}")
PY

cp "${golden}/input.vcf.gz" "${golden}/input.vcf.gz.tbi" "${target}/"
cp "${golden}/reference.fa" "${golden}/reference.fa.fai" "${target}/"

echo "staged to ${target}"
du -sh "${target}"
