#!/usr/bin/env bash
# Stage the vepyr/annotate test data into the layout nf-core/test-datasets expects.
#
# Usage: ./stage-testdata.sh <output-dir>
#
# Copy the resulting data/ tree into the `modules` branch of nf-core/test-datasets.
# The paths produced here are exactly the ones
# modules/nf-core/vepyr/annotate/tests/main.nf.test reads.
#
# The data -- 1,000 HG002 chr22 records, a bgzip chr22 FASTA region and a
# release-116 cache trimmed to what they read -- is built and verified against
# Ensembl VEP 116 by stage_testdata.py; see its docstring. Requires `uv`, the
# project environment, Git LFS files pulled, and samtools, bgzip and tabix on
# PATH. Safe to rerun against the same output directory.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <output-dir>" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$1"
uv run --project "${repo_root}" python "${repo_root}/nf-core-module/stage_testdata.py" "$(cd "$1" && pwd)"
