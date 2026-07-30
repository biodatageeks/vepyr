#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export OUT_DIR=${OUT_DIR:-/home/tgambin/workspace/vep_data2/116/merged_fork_scaling}

exec "$script_dir/run_vep_fork_scaling.sh" merged "$@"
