#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: collect_vepyr_worker_outputs.sh <merged|refseq> <source-dir> <output-dir>

Copies summary and lightweight per-worker artifacts. VCF files are never copied.
USAGE
}

if [[ $# -ne 3 ]]; then
  usage >&2
  exit 2
fi

cache_type=$1
source_dir=$2
output_dir=$3

case "$cache_type" in
  merged|refseq) ;;
  *)
    usage >&2
    exit 2
    ;;
esac

test -f "$source_dir/summary.tsv"
mkdir -p "$output_dir/raw"
cp -p "$source_dir/summary.tsv" "$output_dir/summary.tsv"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
benchmark_workers=(
  $(grep -v '^[[:space:]]*#' "$script_dir/benchmark_workers.txt")
)

for workers in "${benchmark_workers[@]}"; do
  for suffix in metrics.json time.txt stderr.txt stdout.txt; do
    source_file="$source_dir/${cache_type}_workers${workers}.${suffix}"
    test -f "$source_file"
    cp -p "$source_file" "$output_dir/raw/"
  done
done
