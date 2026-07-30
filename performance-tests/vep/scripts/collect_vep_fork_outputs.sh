#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: collect_vep_fork_outputs.sh <merged|refseq> <source-dir> <dest-raw-dir> [fork...]

Copies only lightweight VEP benchmark artifacts:
  <cache>_fork*.time.txt
  <cache>_fork*.stderr.txt
  HG002_annotated_wgs_everything_hgvs_<cache>_fork*.vcf_warnings.txt

Large VCFs, indexes, and HTML stats are never copied.
USAGE
}

if [[ $# -lt 3 ]]; then
  usage >&2
  exit 2
fi

cache_type=$1
source_dir=$2
dest_dir=$3
shift 3

case "$cache_type" in
  merged|refseq)
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if [[ $# -gt 0 ]]; then
  forks=("$@")
else
  forks=(16 8 4 2 1 none)
fi

mkdir -p "$dest_dir"

for fork in "${forks[@]}"; do
  for file in \
    "$source_dir/${cache_type}_fork${fork}.time.txt" \
    "$source_dir/${cache_type}_fork${fork}.stderr.txt" \
    "$source_dir/HG002_annotated_wgs_everything_hgvs_${cache_type}_fork${fork}.vcf_warnings.txt"; do
    if [[ -f "$file" ]]; then
      cp "$file" "$dest_dir/"
    fi
  done
done
