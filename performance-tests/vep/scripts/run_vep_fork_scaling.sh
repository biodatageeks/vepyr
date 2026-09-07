#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run_vep_fork_scaling.sh <merged|refseq> [fork...]

Examples:
  run_vep_fork_scaling.sh merged
  run_vep_fork_scaling.sh refseq 16 8 4 2 1
  run_vep_fork_scaling.sh refseq none

Environment:
  DATA_VEPYR_DIR  Input/cache root. Default: /home/tgambin/workspace/vep_data
  RELEASE         VEP cache release. Default: 116
  VEP_IMAGE       Docker image. Default: ensemblorg/ensembl-vep:release_116.0
  OUT_DIR         Output directory. Default: DATA_VEPYR_DIR/output/RELEASE/<cache>_fork_scaling
  KEEP_VCFS       Keep VCF outputs after a run. Default: 1
  INPUT_DIR       Directory with the normalized input and reference FASTA.
                  Default: DATA_VEPYR_DIR/input
  TIME_BIN        GNU time executable. Default: gtime on macOS, /usr/bin/time on Linux
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

CACHE_KIND=$1
shift

case "$CACHE_KIND" in
  merged)
    cache_flag=(--merged)
    ;;
  refseq)
    cache_flag=(--refseq)
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

DATA_VEPYR_DIR=${DATA_VEPYR_DIR:-/home/tgambin/workspace/vep_data}
RELEASE=${RELEASE:-116}
VEP_IMAGE=${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.0}
OUT_DIR=${OUT_DIR:-$DATA_VEPYR_DIR/output/$RELEASE/${CACHE_KIND}_fork_scaling}
KEEP_VCFS=${KEEP_VCFS:-1}
if [[ -z "${TIME_BIN:-}" ]]; then
  if [[ "$(uname -s)" == "Darwin" ]]; then
    TIME_BIN=gtime
  else
    TIME_BIN=/usr/bin/time
  fi
fi
if ! command -v "$TIME_BIN" >/dev/null 2>&1; then
  printf 'GNU time not found: %s. On macOS: brew install gnu-time\n' "$TIME_BIN" >&2
  exit 2
fi

if [[ $# -gt 0 ]]; then
  forks=("$@")
else
  forks=(16 8 4 2 1 none)
fi

input_dir="${INPUT_DIR:-$DATA_VEPYR_DIR/input}"
cache_dir="$DATA_VEPYR_DIR/homo_sapiens_${CACHE_KIND}/${RELEASE}_GRCh38"
cache_mount="/opt/vep/.vep/homo_sapiens_${CACHE_KIND}/${RELEASE}_GRCh38"
summary_file="$OUT_DIR/${CACHE_KIND}_fork_scaling_summary.tsv"

mkdir -p "$OUT_DIR"

test -f "$input_dir/HG002_normalized.vcf.gz"
test -f "$input_dir/HG002_normalized.vcf.gz.tbi"
test -f "$input_dir/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
test -f "$input_dir/Homo_sapiens.GRCh38.dna.primary_assembly.fa.fai"
test -f "$cache_dir/chr_synonyms.txt"

docker pull "$VEP_IMAGE"

if [[ ! -f "$summary_file" ]]; then
  printf 'fork\texit_status\telapsed_wall\tmax_rss_kb\toutput_file\ttime_file\tstderr_file\n' > "$summary_file"
fi

docker_uid="$(id -u)"
docker_gid="$(id -g)"
failures=0

for fork in "${forks[@]}"; do
  out_name="HG002_annotated_wgs_everything_hgvs_${CACHE_KIND}_fork${fork}.vcf"
  output_file="$OUT_DIR/$out_name"
  time_file="$OUT_DIR/${CACHE_KIND}_fork${fork}.time.txt"
  stderr_file="$OUT_DIR/${CACHE_KIND}_fork${fork}.stderr.txt"

  fork_args=()
  # macOS Bash 3.2 treats an empty array as unset under nounset. The guarded
  # expansion below preserves zero arguments for the no-fork case.
  if [[ "$fork" != "none" ]]; then
    fork_args=(--fork "$fork")
  fi

  rm -f "$output_file" "$output_file"_warnings.txt "$time_file" "$stderr_file"

  status=0
  printf 'Starting VEP cache=%s release=%s fork=%s\n' "$CACHE_KIND" "$RELEASE" "$fork"
  "$TIME_BIN" -v -o "$time_file" \
  docker run --rm \
    --user "$docker_uid:$docker_gid" \
    --env HOME=/tmp \
    -v "$cache_dir:$cache_mount:ro" \
    -v "$input_dir:/input:ro" \
    -v "$OUT_DIR:/output" \
    "$VEP_IMAGE" \
    vep \
    --dir /opt/vep/.vep \
    --cache \
    "${cache_flag[@]}" \
    --offline \
    --assembly GRCh38 \
    --input_file /input/HG002_normalized.vcf.gz \
    --output_file "/output/$out_name" \
    --vcf \
    --force_overwrite \
    --no_stats \
    --everything --hgvs \
    --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    ${fork_args[@]+"${fork_args[@]}"} \
    2> "$stderr_file" || status=$?

  elapsed_wall=$(awk -F': ' '/Elapsed \(wall clock\) time/ {print $2}' "$time_file" 2>/dev/null || true)
  max_rss_kb=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$time_file" 2>/dev/null || true)

  tmp=$(mktemp "$OUT_DIR/summary.XXXXXX")
  awk -v fork="$fork" 'BEGIN { FS = OFS = "\t" } NR == 1 || $1 != fork { print }' "$summary_file" > "$tmp"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$fork" \
    "$status" \
    "$elapsed_wall" \
    "$max_rss_kb" \
    "$output_file" \
    "$time_file" \
    "$stderr_file" >> "$tmp"
  mv "$tmp" "$summary_file"
  printf 'Finished fork=%s exit=%s elapsed=%s\n' "$fork" "$status" "$elapsed_wall"

  if [[ "$KEEP_VCFS" != "1" ]]; then
    rm -f "$output_file"
  fi

  if [[ "$status" -ne 0 ]]; then
    failures=$((failures + 1))
    printf 'fork=%s failed with exit_status=%s; see %s\n' "$fork" "$status" "$stderr_file" >&2
  fi
done

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
