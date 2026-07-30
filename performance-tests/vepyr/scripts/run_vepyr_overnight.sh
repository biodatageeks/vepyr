#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
vepyr_dir=$(cd -- "$script_dir/.." && pwd)

data_vepyr_dir=${DATA_VEPYR_DIR:-/home/tgambin/workspace/vep_data}
archive_root=${VEPYR_ARCHIVE_ROOT:-/home/tgambin/workspace/vep_data2}
release=${RELEASE:-116}
vepyr_python=${VEPYR_PYTHON:-/home/tgambin/.pyenv/versions/3.12.8/bin/python3}
cache_folder_id=${VEPYR_REFSEQ_CACHE_FOLDER_ID:-1iOWw4K954iLElLQsMYNs_LWkIMeVxWqn}
cache_dir="$data_vepyr_dir/cache/${release}_GRCh38_refseq"
run_log_dir="$archive_root/$release/vepyr_overnight"
status_file="$run_log_dir/status.txt"
run_log="$run_log_dir/overnight.log"
lock_file="$run_log_dir/overnight.lock"
expected_cache_files=2349
expected_cache_bytes=33009584587

test -x "$vepyr_python"
export VEPYR_PYTHON="$vepyr_python"

mkdir -p "$run_log_dir"
exec 9>"$lock_file"
if ! flock -n 9; then
  printf 'Another Vepyr overnight run already holds %s\n' "$lock_file" >&2
  exit 1
fi

exec > >(tee -a "$run_log") 2>&1
printf '%s\tSTARTED\n' "$(date --iso-8601=seconds)" > "$status_file"
printf 'pid=%s\n' "$$" > "$run_log_dir/pid"

finish() {
  exit_status=$?
  if [[ "$exit_status" -eq 0 ]]; then
    state=COMPLETED
  else
    state=FAILED
  fi
  printf '%s\t%s\texit_status=%s\n' \
    "$(date --iso-8601=seconds)" "$state" "$exit_status" > "$status_file"
}
trap finish EXIT

cache_file_count() {
  find "$cache_dir" -type f ! -name '*.partial' | wc -l
}

cache_byte_count() {
  find "$cache_dir" -type f ! -name '*.partial' -printf '%s\n' |
    awk '{ total += $1 } END { print total + 0 }'
}

cache_is_complete() {
  [[ -d "$cache_dir" ]] &&
    [[ "$(cache_file_count)" -eq "$expected_cache_files" ]] &&
    [[ "$(cache_byte_count)" -eq "$expected_cache_bytes" ]] &&
    ! find "$cache_dir" -type f -name '*.partial' -print -quit | grep -q .
}

printf '%s\tWAITING_FOR_CACHE\n' "$(date --iso-8601=seconds)" > "$status_file"
while pgrep -x rclone >/dev/null; do
  sleep 30
done

if ! cache_is_complete; then
  printf '%s\tRESUMING_CACHE_DOWNLOAD\n' \
    "$(date --iso-8601=seconds)" > "$status_file"
  rclone copy \
    tgambin: \
    "$cache_dir" \
    --drive-root-folder-id "$cache_folder_id" \
    --transfers 8 \
    --checkers 32 \
    --fast-list \
    --order-by size,descending \
    --multi-thread-streams 4 \
    --stats 1m \
    --stats-one-line
fi

if ! cache_is_complete; then
  printf 'RefSeq cache validation failed: files=%s bytes=%s\n' \
    "$(cache_file_count)" "$(cache_byte_count)" >&2
  exit 1
fi

printf '%s\tREFSEQ_SMOKE_TEST\n' "$(date --iso-8601=seconds)" > "$status_file"
"$script_dir/run_vepyr_refseq_smoke_test.sh"

printf '%s\tMERGED_BENCHMARK\n' "$(date --iso-8601=seconds)" > "$status_file"
"$script_dir/run_vepyr_merged_worker_scaling.sh"

printf '%s\tREFSEQ_BENCHMARK\n' "$(date --iso-8601=seconds)" > "$status_file"
"$script_dir/run_vepyr_refseq_worker_scaling.sh"

outputs_dir="$vepyr_dir/outputs/$release"
figures_dir="$outputs_dir/figures"

printf '%s\tCOLLECTING_RESULTS\n' "$(date --iso-8601=seconds)" > "$status_file"
"$script_dir/collect_vepyr_worker_outputs.sh" \
  merged \
  "$archive_root/$release/vepyr_merged_worker_scaling" \
  "$outputs_dir/merged_worker_scaling"
"$script_dir/collect_vepyr_worker_outputs.sh" \
  refseq \
  "$archive_root/$release/vepyr_refseq_worker_scaling" \
  "$outputs_dir/refseq_worker_scaling"

printf '%s\tGENERATING_FIGURES\n' "$(date --iso-8601=seconds)" > "$status_file"
"$vepyr_python" "$script_dir/plot_vepyr_worker_scaling.py" \
  --cache-type merged \
  --summary "$outputs_dir/merged_worker_scaling/summary.tsv" \
  --output "$figures_dir/vepyr_merged_worker_benchmark_wgs.png" \
  --title "Vepyr 0.3.0 merged cache WGS benchmark"
"$vepyr_python" "$script_dir/plot_vepyr_worker_scaling.py" \
  --cache-type refseq \
  --summary "$outputs_dir/refseq_worker_scaling/summary.tsv" \
  --output "$figures_dir/vepyr_refseq_worker_benchmark_wgs.png" \
  --title "Vepyr 0.3.0 refseq cache WGS benchmark"

test -s "$figures_dir/vepyr_merged_worker_benchmark_wgs.png"
test -s "$figures_dir/vepyr_refseq_worker_benchmark_wgs.png"
printf 'Vepyr overnight benchmark completed successfully.\n'
