#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
vepyr_dir=$(cd -- "$script_dir/.." && pwd)

source "$script_dir/vepyr_benchmark_env.sh"

# Only needed when the cache still has to be downloaded.
rclone_remote=${VEPYR_RCLONE_REMOTE:-}
cache_folder_id=${VEPYR_REFSEQ_CACHE_FOLDER_ID:-1iOWw4K954iLElLQsMYNs_LWkIMeVxWqn}
cache_dir="$data_vepyr_dir/cache/${release}_GRCh38_refseq"
run_log_dir="$archive_root/$release/vepyr_overnight"
status_file="$run_log_dir/status.txt"
run_log="$run_log_dir/overnight.log"
lock_file="$run_log_dir/overnight.lock"
lock_dir="$run_log_dir/overnight.lock.d"
expected_cache_files=2349
expected_cache_bytes=33009584587

command -v "$vepyr_python" >/dev/null
export VEPYR_PYTHON="$vepyr_python"

timestamp() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

# BSD stat and GNU stat spell the size format differently.
if stat -f %z "$script_dir" >/dev/null 2>&1; then
  stat_size_args=(-f %z)
else
  stat_size_args=(-c %s)
fi

mkdir -p "$run_log_dir"

# flock releases automatically when the process dies, so prefer it and fall
# back to a lock directory only where it is missing, such as macOS.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$lock_file"
  if ! flock -n 9; then
    printf 'Another Vepyr overnight run already holds %s\n' "$lock_file" >&2
    exit 1
  fi
elif mkdir "$lock_dir" 2>/dev/null; then
  lock_dir_held=1
else
  printf 'Another Vepyr overnight run already holds %s\n' "$lock_dir" >&2
  exit 1
fi

exec > >(tee -a "$run_log") 2>&1
printf '%s\tSTARTED\n' "$(timestamp)" > "$status_file"
printf 'pid=%s\n' "$$" > "$run_log_dir/pid"

finish() {
  exit_status=$?
  if [[ "$exit_status" -eq 0 ]]; then
    state=COMPLETED
  else
    state=FAILED
  fi
  printf '%s\t%s\texit_status=%s\n' \
    "$(timestamp)" "$state" "$exit_status" > "$status_file"
  if [[ -n "${lock_dir_held:-}" ]]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
}
trap finish EXIT

cache_file_count() {
  find "$cache_dir" -type f ! -name '*.partial' | wc -l
}

cache_byte_count() {
  find "$cache_dir" -type f ! -name '*.partial' \
    -exec stat "${stat_size_args[@]}" {} + |
    awk '{ total += $1 } END { print total + 0 }'
}

cache_is_complete() {
  [[ -d "$cache_dir" ]] &&
    [[ "$(cache_file_count)" -eq "$expected_cache_files" ]] &&
    [[ "$(cache_byte_count)" -eq "$expected_cache_bytes" ]] &&
    [[ -z "$(find "$cache_dir" -type f -name '*.partial' | head -1)" ]]
}

printf '%s\tWAITING_FOR_CACHE\n' "$(timestamp)" > "$status_file"
while pgrep -x rclone >/dev/null; do
  sleep 30
done

if ! cache_is_complete; then
  if [[ -z "$rclone_remote" ]]; then
    printf 'The cache is incomplete and VEPYR_RCLONE_REMOTE is not set, so it %s\n' \
      "cannot be downloaded; set it to an rclone remote such as myremote:" >&2
    exit 1
  fi
  printf '%s\tRESUMING_CACHE_DOWNLOAD\n' \
    "$(timestamp)" > "$status_file"
  rclone copy \
    "$rclone_remote" \
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

printf '%s\tREFSEQ_SMOKE_TEST\n' "$(timestamp)" > "$status_file"
"$script_dir/run_vepyr_refseq_smoke_test.sh"

printf '%s\tMERGED_BENCHMARK\n' "$(timestamp)" > "$status_file"
"$script_dir/run_vepyr_merged_worker_scaling.sh"

printf '%s\tREFSEQ_BENCHMARK\n' "$(timestamp)" > "$status_file"
"$script_dir/run_vepyr_refseq_worker_scaling.sh"

outputs_dir="$vepyr_dir/outputs/$release"
figures_dir="$outputs_dir/figures"

printf '%s\tCOLLECTING_RESULTS\n' "$(timestamp)" > "$status_file"
"$script_dir/collect_vepyr_worker_outputs.sh" \
  merged \
  "$archive_root/$release/vepyr_merged_worker_scaling" \
  "$outputs_dir/merged_worker_scaling"
"$script_dir/collect_vepyr_worker_outputs.sh" \
  refseq \
  "$archive_root/$release/vepyr_refseq_worker_scaling" \
  "$outputs_dir/refseq_worker_scaling"

printf '%s\tGENERATING_FIGURES\n' "$(timestamp)" > "$status_file"
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
