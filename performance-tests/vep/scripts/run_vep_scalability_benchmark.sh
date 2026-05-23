#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=${DATA_DIR:-/home/tgambin/workspace/vep_data2}
OUT_DIR=${OUT_DIR:-$DATA_DIR/vep_benchmark_wgs}
IMAGE=${IMAGE:-ensemblorg/ensembl-vep:release_115.1}
INPUT_VCF=${INPUT_VCF:-$DATA_DIR/HG002_GRCh38_1_22_v4.2.1_benchmark.normalized.vcf.gz}
FASTA=${FASTA:-$DATA_DIR/Homo_sapiens.GRCh38.dna.primary_assembly.fa}
BUFFER_SIZE=${BUFFER_SIZE:-20000}
SPECIES=${SPECIES:-homo_sapiens}
ASSEMBLY=${ASSEMBLY:-GRCh38}
CACHE_VERSION=${CACHE_VERSION:-115}
KEEP_OUTPUTS=${KEEP_OUTPUTS:-0}

if [[ $# -gt 0 ]]; then
  forks=("$@")
else
  forks=(none 1 2 4 8 16)
fi

mkdir -p "$OUT_DIR"
summary="$OUT_DIR/summary.tsv"

if [[ ! -f "$summary" ]]; then
  printf 'fork\tinput_records\toutput_records\tstatus\telapsed\tmax_rss_kb\toutput_size_bytes\n' > "$summary"
fi

if command -v bcftools >/dev/null 2>&1 && [[ -f "${INPUT_VCF}.tbi" ]]; then
  input_records=$(bcftools index -n "$INPUT_VCF")
else
  input_records=$(zgrep -vc '^#' "$INPUT_VCF")
fi

for fork in "${forks[@]}"; do
  output="$OUT_DIR/vep_merged_buffer${BUFFER_SIZE}_wgs_fork_${fork}.vcf"
  runlog="$OUT_DIR/vep_merged_buffer${BUFFER_SIZE}_wgs_fork_${fork}.run.log"
  timelog="$OUT_DIR/vep_merged_buffer${BUFFER_SIZE}_wgs_fork_${fork}.time.txt"

  fork_args=()
  if [[ "$fork" != "none" ]]; then
    fork_args=(--fork "$fork")
  fi

  rm -f "$output" "$output"_summary.html "$output"_warnings.txt "$runlog" "$timelog"

  echo "=== $(date -Is) fork=$fork input_records=$input_records ===" | tee "$runlog"

  /usr/bin/time -v -o "$timelog" docker run --rm \
    -v "$DATA_DIR:/cache:ro" \
    -v "$OUT_DIR:/out" \
    "$IMAGE" vep \
    --input_file "/cache/$(basename "$INPUT_VCF")" \
    --output_file "/out/$(basename "$output")" \
    --offline --cache --merged --dir_cache /cache \
    --species "$SPECIES" --assembly "$ASSEMBLY" --cache_version "$CACHE_VERSION" \
    --fasta "/cache/$(basename "$FASTA")" \
    --format vcf --vcf --everything --buffer_size "$BUFFER_SIZE" \
    "${fork_args[@]}" \
    --force_overwrite --no_stats 2>&1 | tee -a "$runlog"

  output_records=$(grep -vc '^#' "$output")
  status=OK
  if [[ "$output_records" != "$input_records" ]]; then
    status=COUNT_MISMATCH
  fi

  elapsed=$(awk -F': ' '/Elapsed/ {print $2}' "$timelog")
  max_rss_kb=$(awk -F': ' '/Maximum resident set size/ {print $2}' "$timelog")
  output_size=$(stat -c%s "$output")

  tmp=$(mktemp "$OUT_DIR/summary.XXXXXX")
  awk -v fork="$fork" 'BEGIN{FS=OFS="\t"} NR==1 || $1 != fork {print}' "$summary" > "$tmp"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$fork" "$input_records" "$output_records" "$status" "$elapsed" "$max_rss_kb" "$output_size" >> "$tmp"
  mv "$tmp" "$summary"

  echo "=== $(date -Is) fork=$fork done output_records=$output_records status=$status output_size=$output_size ===" | tee -a "$runlog"

  if [[ "$KEEP_OUTPUTS" != "1" ]]; then
    rm -f "$output"
  fi
done
