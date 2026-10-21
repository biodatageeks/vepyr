#!/usr/bin/env bash
# Build the Ensembl VEP 116.0 merged-cache reference for a ClinVar GRCh38 VCF.
#
# Uses the flags of the HG002 golden runs (e2e-testing/vep-docker.md). VEP runs
# without --fork, as one process per contig, and VEP_REFERENCE_JOBS contigs run
# at once. The per-contig outputs are joined, unchanged, into one indexed VCF
# that run_comparison.py takes through --vep. Preprocessing is described in
# e2e-testing/golden/116/clinvar/README.md.
#
# Usage:
#   ./generate_vep_clinvar_reference.sh            # all 25 contigs
#   ./generate_vep_clinvar_reference.sh 21 22      # selected contigs only
#
# Requires: docker, curl, bcftools, bgzip, tabix and samtools. Whatever is
# missing under $DATA_VEPYR_DIR is fetched first: the Ensembl 116 FASTA, the
# raw VEP 116 merged cache, the Docker image and the ClinVar release. A VEP
# process usually grows to 2.5-4.5 GiB, but chr17 reached 22.4 GiB. The default
# is one contig; allow at least 30 GiB of Docker memory for it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PINS="$SCRIPT_DIR/../../porting-tests/PINS.toml"
export SCRIPT_DIR
reference_state() {
  PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m comparison.clinvar_reference_state "$@"
}

DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
CLINVAR_RELEASE="${CLINVAR_RELEASE:-20261011}"
JOBS="${VEP_REFERENCE_JOBS:-1}"
FORCE_REFERENCE="${FORCE_REFERENCE:-0}"
if [[ "$CLINVAR_RELEASE" != "20261011" ]]; then
  echo "ERROR: this reference profile supports CLINVAR_RELEASE=20261011 only" >&2
  exit 1
fi
if [[ ! "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: VEP_REFERENCE_JOBS must be a positive integer (got '$JOBS')" >&2
  exit 1
fi
# Keep the image immutable and aligned with vep_image in the ClinVar manifest.
IMAGE="ensemblorg/ensembl-vep:release_116.0@sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11"
CACHE="$DATA/homo_sapiens_merged/116_GRCh38"
CACHE_COMPLETE="$CACHE/.clinvar-profile-cache-complete"
EXPECTED_CACHE_SUM="26554 29618744"
IN_DIR="$DATA/input/clinvar"
OUT_DIR="$DATA/output/116/clinvar"
NAME="clinvar_${CLINVAR_RELEASE}"
URL="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38"
ENSEMBL="https://ftp.ensembl.org/pub/release-116"
# Largest first, so the longest jobs do not start last.
ALL_CONTIGS=(1 2 17 11 19 3 16 7 5 10 12 6 9 15 X 4 8 14 20 22 13 18 21 MT Y)
if [[ "$#" -gt 0 ]]; then contigs=("$@"); else contigs=("${ALL_CONTIGS[@]}"); fi
reference_state validate-contigs "${contigs[@]}"

FASTA="${FASTA:-$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa}"
if [[ ! -f "$FASTA" && -f "$DATA/$(basename "$FASTA")" ]]; then
  FASTA="$DATA/$(basename "$FASTA")"   # legacy layout, as in profiles.py
fi
mkdir -p "$DATA" "$IN_DIR" "$OUT_DIR/contigs" "$OUT_DIR/logs"

cache_complete() {
  [[ -s "$CACHE/info.txt" && -f "$CACHE_COMPLETE" ]] \
    && [[ "$(cat "$CACHE_COMPLETE")" == "$EXPECTED_CACHE_SUM" ]]
}

CACHE_STAGE=""
cleanup_cache_stage() {
  if [[ -n "$CACHE_STAGE" ]]; then
    rm -rf "$CACHE_STAGE"
  fi
}
trap cleanup_cache_stage EXIT

# Remove abandoned VEP work directories from prior runs. A contig process is
# never expected to need a full day; recent directories may belong to a live run.
find "$OUT_DIR/contigs" -mindepth 1 -maxdepth 1 -type d -name '.chr*' \
  -mmin +1440 -exec rm -rf {} +

# 0. Check the disk before anything is downloaded. Counted in GiB, only for
#    what is missing; the VEP outputs include the plain VCFs of running jobs.
need=40
cache_complete || need=$((need + 60))
[[ -f "$FASTA" ]] || need=$((need + 4))
free=$(df -Pk "$DATA" | awk 'NR == 2 {print int($4 / 1048576)}')
if (( free < need )); then
  echo "ERROR: $free GiB free under $DATA, this run needs about $need GiB" >&2
  exit 1
fi
echo "Disk: $free GiB free under $DATA, about $need GiB needed"

# Fetch the FASTA, the raw VEP cache and the image when they are missing.
if [[ ! -f "$FASTA" ]]; then
  curl -sSfL "$ENSEMBL/fasta/homo_sapiens/dna/$(basename "$FASTA").gz" | gzip -dc > "$FASTA.tmp"
  mv -f "$FASTA.tmp" "$FASTA"
fi
FASTA_SHA256="$(reference_state validate-fasta "$FASTA" "$PINS")"
if ! cache_complete; then
  tarball="$DATA/homo_sapiens_merged_vep_116_GRCh38.tar.gz"
  url="$ENSEMBL/variation/indexed_vep_cache/$(basename "$tarball")"
  # Ensembl serves each connection at about 5 MB/s; aria2c opens several.
  if command -v aria2c > /dev/null; then
    aria2c -q -c -x 8 -s 8 -d "$DATA" -o "$(basename "$tarball")" "$url"
  else
    curl -sSfL -C - -o "$tarball" "$url"
  fi
  # The BSD sum published in the release's CHECKSUMS file.
  reference_state validate-cache-archive "$tarball" "$EXPECTED_CACHE_SUM"
  CACHE_STAGE=$(mktemp -d "$DATA/.clinvar-cache-stage.XXXXXX")
  tar -xzf "$tarball" -C "$CACHE_STAGE"
  staged_cache="$CACHE_STAGE/homo_sapiens_merged/116_GRCh38"
  [[ -s "$staged_cache/info.txt" ]] \
    || { echo "ERROR: archive did not contain a complete VEP cache at $staged_cache" >&2; exit 1; }
  printf '%s\n' "$EXPECTED_CACHE_SUM" > "$staged_cache/.clinvar-profile-cache-complete"
  rm -rf "$CACHE"
  mkdir -p "$(dirname "$CACHE")"
  mv "$staged_cache" "$CACHE"
  cleanup_cache_stage
  CACHE_STAGE=""
  rm -f "$tarball"
fi
# Docker creates a missing -v source as an empty directory, and VEP then skips
# every variant instead of failing (see vep-docker.md).
cache_complete \
  || { echo "ERROR: no complete VEP cache at $CACHE" >&2; exit 1; }
CACHE_INFO_SHA256="$(reference_state sha256 "$CACHE/info.txt")"
CACHE_IDENTITY="$EXPECTED_CACHE_SUM:$CACHE_INFO_SHA256"
docker image inspect "$IMAGE" > /dev/null 2>&1 || docker pull "$IMAGE"

# 1. Download and verify the ClinVar release.
RAW="$IN_DIR/$NAME.vcf.gz"
EXPECTED_RAW_MD5="0f3a1a5e84252386033d5fcab1bf1f6a"
raw_md5() {
  if command -v md5sum > /dev/null; then md5sum < "$1" | cut -d' ' -f1; else md5 -q "$1"; fi
}
if [[ -s "$RAW" && "$(raw_md5 "$RAW")" != "$EXPECTED_RAW_MD5" ]]; then
  echo "Existing ClinVar download is incomplete or changed; replacing it" >&2
  rm -f "$RAW" "$RAW.tbi"
fi
if [[ ! -s "$RAW" ]]; then
  rm -f "$RAW.tmp"
  curl -sSfL -o "$RAW.tmp" "$URL/$NAME.vcf.gz"
  [[ "$(raw_md5 "$RAW.tmp")" == "$EXPECTED_RAW_MD5" ]] \
    || { echo "ERROR: md5 mismatch for downloaded ClinVar VCF" >&2; rm -f "$RAW.tmp"; exit 1; }
  mv -f "$RAW.tmp" "$RAW"
fi
[[ "$(raw_md5 "$RAW")" == "$EXPECTED_RAW_MD5" ]] \
  || { echo "ERROR: md5 mismatch for $RAW" >&2; rm -f "$RAW.tbi"; exit 1; }
# Build the source index locally. This also replaces any partial or stale index
# left behind by an interrupted earlier run.
tabix -f -p vcf "$RAW"

# 2. Preprocess: drop ALT="." sites and records outside 1-22, X, Y, MT, then
#    split multiallelic records exactly as the HG002 input was split.
NORM="$IN_DIR/$NAME.norm.vcf.gz"
EXPECTED_BODY_MD5="547285cb0e2b3f97c7222156a748c371"
vcf_body_md5() {
  if command -v md5sum > /dev/null; then
    bcftools view -H "$1" | md5sum | cut -d' ' -f1
  else
    bcftools view -H "$1" | md5 -q
  fi
}
rebuild_norm=false
if [[ ! -s "$NORM" || ! -s "$NORM.tbi" ]]; then
  rebuild_norm=true
elif [[ "$(vcf_body_md5 "$NORM")" != "$EXPECTED_BODY_MD5" ]]; then
  echo "Existing preprocessed ClinVar records have the wrong body MD5; rebuilding" >&2
  rebuild_norm=true
fi
if [[ "$rebuild_norm" == true ]]; then
  rm -f "$NORM" "$NORM.tbi" "$NORM.tmp" "$NORM.tmp.tbi"
  region_list=$(IFS=,; echo "${ALL_CONTIGS[*]}")
  bcftools view -e 'ALT="."' -t "$region_list" "$RAW" -Ou \
    | bcftools norm -m -both -Oz -o "$NORM.tmp"
  tabix -f -p vcf "$NORM.tmp"
  mv -f "$NORM.tmp.tbi" "$NORM.tbi"
  mv -f "$NORM.tmp" "$NORM"
fi
[[ "$(vcf_body_md5 "$NORM")" == "$EXPECTED_BODY_MD5" ]] \
  || { echo "ERROR: preprocessed ClinVar body MD5 mismatch for $NORM" >&2; rm -f "$NORM" "$NORM.tbi"; exit 1; }
reference_state index-vcf "$NORM"
INPUT_BODY_MD5="$(vcf_body_md5 "$NORM")"

VEP_ARGS=(
  vep
  --dir /opt/vep/.vep
  --cache
  --offline
  --assembly GRCh38
  --input_file /work/input.vcf.gz
  --output_file /work/output.vcf
  --vcf
  --force_overwrite
  --no_stats
  --everything
  --hgvs
  --fasta "/ref/$(basename "$FASTA")"
  --merged
)
VEP_ARG_LINES="$(printf '%s\n' "${VEP_ARGS[@]}")"
PROVENANCE_ARGS=()
for arg in "${VEP_ARGS[@]}"; do PROVENANCE_ARGS+=("--vep-arg=$arg"); done
EXPECTED_PROVENANCE="$(reference_state provenance \
  "$FASTA_SHA256" "$CACHE_IDENTITY" "$IMAGE" "$INPUT_BODY_MD5" \
  "${PROVENANCE_ARGS[@]}")"

# 3. One VEP process per contig.
run_one() {
  local chrom="$1"
  local out="$OUT_DIR/contigs/${NAME}_chr${chrom}_vep116_merged.vcf"
  local log="$OUT_DIR/logs/chr${chrom}.log"
  local stamp="$out.gz.provenance"
  if reference_state contig-current \
    "$out.gz" "$out.gz.tbi" "$stamp" "$EXPECTED_PROVENANCE"; then
    echo "SKIP chr${chrom}: provenance matches"
    return
  fi
  if reference_state remove-incomplete-contig "$out.gz" "$out.gz.tbi" "$stamp"; then
    echo "REBUILD chr${chrom}: removed interrupted VCF/index publication"
  fi
  if [[ -e "$out.gz" || -e "$out.gz.tbi" || -e "$stamp" ]]; then
    if [[ "$FORCE_REFERENCE" == "1" ]]; then
      rm -f "$out.gz" "$out.gz.tbi" "$stamp" "$stamp.tmp"
    else
      echo "ERROR chr${chrom}: existing output has missing or stale provenance; "\
        "remove the contig files or rerun with FORCE_REFERENCE=1" >&2
      return 1
    fi
  fi
  local n_in
  n_in=$(tabix "$NORM" "$chrom" | wc -l | tr -d ' ')
  local work
  work=$(mktemp -d "$OUT_DIR/contigs/.chr${chrom}.XXXXXX")
  local -a vep_args=()
  while IFS= read -r arg; do vep_args+=("$arg"); done <<< "$VEP_ARG_LINES"
  { tabix -H "$NORM"; tabix "$NORM" "$chrom"; } | bgzip -c > "$work/input.vcf.gz"
  echo "START chr${chrom}: $n_in records"
  local t0=$SECONDS
  if docker run --rm --user "$(id -u):$(id -g)" \
      -v "$CACHE:/opt/vep/.vep/homo_sapiens_merged/116_GRCh38:ro" \
      -v "$(dirname "$FASTA"):/ref:ro" \
      -v "$work:/work" \
      "$IMAGE" \
      "${vep_args[@]}" > "$log" 2>&1; then
    bgzip -@ 2 -c "$work/output.vcf" > "$out.gz.tmp"
    tabix -f -p vcf "$out.gz.tmp"
    # Remove the old pair before publishing. If interrupted during either move,
    # the next run sees one member missing and regenerates this contig.
    rm -f "$out.gz" "$out.gz.tbi"
    mv -f "$out.gz.tmp" "$out.gz"
    mv -f "$out.gz.tmp.tbi" "$out.gz.tbi"
    rm -rf "$work"
    local n_out
    n_out=$(tabix "$out.gz" "$chrom" | wc -l | tr -d ' ')
    if [[ "$n_out" != "$n_in" ]]; then
      rm -f "$out.gz" "$out.gz.tbi"
      echo "ERROR chr${chrom}: VEP wrote $n_out of $n_in records; output discarded" >&2
      return 1
    fi
    printf '%s\n' "$EXPECTED_PROVENANCE" > "$stamp.tmp"
    mv -f "$stamp.tmp" "$stamp"
    printf '%s\t%s\t%s\t%s\n' "$chrom" "$n_in" "$n_out" "$((SECONDS - t0))" >> "$OUT_DIR/timings.tsv"
    echo "DONE chr${chrom}: $n_out/$n_in records in $((SECONDS - t0)) s"
  else
    echo "ERROR chr${chrom}: see $log (work kept at $work)" >&2
    return 1
  fi
}
export -f run_one reference_state
export OUT_DIR NAME NORM CACHE FASTA IMAGE FORCE_REFERENCE VEP_ARG_LINES EXPECTED_PROVENANCE

printf '%s\n' "${contigs[@]}" \
  | xargs -P "$JOBS" -I{} bash -euo pipefail -c 'run_one "$@"' _ {}

# 4. Join the contigs in input order. The records are copied as text: bcftools
#    concat would reprint ClinVar's floating-point INFO values.
[[ "$#" -gt 0 ]] && exit 0
JOINED="$DATA/output/116/${NAME}_annotated_wgs_everything_hgvs_merged.vcf.gz"
mkdir -p "$(dirname "$JOINED")"
order=$(tabix -l "$NORM")
first=$(head -n1 <<< "$order")
{
  bgzip -dc "$OUT_DIR/contigs/${NAME}_chr${first}_vep116_merged.vcf.gz" | awk '/^#/ { print }'
  for chrom in $order; do
    bgzip -dc "$OUT_DIR/contigs/${NAME}_chr${chrom}_vep116_merged.vcf.gz" | awk '!/^#/ { print }'
  done
} | bgzip -@ 4 -c > "$JOINED.tmp"
tabix -f -p vcf "$JOINED.tmp"
rm -f "$JOINED" "$JOINED.tbi"
mv -f "$JOINED.tmp" "$JOINED"
mv -f "$JOINED.tmp.tbi" "$JOINED.tbi"
echo "Reference: $JOINED ($(bcftools index -n "$JOINED") records)"
