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
# raw VEP 116 merged cache, the Docker image and the ClinVar release. Each VEP
# process grows to 2-3.5 GiB, so allow about 3.5 GiB of Docker memory per job.

set -euo pipefail

DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
CLINVAR_RELEASE="${CLINVAR_RELEASE:-20261011}"
JOBS="${VEP_REFERENCE_JOBS:-6}"
IMAGE="ensemblorg/ensembl-vep:release_116.0"
CACHE="$DATA/homo_sapiens_merged/116_GRCh38"
IN_DIR="$DATA/input/clinvar"
OUT_DIR="$DATA/output/116/clinvar"
NAME="clinvar_${CLINVAR_RELEASE}"
URL="https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38"
ENSEMBL="https://ftp.ensembl.org/pub/release-116"
# Largest first, so the longest jobs do not start last.
ALL_CONTIGS=(1 2 17 11 19 3 16 7 5 10 12 6 9 15 X 4 8 14 20 22 13 18 21 MT Y)

FASTA="${FASTA:-$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa}"
if [[ ! -f "$FASTA" && -f "$DATA/$(basename "$FASTA")" ]]; then
  FASTA="$DATA/$(basename "$FASTA")"   # legacy layout, as in profiles.py
fi
mkdir -p "$DATA" "$IN_DIR" "$OUT_DIR/contigs" "$OUT_DIR/logs"

# 0. Check the disk before anything is downloaded. Counted in GiB, only for
#    what is missing; the VEP outputs include the plain VCFs of running jobs.
need=40
[[ -s "$CACHE/info.txt" ]] || need=$((need + 60))   # archive plus extracted cache
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
[[ -f "$FASTA.fai" ]] || samtools faidx "$FASTA"
if [[ ! -s "$CACHE/info.txt" ]]; then
  tarball="$DATA/homo_sapiens_merged_vep_116_GRCh38.tar.gz"
  url="$ENSEMBL/variation/indexed_vep_cache/$(basename "$tarball")"
  # Ensembl serves each connection at about 5 MB/s; aria2c opens several.
  if command -v aria2c > /dev/null; then
    aria2c -q -c -x 8 -s 8 -d "$DATA" -o "$(basename "$tarball")" "$url"
  else
    curl -sSfL -C - -o "$tarball" "$url"
  fi
  # The BSD sum published in the release's CHECKSUMS file.
  [[ "$(sum "$tarball" | awk '{print $1, $2}')" == "26554 29618744" ]] \
    || { echo "ERROR: checksum mismatch for $tarball" >&2; exit 1; }
  tar -xzf "$tarball" -C "$DATA"
  rm -f "$tarball"
fi
# Docker creates a missing -v source as an empty directory, and VEP then skips
# every variant instead of failing (see vep-docker.md).
[[ -s "$CACHE/info.txt" ]] || { echo "ERROR: no VEP cache at $CACHE" >&2; exit 1; }
docker image inspect "$IMAGE" > /dev/null 2>&1 || docker pull "$IMAGE"

# 1. Download and verify the ClinVar release.
RAW="$IN_DIR/$NAME.vcf.gz"
if [[ ! -s "$RAW.tbi" ]]; then
  for f in "$NAME.vcf.gz" "$NAME.vcf.gz.tbi" "$NAME.vcf.gz.md5"; do
    curl -sSfL -C - -o "$IN_DIR/$f" "$URL/$f"
  done
  if command -v md5sum > /dev/null; then got=$(md5sum < "$RAW" | cut -d' ' -f1); else got=$(md5 -q "$RAW"); fi
  [[ "$(cut -d' ' -f1 "$RAW.md5")" == "$got" ]] \
    || { echo "ERROR: md5 mismatch for $RAW" >&2; rm -f "$RAW.tbi"; exit 1; }
fi

# 2. Preprocess: drop ALT="." sites and records outside 1-22, X, Y, MT, then
#    split multiallelic records exactly as the HG002 input was split.
NORM="$IN_DIR/$NAME.norm.vcf.gz"
if [[ ! -s "$NORM.tbi" ]]; then
  contigs=$(IFS=,; echo "${ALL_CONTIGS[*]}")
  bcftools view -e 'ALT="."' -t "$contigs" "$RAW" -Ou \
    | bcftools norm -m -both -Oz -o "$NORM.tmp"
  tabix -f -p vcf "$NORM.tmp"
  mv -f "$NORM.tmp.tbi" "$NORM.tbi"
  mv -f "$NORM.tmp" "$NORM"
fi

# 3. One VEP process per contig.
run_one() {
  local chrom="$1"
  local out="$OUT_DIR/contigs/${NAME}_chr${chrom}_vep116_merged.vcf"
  local log="$OUT_DIR/logs/chr${chrom}.log"
  local n_in
  n_in=$(tabix "$NORM" "$chrom" | wc -l | tr -d ' ')
  if [[ -s "$out.gz" && -s "$out.gz.tbi" ]]; then
    echo "SKIP chr${chrom}: complete"
    return
  fi
  local work
  work=$(mktemp -d "$OUT_DIR/contigs/.chr${chrom}.XXXXXX")
  { tabix -H "$NORM"; tabix "$NORM" "$chrom"; } | bgzip -c > "$work/input.vcf.gz"
  echo "START chr${chrom}: $n_in records"
  local t0=$SECONDS
  if docker run --rm --user "$(id -u):$(id -g)" \
      -v "$CACHE:/opt/vep/.vep/homo_sapiens_merged/116_GRCh38:ro" \
      -v "$(dirname "$FASTA"):/ref:ro" \
      -v "$work:/work" \
      "$IMAGE" \
      vep --dir /opt/vep/.vep --cache --offline --assembly GRCh38 \
        --input_file /work/input.vcf.gz --output_file /work/output.vcf \
        --vcf --force_overwrite --no_stats --everything --hgvs \
        --fasta "/ref/$(basename "$FASTA")" --merged > "$log" 2>&1; then
    bgzip -@ 2 -c "$work/output.vcf" > "$out.gz.tmp"
    tabix -f -p vcf "$out.gz.tmp"
    mv -f "$out.gz.tmp.tbi" "$out.gz.tbi"
    mv -f "$out.gz.tmp" "$out.gz"
    rm -rf "$work"
    local n_out
    n_out=$(tabix "$out.gz" "$chrom" | wc -l | tr -d ' ')
    printf '%s\t%s\t%s\t%s\n' "$chrom" "$n_in" "$n_out" "$((SECONDS - t0))" >> "$OUT_DIR/timings.tsv"
    echo "DONE chr${chrom}: $n_out/$n_in records in $((SECONDS - t0)) s"
    # VEP skips lines it cannot parse; the md5 check then reports the contig.
    [[ "$n_out" == "$n_in" ]] || echo "WARN chr${chrom}: VEP wrote $n_out of $n_in records" >&2
  else
    echo "ERROR chr${chrom}: see $log (work kept at $work)" >&2
    return 1
  fi
}
export -f run_one
export OUT_DIR NAME NORM CACHE FASTA IMAGE

if [[ "$#" -gt 0 ]]; then contigs=("$@"); else contigs=("${ALL_CONTIGS[@]}"); fi
printf '%s\n' "${contigs[@]}" | xargs -P "$JOBS" -I{} bash -c 'run_one {}'

# 4. Join the contigs in input order. The records are copied as text: bcftools
#    concat would reprint ClinVar's floating-point INFO values.
[[ "$#" -gt 0 ]] && exit 0
JOINED="$DATA/output/116/${NAME}_annotated_wgs_everything_hgvs_merged.vcf.gz"
mkdir -p "$(dirname "$JOINED")"
order=$(tabix -l "$NORM")
first=$(head -n1 <<< "$order")
{
  bgzip -dc "$OUT_DIR/contigs/${NAME}_chr${first}_vep116_merged.vcf.gz" | grep '^#'
  for chrom in $order; do
    bgzip -dc "$OUT_DIR/contigs/${NAME}_chr${chrom}_vep116_merged.vcf.gz" | grep -v '^#'
  done
} | bgzip -@ 4 -c > "$JOINED.tmp"
tabix -f -p vcf "$JOINED.tmp"
mv -f "$JOINED.tmp.tbi" "$JOINED.tbi"
mv -f "$JOINED.tmp" "$JOINED"
echo "Reference: $JOINED ($(bcftools index -n "$JOINED") records)"
