#!/usr/bin/env bash
# Build the chr22 work directory. Everything lives under $DATA_VEPYR_DIR so the
# VEP container, which mounts only that tree, can reach it.
set -euo pipefail
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
WORK="${PIBENCH_WORK:-$DATA/polars_integration/chr22}"
SRC="$DATA/input/HG002_normalized.vcf.gz"
FASTA="$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
mkdir -p "$WORK/input"
if [[ ! -s "$WORK/input/HG002_chr22.vcf.gz.tbi" ]]; then
  bcftools view -r chr22 -Oz -o "$WORK/input/HG002_chr22.vcf.gz" "$SRC"
  tabix -f -p vcf "$WORK/input/HG002_chr22.vcf.gz"
fi
n=$(bcftools view -H "$WORK/input/HG002_chr22.vcf.gz" | wc -l | tr -d ' ')
[[ "$n" == 50861 ]] || { echo "expected 50861 chr22 records, got $n" >&2; exit 1; }
# Hard links, not symlinks: a symlink target outside the mount is invisible in the container.
for f in "$FASTA" "$FASTA.fai"; do
  [[ -e "$WORK/input/$(basename "$f")" ]] || ln "$f" "$WORK/input/$(basename "$f")"
done
echo "chr22 input ready in $WORK/input ($n records)"
