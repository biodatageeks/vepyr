#!/usr/bin/env bash
# Re-run the chrMT mismatch fixtures through Ensembl VEP 116.2 and compare the
# record bodies with the stored VEP 116.0 and vepyr 0.10.0 outputs, then repeat
# the TRNT experiment of the analysis (hgvs_shift without the 9 kb deletion,
# annotated against TRNT only and against everything but CYTB).
#
# Needs: docker with ensemblorg/ensembl-vep:release_116.2, the raw VEP merged
# cache at $DATA/homo_sapiens_merged/116_GRCh38 and the Ensembl 116
# primary-assembly FASTA at $DATA/input. Writes into $OUT only.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$HERE" rev-parse --show-toplevel)"
FIXTURES="$REPO/e2e-testing/golden/116/clinvar/mismatches/chrMT"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.2}"
OUT="${1:-$(mktemp -d)}"
mkdir -p "$OUT"

run_vep() {
  local workdir="$1" input="$2" output="$3"
  shift 3
  docker run --rm \
    -v "$DATA/homo_sapiens_merged:/opt/vep/.vep/homo_sapiens_merged:ro" \
    -v "$DATA/input:/ref:ro" \
    -v "$workdir:/w" \
    "$IMAGE" \
    vep --dir /opt/vep/.vep --cache --offline --assembly GRCh38 --merged \
      --vcf --force_overwrite --no_stats --everything --hgvs \
      --fasta /ref/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
      -i "/w/$input" -o "/w/$output" "$@"
}

body_diff() {
  diff <(grep -v '^#' "$1") <(grep -v '^#' "$2") | grep -c '^[<>]' || true
}

for class in codon_table csq_order hgvs_shift; do
  mkdir -p "$OUT/$class"
  cp "$FIXTURES/$class/input.vcf" "$OUT/$class/input.vcf"
  run_vep "$OUT/$class" input.vcf vep116_2.vcf
  printf '%-12s 116.2 vs 116.0: %s differing lines | 116.2 vs vepyr: %s\n' \
    "$class" \
    "$(body_diff "$OUT/$class/vep116_2.vcf" "$FIXTURES/$class/vep.vcf")" \
    "$(body_diff "$OUT/$class/vep116_2.vcf" "$FIXTURES/$class/vepyr.vcf")"
done

cp "$HERE/hgvs_shift_trnt_input.vcf" "$OUT/hgvs_shift/input_trnt.vcf"
run_vep "$OUT/hgvs_shift" input_trnt.vcf trnt_only.vcf \
  --transcript_filter "stable_id in TRNT,ENST00000387460"
run_vep "$OUT/hgvs_shift" input_trnt.vcf no_cytb.vcf \
  --transcript_filter "not stable_id in CYTB,ENST00000361789"
for name in trnt_only no_cytb; do
  echo "== TRNT HGVSc, $name"
  grep -v '^#' "$OUT/hgvs_shift/$name.vcf" | grep -o 'CSQ=.*' | tr ',' '\n' \
    | grep '|TRNT|' | grep -oE 'TRNT\.1:n\.[0-9a-z_]+'
done

echo "outputs in $OUT"
