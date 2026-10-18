#!/usr/bin/env bash
# Re-run the chrMT mismatch fixtures through Ensembl VEP 116.2 and compare the
# record bodies with the stored VEP 116.0 and vepyr 0.10.0 outputs, then repeat
# the TRNT experiment of the analysis (hgvs_shift without the 9 kb deletion,
# annotated against TRNT only and against everything but CYTB).
#
# Needs: docker, the raw VEP merged cache at
# $DATA/homo_sapiens_merged/116_GRCh38, and the Ensembl 116
# primary-assembly FASTA at $DATA/input. Writes into $OUT only.
# The default image is digest-pinned; VEP_IMAGE overrides must have the exact
# 116.2, core, and variation identities asserted below.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$HERE" rev-parse --show-toplevel)"
FIXTURES="$REPO/e2e-testing/golden/116/clinvar/mismatches/chrMT"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.2@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da}"
OUT="${1:-$(mktemp -d)}"
mkdir -p "$OUT"

assert_vep116_2() {
  local output="$1" header
  header="$(grep -m1 '^##VEP=' "$output")"
  [[ "$header" == *'##VEP="v116.2" API="v116"'* ]] || {
    echo "expected VEP 116.2 / API 116 identity in $output" >&2
    return 1
  }
  [[ "$header" == *'ensembl=116.0d85231'* ]] || {
    echo "unexpected Ensembl core revision in $output: $header" >&2
    return 1
  }
  [[ "$header" == *'ensembl-variation=116.db7394a'* ]] || {
    echo "unexpected Ensembl variation revision in $output: $header" >&2
    return 1
  }
}

record_count() {
  awk '!/^#/ { n++ } END { print n + 0 }' "$1"
}

assert_equal() {
  local label="$1" actual="$2" expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "$label: expected $expected, got $actual" >&2
    return 1
  fi
}

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
  assert_vep116_2 "$workdir/$output"
  assert_equal "record count for $input -> $output" \
    "$(record_count "$workdir/$output")" "$(record_count "$workdir/$input")"
}

body_diff_lines() {
  local count status=0
  count="$(diff <(grep -v '^#' "$1") <(grep -v '^#' "$2") \
    | awk '/^[<>]/ { n++ } END { print n + 0 }')" || status=$?
  (( status <= 1 )) || return "$status"
  printf '%s\n' "$count"
}

expected_vepyr_diff_lines=(24 6 8)
index=0
for class in codon_table csq_order hgvs_shift; do
  mkdir -p "$OUT/$class"
  cp "$FIXTURES/$class/input.vcf" "$OUT/$class/input.vcf"
  run_vep "$OUT/$class" input.vcf vep116_2.vcf
  old_reference_diff="$(body_diff_lines "$OUT/$class/vep116_2.vcf" "$FIXTURES/$class/vep.vcf")"
  assert_equal "$class: VEP 116.2 vs stored VEP 116.0 changed body lines" \
    "$old_reference_diff" 0
  vepyr_diff="$(body_diff_lines "$OUT/$class/vep116_2.vcf" "$FIXTURES/$class/vepyr.vcf")"
  assert_equal "$class: VEP 116.2 vs stored vepyr changed body lines" \
    "$vepyr_diff" "${expected_vepyr_diff_lines[$index]}"
  printf '%-12s VEP 116.2 identity confirmed; 116.0 body diff=%s; vepyr body diff=%s lines\n' \
    "$class" "$old_reference_diff" "$vepyr_diff"
  index=$((index + 1))
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
