#!/usr/bin/env bash
# Reproduce the full ClinVar chrMT comparison against pinned Ensembl VEP 116.2
# and vepyr 0.10.0. Writes generated VCFs into OUT only.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$HERE" rev-parse --show-toplevel)"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
OUT="${1:-$(mktemp -d)}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.2@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da}"
SOURCE="$DATA/input/clinvar/clinvar_20261011.vcf.gz"
FASTA="$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
CACHE="$DATA/cache/116_GRCh38_merged"
RAW_CACHE="$DATA/homo_sapiens_merged"
mkdir -p "$OUT"

source_md5="$(python3 - "$SOURCE" <<'PY'
import hashlib
import sys

digest = hashlib.md5()
with open(sys.argv[1], "rb") as source:
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
)"
[[ "$source_md5" == "0f3a1a5e84252386033d5fcab1bf1f6a" ]] || {
  echo "ClinVar source checksum does not match release 20261011" >&2
  exit 1
}

bcftools view -e 'ALT="."' -r MT "$SOURCE" -Ou \
  | bcftools norm -m -both -Oz -o "$OUT/clinvar_20261011_MT.norm.vcf.gz"
tabix -f -p vcf "$OUT/clinvar_20261011_MT.norm.vcf.gz"
[[ "$(bcftools index -n "$OUT/clinvar_20261011_MT.norm.vcf.gz")" == "3134" ]] || {
  echo "expected 3,134 normalized MT records" >&2
  exit 1
}

docker run --rm \
  -v "$RAW_CACHE:/opt/vep/.vep/homo_sapiens_merged:ro" \
  -v "$DATA/input:/ref:ro" \
  -v "$OUT:/w" \
  "$IMAGE" \
  vep --dir /opt/vep/.vep --cache --offline --assembly GRCh38 --merged \
    --vcf --force_overwrite --no_stats --everything --hgvs \
    --fasta /ref/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    -i /w/clinvar_20261011_MT.norm.vcf.gz \
    -o /w/clinvar_20261011_MT.vep116_2.vcf

VEP_OUT="$OUT/clinvar_20261011_MT.vep116_2.vcf"
grep -Fq '##VEP="v116.2" API="v116"' "$VEP_OUT"
grep -Fq 'ensembl=116.0d85231' "$VEP_OUT"
grep -Fq 'ensembl-variation=116.db7394a' "$VEP_OUT"
[[ "$(awk '!/^#/ {n++} END {print n+0}' "$VEP_OUT")" == "3134" ]] || {
  echo "VEP 116.2 output does not contain 3,134 records" >&2
  exit 1
}

uv tool run --from vepyr==0.10.0 vepyr annotate \
  -i "$OUT/clinvar_20261011_MT.norm.vcf.gz" \
  -o "$OUT/clinvar_20261011_MT.vepyr_010.vcf" \
  --dir_cache "$CACHE" --fasta "$FASTA" --workers 2

python3 - "$REPO" "$OUT" <<'PY'
import sys
from pathlib import Path

repo, out = map(Path, sys.argv[1:])
scripts = repo / "e2e-testing/scripts"
sys.path.insert(0, str(scripts))
from md5_concordance import Pair, compare, explain

vep = out / "clinvar_20261011_MT.vep116_2.vcf"
vepyr = out / "clinvar_20261011_MT.vepyr_010.vcf"
pair = Pair("chrMT", str(vep), str(vepyr))
strict = compare(pair, "strict")
canonical = compare(pair, "canonical")
classes = explain(str(vep), str(vepyr), limit=0)
expected = {"identical": 807, "CSQ order": 2030, "INFO VALUES": 297}
if dict(classes) != expected:
    raise SystemExit(f"unexpected VEP 116.2 mismatch classes: {dict(classes)}")
if not strict.count_match or strict.vep.records != 3134:
    raise SystemExit("VEP/vepyr record counts differ from 3,134")
if strict.body_match or canonical.body_match:
    raise SystemExit("expected VEP/vepyr body differences were not reproduced")
print("VEP 116.2 body md5:", strict.vep.body)
print("vepyr 0.10.0 body md5:", strict.vepyr.body)
print("VEP 116.2 canonical md5:", canonical.vep.body)
print("vepyr canonical md5:", canonical.vepyr.body)
print("record comparison:", dict(classes))
PY

echo "full chrMT VEP 116.2 and vepyr 0.10.0 outputs: $OUT"
