#!/usr/bin/env bash
#
# Build the Ensembl VEP 116 PhenotypeOrthologous reference for one chromosome:
# the golden output `run_comparison.py --profile merged_phenotypeorthologous`
# compares against. Same recipe as build_vep_plugin_reference.sh (merged cache,
# --everything, normalized input), with exactly one plugin loaded and BGZF
# written by VEP itself.
#
# Usage: ./build_vep_phenotypeorthologous_reference.sh 22 [workdir]
#   DATA_VEPYR_DIR      data root (default ~/workspace/data_vepyr)
#   VEP_NORMALIZED_VCF  normalized input (default $DATA/input/HG002_norm.vcf.gz)
#   VEP_OUTPUT_VCF      output .vcf.gz (default $DATA/output/116/plugins/HG002_chr<N>_phenotypeorthologous_vep116.vcf.gz)
#   VEP_PLUGIN_DIR      directory holding PhenotypeOrthologous.pm (default $DATA/output/116/plugins/plugin_code)
set -euo pipefail

CHROM="${1:?usage: $0 <chrom> [workdir]}"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
WORK="${2:-$DATA/vep116_po_chr${CHROM}}"
IMAGE="ensemblorg/ensembl-vep:release_116.0"
REF_DIR="$DATA/output/116/plugins"
PLUGIN_DIR="${VEP_PLUGIN_DIR:-$REF_DIR/plugin_code}"
SRC_DIR="$DATA/plugin_input/phenotypeorthologous"
SRC_NAME="PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
SRC_URL="https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/$SRC_NAME"
SRC_MD5="20e5401a198d7d3db66a982c037d3ad4"
TBI_MD5="1eb833a26c247418910685289c6528cc"
PLUGIN_COMMIT="a7e03a5c6497e29e0598eed7b4795f953d9a1b5f"
PLUGIN_SHA256="89be8f30dd464f81913bef367831e8a08258e4085207f8291c956487f47de8ac"
OUT="${VEP_OUTPUT_VCF:-$REF_DIR/HG002_chr${CHROM}_phenotypeorthologous_vep116.vcf.gz}"

mkdir -p "$WORK/input" "$REF_DIR" "$PLUGIN_DIR" "$SRC_DIR"

md5_file() {
  if command -v md5sum >/dev/null 2>&1; then md5sum "$1" | awk '{print $1}'; else md5 -q "$1"; fi
}
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# ---------------------------------------------------------------------------
# 1. Source data and plugin code, both pinned by digest
# ---------------------------------------------------------------------------
for f in "$SRC_NAME" "$SRC_NAME.tbi"; do
  if [[ ! -s "$SRC_DIR/$f" ]]; then
    curl --fail --silent --show-error -o "$SRC_DIR/$f.partial" "$SRC_URL${f#"$SRC_NAME"}"
    mv "$SRC_DIR/$f.partial" "$SRC_DIR/$f"
  fi
done
[[ "$(md5_file "$SRC_DIR/$SRC_NAME")" == "$SRC_MD5" ]] || { echo "ERROR: $SRC_NAME md5 mismatch" >&2; exit 1; }
[[ "$(md5_file "$SRC_DIR/$SRC_NAME.tbi")" == "$TBI_MD5" ]] || { echo "ERROR: $SRC_NAME.tbi md5 mismatch" >&2; exit 1; }

if [[ ! -s "$PLUGIN_DIR/PhenotypeOrthologous.pm" ]]; then
  curl --fail --silent --show-error \
    -o "$PLUGIN_DIR/PhenotypeOrthologous.pm" \
    "https://raw.githubusercontent.com/Ensembl/VEP_plugins/$PLUGIN_COMMIT/PhenotypeOrthologous.pm"
fi
actual="$(sha256_file "$PLUGIN_DIR/PhenotypeOrthologous.pm")"
[[ "$actual" == "$PLUGIN_SHA256" ]] || { echo "ERROR: PhenotypeOrthologous.pm sha256 $actual != $PLUGIN_SHA256" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2. Normalized input slice
# ---------------------------------------------------------------------------
NORM="${VEP_NORMALIZED_VCF:-$DATA/input/HG002_norm.vcf.gz}"
[[ -s "$NORM" && -s "$NORM.tbi" ]] || { echo "ERROR: normalized input $NORM (+.tbi) missing; see build_vep_plugin_reference.sh" >&2; exit 1; }
IN="$WORK/input/HG002_norm_chr${CHROM}.vcf.gz"
{ tabix -H "$NORM"; tabix "$NORM" "chr${CHROM}"; } | bgzip -c > "$IN"
tabix -f -p vcf "$IN"

# ---------------------------------------------------------------------------
# 3. Annotate (BGZF written by VEP, --compress_output bgzip)
# ---------------------------------------------------------------------------
# --assembly is load-bearing: PhenotypeOrthologous.pm reads config->{assembly}
# in new() and dies with "Assembly is not GRCh38" when it is unset, which VEP
# only reports as a warning ("Failed to instantiate plugin") and then runs on
# without the plugin. The 4-field header check below is the backstop.
to_container() {  # host path under $DATA -> /data/...
  local p d
  p="$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"
  d="$(cd "$DATA" && pwd -P)"
  [[ "$p" == "$d/"* ]] || { echo "ERROR: $1 must live under $DATA" >&2; exit 1; }
  printf '/data/%s\n' "${p#"$d/"}"
}
IN_C="$(to_container "$IN")"
OUT_C="$(to_container "$OUT")"
SRC_C="$(to_container "$SRC_DIR/$SRC_NAME")"

docker run --rm --user "$(id -u):$(id -g)" \
  -v "$DATA":/data -v "$PLUGIN_DIR":/plugins:ro "$IMAGE" \
  vep --cache --cache_version 116 --dir_cache /data --offline --merged \
      --assembly GRCh38 \
      --everything --no_stats --force_overwrite --vcf --compress_output bgzip \
      --fasta /data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
      --input_file "$IN_C" --output_file "$OUT_C" \
      --dir_plugins /plugins \
      --plugin "PhenotypeOrthologous,file=$SRC_C"

# ---------------------------------------------------------------------------
# 4. Index and check the four plugin fields landed
# ---------------------------------------------------------------------------
tabix -f -p vcf "$OUT"
n_plugin=$(tabix -H "$OUT" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -c '^PhenotypeOrthologous_' || true)
if [[ "$n_plugin" -ne 4 ]]; then
  echo "ERROR: expected 4 PhenotypeOrthologous CSQ fields, found $n_plugin" >&2
  grep -i 'failed to instantiate' "${OUT%.vcf.gz}.vcf_warnings.txt" >&2 || true
  exit 1
fi
{
  printf 'PLUGIN PhenotypeOrthologous %s\n' "$PLUGIN_SHA256"
  printf 'SOURCE %s md5=%s\n' "$SRC_URL" "$SRC_MD5"
} > "$OUT.plugins"
echo "OK: chr${CHROM} - $(bgzip -dc "$OUT" | grep -vc '^#') records, $n_plugin plugin CSQ fields"
echo "     $OUT"
