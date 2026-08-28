#!/usr/bin/env bash
#
# Build the five-plugin Ensembl VEP reference for one chromosome.
#
# This is the golden output `run_comparison.py --profile merged_plugins` is
# compared against. It must be produced from a **normalized** input and a VEP
# release that matches vepyr's registered support target, or the comparison
# either refuses to run or pairs records that are not the same records.
#
# Usage:
#   ./build_vep_plugin_reference.sh 21 [outdir]
#   VEP_PLUGIN_DIR=/path/to/plugins ./build_vep_plugin_reference.sh 1
#
# Requires: docker, bcftools, bgzip, tabix, and the raw plugin sources tabix-
# indexed (see docs — they can be sliced over HTTP with `rclone serve http`
# instead of downloading 168 GB).

set -euo pipefail

CHROM="${1:?usage: $0 <chrom> [outdir]}"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
WORK="${2:-$DATA/vep116_chr${CHROM}}"
IMAGE="ensemblorg/ensembl-vep:release_116.0"
PLUGIN_DIR="${VEP_PLUGIN_DIR:-$WORK/plugins}"

# The image must match vepyr's supported target exactly (vep 116.0 / API 116 /
# ensembl 116.c0cf13d / ensembl-variation 116.2fb834b), or
# validate_vep_reference_identity() rejects the reference. release_116.0 has a
# native arm64 build, so no emulation on Apple silicon.

SLICES="$WORK/slices"
mkdir -p "$WORK/input" "$WORK/output" "$WORK/plugins" "$SLICES"

# Optionally materialise chromosome slices directly from a range-readable
# bgzip/tabix source tree. This keeps the 168 GB source corpus remote while VEP
# still receives ordinary local indexed files. Existing complete slices are
# reused, making an interrupted build resumable.
SOURCE_BASE="${VEP_PLUGIN_SOURCE_URL:-}"

fetch_slice() {
  local remote_path="$1"
  local region="$2"
  local output="$3"
  local kind="$4"

  if [[ -s "$output" && -s "$output.tbi" ]] && \
      tabix -l "$output" | grep -Fxq "$region"; then
    echo "Reusing slice: $output"
    return
  fi

  if [[ -z "$SOURCE_BASE" ]]; then
    echo "ERROR: missing slice $output and VEP_PLUGIN_SOURCE_URL is unset" >&2
    exit 1
  fi

  local partial="${output}.partial.$$.gz"
  echo "Slicing ${SOURCE_BASE%/}/${remote_path} region $region"
  tabix -h "${SOURCE_BASE%/}/${remote_path}" "$region" | bgzip -c > "$partial"
  mv "$partial" "$output"

  case "$kind" in
    vcf) tabix -f -p vcf "$output" ;;
    tsv) tabix -f -s 1 -b 2 -e 2 "$output" ;;
    *)
      echo "ERROR: unsupported slice kind: $kind" >&2
      exit 1
      ;;
  esac

  if ! tabix -l "$output" | grep -Fxq "$region"; then
    echo "ERROR: slice $output contains no indexed region $region" >&2
    exit 1
  fi
}

if [[ -n "$SOURCE_BASE" ]]; then
  fetch_slice "clinvar/clinvar.vcf.gz" "$CHROM" \
    "$SLICES/clinvar_chr${CHROM}.vcf.gz" vcf
  fetch_slice "spliceai/spliceai_scores.masked.snv.ensembl_mane.grch38.110.vcf.gz" \
    "$CHROM" "$SLICES/spliceai_chr${CHROM}.vcf.gz" vcf
  fetch_slice "alphamissense/AlphaMissense_hg38.bgz.tsv.gz" "chr${CHROM}" \
    "$SLICES/alphamissense_chr${CHROM}.tsv.gz" tsv
  fetch_slice "dbnsfp/dbNSFP5.3.1a_grch38.gz" "$CHROM" \
    "$SLICES/dbNSFP5.3.1a_grch38_chr${CHROM}.gz" tsv
  fetch_slice "cadd/whole_genome_SNVs.tsv.gz" "$CHROM" \
    "$SLICES/cadd_snv_chr${CHROM}.tsv.gz" tsv
  fetch_slice "cadd/gnomad.genomes.r4.0.indel.tsv.gz" "$CHROM" \
    "$SLICES/cadd_indel_chr${CHROM}.tsv.gz" tsv
fi

# ---------------------------------------------------------------------------
# 1. Normalized input
# ---------------------------------------------------------------------------
# Every other reference in the suite is built from a `bcftools norm -m -both`
# input. The original five-plugin reference was not, which left 602 chr21
# records multi-allelic and unpairable against vepyr's split output. Splitting
# here keeps this reference consistent with the rest of the set.
NORM="${VEP_NORMALIZED_VCF:-$DATA/input/HG002_norm.vcf.gz}"
if [[ ! -f "$NORM" ]]; then
  # The documented layout keeps the downloaded benchmark under $DATA/input/;
  # older data roots have it beside it. Mirror the comparison profiles'
  # legacy fallback so a fresh standard layout works on the first build.
  BENCHMARK_NAME="HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
  BENCHMARK="${VEP_BENCHMARK_VCF:-}"
  if [[ -z "$BENCHMARK" ]]; then
    if [[ -f "$DATA/input/$BENCHMARK_NAME" ]]; then
      BENCHMARK="$DATA/input/$BENCHMARK_NAME"
    elif [[ -f "$DATA/$BENCHMARK_NAME" ]]; then
      BENCHMARK="$DATA/$BENCHMARK_NAME"
      echo "Note: using legacy location $BENCHMARK; move it under input/" >&2
    else
      # Name the documented path in the failure, not the legacy one.
      BENCHMARK="$DATA/input/$BENCHMARK_NAME"
    fi
  fi
  [[ -f "$BENCHMARK" ]] || { echo "ERROR: benchmark VCF not found: $BENCHMARK" >&2; exit 1; }
  mkdir -p "$(dirname "$NORM")"
  # generate_vep_plugin_references.sh runs VEP_REFERENCE_JOBS chromosomes at
  # once, and on a fresh data root every one of them reaches this branch. Build
  # under a unique name and publish atomically so concurrent jobs cannot write
  # the same file, or read a half-written one. The index is moved into place
  # first: $NORM appearing is what the existence check above keys on.
  norm_tmp="${NORM}.tmp.$$"
  bcftools norm -m -both -Oz -o "$norm_tmp" "$BENCHMARK"
  tabix -f -p vcf "$norm_tmp"
  mv -f "$norm_tmp.tbi" "$NORM.tbi"
  mv -f "$norm_tmp" "$NORM"
fi
IN="$WORK/input/HG002_norm_chr${CHROM}.vcf.gz"
{ tabix -H "$NORM"; tabix "$NORM" "chr${CHROM}"; } | bgzip -c > "$IN"
tabix -f -p vcf "$IN"

# ---------------------------------------------------------------------------
# 2. Plugin data slices — three file-shape traps
# ---------------------------------------------------------------------------
#  a) dbNSFP parses its *version from the filename*: a slice named
#     `dbnsfp_chr21.tsv.gz` is rejected outright and its 19 fields silently
#     vanish from the output. The name must contain e.g. `5.3.1a`.
#  b) The official CADD VEP plugin opens its SNV and indel sources through
#     Bio::DB::HTS::Tabix. Keep them as two bgzipped, tabix-indexed files. This
#     differs intentionally from vepyr's cache builder, whose manifest ingests
#     one combined plain-text source.
#  c) VEP 116.0/116.1 CADD.pm reads the HGVS-shifted overlap allele and therefore
#     misses valid annotations. The golden reference requires the corrected
#     CADD.pm from mwiewior/VEP_plugins commit 7a1f6450fd12. Other plugin files
#     must remain at their original VEP 116 comparison versions.
#
# Expected slice names in $SLICES:
#   clinvar_chr${CHROM}.vcf.gz
#   spliceai_chr${CHROM}.vcf.gz
#   alphamissense_chr${CHROM}.tsv.gz
#   dbNSFP5.3.1a_grch38_chr${CHROM}.gz
#   cadd_snv_chr${CHROM}.tsv.gz  +  cadd_indel_chr${CHROM}.tsv.gz
for plugin in AlphaMissense CADD SpliceAI dbNSFP; do
  if [[ ! -f "$PLUGIN_DIR/${plugin}.pm" ]]; then
    echo "ERROR: missing $PLUGIN_DIR/${plugin}.pm" >&2
    exit 1
  fi
done

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

verify_plugin() {
  local plugin="$1"
  local expected="$2"
  local actual
  actual="$(sha256_file "$PLUGIN_DIR/${plugin}.pm")"
  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: unexpected ${plugin}.pm SHA-256: $actual (expected $expected)" >&2
    exit 1
  fi
}

# Plugin code is not represented in VEP's output header. Pin its bytes here so
# a checkout drift cannot manufacture or hide parity mismatches.
verify_plugin AlphaMissense 25e7164d2da4ff69f3ad42b4fbfc27022050933c6427b8d7ec483c978e68d14b
verify_plugin CADD f0bbe7ab1d4aff0de9e45aeedd4e112824394e81cc3e201ea1b8f001dd9e5cb4
verify_plugin SpliceAI d08ce7d8c2e6b3229cef638beff145e465e224f0e3fcd43fd49d501ebc2740b1
verify_plugin dbNSFP 1574736e0371b12378211914db6f5de720eeba395cd42517fbe689cededb9f67

# The checks above run only when this script runs. A reference already on disk
# carries no record of which plugin bytes produced it -- VEP's header does not
# name them -- so a generator that reuses it cannot tell a current reference
# from one built with an older CADD.pm. Emit the provenance beside the output
# so reuse can be gated on it.
plugin_provenance() {
  local plugin
  for plugin in AlphaMissense CADD SpliceAI dbNSFP; do
    printf '%s %s\n' "$plugin" "$(sha256_file "$PLUGIN_DIR/${plugin}.pm")"
  done
}

if ! grep -q 'my \$alt_alleles = \$bvf->alt_alleles' "$PLUGIN_DIR/CADD.pm"; then
  echo "ERROR: CADD.pm lacks the HGVS-shift fix from VEP_plugins 7a1f6450fd12" >&2
  exit 1
fi

DBNSFP_COLS="SIFT4G_score,SIFT4G_pred,Polyphen2_HDIV_score,Polyphen2_HVAR_score,\
MutationTaster_score,MutationTaster_pred,PROVEAN_score,PROVEAN_pred,VEST4_score,\
MetaSVM_score,MetaSVM_pred,MetaLR_score,MetaLR_pred,REVEL_score,GERP++_RS,\
phyloP100way_vertebrate,phastCons100way_vertebrate,CADD_raw,CADD_phred"

# Only $DATA is mounted at /data, so a work/output directory outside it is
# invisible to VEP. Stripping the prefix off such a path is a no-op and yields
# "/data//abs/host/path", which VEP reports as a missing input rather than as
# the configuration error it is. Translate through here and fail loudly.
# Collapse "." and ".." lexically. Needed because the target directory may not
# exist yet, and `cd`-based resolution silently leaves such a path untouched --
# which would let "$DATA/../scratch" keep matching a "$DATA/" prefix test.
normalize_lexical() {
  local path="$1" part
  local -a out=()
  [[ "$path" != /* ]] && path="$PWD/$path"
  local IFS=/
  for part in $path; do
    case "$part" in
      ''|.) ;;
      ..) [[ ${#out[@]} -gt 0 ]] && unset "out[$((${#out[@]} - 1))]" && out=("${out[@]}") ;;
      *) out+=("$part") ;;
    esac
  done
  printf '/%s\n' "${out[*]}"
}

# Absolute, symlink-free where the path exists, ".."-free always. The leaf need
# not exist yet.
canonical_path() {
  local path dir base
  path="$(normalize_lexical "$1")"
  if [[ -d "$path" ]]; then
    ( cd "$path" 2>/dev/null && pwd -P )
    return
  fi
  dir="$(dirname "$path")"
  base="$(basename "$path")"
  if [[ -d "$dir" ]]; then
    printf '%s/%s\n' "$( cd "$dir" 2>/dev/null && pwd -P )" "$base"
  else
    printf '%s\n' "$path"
  fi
}

docker_path() {
  local label="$1" path="$2" rel canon data_canon
  # A lexical prefix test is not containment: "$DATA/../scratch" passes it and
  # becomes /data/../scratch, which resolves inside the container to /scratch --
  # outside the only bind mount. Canonicalise both sides first, so `..` and
  # escaping symlinks are rejected rather than silently redirected.
  canon="$(canonical_path "$path")"
  data_canon="$(canonical_path "$DATA")"
  rel="${canon#"$data_canon/"}"
  if [[ "$rel" == "$canon" ]]; then
    echo "ERROR: $label must live under \$DATA ($data_canon) so the VEP container can reach it." >&2
    echo "       got: $path" >&2
    [[ "$canon" != "$path" ]] && echo "       resolves to: $canon" >&2
    return 1
  fi
  printf '/data/%s\n' "$rel"
}

OUT="${VEP_OUTPUT_VCF:-$WORK/output/HG002_chr${CHROM}_5plugins_vep116_caddfix.vcf}"

# ---------------------------------------------------------------------------
# 3. Annotate
# ---------------------------------------------------------------------------
# Plugin flag ORDER IS SIGNIFICANT: VEP emits each plugin's CSQ block in the
# order the flags appear, and appends --custom blocks last. vepyr reproduces
# that layout via `csq_rank` in each plugin's source manifest, so changing the
# order here desynchronises the two without changing any value.
#
# NOTE: `--database 0` appears in VEP's own ##VEP-command-line header but is
# NOT valid input (it parses as a stray positional). `--offline` covers it.
IN_C="$(docker_path "the input VCF (work dir / VEP_NORMALIZED_VCF)" "$IN")"
OUT_C="$(docker_path "the output VCF (VEP_OUTPUT_VCF)" "$OUT")"
SLICES_C="$(docker_path "the plugin slices directory (work dir)" "$SLICES")"

docker run --rm --user "$(id -u):$(id -g)" \
  -v "$DATA":/data -v "$PLUGIN_DIR":/plugins:ro "$IMAGE" \
  vep --cache --cache_version 116 --dir_cache /data --offline --merged \
      --everything --no_stats --force_overwrite --vcf \
      --fasta /data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
      --input_file "$IN_C" \
      --output_file "$OUT_C" \
      --dir_plugins /plugins \
      --custom "$SLICES_C/clinvar_chr${CHROM}.vcf.gz,ClinVar,vcf,exact,0,CLNSIG,CLNREVSTAT,CLNDN,CLNVC,CLNVI" \
      --plugin "SpliceAI,snv=$SLICES_C/spliceai_chr${CHROM}.vcf.gz,indel=$SLICES_C/spliceai_chr${CHROM}.vcf.gz" \
      --plugin "CADD,snv=$SLICES_C/cadd_snv_chr${CHROM}.tsv.gz,indels=$SLICES_C/cadd_indel_chr${CHROM}.tsv.gz" \
      --plugin "AlphaMissense,file=$SLICES_C/alphamissense_chr${CHROM}.tsv.gz" \
      --plugin "dbNSFP,$SLICES_C/dbNSFP5.3.1a_grch38_chr${CHROM}.gz,$DBNSFP_COLS"

# ---------------------------------------------------------------------------
# 4. Index, and check the plugin fields actually landed
# ---------------------------------------------------------------------------
# A plugin that fails to instantiate only WARNS — its fields disappear from the
# CSQ header and the run still exits 0. Fail loudly here instead.
bgzip -f -c "$OUT" > "$OUT.gz"
tabix -f -p vcf "$OUT.gz"

n_plugin=$(tabix -H "$OUT.gz" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -cE \
  'SpliceAI_pred|CADD_(RAW|PHRED|raw|phred)|am_(class|pathogenicity)|ClinVar|SIFT4G|Polyphen2|MutationTaster|PROVEAN|VEST4|MetaSVM|MetaLR|REVEL|GERP|phyloP|phastCons')
if [[ "$n_plugin" -ne 38 ]]; then
  echo "ERROR: expected 38 plugin CSQ fields, found $n_plugin — a plugin failed to load" >&2
  grep -i 'failed to instantiate' "${OUT}_warnings.txt" >&2 || true
  exit 1
fi
plugin_provenance > "$OUT.gz.plugins"
echo "OK: chr${CHROM} — $(grep -vc '^#' "$OUT") records, $n_plugin plugin CSQ fields"
echo "     $OUT.gz"

if [[ "${VEP_KEEP_PLAIN:-1}" == "0" ]]; then
  rm -f "$OUT"
fi
