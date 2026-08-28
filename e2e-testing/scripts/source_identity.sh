#!/usr/bin/env bash
# Shared between build_vep_plugin_reference.sh (which records) and
# generate_vep_plugin_references.sh (which validates), so the two cannot drift.
#
# A reference's CSQ bytes depend on the plugin *data* as much as the plugin
# code. Several sources are rolling -- ClinVar most obviously -- and keep the
# same filename as their contents change, so a filename alone proves nothing.
# Identify each source by what the server reports for it: ETag when present,
# otherwise Last-Modified plus Content-Length.

# The remote paths a five-plugin reference is built from.
PLUGIN_SOURCE_PATHS=(
  "clinvar/clinvar.vcf.gz"
  "spliceai/spliceai_scores.masked.snv.ensembl_mane.grch38.110.vcf.gz"
  "alphamissense/AlphaMissense_hg38.bgz.tsv.gz"
  "dbnsfp/dbNSFP5.3.1a_grch38.gz"
  "cadd/whole_genome_SNVs.tsv.gz"
  "cadd/gnomad.genomes.r4.0.indel.tsv.gz"
)

# source_identity <base_url> <remote_path>
# Prints "<remote_path> <identity>", or returns non-zero if the server cannot
# be reached -- callers decide whether that is fatal.
source_identity() {
  local base="$1" path="$2" headers etag modified length identity
  headers="$(curl --fail --silent --show-error --head "${base%/}/${path}" 2>/dev/null)" || return 1
  etag="$(printf '%s' "$headers" | tr -d '\r' | awk 'tolower($1) == "etag:" {print $2; exit}')"
  modified="$(printf '%s' "$headers" | tr -d '\r' | awk 'tolower($1) == "last-modified:" {$1=""; sub(/^ /,""); print; exit}')"
  length="$(printf '%s' "$headers" | tr -d '\r' | awk 'tolower($1) == "content-length:" {print $2; exit}')"
  if [[ -n "$etag" ]]; then
    identity="etag=$etag"
  elif [[ -n "$modified" || -n "$length" ]]; then
    identity="mtime=${modified:-?};size=${length:-?}"
  else
    return 1
  fi
  printf '%s %s\n' "$path" "$identity"
}

# all_source_identities <base_url> -- one "SOURCE <path> <identity>" per line.
all_source_identities() {
  local base="$1" path line
  for path in "${PLUGIN_SOURCE_PATHS[@]}"; do
    line="$(source_identity "$base" "$path")" || return 1
    printf 'SOURCE %s\n' "$line"
  done
}
