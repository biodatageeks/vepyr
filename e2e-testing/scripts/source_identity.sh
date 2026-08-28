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
#
# All or nothing: a caller that fails partway must not have already emitted
# lines for the sources it did reach. Callers use this in an `|| fallback`
# without a command substitution, so partial output would be concatenated with
# whatever the fallback prints -- producing duplicate, conflicting entries for
# the paths that succeeded before a mid-loop network blip.
all_source_identities() {
  local base="$1" path line
  local -a lines=()
  for path in "${PLUGIN_SOURCE_PATHS[@]}"; do
    line="$(source_identity "$base" "$path")" || return 1
    lines+=("SOURCE $line")
  done
  printf '%s\n' "${lines[@]}"
}

# Which remote sources each plugin's cache shard is built from. A shard is only
# reusable while these still match, so the mapping lives beside the identity
# helper rather than being restated per script.
plugin_source_paths() {
  case "$1" in
    clinvar) printf '%s\n' "clinvar/clinvar.vcf.gz" ;;
    spliceai) printf '%s\n' "spliceai/spliceai_scores.masked.snv.ensembl_mane.grch38.110.vcf.gz" ;;
    alphamissense) printf '%s\n' "alphamissense/AlphaMissense_hg38.bgz.tsv.gz" ;;
    dbnsfp) printf '%s\n' "dbnsfp/dbNSFP5.3.1a_grch38.gz" ;;
    cadd)
      printf '%s\n' "cadd/whole_genome_SNVs.tsv.gz"
      printf '%s\n' "cadd/gnomad.genomes.r4.0.indel.tsv.gz"
      ;;
    *) return 1 ;;
  esac
}

# plugin_source_identities <base_url> <plugin> -- all or nothing, as above.
plugin_source_identities() {
  local base="$1" plugin="$2" path line
  local -a paths=() lines=()
  while IFS= read -r path; do
    paths+=("$path")
  done < <(plugin_source_paths "$plugin") || return 1
  [[ ${#paths[@]} -gt 0 ]] || return 1
  for path in "${paths[@]}"; do
    line="$(source_identity "$base" "$path")" || return 1
    lines+=("SOURCE $line")
  done
  printf '%s\n' "${lines[@]}"
}
