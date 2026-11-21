#!/usr/bin/env bash
# Staging of the /parity-caches inputs on the runner host, next to the
# /parity-wgs data. Run stage-data.sh first: it stages the published core
# caches, which /parity-caches compares against and takes plugin tiers from.
# Usage: stage-cache-inputs.sh [--data /data/vepyr] [--select "<selectors>"]
# Selectors are those of /parity-caches (core, plugins, a cache type, a plugin
# name); none stages every default target. An opt-in plugin (CADD, SpliceAI)
# is staged only when named. Idempotent: verified files are kept.
#
# Stages, under --data:
#   vep-cache/<species dir>/<release>_<assembly>/   raw Ensembl VEP cache (core targets)
#   plugin-cache/<release>_<assembly>/plugin/<p>/   published plugin cache at the pin
#   plugin-source/<p>/<file> (+ index)              the files its manifest.json lists
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; repo="$(cd "$here/../.." && pwd)"
manifest="$repo/e2e-testing/golden/cache-parity-manifest.json"
published_py="$repo/src/vepyr/_published.py"
DATA=/data/vepyr
SELECT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --data) DATA="$2"; shift 2 ;;
    --select) SELECT="$2"; shift 2 ;;
    *) echo "usage: $0 [--data /data/vepyr] [--select \"<selectors>\"]" >&2; exit 2 ;;
  esac
done

mf() { python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))" "$manifest" "$1"; }
md5_of() { md5sum "$1" | awk '{print $1}'; }
fetch() { # url dest: resumable download to .part
  [ -f "$2" ] || { curl -fL --retry 3 -C - -o "$2.part" "$1" && mv "$2.part" "$2"; }
}
release="$(mf "d['release']")"; assembly="$(mf "d['assembly']")"
selection="$(python3 "$repo/ci/ci_helpers.py" cache-targets --published "$published_py" \
  --manifest "$manifest" --select "$SELECT")"
targets="$(sed -n 's/^targets=//p' <<<"$selection" | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')"
echo "staging for: $targets"

# 1. layout (a previous run leaves these read-only)
mkdir -p "$DATA" && chmod u+w "$DATA"
for d in vep-cache plugin-cache plugin-source; do
  [ ! -d "$DATA/$d" ] || chmod -R u+w "$DATA/$d"
  mkdir -p "$DATA/$d"
done

# 2. raw Ensembl VEP caches: the archive vepyr.build_cache() downloads by
# default, unpacked where the manifest says. Ensembl's CHECKSUMS file lists
# BSD `sum` output, which catches a truncated download.
stage_raw() { # cache_type
  local rel dest parent root name url want
  rel="$(mf "d['core']['$1']['raw']")"
  dest="$DATA/$rel"
  if [ -f "$dest/info.txt" ]; then echo "raw $1 cache already at $dest"; return; fi
  parent="$(dirname "$dest")"; root="$(dirname "$parent")"
  name="$(basename "$parent")_vep_${release}_${assembly}.tar.gz"
  url="https://ftp.ensembl.org/pub/release-$release/variation/indexed_vep_cache/$name"
  mkdir -p "$root"
  fetch "$url" "$root/$name"
  want="$(curl -fsL --retry 3 "$(dirname "$url")/CHECKSUMS" | awk -v f="$name" '$3 == f {print $1, $2}')"
  if [ -z "$want" ] || [ "$(sum "$root/$name" | awk '{print $1, $2}')" != "$want" ]; then
    echo "Ensembl checksum mismatch for $root/$name (remove it to download again)" >&2; exit 1
  fi
  tar -xzf "$root/$name" -C "$root"
  [ -f "$dest/info.txt" ] || { echo "$name did not unpack to $dest" >&2; exit 1; }
  rm -f "$root/$name"
}

# 3. plugin targets: the published cache at the pinned revision (the job
# compares against this copy instead of downloading it), then every source
# file its manifest.json records, checked against the digest recorded there.
pin_of() { # plugin -> "<repo_id> <revision>"
  PYTHONPATH="$repo/ci" python3 -c '
import sys
from pathlib import Path
import ci_helpers
published, release, assembly, plugin = sys.argv[1:5]
pins = ci_helpers.published_pins(Path(published))
print(f"biodatageeks/vepyr_{release}_{assembly}_plugin_{plugin}", pins[(int(release), assembly, "plugin_" + plugin)])
' "$published_py" "$release" "$assembly" "$1"
}
stage_published() { # plugin dest
  local repo_id rev marker
  read -r repo_id rev < <(pin_of "$1")
  marker="{\"repo_id\": \"$repo_id\", \"revision\": \"$rev\"}"
  if [ -f "$2/.revision" ] && [ "$(cat "$2/.revision")" = "$marker" ]; then
    echo "published $1 cache already at $rev"; return
  fi
  rm -rf "$2.new"
  uvx --from huggingface_hub hf download "$repo_id" --repo-type dataset \
    --revision "$rev" --local-dir "$2.new"
  # .revision is the JSON identity cache_parity.py requires, as for core caches.
  printf '%s\n' "$marker" > "$2.new/.revision"
  rm -rf "$2" && mv "$2.new" "$2"
}
check_md5() { # file expected label: exit unless the digest matches
  local got; got="$(md5_of "$1")"
  [ "$got" = "$2" ] || { echo "md5 mismatch for $1 ($3): got $got, want $2" >&2; exit 1; }
}
stage_sources() { # plugin published-dir
  local plugin="$1" dir="$DATA/plugin-source/$1" file url md5 verified index index_md5 src
  mkdir -p "$dir"
  while IFS=$'\t' read -r file url md5 verified index index_md5; do
    src="$dir/$file"
    if [ "$md5" = "$verified" ]; then
      # The published build read the upstream bytes: fetch them, and the
      # index published beside them.
      if [ -f "$src" ] && [ "$md5" != - ] && [ "$(md5_of "$src")" != "$md5" ]; then
        echo "re-downloading $src (md5 differs)"; rm -f "$src"
      fi
      fetch "$url" "$src"
      [ "$md5" = - ] || check_md5 "$src" "$md5" "$url"
      if [ "$index" != - ]; then
        fetch "$url.${index##*.}" "$dir/$index"
        [ "$index_md5" = - ] || [ "$(md5_of "$dir/$index")" = "$index_md5" ] \
          || echo "WARNING: $dir/$index is not the index the published cache was built with" >&2
      fi
    elif [ -f "$src" ] && { [ "$index" = - ] || [ -f "$dir/$index" ]; }; then
      # A derived build input (see below) that is already there: BGZF output
      # is not byte-stable across htslib versions, so its digest is not checked.
      echo "keeping $src (derived from $url)"
    elif [ "$plugin" = alphamissense ]; then
      # vepyr-plugins plugins/alphamissense/README.md: the upstream file is
      # plain gzip; the build input is its BGZF re-compression plus a tabix index.
      command -v bgzip >/dev/null && command -v tabix >/dev/null \
        || { echo "bgzip and tabix are needed to prepare $src (apt install tabix)" >&2; exit 1; }
      fetch "$url" "$src.upstream"
      check_md5 "$src.upstream" "$md5" "$url"
      gzip -dc "$src.upstream" | bgzip -c > "$src.part" && mv "$src.part" "$src"
      tabix -f -s 1 -b 2 -e 2 -c '#' "$src"
      rm -f "$src.upstream"
    else
      echo "$src is derived from $url (md5 $md5) and must be prepared by hand; see the plugin's README in vepyr-plugins" >&2
      exit 1
    fi
  done < <(python3 -c '
import json, sys
for s in json.load(open(sys.argv[1])).get("sources") or []:
    index = s.get("index") or {}
    row = [s["file"], s.get("url") or "-", s.get("md5") or "-", s.get("verified_md5") or "-",
           index.get("file") or "-", index.get("verified_md5") or "-"]
    print("\t".join(row))
' "$2/manifest.json")
}

for target in $targets; do
  case "$target" in
    plugin_*)
      plugin="${target#plugin_}"
      dest="$DATA/plugin-cache/${release}_${assembly}/plugin/$plugin"
      mkdir -p "$(dirname "$dest")"
      stage_published "$plugin" "$dest"
      stage_sources "$plugin" "$dest"
      ;;
    *) stage_raw "$target" ;;
  esac
done

# 4. read-only from here on (best effort), then totals
chmod -R a-w "$DATA/vep-cache" "$DATA/plugin-cache" "$DATA/plugin-source" || true
chmod a-w "$DATA" || true
du -sh "$DATA/vep-cache"/* "$DATA/plugin-cache"/*/plugin/* "$DATA/plugin-source"/* 2>/dev/null || true
echo "staged under $DATA"
