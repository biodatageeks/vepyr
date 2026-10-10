#!/usr/bin/env bash
# One-time (and on manifest change) staging of the /parity-wgs data on the
# runner host. Usage: stage-data.sh --goldens-from <rsync-source> [--data /data/vepyr]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; repo="$(cd "$here/../.." && pwd)"
manifest="$repo/e2e-testing/golden/wgs-manifest.json"
DATA=/data/vepyr
GOLDENS_FROM=""
while [ $# -gt 0 ]; do
  case "$1" in
    --goldens-from) GOLDENS_FROM="$2"; shift 2 ;;
    --data) DATA="$2"; shift 2 ;;
    *) echo "usage: $0 --goldens-from <rsync-source> [--data /data/vepyr]" >&2; exit 2 ;;
  esac
done
[ -n "$GOLDENS_FROM" ] || { echo "--goldens-from is required" >&2; exit 2; }

mf() { python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))" "$manifest" "$1"; }
check_sha() { # file expected-sha256
  local got
  got="$(sha256sum "$1" | awk '{print $1}')"
  [ "$got" = "$2" ] || { echo "sha256 mismatch for $1: got $got, want $2" >&2; exit 1; }
}

# 1. layout (a previous run leaves $DATA read-only)
[ ! -d "$DATA" ] || chmod -R u+w "$DATA"
mkdir -p "$DATA/cache" "$DATA/input" "$DATA/output/116"

# 2. caches
for flavour in $(mf "' '.join(sorted(d['caches']))"); do
  repo_id="$(mf "d['caches']['$flavour']['repo_id']")"
  rev="$(mf "d['caches']['$flavour']['revision']")"
  uvx --from huggingface_hub hf download "$repo_id" --repo-type dataset \
    --revision "$rev" --local-dir "$DATA/cache/116_GRCh38_$flavour"
done

# 3. reference FASTA
pin() { python3 - "$repo/porting-tests/PINS.toml" "$1" <<'PY'
import sys, tomllib
print(tomllib.load(open(sys.argv[1], "rb"))["grch38_fasta"][sys.argv[2]])
PY
}
fa="$DATA/$(mf "d['fasta']")"
if [ ! -f "$fa" ]; then
  gz="$fa.gz"
  curl -fL --retry 3 -o "$gz" "$(pin repo)/$(pin ref)"
  [ "$(sum "$gz" | awk '{print $1, $2}')" = "$(pin ensembl_sum)" ] \
    || { echo "Ensembl checksum mismatch for $gz" >&2; exit 1; }
  gunzip "$gz"
fi
PYTHONPATH="$repo/porting-tests/tools" python3 -c \
  'import sys; from pathlib import Path; from run_tests.fetch import write_fai; write_fai(Path(sys.argv[1]))' "$fa"
check_sha "$fa" "$(pin sha)"

# 4. HG002 VCF + index
giab=https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38
for f in HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi; do
  [ -f "$DATA/input/$f" ] || curl -fL --retry 3 -o "$DATA/input/$f" "$giab/$f"
  check_sha "$DATA/input/$f" "$(mf "d['inputs']['input/$f']['sha256']")"
done

# 5. goldens (+ tbi) from the host that holds the VEP reference runs
rsync -a --progress \
  --include='HG002_annotated_wgs_everything_hgvs_vep.vcf.gz*' \
  --include='HG002_annotated_wgs_everything_hgvs_merged.vcf.gz*' \
  --include='HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz*' \
  --exclude='*' "$GOLDENS_FROM/" "$DATA/output/116/"
for p in $(mf "' '.join(sorted(d['profiles']))"); do
  g="$(mf "d['profiles']['$p']['golden']")"
  check_sha "$DATA/$g" "$(mf "d['profiles']['$p']['sha256']")"
  check_sha "$DATA/$g.tbi" "$(mf "d['inputs']['$g.tbi']['sha256']")"
done

# 6. record what was staged
python3 "$repo/e2e-testing/scripts/wgs_manifest.py" record-staged --data "$DATA" --manifest "$manifest" --out "$DATA/STAGED.json"

# 7. read-only from here on (best effort), then totals
chmod -R a-w "$DATA" || true
du -sh "$DATA"/* 
echo "staged under $DATA"
