#!/usr/bin/env bash
# Smoke check of the runner host: image present, staged data consistent, disk.
# Usage: check.sh [data root] ["<selectors>"]. The selectors are those of
# /parity-caches and stage-cache-inputs.sh; they add opt-in plugins to the
# cache parity inputs that are checked.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; repo="$(cd "$here/../.." && pwd)"
data="${1:-/data/vepyr}"
selectors="${2:-}"
tag="$(cat "$here/IMAGE")"
docker image inspect "$tag" >/dev/null || { echo "image $tag missing: run build-image.sh"; exit 1; }
python3 "$repo/e2e-testing/scripts/wgs_manifest.py" check --data "$data" --manifest "$repo/e2e-testing/golden/wgs-manifest.json"
# Egress firewall (README step 5): fail if the rules are missing, warn if
# they cannot be read (needs root or passwordless sudo).
ipt() { iptables "$@" 2>/dev/null || sudo -n iptables "$@" 2>/dev/null; }
ipt6() { ip6tables "$@" 2>/dev/null || sudo -n ip6tables "$@" 2>/dev/null; }
if user_rules=$(ipt -S DOCKER-USER) && input_rules=$(ipt -S INPUT) && input6_rules=$(ipt6 -S INPUT); then
  missing=()
  for net in 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 169.254.0.0/16 100.64.0.0/10; do
    grep -- "-d $net" <<<"$user_rules" | grep -- "-i docker0" | grep -q -- "-j DROP" || missing+=("DOCKER-USER $net")
  done
  grep -- "-i docker0" <<<"$input_rules" | grep -v -- " -d \| -p \| -s " | grep -q -- "-j DROP" || missing+=("INPUT docker0")
  grep -- "-i docker0" <<<"$input6_rules" | grep -v -- " -d \| -p \| -s " | grep -q -- "-j DROP" || missing+=("ip6tables INPUT docker0")
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "egress firewall missing: ${missing[*]}; run sudo ci/wgs-runner/egress-firewall.sh"; exit 1
  fi
else
  echo "WARNING: cannot read iptables (not root, no passwordless sudo); verify the egress firewall by hand: sudo iptables -S DOCKER-USER; sudo iptables -S INPUT; sudo ip6tables -S INPUT" >&2
fi
mkdir -p /scratch/vepyr-wgs
avail=$(df -Pk /scratch/vepyr-wgs | awk 'NR==2 {print $4}')
[ "$avail" -gt $((20*1024*1024)) ] || { echo "less than 20 GB free under /scratch"; exit 1; }
# /parity-caches inputs: what a job checks before it builds (staged caches at
# the pinned revisions, raw caches, plugin sources, scratch per target), so that
# a missing input shows here and not as a job that ends with exit code 3. A
# host that stage-cache-inputs.sh never ran on serves /parity-wgs only.
if [ -d "$data/vep-cache" ] || [ -d "$data/plugin-cache" ] || [ -d "$data/plugin-source" ]; then
  python3 "$repo/ci/ci_helpers.py" cache-inputs --data "$data" --scratch /scratch/vepyr-wgs \
    --published "$repo/src/vepyr/_published.py" \
    --manifest "$repo/e2e-testing/golden/cache-parity-manifest.json" --select "$selectors"
else
  echo "cache parity inputs not staged: /parity-caches jobs would exit 3 here (run stage-cache-inputs.sh)"
fi
echo "runner OK: $tag, data consistent, $((avail/1024/1024)) GB free"
