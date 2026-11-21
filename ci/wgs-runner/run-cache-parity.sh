#!/usr/bin/env bash
# /parity-caches: one target, inside a fresh container. Repo at /src (read-only),
# data at /data (read-only), scratch at /work. See ci/wgs-runner/README.md.
# A target is a cache type (ensembl, merged, refseq) or plugin_<name>.
#
# Exit codes: 0 the rebuilt cache is the published one, 1 parity mismatch,
# 3 the runner is not ready (staged data missing or stale, scratch too small),
# anything else (a failed set-up, a cache_parity crash) is an error.
set -uo pipefail
[ $# -eq 2 ] || { echo "usage: run-cache-parity.sh <target> <wheel-path>" >&2; exit 2; }
target="$1"; wheel="$2"
cd /src || exit 2
mkdir -p /work/tmp /work/out
export TMPDIR=/work/tmp
: > /work/run.log
# A set-up step that fails ends the job here, as an error that names the step,
# not later as a missing interpreter or a failed import.
setup_failed() {
  echo "ERROR: $1" | tee /work/summary.txt
  python3.12 ci/ci_helpers.py result --name "caches/$target" --outcome error \
    --summary "ERROR: $1" --out /work/result.json
  exit 4
}
[ -f "$wheel" ] || setup_failed "wheel not found: $wheel"
uv venv --python python3.12 /work/venv 2>&1 | tee -a /work/run.log
[ "${PIPESTATUS[0]}" -eq 0 ] \
  || setup_failed "uv venv could not create the Python 3.12 environment (see run.log)"
uv pip install --python /work/venv/bin/python "$wheel" "huggingface_hub==1.28.0" 2>&1 | tee -a /work/run.log
[ "${PIPESTATUS[0]}" -eq 0 ] \
  || setup_failed "uv pip install failed for the wheel and its dependencies (see run.log)"
export PATH="/work/venv/bin:$PATH"
/work/venv/bin/python e2e-testing/scripts/cache_parity.py run --target "$target" \
  --data /data --work /work/out \
  --manifest e2e-testing/golden/cache-parity-manifest.json \
  --summary /work/summary.txt --report /work/out/reports/cache-parity.json \
  --jobs "$(nproc)" 2>&1 | tee -a /work/run.log
rc=${PIPESTATUS[0]}
# cache_parity.py ends its summary with one PASS:, FAIL: or ERROR: line. A
# Python crash also exits 1, so only a FAIL: line marks a real mismatch and
# only a PASS: line a match; anything else is an error.
verdict="$(grep -m1 -E '^(PASS|FAIL|ERROR): ' /work/summary.txt 2>/dev/null || true)"
case "$rc" in
  0)
    if [[ "$verdict" == PASS:* ]]; then outcome=success; code=0; else outcome=error; code=4; fi ;;
  1)
    if [[ "$verdict" == FAIL:* ]]; then outcome=failure; code=1; else outcome=error; code=4; fi ;;
  *) outcome=error; code="$rc" ;;
esac
[ -n "$verdict" ] || verdict="cache_parity exit $rc"
[ -s /work/summary.txt ] || echo "$verdict" > /work/summary.txt
python3.12 ci/ci_helpers.py result --name "caches/$target" --outcome "$outcome" \
  --summary "$verdict" --out /work/result.json
exit "$code"
