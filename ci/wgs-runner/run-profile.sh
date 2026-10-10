#!/usr/bin/env bash
# /parity-wgs: one profile, inside a fresh container. Repo at /src (read-only),
# data at /data (read-only), scratch at /work. See ci/wgs-runner/README.md.
#
# Exit codes: 0 parity match, 1 md5 mismatch, 3 stale staged data, anything
# else (including a run_comparison crash) is an error.
set -uo pipefail
[ $# -eq 2 ] || { echo "usage: run-profile.sh <profile> <wheel-path>" >&2; exit 2; }
profile="$1"; wheel="$2"
cd /src || exit 2
mkdir -p /work/tmp
export TMPDIR=/work/tmp
python3.12 e2e-testing/scripts/wgs_manifest.py check --data /data --manifest e2e-testing/golden/wgs-manifest.json
rc=$?
if [ "$rc" -ne 0 ]; then
  if [ "$rc" -eq 3 ]; then
    msg="staged data differs from the manifest; re-run ci/wgs-runner/stage-data.sh"; code=3
  else
    msg="data check crashed (exit $rc); inspect /data/STAGED.json and re-run ci/wgs-runner/stage-data.sh"; code=4
  fi
  echo "$msg" > /work/summary.txt
  python3.12 ci/ci_helpers.py result --name "wgs/$profile" --outcome error \
    --summary "$msg" --out /work/result.json
  exit "$code"
fi
uv venv --python python3.12 /work/venv >/dev/null
uv pip install --python /work/venv/bin/python "$wheel" "huggingface_hub==1.28.0"
export PATH="/work/venv/bin:$PATH"
/work/venv/bin/python e2e-testing/scripts/run_comparison.py --release 116 --profile "$profile" \
  --comparison-mode md5 --md5-mode both --bgzf --workers "$(nproc)" \
  --output-dir /work/out 2>&1 | tee /work/run.log
rc=${PIPESTATUS[0]}
# Table rows look like "  chr1   1,234  ok  DIFFER" (contigs are canonicalised to chr*).
grep -E 'md5 (strict|canonical)|^  chr[0-9XYM]+ |MD5 concordance|PASS|FAIL|no md5 evidence' /work/run.log | tail -80 > /work/summary.txt || true
# run_comparison returns 1 for BOTH an md5 mismatch and a crashed contig (2 for
# usage/provenance errors). Only the md5 table's "FAIL: body digest differs"
# line marks a real mismatch; a bare 1 without it is an error.
case "$rc" in
  0) outcome=success; code=0 ;;
  1)
    if grep -q 'FAIL: body digest differs' /work/summary.txt; then
      outcome=failure; code=1
    else
      outcome=error; code=4
    fi ;;
  *) outcome=error; code="$rc" ;;
esac
if [ "$outcome" = error ]; then
  summary="$(grep -m1 'FAILED contigs' /work/run.log | xargs || true)"
  [ -n "$summary" ] || summary="run_comparison exit $rc"
else
  summary="$(grep -m1 -E 'PASS:|FAIL:' /work/summary.txt | xargs || true)"
  [ -n "$summary" ] || summary="run_comparison exit $rc"
fi
python3.12 ci/ci_helpers.py result --name "wgs/$profile" --outcome "$outcome" \
  --summary "$summary" --out /work/result.json
exit "$code"
