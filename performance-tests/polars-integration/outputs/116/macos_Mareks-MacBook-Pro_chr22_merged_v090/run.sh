#!/usr/bin/env bash
# Full re-run of the Polars benchmarks with every query on the release-116 merged
# cache, vepyr from PyPI. Nothing is reused from earlier runs: VEP (core and
# plugins, --fork 0/1/3/7), the parity gate, A, C, B (including filter_vep) and
# B_sliced are all measured here. Expect well over 12 h.
# C_old_build_pre_fix/ and verify_old_build.json are carried over for the
# plugin-fix table; they cover P1-P5 only, which already used the merged cache.
set -euo pipefail
cd "$(dirname "$0")/../../.."

export DATA_VEPYR_DIR=~/workspace/data_vepyr PIBENCH_MAX_LOAD=4
PY=/Users/mwiewior/research/git/vepyr/.venv/bin/python
VEPYR_EXPECTED=${VEPYR_EXPECTED:-0.9.0}
RUN=outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v090
DEST=${DEST:-$RUN/supplement}

"$PY" -c "import vepyr, sys; v = vepyr.__version__; sys.exit(None if v == '$VEPYR_EXPECTED' else f'vepyr {v}, expected $VEPYR_EXPECTED')"
test "$(ls "$DATA_VEPYR_DIR/homo_sapiens_merged/116_GRCh38" | wc -l)" -gt 1000 \
  || { echo "VEP merged cache missing or empty" >&2; exit 1; }
uptime; docker ps --format '{{.Names}}'

bash scripts/prepare_chr22.sh 2>&1 | tee $RUN/prepare.log
"$PY" scripts/run_vep.py --run-dir $RUN 2>&1 | tee $RUN/vep.log
"$PY" scripts/verify.py --run-dir $RUN 2>&1 | tee $RUN/verify.log
"$PY" - "$RUN/verify.json" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
bad = {q: v["pass"] for q, v in d.items() if not all(v["pass"].values())}
if len(d) != 18 or bad:
    sys.exit(f"parity gate failed: {len(d)} queries verified, failing: {bad}")
print("parity gate: all 18 queries pass")
PYEOF
"$PY" scripts/run_filter_only.py --run-dir $RUN 2>&1 | tee $RUN/A.log
"$PY" scripts/run_pushdown.py --run-dir $RUN 2>&1 | tee $RUN/C.log
"$PY" scripts/run_e2e.py --run-dir $RUN 2>&1 | tee $RUN/B.log
"$PY" scripts/run_vep_sliced.py --run-dir $RUN 2>&1 | tee $RUN/B_sliced.log
"$PY" scripts/make_supplement.py --run-dir $RUN --dest "$DEST" 2>&1 | tee $RUN/make_supplement.log
echo "done: tables and figures in $DEST"
