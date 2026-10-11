#!/usr/bin/env bash
# Re-time the vepyr side of the Polars benchmarks on vepyr 0.9.0 (PyPI wheel).
# Run from performance-tests/polars-integration. VEP + filter_vep rows of B are
# reused from 20260926_master; A, B_sliced and vep/ were copied from that run.
set -euo pipefail
cd "$(dirname "$0")/../../.."

export DATA_VEPYR_DIR=~/workspace/data_vepyr PIBENCH_MAX_LOAD=4
PY=/Users/mwiewior/research/git/vepyr/.venv/bin/python
RUN=outputs/116/macos_Mareks-MacBook-Pro_chr22_20261011_v090
OLD=outputs/116/macos_Mareks-MacBook-Pro_chr22_20260926_master
DEST=${DEST:-$RUN/supplement}

"$PY" -c 'import vepyr; assert vepyr.__version__ == "0.9.0", vepyr.__version__'
uptime; docker ps --format '{{.Names}}'

"$PY" scripts/verify.py --run-dir $RUN 2>&1 | tee $RUN/verify.log
"$PY" - "$RUN/verify.json" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
bad = {q: v["pass"] for q, v in d.items() if not all(v["pass"].values())}
if len(d) != 18 or bad:
    sys.exit(f"parity gate failed: {len(d)} queries verified, failing: {bad}")
print("parity gate: all 18 queries pass")
EOF
"$PY" scripts/run_pushdown.py --run-dir $RUN 2>&1 | tee $RUN/C.log
"$PY" scripts/run_e2e.py --run-dir $RUN --reuse-filter-vep $OLD 2>&1 | tee $RUN/B.log
"$PY" scripts/make_supplement.py --run-dir $RUN --dest "$DEST" 2>&1 | tee $RUN/make_supplement.log
echo "done: tables and figures in $DEST"
