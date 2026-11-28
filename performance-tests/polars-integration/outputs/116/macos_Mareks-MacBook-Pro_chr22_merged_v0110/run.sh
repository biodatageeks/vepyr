#!/usr/bin/env bash
# Supplement Sections 8.3 and 8.5 on the vepyr 0.11.0 PyPI wheel (polars 1.44.2, polars-bio 0.38.0).
#
# Only vepyr is measured. Docker was not available on the host, so every VEP-side result is reused from
# ..._chr22_merged_v0100 (same host, VEP image, cache and plugin data): VEP annotation (vep/), VEP + filter_vep
# (B/*_vep_fork*.json, through --reuse-filter-vep), the tabix-sliced VEP baseline (B_sliced/) and the filter-only
# experiment (A/), which does not involve vepyr. The VEP outputs that verify.py compares against no longer exist,
# so the parity gate of the 0.10.0 run (verify.json, copied) is carried over by equiv.py: for each of the 18
# queries and each output path, the result of vepyr 0.11.0 must equal that of vepyr 0.10.0.
#
# Usage: run.sh sweep | prepare | equiv | pushdown | e2e | tables   (in this order)
set -euo pipefail
cd "$(dirname "$0")/../../.."

export DATA_VEPYR_DIR=~/workspace/data_vepyr PIBENCH_WORK=~/workspace/data_vepyr/polars_integration/chr22_v0110 PIBENCH_MAX_LOAD=4 PIBENCH_LOAD_TIMEOUT=21600
PY=/Users/mwiewior/workspace/venvs/vepyr-0.11.0/bin/python
PY_OLD=/Users/mwiewior/workspace/venvs/vepyr-0.10.0/bin/python
VEPYR_EXPECTED=${VEPYR_EXPECTED:-0.11.0}
RUN=outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110
OLD=outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0100
TREE=/Users/mwiewior/research/benchmarks/local-gate/vepyr-build   # vepyr at tag 0.11.0, for the sweep script
DEST=${DEST:-$RUN/supplement}
STAGE=${1:?stage: sweep | prepare | equiv | pushdown | e2e | tables}

"$PY" -c "import vepyr, sys; v = vepyr.__version__; sys.exit(None if v == '$VEPYR_EXPECTED' else f'vepyr {v}, expected $VEPYR_EXPECTED')"
echo "[$(date +%H:%M:%S)] stage $STAGE; $(uptime)"

case $STAGE in
sweep)
  # 8.3: worker sweep on the normalized input (one timed run per configuration)
  for c in 22 1; do
    [[ -s $RUN/workers_chr$c/report.json ]] && { echo "8.3 chr$c: kept"; continue; }
    "$PY" $TREE/e2e-testing/scripts/lazyframe_workers_parity.py \
      --release 116 --input $DATA_VEPYR_DIR/input/HG002_normalized.vcf.gz \
      --chrom $c --sweep 1 2 4 8 --profiles ensembl merged refseq \
      --out-dir $RUN/workers_chr$c 2>&1 | tee $RUN/workers_chr$c.log
  done ;;
prepare)
  bash scripts/prepare_chr22.sh 2>&1 | tee $RUN/prepare.log
  mkdir -p $RUN/vep $RUN/A $RUN/B_sliced
  cp $OLD/vep/*.json $RUN/vep/
  cp $OLD/A/*.json $RUN/A/
  cp $OLD/B_sliced/*.json $RUN/B_sliced/
  cp $OLD/verify.json $RUN/verify.json
  "$PY" - "$RUN/env.json" <<'PYEOF'
import importlib.metadata as md, json, os, platform, subprocess, sys
from datetime import datetime, timezone
json.dump({"date_utc": datetime.now(timezone.utc).isoformat(), "host": platform.node(), "os": platform.platform(),
           "cpu": subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip(),
           "ncpu": os.cpu_count(), "python": sys.version.split()[0], "vepyr": md.version("vepyr"),
           "polars": md.version("polars"), "polars_bio": md.version("polars-bio"),
           "vep_side": "reused from macos_Mareks-MacBook-Pro_chr22_merged_v0100 (Docker not available)"},
          open(sys.argv[1], "w"), indent=2)
PYEOF
  ;;
equiv)
  "$PY" $RUN/equiv.py "$PY" "$PY_OLD" $RUN 2>&1 | tee $RUN/equiv.log ;;
pushdown)
  "$PY" scripts/run_pushdown.py --run-dir $RUN 2>&1 | tee $RUN/C.log ;;
e2e)
  "$PY" scripts/run_e2e.py --run-dir $RUN --reuse-filter-vep $OLD 2>&1 | tee $RUN/B.log ;;
tables)
  "$PY" scripts/make_supplement.py --run-dir $RUN --dest "$DEST" 2>&1 | tee $RUN/make_supplement.log
  echo "done: tables and figures in $DEST" ;;
*) echo "unknown stage $STAGE" >&2; exit 2 ;;
esac
echo "[$(date +%H:%M:%S)] stage $STAGE finished"
