#!/usr/bin/env bash
# Supplement Sections 8.3 and 8.5 on the vepyr 0.10.0 PyPI wheel.
#
# 8.3: LazyFrame worker sweep (chr22 and chr1, ensembl/merged/refseq, 1/2/4/8).
# 8.5: every vepyr measurement is re-run. VEP is re-run only where the earlier
# runs used the Ensembl cache: core annotation (forks 0/1/3/7), filter_vep for
# the 13 queries without plugins (A and B), and the sliced region workflow
# (B_sliced). The plugin side (VEP --merged + 5 plugins, filter_vep for P1-P5)
# is reused from ..._chr22_20260926_master, which ran on the same host.
set -euo pipefail
cd "$(dirname "$0")/../../.."

export DATA_VEPYR_DIR=~/workspace/data_vepyr PIBENCH_WORK=~/workspace/data_vepyr/polars_integration/chr22_v0100 PIBENCH_MAX_LOAD=4 PIBENCH_LOAD_TIMEOUT=21600
PY=/Users/mwiewior/workspace/venvs/vepyr-0.10.0/bin/python
VEPYR_EXPECTED=${VEPYR_EXPECTED:-0.10.0}
RUN=outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0100
OLD=outputs/116/macos_Mareks-MacBook-Pro_chr22_20260926_master
OLD_WORK=~/workspace/data_vepyr/polars_integration/chr22
DEST=${DEST:-$RUN/supplement}
CORE="Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 R1 R2 R3"
PLUG="P1 P2 P3 P4 P5"

"$PY" -c "import vepyr, sys; v = vepyr.__version__; sys.exit(None if v == '$VEPYR_EXPECTED' else f'vepyr {v}, expected $VEPYR_EXPECTED')"
uptime; docker ps --format '{{.Names}}'

# 8.3: worker sweep on the normalized input (one timed run per configuration);
# the script is taken from vepyr master d3ee7ac (0.10.0), matching the wheel
for c in 22 1; do
  [[ -s $RUN/workers_chr$c/report.json ]] && { echo "8.3 chr$c: kept"; continue; }
  "$PY" ~/research/git/_wt/master-build/e2e-testing/scripts/lazyframe_workers_parity.py \
    --release 116 --input $DATA_VEPYR_DIR/input/HG002_normalized.vcf.gz \
    --chrom $c --sweep 1 2 4 8 --profiles ensembl merged refseq \
    --out-dir $RUN/workers_chr$c 2>&1 | tee $RUN/workers_chr$c.log
done

# 8.5: inputs; plugin VEP outputs and timings reused
bash scripts/prepare_chr22.sh 2>&1 | tee $RUN/prepare.log
mkdir -p $PIBENCH_WORK/vep/plugins $RUN/vep $RUN/A
for f in 0 1 3 7; do
  ln -sfn $OLD_WORK/vep/plugins/fork$f $PIBENCH_WORK/vep/plugins/fork$f
  cp $OLD/vep/plugins_fork$f.json $RUN/vep/
done
for q in $PLUG; do cp $OLD/A/${q}_*.json $RUN/A/; done

"$PY" scripts/run_vep.py --run-dir $RUN --modes core 2>&1 | tee $RUN/vep.log
"$PY" scripts/verify.py --run-dir $RUN 2>&1 | tee $RUN/verify.log
"$PY" - "$RUN/verify.json" <<'PYEOF'
import json, sys
d = json.load(open(sys.argv[1]))
bad = {q: v["pass"] for q, v in d.items() if not all(v["pass"].values())}
if len(d) != 18 or bad:
    sys.exit(f"parity gate failed: {len(d)} queries verified, failing: {bad}")
print("parity gate: all 18 queries pass")
PYEOF
"$PY" scripts/run_filter_only.py --run-dir $RUN --queries $CORE 2>&1 | tee $RUN/A.log
"$PY" scripts/run_pushdown.py --run-dir $RUN 2>&1 | tee $RUN/C.log
"$PY" scripts/run_e2e.py --run-dir $RUN --queries $PLUG --reuse-filter-vep $OLD 2>&1 | tee $RUN/B_plugins.log
"$PY" scripts/run_e2e.py --run-dir $RUN --queries $CORE 2>&1 | tee $RUN/B_core.log
"$PY" scripts/run_vep_sliced.py --run-dir $RUN 2>&1 | tee $RUN/B_sliced.log
"$PY" scripts/make_supplement.py --run-dir $RUN --dest "$DEST" 2>&1 | tee $RUN/make_supplement.log
echo "done: tables and figures in $DEST"
