#!/bin/zsh
# Continuation of chain.zsh after the equivalence stage stopped on the extra column of 0.11.0 (_vcf_raw_info):
# compare the existing outputs again with the corrected equiv.py, then pushdown (C), end to end (B) and tables.
B=/Users/mwiewior/research/benchmarks/vepyr-0110-supplement-20261121
P=/Users/mwiewior/research/git/vepyr/.worktrees/polars-0110/performance-tests/polars-integration
RUN=$P/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110
step() { echo "[$(date +%H:%M:%S)] $1 ($(pmset -g batt | head -1 | cut -d"'" -f2), $(pmset -g batt | grep -o '[0-9]*%'))"; }
step "polars equiv (compare only)"; EQUIV_REUSE=1 bash $RUN/run.sh equiv > $B/polars-equiv2.log 2>&1 || { echo "FAILED polars equiv"; tail -3 $B/polars-equiv2.log; exit 1; }
for s in pushdown e2e tables; do
  step "polars $s"; bash $RUN/run.sh $s > $B/polars-$s.log 2>&1 || { echo "FAILED polars $s"; tail -5 $B/polars-$s.log; exit 1; }
done
step finished
