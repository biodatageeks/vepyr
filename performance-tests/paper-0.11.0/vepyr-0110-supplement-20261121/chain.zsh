#!/bin/zsh
# Timing runs of the vepyr 0.11.0 wheel for the supplement, one after another, with nothing else on the host.
# 1 worker sweep (8.3)  2 whole genome, second set (8.2)  3 writer memory (8.4), 0.10.0 first as a check of the
# method, then 0.11.0  4 Polars queries (8.5): prepare, equivalence with 0.10.0, pushdown (C), end to end (B), tables
B=/Users/mwiewior/research/benchmarks/vepyr-0110-supplement-20261121
P=/Users/mwiewior/research/git/vepyr/.worktrees/polars-0110/performance-tests/polars-integration
RUN=$P/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110
DATA=/Users/mwiewior/workspace/data_vepyr
FA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
step() { echo "[$(date +%H:%M:%S)] $1 ($(pmset -g batt | head -1 | cut -d"'" -f2), $(pmset -g batt | grep -o '[0-9]*%'))"; }
step sweep;  bash $RUN/run.sh sweep > $B/sweep.log 2>&1 || { echo "FAILED sweep"; exit 1; }
step wgs2;   zsh $B/run_wgs2.zsh > $B/wgs2/run.log 2>&1 || { echo "FAILED wgs2"; exit 1; }
CHR1=$(find $RUN/workers_chr1/input -name '*.vcf.gz' | head -1)
[ -n "$CHR1" ] || { echo "no chr1 slice"; exit 1; }
step "writer memory 0.10.0 ($CHR1)"
/Users/mwiewior/research/git/vepyr/.venv/bin/python -I $B/writer_memory.py /Users/mwiewior/workspace/venvs/vepyr-0.10.0/bin/python \
  $CHR1 $DATA/cache/116_GRCh38_ensembl $FA $B/writer/work $B/writer/v0100.json > $B/writer/v0100.log 2>&1 || { echo "FAILED writer 0.10.0"; exit 1; }
step "writer memory 0.11.0"
/Users/mwiewior/research/git/vepyr/.venv/bin/python -I $B/writer_memory.py /Users/mwiewior/workspace/venvs/vepyr-0.11.0/bin/python \
  $CHR1 $DATA/cache/116_GRCh38_ensembl $FA $B/writer/work $B/writer/v0110.json > $B/writer/v0110.log 2>&1 || { echo "FAILED writer 0.11.0"; exit 1; }
for s in prepare equiv pushdown e2e tables; do
  step "polars $s"; bash $RUN/run.sh $s > $B/polars-$s.log 2>&1 || { echo "FAILED polars $s"; tail -5 $B/polars-$s.log; exit 1; }
done
step finished
