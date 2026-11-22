#!/bin/zsh
# Whole-genome tiering comparison, one chromosome at a time (run in your own terminal: it needs sudo
# for `purge` before every cold run). Re-running skips chromosomes that already have a summary.
#   ./run_wgs_tiering.zsh [DATA_DIR]      (default ~/workspace/data_vepyr/cache/tiering-gain)
set -e
R=${0:A:h}
D=${1:-$HOME/workspace/data_vepyr/cache/tiering-gain}
W=$D/wgs
PY=${PY:-$HOME/research/git/vepyr/.venv/bin/python}
mkdir -p $W
for L in tiered sorted; do
  if ! grep -q '"chr22"' $D/$L/layout.json 2>/dev/null; then
    echo "the $L cache for chr1-22 is not ready yet ($D/$L/layout.json missing); wait for the preparation"; exit 1
  fi
done
sudo -v
# Keep the sudo timestamp fresh and the Mac awake while this script runs.
( while kill -0 $$ 2>/dev/null; do sudo -n true; sleep 50; done ) 2>/dev/null &
caffeinate -dimsu -w $$ &
for c in chr{1..22}; do
  if [[ -f $W/$c/summary.json ]]; then echo "$c: already done"; continue; fi
  n=$(<$D/inputs/HG002_$c.count)
  echo "$(date +%H:%M:%S) $c: $n records"
  $PY $R/run_tiering_gain.py --cache-root $D --input-vcf $D/inputs/HG002_$c.vcf.gz \
    --expected-records $n --contig $c --workdir $W/$c --workers 1 8 --repeats 3 --cold 2 \
    --min-disk-gib 20 > $W/$c.log 2>&1 || { echo "$c FAILED, see $W/$c.log"; exit 1; }
  $PY $R/aggregate_wgs.py $W > /dev/null
  grep -E "^\| ($c|\*\*total)" $W/wgs_summary.md
done
$PY $R/aggregate_wgs.py $W
echo "done: $W/wgs_summary.md"
