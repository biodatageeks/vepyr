#!/bin/zsh
# Page counts per chromosome and layout (after the timed run: it reads all three caches).
#   ./run_wgs_pages.zsh [DATA_DIR]
set -e
R=${0:A:h}
D=${1:-$HOME/workspace/data_vepyr/cache/tiering-gain}
P=$D/wgs-pages
TOOL=$R/pages/target/release/pages76
PY=${PY:-$HOME/research/git/vepyr/.venv/bin/python}
for c in chr{1..22}; do
  mkdir -p $P/$c
  for L in tiered sorted original; do
    [[ -s $P/$c/${L}_ext.txt ]] && continue
    $TOOL measure $D/$L/116_GRCh38_merged/variation/$c.parquet $D/inputs/$c.variants.tsv 5000 ext payload > $P/$c/${L}_ext.txt.part
    mv $P/$c/${L}_ext.txt.part $P/$c/${L}_ext.txt
  done
  echo "$(date +%H:%M:%S) $c pages done"
done
$PY $R/aggregate_wgs.py $D/wgs --page-dir $P > /dev/null
echo "done: $D/wgs/wgs_pages.md"
