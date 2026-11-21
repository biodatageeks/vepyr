#!/bin/zsh
# Whole-data check of the CSQ fields that are ever filled, per tool and cache (2026-11-20).
# Usage: scan_all.zsh golden|fastVEP|vep-rs
#   golden   the stored VEP 116.0 outputs: HG002 (merged and Ensembl cache) and ClinVar (merged cache), every chromosome
#   fastVEP  fastVEP 0.6.1 on every chromosome of HG002 with the Ensembl and the merged cache
#   vep-rs   vep-rs 0.5.0, the same
# Each engine runs with one worker and writes into a named pipe that fieldscan.py reads, so no output file is
# written. Everything runs at background priority (efficiency cores), and a chromosome starts only when at least
# FREE_MIN GiB of memory are free, so that a benchmark running at the same time keeps its free memory.
# Results: whole/<set>/<chromosome>.json (resumable); summarise with aggregate.py.
R=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
D=$R/fields-filled
M=/Users/mwiewior/research/benchmarks/merged-engine-comparison-20261107/all-chromosomes-w8
W=/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/hg002-w8
FA=/Users/mwiewior/workspace/data_vepyr/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
PY=/Users/mwiewior/research/git/vepyr/.venv/bin/python
what=$1
FREE_MIN=${FREE_MIN:-16}
[ "$what" = vep-rs ] && FREE_MIN=${FREE_MIN_VEPRS:-20}
FIFO=$D/whole/.pipe-$what
mkdir -p $D/whole
wait_free() {
  while [ "$(vm_stat | awk '/Pages free/{printf "%d", $3*16384/1073741824}')" -lt $FREE_MIN ]; do /bin/sleep 10; done
}
if [ "$what" = golden ]; then
  for set in "vep-merged-HG002 $M/HG002" "vep-ensembl-HG002 $W/HG002" "vep-merged-ClinVar $M/ClinVar"; do
    name=${set%% *}; dir=${set##* }
    mkdir -p $D/whole/$name
    for c in $(seq 1 22) X Y MT; do
      f=$dir/chr$c/inputs/golden.vcf.gz
      [ -f $f ] || continue
      [ -s $D/whole/$name/$c.json ] && continue
      wait_free
      taskpolicy -b gzip -dc $f | taskpolicy -b $PY -I $D/fieldscan.py $D/whole/$name/$c.json.tmp && mv $D/whole/$name/$c.json.tmp $D/whole/$name/$c.json
    done
    echo "[$(date +%H:%M:%S)] $name done"
  done
  exit 0
fi
rm -f $FIFO; mkfifo $FIFO
for cache in ensembl merged; do
  name=$what-$cache-HG002
  mkdir -p $D/whole/$name
  for c in $(seq 1 22); do
    [ -s $D/whole/$name/$c.json ] && continue
    if [ $cache = merged ]; then root=$M/caches/chr$c; in=$M/HG002/chr$c/prepared/HG002/input.vcf.gz
    else root=$W/caches/chr$c; in=$W/HG002/chr$c/prepared/HG002/input.vcf.gz; fi
    wait_free
    if [ "$what" = fastVEP ]; then
      (env -u CONDA_PREFIX -u VIRTUAL_ENV -u PYTHONPATH RAYON_NUM_THREADS=1 taskpolicy -b $R/bin/fastVEP-0.6.1 annotate \
        --transcript-cache $root/fastVEP-main-merged116.cache --cache-dir $root/cache-variation --fasta $FA --hgvs --symbol \
        --canonical --distance 5000 --no-progress --output-format vcf -i $in -o $FIFO --everything > $D/whole/$name/$c.log 2>&1
       echo "rc=$?" >> $D/whole/$name/$c.log) &
    else
      flavour=(); extra=()
      [ $cache = merged ] && flavour=(--merged) && extra=(--use_transcript_ref)
      (env -u CONDA_PREFIX -u VIRTUAL_ENV -u PYTHONPATH taskpolicy -b $R/bin/vep-rs-0.5.0 --json_cache $root/cache-veprs \
        --fasta $FA --assembly GRCh38 $flavour --fork 1 --decompression_threads 1 --buffer_size 5000 --everything $extra \
        --vcf --max_sv_size -1 --no_stats --force_overwrite --quiet -i $in -o $FIFO > $D/whole/$name/$c.log 2>&1
       echo "rc=$?" >> $D/whole/$name/$c.log) &
    fi
    taskpolicy -b $PY -I $D/fieldscan.py $D/whole/$name/$c.json.tmp < $FIFO
    wait
    if grep -q "^rc=0" $D/whole/$name/$c.log; then mv $D/whole/$name/$c.json.tmp $D/whole/$name/$c.json
    else echo "[$(date +%H:%M:%S)] FAILED $name chr$c: $(tail -2 $D/whole/$name/$c.log | tr '\n' ' ')"; fi
  done
  echo "[$(date +%H:%M:%S)] $name done"
done
rm -f $FIFO
