#!/bin/zsh
# Second, undisturbed set (see README.txt). Whole-genome runs of the vepyr 0.11.0 PyPI wheel on macOS (supplement Section 8.2, merged cache, plain VCF output).
# One discarded run with 8 workers, then 8, 4, 2 and 1 workers, one run each, measured by the repository's runner
# (whole Python process, wait4). After each run the record lines are checksummed per contig without the file cache
# and the 29 GiB output is deleted. Results: wgs/<label>.{time.txt,metrics.json,md5.json}, wgs/memlog.csv.
B=/Users/mwiewior/research/benchmarks/vepyr-0110-supplement-20261121
E=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
DATA=/Users/mwiewior/workspace/data_vepyr
PY=/Users/mwiewior/workspace/venvs/vepyr-0.11.0/bin/python
PYS=/Users/mwiewior/research/git/vepyr/.venv/bin/python
S=/Users/mwiewior/research/benchmarks/local-gate/vepyr-build/performance-tests/vepyr/scripts
SSD=$DATA/output/116/vepyr_merged_worker_scaling_v0110
ARC=$DATA/archive/116/vepyr_merged_worker_scaling_v0110
export VEPYR_EXPECTED_VERSION=0.11.0
mem_free() { vm_stat | awk '/Pages free/{printf "%d", $3*16384/1073741824}'; }
ensure_free() { # label limit
  local have=$(mem_free) released=0
  if [ $have -lt $2 ]; then
    released=27
    $PYS -I -c 'import sys
n = int(sys.argv[1]) << 30
b = bytearray(n)
for i in range(0, n, 16384):
    b[i] = 1' $released
  fi
  echo "$(date +%H:%M:%S),$1,$have,$released,$(mem_free)" >> $B/wgs2/memlog.csv
}
run_one() { # label workers limit
  ensure_free $1 $3
  echo "[$(date +%H:%M:%S)] $1 (workers=$2), power: $(pmset -g batt | head -1 | cut -d"'" -f2)"
  $PY -P $S/run_vepyr_worker_scaling.py --input-vcf $DATA/input/HG002_normalized.vcf.gz \
    --cache-dir $DATA/cache/116_GRCh38_merged --cache-type merged \
    --reference-fasta $DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    --ssd-output-dir $SSD --archive-dir $ARC --expected-records 4096123 --compression plain --force \
    --workers $2 || { echo "FAILED $1"; return 1; }
  cp $ARC/merged_workers$2.time.txt $B/wgs2/$1.time.txt
  cp $ARC/merged_workers$2.metrics.json $B/wgs2/$1.metrics.json
  $PYS -I $E/scripts/contig_md5.py $ARC/HG002_annotated_wgs_everything_hgvs_vepyr_merged_workers$2.vcf $B/wgs2/$1.md5.json
  rm -f "${ARC:?}/HG002_annotated_wgs_everything_hgvs_vepyr_merged_workers$2.vcf"
  grep -a 'Elapsed\|Maximum resident' $B/wgs2/$1.time.txt
}
$PY -c 'import vepyr, polars; print("vepyr", vepyr.__version__, "polars", polars.__version__)'
run_one warmup 8 20 || exit 1
for w in 8 4 2 1; do run_one w$w $w 8 || exit 1; done
cp $ARC/summary.tsv $B/wgs2/summary.tsv
echo "[$(date +%H:%M:%S)] finished"
