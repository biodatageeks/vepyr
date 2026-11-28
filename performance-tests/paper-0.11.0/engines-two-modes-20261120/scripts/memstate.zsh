#!/bin/zsh
# Probe: does the wall time of the sentinel run (fastVEP 0.6.1, HG002 chr1, merged cache, 8 threads) depend on
# the number of free pages? Steps are given as arguments and executed in order:
#   sent          one sentinel run; prints free memory before the run and wall/user/sys/RSS
#   fill          read files that the sentinel does not use until under 1 GiB of memory is free (fills the file cache)
#   release <G>   allocate and touch <G> GiB of anonymous memory, then exit (turns file cache into free pages)
#   snap          print the memory state
# Results are appended to memstate.log beside runs.csv.
R=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
M=/Users/mwiewior/research/benchmarks/merged-engine-comparison-20261107/all-chromosomes-w8
DATA=/Users/mwiewior/workspace/data_vepyr
FA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
PYS=/Users/mwiewior/research/git/vepyr/.venv/bin/python
OUT=$R/out.noindex/probe.vcf
LOG=$R/memstate.log
free_gib() { vm_stat | awk '/Pages free/{printf "%.1f", $3*16384/2^30}'; }
snap() {
  vm_stat | awk '/Pages free/{f=$3} /Pages inactive/{i=$3} /File-backed/{fb=$3} /Anonymous pages/{a=$3}
    END{printf "free %.1f GiB, inactive %.1f, file-backed %.1f, anonymous %.1f", f*16384/2^30, i*16384/2^30, fb*16384/2^30, a*16384/2^30}'
}
say() { echo "$(date +%H:%M:%S) $*" | tee -a $LOG; }
sent() {
  rm -f "${R:?}/out.noindex/probe.vcf"
  local before=$(free_gib) root=$M/caches/chr1
  /usr/bin/time -l env -u CONDA_PREFIX -u VIRTUAL_ENV -u PYTHONPATH -u RUST_LOG RAYON_NUM_THREADS=8 POLARS_MAX_THREADS=8 \
    OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    $R/bin/fastVEP-0.6.1 annotate --transcript-cache $root/fastVEP-main-merged116.cache --cache-dir $root/cache-variation \
    --fasta $FA --hgvs --symbol --canonical --distance 5000 --no-progress --output-format vcf \
    -i $M/HG002/chr1/prepared/HG002/input.vcf.gz -o $OUT --everything > /dev/null 2> $OUT.time || { say "sentinel FAILED"; return 1; }
  say "sent  free before $before GiB, after $(free_gib) GiB: $(awk '/ real /{w=$1;u=$3;s=$5} /maximum resident set size/{r=$1/2^30} END{printf "wall %.2f user %.2f sys %.2f rss %.2f GiB", w,u,s,r}' $OUT.time), output $(($(stat -f %z $OUT) / 1048576)) MiB"
}
fill() {
  local f
  for f in $DATA/cache/116_GRCh38_ensembl/variation/**/*(.N) $DATA/cache/116_GRCh38_ensembl/translation_sift/**/*(.N); do
    cat $f > /dev/null
    [ "$(vm_stat | awk '/Pages free/{print int($3*16384/2^30)}')" -lt 1 ] && break
  done
  say "fill  $(snap)"
}
release() {
  $PYS -I -c 'import sys
n = int(sys.argv[1]) << 30
b = bytearray(n)
for i in range(0, n, 16384):
    b[i] = 1' $1
  say "release $1  $(snap)"
}
for step in "$@"; do
  case $step in
    sent) sent ;;
    fill) fill ;;
    snap) say "snap  $(snap)" ;;
    release=*) release ${step#release=} ;;
    *) echo "unknown step $step" >&2; exit 2 ;;
  esac
done
rm -f "${R:?}/out.noindex/probe.vcf" "${R:?}/out.noindex/probe.vcf.time"
