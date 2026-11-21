#!/bin/zsh
# vepyr 0.11.0, vep-rs 0.5.0 and fastVEP 0.6.1 at 1, 2, 4 and 8 workers on HG002 and ClinVar with the merged and
# the Ensembl cache, in two modes:
#   split  one process per chromosome with that chromosome's input and cache; totals are sums over chromosomes
#   whole  one process for the whole VCF with the whole-genome cache; the engine decides how to parallelise
# Process wall/CPU/peak RSS from /usr/bin/time. Each engine is measured in its own block, so every run follows
# a run of the same engine; one discarded warm-up (8 workers) per chromosome (split) or per panel (whole).
# The order of the worker counts alternates by chromosome in split mode. Checksums are taken outside the timed
# section, at 8 and 1 workers. Before every engine block a sentinel (fastVEP 0.6.1, HG002 chr1, merged cache,
# 8 threads) records the state of the machine. Free memory is kept available so that the inputs of an engine stay
# in the file cache between runs (ensure_free; log in memlog.csv). Resumable from runs.csv.
# Environment: RUN_PANELS ("merged:HG002 ensembl:HG002 ..."), RUN_MODES ("split whole"), RUN_ENGINES,
# RUN_CONTIGS (split mode only), RUN_WORKERS, DRY=1 (check that every input and cache exists); for validation
# runs RUN_CSV, RUN_MD5DIR, RUN_WHOLE_INPUT (input of the whole mode) and RUN_NO_SENTINEL=1.
R=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
B=/Users/mwiewior/research/benchmarks/merged-engine-comparison-20261107
M=$B/all-chromosomes-w8
W=/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/hg002-w8
XMT=/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/clinvar-x-y-mt
DATA=/Users/mwiewior/workspace/data_vepyr
FA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
PYV=$R/venv-vepyr-0.11.0/bin/python
PYS=/Users/mwiewior/research/git/vepyr/.venv/bin/python
CSV=${RUN_CSV:-$R/runs.csv}
MD5DIR=${RUN_MD5DIR:-$R/whole-md5}
SENT=$R/sentinel.csv
FAILS=$R/failures.txt
OUT=$R/out.noindex/run.vcf
PANELS=(${=RUN_PANELS:-merged:HG002 ensembl:HG002 merged:ClinVar ensembl:ClinVar})
MODES=(${=RUN_MODES:-split whole})
ENGINES=(${=RUN_ENGINES:-vepyr fastVEP vep-rs})
WORKERS=(${=RUN_WORKERS:-8 4 2 1})
MIN_FREE=${RUN_MIN_FREE:-25}
FREE_TARGET=${RUN_FREE_MEM:-26}
FREE_LOW=${RUN_FREE_LOW:-20}
FREE_FLOOR=${RUN_FREE_FLOOR:-8}
MEMLOG=${RUN_MEMLOG:-$R/memlog.csv}
UNSET=(-u CONDA_PREFIX -u VIRTUAL_ENV -u PYTHONPATH -u RUST_LOG)
for k in ${(k)parameters[(R)*export*]}; do [[ $k == VEP_* || $k == MIMALLOC_* ]] && UNSET+=(-u $k); done
CODE='import sys,vepyr;vepyr.annotate(vcf=sys.argv[1],output_vcf=sys.argv[2],workers=int(sys.argv[3]),cache_dir=sys.argv[4],reference_fasta=sys.argv[5],compression="plain",preserve_record_layout=True,show_progress=False)'
mkdir -p $R/out.noindex $MD5DIR
[ -f $CSV ] || echo "phase,mode,cache,dataset,contig,engine,workers,wall_s,user_s,sys_s,rss_gib,bg_cpu_pct,free_gib,swap_mb,out_bytes,records,md5,started" > $CSV
[ -f $SENT ] || echo "time,before,wall_s,user_s,sys_s,bg_cpu_pct,mem_free_pct,swap_mb,power" > $SENT
[ -f $MEMLOG ] || echo "time,run,free_gib,released_gib,free_after_gib,inputs_resident_gib" > $MEMLOG
bg_cpu() { ps -A -o %cpu= | awk '{s+=$1} END{printf "%d", s}'; }
quiet() { local n=0; while [ "$(bg_cpu)" -gt 300 ] && [ $n -lt 10 ]; do sleep 2; n=$((n+1)); done; }
free_gib() { df -g /System/Volumes/Data | awk 'NR==2{print $4}'; }
swap_mb() { sysctl -n vm.swapusage | awk '{gsub("M","",$6); printf "%d", $6}'; }
clean() { rm -f "${R:?}/out.noindex/run.vcf" "${R:?}/out.noindex/run.vcf_warnings.txt" "${R:?}/out.noindex/run.vcf_summary.html" "${R:?}/out.noindex/run.vcf.tbi"; }
# Free memory before a run. When the file cache has taken all free pages, macOS does not keep the files read by a
# run in the cache (measured 2026-11-20, memstate.log: 1.4 instead of 1.9 GiB of the sentinel inputs resident, wall
# time of the sentinel 9.0 instead of 7.15 s on every repetition), so each run reads its inputs from disk again.
# With free pages available the inputs stay cached. ensure_free tops the free pool up to FREE_TARGET GiB by
# allocating and releasing anonymous memory, which evicts file pages. It does so before the first run of a
# chromosome or panel (kind "top": when under FREE_LOW GiB are free); between the runs of one chromosome only
# when under FREE_FLOOR GiB are free (kind "floor"), because a release directly after a warm-up was seen to
# evict part of the inputs that the warm-up had just read. Every call and the free memory after every run are
# logged in memlog.csv.
mem_free() { vm_stat | awk '/Pages free/{printf "%d", $3*16384/1073741824}'; }
ensure_free() { # label top|floor [paths whose residency is logged]
  local label=$1 limit=$FREE_LOW have=$(mem_free) released=0 resident=-
  [ $2 = floor ] && limit=$FREE_FLOOR
  shift 2
  if [ $have -lt $limit ]; then
    released=$((FREE_TARGET + 1))
    $PYS -I -c 'import sys
n = int(sys.argv[1]) << 30
b = bytearray(n)
for i in range(0, n, 16384):
    b[i] = 1' $released
  fi
  [ $# -gt 0 ] && resident=$($PYS -I $R/scripts/pagecache.py resident "$@" | awk '{print $2 "/" $4}')
  echo "$(date +%H:%M:%S),$label,$have,$released,$(mem_free),$resident" >> $MEMLOG
}
split_root() { # cache contig
  if [ $1 = merged ]; then echo $M/caches/chr$2
  elif [ -d $W/caches/chr$2 ]; then echo $W/caches/chr$2
  else echo $XMT/caches/chr$2; fi
}
input_of() { # mode cache dataset contig
  if [ $1 = whole ]; then echo ${RUN_WHOLE_INPUT:-$R/whole/inputs/$3.vcf.gz}
  elif [ $2 = ensembl ] && [ $3 = HG002 ]; then echo $W/HG002/chr$4/prepared/HG002/input.vcf.gz
  else echo $M/$3/chr$4/prepared/$3/input.vcf.gz; fi
}
command_of() { # mode cache contig engine workers input -> array cmd
  local mode=$1 cache=$2 ch=$3 engine=$4 n=$5 input=$6 root
  [ $mode = split ] && root=$(split_root $cache $ch)
  case $engine in
    vepyr)
      local dir=$root/cache-vepyr
      [ $mode = whole ] && dir=$DATA/cache/116_GRCh38_$cache
      cmd=($PYV -P -c $CODE $input $OUT $n $dir $FA) ;;
    vep-rs)
      local jc=$root/cache-veprs
      [ $mode = whole ] && jc=$R/whole/$cache/cache-veprs
      local -a flavour
      [ $cache = merged ] && flavour=(--merged)
      cmd=($R/bin/vep-rs-0.5.0 --json_cache $jc --fasta $FA --assembly GRCh38 $flavour --fork $n
           --decompression_threads 1 --buffer_size 5000 --everything)
      [ $cache = merged ] && cmd+=(--use_transcript_ref)
      cmd+=(--vcf --max_sv_size -1 --no_stats --force_overwrite --quiet -i $input -o $OUT) ;;
    fastVEP)
      local tc=$root/fastVEP-main-merged116.cache vc=$root/cache-variation
      [ $mode = whole ] && tc=$R/whole/$cache/fastVEP.cache && vc=$R/whole/$cache/cache-variation
      cmd=($R/bin/fastVEP-0.6.1 annotate --transcript-cache $tc --cache-dir $vc --fasta $FA --hgvs --symbol
           --canonical --distance 5000 --no-progress --output-format vcf -i $input -o $OUT --everything) ;;
  esac
}
timed() { # workers -> runs $cmd, leaves $OUT.time; returns the exit status of the engine
  /usr/bin/time -l env $UNSET RAYON_NUM_THREADS=$1 POLARS_MAX_THREADS=$1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 $cmd > /dev/null 2> $OUT.time
}
sentinel() { # label
  [ -n "$DRY$RUN_NO_SENTINEL" ] && return
  local -a cmd
  local i
  for i in 1 2; do
    clean
    if [ $i = 1 ]; then ensure_free "sentinel-1 before $1" top; else ensure_free "sentinel-2 before $1" floor; fi
    command_of split merged 1 fastVEP 8 $(input_of split merged HG002 1)
    local bg=$(bg_cpu)
    timed 8 || { echo "FAIL sentinel before $1" >> $FAILS; return; }
    [ $i = 2 ] && awk -v t=$(date +%H:%M:%S) -v l=$1 -v g=$bg -v m=$(memory_pressure | awk '/free percentage/{gsub("%","",$5); print $5}') \
      -v s=$(swap_mb) -v p=$(pmset -g batt | head -1 | grep -o "'.*'" | tr -d "' ") \
      '/[0-9.]+ real +[0-9.]+ user +[0-9.]+ sys/ {printf "%s,%s,%.2f,%.2f,%.2f,%s,%s,%s,%s\n",t,l,$1,$3,$5,g,m,s,p}' $OUT.time >> $SENT
  done
  clean
}
run() { # phase mode cache dataset contig engine workers
  local phase=$1 mode=$2 cache=$3 ds=$4 ch=$5 engine=$6 n=$7
  local input=$(input_of $mode $cache $ds $ch)
  local -a cmd
  command_of $mode $cache $ch $engine $n $input
  if [ -n "$DRY" ]; then
    local x
    for x in $input $cmd[1]; do [ -e $x ] || echo "MISSING $mode $cache $ds $ch $engine: $x"; done
    for x in $cmd; do [[ $x == /Users/*cache* ]] && [ ! -e $x ] && echo "MISSING $mode $cache $ds $ch $engine: $x"; done
    return
  fi
  grep -q "^$phase,$mode,$cache,$ds,$ch,$engine,$n," $CSV && return
  if [ "$(free_gib)" -lt $MIN_FREE ]; then echo "STOP: under $MIN_FREE GiB free before $phase $mode $cache $ds $ch $engine w$n" | tee -a $FAILS >&2; exit 3; fi
  if [ $mode = whole ]; then
    # The whole-VCF output is one plain file: its size is the sum of the per-chromosome outputs of this engine
    # (split mode, 8 workers) when those exist, else an upper estimate. Skip the run if it would not leave MIN_FREE.
    local est=$(awk -F, -v k=$cache -v d=$ds -v e=$engine '$1=="measured" && $2=="split" && $3==k && $4==d && $6==e && $7==8 {s+=$15; c++} END{printf "%d %d", s/1073741824+1, c}' $CSV)
    local gib=${est%% *} have=${est##* } all=22
    [ $ds = ClinVar ] && all=25
    if [ $have -ne $all ]; then gib=45; [ $ds = ClinVar ] && gib=100; fi
    [ -n "$RUN_WHOLE_INPUT" ] && gib=3
    if [ "$(free_gib)" -lt $((gib + MIN_FREE)) ]; then
      echo "SKIP $phase whole $cache $ds $engine w$n: output about $gib GiB, $(free_gib) GiB free, $MIN_FREE GiB must stay free" >> $FAILS
      return
    fi
  fi
  quiet
  clean
  local kind=floor
  [ $phase = warmup ] && kind=top
  if [ $mode = whole ]; then
    # the cache directories and the FASTA are the inputs whose residency is logged ($cmd holds their paths)
    local -a inputs
    local x
    for x in $cmd; do [[ $x == /Users/* && $x != $OUT && $x != $cmd[1] && -e $x ]] && inputs+=($x); done
    ensure_free "$phase $mode $cache $ds $ch $engine w$n" $kind $inputs
  else
    ensure_free "$phase $mode $cache $ds $ch $engine w$n" $kind
  fi
  local bg=$(bg_cpu) fr=$(free_gib) sw=$(swap_mb) started=$(date +%Y-%m-%dT%H:%M:%S)
  if ! timed $n; then
    echo "FAIL $phase $mode $cache $ds $ch $engine w$n: $(tail -3 $OUT.time | tr '\n' ' ')" >> $FAILS
    clean
    return
  fi
  echo "$(date +%H:%M:%S),end of $phase $mode $cache $ds $ch $engine w$n,$(mem_free),,," >> $MEMLOG
  local bytes=$(stat -f %z $OUT) nrec=- sum=-
  if [ $phase = measured ] && { [ $n = 8 ] || [ $n = 1 ]; }; then
    if [ $mode = whole ]; then
      local -a res
      res=($($PYS -I $R/scripts/contig_md5.py $OUT $MD5DIR/$cache-$ds-$engine-w$n.json))
      nrec=$res[1]; sum=$res[2]
    else
      nrec=$(LC_ALL=C grep -vc '^#' $OUT)
      sum=$(LC_ALL=C grep -v '^#' $OUT | md5)
    fi
  fi
  awk -v p=$phase -v o=$mode -v k=$cache -v d=$ds -v c=$ch -v e=$engine -v n=$n -v g=$bg -v f=$fr -v w=$sw -v b=$bytes -v q=$nrec -v m=$sum -v t=$started '
    /[0-9.]+ real +[0-9.]+ user +[0-9.]+ sys/ {wall=$1;u=$3;s=$5}
    /maximum resident set size/ {rss=$1/1073741824}
    END {printf "%s,%s,%s,%s,%s,%s,%s,%.2f,%.2f,%.2f,%.3f,%s,%s,%s,%s,%s,%s,%s\n",p,o,k,d,c,e,n,wall,u,s,rss,g,f,w,b,q,m,t}' $OUT.time >> $CSV
  clean
}
for panel in $PANELS; do
  cache=${panel%%:*}; ds=${panel##*:}
  if [ $ds = HG002 ]; then contigs=($(seq 1 22)); else contigs=($(seq 1 22) X Y MT); fi
  [ -n "$RUN_CONTIGS" ] && contigs=(${=RUN_CONTIGS})
  for mode in $MODES; do
    for engine in $ENGINES; do
      echo "[$(date +%H:%M:%S)] $cache $ds $mode $engine"
      sentinel "$cache-$ds-$mode-$engine"
      if [ $mode = split ]; then
        c=0
        for ch in $contigs; do
          run warmup split $cache $ds $ch $engine 8
          if [ $((c % 2)) -eq 0 ]; then order=($WORKERS); else order=(${(Oa)WORKERS}); fi
          for n in $order; do run measured split $cache $ds $ch $engine $n; done
          c=$((c+1))
        done
      else
        run warmup whole $cache $ds all $engine 8
        for n in $WORKERS; do run measured whole $cache $ds all $engine $n; done
      fi
    done
  done
done
sentinel end
echo "[$(date +%H:%M:%S)] finished"
