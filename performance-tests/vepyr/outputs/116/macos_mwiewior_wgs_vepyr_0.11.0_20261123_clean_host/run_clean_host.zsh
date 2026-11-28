#!/bin/zsh
# Clean-host re-measurement of every vepyr whole-genome number in the paper (macOS), in one session:
#   1. Table 35 / Figure 1A block: HG002, merged cache, one invocation for the whole VCF, vepyr 0.11.0
#      (engine harness of engines-two-modes-20261120: python -c vepyr.annotate under /usr/bin/time)
#   2. Table 17 / Figure 1E: vepyr scaling harness (vepyr PR #293 worktree, record count untimed and uncached)
#   3. the same Table 35 block for fastVEP 0.6.1 and vep-rs 0.5.0
# Run after a reboot, with Spotlight indexing off (sudo mdutil -a -i off), on AC power, with nothing else open.
# Engine harness: each engine in its own block (every run follows a run of the same engine), one discarded
# 8-worker warm-up, then three repeats in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1; medians are reported.
# Before every run at least 20 GiB of memory are made free. Swap-outs (vm_stat) are counted over each timed
# run; a run that swapped is repeated (at most three attempts) and, if it still swaps, kept as invalid.
# Checksums at 1 and 8 workers are read past the file cache and compared with the Table 35 references.
# Resumable: valid rows in engines.csv are not run again; the scaling part is skipped once scaling/DONE exists.
# Environment: ALLOW_INDEXING=1 to run with Spotlight indexing on; ENGINES ("vepyr fastVEP vep-rs"; "" for none);
# SKIP_SCALING=1; REPEATS (default 3).
setopt pipefail
R=${0:A:h}
E=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
DATA=/Users/mwiewior/workspace/data_vepyr
FA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
INPUT=$E/whole/inputs/HG002.vcf.gz
PYV=$E/venv-vepyr-0.11.0/bin/python
PYS=/Users/mwiewior/research/git/vepyr/.venv/bin/python
MD5=$E/scripts/contig_md5.py
CSV=$R/engines.csv
OUT=$R/out.noindex/run.vcf
ENGINES=(${=ENGINES-vepyr fastVEP vep-rs})  # ENGINES="" runs no engine block
REPEATS=${REPEATS:-3}
UNSET=(-u CONDA_PREFIX -u VIRTUAL_ENV -u PYTHONPATH -u RUST_LOG)
for k in ${(k)parameters[(R)*export*]}; do [[ $k == VEP_* || $k == MIMALLOC_* ]] && UNSET+=(-u $k); done
CODE='import sys,vepyr;vepyr.annotate(vcf=sys.argv[1],output_vcf=sys.argv[2],workers=int(sys.argv[3]),cache_dir=sys.argv[4],reference_fasta=sys.argv[5],compression="plain",preserve_record_layout=True,show_progress=False)'
typeset -A REF=(vepyr 157fca5b065c20eb7cec3e7a18ef28b7 fastVEP 85bae770a361ed40cdcc9d1a61dbb10e
                vep-rs 4e914b81ca615c0ff04b90fa7cfe4144)
mkdir -p $R/out.noindex $R/md5

disk_free() { df -g /System/Volumes/Data | awk 'NR==2{print $4}'; }
mem_free() { vm_stat | awk '/page size of/{ps=$8} /Pages free/{gsub(/\./,"",$3); printf "%d", $3*ps/1073741824}'; }
swapouts() { vm_stat | awk '/Swapouts/{gsub(/\./,"",$2); print $2}'; }
compressor_gib() { vm_stat | awk '/page size of/{ps=$8} /occupied by compressor/{gsub(/\./,"",$5); printf "%.1f", $5*ps/1073741824}'; }
swap_used_mb() { sysctl -n vm.swapusage | awk '{gsub("M","",$6); printf "%d", $6}'; }
bg_cpu() { ps -A -o %cpu= | awk '{s+=$1} END{printf "%d", s}'; }
quiet() { local n=0; while [ "$(bg_cpu)" -gt 300 ] && [ $n -lt 10 ]; do sleep 2; n=$((n+1)); done; }
clean() { rm -f "${R:?}/out.noindex/run.vcf" "${R:?}/out.noindex/run.vcf_warnings.txt" "${R:?}/out.noindex/run.vcf_summary.html" "${R:?}/out.noindex/run.vcf.tbi"; }
ensure_free() {
  local have=$(mem_free) released=0
  if (( have < 20 )); then
    released=27
    $PYS -I -c 'import sys
n = int(sys.argv[1]) << 30
b = bytearray(n)
for i in range(0, n, 16384):
    b[i] = 1' $released
  fi
  echo "$(date +%H:%M:%S),$1,$have,$released,$(mem_free)" >> $R/memlog.csv
}

# ---- preflight ----
pmset -g batt | head -1 | grep -q "AC Power" || { echo "plug in the power supply first"; exit 1; }
(( $(disk_free) >= 100 )) || { echo "need at least 100 GiB free on the data volume; have $(disk_free)"; exit 1; }
if mdutil -a -s 2>/dev/null | grep -q "Indexing enabled" && [ -z "$ALLOW_INDEXING" ]; then
  echo "Spotlight indexing is on: run 'sudo mdutil -a -i off' first (or ALLOW_INDEXING=1)"; exit 1
fi
for x in $INPUT $FA $PYV $MD5 $E/bin/fastVEP-0.6.1 $E/bin/vep-rs-0.5.0 $DATA/cache/116_GRCh38_merged \
         $E/whole/merged/cache-veprs $E/whole/merged/fastVEP.cache $E/whole/merged/cache-variation; do
  [ -e $x ] || { echo "missing: $x"; exit 1; }
done
{
  echo "== $(date)"; uptime; sysctl vm.swapusage; mdutil -a -s 2>&1 | grep -i indexing | sort | uniq -c
  vm_stat | head -12; pmset -g batt | head -1; df -h /System/Volumes/Data | tail -1
} >> $R/preflight.txt
[ -f $CSV ] || echo "phase,engine,workers,attempt,valid,wall_s,user_s,sys_s,rss_gib,swapouts,compressor_gib,free_mem_gib,swap_used_mb,bg_cpu_pct,records,md5,md5_ok,started" > $CSV
[ -f $R/memlog.csv ] || echo "time,run,free_gib,released_gib,free_after_gib" > $R/memlog.csv

command_of() { # engine workers -> array cmd
  case $1 in
    vepyr) cmd=($PYV -P -c $CODE $INPUT $OUT $2 $DATA/cache/116_GRCh38_merged $FA) ;;
    vep-rs) cmd=($E/bin/vep-rs-0.5.0 --json_cache $E/whole/merged/cache-veprs --fasta $FA --assembly GRCh38 --merged
                 --fork $2 --decompression_threads 1 --buffer_size 5000 --everything --use_transcript_ref
                 --vcf --max_sv_size -1 --no_stats --force_overwrite --quiet -i $INPUT -o $OUT) ;;
    fastVEP) cmd=($E/bin/fastVEP-0.6.1 annotate --transcript-cache $E/whole/merged/fastVEP.cache
                  --cache-dir $E/whole/merged/cache-variation --fasta $FA --hgvs --symbol --canonical --distance 5000
                  --no-progress --output-format vcf -i $INPUT -o $OUT --everything) ;;
  esac
}
timed() { # workers
  /usr/bin/time -l env $UNSET RAYON_NUM_THREADS=$1 POLARS_MAX_THREADS=$1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 $cmd > /dev/null 2> $OUT.time
}
run_once() { # phase engine workers attempt -> sets SWAPPED
  local phase=$1 engine=$2 n=$3 attempt=$4
  local -a cmd; command_of $engine $n
  quiet; clean; ensure_free "$phase $engine w$n a$attempt"
  local bg=$(bg_cpu) fm=$(mem_free) su=$(swap_used_mb) c0=$(compressor_gib) s0=$(swapouts) started=$(date +%Y-%m-%dT%H:%M:%S)
  echo "[$(date +%H:%M:%S)] $phase $engine w$n (attempt $attempt)"
  if ! timed $n; then echo "FAIL $phase $engine w$n: $(tail -3 $OUT.time | tr '\n' ' ')" | tee -a $R/failures.txt; clean; exit 2; fi
  local s1=$(swapouts); SWAPPED=$((s1 - s0))
  local nrec=- sum=- ok=-
  if [ $phase != warmup ] && { [ $n = 8 ] || [ $n = 1 ]; }; then
    local -a res; res=($($PYS -I $MD5 $OUT $R/md5/$engine-w$n-$phase.json))
    nrec=$res[1]; sum=$res[2]; ok=$([ "$sum" = "$REF[$engine]" ] && echo 1 || echo 0)
  fi
  local valid=$([ $SWAPPED -eq 0 ] && echo 1 || echo 0)
  awk -v p=$phase -v e=$engine -v n=$n -v a=$attempt -v v=$valid -v sw=$SWAPPED -v c=$c0 -v f=$fm -v u=$su -v g=$bg \
      -v q=$nrec -v m=$sum -v o=$ok -v t=$started '
    /[0-9.]+ real +[0-9.]+ user +[0-9.]+ sys/ {wall=$1;usr=$3;sys=$5}
    /maximum resident set size/ {rss=$1/1073741824}
    END {printf "%s,%s,%s,%s,%s,%.2f,%.2f,%.2f,%.3f,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",p,e,n,a,v,wall,usr,sys,rss,sw,c,f,u,g,q,m,o,t}' \
    $OUT.time >> $CSV
  clean
}
run() { # phase engine workers: up to three attempts until a run does not swap
  local attempt
  grep -q "^$1,$2,$3,[0-9],1," $CSV && return
  for attempt in 1 2 3; do
    (( $(disk_free) >= 60 )) || { echo "STOP: under 60 GiB free on the data volume" | tee -a $R/failures.txt; exit 3; }
    run_once $1 $2 $3 $attempt
    (( SWAPPED == 0 )) && return
    echo "   swapped $SWAPPED pages"
    [ $1 = warmup ] && return
  done
}
engine_block() { # engine
  local r n order
  run warmup $1 8
  for r in $(seq 1 $REPEATS); do
    if (( r % 2 )); then order=(8 4 2 1); else order=(1 2 4 8); fi
    for n in $order; do run r$r $1 $n; done
  done
}

# ---- 1. vepyr, engine harness ----
[[ ${ENGINES[(Ie)vepyr]} -gt 0 ]] && engine_block vepyr
# ---- 2. vepyr, scaling harness ----
if [ -z "$SKIP_SCALING" ] && [ ! -f $R/scaling/DONE ]; then
  echo "[$(date +%H:%M:%S)] scaling harness"
  $R/scaling/run_macos_rerun.zsh > $R/scaling/run.log 2>&1 && touch $R/scaling/DONE || { echo "scaling part failed, see scaling/run.log"; exit 4; }
fi
# ---- 3. fastVEP and vep-rs ----
for e in fastVEP vep-rs; do [[ ${ENGINES[(Ie)$e]} -gt 0 ]] && engine_block $e; done

# ---- summary ----
$PYS -I - $CSV <<'PY' | tee $R/summary.txt
import csv, statistics as st, sys
rows = [r for r in csv.DictReader(open(sys.argv[1])) if r["phase"] != "warmup" and r["valid"] == "1"]
bad = [r for r in csv.DictReader(open(sys.argv[1])) if r["phase"] != "warmup" and r["valid"] == "0"]
print("HG002, merged cache, one invocation for the whole VCF (medians of valid runs)")
print("engine   wall w1  w2  w4  w8 | CPU w1  w8 | speed-up | peak RSS w8 | n | checksums ok")
for e in ("vepyr", "vep-rs", "fastVEP"):
    er = [r for r in rows if r["engine"] == e]
    if not er: continue
    med = lambda w, f: st.median(float(r[f]) for r in er if r["workers"] == w)
    cpu = lambda w: st.median(float(r["user_s"]) + float(r["sys_s"]) for r in er if r["workers"] == w)
    walls = [med(w, "wall_s") for w in ("1", "2", "4", "8")]
    rss = max(float(r["rss_gib"]) for r in er if r["workers"] == "8")
    oks = {r["md5_ok"] for r in er if r["md5_ok"] != "-"}
    print(e, " ".join(f"{x:.1f}" for x in walls), "|", f"{cpu('1'):.1f} {cpu('8'):.1f}", "|",
          f"{cpu('1') / walls[3]:.2f}", "|", f"{rss:.2f}", "|", len(er), "|", oks)
print("runs still swapping after three attempts (excluded):", [(r["phase"], r["engine"], r["workers"]) for r in bad
      if not any(v["phase"] == r["phase"] and v["engine"] == r["engine"] and v["workers"] == r["workers"] for v in rows)] or "none")
PY
[ -f $R/scaling/run.log ] && { echo; echo "Scaling harness:"; sed -n '/^workers/,$p' $R/scaling/run.log; }
echo "[$(date +%H:%M:%S)] finished"
