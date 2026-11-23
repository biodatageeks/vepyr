#!/bin/zsh
# macOS re-run of vepyr 0.11.0 whole-genome scaling (HG002, merged cache, plain VCF) with the
# scaling harness of vepyr PR #293: process wall = start-up + import + annotation + writing the
# VCF (record count untimed), AC power required. One discarded 8-worker run, then three repeats
# in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1. Each output is checksummed per contig, then deleted.
# Swap check: vm_stat swap-outs are counted over each run; a run that swapped is repeated (at most
# two more attempts, after freeing memory) and, if it still swaps, kept but excluded from the medians.
set -e
DATA=$HOME/workspace/data_vepyr
OUT=${0:A:h}
S=$HOME/research/git/vepyr/.worktrees/scaling-untimed-count/performance-tests/vepyr/scripts
PY=$HOME/workspace/venvs/vepyr-0.11.0/bin/python
PYS=$HOME/research/git/vepyr/.venv/bin/python
MD5=$HOME/research/git/vepyr/.worktrees/cache-tiering-gain/performance-tests/cache-tiering/contig_md5.py
export VEPYR_EXPECTED_VERSION=0.11.0
free_gib() { df -g $DATA | awk 'NR==2{print $4}'; }
if (( $(free_gib) < 75 )); then
  echo "need at least 75 GiB free on the data volume (29 GB VCF + the runner's 40 GiB check); have $(free_gib)"; exit 1
fi
pmset -g batt | head -1 | grep -q "AC Power" || { echo "plug in the power supply first"; exit 1; }
mem_free() { vm_stat | awk '/page size of/{ps=$8} /Pages free/{gsub(/\./,"",$3); printf "%d", $3*ps/1073741824}'; }
ensure_free() { # label
  local have=$(mem_free) released=0
  if (( have < 20 )); then
    released=27
    $PYS -I -c 'import sys
n = int(sys.argv[1]) << 30
b = bytearray(n)
for i in range(0, n, 16384):
    b[i] = 1' $released
  fi
  echo "$(date +%H:%M:%S),$1,$have,$released,$(mem_free)" >> $OUT/memlog.csv
}
swapouts() { vm_stat | awk '/Swapouts/{gsub(/\./,"",$2); print $2}'; }
compressor_gib() { vm_stat | awk '/page size of/{ps=$8} /occupied by compressor/{gsub(/\./,"",$5); printf "%.1f", $5*ps/1073741824}'; }
run() { # label workers
  local label=$1 w=$2 attempt s0 s1
  for attempt in 1 2 3; do
    run_once $label $w
    s1=$S1
    echo "$(date +%H:%M:%S),$label,$attempt,$S0_C,$(( s1 - S0 ))" >> $OUT/swaplog.csv
    (( s1 - S0 == 0 )) && return 0
    echo "$label swapped $(( s1 - S0 )) pages (attempt $attempt)"
  done
  echo "$label" >> $OUT/swapped_runs.txt
}
run_once() { # label workers
  local label=$1 w=$2
  mkdir -p $OUT/$label
  ensure_free $label
  S0_C=$(compressor_gib); S0=$(swapouts)
  echo "$(date +%H:%M:%S) $label (workers=$w)"
  $PY -P $S/run_vepyr_worker_scaling.py --input-vcf $DATA/input/HG002_normalized.vcf.gz \
    --cache-dir $DATA/cache/116_GRCh38_merged --cache-type merged \
    --reference-fasta $DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    --ssd-output-dir $OUT/$label/ssd --archive-dir $OUT/$label --expected-records 4096123 \
    --compression plain --force --require-ac-power --workers $w > $OUT/$label.log 2>&1
  S1=$(swapouts)  # swap-outs during the timed run only, not the checksum
  local vcf; vcf=$(ls $OUT/$label/*.vcf) || { echo "$label: no output VCF, see $OUT/$label.log"; exit 1; }
  $PYS -I $MD5 $vcf $OUT/$label/merged_workers$w.md5.json > /dev/null
  rm -f -- "$vcf"
}
echo "time,label,have_gib,released_gib,after_gib" > $OUT/memlog.csv
echo "time,label,attempt,compressor_gib_at_start,swapouts" > $OUT/swaplog.csv
: > $OUT/swapped_runs.txt
run warmup 8
for w in 8 4 2 1; do run r1_w$w $w; done
for w in 1 2 4 8; do run r2_w$w $w; done
for w in 8 4 2 1; do run r3_w$w $w; done
$PYS -I - $OUT <<'PY'
import csv, glob, json, statistics as st, sys
out = sys.argv[1]
rows = []
swapped = set(open(f"{out}/swapped_runs.txt").read().split())
print("excluded (still swapping after 3 attempts):", sorted(swapped) or "none")
for f in sorted(glob.glob(f"{out}/r*_w*/summary.tsv")):
    if f.split("/")[-2] in swapped: continue
    rows += list(csv.DictReader(open(f), delimiter="\t"))
md5 = {json.load(open(f))["md5"] for f in glob.glob(f"{out}/*/merged_workers*.md5.json")}
def secs(v):
    t = 0.0
    for p in v.split(":"): t = t * 60 + float(p)
    return t
print("workers  process_wall_median  annotation_median  power  n")
for w in ("1", "2", "4", "8"):
    rs = [r for r in rows if r["workers"] == w]
    print(w, round(st.median(secs(r["process_elapsed_wall"]) for r in rs), 2),
          round(st.median(float(r["annotation_seconds"]) for r in rs), 2), {r.get("power_source") for r in rs}, len(rs))
print("output MD5s:", md5)
PY
