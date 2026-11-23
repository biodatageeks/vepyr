#!/usr/bin/env bash
# Linux re-run of the vepyr 0.11.0 whole-genome worker scaling (HG002, merged cache, plain VCF) with the
# benchmark scripts of vepyr PR #293 and the protocol of the macOS clean-host set
# (../macos_mwiewior_wgs_vepyr_0.11.0_20261123_clean_host):
#   - process wall = Python start-up + import + annotation + writing the VCF; the record count that checks the
#     output runs after the measurement (PR #293);
#   - one discarded run with 8 workers, then three repeats in the orders 8-4-2-1, 1-2-4-8, 8-4-2-1;
#   - swap-outs (/proc/vmstat pswpout) are counted over each run; a run that swapped is repeated, at most twice,
#     and if it still swaps it is kept but excluded from the medians;
#   - every output is checksummed per contig (must give MD5 157fca5b..., as on macOS), then deleted.
# Paths are those of ../linux_tgambin_wgs_vepyr_0.11.0_20261121/run.sh; every path must be on the NVMe SSD.
# Results: raw/<label>/{summary.tsv,merged_workers<N>.*}, raw/{free_memory.csv,swaplog.csv,environment.txt},
# medians.tsv and wgs_scaling_rows.csv (the four Linux rows of the paper's data/wgs_scaling.csv).
# Before starting: close the browser and other heavy programs; nothing else should run during the ~1.5 h.
set -euo pipefail

REPO=/home/tgambin/workspace/vepyr
OUT=$(cd "$(dirname "$0")" && pwd)
RAW=$OUT/raw
DATA=/home/tgambin/workspace/vep_data
INPUT=$DATA/input/HG002_normalized.vcf.gz
FASTA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
CACHE=$DATA/cache/vepyr_0.11.0/116_GRCh38_merged
PY=$REPO/venv/vepyr-0.11.0/bin/python
SSD=$DATA/output/116/vepyr_merged_worker_scaling_pr293
SSD_DEVICE=/dev/nvme0n1p5
# Benchmark scripts of vepyr PR #293 (branch perf/scaling-untimed-record-count) at a fixed commit, and the
# per-contig checksum of the earlier Linux set; both are read from git, so the checkout does not matter.
PR293_COMMIT=3b06135
MD5_SRC=ae3df1e:performance-tests/paper-0.11.0/engines-two-modes-20261120/scripts/contig_md5.cached-read.py
EXPECTED_MD5=157fca5b065c20eb7cec3e7a18ef28b7
export VEPYR_EXPECTED_VERSION=0.11.0

S=$OUT/scripts
mkdir -p "$RAW" "$S" "$SSD"
git -C "$REPO" fetch -q origin perf/scaling-untimed-record-count || true
for f in run_vepyr_worker_scaling.py benchmark_vepyr_once.py; do
  git -C "$REPO" show "$PR293_COMMIT:performance-tests/vepyr/scripts/$f" > "$S/$f"
done
git -C "$REPO" show "$MD5_SRC" > "$S/contig_md5.py"

for p in "$INPUT" "$FASTA" "$CACHE" "$SSD" "$OUT" "$PY"; do
  dev=$(findmnt -no SOURCE -T "$p")
  [ "$dev" = "$SSD_DEVICE" ] || { echo "$p is on $dev, not on $SSD_DEVICE"; exit 1; }
done
avail_gib() { awk '$1 == "MemAvailable:" {printf "%d", $2 / 1048576}' /proc/meminfo; }
disk_gib() { df -BG --output=avail "$SSD" | tail -1 | tr -dc 0-9; }
(( $(disk_gib) >= 75 )) || { echo "need 75 GiB free on the SSD (29 GB VCF + the runner's 40 GiB check), have $(disk_gib)"; exit 1; }

gib() { awk -v k="$1:" '$1 == k {printf "%.1f", $2 / 1048576}' /proc/meminfo; }
pswpout() { awk '$1 == "pswpout" {print $2}' /proc/vmstat; }
echo "time,run,mem_free_gib,mem_available_gib,page_cache_gib,swap_used_mib" > "$RAW/free_memory.csv"
echo "time,label,attempt,swapouts" > "$RAW/swaplog.csv"
: > "$RAW/swapped_runs.txt"

{
  echo "date: $(date --iso-8601=seconds)"
  echo "host: $(hostname)"
  echo "os: $(lsb_release -ds 2>/dev/null), kernel $(uname -r)"
  echo "cpu: $(lscpu | sed -n 's/^Model name: *//p'), $(nproc) hardware threads"
  echo "cpu frequency governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "memory: $(free -g | awk '/^Mem:/ {print $2}') GiB, swap $(free -g | awk '/^Swap:/ {print $2}') GiB"
  echo "uptime: $(uptime -p)"
  echo "storage: $SSD_DEVICE, $(findmnt -no FSTYPE -T "$DATA") at $(findmnt -no TARGET -T "$DATA")"
  echo "benchmark scripts: vepyr $PR293_COMMIT (PR #293); checksum script: $MD5_SRC"
  "$PY" -I -c 'import importlib.metadata as m, sys
print("python:", sys.version.split()[0], sys.executable)
for p in ("vepyr", "pyarrow", "polars"):
    print(p + ":", m.version(p))'
  echo "cache: $CACHE"
  echo "cache download record: $(tr -d '\n' < "$CACHE/.cache/vepyr/download.json")"
  echo "input: $INPUT sha256 $(sha256sum "$INPUT" | cut -d' ' -f1)"
} > "$RAW/environment.txt"
cat "$RAW/environment.txt"

run_once() { # label workers
  local label=$1 w=$2 dir=$RAW/$1
  mkdir -p "$dir"
  echo "$(date +%H:%M:%S),$label,$(gib MemFree),$(gib MemAvailable),$(gib Cached),$(free -m | awk '/^Swap:/ {print $3}')" >> "$RAW/free_memory.csv"
  (( $(avail_gib) >= 20 )) || echo "warning: only $(avail_gib) GiB available before $label"
  S0=$(pswpout)
  echo "[$(date +%H:%M:%S)] $label (workers=$w)"
  "$PY" -P "$S/run_vepyr_worker_scaling.py" --input-vcf "$INPUT" \
    --cache-dir "$CACHE" --cache-type merged --reference-fasta "$FASTA" \
    --ssd-output-dir "$SSD/$label" --archive-dir "$dir" --expected-records 4096123 --compression plain --force \
    --workers "$w" > "$dir/run.log" 2>&1 || { echo "FAILED $label, see $dir/run.log"; exit 1; }
  S1=$(pswpout)  # swap-outs during the timed run and the untimed record count, not the checksum
  local vcf; vcf=$(ls "$dir"/*.vcf "$SSD/$label"/*.vcf 2>/dev/null | head -1)
  [ -n "$vcf" ] || { echo "$label: no output VCF, see $dir/run.log"; exit 1; }
  "$PY" -I "$S/contig_md5.py" "$vcf" "$dir/merged_workers$w.md5.json" > /dev/null
  rm -f -- "$vcf"
  sync
}
run() { # label workers
  local label=$1 w=$2 attempt
  for attempt in 1 2 3; do
    run_once "$label" "$w"
    echo "$(date +%H:%M:%S),$label,$attempt,$(( S1 - S0 ))" >> "$RAW/swaplog.csv"
    (( S1 - S0 == 0 )) && return 0
    echo "$label swapped $(( S1 - S0 )) pages (attempt $attempt)"
  done
  echo "$label" >> "$RAW/swapped_runs.txt"
}

run warmup 8
for w in 8 4 2 1; do run "r1_w$w" "$w"; done
for w in 1 2 4 8; do run "r2_w$w" "$w"; done
for w in 8 4 2 1; do run "r3_w$w" "$w"; done

"$PY" -I - "$RAW" "$OUT" "$EXPECTED_MD5" <<'PY'
import csv, glob, json, statistics as st, sys
raw, out, expected = sys.argv[1:]
swapped = set(open(f"{raw}/swapped_runs.txt").read().split())
rows = []
for f in sorted(glob.glob(f"{raw}/r*_w*/summary.tsv")):
    if f.split("/")[-2] not in swapped:
        rows += list(csv.DictReader(open(f), delimiter="\t"))
md5 = {json.load(open(f))["md5"] for f in glob.glob(f"{raw}/*/merged_workers*.md5.json")}
bad = [r for r in rows if r["status"] != "ok" or r["records_match_vep"] != "True"]
def secs(v):
    t = 0.0
    for p in v.split(":"):
        t = t * 60 + float(p)
    return t
med = {}
with open(f"{out}/medians.tsv", "w") as f:
    f.write("workers\tprocess_wall_median_s\tannotation_median_s\tpeak_rss_median_gib\tn\n")
    for w in ("1", "2", "4", "8"):
        rs = [r for r in rows if r["workers"] == w]
        med[w] = (round(st.median(secs(r["process_elapsed_wall"]) for r in rs), 2),
                  round(st.median(int(r["max_rss_kb"]) for r in rs) / 1048576, 2), len(rs))
        f.write(f"{w}\t{med[w][0]}\t{round(st.median(float(r['annotation_seconds']) for r in rs), 2)}\t{med[w][1]}\t{med[w][2]}\n")
print(open(f"{out}/medians.tsv").read())
print("excluded (still swapping after 3 attempts):", sorted(swapped) or "none")
print("output MD5s:", md5, "OK" if md5 == {expected} else f"EXPECTED {expected}")
print("failed or mismatched runs:", len(bad))
# The vepyr columns of the four Linux rows of the paper's data/wgs_scaling.csv (VEP columns unchanged).
with open(f"{out}/wgs_scaling_rows.csv", "w") as f:
    f.write("platform,processes,vepyr_seconds,vepyr_peak_gib,vepyr_runs\n")
    for w in ("1", "2", "4", "8"):
        f.write(f"linux,{w},{med[w][0]},{med[w][1]:.2f},{med[w][2]}\n")
print(open(f"{out}/wgs_scaling_rows.csv").read())
PY
echo "[$(date +%H:%M:%S)] finished; send back raw/, medians.tsv and wgs_scaling_rows.csv (no VCFs)"
