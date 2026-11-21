#!/usr/bin/env bash
# Whole-genome runs of the vepyr 0.11.0 PyPI wheel on Linux (merged cache, plain VCF output), measured as the macOS
# set of branch perf/paper-results-0.11.0 (performance-tests/vepyr/outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261121/run.zsh):
# one discarded run with 8 workers, then 8, 4, 2 and 1 workers, one run each, by the unchanged runner of tag 0.11.0
# (whole Python process, wait4). After each run the record lines are checksummed per contig and the 29 GiB output is
# deleted. Every measured and archived path is on the NVMe SSD; nothing goes to the HDD.
# Results: raw/{warmup,merged}_workers<N>.{time.txt,metrics.json,md5.json,stderr.txt,stdout.txt}, raw/summary.tsv,
# raw/free_memory.csv, raw/environment.txt.
set -euo pipefail

REPO=/home/tgambin/workspace/vepyr
OUT=$REPO/performance-tests/vepyr/outputs/116/linux_tgambin_wgs_vepyr_0.11.0_20261121
RAW=$OUT/raw
DATA=/home/tgambin/workspace/vep_data
INPUT=$DATA/input/HG002_normalized.vcf.gz
FASTA=$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
CACHE=$DATA/cache/vepyr_0.11.0/116_GRCh38_merged
PY=$REPO/venv/vepyr-0.11.0/bin/python
S=$REPO/performance-tests/vepyr/scripts
SSD=$DATA/output/116/vepyr_merged_worker_scaling_v0110
ARC=$DATA/archive/116/vepyr_merged_worker_scaling_v0110
# The per-contig checksum of the paper runs. Its macOS variant reads with F_NOCACHE, which Linux does not have, so
# the plain-read variant of the same commit is used.
MD5=$DATA/archive/116/contig_md5.cached-read.py
MD5_SRC=ae3df1e:performance-tests/paper-0.11.0/engines-two-modes-20261120/scripts/contig_md5.cached-read.py
SSD_DEVICE=/dev/nvme0n1p5
export VEPYR_EXPECTED_VERSION=0.11.0

mkdir -p "$RAW" "$DATA/archive/116"
git -C "$REPO" show "$MD5_SRC" > "$MD5"

for p in "$INPUT" "$FASTA" "$CACHE" "$DATA/output/116" "$DATA/archive/116" "$PY"; do
  dev=$(findmnt -no SOURCE -T "$p")
  [ "$dev" = "$SSD_DEVICE" ] || { echo "$p is on $dev, not on $SSD_DEVICE"; exit 1; }
done

gib() { awk -v k="$1:" '$1 == k {printf "%.1f", $2 / 1048576}' /proc/meminfo; }
log_mem() { echo "$(date +%H:%M:%S),$1,$(gib MemFree),$(gib MemAvailable),$(gib Cached)" >> "$RAW/free_memory.csv"; }
echo "time,run,mem_free_gib,mem_available_gib,page_cache_gib" > "$RAW/free_memory.csv"

{
  echo "date: $(date --iso-8601=seconds)"
  echo "host: $(hostname)"
  echo "os: $(lsb_release -ds 2>/dev/null), kernel $(uname -r)"
  echo "cpu: $(lscpu | sed -n 's/^Model name: *//p'), $(nproc) hardware threads"
  echo "cpu frequency governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "memory: $(free -g | awk '/^Mem:/ {print $2}') GiB, swap $(free -g | awk '/^Swap:/ {print $2}') GiB"
  echo "storage: $SSD_DEVICE ($(lsblk -dno MODEL /dev/nvme0n1)), $(findmnt -no FSTYPE -T "$DATA") at $(findmnt -no TARGET -T "$DATA")"
  echo "repository: $(git -C "$REPO" describe --tags --always) $(git -C "$REPO" rev-parse HEAD)"
  echo "checksum script: $MD5_SRC"
  "$PY" -I -c 'import importlib.metadata as m, sys
print("python:", sys.version.split()[0], sys.executable)
for p in ("vepyr", "pyarrow", "polars", "huggingface-hub"):
    print(p + ":", m.version(p))
wheel = m.distribution("vepyr").read_text("WHEEL")
print("vepyr wheel tags:", ", ".join(l.split(": ")[1] for l in wheel.splitlines() if l.startswith("Tag:")))
print("vepyr installed from:", "PyPI index" if m.distribution("vepyr").read_text("direct_url.json") is None else "a local or direct URL")'
  echo "cache: $CACHE"
  echo "cache download record: $(tr -d '\n' < "$CACHE/.cache/vepyr/download.json")"
  echo "cache size: $(du -sh --apparent-size "$CACHE" | cut -f1)"
  echo "input: $INPUT sha256 $(sha256sum "$INPUT" | cut -d' ' -f1)"
  echo "reference: $FASTA"
  echo "VEPYR_EXPECTED_VERSION: $VEPYR_EXPECTED_VERSION"
} > "$RAW/environment.txt"
cat "$RAW/environment.txt"

run_one() { # label workers
  local label=$1 w=$2 vcf=$ARC/HG002_annotated_wgs_everything_hgvs_vepyr_merged_workers$2.vcf
  log_mem "$label$w"
  echo "[$(date +%H:%M:%S)] $label (workers=$w)"
  "$PY" -P "$S/run_vepyr_worker_scaling.py" --input-vcf "$INPUT" \
    --cache-dir "$CACHE" --cache-type merged --reference-fasta "$FASTA" \
    --ssd-output-dir "$SSD" --archive-dir "$ARC" --expected-records 4096123 --compression plain --force \
    --workers "$w" || { echo "FAILED $label$w"; return 1; }
  for f in time.txt metrics.json stderr.txt stdout.txt; do
    cp "$ARC/merged_workers$w.$f" "$RAW/${label}_workers$w.$f"
  done
  "$PY" -I "$MD5" "$vcf" "$RAW/${label}_workers$w.md5.json"
  rm -f "$vcf"
  sync
  grep -a 'Elapsed\|Maximum resident' "$RAW/${label}_workers$w.time.txt"
}

run_one warmup 8
for w in 8 4 2 1; do run_one merged "$w"; done
cp "$ARC/summary.tsv" "$RAW/summary.tsv"
echo "[$(date +%H:%M:%S)] finished"
