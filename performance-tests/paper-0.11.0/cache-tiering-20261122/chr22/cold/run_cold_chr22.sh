#!/bin/zsh
# Cold chr22 runs of the tiering comparison: sudo purge before each run (asks for the password once).
set -e
cd ~/research/git/vepyr/.worktrees/cache-tiering-gain/performance-tests/cache-tiering
D=~/workspace/data_vepyr/cache/tiering-gain
sudo -v
~/research/git/vepyr/.venv/bin/python run_tiering_gain.py \
  --cache-root "$D" \
  --input-vcf "$D/HG002_chr22.vcf.gz" \
  --expected-records 50861 \
  --workdir "$D/chr22-cold" \
  --workers 1 8 --repeats 0 --cold 3
echo "done: $D/chr22-cold/summary.md"
