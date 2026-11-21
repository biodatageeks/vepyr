#!/bin/bash
# VEP 116.0, HG002 whole genome, merged cache, --fork 7 (8 processes), with a memory sampler.
# Every 5 s: number of processes, summed Rss and summed Pss (kB) over all processes of the container
# (/proc/<pid>/smaps_rollup). Written to /output/mem.tsv; /output/times.txt has start and end (epoch s).
out=/output/mem.tsv
: > $out
( while true; do
    rss=0; pss=0; n=0
    for p in /proc/[0-9]*; do
      [ -r $p/smaps_rollup ] || continue
      r=$(awk '/^Rss:/{print $2}' $p/smaps_rollup 2>/dev/null); s=$(awk '/^Pss:/{print $2}' $p/smaps_rollup 2>/dev/null)
      if [ -n "$r" ] && [ -n "$s" ]; then rss=$((rss+r)); pss=$((pss+s)); n=$((n+1)); fi
    done
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$n" "$rss" "$pss" >> $out
    sleep 5
  done ) &
sampler=$!
echo "start $(date +%s)" > /output/times.txt
vep --dir /opt/vep/.vep --cache --merged --offline --assembly GRCh38 \
    --input_file /input/HG002_normalized.vcf.gz \
    --output_file /output/HG002_merged_fork7.vcf \
    --vcf --force_overwrite --no_stats --everything --hgvs \
    --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa --fork 7 > /output/vep.stdout.txt 2> /output/vep.stderr.txt
rc=$?
echo "end $(date +%s) rc=$rc" >> /output/times.txt
kill $sampler 2>/dev/null
cat /sys/fs/cgroup/memory.peak > /output/cgroup_memory_peak.txt 2>/dev/null
exit $rc
