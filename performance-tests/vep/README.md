# VEP 116 Fork Scaling

This directory contains VEP 116 WGS fork-scaling scripts and lightweight benchmark outputs.

Run merged cache benchmark:

```bash
performance-tests/vep/scripts/run_vep_merged_fork_scaling.sh
```

Run the refseq cache benchmark:

```bash
performance-tests/vep/scripts/run_vep_refseq_fork_scaling.sh
```

Copy lightweight artifacts into the repository outputs:

```bash
performance-tests/vep/scripts/collect_vep_fork_outputs.sh \
  refseq \
  /home/tgambin/workspace/vep_data2/116/refseq_fork_scaling \
  performance-tests/vep/outputs/116/refseq_fork_scaling/raw \
  16 8 4 2 1 none
```

Default paths can be overridden with environment variables:

```bash
DATA_VEPYR_DIR=/home/tgambin/workspace/vep_data \
OUT_DIR=/home/tgambin/workspace/vep_data2/116/merged_fork_scaling \
performance-tests/vep/scripts/run_vep_fork_scaling.sh merged
```

Generate the merged WGS plot from copied lightweight outputs:

```bash
python3 performance-tests/vep/scripts/plot_vep_fork_scaling.py \
  --cache-type merged \
  --input-dir performance-tests/vep/outputs/116/merged_fork_scaling/raw \
  --summary performance-tests/vep/outputs/116/merged_fork_scaling/summary.tsv \
  --output performance-tests/vep/outputs/116/figures/vep_merged_fork_benchmark_wgs.png \
  --title "VEP 116 merged cache WGS benchmark"
```

Generate the refseq WGS plot:

```bash
python3 performance-tests/vep/scripts/plot_vep_fork_scaling.py \
  --cache-type refseq \
  --input-dir performance-tests/vep/outputs/116/refseq_fork_scaling/raw \
  --summary performance-tests/vep/outputs/116/refseq_fork_scaling/summary.tsv \
  --output performance-tests/vep/outputs/116/figures/vep_refseq_fork_benchmark_wgs.png \
  --title "VEP 116 refseq cache WGS benchmark" \
  --baseline-fork none
```

The repository output directory intentionally excludes large VCF files.
