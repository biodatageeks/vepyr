# Linux VEP 116 and vepyr 0.9.0 worker scaling

Whole-genome HG002 GRCh38 measurements on the same Linux machine, with merged
and RefSeq release-116 caches and vepyr workers 1, 2, 4 and 8.

## Environment and measurement

- Ubuntu 22.04.3 LTS, AMD Ryzen 9 5950X (16 cores, 32 hardware threads), 64 GB RAM.
- vepyr 0.9.0 installed from the Linux x86-64 PyPI wheel, using Python 3.12.
  This is not a locally compiled native-CPU build.
- Measurements recorded on 2026-10-02 and 2026-10-03.
- Normalized input: `HG002_normalized.vcf.gz`, 4,096,123 records.
- Reference: `Homo_sapiens.GRCh38.dna.primary_assembly.fa`.
- Plain VCF output, `preserve_record_layout=True`.
- Input, reference, converted cache and measured output were on NVMe. The VCF
  and logs were moved to a separate HDD after each measurement.

The existing merged and RefSeq benchmark scripts from commit
`b14bda3d4cf80c0f16d62ab371c28866ae589c33` were used without modifications.
`VEPYR_PYTHON` selected the isolated PyPI environment and
`VEPYR_EXPECTED_VERSION=0.9.0` guarded the imported version. Worker counts were
passed as script arguments. No engine or benchmark-runner change is part of
this result set.

Each cache directory contains a `summary.tsv` and, for every worker setting,
the original `*.metrics.json`, `*.time.txt`, `*.stderr.txt` and `*.stdout.txt`.
Artifact paths in the summaries are relative to their containing directory.
The generated VCFs and caches are not included. Raw logs and metrics retain
the measurement machine's paths and timestamps.

All eight runs exited successfully and produced exactly 4,096,123 records.
This checks record counts, not field-level annotation parity.

## Timing

The comparison figure uses **whole-process elapsed wall time** for both tools:
`elapsed_wall` for the VEP Docker invocation and `process_elapsed_wall` for the
vepyr benchmark process. For vepyr, this includes startup, annotation, VCF
writing and record counting. HDD archiving is excluded. The separate
`annotation_seconds` metric times only `vepyr.annotate()` and is not used in
the comparison figure. One measurement is displayed per setting.

| vepyr workers | Merged annotation (s) | Merged process wall (s) | RefSeq annotation (s) | RefSeq process wall (s) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 471.80 | 482.31 | 244.07 | 247.59 |
| 2 | 310.34 | 320.53 | 132.43 | 136.58 |
| 4 | 208.28 | 218.40 | 83.29 | 86.86 |
| 8 | 205.04 | 216.11 | 62.08 | 65.54 |

VEP results come from the existing
`performance-tests/vep/outputs/116/{merged,refseq}_fork_scaling/summary.tsv` files.
VEP fork 0 (stored as `none`), 1, 3 and 7 are paired with vepyr workers 1, 2, 4
and 8. For a forked VEP invocation, the process count is the annotation
children plus the parent; fork 0 is the single-process invocation. Ratios on
the figure are VEP process wall time divided by vepyr process wall time.

## Reproduce the figure

With Python and Matplotlib installed, run from the repository root:

```bash
MPLBACKEND=Agg python performance-tests/scripts/plot_vep_vs_vepyr_linux.py
```

The script reads the summaries and adjacent metrics in this repository. No
VCFs, cache downloads or local archive paths are needed. It verifies successful
runs, the vepyr version, record counts, output settings and the four expected
worker pairs before drawing the figure.

Outputs:

- `performance-tests/figures/vep_vs_vepyr_linux_116.png`
- `performance-tests/figures/vep_vs_vepyr_linux_116.svg`

The layout and palette follow `plot_vep_vs_vepyr_macos.py`, with merged and
RefSeq panels for the Linux WGS measurements.
