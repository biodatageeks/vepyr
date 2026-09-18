# Performance tests

- [`vep/`](vep/README.md) — Ensembl VEP fork-scaling runner, plotter and outputs
- [`vepyr/`](vepyr/README.md) — vepyr worker-scaling runner, plotter and outputs
- [`scripts/plot_vep_vs_vepyr_macos.py`](scripts/plot_vep_vs_vepyr_macos.py) — the
  side-by-side figure below
- [`figures/`](figures/) — cross-tool figures

Generated VCFs, the staging FASTA and the caches stay outside Git; every run
directory keeps its exact command, GNU time logs, stderr and a summary TSV.

## Ensembl VEP vs vepyr on one Mac (release 116)

![Ensembl VEP vs vepyr on an M3 Max](figures/vep_vs_vepyr_macos_116.png)

Both tools annotated the same normalized HG002 GRCh38 input with the same
release-116 merged cache on the same laptop, once per setting, chr22 first and
then the whole genome. The measured number is whole-process wall time in both
cases: GNU time around `docker run` for VEP, GNU time around the Python
benchmark process for vepyr, so start-up and writing the plain VCF are inside
every figure.

The two parallelism knobs are not the same unit. VEP `--fork N` starts N
annotation children next to the parent that reads and writes, so `--fork 1`
already runs two processes, while vepyr's default `workers=1` is a single
pipeline. The tables therefore pair the runs by process count: VEP without
`--fork` against `workers=1`, `--fork 1` against `workers=2`, and the eight-way
runs against each other with VEP one process ahead (nine against eight).

### Environment

- Host: MacBook Pro, Apple M3 Max (12 performance + 4 efficiency cores), 64 GiB
  RAM, macOS 15.7.9. Desktop applications stayed open; no other heavy workload
  ran during the measurements.
- VEP: Docker Desktop 29.6.2, native `linux/arm64` image
  `ensemblorg/ensembl-vep:release_116.0`
  (`sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11`),
  VM with 16 vCPU and 16.5 GiB RAM. Ensembl merged cache `116_GRCh38`,
  `info.txt` SHA-256 `7eb552fe73f5ad1d2c06ce1f76eac1998c62235a82919af43c76efcdbc17b62c`.
- vepyr: 0.7.0 from this repository's `.venv`, running natively on the host
  against the Parquet conversion of the same merged cache
  (`cache/116_GRCh38_merged`).
- Flags: `--merged --offline --everything --hgvs` with the GRCh38 primary
  assembly FASTA for VEP; `everything=True, hgvs=True, reference_fasta=...`
  with `output_vcf` for vepyr. Plain, uncompressed VCF output on both sides.
- Inputs: HG002 GRCh38 v4.2.1 benchmark VCF split with `bcftools norm -m -both`
  (bcftools 1.21). Whole genome: 4,096,123 records, SHA-256
  `76931747a93b077464046236fb7357e0bc8ca8e4581e6f19a95c97334867965b`.
  chr22 slice: 50,861 records, SHA-256
  `9d2071232a2ed50a36c3dda8482c4509216fb42e4824945f0ac94492ffa4ee6c`.

### chr22, 50,861 variants

| Processes | VEP setting | VEP wall | vepyr setting | vepyr wall (process) | vepyr annotation only | VEP / vepyr |
|---|---|---|---|---|---|---|
| 1 | no fork | 7:54.33 | `workers=1` | 4.23 s | 3.89 s | 112× |
| 2 | `--fork 1` | 4:08.89 | `workers=2` | 3.20 s | 2.82 s | 78× |
| 3 | `--fork 2` | 2:55.10 | | | | |
| 4 | | | `workers=4` | 2.88 s | 2.54 s | |
| 5 | `--fork 4` | 1:51.60 | | | | 39× vs `workers=4` |
| 8 | | | `workers=8` | 2.31 s | 1.89 s | |
| 9 | `--fork 8` | 1:13.21 | | | | 32× vs `workers=8` |

VEP: [`vep/outputs/116/macos_mwiewior_chr22_20260913`](vep/outputs/116/macos_mwiewior_chr22_20260913)
(one `--fork 8` warm-up, 1:13.93, then `1 2 4 8 none`, 2026-09-13).
vepyr: [`vepyr/outputs/116/macos_mwiewior_chr22_20260914`](vepyr/outputs/116/macos_mwiewior_chr22_20260914)
(one `workers=8` warm-up, 2.25 s, then `8 4 2 1`, 2026-09-18). Peak RSS of the
vepyr process: 1.2 GiB at one worker, 1.6 GiB at eight.

### Whole genome, 4,096,123 variants

| Processes | VEP setting | VEP wall | vepyr setting | vepyr wall (process) | vepyr annotation only | vepyr peak RSS | VEP / vepyr |
|---|---|---|---|---|---|---|---|
| 1 | no fork | 8:45:41 | `workers=1` | 4:40.98 | 268.8 s | 3.8 GiB | 112× |
| 2 | `--fork 1` | 4:29:09 | `workers=2` | 2:40.45 | 148.0 s | 5.7 GiB | 101× |
| 3 | `--fork 2` | 3:12:07 | | | | | |
| 4 | | | `workers=4` | 1:46.58 | 94.1 s | 6.7 GiB | |
| 5 | `--fork 4` | 2:07:07 | | | | | 72× vs `workers=4` |
| 8 | | | `workers=8` | 1:23.57 | 70.9 s | 9.2 GiB | |
| 9 | `--fork 8` | 1:23:15 | | | | | 60× vs `workers=8` |

VEP: [`vep/outputs/116/macos_mwiewior_wgs_20260914`](vep/outputs/116/macos_mwiewior_wgs_20260914)
(`8 4 2 1 none`, no separate warm-up, 2026-09-14). vepyr:
[`vepyr/outputs/116/macos_mwiewior_wgs_20260914`](vepyr/outputs/116/macos_mwiewior_wgs_20260914)
(one `workers=8` warm-up, then `8 4 2 1`, 2026-09-14).

Default against default, VEP's serial run takes 112× the vepyr `workers=1`
run on both inputs. At eight workers vepyr annotates the whole genome in the
time VEP needs for chr22 at `--fork 8` plus ten seconds, and VEP's serial run
is 377× the vepyr `workers=8` run. vepyr's CPU time grows from 410 s at one
worker to 501 s at eight, so the eight-way run is 3.4× faster on wall time
for 1.2× the CPU; `workers=1` itself averages 1.5 cores because reading and
writing overlap the single annotation pipeline. VEP gains 3.2× from `--fork 1`
to `--fork 8` on the whole genome and 3.4× on chr22.

### Output parity on chr22

The chr22 run kept its VCFs, so the outputs were compared record by record on
`INFO/CSQ` (same CHROM, POS, REF, ALT):

| Pair | Identical CSQ | Different |
|---|---|---|
| VEP no fork vs vepyr `workers=8` | 50,861 / 50,861 | 0 |
| VEP `--fork 8` vs vepyr `workers=8` | 50,746 / 50,861 | 115 |

Every one of the 115 differences is VEP's `--fork` output leaving `HGNC_ID`
empty on a RefSeq transcript where the serial VEP run and vepyr both print it.
That is the known fork-dependent VEP behaviour and not a vepyr mismatch.

### Reading the numbers

- GNU time's RSS for VEP is that of the host Docker CLI (about 27 MB), not
  VEP's memory inside the VM, and is not reported as VEP memory here.
- VEP `--fork 1` still has a parent process; the separate no-fork run is VEP's
  serial path.
- vepyr's `annotation_seconds` covers only the `vepyr.annotate()` call; the
  process wall time also includes interpreter start-up and the record count
  that verifies the output. The tables use the process wall time for the ratio.
- Every whole-genome VEP run emitted the same 43 `Transcript-assembly mismatch`
  warnings (forked runs print each twice); they are kept in the
  `raw/*.stderr.txt` files.
- One run per setting. Repeats on a shared laptop typically move by a few
  percent; the ratios do not depend on that.

### Reproduce

Regenerate the summaries, the per-run figures and the comparison figure from
the committed logs:

```bash
ENV='MacBook Pro M3 Max (12P + 4E cores), 64 GiB RAM | native ARM64 Docker Desktop 29.6.2, 16 vCPU / 16.5 GiB VM RAM'
V=performance-tests/vep/outputs/116
P=performance-tests/vepyr/outputs/116
.venv/bin/python performance-tests/vep/scripts/plot_vep_fork_scaling.py \
  --cache-type merged --release 116 --records 50861 \
  --input-dir $V/macos_mwiewior_chr22_20260913/raw \
  --summary $V/macos_mwiewior_chr22_20260913/summary.tsv \
  --output $V/macos_mwiewior_chr22_20260913/vep116_macos_chr22_scaling.png \
  --title 'VEP 116.0 fork scaling on Apple M3 Max / Docker' \
  --dataset 'HG002 GRCh38 chr22' --baseline-fork 1 --scaling-panels \
  --environment "$ENV" \
  --run-note 'One measured run per setting after a separate --fork 8 warm-up.' \
  --command-file $V/macos_mwiewior_chr22_20260913/command.txt
.venv/bin/python performance-tests/vep/scripts/plot_vep_fork_scaling.py \
  --cache-type merged --release 116 --records 4096123 \
  --input-dir $V/macos_mwiewior_wgs_20260914/raw \
  --summary $V/macos_mwiewior_wgs_20260914/summary.tsv \
  --output $V/macos_mwiewior_wgs_20260914/vep116_macos_wgs_scaling.png \
  --title 'VEP 116.0 fork scaling on Apple M3 Max / Docker' \
  --dataset 'HG002 GRCh38 whole genome' --baseline-fork 1 --scaling-panels \
  --environment "$ENV" \
  --run-note 'One measured run per setting, no separate warm-up.' \
  --command-file $V/macos_mwiewior_wgs_20260914/command.txt
.venv/bin/python performance-tests/vepyr/scripts/plot_vepyr_worker_scaling.py \
  --cache-type merged --summary $P/macos_mwiewior_wgs_20260914/raw/summary.tsv \
  --output $P/macos_mwiewior_wgs_20260914/vepyr116_macos_wgs_scaling.png \
  --title 'vepyr 0.7.0 merged cache WGS benchmark on Apple M3 Max' \
  --expected-workers '1 2 4 8'
.venv/bin/python performance-tests/vepyr/scripts/plot_vepyr_worker_scaling.py \
  --cache-type merged --records 50861 \
  --summary $P/macos_mwiewior_chr22_20260914/raw/summary.tsv \
  --output $P/macos_mwiewior_chr22_20260914/vepyr116_macos_chr22_scaling.png \
  --title 'vepyr 0.7.0 merged cache chr22 benchmark on Apple M3 Max' \
  --expected-workers '1 2 4 8'
.venv/bin/python performance-tests/scripts/plot_vep_vs_vepyr_macos.py
```

To measure again, each run directory's `command.txt` is the exact invocation
that produced it. The VEP runner takes `INPUT_DIR`, `TIME_BIN` (GNU time,
`brew install gnu-time` on macOS) and `KEEP_VCFS`; it refuses a `VEP_IMAGE`
whose major release differs from `RELEASE`, because VEP then reports
"No cache found" after a sub-second run. Point the vepyr runner at a fresh
`--archive-dir`; reusing one replaces that worker count's measurement.
