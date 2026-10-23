# Thesis: distributed VEPYR cache build (containers on one host)

Scripts used in the master's thesis (Politechnika Warszawska, 2026) for the
container-based ("distributed") construction of the VEPYR Parquet cache,
release 116, merged profile, 1904 contigs with a known-variant file, five
entities (variation, transcript, exon, regulatory, motif). All containers
ran on one host (Apple M5 laptop, Colima VM); no multi-node run was measured.

| Script | Experiment | Result reported in the thesis |
|---|---|---|
| `full_cache_20_runs.py` | repetition 1 of the 10-container build and of the 1-container baseline (2026-10-15) | |
| `distributed_full_4_runs.py` | repetitions 2-5 of the 10-container build: static greedy bin-packing into 10 groups, each container `--cpus=2 --memory=6g`, `partitions=1`, cache mounted read-only | mean 4932.31 s |
| `baseline_9cpu24gb_4_runs.py` | 1-container sequential baseline, 9 CPU / 24 GiB | mean 10984.15 s |
| `baseline_10cpu_5_runs.py`, `monitor_baseline_10cpu.py` | 1-container baseline, 10 CPU, no memory limit | mean 11198.56 s |
| `Dockerfile`, `worker.py` | worker image `vepyr-cache-worker` and its entrypoint | |
| `k8s/` | minikube Indexed Jobs and single-host sweeps (2026-10-13; not reported in the thesis) | |
| `k8s/scaffold-2026-09/` | earlier, never-committed k8s scaffold (`distributed-cache-build-k8s`) | |
| `followups/` | later experiments (2026-10-17..19) with patched images; not reported | |

## Code version

The worker image was not kept and its ID was not recorded. From the API it
calls (`_core.build_cache_entity`) and from the absence of the native
single-pass diagnostics in its logs, it was most likely built from vepyr 0.4.0
(this branch's base, 306926d) with datafusion-bio-functions v0.19.2
(3a3fca3). This is an inference, not a recorded fact. The follow-up
experiments used images built from the working trees that correspond to the
`thesis/cache-build-parallel` branches of vepyr and datafusion-bio-functions.

The scripts use absolute paths of the author's machine and relative `runs/...`
paths (run from the thesis repository root); adjust before reuse.
