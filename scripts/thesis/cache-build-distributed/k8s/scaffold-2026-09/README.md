# Distributed cache-build benchmark (K8s / minikube multi-node)

Tests the thesis's "rozproszona" (distributed) claim honestly: N separate
Kubernetes **nodes** (each a real VM under minikube's `qemu2` driver — not
just containers sharing one kernel), each running a worker pod that builds a
disjoint subset of the 6 cache entities (variation, transcript, exon,
translation, regulatory, motif) for release 116/merged, chr1+chr22 — the same
scope used throughout this thesis's other measurements.

This is a *separate* axis from the `cache-build-lpt-v2` branch (local,
single-host concurrency across CPU cores). Distribution here is scheduling
across nodes, not the LPT scheduler; each pod builds its shard with plain
sequential entity/chrom loops (`build_concurrency` is irrelevant here — one
node builds at most 1-2 entities, not enough units for it to matter).

No new Rust code. Reuses the existing `build_cache_entity(cache_root,
output_dir, entity, ..., chroms=None)` primitive already on `vepyr` master —
each entity's manifest write is already a merge operation, designed for
exactly this kind of partial/sharded build (see chapters/04-architektura.tex,
"W nowszym API istnieje również budowa wybranej encji...").

## Design

- **Input** (2.8 GB raw Ensembl cache, chr1+chr22 only): baked into the
  worker image at build time (`docker build`), so every node has it locally
  once the image is loaded — no shared/networked input filesystem needed.
- **Output**: each pod writes to local `emptyDir` storage (no shared output
  filesystem either — avoids needing an NFS server / CSI provisioner inside
  minikube, which is the most likely thing to break without live debugging).
  Results are pulled back to the host via `kubectl cp` after the Job
  completes, then merged/verified on the host.
- **Timing**: Kubernetes Job `.status.startTime` / `.status.completionTime`
  (native, no custom coordination).
- **Correctness**: same method as the LPT branch — sha256 of every produced
  `.parquet` shard, compared against `/tmp/lpt_ref_c1` (the serial
  `build_concurrency=1` reference tree from the LPT sweep). Each
  (entity, chrom) build is deterministic and independent of *which* machine
  runs it, so shards should be byte-identical, not just row-equal.
- **Sharding**: by entity (round-robin over the 6 entities across N pods),
  matching the granularity the existing `build_cache_entity` API already
  supports. With only 2 chromosomes in scope, chrom-level sharding would cap
  at N=2 useful workers; entity-level allows a real N=1/2/4/6 sweep.

## Not done yet / do not run without checking with the user first

Per instruction: code/scaffolding only for now. `minikube start` and any
actual cluster run are deferred until the local-concurrency (LPT) sweep is
finished — both are heavy on this machine's CPU/RAM and must not run at the
same time.

## Files

- `Dockerfile` — worker image: python + vepyr wheel (plain `master`, **not**
  the `cache-build-lpt-v2` patched build — keeps this experiment's variable
  isolated from the LPT one) + baked-in raw cache + `worker.py`.
- `worker.py` — pod entrypoint. Reads `JOB_COMPLETION_INDEX` (K8s indexed Job)
  and `WORKER_COUNT`, builds its entity shard via `vepyr._core.build_cache_entity`.
- `k8s/build-job.yaml.tmpl` — indexed Job template, `${WORKER_COUNT}`
  substituted by `run_sweep.sh`.
- `run_sweep.sh` — `setup` builds the image once and starts multi-node minikube;
  `sweep {chr22|chr1|full}` applies a job per worker count, waits for
  completion, `kubectl cp`s outputs back, times it, runs
  `verify_distributed.py`, tears down, repeats. `chr22` (N=1,2) is the fast
  smoke test to validate the mechanism before committing to the slow `chr1`
  or `full` (chr1+chr22, N=1,2,4) runs — same "prove it works small, then
  scale up" approach used for the LPT local-concurrency branch.
- `verify_distributed.py` — sha256 tree-hash compare against
  `/tmp/lpt_ref_c1`, reusing the same method as `lpt_build_concurrency_sweep.py`.

## Known risk / fallback

Multi-node `minikube --driver=qemu2` on an M1/16GB machine is the least
battle-tested part of this plan (real per-node VMs, not containers — see the
distributed-vs-parallel discussion this branch exists to settle honestly).
If it doesn't come up cleanly, the fallback (weaker distributed claim, but
still genuinely separate OS-level isolation, no shared memory) is
`docker compose --scale worker=N` with the same `worker.py`, described
honestly in the thesis as "izolowane kontenery na jednym hoście (K8s pody
jednowęzłowe), nie osobne maszyny fizyczne" rather than overclaimed.
