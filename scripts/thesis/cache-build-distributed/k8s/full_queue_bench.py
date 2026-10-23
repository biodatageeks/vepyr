#!/usr/bin/env python3
"""Three-worker queue benchmark for full chr1-3 cache.

Variation is split into six physical ranges per chromosome. Other entities are
queued as whole-chromosome tasks because range filtering is not implemented for
them yet. Every worker uses local partitions=1.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from benchmark_range_shards import ROOT, prepare

IMAGE = "vepyr-cache-worker:chr1-3"
CHROMS = ["1", "2", "3"]
OTHER = ["transcript", "exon", "regulatory", "motif"]
WORKERS = 3


def main() -> None:
    run_dir = ROOT / "full_queue_n6"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    prepared = {}
    prep_seconds = 0.0
    for chrom in CHROMS:
        p, dt, ranges = prepare(chrom, 6)
        prepared[chrom] = (p, ranges)
        prep_seconds += dt

    tasks = []
    for chrom in CHROMS:
        p, ranges = prepared[chrom]
        for e in ranges:
            tasks.append({"kind": "variation", "chrom": chrom, "range": e, "prepared": str(p)})
    for chrom in CHROMS:
        for entity in OTHER:
            tasks.append({"kind": "entity", "chrom": chrom, "entity": entity})

    pending = list(tasks)
    active = []
    completed = []
    started = time.time()
    while pending or active:
        while pending and len(active) < WORKERS:
            task = pending.pop(0)
            idx = len(completed) + len(active)
            out = run_dir / f"task_{idx:02d}_{task['kind']}_{task['chrom']}"
            out.mkdir(parents=True)
            cmd = ["docker", "run", "--rm", "--cpus=2", "--memory=6g", "--memory-swap=6g",
                   "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
                   "-e", "OUTPUT_DIR=/output", "-e", f"CHROM={task['chrom']}",
                   "-e", f"ENTITY={task.get('entity', 'variation')}",
                   "-v", f"{out}:/output", "-v", f"{Path.cwd() / 'runs/point5_k8s/range_worker.py'}:/app/range_worker.py:ro"]
            if task["kind"] == "variation":
                source = Path(task["range"]["file"]).resolve()
                cache = out / "cache" / "homo_sapiens_merged" / "116_GRCh38"
                (cache / task["chrom"]).mkdir(parents=True)
                shutil.copy2(Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38/info.txt"), cache / "info.txt")
                os.link(source, cache / task["chrom"] / "all_vars.gz")
                cmd += ["-v", f"{out / 'cache'}:/raw_cache"]
            cmd += ["--entrypoint", "python", IMAGE, "/app/range_worker.py"]
            (out / "command.txt").write_text(" ".join(cmd) + "\n")
            log = (out / "worker.log").open("w")
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            active.append((proc, task, out, log, time.time()))
        time.sleep(1)
        still = []
        for proc, task, out, log, t0 in active:
            if proc.poll() is None:
                still.append((proc, task, out, log, t0))
                continue
            log.close()
            if proc.returncode:
                raise RuntimeError(f"task failed: {task}; see {out / 'worker.log'}")
            completed.append({"task": task, "seconds": time.time() - t0, "output": str(out)})
        active = still

    result = {"workers": WORKERS, "ranges_per_chrom": 6, "task_count": len(tasks),
              "preparation_seconds": prep_seconds, "build_wall_seconds": time.time() - started,
              "end_to_end_seconds": time.time() - started + prep_seconds, "tasks": completed}
    (run_dir / "summary.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
