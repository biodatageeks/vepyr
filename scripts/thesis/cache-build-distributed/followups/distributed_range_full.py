#!/usr/bin/env python3
"""External BGZF-range plus context-group cache build probe.

Every Docker worker is synchronous: one CPU, one DataFusion partition and one
Rayon thread. Variation ranges are split outside the worker; context entities
use disjoint contig groups. Output stays in per-task host directories so row
counts and shards can be validated after the timing run.
"""
from __future__ import annotations

import concurrent.futures
import json
import math
import os
import shutil
import subprocess
import time
from pathlib import Path

DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
IMAGE = os.environ.get("DIST_IMAGE", "vepyr-cache-worker:range2")
ROOT = Path(os.environ.get("DIST_ROOT", "runs/distributed_range_full"))
MAX_ACTIVE = int(os.environ.get("DIST_MAX_ACTIVE", "9"))
CPU = os.environ.get("DIST_CPU", "1")
MEMORY = os.environ.get("DIST_MEMORY", "2.6g")
MIN_FREE_GIB = float(os.environ.get("DIST_MIN_FREE_GIB", "8"))
GROUP_COUNT = int(os.environ.get("DIST_CONTEXT_GROUPS", "40"))
RANGE_TARGET = int(os.environ.get("DIST_RANGE_TARGET_MB", "512")) * 1024 * 1024
CONTEXT_ENTITIES = ("transcript", "exon", "regulatory", "motif")


def free_gib() -> float:
    return shutil.disk_usage(Path.cwd()).free / 1024**3


def contigs() -> list[str]:
    return sorted(d.name for d in DATA.iterdir() if (d / "all_vars.gz").is_file())


def context_groups(cs: list[str]) -> list[list[str]]:
    bins: list[tuple[int, list[str]]] = [(0, []) for _ in range(GROUP_COUNT)]
    fixed = 32 * 1024 * 1024
    weighted = sorted(
        ((int((DATA / c / "all_vars.gz").stat().st_size), c) for c in cs),
        reverse=True,
    )
    for size, c in weighted:
        i = min(range(GROUP_COUNT), key=lambda j: bins[j][0])
        total, group = bins[i]
        bins[i] = (total + size + fixed, group + [c])
    return [sorted(g) for _, g in bins if g]


def make_tasks(cs: list[str]) -> list[dict]:
    tasks: list[dict] = []
    tid = 0
    large = []
    small = []
    for c in cs:
        size = (DATA / c / "all_vars.gz").stat().st_size
        if size < RANGE_TARGET:
            small.append(c)
            continue
        large.append(c)
        count = math.ceil(size / RANGE_TARGET)
        for index in range(count):
            tasks.append({
                "id": tid, "kind": "variation_range", "entity": "variation",
                "chrom": f"chr{c}", "range_index": index, "range_count": count,
                "contigs": [c],
            })
            tid += 1
    # Tiny scaffold variation files are grouped. Creating one Docker process
    # per scaffold would spend more time on startup than on decompression.
    for group_index, group in enumerate(context_groups(small)):
        tasks.append({
            "id": tid, "kind": "variation_group", "entity": "variation",
            "group": group_index, "contigs": group,
        })
        tid += 1
    for entity in CONTEXT_ENTITIES:
        for group_index, group in enumerate(context_groups(cs)):
            tasks.append({
                "id": tid, "kind": "context_group", "entity": entity,
                "group": group_index, "contigs": group,
            })
            tid += 1
    return tasks


def command_for(task: dict, output: Path) -> list[str]:
    env = {
        "WORKER_COUNT": "1", "WORKER_INDEX": "0",
        "VEPYR_ENTITIES": task["entity"], "VEPYR_PARTITIONS": "1",
        "VEPYR_VAR_PARTITIONS": "1", "CACHE_ROOT": "/raw_cache/homo_sapiens_merged/116_GRCh38",
        "OUTPUT_DIR": "/output", "CACHE_TYPE": "merged", "EXPECTED_CACHE_VERSION": "116",
        "RAYON_NUM_THREADS": "1", "MALLOC_ARENA_MAX": "1",
    }
    if task["kind"] == "variation_range":
        env.update({
            "CHROMS": task["chrom"], "VEPYR_DISABLE_WARM": "1",
            "VEPYR_VAR_RANGE_INDEX": str(task["range_index"]),
            "VEPYR_VAR_RANGE_COUNT": str(task["range_count"]),
        })
    else:
        env["CHROMS"] = ",".join(task["contigs"])
        if task["entity"] == "variation":
            env["VEPYR_DISABLE_WARM"] = "1"
    cmd = ["docker", "run", "--rm", "--cpus", CPU, "--memory", MEMORY,
           "--memory-swap", MEMORY]
    for key, value in env.items():
        cmd += ["-e", f"{key}={value}"]
    cmd += ["-v", f"{DATA}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro",
            "-v", f"{output.resolve()}:/output", IMAGE]
    return cmd


def run_task(run: Path, task: dict) -> dict:
    output = run / "output" / f"task_{task['id']:04d}"
    output.mkdir(parents=True, exist_ok=True)
    log = run / "logs" / f"task_{task['id']:04d}.log"
    cmd = command_for(task, output)
    (run / "commands" / f"task_{task['id']:04d}.command").write_text(" ".join(cmd) + "\n")
    started = time.time()
    with log.open("w") as fh:
        fh.write("COMMAND: " + " ".join(cmd) + "\n")
        fh.flush()
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT)
    result = dict(task)
    result.update({
        "returncode": proc.returncode, "seconds": time.time() - started,
        "free_gib_after": free_gib(),
        "output_bytes": sum(p.stat().st_size for p in output.rglob("*") if p.is_file()),
    })
    (run / "results" / f"task_{task['id']:04d}.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    run = ROOT / f"run_{int(time.time())}"
    for name in ("output", "logs", "commands", "results"):
        (run / name).mkdir(parents=True)
    cs = contigs()
    tasks = make_tasks(cs)
    if os.environ.get("DIST_ONLY_CONTEXT") == "1":
        tasks = [task for task in tasks if task["kind"] == "context_group"]
    manifest = {"image": IMAGE, "active": MAX_ACTIVE, "cpu": CPU, "memory": MEMORY,
                "contigs": len(cs), "tasks": len(tasks), "context_groups": GROUP_COUNT,
                "range_target_mb": RANGE_TARGET // 1024 // 1024, "entities": list(CONTEXT_ENTITIES) + ["variation"]}
    (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (run / "tasks.json").write_text(json.dumps(tasks, indent=2) + "\n")
    if os.environ.get("DIST_DRY_RUN") == "1":
        print(json.dumps(manifest, indent=2)); return
    started = time.time(); results: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_ACTIVE) as pool:
        futures = [pool.submit(run_task, run, task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            result = future.result(); results.append(result)
            (run / "progress.json").write_text(json.dumps({
                "completed": len(results), "total": len(tasks),
                "elapsed_seconds": time.time() - started,
                "free_gib": free_gib(), "results": sorted(results, key=lambda x: x["id"]),
            }, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            if result["returncode"] != 0 or result["free_gib_after"] < MIN_FREE_GIB:
                raise RuntimeError(f"worker failure or disk guard: {result}")
    summary = {"wall_seconds": time.time() - started, **manifest,
               "completed": len(results), "all_success": all(r["returncode"] == 0 for r in results),
               "results": sorted(results, key=lambda x: x["id"])}
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
