#!/usr/bin/env python3
"""Test distributed-style dynamic shard queues with bounded Docker workers."""
from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
IMAGE = os.environ.get("DIST_IMAGE", "vepyr-cache-worker:chr1-3")
ROOT = Path("runs/distributed_queue_sweep")
ENTITIES = ["variation", "transcript", "exon", "regulatory", "motif"]
MAX_ACTIVE = int(os.environ.get("DIST_MAX_ACTIVE", "10"))
GROUPS = int(os.environ.get("DIST_GROUPS", "20"))
MEMORY = os.environ.get("DIST_MEMORY", "4g")
CPU = os.environ.get("DIST_CPU", "1")
DISABLE_WARM = os.environ.get("DIST_DISABLE_WARM", "0") == "1"
MIN_FREE_GIB = 35.0


def free_gib() -> float:
    return shutil.disk_usage(Path.cwd()).free / 1024**3


def all_contigs() -> list[str]:
    return sorted(d.name for d in DATA.iterdir() if (d / "all_vars.gz").is_file())


def make_groups(contigs: list[str], count: int) -> list[list[str]]:
    # Include fixed per-contig startup/metadata cost. Byte-only balancing
    # leaves hundreds of tiny contigs in a few very slow shards.
    fixed_cost = 32 * 1024 * 1024
    bins: list[tuple[int, list[str]]] = [(0, []) for _ in range(count)]
    weighted = sorted(
        ((int((DATA / c / "all_vars.gz").stat().st_size), c) for c in contigs),
        reverse=True,
    )
    for size, contig in weighted:
        idx = min(range(count), key=lambda i: bins[i][0])
        total, group = bins[idx]
        bins[idx] = (total + size + fixed_cost, group + [contig])
    return [sorted(group) for _, group in bins if group]


def run_group(run_dir: Path, idx: int, group: list[str], entity: str | None = None) -> dict:
    volume = f"vepyr-dist-queue-{os.getpid()}-{idx}"
    subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
    chroms = ",".join(group)
    cmd = [
        "docker", "run", "--rm", "--cpus", CPU, "--memory", MEMORY,
        "--memory-swap", MEMORY,
        "-e", "WORKER_COUNT=1", "-e", "WORKER_INDEX=0",
        "-e", f"CHROMS={chroms}",
        *(["-e", f"VEPYR_ENTITIES={entity}"] if entity else []),
        "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
        "-e", "OUTPUT_DIR=/output", "-e", "MALLOC_ARENA_MAX=1",
        "-e", "RAYON_NUM_THREADS=1", "-e", "VEPYR_ENTITY_CONCURRENCY=1",
        "-e", "VEPYR_PARTITIONS=1", "-e", "VEPYR_VAR_PARTITIONS=1",
        *( ["-e", "VEPYR_DISABLE_WARM=1"] if DISABLE_WARM else [] ),
        "-v", f"{DATA}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro",
        "-v", f"{volume}:/output", IMAGE,
    ]
    log = run_dir / f"shard_{idx:03d}.log"
    (run_dir / f"shard_{idx:03d}.command").write_text(" ".join(cmd) + "\n")
    started = time.time()
    with log.open("w") as fh:
        fh.write("COMMAND: " + " ".join(cmd) + "\n")
        fh.flush()
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT)
    result = {
        "shard": idx, "entity": entity or "all", "contigs": len(group), "returncode": proc.returncode,
        "seconds": time.time() - started, "free_gib_after": free_gib(),
    }
    (run_dir / f"shard_{idx:03d}.json").write_text(json.dumps(result, indent=2) + "\n")
    subprocess.run(["docker", "volume", "rm", volume], check=False, stdout=subprocess.DEVNULL)
    return result


def main() -> None:
    run_dir = ROOT / f"groups_{GROUPS}_active_{MAX_ACTIVE}_{int(time.time())}"
    run_dir.mkdir(parents=True, exist_ok=True)
    contigs = all_contigs()
    groups = make_groups(contigs, GROUPS)
    entity_tasks = os.environ.get("DIST_ENTITY_TASKS", "0") == "1"
    tasks = [(idx, group, entity) for idx, group in enumerate(groups)
             for entity in (ENTITIES if entity_tasks else [None])]
    (run_dir / "manifest.json").write_text(json.dumps({
        "groups": GROUPS, "active": MAX_ACTIVE, "cpu": CPU, "memory": MEMORY,
        "contigs": len(contigs), "entity_tasks": entity_tasks, "disable_warm": DISABLE_WARM,
        "tasks": len(tasks), "shards": groups,
    }, indent=2) + "\n")
    started = time.time()
    results: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_ACTIVE) as pool:
        futures = {pool.submit(run_group, run_dir, task_idx, group, entity): task_idx
                   for task_idx, (group_idx, group, entity) in enumerate(tasks)}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            (run_dir / "progress.json").write_text(json.dumps({
                "completed": len(results), "total": len(tasks),
                "elapsed_seconds": time.time() - started, "results": sorted(results, key=lambda x: x["shard"]),
            }, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            if result["returncode"] != 0 or result["free_gib_after"] < MIN_FREE_GIB:
                raise RuntimeError(f"worker failure or disk guard: {result}")
    summary = {"groups": GROUPS, "tasks": len(tasks), "entity_tasks": entity_tasks,
               "disable_warm": DISABLE_WARM,
               "active": MAX_ACTIVE, "cpu": CPU, "memory": MEMORY,
               "wall_seconds": time.time() - started, "results": sorted(results, key=lambda x: x["shard"]),
               "all_success": all(r["returncode"] == 0 for r in results)}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
