#!/usr/bin/env python3
"""Run four full-cache configurations, five repeats each, with cleanup."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path.cwd() / "runs/full_cache_20"
ROOT.mkdir(parents=True, exist_ok=True)
DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
IMAGE = "vepyr-cache-worker:chr1-3"
WORKER = Path.cwd() / "runs/point5_k8s/worker.py"
ENTITIES = ["variation", "transcript", "exon", "regulatory", "motif"]
VARIANTS = ["parallel", "distributed", "baseline_9cpu24gb", "baseline_unlimited"]
REPEATS = 5
MIN_FREE_GB = 25


def all_contigs() -> list[str]:
    return sorted(
        d.name for d in DATA.iterdir()
        if d.is_dir() and (d / "all_vars.gz").is_file()
    )


def group_contigs(contigs: list[str], n: int = 10) -> list[list[str]]:
    # Balance by compressed all_vars size, not by directory name.
    bins = [(0, []) for _ in range(n)]
    weighted = sorted(
        (( (DATA / c / "all_vars.gz").stat().st_size, c) for c in contigs),
        reverse=True,
    )
    for size, chrom in weighted:
        i = min(range(n), key=lambda j: bins[j][0])
        bins[i] = (bins[i][0] + size, bins[i][1] + [chrom])
    return [sorted(x[1]) for x in bins if x[1]]


def free_gb() -> float:
    return shutil.disk_usage(Path.cwd()).free / 1024**3


def base_env(chroms: list[str], out: str) -> list[str]:
    return [
        "-e", "WORKER_COUNT=1", "-e", "WORKER_INDEX=0",
        "-e", f"CHROMS={','.join(chroms)}",
        "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
        "-e", f"OUTPUT_DIR={out}", "-e", "MALLOC_ARENA_MAX=1",
    ]


def run_process(cmd: list[str], log: Path, timeout_poll: int = 10) -> dict:
    t0 = time.time()
    with log.open("w") as fh:
        fh.write("COMMAND: " + " ".join(cmd) + "\n")
        fh.flush()
        proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
        killed = False
        while proc.poll() is None:
            heartbeat = os.environ.get("VEPYR_HEARTBEAT")
            if heartbeat:
                Path(heartbeat).write_text(json.dumps({"pid": proc.pid, "ts": time.time()}))
            if free_gb() < MIN_FREE_GB:
                fh.write(f"DISK_GUARD_KILL free_gb={free_gb():.2f}\n")
                fh.flush()
                proc.terminate()
                killed = True
                break
            time.sleep(timeout_poll)
        code = proc.wait()
    return {"seconds_wall": time.time() - t0, "returncode": code, "disk_guard_kill": killed}


def run_single(variant: str, repeat: int, chroms: list[str]) -> dict:
    outdir = ROOT / variant / f"repeat_{repeat}"
    outdir.mkdir(parents=True, exist_ok=True)
    volume = f"vepyr-full-{variant}-{repeat}"
    subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
    cmd = ["docker", "run", "--rm"]
    if variant == "baseline_9cpu24gb":
        cmd += ["--cpus=9", "--memory=24g", "--memory-swap=24g"]
    elif variant == "baseline_10cpu":
        cmd += ["--cpus=10"]
    elif variant == "parallel":
        cmd += ["-e", "VEPYR_ENTITY_CONCURRENCY=3", "-e", "VEPYR_PARTITIONS=1"]
    cmd += base_env(chroms, "/output")
    cmd += ["-v", f"{DATA}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro",
            "-v", f"{volume}:/output", IMAGE]
    (outdir / "command.txt").write_text(" ".join(cmd) + "\n")
    result = run_process(cmd, outdir / "run.log")
    result.update({"variant": variant, "repeat": repeat, "contigs": len(chroms), "entities": ENTITIES})
    (outdir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    subprocess.run(["docker", "volume", "rm", volume], check=False, stdout=subprocess.DEVNULL)
    return result


def run_distributed(repeat: int, groups: list[list[str]]) -> dict:
    variant = "distributed"
    outdir = ROOT / variant / f"repeat_{repeat}"
    outdir.mkdir(parents=True, exist_ok=True)
    procs = []
    started = time.time()
    for i, group in enumerate(groups):
        volume = f"vepyr-full-distributed-{repeat}-{i}"
        subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
        cmd = ["docker", "run", "--rm", "--cpus=2", "--memory=6g", "--memory-swap=6g"]
        cmd += base_env(group, "/output")
        cmd += ["-v", f"{DATA}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro",
                "-v", f"{volume}:/output", IMAGE]
        (outdir / f"worker_{i:02d}.command").write_text(" ".join(cmd) + "\n")
        log = (outdir / f"worker_{i:02d}.log").open("w")
        log.write("COMMAND: " + " ".join(cmd) + "\n")
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        procs.append((i, group, volume, log, proc, time.time()))
    rows = []
    guard = False
    while procs:
        if free_gb() < MIN_FREE_GB:
            guard = True
            for _, _, _, log, proc, _ in procs:
                log.write(f"DISK_GUARD_KILL free_gb={free_gb():.2f}\n")
                log.flush(); proc.terminate()
        still = []
        for i, group, volume, log, proc, t0 in procs:
            if proc.poll() is None:
                still.append((i, group, volume, log, proc, t0)); continue
            code = proc.wait(); log.close()
            rows.append({"worker": i, "contigs": len(group), "seconds_wall": time.time()-t0, "returncode": code})
            subprocess.run(["docker", "volume", "rm", volume], check=False, stdout=subprocess.DEVNULL)
        procs = still
        time.sleep(10 if procs else 0)
    result = {"variant": variant, "repeat": repeat, "workers": len(groups),
              "wall_seconds": time.time()-started, "disk_guard_kill": guard, "workers_result": rows}
    (outdir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    contigs = all_contigs()
    groups = group_contigs(contigs, 10)
    (ROOT / "manifest.json").write_text(json.dumps({"contigs": contigs, "count": len(contigs), "groups": groups}, indent=2) + "\n")
    all_results = []
    for repeat in range(1, REPEATS + 1):
        all_results.append(run_single("parallel", repeat, contigs))
        all_results.append(run_distributed(repeat, groups))
        all_results.append(run_single("baseline_9cpu24gb", repeat, contigs))
        all_results.append(run_single("baseline_unlimited", repeat, contigs))
        (ROOT / "progress.json").write_text(json.dumps(all_results, indent=2) + "\n")
        print(json.dumps(all_results[-4:], indent=2), flush=True)
    (ROOT / "summary.json").write_text(json.dumps(all_results, indent=2) + "\n")


if __name__ == "__main__":
    main()
