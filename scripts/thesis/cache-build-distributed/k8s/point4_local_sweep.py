#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path.cwd() / "runs/point5_k8s/point4_local_sweep"
IMAGE = "vepyr-cache-worker:chr1-3"


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    rows = []
    for p in [1, 2, 4, 6]:
        volume = f"vepyr-point4-{p}"
        subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
        cmd = ["docker", "run", "--rm", "--cpus=6", "--memory=24g", "--memory-swap=24g",
               "-e", "WORKER_COUNT=1", "-e", "WORKER_INDEX=0", "-e", "CHROMS=22",
               "-e", "VEPYR_PARTITIONS=" + str(p), "-e", "VEPYR_VAR_PARTITIONS=" + str(p),
               "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
               "-e", "OUTPUT_DIR=/output", "-v", f"{volume}:/output",
               "-v", f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro", IMAGE]
        (ROOT / f"p{p}.command").write_text(" ".join(cmd) + "\n")
        log = ROOT / f"p{p}.log"
        t0 = time.time()
        with log.open("w") as fh:
            proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT)
        elapsed = time.time() - t0
        row = {"partitions": p, "seconds_wall": elapsed, "returncode": proc.returncode}
        rows.append(row)
        (ROOT / "summary.json").write_text(json.dumps(rows, indent=2))
        subprocess.run(["docker", "volume", "rm", volume], check=True, stdout=subprocess.DEVNULL)
        print(json.dumps(row), flush=True)
        if proc.returncode:
            raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
