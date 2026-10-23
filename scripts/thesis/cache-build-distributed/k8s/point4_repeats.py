#!/usr/bin/env python3
import json
import subprocess
import time
from pathlib import Path

ROOT = Path.cwd() / "runs/point5_k8s/point4_repeats"
IMAGE = "vepyr-cache-worker:chr1-3"

def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    rows = []
    for p in (1, 4):
        for rep in range(1, 6):
            volume = f"vepyr-point4-{p}-r{rep}"
            subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
            cmd = ["docker", "run", "--rm", "--cpus=6", "--memory=24g", "--memory-swap=24g",
                   "-e", "WORKER_COUNT=1", "-e", "WORKER_INDEX=0", "-e", "CHROMS=22",
                   "-e", f"VEPYR_PARTITIONS={p}", "-e", f"VEPYR_VAR_PARTITIONS={p}",
                   "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
                   "-e", "OUTPUT_DIR=/output", "-v", f"{volume}:/output",
                   "-v", f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro", IMAGE]
            prefix = ROOT / f"p{p}_r{rep}"
            (prefix.with_suffix(".command")).write_text(" ".join(cmd) + "\n")
            t0 = time.time()
            with prefix.with_suffix(".log").open("w") as fh:
                proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT)
            row = {"partitions": p, "repeat": rep, "seconds_wall": time.time() - t0, "returncode": proc.returncode}
            rows.append(row)
            (ROOT / "summary.json").write_text(json.dumps(rows, indent=2))
            subprocess.run(["docker", "volume", "rm", volume], check=True, stdout=subprocess.DEVNULL)
            print(json.dumps(row), flush=True)
            if proc.returncode:
                raise SystemExit(proc.returncode)

if __name__ == "__main__":
    main()
