#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path


ROOT = Path.cwd() / "runs/point5_k8s/baseline_full_5repeats"
ROOT.mkdir(parents=True, exist_ok=True)
IMAGE = "vepyr-cache-worker:chr1-3"


def main() -> None:
    results = []
    for i in range(1, 6):
        volume = f"vepyr-baseline-full-5x-{i}"
        subprocess.run(["docker", "volume", "create", volume], check=True, stdout=subprocess.DEVNULL)
        log = ROOT / f"repeat_{i}.log"
        command = [
            "docker", "run", "--rm", "--cpus=9", "--memory=24g", "--memory-swap=24g",
            "-e", "WORKER_COUNT=1", "-e", "WORKER_INDEX=0", "-e", "CHROMS=1,2,3",
            "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
            "-e", "OUTPUT_DIR=/output", "-v", f"{volume}:/output", IMAGE,
        ]
        (ROOT / f"repeat_{i}.command").write_text(" ".join(command) + "\n")
        t0 = time.time()
        with log.open("w") as fh:
            proc = subprocess.run(command, stdout=fh, stderr=subprocess.STDOUT)
        elapsed = time.time() - t0
        result = {"repeat": i, "seconds_wall": elapsed, "returncode": proc.returncode}
        results.append(result)
        (ROOT / "summary.json").write_text(json.dumps(results, indent=2))
        # Keep logs/commands, remove large output before next repeat.
        subprocess.run(["docker", "volume", "rm", volume], check=True, stdout=subprocess.DEVNULL)
        print(json.dumps(result), flush=True)
        if proc.returncode:
            raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
