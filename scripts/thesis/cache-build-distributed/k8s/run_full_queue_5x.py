#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path


ROOT = Path.cwd() / "runs/point5_k8s/range_shard_bench"
OUT = ROOT / "full_queue_5repeats"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    rows = []
    for i in range(1, 6):
        log = OUT / f"repeat_{i}.log"
        with log.open("w") as fh:
            proc = subprocess.run(["python3", "runs/point5_k8s/full_queue_bench.py"], stdout=fh, stderr=subprocess.STDOUT)
        summary = ROOT / "full_queue_n6" / "summary.json"
        if proc.returncode:
            raise SystemExit(proc.returncode)
        data = json.loads(summary.read_text())
        data["repeat"] = i
        rows.append(data)
        (OUT / "summary.json").write_text(json.dumps(rows, indent=2))
        shutil.rmtree(ROOT / "full_queue_n6")
        print(json.dumps({"repeat": i, "build_wall_seconds": data["build_wall_seconds"],
                          "preparation_seconds": data["preparation_seconds"]}), flush=True)


if __name__ == "__main__":
    main()
