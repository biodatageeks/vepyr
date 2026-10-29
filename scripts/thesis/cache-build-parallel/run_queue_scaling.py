#!/usr/bin/env python3
"""Scaling curve of the dynamic cache-build queue (thesis, 2026-10-29).

Runs parallel_entity_queue.py unchanged (20 groups, 100 entity-group tasks,
RAYON_NUM_THREADS=1, partitions=1) with PARALLEL_PROCESSES in PROCESSES, REPEATS
times. Repeat-major order: every repeat runs each process count once, in an order
shuffled with a fixed seed, so slow drift of the machine does not fall on a single
process count. The per-task outputs (~31 GB) are deleted after each run; the
summary with per-task timings is kept.

Usage:
  nohup caffeinate -is .venv-lpt/bin/python3 run_queue_scaling.py OUT_DIR > OUT_DIR/runner.out 2>&1 &
Environment: QUEUE_PROCESSES (default 1,2,4,8,10), QUEUE_REPEATS (default 5).
"""
from __future__ import annotations

import json
import os
import platform
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUEUE = HERE / "parallel_entity_queue.py"
PY = sys.executable
PROCESSES = [int(x) for x in os.environ.get("QUEUE_PROCESSES", "1,2,4,8,10").split(",")]
REPEATS = int(os.environ.get("QUEUE_REPEATS", "5"))
MIN_FREE_GB = 80


def sh(cmd: list[str]) -> str:
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def snapshot() -> dict:
    return {
        "utc": datetime.now(timezone.utc).isoformat(),
        "therm": sh(["pmset", "-g", "therm"]),
        "batt": sh(["pmset", "-g", "batt"]),
        "load": os.getloadavg(),
        "free_gb": shutil.disk_usage(OUT).free / 1e9,
    }


def main() -> int:
    global OUT
    OUT = Path(sys.argv[1]).resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    current = OUT / "runs/parallel_full_cache/current"
    rng = random.Random(20261029)
    plan = []
    for rep in range(1, REPEATS + 1):
        order = PROCESSES[:]
        rng.shuffle(order)
        plan += [(rep, p) for p in order]
    (OUT / "plan.json").write_text(json.dumps({
        "plan": plan, "python": PY, "queue": str(QUEUE), "groups": 20,
        "macos": platform.mac_ver()[0], "machine": platform.machine(),
    }, indent=2) + "\n")
    done = set()
    jsonl = OUT / "runs.jsonl"
    if jsonl.exists():
        done = {(r["repeat"], r["processes"]) for r in map(json.loads, jsonl.read_text().splitlines()) if r.get("exit_code") == 0}
    for rep, p in plan:
        if (rep, p) in done:
            continue
        if shutil.disk_usage(OUT).free / 1e9 < MIN_FREE_GB:
            print(f"stop: less than {MIN_FREE_GB} GB free", flush=True)
            return 1
        shutil.rmtree(current, ignore_errors=True)
        before = snapshot()
        log = OUT / f"p{p:02d}_r{rep}.log"
        t0 = time.time()
        with log.open("w") as fh:
            proc = subprocess.run([PY, "-u", str(QUEUE)], cwd=OUT, stdout=fh, stderr=subprocess.STDOUT,
                                  env={**os.environ, "PARALLEL_GROUPS": "20", "PARALLEL_PROCESSES": str(p)})
        wall = time.time() - t0
        item = {"repeat": rep, "processes": p, "exit_code": proc.returncode, "seconds_wall_process": wall,
                "before": before, "after": snapshot()}
        summary = current / "summary.json"
        ok = proc.returncode == 0 and summary.exists()
        if summary.exists():
            s = json.loads(summary.read_text())
            item["seconds_wall"] = s["seconds_wall"]
            item["sum_task_seconds"] = sum(t["seconds"] for t in s["results"])
            item["max_task_seconds"] = max(t["seconds"] for t in s["results"])
            item["files"] = sum(len(st[1]) for t in s["results"] for st in t["stats"])
            (OUT / f"p{p:02d}_r{rep}.summary.json").write_text(json.dumps(s, indent=1) + "\n")
        with jsonl.open("a") as f:
            f.write(json.dumps(item) + "\n")
        print(json.dumps({k: v for k, v in item.items() if k not in ("before", "after")}), flush=True)
        shutil.rmtree(current, ignore_errors=True)
        if not ok:
            return 1
        time.sleep(20)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
