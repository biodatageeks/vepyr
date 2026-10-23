#!/usr/bin/env python3
import json, os, shutil, subprocess, sys, time
from pathlib import Path

ROOT = Path.cwd() / "runs/point5_k8s/range_shard_bench"
RUNNER = Path.cwd() / "runs/point5_k8s/full_queue_arch_workers.py"
RESULTS = ROOT / "arch_n8_repeats"

def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    rows = []
    for repeat in range(1, 6):
        out = ROOT / "full_queue_arch_n8"
        prep = ROOT / "prepared"
        shutil.rmtree(out, ignore_errors=True)
        shutil.rmtree(prep, ignore_errors=True)
        log = RESULTS / f"repeat_{repeat}.log"
        started = time.time()
        env = os.environ.copy(); env["ARCH_WORKERS"] = "8"
        with log.open("w") as fh:
            proc = subprocess.run([sys.executable, "-u", str(RUNNER)], env=env, stdout=fh, stderr=subprocess.STDOUT)
        elapsed = time.time() - started
        if proc.returncode:
            raise SystemExit(f"repeat {repeat} failed; see {log}")
        summary = json.loads((out / "summary.json").read_text())
        row = {"repeat": repeat, "wall_seconds": elapsed, **{k: summary[k] for k in ("preparation_seconds", "build_wall_seconds", "end_to_end_seconds", "workers", "task_count")}}
        rows.append(row)
        (RESULTS / "summary.json").write_text(json.dumps(rows, indent=2))
        shutil.rmtree(out, ignore_errors=True)
        shutil.rmtree(prep, ignore_errors=True)
        print(json.dumps(row), flush=True)

if __name__ == "__main__":
    main()
