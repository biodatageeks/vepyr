#!/usr/bin/env python3
"""Five full-cache baseline runs using all 10 CPU, with resumable progress."""
import json
import os
from full_cache_20_runs import all_contigs, run_single, ROOT

VARIANT = "baseline_10cpu"

def main() -> None:
    contigs = all_contigs()
    progress_path = ROOT / "progress.json"
    results = json.loads(progress_path.read_text()) if progress_path.exists() else []
    done = {r.get("repeat") for r in results if r.get("variant") == VARIANT and r.get("returncode") == 0}
    os.environ["VEPYR_HEARTBEAT"] = str(ROOT / "baseline_10cpu.heartbeat.json")
    for repeat in range(1, 6):
        if repeat in done:
            continue
        result = run_single(VARIANT, repeat, contigs)
        results.append(result)
        progress_path.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result, indent=2), flush=True)
    (ROOT / "baseline_10cpu_5_summary.json").write_text(json.dumps(
        [r for r in results if r.get("variant") == VARIANT], indent=2
    ) + "\n")

if __name__ == "__main__":
    main()
