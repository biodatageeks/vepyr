#!/usr/bin/env python3
"""Complete distributed full-cache repeats 2-5 after stopping parallel sweep."""
import json
from pathlib import Path
from full_cache_20_runs import all_contigs, group_contigs, run_distributed, ROOT

def main() -> None:
    contigs = all_contigs()
    groups = group_contigs(contigs, 10)
    progress_path = ROOT / "progress.json"
    results = json.loads(progress_path.read_text()) if progress_path.exists() else []
    done = {r.get("repeat") for r in results if r.get("variant") == "distributed"}
    for repeat in range(1, 6):
        if repeat in done:
            continue
        result = run_distributed(repeat, groups)
        results.append(result)
        progress_path.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result, indent=2), flush=True)
    (ROOT / "distributed_5_summary.json").write_text(json.dumps(
        [r for r in results if r.get("variant") == "distributed"], indent=2
    ) + "\n")

if __name__ == "__main__":
    main()
