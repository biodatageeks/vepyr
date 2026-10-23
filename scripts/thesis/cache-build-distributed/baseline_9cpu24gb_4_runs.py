#!/usr/bin/env python3
"""Complete constrained-baseline repeats 2-5 after distributed series."""
import json
from full_cache_20_runs import all_contigs, run_single, ROOT

def main() -> None:
    contigs = all_contigs()
    progress_path = ROOT / "progress.json"
    results = json.loads(progress_path.read_text()) if progress_path.exists() else []
    done = {r.get("repeat") for r in results if r.get("variant") == "baseline_9cpu24gb"}
    for repeat in range(1, 6):
        if repeat in done:
            continue
        result = run_single("baseline_9cpu24gb", repeat, contigs)
        results.append(result)
        progress_path.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result, indent=2), flush=True)
    (ROOT / "baseline_9cpu24gb_5_summary.json").write_text(json.dumps(
        [r for r in results if r.get("variant") == "baseline_9cpu24gb"], indent=2
    ) + "\n")

if __name__ == "__main__":
    main()
