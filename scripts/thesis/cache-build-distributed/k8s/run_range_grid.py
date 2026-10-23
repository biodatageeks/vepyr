#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from pathlib import Path

from benchmark_range_shards import ROOT, benchmark


TESTS = [
    ("22", 1, 1),
    ("22", 2, 2),
    ("22", 3, 3),
    ("22", 6, 3),
    ("22", 12, 3),
    ("1", 2, 2),
    ("1", 3, 3),
    ("1", 6, 3),
    ("1", 12, 3),
    ("3", 6, 3),
]


def clean_heavy() -> None:
    for p in (ROOT / "prepared", ROOT / "results"):
        if not p.exists():
            continue
        for q in p.rglob("*.parquet"):
            q.unlink(missing_ok=True)
        for q in p.rglob("all_vars_*.gz"):
            q.unlink(missing_ok=True)
        for q in list(p.rglob("cache")):
            if q.is_dir():
                shutil.rmtree(q)


def main() -> None:
    out = ROOT / "grid_summary.json"
    rows = []
    for i, (chrom, ranges, workers) in enumerate(TESTS, 1):
        print(f"[{i}/{len(TESTS)}] chr{chrom} ranges={ranges} workers={workers}", flush=True)
        row = benchmark(chrom, ranges, workers)
        rows.append(row)
        out.write_text(json.dumps(rows, indent=2))
        clean_heavy()
        print(json.dumps(row), flush=True)
    print(json.dumps(rows, indent=2), flush=True)


if __name__ == "__main__":
    main()
