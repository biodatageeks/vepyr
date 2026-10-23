#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from pathlib import Path

from benchmark_range_shards import ROOT, benchmark


def clean() -> None:
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
    out = ROOT / "best_chr1_n6_5repeats"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(1, 6):
        row = benchmark("1", 6, 3)
        row["repeat"] = i
        rows.append(row)
        (out / "summary.json").write_text(json.dumps(rows, indent=2))
        clean()
        print(json.dumps(row), flush=True)
    print(json.dumps(rows, indent=2), flush=True)


if __name__ == "__main__":
    main()
