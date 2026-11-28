"""Compare the per-buffer page and byte counts of the page tool for the tiered and sorted shards.

    python pages_summary.py TIERED.txt SORTED.txt OUT_DIR

TIERED.txt / SORTED.txt are the output of `pages measure <shard> <variants.tsv> 5000 ext payload`.
Writes OUT_DIR/pages.csv (one row per buffer and metric) and OUT_DIR/pages.json (per buffer, and
the range over full buffers of the reduction 1 - tiered/sorted; negative means more with tiering).
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

# Tool column -> meaning, in report order.
METRICS = {
    "cand_pages": "position pages decoded to find the rows (phase 2)",
    "p2_start_pages_in_fetched_full": "position pages covered by the bytes fetched in phase 2",
    "p2_bytes": "bytes fetched in phase 2",
    "p2_ops": "read operations in phase 2",
    "hit_pages": "position pages holding a matched row",
    "p3_req_ranges": "pages decoded to read the matched rows, all columns (phase 3)",
    "p3_all_leaf_pages_in_fetched": "pages covered by the bytes fetched in phase 3",
    "p3_bytes": "bytes fetched in phase 3",
    "p3_ops": "read operations in phase 3",
}


def parse(path: Path) -> dict[int, dict]:
    header: list[str] | None = None
    rows: dict[int, dict] = {}
    for line in Path(path).read_text().splitlines():
        if line.startswith("  buf\t"):
            header = line.strip().split("\t")
        elif m := re.match(r"  HIT buf=(\d+) .*?hit_pages=(\d+)", line):
            rows.setdefault(int(m.group(1)), {})["hit_pages"] = int(m.group(2))
        elif header and re.match(r"  \d+\t", line):
            values = dict(zip(header, line.strip().split("\t")))
            buf = int(values.pop("buf"))
            rows.setdefault(buf, {}).update({k: int(v) for k, v in values.items()})
    return rows


def compare(
    tiered: dict[int, dict], sorted_: dict[int, dict], metrics=tuple(METRICS)
) -> dict:
    if set(tiered) != set(sorted_):
        raise ValueError("the two files cover different buffers")
    full = max(r["recs"] for r in tiered.values())
    buffers: dict[int, dict] = {}
    ranges: dict[str, list[float]] = {}
    for b in sorted(tiered):
        t, s = tiered[b], sorted_[b]
        if t["recs"] != s["recs"]:
            raise ValueError(f"buffer {b}: {t['recs']} vs {s['recs']} records")
        buffers[b] = {"recs": t["recs"]}
        for m in metrics:
            red = 1 - t[m] / s[m] if s[m] else None
            buffers[b][m] = {"tiered": t[m], "sorted": s[m], "reduction": red}
            if t["recs"] == full and red is not None:
                lo, hi = ranges.get(m, [red, red])
                ranges[m] = [min(lo, red), max(hi, red)]
    return {"buffers": buffers, "range_full_buffers": ranges}


def main() -> None:
    tiered_txt, sorted_txt, out_dir = (Path(a) for a in sys.argv[1:4])
    result = compare(parse(tiered_txt), parse(sorted_txt))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages.json").write_text(
        json.dumps({"metrics": METRICS, **result}, indent=2) + "\n"
    )
    with (out_dir / "pages.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["buffer", "records", "metric", "tiered", "sorted", "reduction"])
        for b, row in result["buffers"].items():
            for m in METRICS:
                v = row[m]
                w.writerow(
                    [b, row["recs"], m, v["tiered"], v["sorted"], v["reduction"]]
                )
    for m, (lo, hi) in result["range_full_buffers"].items():
        print(
            f"{m:32s} reduction over full buffers {lo:+.1%} .. {hi:+.1%}  ({METRICS[m]})"
        )


if __name__ == "__main__":
    main()
