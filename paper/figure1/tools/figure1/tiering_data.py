"""Warm/cold cache layout on the whole HG002 genome (Figure 1C3, C4 and the main text).

Reads data/raw/cache_tiering_wgs (vepyr performance-tests/cache-tiering, wgs results of branch
perf/cache-tiering-gain 240e030; also in vepyr PR #292): wgs_summary.json (CPU and wall time,
sums over chromosomes of per-chromosome medians, cold file cache), pages_per_buffer.json (pages of
the position column per lookup buffer) and wgs_pages.md (pages decoded, find + read, per layout).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

DATA = Path(__file__).resolve().parents[2] / "data/raw/cache_tiering_wgs"


def load() -> dict:
    summary = json.loads((DATA / "wgs_summary.json").read_text())["totals"]
    pages = json.loads((DATA / "pages_per_buffer.json").read_text())

    def total(layout: str, key: str, chrom: str | None = None) -> int:
        chroms = [chrom] if chrom else list(pages)
        return sum(b[key] for c in chroms for b in pages[c][layout].values())

    m = re.search(
        r"\*\*total\*\* \| [\d ]+/[\d ]+/[\d ]+ \| (\d+) / (\d+) / \d+",
        (DATA / "wgs_pages.md").read_text(),
    )
    decoded = {"tiered": int(m.group(1)), "sorted": int(m.group(2))}
    out = {"time": {}, "pages": {}}
    for workers in (1, 8):
        t = summary[f"cold_{workers}w"]
        for metric in ("cpu_s", "wall_s"):
            out["time"][(metric, workers)] = (t["tiered"][metric], t["sorted"][metric])
    find = {lay: total(lay, "cand_pages") for lay in ("tiered", "sorted")}
    out["pages"]["searched"] = (find["tiered"], find["sorted"])
    out["pages"]["matched"] = (
        total("tiered", "hit_pages"),
        total("sorted", "hit_pages"),
    )
    out["pages"]["read"] = (
        decoded["tiered"] - find["tiered"],
        decoded["sorted"] - find["sorted"],
    )
    out["chr22_matched"] = (
        total("sorted", "hit_pages", "chr22"),
        total("tiered", "hit_pages", "chr22"),
    )
    return out
